import os
import base64
import json
import time
import uuid
import threading
import requests
import boto3
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, render_template, request, Response, jsonify
from botocore.config import Config

app = Flask(__name__)

# ── Config ─────────────────────────────────────────────────────────────────
API_KEY        = os.environ.get("GEMINI_API_KEY")
MODEL_URL      = "https://generativelanguage.googleapis.com/v1beta/models/gemini-3.1-flash-image:generateContent"
COST_PER_IMAGE = 0.067
TIKTOK_SIZE    = 7
FIXED_CAPTION  = "3 Maillot Acheté 1 Offert 🎁 #volakits #ete #foot"
SCHEDULE_TIMES = ["12:30", "16:00", "19:30", "21:00"]

R2_ENDPOINT   = os.environ.get("R2_ENDPOINT")
R2_ACCESS_KEY = os.environ.get("R2_ACCESS_KEY_ID")
R2_SECRET_KEY = os.environ.get("R2_SECRET_ACCESS_KEY")
R2_BUCKET     = os.environ.get("R2_BUCKET", "jersey-templates")

ROBINREACH_API_KEY  = os.environ.get("ROBINREACH_API_KEY")
ROBINREACH_BRAND_ID = os.environ.get("ROBINREACH_BRAND_ID")

# Préfixes R2
PFX_QUEUE     = "queue/"
PFX_SCHEDULED = "scheduled/"
PFX_TEMPLATES = "templates/"
PFX_LOGS      = "logs/"
KEY_BUFFER    = "buffer/pending.json"
KEY_COUNTER   = "meta/tiktok_counter.json"
KEY_ACCOUNTS  = "meta/accounts.json"

# Lock pour éviter race conditions sur le compteur/buffer
_r2_lock = threading.Lock()

def get_r2():
    if not R2_ENDPOINT:
        return None
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )

def r2_put_json(key, data):
    r2 = get_r2()
    if not r2: return False
    try:
        r2.put_object(Bucket=R2_BUCKET, Key=key,
            Body=json.dumps(data, ensure_ascii=False).encode(),
            ContentType="application/json")
        return True
    except Exception as e:
        print(f"[R2 put_json error] {key}: {e}")
        return False

def r2_get_json(key):
    r2 = get_r2()
    if not r2: return None
    try:
        obj = r2.get_object(Bucket=R2_BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode())
    except Exception:
        return None

def r2_list_keys(prefix, suffix=".json"):
    """Liste les clés R2 avec pagination complète"""
    r2 = get_r2()
    if not r2: return []
    keys = []
    kwargs = {"Bucket": R2_BUCKET, "Prefix": prefix}
    while True:
        resp = r2.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            if obj["Key"].endswith(suffix):
                keys.append(obj["Key"])
        if not resp.get("IsTruncated"):
            break
        kwargs["ContinuationToken"] = resp["NextContinuationToken"]
    return keys

def r2_delete(key):
    r2 = get_r2()
    if not r2: return False
    try:
        r2.delete_object(Bucket=R2_BUCKET, Key=key)
        return True
    except Exception:
        return False

def r2_put_image(key, img_bytes, mime="image/png"):
    r2 = get_r2()
    if not r2: return False
    try:
        r2.put_object(Bucket=R2_BUCKET, Key=key, Body=img_bytes, ContentType=mime)
        return True
    except Exception as e:
        print(f"[R2 put_image error] {key}: {e}")
        return False

def r2_presigned(key, expires=86400):  # 24h
    r2 = get_r2()
    if not r2: return None
    try:
        return r2.generate_presigned_url("get_object",
            Params={"Bucket": R2_BUCKET, "Key": key}, ExpiresIn=expires)
    except Exception:
        return None

# ── Compteur TikTok (atomique via R2) ─────────────────────────────────────
def get_next_tiktok_number():
    """Incrémente et retourne le prochain numéro de TikTok"""
    data = r2_get_json(KEY_COUNTER) or {"next": 1}
    num = data["next"]
    data["next"] = num + 1
    r2_put_json(KEY_COUNTER, data)
    return num

# ── Logs persistants sur R2 ────────────────────────────────────────────────
def log_generation(user, success):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"{PFX_LOGS}{today}.jsonl"
    entry = json.dumps({
        "ts": datetime.now(timezone.utc).isoformat(),
        "user": user or "Inconnu",
        "success": success
    }) + "\n"
    r2 = get_r2()
    if not r2: return
    try:
        try:
            obj = r2.get_object(Bucket=R2_BUCKET, Key=key)
            existing = obj["Body"].read().decode()
        except Exception:
            existing = ""
        r2.put_object(Bucket=R2_BUCKET, Key=key,
            Body=(existing + entry).encode(), ContentType="text/plain")
    except Exception as e:
        print(f"[log error] {e}")

def read_logs(days=30):
    r2 = get_r2()
    if not r2: return []
    entries = []
    keys = r2_list_keys(PFX_LOGS, suffix=".jsonl")
    for key in sorted(keys)[-days:]:
        try:
            obj = r2.get_object(Bucket=R2_BUCKET, Key=key)
            for line in obj["Body"].read().decode().splitlines():
                if line.strip():
                    try: entries.append(json.loads(line))
                    except: pass
        except Exception:
            pass
    return entries

# ── Comptes TikTok (stockés sur R2) ───────────────────────────────────────
# ── Comptes TikTok RobinReach (IDs réels) ──────────────────────────────────
ROBINREACH_ACCOUNTS = {
    "Volakits Principal": 11739,   # compte principal, wael
    "Volakits2": 11848,
    "Volakits (wassim)": 11846,
    "Volakits (seik)": 11847,
}
DEFAULT_MAIN_ACCOUNT = "Volakits Principal"

def get_accounts():
    data = r2_get_json(KEY_ACCOUNTS)
    if data and data.get("main"):
        return data
    # Valeur par défaut si rien configuré
    return {
        "main": DEFAULT_MAIN_ACCOUNT,
        "others": [k for k in ROBINREACH_ACCOUNTS if k != DEFAULT_MAIN_ACCOUNT]
    }

def save_accounts(data):
    return r2_put_json(KEY_ACCOUNTS, data)

# ── Buffer persistant R2 ───────────────────────────────────────────────────

# ── Buffer local (/tmp) ────────────────────────────────────────────────────
BUFFER_FILE = "/tmp/tiktok_buffer.json"

def get_buffer():
    try:
        if os.path.exists(BUFFER_FILE):
            with open(BUFFER_FILE) as f:
                return json.load(f)
    except Exception:
        pass
    return {"images_b64": [], "flockages": [], "user": None}

def _save_buffer(buf):
    try:
        with open(BUFFER_FILE, "w") as f:
            json.dump(buf, f)
        return True
    except Exception as e:
        print(f"[BUFFER SAVE ERROR] {e}")
        return False

def add_to_buffer_and_create_tiktoks(new_images_b64, new_flockages, user):
    buf = get_buffer()
    if not buf.get("user"):
        buf["user"] = user
    buf["images_b64"].extend(new_images_b64)
    buf["flockages"].extend(new_flockages)
    print(f"[BUFFER] Now has {len(buf['images_b64'])} images")

    created = []
    while len(buf["images_b64"]) >= TIKTOK_SIZE:
        batch_b64  = buf["images_b64"][:TIKTOK_SIZE]
        batch_floc = buf["flockages"][:TIKTOK_SIZE]
        tiktok_num = get_next_tiktok_number()
        print(f"[BUFFER] Creating TikTok {tiktok_num}...")
        _save_tiktok(tiktok_num, batch_b64, buf["user"], batch_floc)
        created.append(tiktok_num)
        buf["images_b64"] = buf["images_b64"][TIKTOK_SIZE:]
        buf["flockages"]  = buf["flockages"][TIKTOK_SIZE:]

    _save_buffer(buf)
    print(f"[BUFFER] Done — {len(created)} TikToks created, {len(buf['images_b64'])} pending")
    return created, len(buf["images_b64"])


# ── TikTok queue ───────────────────────────────────────────────────────────
def _save_tiktok(num, images_b64, user, flockages):
    r2 = get_r2()
    if not r2: return False
    image_keys = []
    for i, b64 in enumerate(images_b64):
        if not b64: continue
        k = f"queue/imgs/tiktok_{num:04d}_{i+1:02d}.png"
        r2_put_image(k, base64.b64decode(b64))
        image_keys.append(k)

    meta = {
        "id": f"tiktok_{num:04d}",
        "number": num,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "user": user,
        "image_keys": image_keys,
        "flockages": flockages,
        "status": "pending",
        "account": None,
        "scheduled_at": None,
    }
    return r2_put_json(f"{PFX_QUEUE}tiktok_{num:04d}.json", meta)

def _enrich_tiktok(data, key):
    """Ajoute les URLs signées et la clé R2"""
    data["image_urls"] = [r2_presigned(k) for k in data.get("image_keys", [])]
    data["r2_key"] = key
    return data

def get_queue():
    keys = sorted(r2_list_keys(PFX_QUEUE))
    result = []
    for k in keys:
        # Ignorer les images dans queue/imgs/
        if "/imgs/" in k: continue
        d = r2_get_json(k)
        if d: result.append(_enrich_tiktok(d, k))
    return result

def get_scheduled():
    keys = sorted(r2_list_keys(PFX_SCHEDULED), reverse=True)
    result = []
    for k in keys[:200]:  # limiter à 200 derniers
        if "/imgs/" in k: continue
        d = r2_get_json(k)
        if d: result.append(_enrich_tiktok(d, k))
    return result

def move_to_scheduled(queue_key, account, dt_str):
    data = r2_get_json(queue_key)
    if not data: return False
    data["status"] = "scheduled"
    data["account"] = account
    data["scheduled_at"] = dt_str
    # Déplacer les images vers scheduled/imgs/
    new_img_keys = []
    for old_k in data.get("image_keys", []):
        new_k = old_k.replace("queue/imgs/", "scheduled/imgs/")
        r2 = get_r2()
        if r2:
            try:
                r2.copy_object(Bucket=R2_BUCKET,
                    CopySource={"Bucket": R2_BUCKET, "Key": old_k},
                    Key=new_k)
                r2_delete(old_k)
                new_img_keys.append(new_k)
            except Exception:
                new_img_keys.append(old_k)
    data["image_keys"] = new_img_keys
    new_key = queue_key.replace(PFX_QUEUE, PFX_SCHEDULED)
    r2_put_json(new_key, data)
    r2_delete(queue_key)
    return True

# ── Prompt Gemini ──────────────────────────────────────────────────────────
def build_prompt(name, number, name_below=None):
    name = name.strip().upper()
    number = number.strip()
    name_below = (name_below or name).strip().upper()
    parts = ["Edit this image of a sports jersey (back view). Precise text-replacement only, not a redesign."]
    if name:
        parts.append(f'Replace the main back name text with "{name}". Keep exact font, weight, outline, color, size and position.')
    if number:
        digits = ", ".join(f'"{d}"' for d in number)
        parts.append(f'Replace the large back number with "{number}" ({len(number)} digit(s): {digits}). Render ALL digits, none missing. Keep font, color, outline and center position. Scale digit width (not height/stroke) if digit count differs. No added logos or marks.')
    if name_below:
        parts.append(f'Replace the smaller text below the badge with "{name_below}". Keep same position gap, font, size, color and outline.')
    parts.append("Keep ALL else identical: colors, texture, pattern, outlines, lighting, shadows, background, tags. Only swap text content.")
    return " ".join(parts)

def call_gemini(img_bytes, mime, name, number, name_below=None, max_retries=2):
    img_b64 = base64.b64encode(img_bytes).decode()
    prompt = build_prompt(name, number, name_below)
    payload = {"contents": [{"parts": [{"text": prompt}, {"inline_data": {"mime_type": mime, "data": img_b64}}]}]}
    last_error = None
    for _ in range(max_retries + 1):
        try:
            resp = requests.post(MODEL_URL,
                headers={"x-goog-api-key": API_KEY, "Content-Type": "application/json"},
                json=payload, timeout=120)
        except requests.RequestException as e:
            last_error = f"Erreur réseau: {e}"; time.sleep(1); continue
        if resp.status_code != 200:
            last_error = f"API {resp.status_code}: {resp.text[:200]}"; time.sleep(1); continue
        data = resp.json()
        try:
            for part in data["candidates"][0]["content"]["parts"]:
                if "inlineData" in part:
                    return {"success": True, "image": part["inlineData"]["data"]}
            last_error = "Pas d'image dans la réponse."
        except (KeyError, IndexError) as e:
            last_error = f"Réponse inattendue: {e}"
    return {"success": False, "error": last_error}

# ── Pages ──────────────────────────────────────────────────────────────────
@app.route("/")
def index(): return render_template("index.html")

@app.route("/queue")
def queue_page(): return render_template("queue.html")

@app.route("/scheduled")
def scheduled_page(): return render_template("scheduled.html")

@app.route("/templates")
def templates_page(): return render_template("templates.html")

@app.route("/dashboard")
def dashboard():
    entries = read_logs(7)
    today = datetime.now(timezone.utc).date().isoformat()
    today_e = [e for e in entries if e.get("ts","")[:10] == today]
    by_user = {}
    for e in today_e:
        u = e.get("user","?"); by_user[u] = by_user.get(u,0)+1
    queue_count = len([k for k in r2_list_keys(PFX_QUEUE) if "/imgs/" not in k])
    sched_count = len([k for k in r2_list_keys(PFX_SCHEDULED) if "/imgs/" not in k])
    today_count = len(today_e)
    return render_template("dashboard.html",
        today_count=today_count,
        today_success=sum(1 for e in today_e if e.get("success")),
        today_cost=round(today_count*COST_PER_IMAGE,2),
        tiktoks_done=today_count//7, tiktoks_goal=20,
        by_user=by_user, recent_sessions=[],
        queue_count=queue_count, scheduled_count=sched_count)

@app.route("/stats")
def stats():
    entries = read_logs(30)
    by_day_user = {}
    total_count = 0; total_success = 0
    for e in entries:
        day = e.get("ts","")[:10]; user = e.get("user","?"); key = (day,user)
        by_day_user.setdefault(key,{"total":0,"success":0})
        by_day_user[key]["total"] += 1; total_count += 1
        if e.get("success"): by_day_user[key]["success"]+=1; total_success+=1
    rows = [{"day":d,"user":u,"total":c["total"],"success":c["success"],"cost":round(c["total"]*COST_PER_IMAGE,3)}
            for (d,u),c in sorted(by_day_user.items(),reverse=True)]
    return render_template("stats.html", rows=rows, total_count=total_count,
        total_success=total_success, total_cost=round(total_count*COST_PER_IMAGE,2),
        cost_per_image=COST_PER_IMAGE)

# ── API Buffer ──────────────────────────────────────────────────────────────
@app.route("/api/buffer")
def api_buffer():
    buf = get_buffer()
    pending = len(buf.get("images_b64", []))
    return jsonify({"pending": pending, "needed": max(0, TIKTOK_SIZE - pending)})

@app.route("/api/buffer/clear", methods=["POST"])
def api_buffer_clear():
    _save_buffer({"images_b64": [], "flockages": [], "user": None})
    return jsonify({"success": True})

# ── API Comptes ─────────────────────────────────────────────────────────────
@app.route("/api/accounts")
def api_get_accounts():
    data = get_accounts()
    data["available"] = list(ROBINREACH_ACCOUNTS.keys())
    return jsonify(data)

@app.route("/api/accounts", methods=["POST"])
def api_save_accounts():
    data = request.json
    save_accounts({"main": data.get("main",""), "others": data.get("others",[])})
    return jsonify({"success": True})

# ── API Queue ───────────────────────────────────────────────────────────────
@app.route("/api/queue")
def api_queue():
    return jsonify({"tiktoks": get_queue()})

@app.route("/api/scheduled")
def api_scheduled():
    return jsonify({"tiktoks": get_scheduled()})

@app.route("/api/queue/assign", methods=["POST"])
def api_assign():
    data = request.json; key = data.get("key"); account = data.get("account")
    if not key: return jsonify({"error":"key requis"}),400
    t = r2_get_json(key)
    if not t: return jsonify({"error":"introuvable"}),404
    t["account"] = account
    r2_put_json(key, t)
    return jsonify({"success": True})

@app.route("/api/queue/dispatch", methods=["POST"])
def api_dispatch():
    data = request.json; accounts = data.get("accounts",[])
    if not accounts: return jsonify({"error":"Aucun compte"}),400
    queue = get_queue()
    unassigned = [t for t in queue if not t.get("account")]
    for i,t in enumerate(unassigned):
        acc = accounts[i % len(accounts)]
        t["account"] = acc
        r2_put_json(t["r2_key"], {**t, "image_urls": None, "r2_key": None, "account": acc})
    return jsonify({"success":True,"count":len(unassigned)})

@app.route("/api/queue/schedule", methods=["POST"])
def api_schedule():
    queue = get_queue()
    assigned = [t for t in queue if t.get("account")]
    if not assigned: return jsonify({"error":"Aucun TikTok avec compte"}),400

    now = datetime.now(timezone.utc)
    scheduled_count = 0; errors = []

    # Grouper par compte
    by_account = {}
    for t in assigned:
        by_account.setdefault(t["account"],[]).append(t)

    for account, tiktoks in by_account.items():
        slot_date = now.date()
        slot_index = 0
        for tiktok in tiktoks:
            # Trouver le prochain créneau disponible
            while True:
                h,m = map(int, SCHEDULE_TIMES[slot_index % len(SCHEDULE_TIMES)].split(":"))
                slot_dt = datetime(slot_date.year,slot_date.month,slot_date.day,h,m,tzinfo=timezone.utc)
                if slot_dt > now + timedelta(minutes=30): break
                slot_index += 1
                if slot_index % len(SCHEDULE_TIMES) == 0:
                    slot_date += timedelta(days=1)

            dt_str = slot_dt.isoformat()

            # Convertir le nom de compte en ID numérique RobinReach
            robinreach_id = ROBINREACH_ACCOUNTS.get(account)

            # Appel RobinReach si configuré
            if ROBINREACH_API_KEY and ROBINREACH_BRAND_ID and robinreach_id:
                try:
                    image_urls = [u for u in tiktok.get("image_urls",[]) if u]
                    resp = requests.post(
                        f"https://robinreach.com/api/v1/posts?api_key={ROBINREACH_API_KEY}&brand_id={ROBINREACH_BRAND_ID}",
                        headers={"Accept":"application/json","Content-Type":"application/json"},
                        json={"content":FIXED_CAPTION,"media_urls":image_urls,"publish_time":dt_str,
                              "social_profile_ids":[robinreach_id],"title":FIXED_CAPTION[:50]},
                        timeout=30)
                    if resp.status_code not in (200,201):
                        errors.append(f"{tiktok['id']}: {resp.text[:150]}"); continue
                except Exception as e:
                    errors.append(f"{tiktok['id']}: {e}"); continue
            elif not robinreach_id:
                errors.append(f"{tiktok['id']}: compte '{account}' non reconnu"); continue

            move_to_scheduled(tiktok["r2_key"], account, dt_str)
            scheduled_count += 1
            slot_index += 1
            if slot_index % len(SCHEDULE_TIMES) == 0:
                slot_date += timedelta(days=1)

    return jsonify({"success":True,"scheduled":scheduled_count,"errors":errors})

@app.route("/api/robinreach/profiles")
def api_robinreach_profiles():
    """Diagnostic : liste les profils sociaux connectés sur RobinReach avec leurs IDs"""
    if not ROBINREACH_API_KEY or not ROBINREACH_BRAND_ID:
        return jsonify({"error": "ROBINREACH_API_KEY ou ROBINREACH_BRAND_ID manquant dans les variables Railway"}), 400
    try:
        resp = requests.get(
            f"https://robinreach.com/api/v1/social_profiles?api_key={ROBINREACH_API_KEY}&brand_id={ROBINREACH_BRAND_ID}",
            headers={"Accept": "application/json"},
            timeout=30
        )
        return jsonify({"status_code": resp.status_code, "raw_response": resp.text[:2000]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/queue/delete", methods=["POST"])
def api_delete():
    data = request.json; key = data.get("key")
    if not key: return jsonify({"error":"key requis"}),400
    t = r2_get_json(key)
    if t:
        for k in t.get("image_keys",[]): r2_delete(k)
    r2_delete(key)
    return jsonify({"success":True})

# ── Generate single ─────────────────────────────────────────────────────────
@app.route("/generate_single", methods=["POST"])
def generate_single():
    if not API_KEY: return jsonify({"error":"Clé API manquante"}),500
    f = request.files.get("image")
    user = request.form.get("user","").strip()
    name = request.form.get("name","").strip()
    number = request.form.get("number","").strip()
    name_below = request.form.get("name_below","").strip() or None
    if not f: return jsonify({"error":"Aucune image"}),400
    result = call_gemini(f.read(), f.mimetype or "image/png", name, number, name_below)
    log_generation(user, result["success"])
    if result["success"]:
        # Ajouter au buffer
        floc = f"{name}/{number}/{name_below or ''}"
        add_to_buffer_and_create_tiktoks([result["image"]], [floc], user)
        return jsonify({"image": result["image"]})
    return jsonify({"error": result["error"]}), 500

# ── Generate bulk ───────────────────────────────────────────────────────────
@app.route("/generate_bulk", methods=["POST"])
def generate_bulk():
    if not API_KEY: return jsonify({"error":"Clé API manquante"}),500

    files = request.files.getlist("images")
    flockages_raw = request.form.get("flockages","")
    user = request.form.get("user","").strip()
    session_id = request.form.get("session_id", str(uuid.uuid4()))
    parallel = min(int(request.form.get("parallel",5)), 10)
    auto_queue = request.form.get("auto_queue","true").lower() == "true"

    lines = [l.strip() for l in flockages_raw.splitlines() if l.strip()]
    if not files: return jsonify({"error":"Aucune image"}),400
    if not lines: return jsonify({"error":"Aucun flocage"}),400
    if len(files) != len(lines): return jsonify({"error":f"{len(files)} images / {len(lines)} flocages"}),400

    items = []
    for f,line in zip(files, lines):
        p = line.split("/") if "/" in line else (line.split(",") if "," in line else [line])
        items.append({
            "bytes": f.read(), "mime": f.mimetype or "image/png",
            "name": p[0].strip() if p else "",
            "number": p[1].strip() if len(p)>1 else "",
            "name_below": p[2].strip() if len(p)>2 else None,
        })

    session_start = datetime.now(timezone.utc).isoformat()
    results_map = {}

    def process(idx, item):
        res = call_gemini(item["bytes"], item["mime"], item["name"], item["number"], item["name_below"])
        log_generation(user, res["success"])
        payload = {"index":idx,"total":len(items),"name":item["name"],"number":item["number"]}
        if res["success"]: payload["image"] = res["image"]
        else: payload["error"] = res["error"]
        results_map[idx] = payload

    new_b64 = []; new_floc = []

    def stream():
        sent = set(); nts = 0
        with ThreadPoolExecutor(max_workers=parallel) as ex:
            futures = {ex.submit(process,i,it): i for i,it in enumerate(items)}
            while len(sent) < len(items):
                for fut in list(futures):
                    if fut.done() and futures[fut] not in sent:
                        sent.add(futures[fut])
                while nts in results_map:
                    d = results_map[nts]
                    yield json.dumps(d)+"\n"
                    if auto_queue and d.get("image"):
                        it = items[nts]
                        new_b64.append(d["image"])
                        new_floc.append(f"{it['name']}/{it['number']}/{it.get('name_below','') or ''}")
                    nts += 1
                if len(sent) < len(items): time.sleep(0.05)

        while nts < len(items):
            if nts in results_map:
                d = results_map[nts]
                yield json.dumps(d)+"\n"
                if auto_queue and d.get("image"):
                    it = items[nts]
                    new_b64.append(d["image"])
                    new_floc.append(f"{it['name']}/{it['number']}/{it.get('name_below','') or ''}")
                nts += 1
            else: time.sleep(0.05)

        # Ajouter au buffer et créer TikToks directement
        if auto_queue and new_b64:
            try:
                print(f"[BUFFER] Saving {len(new_b64)} images...")
                created, remaining = add_to_buffer_and_create_tiktoks(new_b64, new_floc, user)
                print(f"[BUFFER] Done — {len(created)} TikToks created, {remaining} pending")
                for n in created:
                    yield json.dumps({"tiktok_created": n}) + "\n"
                if remaining > 0:
                    yield json.dumps({"buffer_update": True, "pending": remaining, "needed": TIKTOK_SIZE - remaining}) + "\n"
            except Exception as e:
                print(f"[BUFFER ERROR] {e}")
                import traceback; traceback.print_exc()

        success_count = sum(1 for r in results_map.values() if "image" in r)
        r2_put_json(f"sessions/{session_id}.json", {
            "id":session_id,"user":user,
            "start":session_start,"end":datetime.now(timezone.utc).isoformat(),
            "total":len(items),"success":success_count
        })

    return Response(stream(), mimetype="application/x-ndjson")

# ── API Templates ───────────────────────────────────────────────────────────
@app.route("/api/templates")
def api_templates():
    r2 = get_r2()
    if not r2: return jsonify({"templates":[],"error":"R2 non configuré"})
    keys = r2_list_keys(PFX_TEMPLATES, suffix=(".png",".jpg",".jpeg",".webp"))
    # r2_list_keys filtre .json, on refait manuellement
    r2 = get_r2()
    try:
        all_keys = []
        kwargs = {"Bucket":R2_BUCKET,"Prefix":PFX_TEMPLATES}
        while True:
            resp = r2.list_objects_v2(**kwargs)
            for obj in resp.get("Contents",[]):
                k = obj["Key"]
                if k.lower().endswith((".png",".jpg",".jpeg",".webp")):
                    all_keys.append({"key":k,"name":k.replace(PFX_TEMPLATES,"").rsplit(".",1)[0],
                        "url":r2_presigned(k),"size":obj["Size"]})
            if not resp.get("IsTruncated"): break
            kwargs["ContinuationToken"] = resp["NextContinuationToken"]
        return jsonify({"templates":all_keys})
    except Exception as e:
        return jsonify({"templates":[],"error":str(e)})

@app.route("/api/templates/upload", methods=["POST"])
def api_templates_upload():
    r2 = get_r2()
    if not r2: return jsonify({"error":"R2 non configuré"}),500
    files = request.files.getlist("files")
    uploaded = []
    for f in files:
        key = f"{PFX_TEMPLATES}{f.filename}"
        r2.upload_fileobj(f, R2_BUCKET, key, ExtraArgs={"ContentType":f.mimetype or "image/png"})
        uploaded.append(key)
    return jsonify({"uploaded":uploaded})

@app.route("/api/templates/delete", methods=["POST"])
def api_templates_delete():
    key = (request.json or {}).get("key")
    if not key: return jsonify({"error":"key requis"}),400
    r2_delete(key)
    return jsonify({"deleted":key})

@app.route("/api/template_image")
def api_template_image():
    key = request.args.get("key")
    if not key: return jsonify({"error":"key requis"}),400
    r2 = get_r2()
    if not r2: return jsonify({"error":"R2 non configuré"}),500
    try:
        obj = r2.get_object(Bucket=R2_BUCKET, Key=key)
        return jsonify({"image":base64.b64encode(obj["Body"].read()).decode(),"mime":obj.get("ContentType","image/png")})
    except Exception as e:
        return jsonify({"error":str(e)}),500

if __name__ == "__main__":
    port = int(os.environ.get("PORT",5000))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)

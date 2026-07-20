import os
import base64
import json
import time
import uuid
import threading
import requests
import boto3

REPLICATE_API_KEY = os.environ.get("REPLICATE_API_KEY")
print(f"[DEBUG] REPLICATE_API_KEY: {'OK' if REPLICATE_API_KEY else 'MANQUANTE'}")
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, render_template, request, Response, jsonify
from botocore.config import Config

app = Flask(__name__)

# ── Config ─────────────────────────────────────────────────────────────────
API_KEY        = os.environ.get("GEMINI_API_KEY")
MODEL_URL      = "https://generativelanguage.googleapis.com/v1beta/models/gemini-3-pro-image-preview:generateContent"
COST_PER_IMAGE = 0.134
TIKTOK_SIZE    = 7
FIXED_CAPTION  = "3 Maillot Acheté 1 Offert 🎁 #volakits #ete #foot"
SCHEDULE_TIMES = ["10:30", "14:00", "17:30", "19:00"]

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

def move_to_scheduled(queue_key, account, dt_str, robinreach_post_id=None):
    data = r2_get_json(queue_key)
    if not data: return False
    data["status"] = "scheduled"
    data["account"] = account
    data["scheduled_at"] = dt_str
    data["robinreach_post_id"] = robinreach_post_id
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

def call_gemini(img_bytes, mime, name, number, name_below=None, max_retries=2, resolution="1k"):
    img_b64 = base64.b64encode(img_bytes).decode()
    prompt = build_prompt(name, number, name_below)
    payload = {"contents": [{"parts": [
        {"text": prompt},
        {"inline_data": {"mime_type": mime, "data": img_b64}}
    ]}]}
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
                    img = part["inlineData"]["data"]
                    if REPLICATE_API_KEY:
                        try:
                            print(f"[UPSCALE] Démarrage upscaling...")
                            r = requests.post(
                                "https://api.replicate.com/v1/models/nightmareai/real-esrgan/predictions",
                                headers={"Authorization": f"Bearer {REPLICATE_API_KEY}", "Content-Type": "application/json", "Prefer": "wait"},
                                json={"input": {"image": f"data:image/png;base64,{img}", "scale": 2, "face_enhance": False}},
                                timeout=120
                            )
                            print(f"[UPSCALE] Réponse: {r.status_code} — {r.text[:200]}")
                            if r.status_code in (200, 201):
                                data_r = r.json()
                                output = data_r.get("output")
                                if not output:
                                    # polling si pas encore prêt
                                    pid = data_r.get("id")
                                    for _ in range(30):
                                        time.sleep(2)
                                        p = requests.get(f"https://api.replicate.com/v1/predictions/{pid}", headers={"Authorization": f"Bearer {REPLICATE_API_KEY}"}, timeout=15).json()
                                        if p.get("status") == "succeeded" and p.get("output"):
                                            output = p["output"]
                                            break
                                        elif p.get("status") in ("failed", "canceled"):
                                            break
                                if output:
                                    img = base64.b64encode(requests.get(output, timeout=30).content).decode()
                                    print("[UPSCALE] ✅ 2K")
                        except Exception as e:
                            print(f"[UPSCALE] Erreur: {e}")
                    return {"success": True, "image": img}
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
    data = request.json or {}
    start_date_str = data.get("start_date")
    custom_slots = data.get("custom_slots", {})
    single_key = data.get("single_key")  # programmer un seul TikTok

    queue = get_queue()
    if single_key:
        assigned = [t for t in queue if t.get("account") and t["r2_key"] == single_key]
    else:
        assigned = [t for t in queue if t.get("account")]
    if not assigned: return jsonify({"error":"Aucun TikTok avec compte assigné"}),400

    now = datetime.now(timezone.utc)
    from zoneinfo import ZoneInfo
    paris_tz = ZoneInfo("Europe/Paris")

    if start_date_str:
        try:
            y,mo,d = map(int, start_date_str.split("-"))
            from datetime import date as date_cls
            start_date = date_cls(y,mo,d)
        except Exception:
            start_date = now.date()
    else:
        start_date = now.date()

    scheduled_count = 0
    errors = []
    scheduled_details = []

    by_account = {}
    for t in assigned:
        by_account.setdefault(t["account"],[]).append(t)
    for acc in by_account:
        by_account[acc].sort(key=lambda x: x.get("number",0))

    for account, tiktoks in by_account.items():
        robinreach_id = ROBINREACH_ACCOUNTS.get(account)
        if not robinreach_id:
            for t in tiktoks:
                errors.append(f"Compte '{account}' non reconnu (TikTok {t.get('number','')})")
            continue

        slot_date = start_date
        slot_index = 0

        for tiktok in tiktoks:
            tiktok_data = r2_get_json(tiktok["r2_key"])
            if not tiktok_data:
                errors.append(f"TikTok {tiktok.get('number','')} introuvable")
                continue
            if tiktok_data.get("status") == "scheduled":
                errors.append(f"TikTok {tiktok.get('number','')} déjà programmé, ignoré")
                continue

            tiktok_data["status"] = "sending"
            r2_put_json(tiktok["r2_key"], tiktok_data)

            # Créneau personnalisé ?
            custom = custom_slots.get(tiktok["r2_key"])
            use_custom = False
            if custom:
                try:
                    if "T" in custom:
                        naive = datetime.fromisoformat(custom)
                        slot_dt = naive.replace(tzinfo=paris_tz).astimezone(timezone.utc)
                    else:
                        h, m = map(int, custom.split(":"))
                        slot_dt = datetime(slot_date.year,slot_date.month,slot_date.day,h,m,tzinfo=paris_tz).astimezone(timezone.utc)
                    use_custom = True
                except Exception:
                    use_custom = False

            if not use_custom:
                while True:
                    h,m = map(int, SCHEDULE_TIMES[slot_index % len(SCHEDULE_TIMES)].split(":"))
                    slot_dt = datetime(slot_date.year,slot_date.month,slot_date.day,h,m,tzinfo=timezone.utc)
                    if start_date == now.date():
                        if slot_dt > now + timedelta(minutes=30): break
                    else:
                        break
                    slot_index += 1
                    if slot_index % len(SCHEDULE_TIMES) == 0:
                        slot_date += timedelta(days=1)

            dt_str = slot_dt.isoformat()
            paris_dt = slot_dt.astimezone(paris_tz)
            display_time = paris_dt.strftime("%d/%m/%Y à %Hh%M")

            if ROBINREACH_API_KEY and ROBINREACH_BRAND_ID:
                try:
                    image_urls = [u for u in tiktok.get("image_urls",[]) if u]
                    # Format correct de l'API RobinReach
                    paris_local = slot_dt.astimezone(paris_tz)
                    image_urls = [u for u in tiktok.get("image_urls",[]) if u]
                    payload = {
                        "content": FIXED_CAPTION,
                        "media_urls": image_urls,
                        "social_profile_ids": [robinreach_id],
                        "publish_time": dt_str,
                        "status": "scheduled",
                        "timezone": "UTC",
                        "platform_options": {
                            "tiktok": {
                                "add_music": True
                            }
                        }
                    }
                    print(f"[ROBINREACH] Sending payload: {json.dumps(payload)[:500]}")
                    resp = requests.post(
                        f"https://robinreach.com/api/v1/posts?api_key={ROBINREACH_API_KEY}&brand_id={ROBINREACH_BRAND_ID}",
                        headers={"Accept": "application/json", "Content-Type": "application/json"},
                        json=payload,
                        timeout=30
                    )
                    print(f"[ROBINREACH] Response {resp.status_code}: {resp.text[:500]}")
                    if resp.status_code not in (200,201):
                        tiktok_data["status"] = "pending"
                        r2_put_json(tiktok["r2_key"], tiktok_data)
                        errors.append(f"TikTok {tiktok.get('number','')}: {resp.text[:200]}")
                        continue
                    # Sauvegarder l'ID du post RobinReach pour pouvoir le supprimer plus tard
                    try:
                        resp_data = resp.json()
                        robinreach_post_id = resp_data.get("id") or resp_data.get("post_id") or resp_data.get("data",{}).get("id")
                        tiktok_data["robinreach_post_id"] = robinreach_post_id
                        print(f"[ROBINREACH] Post ID: {robinreach_post_id}")
                    except Exception:
                        pass
                except Exception as e:
                    tiktok_data["status"] = "pending"
                    r2_put_json(tiktok["r2_key"], tiktok_data)
                    errors.append(f"TikTok {tiktok.get('number','')}: {str(e)}")
                    continue

            move_to_scheduled(tiktok["r2_key"], account, dt_str, tiktok_data.get("robinreach_post_id"))
            scheduled_count += 1
            scheduled_details.append({
                "tiktok": tiktok.get("number",""),
                "account": account,
                "time": display_time
            })

            if not use_custom:
                slot_index += 1
                if slot_index % len(SCHEDULE_TIMES) == 0:
                    slot_date += timedelta(days=1)

    return jsonify({
        "success": True,
        "scheduled": scheduled_count,
        "details": scheduled_details,
        "errors": errors
    })

@app.route("/api/queue/reorder", methods=["POST"])
def api_reorder_images():
    """Réordonne les images d'un TikTok"""
    data = request.json
    key = data.get("key")
    new_order = data.get("order", [])  # liste d'indices dans le nouvel ordre
    if not key: return jsonify({"error": "key requis"}), 400
    tiktok = r2_get_json(key)
    if not tiktok: return jsonify({"error": "TikTok introuvable"}), 404
    img_keys = tiktok.get("image_keys", [])
    flockages = tiktok.get("flockages", [])
    if len(new_order) != len(img_keys):
        return jsonify({"error": "Ordre invalide"}), 400
    try:
        tiktok["image_keys"] = [img_keys[i] for i in new_order]
        tiktok["flockages"] = [flockages[i] if i < len(flockages) else "" for i in new_order]
        r2_put_json(key, tiktok)
        return jsonify({"success": True})
    except (IndexError, TypeError) as e:
        return jsonify({"error": str(e)}), 400

@app.route("/api/queue/delete_image", methods=["POST"])
def api_delete_image():
    """Supprime une image d'un TikTok"""
    data = request.json
    key = data.get("key")
    img_index = data.get("index")
    if not key or img_index is None: return jsonify({"error": "key et index requis"}), 400
    tiktok = r2_get_json(key)
    if not tiktok: return jsonify({"error": "TikTok introuvable"}), 404
    img_keys = tiktok.get("image_keys", [])
    flockages = tiktok.get("flockages", [])
    if img_index < 0 or img_index >= len(img_keys):
        return jsonify({"error": "Index invalide"}), 400
    # Supprimer l'image de R2
    r2_delete(img_keys[img_index])
    tiktok["image_keys"] = [k for i,k in enumerate(img_keys) if i != img_index]
    tiktok["flockages"] = [f for i,f in enumerate(flockages) if i != img_index]
    r2_put_json(key, tiktok)
    return jsonify({"success": True, "remaining": len(tiktok["image_keys"])})

@app.route("/api/scheduled/unschedule", methods=["POST"])
def api_unschedule():
    """Remet des TikToks programmés dans la file d'attente ET supprime de RobinReach"""
    data = request.json
    keys = data.get("keys", [])
    if not keys: return jsonify({"error": "keys requis"}), 400
    count = 0
    robinreach_errors = []
    for sched_key in keys:
        tiktok = r2_get_json(sched_key)
        if not tiktok: continue

        # Supprimer le post sur RobinReach si on a son ID
        robinreach_post_id = tiktok.get("robinreach_post_id")
        if robinreach_post_id and ROBINREACH_API_KEY and ROBINREACH_BRAND_ID:
            try:
                del_resp = requests.delete(
                    f"https://robinreach.com/api/v1/posts/{robinreach_post_id}?api_key={ROBINREACH_API_KEY}&brand_id={ROBINREACH_BRAND_ID}",
                    headers={"Accept": "application/json"},
                    timeout=30
                )
                print(f"[ROBINREACH DELETE] Post {robinreach_post_id}: {del_resp.status_code}")
                if del_resp.status_code not in (200, 204):
                    robinreach_errors.append(f"Post {robinreach_post_id}: {del_resp.text[:100]}")
            except Exception as e:
                robinreach_errors.append(f"Post {robinreach_post_id}: {str(e)}")

        # Déplacer les images vers queue/imgs/
        new_img_keys = []
        r2 = get_r2()
        for old_k in tiktok.get("image_keys", []):
            new_k = old_k.replace("scheduled/imgs/", "queue/imgs/")
            if r2 and old_k != new_k:
                try:
                    r2.copy_object(Bucket=R2_BUCKET,
                        CopySource={"Bucket": R2_BUCKET, "Key": old_k}, Key=new_k)
                    r2_delete(old_k)
                    new_img_keys.append(new_k)
                except Exception:
                    new_img_keys.append(old_k)
            else:
                new_img_keys.append(old_k)

        tiktok["image_keys"] = new_img_keys
        tiktok["status"] = "pending"
        tiktok["account"] = None
        tiktok["scheduled_at"] = None
        tiktok["robinreach_post_id"] = None
        queue_key = sched_key.replace(PFX_SCHEDULED, PFX_QUEUE)
        r2_put_json(queue_key, tiktok)
        r2_delete(sched_key)
        count += 1
    return jsonify({"success": True, "count": count, "robinreach_errors": robinreach_errors})

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
@app.route("/remove_box")
def remove_box_page():
    return render_template("remove_box.html")

@app.route("/api/remove_box", methods=["POST"])
def api_remove_box():
    if not API_KEY:
        return jsonify({"error": "Clé API manquante"}), 500
    f = request.files.get("image")
    if not f:
        return jsonify({"error": "Aucune image"}), 400

    img_bytes = f.read()
    img_b64 = base64.b64encode(img_bytes).decode()
    mime = f.mimetype or "image/png"

    prompt = (
        "Edit this image of a sports jersey. "
        "There is a gift box / packaging box visible in the image (it may have a logo, ribbon, or brand name on it). "
        "Remove the gift box completely from the image. "
        "Replace the area where the box was with the background that would naturally be there — "
        "match the floor, wall, or surface texture and color from the surrounding area. "
        "Keep everything else exactly the same: the jersey, the hanger/hook, the background, lighting, shadows. "
        "The result should look like the jersey was always photographed without any box."
    )

    payload = {
        "contents": [{"parts": [
            {"text": prompt},
            {"inline_data": {"mime_type": mime, "data": img_b64}}
        ]}]
    }

    try:
        resp = requests.post(
            MODEL_URL,
            headers={"x-goog-api-key": API_KEY, "Content-Type": "application/json"},
            json=payload,
            timeout=120
        )
        if resp.status_code != 200:
            return jsonify({"error": f"API {resp.status_code}: {resp.text[:200]}"}), 500
        data = resp.json()
        for part in data["candidates"][0]["content"]["parts"]:
            if "inlineData" in part:
                return jsonify({"image": part["inlineData"]["data"]})
        return jsonify({"error": "Pas d'image dans la réponse"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/generate_single", methods=["POST"])
def generate_single():
    if not API_KEY: return jsonify({"error":"Clé API manquante"}),500
    f = request.files.get("image")
    user = request.form.get("user","").strip()
    name = request.form.get("name","").strip()
    number = request.form.get("number","").strip()
    name_below = request.form.get("name_below","").strip() or None
    resolution = request.form.get("resolution", "1k").strip()
    if not f: return jsonify({"error":"Aucune image"}),400
    result = call_gemini(f.read(), f.mimetype or "image/png", name, number, name_below, resolution=resolution)
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
    resolution = request.form.get("resolution", "1k").strip()

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
        res = call_gemini(item["bytes"], item["mime"], item["name"], item["number"], item["name_below"], resolution=resolution)
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

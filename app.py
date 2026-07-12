import os
import base64
import json
import time
import uuid
import requests
import boto3
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, render_template, request, Response, jsonify
from botocore.config import Config
from io import BytesIO

app = Flask(__name__)

# ── Config ─────────────────────────────────────────────────────────────────
API_KEY = os.environ.get("GEMINI_API_KEY")
MODEL_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-3.1-flash-image:generateContent"
COST_PER_IMAGE = 0.067
LOG_FILE = "/tmp/generation_log.jsonl"
SESSION_FILE = "/tmp/sessions.jsonl"
TIKTOK_SIZE = 7  # images par TikTok
FIXED_CAPTION = "3 Maillot Acheté 1 Offert 🎁 #volakits #ete #foot"
SCHEDULE_TIMES = ["12:30", "16:00", "19:30", "21:00"]  # créneaux fixes

# ── Cloudflare R2 ──────────────────────────────────────────────────────────
R2_ENDPOINT = os.environ.get("R2_ENDPOINT")
R2_ACCESS_KEY = os.environ.get("R2_ACCESS_KEY_ID")
R2_SECRET_KEY = os.environ.get("R2_SECRET_ACCESS_KEY")
R2_BUCKET = os.environ.get("R2_BUCKET", "jersey-templates")
R2_QUEUE_PREFIX = "queue/"
R2_SCHEDULED_PREFIX = "scheduled/"
R2_TEMPLATES_PREFIX = "templates/"

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
        r2.put_object(
            Bucket=R2_BUCKET, Key=key,
            Body=json.dumps(data, ensure_ascii=False).encode(),
            ContentType="application/json"
        )
        return True
    except Exception as e:
        print(f"R2 put error: {e}")
        return False

def r2_get_json(key):
    r2 = get_r2()
    if not r2: return None
    try:
        obj = r2.get_object(Bucket=R2_BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode())
    except Exception:
        return None

def r2_list_keys(prefix):
    r2 = get_r2()
    if not r2: return []
    try:
        resp = r2.list_objects_v2(Bucket=R2_BUCKET, Prefix=prefix)
        return [o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".json")]
    except Exception:
        return []

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
        print(f"R2 image put error: {e}")
        return False

def r2_get_presigned(key, expires=3600):
    r2 = get_r2()
    if not r2: return None
    try:
        return r2.generate_presigned_url("get_object",
            Params={"Bucket": R2_BUCKET, "Key": key}, ExpiresIn=expires)
    except Exception:
        return None

# ── Logging ────────────────────────────────────────────────────────────────
def log_generation(user, success):
    entry = {"timestamp": datetime.now(timezone.utc).isoformat(), "user": user or "Inconnu", "success": success}
    try:
        with open(LOG_FILE, "a") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception:
        pass

def read_logs():
    if not os.path.exists(LOG_FILE): return []
    entries = []
    with open(LOG_FILE) as f:
        for line in f:
            line = line.strip()
            if not line: continue
            try: entries.append(json.loads(line))
            except: continue
    return entries

def save_session(session):
    try:
        with open(SESSION_FILE, "a") as f:
            f.write(json.dumps(session) + "\n")
    except: pass

def read_sessions():
    if not os.path.exists(SESSION_FILE): return []
    sessions = []
    with open(SESSION_FILE) as f:
        for line in f:
            line = line.strip()
            if not line: continue
            try: sessions.append(json.loads(line))
            except: continue
    return sessions

# ── Queue TikTok ───────────────────────────────────────────────────────────
def get_next_tiktok_number():
    """Calcule le prochain numéro de TikTok dans la queue + scheduled"""
    queue_keys = r2_list_keys(R2_QUEUE_PREFIX)
    scheduled_keys = r2_list_keys(R2_SCHEDULED_PREFIX)
    all_keys = queue_keys + scheduled_keys
    if not all_keys:
        return 1
    numbers = []
    for k in all_keys:
        try:
            # format: queue/tiktok_042.json
            n = int(k.split("/")[-1].replace("tiktok_", "").replace(".json", ""))
            numbers.append(n)
        except:
            continue
    return max(numbers) + 1 if numbers else 1

def save_tiktok_to_queue(tiktok_num, images_b64, user, flockages):
    """Sauvegarde un TikTok (7 images) dans la queue R2"""
    r2 = get_r2()
    if not r2: return False

    # Sauvegarder les images
    image_keys = []
    for i, img_b64 in enumerate(images_b64):
        img_key = f"queue/images/tiktok_{tiktok_num:03d}_img_{i+1:02d}.png"
        img_bytes = base64.b64decode(img_b64)
        r2_put_image(img_key, img_bytes)
        image_keys.append(img_key)

    # Sauvegarder les métadonnées
    meta = {
        "id": f"tiktok_{tiktok_num:03d}",
        "number": tiktok_num,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "user": user,
        "image_keys": image_keys,
        "flockages": flockages,
        "status": "pending",
        "account": None,
        "scheduled_at": None,
    }
    key = f"{R2_QUEUE_PREFIX}tiktok_{tiktok_num:03d}.json"
    return r2_put_json(key, meta)

def get_queue():
    """Récupère tous les TikToks en attente"""
    keys = r2_list_keys(R2_QUEUE_PREFIX)
    tiktoks = []
    for key in sorted(keys):
        data = r2_get_json(key)
        if data:
            # Générer URLs signées pour les images
            data["image_urls"] = [r2_get_presigned(k) for k in data.get("image_keys", [])]
            data["r2_key"] = key
            tiktoks.append(data)
    return sorted(tiktoks, key=lambda x: x.get("number", 0))

def get_scheduled():
    """Récupère tous les TikToks programmés"""
    keys = r2_list_keys(R2_SCHEDULED_PREFIX)
    tiktoks = []
    for key in sorted(keys):
        data = r2_get_json(key)
        if data:
            data["image_urls"] = [r2_get_presigned(k) for k in data.get("image_keys", [])]
            data["r2_key"] = key
            tiktoks.append(data)
    return sorted(tiktoks, key=lambda x: x.get("scheduled_at", ""), reverse=True)

def move_to_scheduled(queue_key, account, scheduled_datetime):
    """Déplace un TikTok de queue vers scheduled"""
    data = r2_get_json(queue_key)
    if not data: return False
    data["status"] = "scheduled"
    data["account"] = account
    data["scheduled_at"] = scheduled_datetime
    new_key = queue_key.replace(R2_QUEUE_PREFIX, R2_SCHEDULED_PREFIX)
    r2_put_json(new_key, data)
    r2_delete(queue_key)
    return True

# ── Prompt ─────────────────────────────────────────────────────────────────
def build_prompt(name, number, name_below=None):
    name = name.strip().upper()
    number = number.strip()
    name_below = (name_below or name).strip().upper()
    parts = ["Edit this image of a sports jersey (back view). This is a precise text-replacement task, not a redesign."]
    if name:
        parts.append(f'Replace the main back name text (large curved/straight text near the top) with "{name}". Keep the exact same font, font weight, outline style, color, letter size, and curvature/position as the original text.')
    if number:
        digits_spelled = ", ".join(f'"{d}"' for d in number)
        parts.append(f'Replace the large back number with the {len(number)}-digit number "{number}". This number is made of exactly {len(number)} characters, in this exact order: {digits_spelled}. Render EVERY one of these {len(number)} digits, none missing, none merged, none dropped. Keep the exact same font, color, outline style and centered position as the original number. If "{number}" has a different number of digits than the original, adjust font size proportionally. Do not add any logos, icons or marks inside or around the number.')
    if name_below:
        parts.append(f'There is also a smaller name text printed below the number/badge — replace that text with "{name_below}". Place it directly below the badge with the SAME small vertical gap as in the original image. Keep the same font, size, color and outline style as the original smaller text.')
    parts.append("Keep absolutely everything else identical to the original image: jersey color, fabric texture, pattern, font style, text outline, text color, exact position, exact size, exact spacing between text elements, lighting, shadows, background, tags, and overall composition. Do not regenerate, redesign, resize or reposition anything — only swap the text content itself.")
    return " ".join(parts)

def call_gemini(img_bytes, mime_type, name, number, name_below=None, max_retries=2):
    img_b64 = base64.b64encode(img_bytes).decode()
    prompt = build_prompt(name, number, name_below)
    payload = {"contents": [{"parts": [{"text": prompt}, {"inline_data": {"mime_type": mime_type, "data": img_b64}}]}]}
    last_error = None
    for attempt in range(max_retries + 1):
        try:
            resp = requests.post(MODEL_URL, headers={"x-goog-api-key": API_KEY, "Content-Type": "application/json"}, json=payload, timeout=120)
        except requests.RequestException as e:
            last_error = f"Erreur réseau: {e}"; time.sleep(1); continue
        if resp.status_code != 200:
            last_error = f"Erreur API ({resp.status_code}): {resp.text[:300]}"; time.sleep(1); continue
        data = resp.json()
        try:
            candidates = data.get("candidates", [])
            if not candidates:
                last_error = "Aucune réponse générée."; continue
            for part in candidates[0]["content"]["parts"]:
                if "inlineData" in part:
                    return {"success": True, "image": part["inlineData"]["data"]}
            last_error = "Pas d'image dans la réponse."
        except (KeyError, IndexError) as e:
            last_error = f"Réponse inattendue: {e}"
    return {"success": False, "error": last_error}

# ── Routes ─────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/queue")
def queue_page():
    return render_template("queue.html")

@app.route("/scheduled")
def scheduled_page():
    return render_template("scheduled.html")

@app.route("/stats")
def stats():
    entries = read_logs()
    by_day_user = {}
    total_count = 0; total_success = 0
    for e in entries:
        day = e["timestamp"][:10]; user = e.get("user") or "Inconnu"; key = (day, user)
        by_day_user.setdefault(key, {"total": 0, "success": 0})
        by_day_user[key]["total"] += 1; total_count += 1
        if e.get("success"): by_day_user[key]["success"] += 1; total_success += 1
    rows = [{"day": day, "user": user, "total": counts["total"], "success": counts["success"], "cost": round(counts["total"] * COST_PER_IMAGE, 3)} for (day, user), counts in sorted(by_day_user.items(), reverse=True)]
    return render_template("stats.html", rows=rows, total_count=total_count, total_success=total_success, total_cost=round(total_count * COST_PER_IMAGE, 2), cost_per_image=COST_PER_IMAGE)

@app.route("/dashboard")
def dashboard():
    entries = read_logs()
    today = datetime.now(timezone.utc).date().isoformat()
    today_entries = [e for e in entries if e["timestamp"][:10] == today]
    today_count = len(today_entries); today_success = sum(1 for e in today_entries if e.get("success"))
    tiktoks_done = today_count // 7
    by_user = {}
    for e in today_entries:
        u = e.get("user") or "Inconnu"; by_user.setdefault(u, 0); by_user[u] += 1
    queue = get_queue(); scheduled = get_scheduled()
    return render_template("dashboard.html", today_count=today_count, today_success=today_success,
        today_cost=round(today_count * COST_PER_IMAGE, 2), tiktoks_done=tiktoks_done,
        tiktoks_goal=20, by_user=by_user, recent_sessions=read_sessions()[-10:],
        queue_count=len(queue), scheduled_count=len(scheduled))

@app.route("/templates")
def templates_page():
    return render_template("templates.html")

# ── API Queue ──────────────────────────────────────────────────────────────
@app.route("/api/queue", methods=["GET"])
def api_get_queue():
    return jsonify({"tiktoks": get_queue()})

@app.route("/api/scheduled", methods=["GET"])
def api_get_scheduled():
    return jsonify({"tiktoks": get_scheduled()})

@app.route("/api/queue/assign", methods=["POST"])
def api_assign_account():
    """Assigner un compte TikTok à un TikTok en attente"""
    data = request.json
    key = data.get("key")
    account = data.get("account")
    if not key or not account:
        return jsonify({"error": "key et account requis"}), 400
    tiktok = r2_get_json(key)
    if not tiktok:
        return jsonify({"error": "TikTok introuvable"}), 404
    tiktok["account"] = account
    r2_put_json(key, tiktok)
    return jsonify({"success": True})

@app.route("/api/queue/dispatch", methods=["POST"])
def api_dispatch_auto():
    """Dispatcher automatiquement les TikToks sans compte aux autres comptes"""
    data = request.json
    accounts = data.get("accounts", [])  # liste des comptes (sans le principal)
    if not accounts:
        return jsonify({"error": "Aucun compte fourni"}), 400
    queue = get_queue()
    unassigned = [t for t in queue if not t.get("account")]
    if not unassigned:
        return jsonify({"message": "Aucun TikTok sans compte", "count": 0})
    # Round-robin sur les autres comptes
    for i, tiktok in enumerate(unassigned):
        account = accounts[i % len(accounts)]
        tiktok["account"] = account
        r2_put_json(tiktok["r2_key"], tiktok)
    return jsonify({"success": True, "count": len(unassigned)})

@app.route("/api/queue/schedule", methods=["POST"])
def api_schedule_all():
    """Programmer tous les TikToks avec un compte assigné sur RobinReach"""
    robinreach_api_key = os.environ.get("ROBINREACH_API_KEY")
    robinreach_brand_id = os.environ.get("ROBINREACH_BRAND_ID")

    queue = get_queue()
    assigned = [t for t in queue if t.get("account")]
    if not assigned:
        return jsonify({"error": "Aucun TikTok avec compte assigné"}), 400

    # Calculer les créneaux disponibles à partir de maintenant
    now = datetime.now(timezone.utc)
    scheduled_count = 0
    errors = []

    # Grouper par compte
    by_account = {}
    for t in assigned:
        acc = t["account"]
        by_account.setdefault(acc, []).append(t)

    # Pour chaque compte, programmer aux créneaux disponibles
    for account, tiktoks in by_account.items():
        slot_date = now.date()
        slot_index = 0

        for tiktok in tiktoks:
            # Trouver le prochain créneau disponible
            while True:
                slot_time_str = SCHEDULE_TIMES[slot_index % len(SCHEDULE_TIMES)]
                h, m = map(int, slot_time_str.split(":"))
                slot_dt = datetime(slot_date.year, slot_date.month, slot_date.day, h, m, tzinfo=timezone.utc)
                if slot_dt > now + timedelta(minutes=30):
                    break
                slot_index += 1
                if slot_index % len(SCHEDULE_TIMES) == 0:
                    slot_date += timedelta(days=1)

            scheduled_dt_str = slot_dt.isoformat()

            # Appel API RobinReach (si configuré)
            if robinreach_api_key and robinreach_brand_id:
                try:
                    image_urls = tiktok.get("image_urls", [])
                    payload = {
                        "content": FIXED_CAPTION,
                        "media_urls": [url for url in image_urls if url],
                        "publish_time": scheduled_dt_str,
                        "social_profile_ids": [account],
                        "title": FIXED_CAPTION[:50],
                    }
                    resp = requests.post(
                        f"https://robinreach.com/api/v1/posts?api_key={robinreach_api_key}&brand_id={robinreach_brand_id}",
                        headers={"Accept": "application/json", "Content-Type": "application/json"},
                        json=payload,
                        timeout=30,
                    )
                    if resp.status_code not in (200, 201):
                        errors.append(f"{tiktok['id']}: {resp.text[:100]}")
                        continue
                except Exception as e:
                    errors.append(f"{tiktok['id']}: {e}")
                    continue

            # Déplacer vers scheduled
            move_to_scheduled(tiktok["r2_key"], account, scheduled_dt_str)
            scheduled_count += 1

            # Passer au créneau suivant
            slot_index += 1
            if slot_index % len(SCHEDULE_TIMES) == 0:
                slot_date += timedelta(days=1)

    return jsonify({"success": True, "scheduled": scheduled_count, "errors": errors})

@app.route("/api/queue/delete", methods=["POST"])
def api_delete_tiktok():
    data = request.json
    key = data.get("key")
    if not key: return jsonify({"error": "key requis"}), 400
    tiktok = r2_get_json(key)
    if tiktok:
        for img_key in tiktok.get("image_keys", []):
            r2_delete(img_key)
    r2_delete(key)
    return jsonify({"success": True})

# ── Generate single ────────────────────────────────────────────────────────
@app.route("/generate_single", methods=["POST"])
def generate_single():
    if not API_KEY: return jsonify({"error": "Clé API manquante"}), 500
    f = request.files.get("image")
    user = request.form.get("user", "").strip()
    name = request.form.get("name", "").strip()
    number = request.form.get("number", "").strip()
    name_below = request.form.get("name_below", "").strip() or None
    if not f: return jsonify({"error": "Aucune image"}), 400
    result = call_gemini(f.read(), f.mimetype or "image/png", name, number, name_below)
    log_generation(user, result["success"])
    if result["success"]: return jsonify({"image": result["image"]})
    return jsonify({"error": result["error"]}), 500

# ── Generate bulk ──────────────────────────────────────────────────────────
@app.route("/generate_bulk", methods=["POST"])
def generate_bulk():
    if not API_KEY: return jsonify({"error": "Clé API manquante"}), 500

    files = request.files.getlist("images")
    flockages_raw = request.form.get("flockages", "")
    user = request.form.get("user", "").strip()
    session_id = request.form.get("session_id", str(uuid.uuid4()))
    parallel = int(request.form.get("parallel", 5))
    auto_queue = request.form.get("auto_queue", "true").lower() == "true"

    lines = [l.strip() for l in flockages_raw.splitlines() if l.strip()]
    if not files: return jsonify({"error": "Aucune image"}), 400
    if not lines: return jsonify({"error": "Aucun flocage"}), 400
    if len(files) != len(lines): return jsonify({"error": f"{len(files)} images mais {len(lines)} flocages"}), 400

    items = []
    for f, line in zip(files, lines):
        parts = line.split("/") if "/" in line else line.split(",") if "," in line else [line]
        items.append({
            "filename": f.filename,
            "bytes": f.read(),
            "mime": f.mimetype or "image/png",
            "name": parts[0].strip() if len(parts) > 0 else "",
            "number": parts[1].strip() if len(parts) > 1 else "",
            "name_below": parts[2].strip() if len(parts) > 2 else None,
        })

    session_start = datetime.now(timezone.utc).isoformat()
    results_map = {}

    def process_item(idx, item):
        result = call_gemini(item["bytes"], item["mime"], item["name"], item["number"], item["name_below"])
        log_generation(user, result["success"])
        payload = {"index": idx, "total": len(items), "filename": item["filename"], "name": item["name"], "number": item["number"]}
        if result["success"]: payload["image"] = result["image"]
        else: payload["error"] = result["error"]
        results_map[idx] = payload
        return idx, payload

    # Buffer pour grouper par TikTok
    generated_images = {}  # idx -> base64

    def stream():
        sent = set()
        next_to_send = 0
        tiktok_buffer = []  # images accumulées pour le TikTok en cours
        tiktok_flockages = []
        next_tiktok_num = [get_next_tiktok_number()]

        with ThreadPoolExecutor(max_workers=parallel) as executor:
            futures = {executor.submit(process_item, idx, item): idx for idx, item in enumerate(items)}

            while len(sent) < len(items):
                for future in list(futures.keys()):
                    if future.done() and futures[future] not in sent:
                        idx = futures[future]
                        sent.add(idx)

                while next_to_send in results_map:
                    data = results_map[next_to_send]
                    yield json.dumps(data) + "\n"

                    # Accumuler pour la queue TikTok
                    if auto_queue and data.get("image"):
                        generated_images[next_to_send] = data["image"]
                        tiktok_buffer.append(data["image"])
                        item = items[next_to_send]
                        tiktok_flockages.append(f"{item['name']}/{item['number']}/{item.get('name_below','')}")

                        # Quand on a 7 images → créer un TikTok dans la queue
                        if len(tiktok_buffer) == TIKTOK_SIZE:
                            save_tiktok_to_queue(next_tiktok_num[0], tiktok_buffer.copy(), user, tiktok_flockages.copy())
                            tiktok_num = next_tiktok_num[0]
                            next_tiktok_num[0] += 1
                            yield json.dumps({"tiktok_created": tiktok_num, "images_count": TIKTOK_SIZE}) + "\n"
                            tiktok_buffer.clear()
                            tiktok_flockages.clear()

                    next_to_send += 1

                if len(sent) < len(items):
                    time.sleep(0.1)

            while next_to_send < len(items):
                if next_to_send in results_map:
                    data = results_map[next_to_send]
                    yield json.dumps(data) + "\n"
                    if auto_queue and data.get("image"):
                        tiktok_buffer.append(data["image"])
                        item = items[next_to_send]
                        tiktok_flockages.append(f"{item['name']}/{item['number']}/{item.get('name_below','')}")
                        if len(tiktok_buffer) == TIKTOK_SIZE:
                            save_tiktok_to_queue(next_tiktok_num[0], tiktok_buffer.copy(), user, tiktok_flockages.copy())
                            tiktok_num = next_tiktok_num[0]
                            next_tiktok_num[0] += 1
                            yield json.dumps({"tiktok_created": tiktok_num, "images_count": TIKTOK_SIZE}) + "\n"
                            tiktok_buffer.clear()
                            tiktok_flockages.clear()
                    next_to_send += 1
                else:
                    time.sleep(0.1)

        # S'il reste des images (moins de 7), les sauvegarder quand même
        if auto_queue and tiktok_buffer:
            save_tiktok_to_queue(next_tiktok_num[0], tiktok_buffer.copy(), user, tiktok_flockages.copy())
            yield json.dumps({"tiktok_created": next_tiktok_num[0], "images_count": len(tiktok_buffer), "partial": True}) + "\n"

        session_end = datetime.now(timezone.utc).isoformat()
        success_count = sum(1 for r in results_map.values() if "image" in r)
        save_session({"id": session_id, "user": user, "start_time": session_start, "end_time": session_end, "total": len(items), "success": success_count, "flockages": flockages_raw})

    return Response(stream(), mimetype="application/x-ndjson")

# ── R2 Templates ───────────────────────────────────────────────────────────
@app.route("/api/templates", methods=["GET"])
def list_templates():
    r2 = get_r2()
    if not r2: return jsonify({"templates": [], "error": "R2 non configuré"})
    try:
        resp = r2.list_objects_v2(Bucket=R2_BUCKET, Prefix=R2_TEMPLATES_PREFIX)
        templates = []
        for obj in resp.get("Contents", []):
            key = obj["Key"]
            if key.lower().endswith((".png", ".jpg", ".jpeg", ".webp")):
                url = r2.generate_presigned_url("get_object", Params={"Bucket": R2_BUCKET, "Key": key}, ExpiresIn=3600)
                templates.append({"key": key, "name": key.replace(R2_TEMPLATES_PREFIX, "").rsplit(".", 1)[0], "url": url, "size": obj["Size"]})
        return jsonify({"templates": templates})
    except Exception as e:
        return jsonify({"templates": [], "error": str(e)})

@app.route("/api/templates/upload", methods=["POST"])
def upload_template():
    r2 = get_r2()
    if not r2: return jsonify({"error": "R2 non configuré"}), 500
    files = request.files.getlist("files")
    if not files: return jsonify({"error": "Aucun fichier"}), 400
    uploaded = []
    for f in files:
        key = f"{R2_TEMPLATES_PREFIX}{f.filename}"
        r2.upload_fileobj(f, R2_BUCKET, key, ExtraArgs={"ContentType": f.mimetype or "image/png"})
        uploaded.append(key)
    return jsonify({"uploaded": uploaded})

@app.route("/api/templates/delete", methods=["POST"])
def delete_template():
    r2 = get_r2()
    if not r2: return jsonify({"error": "R2 non configuré"}), 500
    data = request.json; key = data.get("key")
    if not key: return jsonify({"error": "Clé manquante"}), 400
    r2.delete_object(Bucket=R2_BUCKET, Key=key)
    return jsonify({"deleted": key})

@app.route("/api/template_image", methods=["GET"])
def get_template_image():
    r2 = get_r2()
    if not r2: return jsonify({"error": "R2 non configuré"}), 500
    key = request.args.get("key")
    if not key: return jsonify({"error": "Clé manquante"}), 400
    try:
        obj = r2.get_object(Bucket=R2_BUCKET, Key=key)
        img_bytes = obj["Body"].read()
        b64 = base64.b64encode(img_bytes).decode()
        return jsonify({"image": b64, "mime": obj.get("ContentType", "image/png")})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/sessions")
def api_sessions():
    return jsonify({"sessions": sorted(read_sessions(), key=lambda s: s.get("start_time", ""), reverse=True)})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True, threaded=True)

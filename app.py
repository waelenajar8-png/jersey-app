import os
import base64
import json
import time
import uuid
import asyncio
import requests
import boto3
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, render_template, request, Response, jsonify
from botocore.config import Config

app = Flask(__name__)

API_KEY = os.environ.get("GEMINI_API_KEY")
MODEL_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-3.1-flash-image:generateContent"

COST_PER_IMAGE = 0.067
LOG_FILE = "/tmp/generation_log.jsonl"
SESSION_FILE = "/tmp/sessions.jsonl"

# ── Cloudflare R2 ─────────────────────────────────────────────────────────
R2_ENDPOINT = os.environ.get("R2_ENDPOINT")
R2_ACCESS_KEY = os.environ.get("R2_ACCESS_KEY_ID")
R2_SECRET_KEY = os.environ.get("R2_SECRET_ACCESS_KEY")
R2_BUCKET = os.environ.get("R2_BUCKET", "jersey-templates")

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

# ── Logging ────────────────────────────────────────────────────────────────
def log_generation(user, success):
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user": user or "Inconnu",
        "success": success,
    }
    try:
        with open(LOG_FILE, "a") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception:
        pass

def read_logs():
    if not os.path.exists(LOG_FILE):
        return []
    entries = []
    with open(LOG_FILE) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return entries

# ── Sessions ───────────────────────────────────────────────────────────────
def save_session(session):
    try:
        with open(SESSION_FILE, "a") as f:
            f.write(json.dumps(session) + "\n")
    except Exception:
        pass

def read_sessions():
    if not os.path.exists(SESSION_FILE):
        return []
    sessions = []
    with open(SESSION_FILE) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                sessions.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return sessions

# ── Prompt ─────────────────────────────────────────────────────────────────
def build_prompt(name, number, name_below=None):
    name = name.strip().upper()
    number = number.strip()
    name_below = (name_below or name).strip().upper()
    parts = ["Edit this image of a sports jersey (back view). This is a precise text-replacement task, not a redesign."]
    if name:
        parts.append(
            f'Replace the main back name text (large curved/straight text near the top) with "{name}". '
            f'Keep the exact same font, font weight, outline style, color, letter size, and curvature/position as the original text.'
        )
    if number:
        digits_spelled = ", ".join(f'"{d}"' for d in number)
        parts.append(
            f'Replace the large back number with the {len(number)}-digit number "{number}". '
            f'This number is made of exactly {len(number)} characters, in this exact order: {digits_spelled}. '
            f'Render EVERY one of these {len(number)} digits, none missing, none merged, none dropped. '
            f'Keep the exact same font, color, outline style and centered position as the original number. '
            f'If "{number}" has a different number of digits than the original, adjust font size proportionally. '
            f'Do not add any logos, icons or marks inside or around the number.'
        )
    if name_below:
        parts.append(
            f'There is also a smaller name text printed below the number/badge — '
            f'replace that text with "{name_below}". '
            f'Place it directly below the badge with the SAME small vertical gap as in the original image. '
            f'Keep the same font, size, color and outline style as the original smaller text.'
        )
    parts.append(
        "Keep absolutely everything else identical to the original image: jersey color, fabric texture, "
        "pattern, font style, text outline, text color, exact position, exact size, exact spacing between "
        "text elements, lighting, shadows, background, tags, and overall composition. "
        "Do not regenerate, redesign, resize or reposition anything — only swap the text content itself."
    )
    return " ".join(parts)

# ── Gemini call ────────────────────────────────────────────────────────────
def call_gemini(img_bytes, mime_type, name, number, name_below=None, max_retries=2):
    img_b64 = base64.b64encode(img_bytes).decode()
    prompt = build_prompt(name, number, name_below)
    payload = {
        "contents": [{"parts": [{"text": prompt}, {"inline_data": {"mime_type": mime_type, "data": img_b64}}]}]
    }
    last_error = None
    for attempt in range(max_retries + 1):
        try:
            resp = requests.post(
                MODEL_URL,
                headers={"x-goog-api-key": API_KEY, "Content-Type": "application/json"},
                json=payload,
                timeout=120,
            )
        except requests.RequestException as e:
            last_error = f"Erreur réseau: {e}"
            time.sleep(1)
            continue
        if resp.status_code != 200:
            last_error = f"Erreur API ({resp.status_code}): {resp.text[:300]}"
            time.sleep(1)
            continue
        data = resp.json()
        try:
            candidates = data.get("candidates", [])
            if not candidates:
                last_error = "Aucune réponse générée."
                continue
            for part in candidates[0]["content"]["parts"]:
                if "inlineData" in part:
                    return {"success": True, "image": part["inlineData"]["data"]}
            last_error = "Pas d'image dans la réponse."
        except (KeyError, IndexError) as e:
            last_error = f"Réponse inattendue: {e}"
    return {"success": False, "error": last_error}

# ── Routes principales ─────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/stats")
def stats():
    entries = read_logs()
    by_day_user = {}
    total_count = 0
    total_success = 0
    for e in entries:
        day = e["timestamp"][:10]
        user = e.get("user") or "Inconnu"
        key = (day, user)
        by_day_user.setdefault(key, {"total": 0, "success": 0})
        by_day_user[key]["total"] += 1
        total_count += 1
        if e.get("success"):
            by_day_user[key]["success"] += 1
            total_success += 1
    rows = []
    for (day, user), counts in sorted(by_day_user.items(), reverse=True):
        rows.append({
            "day": day, "user": user,
            "total": counts["total"], "success": counts["success"],
            "cost": round(counts["total"] * COST_PER_IMAGE, 3),
        })
    return render_template("stats.html", rows=rows, total_count=total_count,
        total_success=total_success, total_cost=round(total_count * COST_PER_IMAGE, 2),
        cost_per_image=COST_PER_IMAGE)

@app.route("/dashboard")
def dashboard():
    entries = read_logs()
    today = datetime.now(timezone.utc).date().isoformat()
    today_entries = [e for e in entries if e["timestamp"][:10] == today]
    today_count = len(today_entries)
    today_success = sum(1 for e in today_entries if e.get("success"))
    today_cost = round(today_count * COST_PER_IMAGE, 2)
    tiktoks_done = today_count // 7
    by_user = {}
    for e in today_entries:
        u = e.get("user") or "Inconnu"
        by_user.setdefault(u, 0)
        by_user[u] += 1
    sessions = read_sessions()
    recent_sessions = sorted(sessions, key=lambda s: s.get("start_time", ""), reverse=True)[:10]
    return render_template("dashboard.html",
        today_count=today_count, today_success=today_success,
        today_cost=today_cost, tiktoks_done=tiktoks_done,
        tiktoks_goal=20, by_user=by_user, recent_sessions=recent_sessions)

# ── R2 Templates API ───────────────────────────────────────────────────────
@app.route("/api/templates", methods=["GET"])
def list_templates():
    r2 = get_r2()
    if not r2:
        return jsonify({"templates": [], "error": "R2 non configuré"})
    try:
        resp = r2.list_objects_v2(Bucket=R2_BUCKET)
        templates = []
        for obj in resp.get("Contents", []):
            key = obj["Key"]
            if key.lower().endswith((".png", ".jpg", ".jpeg", ".webp")):
                url = r2.generate_presigned_url("get_object",
                    Params={"Bucket": R2_BUCKET, "Key": key}, ExpiresIn=3600)
                templates.append({"key": key, "name": key.rsplit(".", 1)[0], "url": url, "size": obj["Size"]})
        return jsonify({"templates": templates})
    except Exception as e:
        return jsonify({"templates": [], "error": str(e)})

@app.route("/api/templates/upload", methods=["POST"])
def upload_template():
    r2 = get_r2()
    if not r2:
        return jsonify({"error": "R2 non configuré"}), 500
    files = request.files.getlist("files")
    if not files:
        return jsonify({"error": "Aucun fichier"}), 400
    uploaded = []
    for f in files:
        key = f.filename
        r2.upload_fileobj(f, R2_BUCKET, key, ExtraArgs={"ContentType": f.mimetype or "image/png"})
        uploaded.append(key)
    return jsonify({"uploaded": uploaded})

@app.route("/api/templates/delete", methods=["POST"])
def delete_template():
    r2 = get_r2()
    if not r2:
        return jsonify({"error": "R2 non configuré"}), 500
    data = request.json
    key = data.get("key")
    if not key:
        return jsonify({"error": "Clé manquante"}), 400
    r2.delete_object(Bucket=R2_BUCKET, Key=key)
    return jsonify({"deleted": key})

@app.route("/api/template_image", methods=["GET"])
def get_template_image():
    r2 = get_r2()
    if not r2:
        return jsonify({"error": "R2 non configuré"}), 500
    key = request.args.get("key")
    if not key:
        return jsonify({"error": "Clé manquante"}), 400
    try:
        obj = r2.get_object(Bucket=R2_BUCKET, Key=key)
        img_bytes = obj["Body"].read()
        mime = obj.get("ContentType", "image/png")
        b64 = base64.b64encode(img_bytes).decode()
        return jsonify({"image": b64, "mime": mime})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ── Generate single ────────────────────────────────────────────────────────
@app.route("/generate_single", methods=["POST"])
def generate_single():
    if not API_KEY:
        return jsonify({"error": "Clé API manquante"}), 500
    f = request.files.get("image")
    user = request.form.get("user", "").strip()
    name = request.form.get("name", "").strip()
    number = request.form.get("number", "").strip()
    name_below = request.form.get("name_below", "").strip() or None
    if not f:
        return jsonify({"error": "Aucune image"}), 400
    result = call_gemini(f.read(), f.mimetype or "image/png", name, number, name_below)
    log_generation(user, result["success"])
    if result["success"]:
        return jsonify({"image": result["image"]})
    return jsonify({"error": result["error"]}), 500

# ── Generate bulk (parallèle) ──────────────────────────────────────────────
@app.route("/generate_bulk", methods=["POST"])
def generate_bulk():
    if not API_KEY:
        return jsonify({"error": "Clé API manquante"}), 500

    files = request.files.getlist("images")
    flockages_raw = request.form.get("flockages", "")
    user = request.form.get("user", "").strip()
    session_id = request.form.get("session_id", str(uuid.uuid4()))
    parallel = int(request.form.get("parallel", 5))

    lines = [l.strip() for l in flockages_raw.splitlines() if l.strip()]
    if not files:
        return jsonify({"error": "Aucune image"}), 400
    if not lines:
        return jsonify({"error": "Aucun flocage"}), 400
    if len(files) != len(lines):
        return jsonify({"error": f"{len(files)} images mais {len(lines)} flocages"}), 400

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
        payload = {
            "index": idx, "total": len(items),
            "filename": item["filename"], "name": item["name"], "number": item["number"],
        }
        if result["success"]:
            payload["image"] = result["image"]
        else:
            payload["error"] = result["error"]
        results_map[idx] = payload
        return idx, payload

    def stream():
        sent = set()
        next_to_send = 0

        with ThreadPoolExecutor(max_workers=parallel) as executor:
            futures = {executor.submit(process_item, idx, item): idx for idx, item in enumerate(items)}

            while len(sent) < len(items):
                for future in list(futures.keys()):
                    if future.done() and futures[future] not in sent:
                        idx = futures[future]
                        sent.add(idx)

                while next_to_send in results_map:
                    yield json.dumps(results_map[next_to_send]) + "\n"
                    next_to_send += 1

                if len(sent) < len(items):
                    time.sleep(0.1)

            while next_to_send < len(items):
                if next_to_send in results_map:
                    yield json.dumps(results_map[next_to_send]) + "\n"
                    next_to_send += 1
                else:
                    time.sleep(0.1)

        session_end = datetime.now(timezone.utc).isoformat()
        success_count = sum(1 for r in results_map.values() if "image" in r)
        save_session({
            "id": session_id, "user": user,
            "start_time": session_start, "end_time": session_end,
            "total": len(items), "success": success_count,
            "flockages": flockages_raw,
        })

    return Response(stream(), mimetype="application/x-ndjson")

# ── Session history API ────────────────────────────────────────────────────
@app.route("/api/sessions")
def api_sessions():
    sessions = read_sessions()
    return jsonify({"sessions": sorted(sessions, key=lambda s: s.get("start_time", ""), reverse=True)})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True, threaded=True)

@app.route("/templates")
def templates_page():
    return render_template("templates.html")

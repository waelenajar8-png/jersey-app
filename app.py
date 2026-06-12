import os
import base64
import json
import time
import requests
from datetime import datetime, timezone
from flask import Flask, render_template, request, Response, jsonify

app = Flask(__name__)

API_KEY = os.environ.get("GEMINI_API_KEY")
MODEL_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-image:generateContent"

COST_PER_IMAGE = 0.039
LOG_FILE = "/tmp/generation_log.jsonl"


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


def build_prompt(name, number, name_below=None):
    name = name.strip().upper()
    number = number.strip()
    name_below = (name_below or name).strip().upper()
    parts = [
        "You are performing a precise text-swap edit on a sports jersey photo. "
        "Treat this like editing a vector text layer in design software: only the TEXT CONTENT changes, "
        "every other visual property (font family, font weight/boldness, outline thickness, fill color, "
        "letter height, letter spacing, baseline position, curvature, rotation) must stay IDENTICAL to the original."
    ]
    if name:
        parts.append(
            f'TASK 1: The main name text near the collar currently shows some text. '
            f'Change its content to "{name}" only. Do not change its font weight, size, outline thickness, '
            f'color, curvature or vertical position. If "{name}" has more or fewer letters than the original word, '
            f'adjust letter spacing/width slightly so it spans the same horizontal area as the original word, '
            f'without changing letter height or stroke thickness.'
        )
    if number:
        digits_spelled = ", ".join(f'"{d}"' for d in number)
        parts.append(
            f'TASK 2: The large center number currently shows a number. '
            f'Change its content to "{number}" ({len(number)} digit(s), in order: {digits_spelled}) only. '
            f'Render all {len(number)} digit(s), none missing or merged. '
            f'Do not change its font weight, outline thickness, color, or vertical centering. '
            f'If "{number}" has a different digit count than the original, scale the digit width '
            f'(not the digit height or stroke thickness) so all digits fit within the same bounding box as the original number.'
        )
    if name_below:
        parts.append(
            f'TASK 3: There is a smaller name text below the number/badge area. '
            f'Change its content to "{name_below}" only, with the exact same font weight, size, outline thickness, '
            f'color and vertical gap to the badge above it as the original.'
        )
    parts.append(
        "CRITICAL RULES: "
        "Do not add, remove, or alter any logos, badges, watermarks, icons, sponsor marks, tags, or decorative "
        "elements anywhere in the image — the only changes are the three text contents listed above. "
        "Do not change jersey color, fabric texture, pattern, lighting, shadows, background, or composition. "
        "The output must look like the exact same photograph with only the text characters swapped."
    )
    return " ".join(parts)


def call_gemini(img_bytes, mime_type, name, number, name_below=None, max_retries=2):
    img_b64 = base64.b64encode(img_bytes).decode()
    prompt = build_prompt(name, number, name_below)

    payload = {
        "contents": [
            {
                "parts": [
                    {"text": prompt},
                    {"inline_data": {"mime_type": mime_type, "data": img_b64}}
                ]
            }
        ]
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
                last_error = "Aucune réponse générée (contenu bloqué ?)."
                continue
            parts_out = candidates[0]["content"]["parts"]
            image_b64_out = None
            for part in parts_out:
                if "inlineData" in part:
                    image_b64_out = part["inlineData"]["data"]
            if image_b64_out:
                return {"success": True, "image": image_b64_out}
            last_error = "Pas d'image dans la réponse."
        except (KeyError, IndexError) as e:
            last_error = f"Réponse inattendue: {e}"

    return {"success": False, "error": last_error}


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
        cost = counts["total"] * COST_PER_IMAGE
        rows.append({
            "day": day,
            "user": user,
            "total": counts["total"],
            "success": counts["success"],
            "cost": round(cost, 3),
        })

    total_cost = round(total_count * COST_PER_IMAGE, 2)

    return render_template(
        "stats.html",
        rows=rows,
        total_count=total_count,
        total_success=total_success,
        total_cost=total_cost,
        cost_per_image=COST_PER_IMAGE,
    )


@app.route("/generate_single", methods=["POST"])
def generate_single():
    if not API_KEY:
        return jsonify({"error": "Clé API non configurée sur le serveur (GEMINI_API_KEY manquante)."}), 500

    f = request.files.get("image")
    user = request.form.get("user", "").strip()
    name = request.form.get("name", "").strip()
    number = request.form.get("number", "").strip()
    name_below = request.form.get("name_below", "").strip() or None

    if not f:
        return jsonify({"error": "Aucune image envoyée."}), 400

    img_bytes = f.read()
    mime_type = f.mimetype or "image/png"

    result = call_gemini(img_bytes, mime_type, name, number, name_below)
    log_generation(user, result["success"])

    if result["success"]:
        return jsonify({"image": result["image"]})
    return jsonify({"error": result["error"]}), 500


@app.route("/generate_bulk", methods=["POST"])
def generate_bulk():
    if not API_KEY:
        return jsonify({"error": "Clé API non configurée sur le serveur (GEMINI_API_KEY manquante)."}), 500

    files = request.files.getlist("images")
    flockages_raw = request.form.get("flockages", "")
    user = request.form.get("user", "").strip()

    lines = [l.strip() for l in flockages_raw.splitlines() if l.strip()]

    if not files:
        return jsonify({"error": "Aucune image envoyée."}), 400
    if not lines:
        return jsonify({"error": "Aucun flocage fourni."}), 400
    if len(files) != len(lines):
        return jsonify({
            "error": f"Le nombre d'images ({len(files)}) ne correspond pas "
                     f"au nombre de lignes de flocage ({len(lines)})."
        }), 400

    items = []
    for f, line in zip(files, lines):
        if "/" in line:
            split_parts = line.split("/")
        elif "," in line:
            split_parts = line.split(",")
        else:
            split_parts = [line]

        name = split_parts[0].strip() if len(split_parts) > 0 else ""
        number = split_parts[1].strip() if len(split_parts) > 1 else ""
        name_below = split_parts[2].strip() if len(split_parts) > 2 else None

        items.append({
            "filename": f.filename,
            "bytes": f.read(),
            "mime_type": f.mimetype or "image/png",
            "name": name,
            "number": number,
            "name_below": name_below,
        })

    def stream():
        total = len(items)
        for idx, item in enumerate(items):
            result = call_gemini(item["bytes"], item["mime_type"], item["name"], item["number"], item["name_below"])
            payload = {
                "index": idx,
                "total": total,
                "filename": item["filename"],
                "name": item["name"],
                "number": item["number"],
            }
            if result["success"]:
                payload["image"] = result["image"]
            else:
                payload["error"] = result["error"]
            log_generation(user, result["success"])
            yield json.dumps(payload) + "\n"

    return Response(stream(), mimetype="application/x-ndjson")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True, threaded=True)

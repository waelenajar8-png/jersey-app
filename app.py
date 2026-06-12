import os
import base64
import json
import time
import requests
from flask import Flask, render_template, request, Response, jsonify

app = Flask(__name__)

API_KEY = os.environ.get("GEMINI_API_KEY")
MODEL_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-image:generateContent"


def build_prompt(name, number):
    name = name.strip().upper()
    number = number.strip()
    parts = ["Edit this image of a sports jersey (back view)."]
    if name:
        parts.append(f'Replace the main back name text (large text near the top) with "{name}".')
    if number:
        parts.append(f'Replace the large back number with "{number}".')
    if name:
        parts.append(
            f'There is also a smaller name text printed below the number/badge — '
            f'replace that text with "{name}" as well (same text as the main name above).'
        )
    parts.append(
        "Keep everything else exactly the same: jersey color, fabric texture, "
        "font style, text color, position, size proportions, lighting, shadows, "
        "background, and overall composition must remain identical to the original image. "
        "Do not regenerate the whole image, only modify the specified text elements."
    )
    return " ".join(parts)


def call_gemini(img_bytes, mime_type, name, number, max_retries=2):
    img_b64 = base64.b64encode(img_bytes).decode()
    prompt = build_prompt(name, number)

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


@app.route("/generate_bulk", methods=["POST"])
def generate_bulk():
    if not API_KEY:
        return jsonify({"error": "Clé API non configurée sur le serveur (GEMINI_API_KEY manquante)."}), 500

    files = request.files.getlist("images")
    flockages_raw = request.form.get("flockages", "")

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

    # Pré-lire tout en mémoire (le générateur tourne hors du contexte de requête)
    items = []
    for f, line in zip(files, lines):
        if "/" in line:
            name, number = line.split("/", 1)
        elif "," in line:
            name, number = line.split(",", 1)
        else:
            name, number = line, ""
        items.append({
            "filename": f.filename,
            "bytes": f.read(),
            "mime_type": f.mimetype or "image/png",
            "name": name.strip(),
            "number": number.strip(),
        })

    def stream():
        total = len(items)
        for idx, item in enumerate(items):
            result = call_gemini(item["bytes"], item["mime_type"], item["name"], item["number"])
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
            yield json.dumps(payload) + "\n"

    return Response(stream(), mimetype="application/x-ndjson")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True, threaded=True)

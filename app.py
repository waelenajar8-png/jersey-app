import os
import base64
import json
import time
import uuid
import threading
import requests
import boto3

REPLICATE_API_KEY = os.environ.get("REPLICATE_API_KEY")
GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON")
VERTEX_PROJECT_ID = "gen-lang-client-0163500565"
VERTEX_LOCATION = "us-central1"

def get_vertex_token():
    """Obtenir un token OAuth2 depuis les credentials JSON"""
    import json, time
    import urllib.request
    import urllib.parse
    creds = json.loads(GOOGLE_CREDENTIALS_JSON)
    
    # Créer le JWT
    import base64
    import hmac
    import hashlib
    
    now = int(time.time())
    header = base64.urlsafe_b64encode(json.dumps({"alg":"RS256","typ":"JWT"}).encode()).rstrip(b'=').decode()
    payload = base64.urlsafe_b64encode(json.dumps({
        "iss": creds["client_email"],
        "scope": "https://www.googleapis.com/auth/cloud-platform",
        "aud": "https://oauth2.googleapis.com/token",
        "iat": now,
        "exp": now + 3600
    }).encode()).rstrip(b'=').decode()
    
    # Signer avec la clé privée RSA
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding
    from cryptography.hazmat.backends import default_backend
    
    private_key = serialization.load_pem_private_key(
        creds["private_key"].encode(),
        password=None,
        backend=default_backend()
    )
    
    signing_input = f"{header}.{payload}".encode()
    signature = private_key.sign(signing_input, padding.PKCS1v15(), hashes.SHA256())
    sig_b64 = base64.urlsafe_b64encode(signature).rstrip(b'=').decode()
    
    jwt = f"{header}.{payload}.{sig_b64}"
    
    # Échanger le JWT contre un access token
    data = urllib.parse.urlencode({
        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
        "assertion": jwt
    }).encode()
    
    req = urllib.request.Request(
        "https://oauth2.googleapis.com/token",
        data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"}
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())["access_token"]

FAL_API_KEY = os.environ.get("FAL_API_KEY")

def upscale_image_fal(img_b64, max_retries=30):
    """Upscale 4K via fal.ai SeedVR2 — haute qualité, pas de quota"""
    if not FAL_API_KEY:
        return None
    
    for attempt in range(1, max_retries + 1):
        try:
            print(f"[FAL] Tentative {attempt}/{max_retries}...")
            # Soumettre la requête
            resp = requests.post(
                "https://queue.fal.run/fal-ai/seedvr2/image",
                headers={
                    "Authorization": f"Key {FAL_API_KEY}",
                    "Content-Type": "application/json"
                },
                json={
                    "image_url": f"data:image/png;base64,{img_b64}",
                    "scale": 4,
                    "model": "seedvr2-3b"
                },
                timeout=30
            )
            
            if resp.status_code not in (200, 201):
                print(f"[FAL] Erreur {resp.status_code}: {resp.text[:200]}")
                if attempt < max_retries:
                    time.sleep(min(4 * attempt, 30))
                continue
            
            data = resp.json()
            request_id = data.get("request_id")
            
            if not request_id:
                # Résultat direct
                img_url = data.get("image", {}).get("url")
                if img_url:
                    img_data = requests.get(img_url, timeout=60).content
                    print("[FAL] ✅ 4K")
                    return base64.b64encode(img_data).decode()
                continue
            
            # Polling pour le résultat
            for _ in range(60):
                time.sleep(3)
                poll = requests.get(
                    f"https://queue.fal.run/fal-ai/seedvr2/image/requests/{request_id}",
                    headers={"Authorization": f"Key {FAL_API_KEY}"},
                    timeout=30
                )
                if poll.status_code == 200:
                    result = poll.json()
                    status = result.get("status")
                    if status == "COMPLETED":
                        img_url = result.get("response", {}).get("image", {}).get("url")
                        if img_url:
                            img_data = requests.get(img_url, timeout=60).content
                            print("[FAL] ✅ 4K")
                            return base64.b64encode(img_data).decode()
                        break
                    elif status in ("FAILED", "CANCELLED"):
                        print(f"[FAL] Statut: {status}")
                        break
        except Exception as e:
            print(f"[FAL] Exception tentative {attempt}: {e}")
            if attempt < max_retries:
                time.sleep(min(4 * attempt, 30))
    
    print(f"[FAL] ❌ Échec après {max_retries} tentatives")
    return None
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, render_template, request, Response, jsonify
from botocore.config import Config

app = Flask(__name__)

# ── Cache flocages chargé au démarrage ─────────────────────────────────────
# (sera initialisé au premier appel si pas encore chargé)

# ── Job Queue Asynchrone ────────────────────────────────────────────────────
# Stockage en mémoire des sessions de génération actives
# { session_id: { total, done, errors, results, status, created_at } }
_job_sessions = {}
_job_sessions_lock = threading.Lock()

# Nombre de workers parallèles pour la génération
WORKER_COUNT = 50

def _get_or_create_session(session_id, total):
    with _job_sessions_lock:
        if session_id not in _job_sessions:
            _job_sessions[session_id] = {
                "total": total,
                "done": 0,
                "errors": 0,
                "results": [],
                "pending_buffer": [],
                "status": "running",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "tiktoks_created": [],
                "buffer_remaining": 0,
                "user": None,
            }
        return _job_sessions[session_id]

def _update_session(session_id, success, image_b64=None, floc=None, error=None, idx=None, user=None, template_key=""):
    batch = None
    session_user = None

    # Sauvegarder l'image dans R2 temp AVANT le lock — libère la RAM immédiatement
    r2_img_key = None
    if success and image_b64:
        try:
            r2_img_key = f"sessions/tmp/{session_id}_{idx}.png"
            r2_put_image(r2_img_key, base64.b64decode(image_b64))
            image_b64 = None  # Libérer la RAM immédiatement
        except Exception as e:
            print(f"[SESSION] Erreur stockage image temp R2: {e}")
            r2_img_key = None

    with _job_sessions_lock:
        s = _job_sessions.get(session_id)
        if not s:
            return
        s["done"] += 1
        if success and r2_img_key:
            s["results"].append({"r2_key": r2_img_key, "floc": floc, "orig_index": idx, "template_key": template_key})
            s["pending_buffer"].append({"r2_key": r2_img_key, "floc": floc, "template_key": template_key})
        else:
            s["errors"] += 1
            s["results"].append({"r2_key": None, "floc": floc or "", "orig_index": idx, "error": error or "Erreur inconnue"})
        if s["done"] >= s["total"]:
            s["status"] = "done"
        pending = s["pending_buffer"]
        session_user = s.get("user") or user
        if len(pending) >= TIKTOK_SIZE:
            batch = pending[:TIKTOK_SIZE]
            s["pending_buffer"] = pending[TIKTOK_SIZE:]
        # Sauvegarder dans R2 toutes les 10 images pour survivre aux crashes
        done_count = s["done"]
        total_count = s["total"]
        session_start = s.get("created_at","")
        tiktoks_so_far = list(s.get("tiktoks_created",[]))

    # Sauvegarder snapshot dans R2 toutes les 10 images
    if done_count % 10 == 0 or done_count == total_count:
        try:
            r2_put_json(f"sessions/{session_id}.json", {
                "id": session_id,
                "user": session_user,
                "start": session_start,
                "last_update": datetime.now(timezone.utc).isoformat(),
                "total": total_count,
                "done": done_count,
                "success": done_count - (0),  # approximatif
                "status": "running" if done_count < total_count else "done",
                "tiktoks_created": tiktoks_so_far,
            })
        except Exception:
            pass

    if batch:
        try:
            # Lire les images depuis R2 temp
            imgs = []
            for r in batch:
                try:
                    obj = get_r2().get_object(Bucket=R2_BUCKET, Key=r["r2_key"])
                    imgs.append(base64.b64encode(obj["Body"].read()).decode())
                    # Supprimer le fichier temp R2 après lecture
                    get_r2().delete_object(Bucket=R2_BUCKET, Key=r["r2_key"])
                except Exception:
                    imgs.append(None)
            flocs = [r["floc"] for r in batch]
            tkeys = [r.get("template_key","") for r in batch]
            created, remaining = add_to_buffer_and_create_tiktoks(imgs, flocs, session_user, tkeys)
            with _job_sessions_lock:
                s = _job_sessions.get(session_id)
                if s:
                    s["tiktoks_created"].extend(created)
                    s["buffer_remaining"] = remaining
            print(f"[SESSION] ✅ {len(created)} TikTok(s) créé(s) en cours de génération")
        except Exception as e:
            print(f"[SESSION] Erreur création TikTok intermédiaire: {e}")

def _finalize_session(session_id, user):
    """Crée les TikToks depuis les images restantes (< 7) à la fin — atomique pour éviter les doublons"""
    with _job_sessions_lock:
        s = _job_sessions.get(session_id)
        if not s:
            return
        # Vider pending_buffer atomiquement pour éviter double traitement
        pending = s["pending_buffer"][:]
        s["pending_buffer"] = []  # ← reset atomique sous lock
        session_user = s.get("user") or user

    if not pending:
        return

    # Lire les images depuis R2 temp
    valid = [r for r in pending if r.get("r2_key")]
    if not valid:
        return

    imgs = []
    for r in valid:
        try:
            obj = get_r2().get_object(Bucket=R2_BUCKET, Key=r["r2_key"])
            imgs.append(base64.b64encode(obj["Body"].read()).decode())
            get_r2().delete_object(Bucket=R2_BUCKET, Key=r["r2_key"])
        except Exception:
            imgs.append(None)
    flocs = [r["floc"] for r in valid]
    tkeys = [r.get("template_key","") for r in valid]
    try:
        created, remaining = add_to_buffer_and_create_tiktoks(imgs, flocs, session_user, tkeys)
        with _job_sessions_lock:
            s = _job_sessions.get(session_id)
            if s:
                s["tiktoks_created"].extend(created)
                s["buffer_remaining"] = remaining
        print(f"[SESSION] Finalisation: {len(created)} TikTok(s), {remaining} images en buffer")
    except Exception as e:
        print(f"[SESSION] Erreur finalisation: {e}")

def _run_bulk_async(session_id, items, user, resolution):
    """Phase 1 : Gemini en parallèle. Phase 2 : Replicate séquentiel → zéro 429"""
    import gc
    session = _get_or_create_session(session_id, len(items))

    for i, item in enumerate(items):
        item["_index"] = i
        item["_gemini_result"] = None  # résultat Gemini avant upscale

    # ── Phase 1 : Gemini en parallèle (WORKER_COUNT workers) ─────────────
    print(f"[BULK] Phase 1 — Gemini sur {len(items)} images avec {WORKER_COUNT} workers...")

    # Charger pepites et normaux UNE SEULE FOIS avant les workers
    import random as _rnd
    try:
        _floc_data = r2_get_json("meta/flocages.json") or {}
        _pepites_list = _floc_data.get("pepites", PEPITE_FLOCAGES) or PEPITE_FLOCAGES
        _all_flocs = _floc_data.get("flocages", DEFAULT_FLOCAGES) or DEFAULT_FLOCAGES
        _pepites_set_lower = {p.lower().strip() for p in _pepites_list}
        _normaux_list = [f for f in _all_flocs if f.lower().strip() not in _pepites_set_lower]
    except Exception:
        _pepites_list = PEPITE_FLOCAGES
        _normaux_list = [f for f in DEFAULT_FLOCAGES if f.lower().strip() not in {p.lower().strip() for p in PEPITE_FLOCAGES}]

    # Assigner pépite ou normal selon la position dans le TikTok
    # Images dont (index % 7) < 4 → pépite, sinon → normal
    def gemini_one(item):
        idx = item["_index"]
        try:
            # Échelonner le démarrage — évite de spammer 50 requêtes simultanées
            time.sleep(idx * 0.1 % 3)  # délai 0-3s selon l'index
            pos_in_tiktok = idx % TIKTOK_SIZE  # 0-6
            if pos_in_tiktok < 4 and _pepites_list:
                floc_str = _rnd.choice(_pepites_list)
            elif _normaux_list:
                floc_str = _rnd.choice(_normaux_list)
            else:
                floc_str = _rnd.choice(_pepites_list or DEFAULT_FLOCAGES)
            parts = [p.strip() for p in floc_str.split("/")]
            fname = parts[0] if parts else ""
            fnum  = parts[1] if len(parts) > 1 else "2"
            fbelow = parts[2] if len(parts) > 2 else ""
            item["_floc"] = floc_str  # stocker pour _update_session
            if item.get("variant") == "v2":
                prompt_fn = build_prompt_v2
            else:
                prompt_fn = None
            res = call_gemini_only(item["bytes"], item["mime"], fname, fnum, fbelow, prompt_fn=prompt_fn)
            item["_gemini_result"] = res
            log_generation(user, res["success"])
            # Marquer la template comme utilisée (lock seulement sur le cache mémoire)
            if res.get("success") and item.get("template_key"):
                flush_now = False
                keys_to_flush = []
                with _used_templates_lock:
                    _used_templates_cache.append(item["template_key"])
                    if len(_used_templates_cache) >= 10:
                        keys_to_flush = list(_used_templates_cache)
                        _used_templates_cache.clear()
                        flush_now = True
                if flush_now:
                    try:
                        used = r2_get_json("meta/templates_used.json") or {"keys": []}
                        keys_list = used.get("keys", [])
                        for tk in keys_to_flush:
                            if tk in keys_list: keys_list.remove(tk)
                            keys_list.insert(0, tk)
                        used["keys"] = keys_list[:200]
                        r2_put_json("meta/templates_used.json", used)
                    except Exception:
                        pass
            if not res["success"]:
                _update_session(session_id, False, error=res.get("error"), idx=idx)
        except Exception as e:
            print(f"[WORKER] Erreur Gemini image {idx}: {e}")
            item["_gemini_result"] = {"success": False, "error": str(e)}
            _update_session(session_id, False, error=str(e), idx=idx)
        finally:
            item["bytes"] = None
            gc.collect()

    with ThreadPoolExecutor(max_workers=WORKER_COUNT) as ex:
        list(ex.map(gemini_one, items))

    # ── Phase 2 : Replicate en parallèle (4 simultanés max) ─────────────
    print(f"[BULK] Phase 2 — Upscaling 4K (4 parallèles)...")
    successful = [it for it in items if it.get("_gemini_result", {}).get("success")]

    def upscale_one(item):
        idx = item["_index"]
        img = item["_gemini_result"]["image"]
        try:
            if REPLICATE_API_KEY:
                upscaled = upscale_image(img)
                if not upscaled:
                    _update_session(session_id, False, error="❌ Upscaling 4K impossible après 30 tentatives.", idx=idx)
                    return
                img = upscaled
            _update_session(session_id, True, img, item.get("_floc",""), idx=idx, user=user, template_key=item.get("template_key",""))
            img = None
        except Exception as e:
            print(f"[UPSCALE] Erreur inattendue image {idx}: {e}")
            _update_session(session_id, False, error=str(e), idx=idx)
        finally:
            if item.get("_gemini_result"):
                item["_gemini_result"]["image"] = None
            item["_gemini_result"] = None
            gc.collect()

    # Real-ESRGAN séquentiel pour éviter la surcharge
    for item in successful:
        upscale_one(item)

    # Créer les TikToks une fois tout terminé
    _finalize_session(session_id, user)

    # Nettoyer les vieilles sessions de la RAM (> 30 min) pour libérer la mémoire
    now_ts = datetime.now(timezone.utc)
    with _job_sessions_lock:
        to_delete = []
        for sid, sess in _job_sessions.items():
            if sess.get("status") in ("done", "cancelled") and sess.get("created_at"):
                try:
                    created = datetime.fromisoformat(sess["created_at"])
                    if (now_ts - created).total_seconds() > 1800:  # 30 min
                        to_delete.append(sid)
                except Exception:
                    pass
        for sid in to_delete:
            del _job_sessions[sid]
        if to_delete:
            print(f"[SESSION] Nettoyage RAM: {len(to_delete)} sessions supprimées")

    # Sauvegarder la session dans R2
    with _job_sessions_lock:
        s = _job_sessions.get(session_id)
        success_count = s["done"] - s["errors"] if s else 0
    r2_put_json(f"sessions/{session_id}.json", {
        "id": session_id, "user": user,
        "start": session["created_at"],
        "end": datetime.now(timezone.utc).isoformat(),
        "total": len(items), "success": success_count
    })

# ── Config ─────────────────────────────────────────────────────────────────
API_KEY        = os.environ.get("GEMINI_API_KEY")
MODEL_URL      = "https://generativelanguage.googleapis.com/v1beta/models/gemini-3.1-flash-image:generateContent"
COST_PER_IMAGE = 0.069
TIKTOK_SIZE    = 7
FIXED_CAPTION  = "3 Maillot Acheté 1 Offert 🎁 #volakits #ete #foot"
# Créneaux de publication en UTC, PAR COMPTE (le principal poste plus souvent que les autres)
# Conversion heure française (UTC+2 été) → UTC : soustraire 2h
SCHEDULE_TIMES_BY_ACCOUNT = {
    "Volakits Main (wael)": ["08:00", "13:00", "16:30", "19:00"],  # 4x/jour — 10h/15h/18h30/21h Paris (UTC+2)
    "Volakits 1 (seik)":    ["08:30", "15:30"],  # 2x/jour — 10h30/17h30 Paris
    "Volakits 2 (momo)":    ["08:30", "15:30"],  # 2x/jour
    "Volakits 6 (wassim)":  ["08:30", "15:30"],  # 2x/jour
}
SCHEDULE_TIMES_DEFAULT = ["08:30", "15:30"]  # 2x/jour par defaut

def get_schedule_times_for_account(account):
    """Retourne les créneaux horaires (UTC) pour un compte donné"""
    return SCHEDULE_TIMES_BY_ACCOUNT.get(account, SCHEDULE_TIMES_DEFAULT)

R2_ENDPOINT   = os.environ.get("R2_ENDPOINT")
R2_PUBLIC_URL = os.environ.get("R2_PUBLIC_URL", "https://pub-2041419f649b434681cde993145feaee.r2.dev")
R2_ACCESS_KEY = os.environ.get("R2_ACCESS_KEY_ID")
R2_SECRET_KEY = os.environ.get("R2_SECRET_ACCESS_KEY")
R2_BUCKET     = os.environ.get("R2_BUCKET", "jersey-templates")


# Metricool API
METRICOOL_TOKEN = os.environ.get("METRICOOL_TOKEN")  # À définir dans Railway
METRICOOL_USER_ID = os.environ.get("METRICOOL_USER_ID", "5037969")
METRICOOL_ACCOUNTS = {
    "Volakits Main (wael)": {"blog_id": "6542376", "active": True},
    "Volakits 1 (seik)":    {"blog_id": "6675120", "active": True},
    "Volakits 2 (momo)":    {"blog_id": "6675158", "active": True},
    "Volakits 6 (wassim)":  {"blog_id": "6675169", "active": True},
}


# ── Auth utilisateurs ──────────────────────────────────────────────────────
# Format: { "prenom": "mot_de_passe" }
# Change les mots de passe dans les variables Railway (AUTH_USERS en JSON)
_DEFAULT_USERS = {
    "Wael": os.environ.get("AUTH_PASS_WAEL", "wael2024"),
    "Moh": os.environ.get("AUTH_PASS_MOH", "moh2024"),
    "Wassim": os.environ.get("AUTH_PASS_WASSIM", "wassim2024"),
    "Seik": os.environ.get("AUTH_PASS_SEIK", "seik2024"),
}

def get_auth_users():
    raw = os.environ.get("AUTH_USERS")
    if raw:
        try: return json.loads(raw)
        except: pass
    return _DEFAULT_USERS

@app.route("/api/auth/login", methods=["POST"])
def api_auth_login():
    data = request.json or {}
    name = data.get("name", "").strip()
    password = data.get("password", "").strip()
    users = get_auth_users()
    if name in users and users[name] == password:
        return jsonify({"success": True, "user": name})
    return jsonify({"success": False, "error": "Prénom ou mot de passe incorrect"}), 401

# Préfixes R2
PFX_QUEUE     = "queue/"
PFX_SCHEDULED = "scheduled/"
PFX_TEMPLATES = "templates/"
PFX_TEMPLATES_V2 = "templates_v2/"
KEY_BOX_REF = "meta/volakits_box_ref.png"  # Image de référence boîte Volakits pour le générateur v2
PFX_LOGS      = "logs/"
KEY_BUFFER    = "buffer/pending.json"
KEY_COUNTER   = "meta/tiktok_counter.json"
KEY_ACCOUNTS  = "meta/accounts.json"
KEY_USED_SLOTS = "meta/used_slots.json"

# Lock pour éviter race conditions sur le compteur/buffer
_r2_lock = threading.Lock()
_log_lock = threading.Lock()
_r2_client = None
_r2_client_lock = threading.Lock()

def get_r2():
    global _r2_client
    if not R2_ENDPOINT:
        return None
    with _r2_client_lock:
        if _r2_client is None:
            _r2_client = boto3.client(
                "s3",
                endpoint_url=R2_ENDPOINT,
                aws_access_key_id=R2_ACCESS_KEY,
                aws_secret_access_key=R2_SECRET_KEY,
                config=Config(signature_version="s3v4", max_pool_connections=100),
                region_name="auto",
            )
        return _r2_client

DEFAULT_FLOCAGES = [
    "UN PEU / 2 / LIMONADE",
    "UN PEU / 2 / GAZOUZ",
    "Juste / 1 / Mec chill",
    "Lover / 2 / Blonde",
    "Jolie / 2 / moiselle",
    "Histoire / 2 / Love",
    "MENTALITÉ / 2 / PIRATE",
    "Je suis / 1 / Charo",
    "J'veux vos / 7 / snaps",
    "J'ai / 1 / P'tit Zgeg",
    "ARRACHEUR / 2 / LATINAS",
    "TA PAS / 1 / SNAP",
    "MET / 2 / LA CREME",
    "MEC / 2 / PANAME",
    "Cousine / 7 / Ma came",
    "Skinny / 2 / Quoi",
    "Fan / 2 / Moi",
    "MAX / 70 / KG",
    "L'HOMME / 2 / TA VIE",
    "CHASSEUR / 2 / LATINA",
    "Le mec / 2 / Mon bâtiment",
    "Pas / 2 / Mariage",
    "QUE MA BFF / 0 / TANA",
    "Pas / 2 / Love",
    "BAISEUR / 2 / MILF",
    "Mec / 2 / Djerba",
    "PAS / 2 / SALAM",
    "Ma copine / 7 / mon combat",
    "Arracheur / 2 / String",
    "Enfant / 2 / Gaza",
    "Love / 02 / Blonde",
    "J'ai Déjà / 1 / MEUF",
    "Arracheuse / 2 / Grec",
    "Envoie ton / 06 / Princesse",
    "Elle veut / 2 / Fou",
    "Elle veut / 2 / Malade",
    "J'veux vos / 4 / Snaps",
    "Pas / 2 / Comme moi",
    "Love / 2 / Moha",
    "Voleuse / 2 / Brainrot",
    "Voleur / 2 / Brainrot",
    "Frère / 2 / Sang",
    "REMPLI / 2 / MOSSEBA",
    "Loveur / 2 / Blonde",
    "Loveur / 2 / Brune",
    "Pilote / 2 / ton coeur",
    "PAS / 2 / TAL",
    "MIEUX / 100 / TOI",
    "J'AI / 4 / FEMMES",
    "JE VEUX / 4 / FEMMES",
    "Fémur / 2 / Acier",
    "Salopard / 100 / Baaraka",
    "CALE / 1 / SNUS",
    "Arabe / 100 / Papier",
    "BUVEUR / 2 / CYPRINE",
    "DEFOURAILLEUR / 2 / MILF",
    "Cherche / 1 / Snap",
    "Mec / 100 / Papier",
    "jamais / 100 / elle",
    "bbl / 2 / malade",
    "pas / 2 / ralentir",
    "BBL / 2 / TANA",
    "Crois pas t'es / 1 / Jamal",
    "Jamais / 100 / Ma blonde",
    "Jamais djadja / 100 / Dinaz",
    "3ARBI / 100 / BARAKA",
    "IN LOVE / 2 / BLONDES",
    "LOVEUR / 2 / BLONDES",
    "Tié / 1 / Tigre",
    "en pétard / 2 / ouf",
    "Bourré / 2 / Talent",
    "Scammer / 2 / Daronnes",
    "Je veux / 1 / Femme",
    "Mangeur / 2 / Cavu",
    "BANDEUR / 2 / BRUNE",
    "L'homme / 2 / La situation",
    "Kiffeur / 2 / Cavu",
    "Tete / 2 / Kiwi",
    "G PAS / 2 / SOUS",
    "Duo / 2 / Charo",
    "Jamais / 2 / Sans 12",
    "Jamais / 2 / Sans 13",
    "Jamais / 2 / Sans 16",
    "Jamais / 2 / Sans 3",
    "LOVEUSE / 2 / MON BRUN",
    "LIVREUR / 2 / QUALITÉ",
    "love / 2 / mon ex",
    "t'as pas / 1 / snap",
    "Reine / 2 / l'apéro",
    "Roi / 2 / l'apéro",
    "JAMAIS / 100 / FEMMES",
    "J'AI DÉJÀ MA / 011 / BOOSTER",
    "J'AI DÉJÀ MA / 016 / BOOSTER",
    "J'AI DÉJÀ MA / 015 / BOOSTER",
    "Mangeur / 2 / Brunes",
    "T'as / 1 / snap",
    "Homme / 2 / ta vie",
    "amoureuse / 2 / mon copain",
    "Love / 2 / Ma copine",
    "Love / 2 / Mon copain",
    "J'AI DÉJÀ / 1 / FEMME",
    "AIGRI / 2 / NATURE",
    "AIGRI / 2 / BASE",
    "Collectionneur / 2 / MST",
    "DONNEUR / 2 / MST",
    "Cherche / 1 / Meuf",
    "CONGOLAISE / 2 / KINSHASA",
    "Italien / 2 / Napoli",
    "Kabyle / 100 / Vice",
    "COPINE / 100 / Vice",
    "Fan / 2 / Tana",
    "Bandeur / 2 / States",
    "OH TIÉ / 13 / SÉDUISANTE",
    "LÂCHE / 1 / SEIN",
    "BBL / 2 / STAR",
    "CHIENNE / 2 / GUERRE",
    "JE BANDE / 13 / VITE",
    "BANDE / 13 / VITE",
    "tigresse / 2 / OUF",
    "Enfants / 2 / LA CAF",
    "Chasseur / 2 / Brunette",
    "Chasseur / 2 / Pétasse",
    "Jamais / 100 / MA SOEUR",
    "Fils / 2 / Stup",
    "Fils / 2 / PUTE",
    "DU RSA / 0 / RS3",
    "Pétase / 100 / Vice",
    "BAISE / 100 / CAPOTE",
    "ARRACHEUR / 2 / CHATTE",
    "LÂCHE / 2 / SEIN",
    "BODYCOUNT / 00 / MEC BIEN",
    "BESOIN / 2 / TON SNAP",
    "Je suis / 1 / Homme simple",
    "Love / 2 / Ma Go",
    "Cherche / 1 / Blonde",
    "Cherche / 1 / Plan Cul",
    "Sénégalais / 100 / Papier",
    "Gattouz / 2 / Partouz",
    "MENTAL / 2 / CHARO",
    "Amoureuse / 2 / Toi",
    "ARRACHEUR / 2 / CAVU",
    "JUSTE / 1 / MEUF CHILL",
    "Chien / 100 / Laisse",
    "croqueur / 2 / cavu",
    "Duo / 100 / Vice",
    "Inshallah / 4 / Femmes",
    "ARRÊTE / 2 / KHLE3",
    "TRAIN2VIE / 2 / HAYAWEN",
    "GRATTEUR / 2 / LA CAF",
    "A LA RECHERCHE / 2 / JUMELLES",
    "EN MANQUE / 2 / SEXE",
    "L'amour / 2 / Ma vie",
    "FAN / 2 / MADAME",
    "FAN / 2 / MONSIEUR",
    "FAN / 2 / BLONDE",
    "J'ai que / 1 / frère",
    "Non / 1 / Posable",
    "Mangeur / 2 / Tacos",
    "Roule / 13 / Vite",
    "Récolteur / 2 / Snap",
    "BBL / 2 / FOU",
    "CHUIS / 1 / MEC BIEN",
    "Kiffeur / 2 / Batata",
    "Bouffeur / 2 / Cul",
    "ANTI SUCEUR / 2 / BITE",
    "zgeg / 10 / proportionné",
    "Dévoreur / 2 / clitos",
    "Homme / 2 / Sa vie",
    "ARRACHEUR / 2 / brunes",
    "Briseuse / 2 / Coeur",
    "CHEF / 2 / BANDE",
    "Baiseur / 2 / Petasse",
    "jamais / 100 / lui",
    "Décaleur / 2 / strings",
    "À / 4 / PATTES",
    "attrapeur / 2 / brunes",
    "BOIS / 100 / MODERATION",
    "Kiffeur / 2 / Brunes",
    "DONNE TON / 06 / BEAUTÉ",
    "Chasseur / 2 / Brune",
    "PAS / 2 / COMME NOUS",
    "LE BOULET / 2 / LA BANDE",
    "Mec / 13 / Haram",
    "Mec / 13 / Halal",
    "Langue / 2 / Molière",
    "C'est l' / 69 / Pelo",
    "MEC / 100 / TITULAIRE",
    "PINEUR / 2 / CHEVRE",
    "VOLEUR / 2 / SNAP",
    "Arracheur / 2 / Tysmé",
    "Briseur / 2 / Coeurs",
    "J'ai / 1 / Meuf",
    "Projet / 4 / Femmes",
    "Love / 2 / Ma femme",
    "PÊCHEUR / 2 / MILF",
    "ARRACHEUR / 2 / SNAP",
    "PAS / 2 / SELEM",
    "Buveur / 2 / Vovo",
    "La Dame / 2 / Quelqu'un",
    "Recolteur / 2 / Snap",
    "Footballeur / 2 / Qualité",
    "Toujours / 100 / Meuf",
    "Nous / 2 / Je le sens",
    "JAMAIS / 2 / PRESSION",
    "ARRACHEUR / 2 / STRINGS",
    "J'ai déjà / 5 / Mecs",
    "Charger / 2 / Malade",
    "Kiffeur / 2 / Binouz",
    "Meuf / 100 / Vice",
    "CHERCHE / 1 / MILF",
    "Aigrie / 2 / Ouf",
    "3arbia / 100 / Vice",
    "VIENS / 2 / SECONDE",
    "SANS PRISE / 2 / TETE",
    "3arbia / 100 / papier",
    "Groupe / 2 / Vicieux",
    "CONSOMMATRICE / 2 / PAIN",
    "Croqueuse / 2 / Diamant",
    "love / 2 / Ma parisienne",
    "Train / 2 / Vie",
    "Aigrie / 2 / Nature",
    "PAS / 2 / MEUF",
    "CASHFLOW / 13 / POSITIF",
    "ARRÊTE / 2 / PISTER",
    "et tié / 13 / séduisante",
    "Loveur / 2 / Femme",
    "ARRACHEUR / 2 / BAR",
    "Ta pas rêver / 2 / Moi",
    "Ta rêver / 2 / Moi",
    "Chouchou / 2 / Madame",
    "Tête / 2 / Turc",
    "Tête / 2 / Noir",
    "Tête / 2 / Arabe",
    "Tête / 2 / Blanc",
    "Briseuse / 2 / Foyer",
    "VIE / 100 / STRESS",
    "Remplie / 2 / Vices",
    "Alcoolique / 2 / Qualité",
    "Caleur / 2 / Snus",
    "Kaleur / 2 / Snus",
    "CHIBRE / 10 / PROPORTIONNEL",
    "CALVITIE / 13 / AVANCÉE",
    "Roi / 2 / Labécane",
    "Fan / 2 / Toi",
    "J'ai pas / 2 / Meufs",
    "T'as / 1 / Snap ?",
    "FAN / 2 / DAMSO",
    "LÈCHEUR / 2 / TÉTON",
    "j'ai plus / 1 / EURO",
    "MÉLANGEUSE / 2 / MEC",
    "j'ai plus / 1 / ROND",
    "DÉREGLEUSE / 2 / MARCHÉ",
    "PAS / 2 / LEASING",
    "ARRACHEUR / 2 / LATINA",
    "BAISEUR / 2 / LATINA",
    "ALCOOLIQUE / 2 / FOU",
    "TOUJOURS / 100 / BATTERIE",
    "JAMAIS / 100 / BATTERIE",
    "Lécheur / 2 / Chatte",
    "Baiseur / 2 / Chatte",
    "Top / 1 / Remplaçant",
    "MADAME / 2 / MONSIEUR",
    "MONSIEUR / 2 / MADAME",
    "Fan / 2 / Lacrim",
    "JAMAIS / 100 / TAC",
    "Jamais / 100 / TIC",
    "Déjà / 1 / Femme",
    "Tranquilo / 2 / Quoi",
    "Mec clean / 100 / bodycount",
    "Donneuse / 2 / Go",
    "Nain / 2 / Jardin",
    "Tacos / 3 / Viande",
    "Calvitie / 2 / Malade",
    "Calvitie / 2 / Barbare",
    "3arbia / 2 / Luxe",
    "Fan / 2 / Mon ex",
    "Remplis / 2 / Mosseba",
    "LECHEUR / 2 / TEUCH",
    "Femme / 2 / Ta vie",
    "Envois / 1 / Snap",
    "Briseur / 2 / Cœur",
    "MEC / 100 / LIMITES",
    "chercheur / 2 / snap",
    "Donneur / 2 / Snap",
    "Uniquement / 2 / L'authentique",
    "VIE / 2 / CAMPAGNE",
    "Sirop / 2 / fraise",
    "J ai pas / 1 / Sous",
    "Buveur / 2 / Flash",
    "CLAQUE / 2 / FESSES",
    "Bande / 2 / Zgegs",
    "Comorien / 100 / Papier",
    "Je mérite / 1 / Bisous",
    "Baise / 100 / Capotes",
    "Lécheur / 2 / Teuch",
    "BBL / 2 / TASPÉ",
    "Baiseur / 100 / Capotes",
    "PAS / 2 / TALES",
    "TIE / 1 / TIGRE",
    "Bandeur / 2 / Blondes",
    "TOUS FANS / 2 / MOI",
    "INCHALLAH / 1 / HOMME RICHE",
    "Arracheuse / 2 / Strings",
    "Chasseur / 2 / Blonde",
    "J'VEUX VOS / 4 / SNAP",
    "Mangeur / 2 / Bouzelouf",
    "T'AS PAS / 1 / SNAP BEAUTÉ ?",
    "Boit / 100 / Modération",
    "KIFFEUR / 2 / HARR",
    "Gitan / 100 / Camping",
    "Buveuse / 100 / Modération",
    "Fan / 2 / Morgane",
    "DORA / 100 / BABOUCHE",
    "FILS / 2 / POULPE",
    "EN AMONT / 69 / LA TRICK",
    "Dune / 2 / Sable",
    "Love / 2 / Toi",
    "J'ai pas / 2 / Daron",
    "Loveur / 2 / Brunes",
    "Kiffeuse / 2 / Vovo",
    "Bandeuse / 2 / Brun",
    "AMOUREUX / 2 / MA FEMME",
    "InshaaAllah / 1 / RS6",
    "CHIANT / 2 / OUF",
    "Je mérite / 1 / Bisous ?",
    "RESPONSABLE / 2 / LAV CAR",
    "Elle a mal / 0 / Reins",
    "Trou / 2 / Balle",
    "j'veux / 1 / sushi",
    "Pro / 2 / DoroParty",
    "Back / 2 / Back",
    "3rbia / 2 / France",
    "MANDA / 30 / ANS",
    "Juste / 1 / Meuf dégénérée",
    "RONALDO / 7 / LE GOAT",
    "L'ex préfère / 2 / ta copine",
    "PSG / 2 / LDC",
    "AMOUREUSE / 2 / L'ARGENT",
    "ATTITUDE / 2 / BADIES",
    "TUNNEL / 2 / OUF",
    "addict / 0 / locksé",
    "J'veux marier / 2 / Portugaise",
    "J'veux / 1 / Portugaise",
    "Accro / 0 / Portugaise",
    "Fan / 2 / Sa copine",
    "Montre / 1 / Sein",
    "JAMAIS / 100 / MON RICARD",
    "JAMAIS / 100 / MON FLASH",
    "WALLAH / 7 / LOURD",
    "Kiffeuse / 2 / Fessées",
    "Décaleur / 2 / String",
    "BESOINS / 2 / TON SNAP",
    "C'EST HARR / 2 / DINGUE",
    "Casse / 1 / Tour",
    "FAIS PLUS / 2 / TIRAMISUS",
    "C'EST / 1 / BATARD",
    "Envoies ton / 06 / Princesse",
    "PREPARATEUR / 2 / FLASH",
    "CHIBRE / 10 / PROPORTIONNE",
    "UN PEU / 2 / GAZZOUZ",
    "MANGEUR / 2 / CROUSTY",
    "C DES JALOUX / 2 / MON FILS",
    "BRISEUR / 2 / CARRIERE",
    "ROUX / 2 / SECOURS",
    "RIEN / 100 / RIEN",
    "PAS / 2 / STRESS",
    "CHUI LA / 7 / ANNEE",
    "REBEU / 10 / TINGUE",
    "CHERCHEUR / 2 / TRAVAIL",
    "PAS / 2 / COMME MOI",
    "CHERCHE / 1 / DARRONE",
    "LOVEUSE / 2 / BRUN",
    "LOVEUR / 2 / BRUNE",
    "BROUTEUR / 2 / MINOU",
    "SOUILLEUR / 2 / TEUCH",
    "LOVE / 2 / MON EX",
    "AMOUR / 1 / POSSIBLE",
    "C HARR / 2 / BZ",
    "GRATTEUR / 2 / LA CAF",
    "MALIEN / 100 / EAU",
    "CARBURE / 0 / ROSE",
    "LANGUE / 2 / MOLIERE",
    "BANDE / 2 / TCHOIN",
    "REMPLIS / 2 / MST",
    "BOIS / 100 / MODERATION",
    "BANDEUR / 2 / TISME",
    "BUVEUR / 2 / BIERE",
    "GRATTEUR / 2 / SNAPS",
    "BRISEUR / 2 / NUQUES",
    "PECHEUR / 2 / MILF",
    "CONSOMMATEUR / 2 / TAGA",
    "CHERCHE / 1 / PLAN CUL",
    "3ARBI / 100 / BARAAKA",
    "TRAQUEUR / 2 / MERES PORTEUSES",
    "JAMAIS / 100 / MA PUFF",
    "BUVEUR / 2 / CYPRINE",
    "DOMPTEUR / 2 / MINEURES",
    "ALGEROISE / 2 / LUXE",
    "3ARBI / 2 / LUXE",
    "MANGEUR / 2 / SHAWARMA",
    "CHERCHE / 1 / FEMME",
    "TROP / 2 / CHARISME",
    "LECHEUR / 2 / TEUCH",
    "JE BAISE / 100 / CAPOTE",
    "MAGIC / 6 / T'AIME",
    "MANGEUR / 2 / MSEMEN",
    "BOURRE / 2 / TALENT",
    "J'AI ENVIE / 2 / CHIER",
    "JAMAIS / 100 / MON VERRE",
    "DUO / 2 / BLONDES",
    "DUO / 2 / BRUNES",
    "HOMME / 13 / EXPERIMENTE",
    "ARRACHEUR / 2 / PERRUQUES",
    "DRIBBLEUR / 2 / KAFFIR",
    "TRAQUEUR / 2 / PUPUCES",
    "BANDE / 2 / TIMP",
    "JE SUIS / 100 / PAPIERS",
    "JE PUE / 2 / FOU",
    "PAS / 2 / DARRON",
    "EN MANQUE / 2 / G3AR",
    "BANDEUR / 2 / MAGHREBINE"
]
PEPITE_FLOCAGES = [
    "Jolie / 2 / moiselle",
    "J'ai / 1 / P'tit Zgeg",
    "PAS / 2 / SALAM",
    "Arracheur / 2 / String",
    "Envoie ton / 06 / Princesse",
    "Elle veut / 2 / Malade",
    "J'veux vos / 4 / Snaps",
    "REMPLI / 2 / MOSSEBA",
    "Pilote / 2 / ton coeur",
    "JE VEUX / 4 / FEMMES",
    "Salopard / 100 / Baaraka",
    "CALE / 1 / SNUS",
    "Arabe / 100 / Papier",
    "BUVEUR / 2 / CYPRINE",
    "DEFOURAILLEUR / 2 / MILF",
    "bbl / 2 / malade",
    "BBL / 2 / TANA",
    "Scammer / 2 / Daronnes",
    "Collectionneur / 2 / MST",
    "DONNEUR / 2 / MST",
    "OH TIÉ / 13 / SÉDUISANTE",
    "LÂCHE / 1 / SEIN",
    "BBL / 2 / STAR",
    "JE BANDE / 13 / VITE",
    "BANDE / 13 / VITE",
    "Enfants / 2 / LA CAF",
    "Fils / 2 / PUTE",
    "DU RSA / 0 / RS3",
    "Pétase / 100 / Vice",
    "BAISE / 100 / CAPOTE",
    "ARRACHEUR / 2 / CHATTE",
    "LÂCHE / 2 / SEIN",
    "ARRACHEUR / 2 / CAVU",
    "Inshallah / 4 / Femmes",
    "GRATTEUR / 2 / LA CAF",
    "EN MANQUE / 2 / SEXE",
    "Non / 1 / Posable",
    "Récolteur / 2 / Snap",
    "Bouffeur / 2 / Cul",
    "ANTI SUCEUR / 2 / BITE",
    "zgeg / 10 / proportionné",
    "Dévoreur / 2 / clitos",
    "Baiseur / 2 / Petasse",
    "Décaleur / 2 / strings",
    "À / 4 / PATTES",
    "DONNE TON / 06 / BEAUTÉ",
    "Mec / 13 / Haram",
    "PINEUR / 2 / CHEVRE",
    "Projet / 4 / Femmes",
    "Buveur / 2 / Vovo",
    "Kiffeur / 2 / Binouz",
    "CHERCHE / 1 / MILF",
    "SANS PRISE / 2 / TETE",
    "CASHFLOW / 13 / POSITIF",
    "ARRÊTE / 2 / PISTER",
    "et tié / 13 / séduisante",
    "Kaleur / 2 / Snus",
    "CHIBRE / 10 / PROPORTIONNEL",
    "CALVITIE / 13 / AVANCÉE",
    "T'as / 1 / Snap ?",
    "LÈCHEUR / 2 / TÉTON",
    "j'ai plus / 1 / EURO",
    "DÉREGLEUSE / 2 / MARCHÉ",
    "BAISEUR / 2 / LATINA",
    "Lécheur / 2 / Chatte",
    "Baiseur / 2 / Chatte",
    "Tranquilo / 2 / Quoi",
    "Donneuse / 2 / Go",
    "Nain / 2 / Jardin",
    "Remplis / 2 / Mosseba",
    "LECHEUR / 2 / TEUCH",
    "Uniquement / 2 / L'authentique",
    "CLAQUE / 2 / FESSES",
    "Baise / 100 / Capotes",
    "Lécheur / 2 / Teuch",
    "BBL / 2 / TASPÉ",
    "INCHALLAH / 1 / HOMME RICHE",
    "J'VEUX VOS / 4 / SNAP",
    "Mangeur / 2 / Bouzelouf",
    "T'AS PAS / 1 / SNAP BEAUTÉ ?",
    "J'ai pas / 2 / Daron",
    "InshaaAllah / 1 / RS6",
    "Je mérite / 1 / Bisous ?",
    "Elle a mal / 0 / Reins",
    "Trou / 2 / Balle",
    "Pro / 2 / DoroParty",
    "L'ex préfère / 2 / ta copine",
    "addict / 0 / locksé",
    "Montre / 1 / Sein",
    "JAMAIS / 100 / MON FLASH",
    "WALLAH / 7 / LOURD",
    "Kiffeuse / 2 / Fessées",
    "Décaleur / 2 / String",
    "Casse / 1 / Tour",
    "C'EST / 1 / BATARD",
    "PREPARATEUR / 2 / FLASH",
    "CHIBRE / 10 / PROPORTIONNE",
    "MANGEUR / 2 / CROUSTY",
    "C DES JALOUX / 2 / MON FILS",
    "BRISEUR / 2 / CARRIERE",
    "RIEN / 100 / RIEN",
    "CHUI LA / 7 / ANNEE",
    "REBEU / 10 / TINGUE",
    "CHERCHEUR / 2 / TRAVAIL",
    "PAS / 2 / COMME MOI",
    "SOUILLEUR / 2 / TEUCH",
    "AMOUR / 1 / POSSIBLE",
    "GRATTEUR / 2 / LA CAF",
    "MALIEN / 100 / EAU",
    "REMPLIS / 2 / MST",
    "BANDEUR / 2 / TISME",
    "BUVEUR / 2 / BIERE",
    "GRATTEUR / 2 / SNAPS",
    "PECHEUR / 2 / MILF",
    "CHERCHE / 1 / PLAN CUL",
    "TRAQUEUR / 2 / MERES PORTEUSES",
    "JAMAIS / 100 / MA PUFF",
    "BUVEUR / 2 / CYPRINE",
    "DOMPTEUR / 2 / MINEURES",
    "LECHEUR / 2 / TEUCH",
    "JE BAISE / 100 / CAPOTE",
    "BOURRE / 2 / TALENT",
    "J'AI ENVIE / 2 / CHIER",
    "ARRACHEUR / 2 / PERRUQUES",
    "DRIBBLEUR / 2 / KAFFIR",
    "BANDE / 2 / TIMP",
    "JE SUIS / 100 / PAPIERS",
    "JE PUE / 2 / FOU",
    "PAS / 2 / DARRON",
    "BANDEUR / 2 / MAGHREBINE"
]



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
    """Incrémente et retourne le prochain numéro de TikTok (thread-safe via lock)"""
    with _r2_lock:
        data = r2_get_json(KEY_COUNTER) or {"next": 1}
        num = data["next"]
        data["next"] = num + 1
        r2_put_json(KEY_COUNTER, data)
        return num

# ── Logs persistants sur R2 ────────────────────────────────────────────────
def log_generation(user, success):
    with _log_lock:
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
DEFAULT_MAIN_ACCOUNT = "Volakits Main (wael)"
ALL_ACCOUNTS = list(METRICOOL_ACCOUNTS.keys())

def get_accounts():
    """Retourne les comptes depuis METRICOOL_ACCOUNTS (source unique de vérité)"""
    return {
        "main": DEFAULT_MAIN_ACCOUNT,
        "others": [k for k in METRICOOL_ACCOUNTS if k != DEFAULT_MAIN_ACCOUNT],
        "available": list(METRICOOL_ACCOUNTS.keys())
    }

def save_accounts(data):
    return r2_put_json(KEY_ACCOUNTS, data)

# ── Index des créneaux utilisés (évite de re-scanner tous les TikToks programmés) ──
def get_used_slots_index():
    """Retourne {account: [scheduled_at, ...]} — un seul appel R2 au lieu de N.
    Si l'index n'existe pas encore (migration), il est reconstruit une seule fois."""
    idx = r2_get_json(KEY_USED_SLOTS)
    if idx is None:
        idx = rebuild_used_slots_index()
    return idx

def add_used_slot(account, dt_str):
    idx = get_used_slots_index()
    idx.setdefault(account, [])
    if dt_str not in idx[account]:
        idx[account].append(dt_str)
    r2_put_json(KEY_USED_SLOTS, idx)

def remove_used_slot(account, dt_str):
    idx = get_used_slots_index()
    if account in idx and dt_str in idx[account]:
        idx[account].remove(dt_str)
        r2_put_json(KEY_USED_SLOTS, idx)

def rebuild_used_slots_index():
    """Reconstruit l'index depuis R2 (utile si l'index se désynchronise) — scan complet, à usage rare"""
    idx = {}
    sched_keys = r2_list_keys(PFX_SCHEDULED)
    for sk in sched_keys:
        if "/imgs/" in sk: continue
        sd = r2_get_json(sk)
        if sd and sd.get("account") and sd.get("scheduled_at"):
            idx.setdefault(sd["account"], []).append(sd["scheduled_at"])
    r2_put_json(KEY_USED_SLOTS, idx)
    return idx

# ── Buffer persistant R2 (thread-safe) ────────────────────────────────────
_buffer_lock = threading.Lock()
_schedule_lock = threading.Lock()

def get_buffer():
    data = r2_get_json(KEY_BUFFER)
    if data:
        # Normaliser le format peu importe la version stockée
        if "images_b64" not in data:
            data["images_b64"] = []
        if "flockages" not in data:
            data["flockages"] = []
        if "user" not in data:
            data["user"] = None
        return data
    return {"images_b64": [], "flockages": [], "user": None}

def _save_buffer(buf):
    return r2_put_json(KEY_BUFFER, buf)

def add_to_buffer_and_create_tiktoks(new_images_b64, new_flockages, user, new_template_keys=None):
    # Phase 1 : mettre à jour le buffer sous lock (rapide)
    with _buffer_lock:
        buf = get_buffer()
        if not buf.get("user"):
            buf["user"] = user
        buf["images_b64"].extend(new_images_b64)
        buf["flockages"].extend(new_flockages)
        if "template_keys" not in buf: buf["template_keys"] = []
        buf["template_keys"].extend(new_template_keys if new_template_keys else [""] * len(new_images_b64))
        print(f"[BUFFER] Now has {len(buf['images_b64'])} images")
        # Extraire les batches à créer
        batches = []
        buf_user = buf["user"]
        while len(buf["images_b64"]) >= TIKTOK_SIZE:
            batch_b64  = buf["images_b64"][:TIKTOK_SIZE]
            batch_floc = buf["flockages"][:TIKTOK_SIZE]
            batch_tkeys = buf.get("template_keys", [])[:TIKTOK_SIZE]
            tiktok_num = get_next_tiktok_number()
            batches.append((tiktok_num, batch_b64, batch_floc, batch_tkeys))
            buf["images_b64"] = buf["images_b64"][TIKTOK_SIZE:]
            buf["flockages"]  = buf["flockages"][TIKTOK_SIZE:]
            if "template_keys" in buf: buf["template_keys"] = buf["template_keys"][TIKTOK_SIZE:]
        remaining = len(buf["images_b64"])
        _save_buffer(buf)

    # Phase 2 : sauvegarder les TikToks HORS du lock (appels R2 lents)
    created = []
    for tiktok_num, batch_b64, batch_floc, batch_tkeys in batches:
        print(f"[BUFFER] Creating TikTok {tiktok_num}...")
        _save_tiktok(tiktok_num, batch_b64, buf_user, batch_floc, batch_tkeys)
        created.append(tiktok_num)

    print(f"[BUFFER] Done — {len(created)} TikToks created, {remaining} pending")
    return created, remaining


# ── TikTok queue ───────────────────────────────────────────────────────────
def schedule_metricool(image_urls, caption, publish_time_iso, blog_id, timezone="Europe/Paris"):
    """Programme un post TikTok via l'API Metricool"""
    if not METRICOOL_TOKEN:
        return {"success": False, "error": "METRICOOL_TOKEN manquant"}
    
    # Normaliser les images (Metricool requiert des mediaId)
    media_ids = []
    for i, url in enumerate(image_urls):
        try:
            # Télécharger l'image depuis R2
            img_resp = requests.get(url, timeout=30)
            if img_resp.status_code != 200:
                print(f"[METRICOOL] Erreur téléchargement image {i+1}: {img_resp.status_code}")
                continue
            
            # Convertir en JPEG (TikTok n'accepte pas PNG)
            try:
                from PIL import Image
                import io
                img = Image.open(io.BytesIO(img_resp.content)).convert("RGB")
                buf = io.BytesIO()
                img.save(buf, format="JPEG", quality=95)
                img_bytes = buf.getvalue()
                mime = "image/jpeg"
                ext = "jpg"
            except Exception:
                img_bytes = img_resp.content
                mime = "image/jpeg"
                ext = "jpg"
            
            # Uploader sur les serveurs Metricool
            files = {"picture": (f"image_{i+1}.{ext}", img_bytes, mime)}
            data = {"userId": METRICOOL_USER_ID, "blogId": blog_id}
            upload_resp = requests.post(
                f"https://app.metricool.com/api/utils/upload",
                headers={"X-Mc-Auth": METRICOOL_TOKEN},
                files=files,
                data=data,
                timeout=60
            )
            print(f"[METRICOOL] Upload image {i+1}: status={upload_resp.status_code} resp={upload_resp.text[:200]}")
            if upload_resp.status_code == 200:
                resp_text = upload_resp.text.strip()
                if resp_text.startswith("http"):
                    media_ids.append(resp_text)
                else:
                    try:
                        d = upload_resp.json()
                        media_url = d.get("url") or d.get("mediaUrl") or d.get("fileUrl")
                        if media_url:
                            media_ids.append(media_url)
                    except Exception:
                        pass
        except Exception as e:
            print(f"[METRICOOL] Erreur upload image {i+1}: {e}")
    
    if not media_ids:
        return {"success": False, "error": "Aucune image normalisée"}
    
    # Créer le post schedulé avec le bon format Metricool
    print(f"[METRICOOL] Payload media: {media_ids[:2]}")
    payload = {
        "publicationDate": {
            "dateTime": publish_time_iso,
            "timezone": timezone
        },
        "text": caption,
        "firstCommentText": "",
        "providers": [{"network": "tiktok"}],
        "media": media_ids,
        "mediaAltText": [None] * len(media_ids),
        "autoPublish": True,
        "shortener": False,
        "draft": False,
        "hasNotReadNotes": False,
        "tiktokData": {
            "disableComment": False,
            "disableDuet": False,
            "disableStitch": False,
            "autoAddMusic": True,
            "privacyOption": "public_to_everyone",
            "photoCoverIndex": 0,
            "isAigc": False,
            "commercialContentOwnBrand": False,
            "commercialContentThirdParty": False
        }
    }
    
    try:
        resp = requests.post(
            f"https://app.metricool.com/api/v2/scheduler/posts?userId={METRICOOL_USER_ID}&blogId={blog_id}",
            headers={"X-Mc-Auth": METRICOOL_TOKEN, "Content-Type": "application/json"},
            json=payload,
            timeout=60
        )
        print(f"[METRICOOL] Response {resp.status_code}: {resp.text[:300]}")
        if resp.status_code in (200, 201):
            data = resp.json()
            post_id = data.get("id") or data.get("postId") or (data.get("data") or {}).get("id")
            return {"success": True, "post_id": post_id}
        else:
            return {"success": False, "error": resp.text[:200]}
    except Exception as e:
        return {"success": False, "error": str(e)}

def get_pepites_count_per_tiktok(n_tiktoks, n_pepites):
    """Calcule combien d'images pépites par TikTok (min 1, max 4)"""
    if n_tiktoks == 0 or n_pepites == 0:
        return 0
    raw = n_pepites / n_tiktoks
    return max(1, min(4, round(raw)))

# ── Tirage sans remise pour les pépites ─────────────────────────────────
_pepites_deck_lock = threading.Lock()
_pepites_deck_mem = []      # paquet en mémoire — évite appels R2 à chaque TikTok
_normaux_cache = []         # liste normaux en cache mémoire
_used_templates_lock = threading.Lock()
_used_templates_cache = []  # buffer des templates utilisées (flush toutes les 10)

def _load_flocages_cache():
    """Charge les listes depuis R2 une seule fois au démarrage/reset"""
    global _pepites_deck_mem, _normaux_cache
    import random as _random
    try:
        floc_data = r2_get_json("meta/flocages.json") or {}
        pepites_list = floc_data.get("pepites", PEPITE_FLOCAGES)[:]
        all_flocs = floc_data.get("flocages", DEFAULT_FLOCAGES)
        # Comparaison insensible à la casse
        pepites_lower = {p.lower().strip() for p in pepites_list}
        normaux = [f for f in all_flocs if f.lower().strip() not in pepites_lower]
    except Exception:
        pepites_list = PEPITE_FLOCAGES[:]
        pepites_lower = {p.lower().strip() for p in pepites_list}
        normaux = [f for f in DEFAULT_FLOCAGES if f.lower().strip() not in pepites_lower]
    # Charger le paquet restant depuis R2 si disponible
    try:
        deck_data = r2_get_json("meta/pepites_deck.json") or {}
        remaining = deck_data.get("remaining", [])
        if remaining:
            _pepites_deck_mem = remaining
        else:
            _random.shuffle(pepites_list)
            _pepites_deck_mem = pepites_list
    except Exception:
        _random.shuffle(pepites_list)
        _pepites_deck_mem = pepites_list
    _normaux_cache = normaux
    print(f"[DECK] Cache chargé: {len(_pepites_deck_mem)} pépites restantes, {len(_normaux_cache)} normaux")

def _draw_pepites(n=4):
    """Tire n pépites sans remise depuis le paquet en mémoire"""
    import random as _random
    global _pepites_deck_mem
    with _pepites_deck_lock:
        if not _pepites_deck_mem:
            _load_flocages_cache()
        # Si encore insuffisant après chargement, remélanges
        if len(_pepites_deck_mem) < n:
            try:
                floc_data = r2_get_json("meta/flocages.json") or {}
                full_list = floc_data.get("pepites", PEPITE_FLOCAGES)[:]
            except Exception:
                full_list = PEPITE_FLOCAGES[:]
            _random.shuffle(full_list)
            _pepites_deck_mem = full_list
            print(f"[DECK] Paquet remelange: {len(_pepites_deck_mem)} pépites")
        chosen = _pepites_deck_mem[:n]
        _pepites_deck_mem = _pepites_deck_mem[n:]
        # Sauvegarder en R2 de façon asynchrone seulement tous les 10 tirages
        if len(_pepites_deck_mem) % 10 == 0:
            try:
                r2_put_json("meta/pepites_deck.json", {"remaining": _pepites_deck_mem})
            except Exception:
                pass
        return chosen

def _draw_normaux(n=3):
    """Tire n flocages normaux depuis le cache mémoire"""
    import random as _random
    global _normaux_cache
    if not _normaux_cache:
        _load_flocages_cache()
    if not _normaux_cache:
        return []
    return _random.sample(_normaux_cache, min(n, len(_normaux_cache)))

def _save_tiktok(num, images_b64, user, flockages=None, template_keys=None):
    r2 = get_r2()
    if not r2: return False

    # ── Réordonner les flocages : pépites en premier (4 max), normaux ensuite ──
    # Les flocages ont été tirés individuellement dans gemini_one
    import random as _rnd
    try:
        floc_data = r2_get_json("meta/flocages.json") or {}
        pepites_raw = floc_data.get("pepites", PEPITE_FLOCAGES)
        # Comparaison insensible à la casse
        pepites_set_lower = {p.lower().strip() for p in pepites_raw}
    except Exception:
        pepites_set_lower = {p.lower().strip() for p in PEPITE_FLOCAGES}

    provided = [f for f in (flockages or []) if f]
    if provided:
        # Réordonner : pépites d'abord, normaux ensuite (comparaison insensible casse)
        pepites_in = [f for f in provided if f.lower().strip() in pepites_set_lower]
        normaux_in = [f for f in provided if f.lower().strip() not in pepites_set_lower]
        final_flockages = pepites_in + normaux_in
    else:
        # Fallback si aucun flocage fourni
        pepites_chosen = _draw_pepites(4)
        normaux_chosen = _draw_normaux(3)
        final_flockages = pepites_chosen + normaux_chosen
    print(f"[TIKTOK {num}] {len(final_flockages)} flocages: {final_flockages[:2]}...")

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
        "flockages": final_flockages,
        "template_keys": template_keys or [],
        "status": "pending",
        "account": None,
        "scheduled_at": None,
    }
    return r2_put_json(f"{PFX_QUEUE}tiktok_{num:04d}.json", meta)

def _enrich_tiktok(data, key, with_images=True):
    """Ajoute les URLs signées et la clé R2. Cache l'URL avec une expiration longue."""
    data["r2_key"] = key
    if with_images:
        data["image_urls"] = [r2_presigned(k, expires=604800) for k in data.get("image_keys", [])]  # 7 jours
    else:
        data["image_urls"] = []
    return data

def get_all_queue_light():
    """Récupère tous les TikToks de la queue SANS générer les URLs images (rapide, pour dispatch/schedule)"""
    keys = sorted(r2_list_keys(PFX_QUEUE))
    keys = [k for k in keys if "/imgs/" not in k]
    result = []
    for k in keys:
        d = r2_get_json(k)
        if d:
            d["r2_key"] = k
            result.append(d)
    return result

def get_queue(page=0, per_page=20):
    keys = sorted(r2_list_keys(PFX_QUEUE))
    keys = [k for k in keys if "/imgs/" not in k]
    total = len(keys)
    start = page * per_page
    page_keys = keys[start:start + per_page]
    result = []
    for k in page_keys:
        d = r2_get_json(k)
        if d: result.append(_enrich_tiktok(d, k))
    return result, total

def get_scheduled(page=0, per_page=20):
    keys = sorted(r2_list_keys(PFX_SCHEDULED), reverse=True)
    keys = [k for k in keys if "/imgs/" not in k][:200]
    total = len(keys)
    start = page * per_page
    page_keys = keys[start:start + per_page]
    result = []
    for k in page_keys:
        d = r2_get_json(k)
        if d: result.append(_enrich_tiktok(d, k))
    return result, total

def move_to_scheduled(queue_key, account, dt_str, robinreach_post_id=None, metricool_post_id=None):
    print(f"[MOVE] Moving {queue_key} -> scheduled, account={account}, dt={dt_str}")
    data = r2_get_json(queue_key)
    if not data:
        print(f"[MOVE] ❌ TikTok introuvable: {queue_key}")
        return False
    data["status"] = "scheduled"
    data["account"] = account
    data["scheduled_at"] = dt_str
    data["robinreach_post_id"] = robinreach_post_id
    data["metricool_post_id"] = metricool_post_id
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
def build_prompt_v2(name, number, name_below=None):
    """Prompt pour la variante flat lay (maillot à plat sur table avec boîte)"""
    name_below = (name_below or name).strip().upper()
    parts = [
        f"This is a flat lay product photo of a jersey on a table with a gift box.",
        f"Keep the exact same angle, lighting, blue background, composition and table surface.",
        f"Replace the existing jersey text/flocking with: top line '{name.upper()}', number '{number}', bottom line '{name_below}'.",
        f"Replace any existing gift box with a Volakits branded black gift box with 'VOLA KITS.' logo and a black ribbon bow, positioned in the same location as the original box.",
        f"Remove any visible price tags, hang tags, QR codes or labels on the jersey.",
        f"Keep ALL else identical: jersey shape, colors, texture, pattern, table surface, shadows, lighting.",
        f"Only change the text/flocking on the jersey and replace the gift box with Volakits box."
    ]
    return " ".join(parts)

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
    parts.append("Remove any visible tags, labels, stickers or QR codes on the jersey (hang tags, price tags, brand tags). Keep ALL else identical: colors, texture, pattern, outlines, lighting, shadows, background. Only swap text content and remove tags.")
    return " ".join(parts)

# Cache de la boîte référence en mémoire (chargée une fois depuis R2)
_box_ref_cache = None

def get_box_ref_b64():
    """Retourne la photo de la boîte Volakits en base64 (cache mémoire)"""
    global _box_ref_cache
    if _box_ref_cache:
        return _box_ref_cache
    # Essayer le fichier b64 pré-encodé en priorité
    for path in ["/app/static/volakits_box_ref_b64.txt", "static/volakits_box_ref_b64.txt"]:
        try:
            with open(path, "r") as f:
                _box_ref_cache = f.read().strip()
                if _box_ref_cache:
                    print("[BOX REF] Chargé depuis fichier local")
                    return _box_ref_cache
        except Exception:
            pass
    # Fallback R2
    try:
        r2 = get_r2()
        if r2:
            obj = r2.get_object(Bucket=R2_BUCKET, Key=KEY_BOX_REF)
            _box_ref_cache = base64.b64encode(obj["Body"].read()).decode()
            print("[BOX REF] Chargé depuis R2")
            return _box_ref_cache
    except Exception:
        pass
    print("[BOX REF] ⚠️ Référence boîte introuvable")
    return None

def call_gemini_only(img_bytes, mime, name, number, name_below=None, max_retries=5, prompt_fn=None):
    """Phase 1 : génère l'image avec Gemini uniquement, sans upscaling"""
    img_b64 = base64.b64encode(img_bytes).decode()
    if prompt_fn:
        prompt = prompt_fn(name, number, name_below)
    else:
        prompt = build_prompt(name, number, name_below)
    
    # Pour v2: envoyer aussi la photo de référence de la boîte Volakits
    parts = [{"text": prompt}, {"inline_data": {"mime_type": mime, "data": img_b64}}]
    if prompt_fn == build_prompt_v2:
        box_b64 = get_box_ref_b64()
        if box_b64:
            parts.append({"text": "This is the Volakits box reference image — use this exact box design to replace the competitor's box in the main image:"})
            parts.append({"inline_data": {"mime_type": "image/png", "data": box_b64}})
    
    payload = {"contents": [{"parts": parts}]}
    last_error = None
    for attempt_num in range(max_retries + 1):
        try:
            resp = requests.post(MODEL_URL,
                headers={"x-goog-api-key": API_KEY, "Content-Type": "application/json"},
                json=payload, timeout=120)
        except requests.RequestException as e:
            last_error = f"Erreur réseau: {e}"
            time.sleep(min(3 * (attempt_num + 1), 15))
            continue
        if resp.status_code != 200:
            last_error = f"API {resp.status_code}: {resp.text[:200]}"
            if resp.status_code in (503, 429):
                wait = min(4 * (attempt_num + 1), 30)
                print(f"[GEMINI] {resp.status_code} — retry dans {wait}s ({attempt_num+1}/{max_retries})")
                time.sleep(wait)
            else:
                time.sleep(1)
            continue
        data = resp.json()
        try:
            for part in data["candidates"][0]["content"]["parts"]:
                if "inlineData" in part:
                    return {"success": True, "image": part["inlineData"]["data"]}
            last_error = "Pas d'image dans la réponse."
        except (KeyError, IndexError) as e:
            last_error = f"Réponse inattendue: {e}"
    return {"success": False, "error": last_error}

def upscale_image(img_b64):
    """Upscale 4K via Real-ESRGAN (modèle privé Replicate)"""
    if not REPLICATE_API_KEY:
        print("[UPSCALE] ⚠️ REPLICATE_API_KEY manquant — image non upscalée")
        return img_b64
    
    MAX_UPSCALE_ATTEMPTS = 30
    attempt = 0
    while attempt < MAX_UPSCALE_ATTEMPTS:
        attempt += 1
        try:
            print(f"[UPSCALE] Tentative {attempt}/{MAX_UPSCALE_ATTEMPTS}...")
            r = requests.post(
                "https://api.replicate.com/v1/predictions",
                headers={"Authorization": f"Bearer {REPLICATE_API_KEY}", "Content-Type": "application/json", "Prefer": "wait"},
                json={"version": "4fa021de8b0fa096ef5b4a541c2f6160d9a6d4c5dab499175e8179122d36aadb", "input": {"image": img_b64}},
                timeout=300
            )
            if r.status_code in (200, 201, 202):
                data_r = r.json()
                output = data_r.get("output")
                if not output:
                    pid = data_r.get("id")
                    for _ in range(60):
                        time.sleep(2)
                        p = requests.get(f"https://api.replicate.com/v1/predictions/{pid}",
                            headers={"Authorization": f"Bearer {REPLICATE_API_KEY}"}, timeout=30).json()
                        if p.get("status") == "succeeded" and p.get("output"):
                            output = p["output"]; break
                        elif p.get("status") in ("failed", "canceled"):
                            break
                if output:
                    if isinstance(output, str) and output.startswith("http"):
                        img_4k = base64.b64encode(requests.get(output, timeout=60).content).decode()
                    elif isinstance(output, str):
                        img_4k = output
                    else:
                        img_4k = output[0] if isinstance(output, list) else output
                    print("[UPSCALE] ✅ 4K Real-ESRGAN")
                    return img_4k
                else:
                    print(f"[UPSCALE] Pas d'output, retry {attempt}...")
                    time.sleep(3)
            else:
                wait = min(3 * attempt, 30) if r.status_code == 429 else min(2 * attempt, 20)
                print(f"[UPSCALE] Erreur {r.status_code}, retry dans {wait}s...")
                time.sleep(wait)
        except Exception as e:
            wait = min(2 * attempt, 20)
            print(f"[UPSCALE] Erreur: {e}, retry dans {wait}s...")
            time.sleep(wait)
    print(f"[UPSCALE] ❌ Échec après {MAX_UPSCALE_ATTEMPTS} tentatives")
    return None
def call_gemini(img_bytes, mime, name, number, name_below=None, max_retries=5, resolution="1k"):
    """Compatibilité — génère et upscale en une fois (pour generate_single)"""
    result = call_gemini_only(img_bytes, mime, name, number, name_below, max_retries)
    if not result["success"]:
        return result
    upscaled = upscale_image(result["image"])
    if upscaled:
        result["image"] = upscaled
    return result

# ── Pages ──────────────────────────────────────────────────────────────────
@app.route("/")
def index(): return render_template("index.html")

@app.route("/queue")
def queue_page(): return render_template("queue.html")

@app.route("/scheduled")
def scheduled_page(): return render_template("scheduled.html")

@app.route("/templates")
def templates_page(): return render_template("templates.html")

@app.route("/generator2")
def page_generator2(): return render_template("generator2.html")

@app.route("/templates2")
def page_templates2(): return render_template("templates2.html")

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

    # Récupérer les vraies sessions récentes depuis R2
    session_keys = sorted(r2_list_keys("sessions/"), reverse=True)[:10]
    recent_sessions = []
    for sk in session_keys:
        s = r2_get_json(sk)
        if s: recent_sessions.append(s)

    return render_template("dashboard.html",
        today_count=today_count,
        today_success=sum(1 for e in today_e if e.get("success")),
        today_cost=round(today_count*COST_PER_IMAGE,2),
        tiktoks_done=today_count//7, tiktoks_goal=20,
        by_user=by_user, recent_sessions=recent_sessions,
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
    data["available"] = list(METRICOOL_ACCOUNTS.keys())
    return jsonify(data)

@app.route("/api/accounts", methods=["POST"])
def api_save_accounts():
    data = request.json
    save_accounts({"main": data.get("main",""), "others": data.get("others",[])})
    return jsonify({"success": True})

# ── API Queue ───────────────────────────────────────────────────────────────
@app.route("/api/queue")
def api_queue():
    page = int(request.args.get("page", 0))
    per_page = int(request.args.get("per_page", 20))
    tiktoks, total = get_queue(page=page, per_page=per_page)
    return jsonify({"tiktoks": tiktoks, "total": total, "page": page, "per_page": per_page})

@app.route("/api/queue/all")
def api_queue_all():
    """Retourne tous les TikToks en une seule requête — pour le cache frontend"""
    keys = sorted(r2_list_keys(PFX_QUEUE))
    keys = [k for k in keys if "/imgs/" not in k]
    result = []
    for k in keys:
        d = r2_get_json(k)
        if d:
            d["r2_key"] = k
            # Retourner seulement la première image pour la thumbnail
            img_keys = d.get("image_keys", [])
            d["thumb_url"] = r2_presigned(img_keys[0], expires=604800) if img_keys else None
            d["image_count"] = len(img_keys)
            d["image_urls"] = []  # pas les URLs complètes — chargées à la demande
            result.append(d)
    return jsonify({"tiktoks": result, "total": len(result)})

@app.route("/api/scheduled")
def api_scheduled():
    page = int(request.args.get("page", 0))
    per_page = int(request.args.get("per_page", 20))
    tiktoks, total = get_scheduled(page=page, per_page=per_page)
    return jsonify({"tiktoks": tiktoks, "total": total, "page": page, "per_page": per_page})

@app.route("/api/queue/assign", methods=["POST"])
def api_assign():
    data = request.json; key = data.get("key"); account = data.get("account")
    if not key: return jsonify({"error":"key requis"}),400
    t = r2_get_json(key)
    if not t: return jsonify({"error":"introuvable"}),404
    t["account"] = account
    r2_put_json(key, t)
    return jsonify({"success": True})

@app.route("/api/queue/unassign_all", methods=["POST"])
def api_unassign_all():
    """Désassigne tous les TikToks en une seule passe R2"""
    keys = sorted(r2_list_keys(PFX_QUEUE))
    keys = [k for k in keys if "/imgs/" not in k]
    done = 0
    for k in keys:
        t = r2_get_json(k)
        if t and t.get("account"):
            t["account"] = None
            r2_put_json(k, t)
            done += 1
    return jsonify({"success": True, "unassigned": done})

@app.route("/api/queue/assign_batch", methods=["POST"])
def api_assign_batch():
    """Assigne plusieurs TikToks d'un coup — évite les 100 appels R2 séquentiels"""
    data = request.json or {}
    assignments = data.get("assignments", [])
    if not assignments: return jsonify({"error": "assignments requis"}), 400
    done = 0
    for item in assignments:
        key = item.get("key")
        account = item.get("account")
        if not key: continue
        t = r2_get_json(key)
        if not t: continue
        t["account"] = account
        r2_put_json(key, t)
        done += 1
    return jsonify({"success": True, "updated": done})

@app.route("/api/queue/dispatch_smart", methods=["POST"])
def api_dispatch_smart():
    """Auto-dispatch intelligent — répartit les TikToks proportionnellement aux créneaux de chaque compte"""
    r2 = get_r2()
    if not r2: return jsonify({"error": "R2 non configuré"}), 500
    
    # Calculer le ratio de créneaux par compte
    all_accounts = list(METRICOOL_ACCOUNTS.keys())
    total_slots = sum(len(get_schedule_times_for_account(a)) for a in all_accounts)
    
    # Charger tous les TikToks en attente non assignés
    keys = sorted(r2_list_keys(PFX_QUEUE))
    keys = [k for k in keys if "/imgs/" not in k]
    pending = []
    for k in keys:
        t = r2_get_json(k)
        if t and t.get("status") == "pending" and not t.get("account"):
            pending.append((k, t))
    
    if not pending:
        return jsonify({"success": True, "count": 0, "by_account": {}, "message": "Aucun TikTok non assigné"})
    
    # Répartir proportionnellement
    assignments = {}
    for acc in all_accounts:
        ratio = len(get_schedule_times_for_account(acc)) / total_slots
        assignments[acc] = max(1, round(len(pending) * ratio))
    
    # Ajuster pour avoir exactement len(pending) assignments
    total_assigned = sum(assignments.values())
    diff = len(pending) - total_assigned
    if diff > 0:
        assignments[all_accounts[0]] += diff
    elif diff < 0:
        assignments[all_accounts[0]] = max(0, assignments[all_accounts[0]] + diff)
    
    # Assigner les TikToks
    idx = 0
    by_account = {}
    batch = {}
    for acc in all_accounts:
        count = assignments[acc]
        for i in range(count):
            if idx >= len(pending): break
            key, tiktok = pending[idx]
            tiktok["account"] = acc
            batch[key] = acc
            by_account[acc] = by_account.get(acc, 0) + 1
            idx += 1
    
    # Sauvegarder les assignations (pending_dict pour accès O(1))
    pending_dict = {k: t for k, t in pending}
    if batch:
        for key, acc in batch.items():
            t = pending_dict.get(key)
            if t:
                t["account"] = acc
                r2_put_json(key, t)
    
    return jsonify({"success": True, "count": idx, "by_account": by_account})

@app.route("/api/queue/dispatch", methods=["POST"])
def api_dispatch():
    data = request.json; accounts = data.get("accounts",[])
    if not accounts: return jsonify({"error":"Aucun compte"}),400
    queue = get_all_queue_light()
    unassigned = [t for t in queue if not t.get("account")]
    for i,t in enumerate(unassigned):
        acc = accounts[i % len(accounts)]
        t["account"] = acc
        r2_put_json(t["r2_key"], {**t, "image_urls": None, "r2_key": None, "account": acc})
    return jsonify({"success":True,"count":len(unassigned)})

_schedule_result = {}

@app.route("/api/queue/schedule", methods=["POST"])
def api_schedule():
    if not _schedule_lock.acquire(blocking=False):
        return jsonify({"error": "Une programmation est déjà en cours, réessaie dans quelques secondes."}), 429
    
    data = request.json or {}
    _schedule_result["last"] = {"pending": True}  # reset avant de lancer
    
    def run_schedule():
        try:
            with app.app_context():
                result = _do_schedule_data(data)
                # Stocker le dict, pas la Response Flask
                if isinstance(result, tuple):
                    resp_obj, status = result[0], result[1]
                    content = resp_obj.get_json()
                    print(f"[SCHEDULE] Résultat tuple status={status}: {content}")
                    _schedule_result["last"] = content if status == 200 else {"scheduled": 0, "errors": [str(content)]}
                elif hasattr(result, 'get_json'):
                    _schedule_result["last"] = result.get_json()
                else:
                    _schedule_result["last"] = result
                print(f"[SCHEDULE] Résultat final stocké: {_schedule_result['last']}")
        except Exception as e:
            import traceback
            print(f"[SCHEDULE] ❌ Exception dans run_schedule: {e}")
            traceback.print_exc()
            _schedule_result["last"] = {"scheduled": 0, "errors": [str(e)]}
        finally:
            _schedule_lock.release()
    
    t = threading.Thread(target=run_schedule, daemon=True)
    t.start()
    return jsonify({"success": True, "message": "Programmation lancée en arrière-plan", "background": True})

@app.route("/api/queue/schedule/status")
def api_schedule_status():
    """Retourne le résultat de la dernière programmation"""
    result = _schedule_result.get("last")
    if result is None:
        return jsonify({"pending": True})
    # Si c'est un objet Response Flask, extraire les données
    if hasattr(result, 'get_json'):
        return result
    if hasattr(result, 'json'):
        return result
    return jsonify(result)

def _do_schedule_data(data=None):
    if data is None:
        data = request.json or {}
    start_date_str = data.get("start_date")
    custom_slots = data.get("custom_slots", {})
    single_key = data.get("single_key")  # programmer un seul TikTok

    queue = get_all_queue_light()
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
            from datetime import date as _date_cls
            start_date = _date_cls(y,mo,d)
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

    # ── Phase 1 : attribuer tous les créneaux en séquence (garanti sans doublons) ──
    # ── Phase 2 : envoyer à RobinReach + sauvegarder R2 en parallèle ──────────────
    jobs = []

    for account, tiktoks in by_account.items():
        metricool_blog_id = METRICOOL_ACCOUNTS.get(account, {}).get("blog_id")
        print(f"[SCHEDULE] account='{account}' metricool_blog_id={metricool_blog_id}")
        if not metricool_blog_id:
            for t in tiktoks:
                errors.append(f"Compte '{account}' non reconnu (TikTok {t.get('number','')})")
            continue

        used_slots_idx = get_used_slots_index()
        used_slots = set(used_slots_idx.get(account, []))
        slot_date = start_date
        slot_index = 0
        # Si tous les créneaux d'aujourd'hui sont pris, commencer demain
        account_times = get_schedule_times_for_account(account)
        today_slots = set()
        for ht in account_times:
            h2,m2 = map(int, ht.split(":"))
            sd = datetime(slot_date.year,slot_date.month,slot_date.day,h2,m2,tzinfo=timezone.utc).isoformat()
            today_slots.add(sd)
        if today_slots.issubset(used_slots):
            slot_date += timedelta(days=1)

        for tiktok in tiktoks:
            tiktok_data = r2_get_json(tiktok["r2_key"])
            if not tiktok_data:
                errors.append(f"TikTok {tiktok.get('number','')} introuvable")
                continue
            if tiktok_data.get("status") in ("scheduled", "scheduling"):
                # Si "scheduling" depuis plus de 10 min c'est bloqué — on reset
                if tiktok_data.get("status") == "scheduling":
                    status_ts = tiktok_data.get("scheduling_at","")
                    try:
                        ts = datetime.fromisoformat(status_ts)
                        if (datetime.now(timezone.utc) - ts).total_seconds() > 600:
                            tiktok_data["status"] = "pending"
                            r2_put_json(tiktok["r2_key"], tiktok_data)
                            print(f"[SCHEDULE] TikTok {tiktok.get('number','')} scheduling bloqué → reset")
                        else:
                            errors.append(f"TikTok {tiktok.get('number','')} déjà en cours, ignoré")
                            continue
                    except Exception:
                        tiktok_data["status"] = "pending"
                        r2_put_json(tiktok["r2_key"], tiktok_data)
                else:
                    errors.append(f"TikTok {tiktok.get('number','')} déjà programmé, ignoré")
                    continue
            # Marquer immédiatement comme "en cours" pour éviter les doublons
            tiktok_data["status"] = "scheduling"
            tiktok_data["scheduling_at"] = datetime.now(timezone.utc).isoformat()
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
                account_times = get_schedule_times_for_account(account)
                while True:
                    h,m = map(int, account_times[slot_index % len(account_times)].split(":"))
                    slot_dt = datetime(slot_date.year,slot_date.month,slot_date.day,h,m,tzinfo=timezone.utc)
                    slot_iso = slot_dt.isoformat()
                    is_future_enough = slot_dt > now + timedelta(minutes=30)
                    if is_future_enough and slot_iso not in used_slots:
                        break
                    slot_index += 1
                    if slot_index % len(account_times) == 0:
                        slot_date += timedelta(days=1)

            dt_str = slot_dt.isoformat()
            paris_dt = slot_dt.astimezone(paris_tz)
            display_time = paris_dt.strftime("%d/%m/%Y à %Hh%M")

            # Stocker le job pour traitement parallèle en Phase 2
            jobs.append({
                "tiktok": tiktok,
                "tiktok_data": tiktok_data,
                "account": account,
                "metricool_blog_id": metricool_blog_id,
                "dt_str": dt_str,
                "display_time": display_time,
            })
            used_slots.add(dt_str)
            add_used_slot(account, dt_str)

            if not use_custom:
                slot_index += 1
                if slot_index % len(account_times) == 0:
                    slot_date += timedelta(days=1)

    # ── Phase 2 : Metricool + R2 en parallèle ─────────────────────────────
    def process_schedule_job(job):
        tiktok = job["tiktok"]
        tiktok_data = job["tiktok_data"]
        account = job["account"]
        metricool_blog_id = job.get("metricool_blog_id")
        dt_str = job["dt_str"]
        display_time = job["display_time"]

        # Metricool uniquement
        metricool_account = METRICOOL_ACCOUNTS.get(account)
        print(f"[SCHEDULE DEBUG] account='{account}' metricool_account={metricool_account} token_ok={bool(METRICOOL_TOKEN)}")
        if metricool_account and metricool_account.get("active") and METRICOOL_TOKEN:
            try:
                # Utiliser URLs publiques R2 pour Metricool (les URLs présignées privées ne sont pas accessibles)
                image_urls = [f"{R2_PUBLIC_URL}/{k}" for k in tiktok.get("image_keys", []) if k]
                print(f"[METRICOOL] {len(image_urls)} images à envoyer pour TikTok {tiktok.get('number','')}")
                print(f"[METRICOOL] URL exemple: {image_urls[0][:100] if image_urls else 'AUCUNE'}")
                from zoneinfo import ZoneInfo
                paris_tz_local = ZoneInfo("Europe/Paris")
                dt_utc = datetime.fromisoformat(dt_str.replace("Z","")).replace(tzinfo=timezone.utc) if "Z" in dt_str else datetime.fromisoformat(dt_str)
                if dt_utc.tzinfo is None:
                    dt_utc = dt_utc.replace(tzinfo=timezone.utc)
                dt_paris = dt_utc.astimezone(paris_tz_local)
                publish_time_local = dt_paris.strftime("%Y-%m-%dT%H:%M:%S")
                print(f"[METRICOOL] Date: {publish_time_local}, blog_id: {metricool_account['blog_id']}")
                result = schedule_metricool(
                    image_urls=image_urls,
                    caption=FIXED_CAPTION,
                    publish_time_iso=publish_time_local,
                    blog_id=metricool_account["blog_id"]
                )
                print(f"[METRICOOL] Résultat: {result}")
                if not result["success"]:
                    return {"error": f"TikTok {tiktok.get('number','')}: {result['error']}"}
                tiktok_data["metricool_post_id"] = result.get("post_id")
                print(f"[METRICOOL] ✅ TikTok {tiktok.get('number','')} programmé")
            except Exception as e:
                import traceback
                print(f"[METRICOOL] ❌ Exception: {e}")
                traceback.print_exc()
                return {"error": f"TikTok {tiktok.get('number','')}: {str(e)}"}
        else:
            return {"error": f"TikTok {tiktok.get('number','')}: Compte '{account}' non configuré sur Metricool"}

        try:
            move_to_scheduled(tiktok["r2_key"], account, dt_str,
                None,
                tiktok_data.get("metricool_post_id"))
        except Exception as e:
            print(f"[SCHEDULE] move_to_scheduled error: {e}")

        return {"success": True, "tiktok": tiktok.get("number",""), "account": account, "time": display_time}

    # Lancer tous les jobs en parallèle (max 5 simultanés)
    with ThreadPoolExecutor(max_workers=5) as ex:
        results_jobs = list(ex.map(process_schedule_job, jobs))

    for r in results_jobs:
        if r.get("error"):
            errors.append(r["error"])
        else:
            scheduled_count += 1
            scheduled_details.append({"tiktok": r["tiktok"], "account": r["account"], "time": r["time"]})

    return jsonify({
        "success": True,
        "scheduled": scheduled_count,
        "details": scheduled_details,
        "errors": errors
    })

@app.route("/api/queue/tiktok")
def api_queue_tiktok():
    """Retourne les métadonnées complètes d'un TikTok (flockages, template_keys...)"""
    key = request.args.get("key")
    if not key: return jsonify({"error": "key requis"}), 400
    t = r2_get_json(key)
    if not t: return jsonify({"error": "introuvable"}), 404
    return jsonify(t)

@app.route("/api/queue/replace_image", methods=["POST"])
def api_replace_image():
    """Remplace une image dans un TikTok par une nouvelle version régénérée"""
    data = request.json or {}
    key = data.get("key")
    index = data.get("index")
    image_b64 = data.get("image")
    new_floc = data.get("new_floc")  # nouveau flocage optionnel
    if not key or index is None or not image_b64: return jsonify({"error": "key, index et image requis"}), 400
    t = r2_get_json(key)
    if not t: return jsonify({"error": "introuvable"}), 404
    img_keys = t.get("image_keys", [])
    if index < 0 or index >= len(img_keys): return jsonify({"error": "index invalide"}), 400
    try:
        r2_put_image(img_keys[index], base64.b64decode(image_b64))
        # Mettre à jour le flocage si modifié
        if new_floc:
            flockages = t.get("flockages", [])
            if index < len(flockages):
                flockages[index] = new_floc
            else:
                while len(flockages) <= index:
                    flockages.append("")
                flockages[index] = new_floc
            t["flockages"] = flockages
            r2_put_json(key, t)
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/queue/images")
def api_queue_images():
    """Retourne toutes les URLs signées d'un TikTok — appelé seulement quand l'user clique pour voir tout"""
    key = request.args.get("key")
    if not key: return jsonify({"error": "key requis"}), 400
    t = r2_get_json(key)
    if not t: return jsonify({"error": "introuvable"}), 404
    image_urls = [r2_presigned(k, expires=604800) for k in t.get("image_keys", [])]
    return jsonify({"image_urls": image_urls})

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

@app.route("/api/scheduled/check_status", methods=["POST"])
def api_check_status():
    """Vérifie le vrai statut de publication sur Metricool pour des TikToks donnés"""
    data = request.json or {}
    keys = data.get("keys", [])
    if not keys:
        return jsonify({"error": "keys requis"}), 400
    if not METRICOOL_TOKEN:
        return jsonify({"error": "METRICOOL_TOKEN non configuré"}), 400

    results = {}
    for key in keys:
        tiktok = r2_get_json(key)
        if not tiktok:
            results[key] = {"status": "introuvable"}
            continue
        
        tiktok_account = tiktok.get("account","")
        mc_acc = METRICOOL_ACCOUNTS.get(tiktok_account, {})
        metricool_id = tiktok.get("metricool_post_id")
        
        # Vérifier sur Metricool en priorité
        if metricool_id and mc_acc.get("active") and METRICOOL_TOKEN:
            try:
                resp = requests.get(
                    f"https://app.metricool.com/api/v2/posts/{metricool_id}?userId={METRICOOL_USER_ID}&blogId={mc_acc['blog_id']}",
                    headers={"X-Mc-Auth": METRICOOL_TOKEN}, timeout=10
                )
                if resp.status_code == 200:
                    d = resp.json()
                    real_status = d.get("status", "scheduled")
                    results[key] = {"status": real_status, "real_status": real_status}
                    tiktok["real_status"] = real_status
                    r2_put_json(key, tiktok)
                    continue
            except Exception as e:
                print(f"[CHECK_STATUS METRICOOL] {e}")
        
        results[key] = {"status": "pas_d_id_metricool"}

    return jsonify({"results": results})

@app.route("/api/scheduled/fix_slots", methods=["POST"])
def api_fix_slots():
    """Recalcule et corrige les créneaux de tous les TikToks programmés d'un compte"""
    data = request.json or {}
    account = data.get("account")
    if not account: return jsonify({"error": "account requis"}), 400

    from zoneinfo import ZoneInfo
    paris_tz = ZoneInfo("Europe/Paris")
    now = datetime.now(timezone.utc)

    # Récupérer tous les TikToks programmés de ce compte
    keys = r2_list_keys(PFX_SCHEDULED)
    keys = [k for k in keys if "/imgs/" not in k]
    tiktoks = []
    for k in keys:
        d = r2_get_json(k)
        if d and d.get("account") == account:
            tiktoks.append((k, d))

    if not tiktoks:
        return jsonify({"error": f"Aucun TikTok programmé pour {account}"}), 404

    # Trier par date de programmation actuelle
    tiktoks.sort(key=lambda x: x[1].get("scheduled_at", ""))

    # Recalculer les créneaux avec les bons horaires
    account_times = get_schedule_times_for_account(account)
    print(f"[FIX_SLOTS] {account}: {len(tiktoks)} TikToks, créneaux: {account_times}")

    # Réinitialiser les créneaux utilisés pour ce compte
    used_slots_idx = get_used_slots_index()
    # Supprimer les anciens créneaux de ce compte
    old_slots = set(used_slots_idx.get(account, []))

    slot_date = now.date()
    slot_index = 0
    used_slots = set()
    updated = 0
    errors = []

    def reschedule_one(item):
        nonlocal slot_date, slot_index, updated
        key, tiktok_data = item
        metricool_post_id_fix = tiktok_data.get("metricool_post_id")

        # Trouver le prochain créneau disponible
        while True:
            h, m = map(int, account_times[slot_index % len(account_times)].split(":"))
            slot_dt = datetime(slot_date.year, slot_date.month, slot_date.day, h, m, tzinfo=timezone.utc)
            slot_iso = slot_dt.isoformat()
            if slot_dt > now + timedelta(minutes=30) and slot_iso not in used_slots:
                break
            slot_index += 1
            if slot_index % len(account_times) == 0:
                slot_date = slot_date + timedelta(days=1)

        new_dt_str = slot_dt.isoformat()
        used_slots.add(new_dt_str)

        # Mettre à jour sur Metricool si applicable
        metricool_post_id_fix = tiktok_data.get("metricool_post_id")
        mc_acc_fix = METRICOOL_ACCOUNTS.get(account, {})
        if metricool_post_id_fix and mc_acc_fix.get("active") and METRICOOL_TOKEN:
            try:
                from zoneinfo import ZoneInfo
                paris_tz_fix = ZoneInfo("Europe/Paris")
                dt_utc_fix = datetime.fromisoformat(new_dt_str.replace("Z","")).replace(tzinfo=timezone.utc)
                dt_paris_fix = dt_utc_fix.astimezone(paris_tz_fix)
                new_dt_paris = dt_paris_fix.strftime("%Y-%m-%dT%H:%M:%S")
                resp = requests.patch(
                    f"https://app.metricool.com/api/v2/posts/{metricool_post_id_fix}?userId={METRICOOL_USER_ID}&blogId={mc_acc_fix['blog_id']}",
                    headers={"X-Mc-Auth": METRICOOL_TOKEN, "Content-Type": "application/json"},
                    json={"publicationDate": {"dateTime": new_dt_paris, "timezone": "Europe/Paris"}},
                    timeout=30
                )
                print(f"[FIX_SLOTS METRICOOL] Post {metricool_post_id_fix} → {new_dt_paris}: {resp.status_code}")
            except Exception as e:
                print(f"[FIX_SLOTS METRICOOL] Erreur: {e}")

        # RobinReach supprimé — Metricool uniquement (fix_slots)

        # Mettre à jour dans R2
        tiktok_data["scheduled_at"] = new_dt_str
        r2_put_json(key, tiktok_data)
        add_used_slot(account, new_dt_str)

        slot_index += 1
        if slot_index % len(account_times) == 0:
            slot_date = slot_date + timedelta(days=1)

    # Supprimer les anciens créneaux de l'index
    used_slots_idx[account] = []
    r2_put_json("meta/used_slots.json", used_slots_idx)

    for item in tiktoks:
        reschedule_one(item)
        updated += 1

    return jsonify({
        "success": True,
        "updated": updated,
        "errors": errors,
        "account": account,
        "slots_used": list(used_slots)[:5]
    })

@app.route("/api/scheduled/find_duplicates")
def api_find_duplicates():
    """Détecte les TikToks programmés au même horaire sur le même compte"""
    keys = r2_list_keys(PFX_SCHEDULED)
    keys = [k for k in keys if "/imgs/" not in k]
    slots = {}  # (account, scheduled_at) -> [keys]
    for k in keys:
        d = r2_get_json(k)
        if not d: continue
        acc = d.get("account")
        at = d.get("scheduled_at")
        if not acc or not at: continue
        slot_key = (acc, at)
        slots.setdefault(slot_key, []).append({"key": k, "number": d.get("number"), "id": d.get("id")})

    duplicates = []
    for (acc, at), items in slots.items():
        if len(items) > 1:
            duplicates.append({"account": acc, "scheduled_at": at, "tiktoks": items})

    return jsonify({"duplicates": duplicates, "count": len(duplicates)})

@app.route("/api/scheduled/recover_robinreach", methods=["POST"])
def api_recover_robinreach():
    """Remet dans la file d'attente tous les TikToks programmés via RobinReach (qui ont échoué)"""
    data = request.json or {}
    account_filter = data.get("account")  # optionnel — filtrer par compte
    
    keys = sorted(r2_list_keys(PFX_SCHEDULED))
    keys = [k for k in keys if "/imgs/" not in k]
    
    recovered = 0
    by_account = {}
    
    for sched_key in keys:
        tiktok = r2_get_json(sched_key)
        if not tiktok: continue
        
        account = tiktok.get("account", "")
        if account_filter and account != account_filter:
            continue
        
        # Remettre dans la file d'attente avec note de récupération
        queue_key = sched_key.replace(PFX_SCHEDULED, PFX_QUEUE)
        original_date = tiktok.get("scheduled_at", "")
        original_account = tiktok.get("account", "")
        tiktok["status"] = "pending"
        tiktok["account"] = None
        tiktok["scheduled_at"] = None
        tiktok["robinreach_post_id"] = None
        tiktok["metricool_post_id"] = None
        tiktok["recovered"] = True
        tiktok["recovered_at"] = datetime.now(timezone.utc).isoformat()
        tiktok["recovered_original_date"] = original_date
        tiktok["recovered_original_account"] = original_account
        
        # Copier vers queue/, supprimer de scheduled/
        r2_put_json(queue_key, tiktok)
        try:
            get_r2().delete_object(Bucket=R2_BUCKET, Key=sched_key)
        except Exception:
            pass
        
        by_account[account] = by_account.get(account, 0) + 1
        recovered += 1
    
    # Vider l'index des créneaux utilisés
    if recovered > 0:
        r2_put_json(KEY_USED_SLOTS, {})
    
    return jsonify({"success": True, "recovered": recovered, "by_account": by_account})

@app.route("/api/scheduled/unschedule", methods=["POST"])
def api_unschedule():
    """Remet des TikToks programmés dans la file d'attente ET supprime de Metricool"""
    data = request.json
    keys = data.get("keys", [])
    if not keys: return jsonify({"error": "keys requis"}), 400
    count = 0
    errors = []
    for sched_key in keys:
        tiktok = r2_get_json(sched_key)
        if not tiktok: continue

        metricool_post_id = tiktok.get("metricool_post_id")
        tiktok_account = tiktok.get("account", "")
        metricool_acc = METRICOOL_ACCOUNTS.get(tiktok_account, {})
        
        # Supprimer sur Metricool si applicable
        if metricool_post_id and metricool_acc.get("active") and METRICOOL_TOKEN:
            try:
                del_resp = requests.delete(
                    f"https://app.metricool.com/api/v2/scheduler/posts/{metricool_post_id}?userId={METRICOOL_USER_ID}&blogId={metricool_acc['blog_id']}",
                    headers={"X-Mc-Auth": METRICOOL_TOKEN},
                    timeout=15
                )
                print(f"[METRICOOL DELETE] Post {metricool_post_id}: {del_resp.status_code} {del_resp.text[:100]}")
            except Exception as e:
                print(f"[METRICOOL DELETE] Erreur: {e}")
        
        # RobinReach supprimé — Metricool uniquement

        # Libérer le créneau dans l'index pour qu'il redevienne disponible
        old_account = tiktok.get("account")
        old_slot = tiktok.get("scheduled_at")
        if old_account and old_slot:
            remove_used_slot(old_account, old_slot)

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
        tiktok["metricool_post_id"] = None
        queue_key = sched_key.replace(PFX_SCHEDULED, PFX_QUEUE)
        r2_put_json(queue_key, tiktok)
        r2_delete(sched_key)
        count += 1
    return jsonify({"success": True, "count": count, "errors": errors})

@app.route("/api/autopilot/plan", methods=["POST"])
def api_autopilot_plan():
    """Calcule combien de TikToks manquent pour remplir N jours sur tous les comptes"""
    data = request.json or {}
    days = int(data.get("days", 7))
    
    all_accounts = list(METRICOOL_ACCOUNTS.keys())
    now = datetime.now(timezone.utc)
    cutoff = now + timedelta(days=days)
    plan = {}
    total_needed = 0
    
    # Charger scheduled et queue une seule fois
    scheduled_keys = [k for k in r2_list_keys(PFX_SCHEDULED) if "/imgs/" not in k]
    queue_keys = [k for k in r2_list_keys(PFX_QUEUE) if "/imgs/" not in k]
    
    # Construire index par compte
    scheduled_by_acc = {}
    for k in scheduled_keys:
        t = r2_get_json(k)
        if not t: continue
        acc = t.get("account","")
        sat = t.get("scheduled_at","")
        if not sat: continue
        try:
            dt = datetime.fromisoformat(sat)
            if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
            if now <= dt <= cutoff:
                scheduled_by_acc[acc] = scheduled_by_acc.get(acc, 0) + 1
        except Exception: pass
    
    queue_by_acc = {}
    for k in queue_keys:
        t = r2_get_json(k)
        if not t: continue
        acc = t.get("account","")
        if acc: queue_by_acc[acc] = queue_by_acc.get(acc, 0) + 1
    
    for acc in all_accounts:
        slots_per_day = len(get_schedule_times_for_account(acc))
        tiktoks_needed = slots_per_day * days
        already_scheduled = scheduled_by_acc.get(acc, 0)
        in_queue = queue_by_acc.get(acc, 0)
        missing = max(0, tiktoks_needed - already_scheduled - in_queue)
        plan[acc] = {
            "needed": tiktoks_needed,
            "scheduled": already_scheduled,
            "in_queue": in_queue,
            "missing": missing,
            "images_to_generate": missing * TIKTOK_SIZE
        }
        total_needed += missing * TIKTOK_SIZE
    
    return jsonify({
        "days": days,
        "plan": plan,
        "total_tiktoks_missing": sum(v["missing"] for v in plan.values()),
        "total_images_to_generate": total_needed
    })

@app.route("/api/metricool/failed_posts")
def api_metricool_failed_posts():
    """Vérifie sur Metricool les posts qui auraient dû être publiés mais ne l'ont pas été"""
    if not METRICOOL_TOKEN:
        return jsonify({"failed": [], "error": "METRICOOL_TOKEN manquant"})
    
    failed = []
    
    for account, mc_acc in METRICOOL_ACCOUNTS.items():
        if not mc_acc.get("active"): continue
        try:
            resp = requests.get(
                f"https://app.metricool.com/api/v2/posts?userId={METRICOOL_USER_ID}&blogId={mc_acc['blog_id']}&status=failed&pageSize=50",
                headers={"X-Mc-Auth": METRICOOL_TOKEN},
                timeout=15
            )
            if resp.status_code == 200:
                data = resp.json()
                posts = data if isinstance(data, list) else data.get("posts", [])
                for post in posts:
                    failed.append({
                        "account": account,
                        "post_id": post.get("id"),
                        "scheduled_at": post.get("publicationDate", {}).get("dateTime") if isinstance(post.get("publicationDate"), dict) else post.get("publicationDate"),
                        "error": post.get("error") or post.get("errorMessage", "Erreur inconnue"),
                        "content": post.get("text","")[:100]
                    })
        except Exception as e:
            print(f"[FAILED_POSTS] {account}: {e}")
    
    return jsonify({"failed": failed, "count": len(failed)})

@app.route("/api/metricool/test")
def api_metricool_test():
    """Test endpoint Metricool — diagnostic upload"""
    TOKEN = METRICOOL_TOKEN
    USER_ID = METRICOOL_USER_ID
    BLOG_ID = "6542376"
    TEST_URLS = [
        "https://pub-2041419f649b434681cde993145feaee.r2.dev/queue/imgs/tiktok_0295_01.png",
        "https://pub-2041419f649b434681cde993145feaee.r2.dev/queue/imgs/tiktok_0295_02.png",
    ]
    
    media_urls = []
    upload_results = []
    
    for i, url in enumerate(TEST_URLS):
        try:
            img = requests.get(url, timeout=30)
            files = {"picture": (f"image_{i+1}.png", img.content, "image/png")}
            data = {"userId": USER_ID, "blogId": BLOG_ID}
            r = requests.post(
                f"https://app.metricool.com/api/utils/upload",
                headers={"X-Mc-Auth": TOKEN},
                files=files,
                data=data,
                timeout=60
            )
            upload_results.append({"status": r.status_code, "resp": r.text[:300]})
            if r.status_code == 200:
                resp_text = r.text.strip()
                if resp_text.startswith("http"):
                    media_urls.append(resp_text)
                else:
                    try:
                        d = r.json()
                        media_url = d.get("url") or d.get("mediaUrl") or str(d)
                        media_urls.append(media_url)
                    except Exception:
                        pass
        except Exception as e:
            upload_results.append({"error": str(e)})
    
    if not media_urls:
        return jsonify({"error": "Upload failed", "upload_results": upload_results})
    
    payload = {
        "publicationDate": {"dateTime": "2026-08-20T10:00:00", "timezone": "Europe/Paris"},
        "text": "Test bot images",
        "firstCommentText": "",
        "providers": [{"network": "tiktok"}],
        "media": media_urls,
        "mediaAltText": [None] * len(media_urls),
        "autoPublish": False,
        "shortener": False,
        "draft": False,
        "hasNotReadNotes": False,
        "tiktokData": {
            "disableComment": False,
            "disableDuet": False,
            "disableStitch": False,
            "autoAddMusic": True,
            "privacyOption": "public_to_everyone",
            "photoCoverIndex": 0
        }
    }
    resp = requests.post(
        f"https://app.metricool.com/api/v2/scheduler/posts?userId={USER_ID}&blogId={BLOG_ID}",
        headers={"X-Mc-Auth": TOKEN, "Content-Type": "application/json"},
        json=payload,
        timeout=60
    )
    
    return jsonify({
        "upload_results": upload_results,
        "media_urls": media_urls,
        "post_status": resp.status_code,
        "post_response": resp.text[:800],
    })

@app.route("/api/metricool/accounts")
def api_metricool_accounts():
    """Liste les comptes Metricool configurés"""
    return jsonify({"accounts": [{"name": k, "blog_id": v["blog_id"], "active": v.get("active", False)} for k,v in METRICOOL_ACCOUNTS.items()]})

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
@app.route("/api/flocages/reset", methods=["GET", "POST"])
def api_reset_flocages():
    global _pepites_deck_mem, _normaux_cache
    r2_put_json("meta/flocages.json", {"flocages": DEFAULT_FLOCAGES, "pepites": PEPITE_FLOCAGES})
    # Vider le deck et le cache mémoire pour forcer rechargement
    r2_put_json("meta/pepites_deck.json", {"remaining": []})
    with _pepites_deck_lock:
        _pepites_deck_mem = []
        _normaux_cache = []
    print("[RESET] Cache flocages vidé — sera rechargé au prochain TikTok")
    return jsonify({"success": True, "count": len(DEFAULT_FLOCAGES)})

@app.route("/api/flocages", methods=["GET"])
def api_get_flocages():
    data = r2_get_json("meta/flocages.json")
    if not data:
        data = {"flocages": DEFAULT_FLOCAGES, "pepites": PEPITE_FLOCAGES}
        r2_put_json("meta/flocages.json", data)
    # Ajouter les pepites si pas encore dans R2
    if "pepites" not in data:
        data["pepites"] = PEPITE_FLOCAGES
        r2_put_json("meta/flocages.json", data)
    return jsonify(data)

@app.route("/api/flocages", methods=["POST"])
def api_save_flocages():
    data = request.json
    r2_put_json("meta/flocages.json", {"flocages": data.get("flocages", [])})
    return jsonify({"success": True})

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
    import random as _rnd
    if not API_KEY: return jsonify({"error":"Clé API manquante"}),500
    f = request.files.get("image")
    user = request.form.get("user","").strip()
    name = request.form.get("name","").strip()
    number = request.form.get("number","").strip()
    name_below = request.form.get("name_below","").strip() or None
    resolution = request.form.get("resolution", "1k").strip()
    skip_buffer = request.form.get("skip_buffer", "false").lower() == "true"
    if not f: return jsonify({"error":"Aucune image"}),400
    # Si pas de flocage fourni, en tirer un aléatoire
    if not name and not number:
        try:
            floc_data = r2_get_json("meta/flocages.json") or {}
            all_flocs = floc_data.get("flocages", DEFAULT_FLOCAGES) or DEFAULT_FLOCAGES
        except Exception:
            all_flocs = DEFAULT_FLOCAGES
        floc_str = _rnd.choice(all_flocs) if all_flocs else "LOVEUR / 2 / BLONDE"
        parts = [p.strip() for p in floc_str.split("/")]
        name = parts[0] if parts else ""
        number = parts[1] if len(parts) > 1 else "2"
        name_below = parts[2] if len(parts) > 2 else None
    result = call_gemini(f.read(), f.mimetype or "image/png", name, number, name_below, resolution=resolution)
    log_generation(user, result["success"])
    if result["success"]:
        # Ajouter au buffer SEULEMENT si ce n'est pas un "Relancer" d'une image déjà comptabilisée
        # (évite de dupliquer l'image dans le buffer/un futur TikTok différent)
        if not skip_buffer:
            floc = f"{name}/{number}/{name_below or ''}"
            add_to_buffer_and_create_tiktoks([result["image"]], [floc], user)
        return jsonify({"image": result["image"]})
    return jsonify({"error": result["error"]}), 500

# ── Generate bulk (ASYNC — plus de timeout Railway) ─────────────────────────
@app.route("/generate_bulk", methods=["POST"])
def generate_bulk():
    if not API_KEY: return jsonify({"error": "Clé API manquante"}), 500

    files = request.files.getlist("images")
    flockages_raw = request.form.get("flockages", "")
    user = request.form.get("user", "").strip()
    session_id = request.form.get("session_id", str(uuid.uuid4()))
    resolution = request.form.get("resolution", "1k").strip()

    if not files: return jsonify({"error": "Aucune image"}), 400

    template_keys_raw = request.form.get("template_keys", "[]")
    try: template_keys = json.loads(template_keys_raw)
    except: template_keys = []
    variant = request.form.get("variant", "v1")  # v1=normal, v2=flat lay

    # Les flocages sont générés automatiquement (4 pépites + 3 normaux par TikTok)
    # Plus besoin de flocages manuels — on ignore le champ flocages
    items = []
    for i, f in enumerate(files):
        items.append({
            "bytes": f.read(), "mime": f.mimetype or "image/png",
            "name": "", "number": "", "name_below": None,
            "template_key": template_keys[i] if i < len(template_keys) else "",
            "variant": variant,
        })

    # Initialiser la session immédiatement
    _get_or_create_session(session_id, len(items))
    with _job_sessions_lock:
        if session_id in _job_sessions:
            _job_sessions[session_id]["user"] = user

    # Lancer la génération en background — Railway ne coupe plus jamais la connexion
    t = threading.Thread(target=_run_bulk_async, args=(session_id, items, user, resolution), daemon=True)
    t.start()

    # Répondre immédiatement (< 100ms) — plus aucun timeout possible
    return jsonify({
        "session_id": session_id,
        "total": len(items),
        "status": "running",
        "workers": WORKER_COUNT,
    })

def _result_with_image(r, fallback_index):
    """Lit l'image depuis R2 temp pour la retourner au frontend"""
    img_b64 = None
    r2_key = r.get("r2_key")
    if r2_key:
        try:
            obj = get_r2().get_object(Bucket=R2_BUCKET, Key=r2_key)
            img_b64 = base64.b64encode(obj["Body"].read()).decode()
        except Exception:
            pass
    return {
        "image": img_b64,
        "floc": r.get("floc",""),
        "index": r.get("orig_index", fallback_index),
        "error": r.get("error")
    }

@app.route("/api/jobs/progress/<session_id>")
def api_jobs_progress(session_id):
    """Polling endpoint — retourne aussi les nouvelles images depuis last_seen"""
    last_seen = int(request.args.get("last_seen", 0))
    with _job_sessions_lock:
        s = _job_sessions.get(session_id)
    # Fallback R2 si Railway a redémarré
    if not s:
        saved = r2_get_json(f"sessions/{session_id}.json")
        if saved:
            return jsonify({
                "session_id": session_id,
                "total": saved.get("total", 0),
                "done": saved.get("total", 0),
                "errors": saved.get("total", 0) - saved.get("success", 0),
                "success": saved.get("success", 0),
                "status": "done",
                "tiktoks_created": [],
                "buffer_remaining": 0,
                "percent": 100,
                "new_results": [],
            })
        return jsonify({"error": "Session introuvable"}), 404
    # Lire tout sous lock pour cohérence — y compris new_results
    with _job_sessions_lock:
        snap = {
            "total": s["total"],
            "done": s["done"],
            "errors": s["errors"],
            "status": s["status"],
            "tiktoks_created": list(s.get("tiktoks_created", [])),
            "buffer_remaining": s.get("buffer_remaining", 0),
            "new_results": list(s["results"][last_seen:]),  # copie sous lock
        }
    new_results = snap["new_results"]
    done = snap["done"]; total = snap["total"]

    # Libérer les images déjà envoyées de la RAM
    if new_results:
        with _job_sessions_lock:
            s2 = _job_sessions.get(session_id)
            if s2:
                for r in s2["results"][:last_seen + len(new_results)]:
                    if not r.get("sent"):
                        r["sent"] = True
                        # Libérer l'image de la RAM après envoi
                        # (on garde juste les métadonnées)
                    elif r.get("image"):
                        r["image"] = None  # déjà envoyée, libérer RAM

    return jsonify({
        "session_id": session_id,
        "total": total,
        "done": done,
        "errors": snap["errors"],
        "success": done - snap["errors"],
        "status": snap["status"],
        "tiktoks_created": snap["tiktoks_created"],
        "buffer_remaining": snap["buffer_remaining"],
        "percent": round(done / total * 100) if total else 0,
        "new_results": [_result_with_image(r, last_seen + i) for i, r in enumerate(new_results)],
        "seen_count": last_seen + len(new_results),
    })

@app.route("/monitor")
def monitor_page():
    return render_template("monitor.html")

@app.route("/api/monitor/sessions")
def api_monitor_sessions():
    """Retourne toutes les sessions actives en RAM pour le monitoring en temps réel"""
    active = []
    with _job_sessions_lock:
        for sid, s in _job_sessions.items():
            active.append({
                "session_id": sid,
                "user": s.get("user", "Inconnu"),
                "total": s["total"],
                "done": s["done"],
                "errors": s["errors"],
                "status": s["status"],
                "percent": round(s["done"] / s["total"] * 100) if s["total"] else 0,
                "tiktoks_created": s.get("tiktoks_created", []),
                "buffer_remaining": s.get("buffer_remaining", 0),
                "created_at": s.get("created_at"),
                "elapsed_seconds": s.get("elapsed_seconds", 0),
            })
    # Trier : en cours d'abord, puis par date
    active.sort(key=lambda x: (x["status"] != "running", x.get("created_at","") or ""), reverse=False)
    return jsonify({"active": active})

@app.route("/api/sessions")
def api_sessions():
    """Retourne les sessions récentes avec leurs stats"""
    session_keys = sorted(r2_list_keys("sessions/"), reverse=True)[:30]
    sessions = []
    for sk in session_keys:
        s = r2_get_json(sk)
        if s: sessions.append(s)
    return jsonify({"sessions": sessions})

@app.route("/api/session/stats", methods=["POST"])
def api_session_stats():
    """Sauvegarde les stats de durée d'une session de génération"""
    data = request.json or {}
    sid = data.get("session_id")
    if not sid: return jsonify({"error": "session_id requis"}), 400
    # Mettre à jour la session R2 avec la durée
    key = f"sessions/{sid}.json"
    session = r2_get_json(key) or {}
    session["elapsed_seconds"] = data.get("elapsed_seconds", 0)
    session["images_per_min"] = round(data.get("total", 0) / max(data.get("elapsed_seconds", 1) / 60, 0.01), 1)
    session["total"] = data.get("total", session.get("total", 0))
    session["success"] = data.get("success", session.get("success", 0))
    session["user"] = data.get("user", session.get("user", ""))
    r2_put_json(key, session)
    return jsonify({"success": True})

@app.route("/api/jobs/cancel/<session_id>", methods=["POST"])
def api_jobs_cancel(session_id):
    """Marque une session comme annulée (les workers en cours finissent leur image)"""
    with _job_sessions_lock:
        s = _job_sessions.get(session_id)
        if s:
            s["status"] = "cancelled"
    return jsonify({"success": True})

# ── API Templates ───────────────────────────────────────────────────────────
@app.route("/api/templates2")
def api_templates2():
    """Liste les templates de la variante v2 (flat lay)"""
    r2 = get_r2()
    if not r2: return jsonify({"templates": []})
    try:
        templates = []
        kwargs = {"Bucket": R2_BUCKET, "Prefix": PFX_TEMPLATES_V2}
        while True:
            resp = r2.list_objects_v2(**kwargs)
            for obj in resp.get("Contents", []):
                k = obj["Key"]
                if k.lower().endswith((".png", ".jpg", ".jpeg", ".webp")):
                    url = r2_presigned(k, expires=3600)
                    templates.append({"key": k, "name": k.replace(PFX_TEMPLATES_V2, "").rsplit(".", 1)[0], "url": url})
            if not resp.get("IsTruncated"): break
            kwargs["ContinuationToken"] = resp["NextContinuationToken"]
        return jsonify({"templates": templates})
    except Exception as e:
        return jsonify({"templates": [], "error": str(e)})

@app.route("/api/template2_image")
def api_template2_image():
    """Retourne une image template v2 en base64"""
    key = request.args.get("key")
    if not key: return jsonify({"error": "key requis"}), 400
    r2 = get_r2()
    if not r2: return jsonify({"error": "R2 non configuré"}), 500
    try:
        obj = r2.get_object(Bucket=R2_BUCKET, Key=key)
        data = obj["Body"].read()
        mime = "image/png" if key.lower().endswith(".png") else "image/jpeg"
        return jsonify({"image": base64.b64encode(data).decode(), "mime": mime})
    except Exception as e:
        return jsonify({"error": str(e)}), 404

@app.route("/api/box_ref/upload", methods=["POST"])
def api_box_ref_upload():
    """Upload la photo de référence de la boîte Volakits pour le générateur v2"""
    global _box_ref_cache
    f = request.files.get("image")
    if not f: return jsonify({"error": "Aucune image"}), 400
    r2 = get_r2()
    if not r2: return jsonify({"error": "R2 non configuré"}), 500
    data = f.read()
    r2.put_object(Bucket=R2_BUCKET, Key=KEY_BOX_REF, Body=data, ContentType=f.mimetype or "image/png")
    _box_ref_cache = base64.b64encode(data).decode()
    return jsonify({"success": True})

@app.route("/api/templates2/upload", methods=["POST"])
def api_templates2_upload():
    """Upload une template v2"""
    files = request.files.getlist("images")
    if not files: return jsonify({"error": "Aucune image"}), 400
    r2 = get_r2()
    if not r2: return jsonify({"error": "R2 non configuré"}), 500
    uploaded = []
    for f in files:
        if not f.filename: continue
        key = f"{PFX_TEMPLATES_V2}{f.filename}"
        r2.put_object(Bucket=R2_BUCKET, Key=key, Body=f.read(), ContentType=f.mimetype or "image/png")
        uploaded.append(key)
    return jsonify({"success": True, "uploaded": uploaded})

@app.route("/api/templates2/random")
def api_templates2_random():
    """Retourne N templates v2 aléatoires"""
    import random as _random
    n = int(request.args.get("n", 50))
    r2 = get_r2()
    if not r2: return jsonify({"templates": []})
    try:
        all_keys = []
        kwargs = {"Bucket": R2_BUCKET, "Prefix": PFX_TEMPLATES_V2}
        while True:
            resp = r2.list_objects_v2(**kwargs)
            for obj in resp.get("Contents", []):
                k = obj["Key"]
                if k.lower().endswith((".png", ".jpg", ".jpeg", ".webp")):
                    all_keys.append(k)
            if not resp.get("IsTruncated"): break
            kwargs["ContinuationToken"] = resp["NextContinuationToken"]
        # Rotation intelligente — éviter les templates récemment utilisées
        try:
            used_data = r2_get_json("meta/templates_used.json") or {}
            used_keys = set(used_data.get("keys", [])[:50])  # les 50 dernières
            # Séparer non-utilisées et utilisées
            fresh = [k for k in all_keys if k not in used_keys]
            recent = [k for k in all_keys if k in used_keys]
            _random.shuffle(fresh)
            _random.shuffle(recent)
            # Prioriser les fraîches, compléter avec les récentes si besoin
            all_keys = fresh + recent
        except Exception:
            _random.shuffle(all_keys)
        selected = all_keys[:min(n, len(all_keys))]
        templates = []
        for k in selected:
            try:
                obj = r2.get_object(Bucket=R2_BUCKET, Key=k)
                data = obj["Body"].read()
                mime = "image/png" if k.lower().endswith(".png") else "image/jpeg"
                templates.append({"key": k, "name": k.replace(PFX_TEMPLATES_V2, "").rsplit(".", 1)[0], "image": base64.b64encode(data).decode(), "mime": mime})
            except Exception:
                pass
        return jsonify({"templates": templates, "total": len(all_keys)})
    except Exception as e:
        return jsonify({"templates": [], "error": str(e)})

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

@app.route("/api/templates/used", methods=["POST"])
def api_mark_template_used():
    """Marque une template comme utilisée récemment"""
    key = (request.json or {}).get("key")
    if not key: return jsonify({"error": "key requis"}), 400
    used = r2_get_json("meta/templates_used.json") or {"keys": [], "max": 100}
    keys_list = used.get("keys", [])
    if key in keys_list: keys_list.remove(key)
    keys_list.insert(0, key)
    used["keys"] = keys_list[:200]  # garder les 200 dernières
    r2_put_json("meta/templates_used.json", used)
    return jsonify({"success": True})

@app.route("/api/templates/random")
def api_templates_random():
    """Retourne N templates aléatoires avec leurs images base64 — évite N appels séparés"""
    import random as _random
    n = int(request.args.get("n", 50))
    r2 = get_r2()
    if not r2: return jsonify({"templates": [], "error": "R2 non configuré"})
    try:
        all_keys = []
        kwargs = {"Bucket": R2_BUCKET, "Prefix": PFX_TEMPLATES}
        while True:
            resp = r2.list_objects_v2(**kwargs)
            for obj in resp.get("Contents", []):
                k = obj["Key"]
                if k.lower().endswith((".png", ".jpg", ".jpeg", ".webp")):
                    all_keys.append(k)
            if not resp.get("IsTruncated"): break
            kwargs["ContinuationToken"] = resp["NextContinuationToken"]
        # Rotation intelligente — éviter les templates récemment utilisées
        try:
            used_data = r2_get_json("meta/templates_used.json") or {}
            used_keys = set(used_data.get("keys", [])[:50])  # les 50 dernières
            # Séparer non-utilisées et utilisées
            fresh = [k for k in all_keys if k not in used_keys]
            recent = [k for k in all_keys if k in used_keys]
            _random.shuffle(fresh)
            _random.shuffle(recent)
            # Prioriser les fraîches, compléter avec les récentes si besoin
            all_keys = fresh + recent
        except Exception:
            _random.shuffle(all_keys)
        selected = all_keys[:min(n, len(all_keys))]
        templates = []
        for k in selected:
            try:
                obj = r2.get_object(Bucket=R2_BUCKET, Key=k)
                data = obj["Body"].read()
                mime = "image/png" if k.lower().endswith(".png") else "image/jpeg"
                templates.append({
                    "key": k,
                    "name": k.replace(PFX_TEMPLATES, "").rsplit(".", 1)[0],
                    "image": base64.b64encode(data).decode(),
                    "mime": mime
                })
            except Exception:
                pass
        return jsonify({"templates": templates, "total": len(all_keys)})
    except Exception as e:
        return jsonify({"templates": [], "error": str(e)})

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

@app.route("/api/templates2/delete", methods=["POST"])
def api_templates2_delete():
    """Supprime une template v2"""
    key = (request.json or {}).get("key")
    if not key: return jsonify({"error": "key requis"}), 400
    r2 = get_r2()
    if not r2: return jsonify({"error": "R2 non configuré"}), 500
    try:
        r2.delete_object(Bucket=R2_BUCKET, Key=key)
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

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

@app.route("/calendar")
def calendar_page():
    return render_template("calendar.html")

@app.route("/api/calendar/live")
def api_calendar_live():
    """Fetch les posts programmés depuis Metricool pour tous les comptes"""
    all_posts = []
    
    # Fetcher depuis Metricool pour les comptes actifs
    for account, mc_acc in METRICOOL_ACCOUNTS.items():
        if not mc_acc.get("active") or not METRICOOL_TOKEN:
            continue
        try:
            page = 1
            while True:
                resp = requests.get(
                    f"https://app.metricool.com/api/v2/posts?userId={METRICOOL_USER_ID}&blogId={mc_acc['blog_id']}&status=scheduled&page={page}&pageSize=100",
                    headers={"X-Mc-Auth": METRICOOL_TOKEN},
                    timeout=15
                )
                if resp.status_code != 200:
                    break
                data = resp.json()
                posts = data if isinstance(data, list) else data.get("posts", data.get("data", []))
                if not posts:
                    break
                for post in posts:
                    pub_time = post.get("publicationDate", {})
                    scheduled_at = pub_time.get("dateTime") if isinstance(pub_time, dict) else pub_time
                    all_posts.append({
                        "account": account,
                        "metricool_id": post.get("id"),
                        "scheduled_at": scheduled_at,
                        "media_urls": [m.get("url","") for m in post.get("media", []) if isinstance(m, dict)],
                        "content": post.get("text", ""),
                        "status": "scheduled",
                    })
                pagination = data.get("pagination", {}) if isinstance(data, dict) else {}
                total_pages = pagination.get("total_pages", pagination.get("totalPages", 1))
                if page >= total_pages:
                    break
                page += 1
        except Exception as e:
            print(f"[CALENDAR] Erreur Metricool {account}: {e}")
    
    # RobinReach supprimé — Metricool uniquement
    return jsonify({"posts": all_posts, "total": len(all_posts)})

@app.route("/api/calendar")
def api_calendar():
    """Retourne tous les TikToks programmés groupés par compte et par date"""
    account_filter = request.args.get("account", "all")
    
    # Lire tous les TikToks programmés
    keys = r2_list_keys(PFX_SCHEDULED)
    events = []
    
    for k in keys:
        if "/imgs/" in k:
            continue
        d = r2_get_json(k)
        if not d or not d.get("scheduled_at"):
            continue
        acc = d.get("account", "")
        if account_filter != "all" and acc != account_filter:
            continue
        
        # Convertir en heure Paris pour l'affichage
        try:
            from zoneinfo import ZoneInfo
            paris_tz = ZoneInfo("Europe/Paris")
            dt_utc = datetime.fromisoformat(d["scheduled_at"])
            if dt_utc.tzinfo is None:
                dt_utc = dt_utc.replace(tzinfo=timezone.utc)
            dt_paris = dt_utc.astimezone(paris_tz)
            date_str = dt_paris.strftime("%Y-%m-%d")
            time_str = dt_paris.strftime("%Hh%M")
        except Exception:
            date_str = d["scheduled_at"][:10]
            time_str = d["scheduled_at"][11:16]
        
        # Thumbnail : première image
        thumb_url = None
        img_keys = d.get("image_keys", [])
        if img_keys:
            thumb_url = r2_presigned(img_keys[0], expires=86400)
        
        events.append({
            "id": d.get("id"),
            "number": d.get("number"),
            "account": acc,
            "date": date_str,
            "time": time_str,
            "scheduled_at": d.get("scheduled_at"),
            "real_status": d.get("real_status", "scheduled"),
            "thumb": thumb_url,
            "r2_key": k,
        })
    
    # Stats par compte
    accounts_list = list(METRICOOL_ACCOUNTS.keys())
    
    return jsonify({
        "events": events,
        "accounts": accounts_list,
        "schedule_times": {
            acc: get_schedule_times_for_account(acc)
            for acc in accounts_list
        }
    })

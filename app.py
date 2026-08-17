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

def _get_or_create_session(session_id, total, carousel_size=None, carousel_plan=None, carousel_assets=None):
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
                # Plan S/A/B pré-calculé pour ce carousel
                "carousel_size":   carousel_size,
                "carousel_plan":   carousel_plan   or [],
                "carousel_assets": carousel_assets or [],
                # Suivi par carousel (buffer séparé, intégrité S/A/B)
                "pending_by_carousel": {},  # {carousel_idx: [{"r2_key","floc","template_key","pos"}, ...]}
                "results_by_carousel": {},  # {carousel_idx: {"success","failed","total","flushed"}}
                "leftover_count":   0,
                "leftover_indices": [],
            }
        return _job_sessions[session_id]

def _basic_quality_check(img_b64):
    """
    Contrôles techniques basiques sur une image générée.
    Retourne (True, None) si OK, (False, "raison") si problème détecté.
    Pas de Gemini Vision, pas d'analyse sémantique, pas de détection de contenu.
    """
    # 1. Base64 décodable
    try:
        img_bytes = base64.b64decode(img_b64)
    except Exception:
        return False, "base64 non décodable"

    # 2. Taille minimale (image non vide / non corrompue)
    if len(img_bytes) < 10_000:  # < 10 KB → probablement vide ou corrompue
        return False, f"image trop petite ({len(img_bytes)} bytes)"

    # 3. PIL peut ouvrir et vérifier l'intégrité de l'image
    try:
        from PIL import Image
        import io
        img = Image.open(io.BytesIO(img_bytes))
        img.verify()  # vérifie l'intégrité sans charger tous les pixels en RAM
    except Exception as e:
        return False, f"image corrompue ou illisible: {e}"

    # 4. Dimensions minimales
    try:
        from PIL import Image
        import io
        img2 = Image.open(io.BytesIO(img_bytes))
        w, h = img2.size
        if w < 256 or h < 256:
            return False, f"dimensions trop petites: {w}x{h}"
    except Exception:
        pass  # si on ne peut pas lire les dimensions, on laisse passer

    return True, None


def _update_session(session_id, success, image_b64=None, floc=None, error=None, idx=None, user=None, template_key=""):
    batch = None
    session_user = None

    # Contrôle technique basique AVANT stockage R2 (image_b64 encore disponible)
    if success and image_b64:
        qc_ok, qc_reason = _basic_quality_check(image_b64)
        if not qc_ok:
            print(f"[QC] ❌ Image idx={idx} rejetée: {qc_reason}")
            success = False
            error = f"Contrôle qualité échoué: {qc_reason}"

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
        session_user = s.get("user") or user

        all_carousels_sess = s.get("all_carousels", [])
        use_carousel_routing = bool(all_carousels_sess) and idx is not None

        # Variables de flush (déterminées ci-dessous)
        batch_carousel   = None   # liste d'items du carousel à créer (multi-carousel)
        batch_c_target   = None   # taille cible du carousel à créer
        batch_c_idx      = None   # carousel_idx concerné
        legacy_batch     = None   # batch du buffer global (generate_single)

        if use_carousel_routing:
            # Trouver le carousel_idx de cette image
            c_idx = None
            c_target = 7
            for c in all_carousels_sess:
                if c["global_start"] <= idx <= c["global_end"]:
                    c_idx = c["carousel_idx"]
                    c_target = c["target_size"]
                    break

            if c_idx is not None:
                # Initialiser les structures du carousel si première image
                if c_idx not in s["pending_by_carousel"]:
                    s["pending_by_carousel"][c_idx] = []
                if c_idx not in s["results_by_carousel"]:
                    s["results_by_carousel"][c_idx] = {"success": 0, "failed": 0, "total": c_target, "flushed": False}

                if success and r2_img_key:
                    s["results"].append({"r2_key": r2_img_key, "floc": floc, "orig_index": idx, "template_key": template_key})
                    s["pending_by_carousel"][c_idx].append({
                        "r2_key": r2_img_key, "floc": floc,
                        "template_key": template_key, "pos": idx,
                    })
                    s["results_by_carousel"][c_idx]["success"] += 1
                else:
                    s["errors"] += 1
                    s["results"].append({"r2_key": None, "floc": floc or "", "orig_index": idx, "error": error or "Erreur inconnue"})
                    s["results_by_carousel"][c_idx]["failed"] += 1

                # Ce carousel est-il complètement résolu ?
                rc = s["results_by_carousel"][c_idx]
                if not rc.get("flushed") and (rc["success"] + rc["failed"]) >= rc["total"]:
                    rc["flushed"] = True
                    batch_carousel = s["pending_by_carousel"].pop(c_idx, [])
                    batch_c_target = rc["total"]
                    batch_c_idx    = c_idx
            else:
                # idx hors de tout carousel planifié (leftover) → comptabiliser en erreur
                s["errors"] += 1
                s["results"].append({"r2_key": None, "floc": floc or "", "orig_index": idx, "error": error or "Image hors carousel (leftover)"})
        else:
            # Pas de routing multi-carousel → comportement global conservé (generate_single)
            if success and r2_img_key:
                s["results"].append({"r2_key": r2_img_key, "floc": floc, "orig_index": idx, "template_key": template_key})
                s["pending_buffer"].append({"r2_key": r2_img_key, "floc": floc, "template_key": template_key})
            else:
                s["errors"] += 1
                s["results"].append({"r2_key": None, "floc": floc or "", "orig_index": idx, "error": error or "Erreur inconnue"})
            # Flush legacy tous les 7 (comportement d'origine)
            pending = s["pending_buffer"]
            if len(pending) >= 7:
                legacy_batch = pending[:7]
                s["pending_buffer"] = pending[7:]

        if s["done"] >= s["total"]:
            s["status"] = "done"

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

    # ── Flush carousel complet (multi-carousel) ──────────────────────────────
    if batch_carousel is not None:
        rc_failed = 0
        with _job_sessions_lock:
            s = _job_sessions.get(session_id)
            if s:
                rc_failed = s.get("results_by_carousel", {}).get(batch_c_idx, {}).get("failed", 0)

        if not batch_carousel:
            print(f"[SESSION] ❌ Carousel {batch_c_idx} vide — toutes les images ont échoué, non créé")
        elif rc_failed > 0:
            # Une ou plusieurs positions échouées → NE PAS créer un carousel incomplet
            print(f"[SESSION] ❌ Carousel {batch_c_idx} incomplet ({rc_failed} position(s) échouée(s) sur {batch_c_target}) — non créé, plan S/A/B préservé")
        else:
            try:
                # TRI PAR POSITION obligatoire — workers asynchrones, ordre non garanti
                batch_carousel.sort(key=lambda x: x["pos"])
                imgs = []
                for r in batch_carousel:
                    try:
                        obj = get_r2().get_object(Bucket=R2_BUCKET, Key=r["r2_key"])
                        imgs.append(base64.b64encode(obj["Body"].read()).decode())
                        get_r2().delete_object(Bucket=R2_BUCKET, Key=r["r2_key"])
                    except Exception:
                        imgs.append(None)
                flocs = [r["floc"] for r in batch_carousel]
                tkeys = [r.get("template_key","") for r in batch_carousel]
                created, remaining = add_to_buffer_and_create_tiktoks(imgs, flocs, session_user, tkeys, target_size=batch_c_target, atomic=True)
                with _job_sessions_lock:
                    s = _job_sessions.get(session_id)
                    if s:
                        s["tiktoks_created"].extend(created)
                        s["buffer_remaining"] = remaining
                print(f"[SESSION] ✅ Carousel {batch_c_idx} créé ({batch_c_target} images) — {len(created)} TikTok(s)")
            except Exception as e:
                print(f"[SESSION] Erreur création carousel {batch_c_idx}: {e}")

    # ── Flush legacy (generate_single) ───────────────────────────────────────
    if legacy_batch is not None:
        try:
            imgs = []
            for r in legacy_batch:
                try:
                    obj = get_r2().get_object(Bucket=R2_BUCKET, Key=r["r2_key"])
                    imgs.append(base64.b64encode(obj["Body"].read()).decode())
                    get_r2().delete_object(Bucket=R2_BUCKET, Key=r["r2_key"])
                except Exception:
                    imgs.append(None)
            flocs = [r["floc"] for r in legacy_batch]
            tkeys = [r.get("template_key","") for r in legacy_batch]
            created, remaining = add_to_buffer_and_create_tiktoks(imgs, flocs, session_user, tkeys, target_size=len(legacy_batch))
            with _job_sessions_lock:
                s = _job_sessions.get(session_id)
                if s:
                    s["tiktoks_created"].extend(created)
                    s["buffer_remaining"] = remaining
            print(f"[SESSION] ✅ {len(created)} TikTok(s) créé(s) (buffer legacy)")
        except Exception as e:
            print(f"[SESSION] Erreur création TikTok legacy: {e}")

def _finalize_session(session_id, user):
    """
    Traite les carousels non encore flushés à la fin du bulk.
    Chaque carousel est traité individuellement avec sa propre target_size.
    Un carousel incomplet (positions échouées) n'est PAS créé avec des images d'un autre.
    Le buffer legacy (generate_single) est traité séparément.
    """
    with _job_sessions_lock:
        s = _job_sessions.get(session_id)
        if not s:
            return
        session_user = s.get("user") or user
        all_carousels_sess = s.get("all_carousels", [])
        use_carousel_routing = bool(all_carousels_sess)

        # Récupérer et vider atomiquement les structures par carousel
        pending_by_carousel = dict(s.get("pending_by_carousel", {}))
        results_by_carousel = dict(s.get("results_by_carousel", {}))
        s["pending_by_carousel"] = {}

        # Buffer legacy (generate_single)
        legacy_pending = s["pending_buffer"][:]
        s["pending_buffer"] = []

    # ── Traiter chaque carousel individuellement (multi-carousel) ────────────
    if use_carousel_routing:
        c_map = {c["carousel_idx"]: c for c in all_carousels_sess}
        for c_idx, items in pending_by_carousel.items():
            rc = results_by_carousel.get(c_idx, {})
            if rc.get("flushed"):
                continue  # déjà créé dans _update_session
            c_info   = c_map.get(c_idx, {})
            c_target = c_info.get("target_size", 7)
            failed   = rc.get("failed", 0)

            if not items:
                print(f"[FINALIZE] Carousel {c_idx} vide — ignoré")
                continue
            if failed > 0:
                print(f"[FINALIZE] ❌ Carousel {c_idx} incomplet ({failed} échec(s) sur {c_target}) — non créé, plan S/A/B préservé")
                continue
            # Sécurité : nombre d'images doit correspondre exactement à target_size
            valid = [r for r in items if r.get("r2_key")]
            if len(valid) != c_target:
                print(f"[FINALIZE] ❌ Carousel {c_idx}: {len(valid)} images valides ≠ target {c_target} — non créé")
                continue

            # TRI PAR POSITION obligatoire avant flush (workers asynchrones)
            valid.sort(key=lambda x: x["pos"])
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
                created, remaining = add_to_buffer_and_create_tiktoks(imgs, flocs, session_user, tkeys, target_size=c_target, atomic=True)
                with _job_sessions_lock:
                    s = _job_sessions.get(session_id)
                    if s:
                        s["tiktoks_created"].extend(created)
                        s["buffer_remaining"] = remaining
                        if c_idx in s.get("results_by_carousel", {}):
                            s["results_by_carousel"][c_idx]["flushed"] = True
                print(f"[FINALIZE] ✅ Carousel {c_idx} créé ({c_target} images) — {len(created)} TikTok(s)")
            except Exception as e:
                print(f"[FINALIZE] Erreur création carousel {c_idx}: {e}")

    # ── Traiter le buffer legacy (generate_single) ───────────────────────────
    if legacy_pending:
        valid = [r for r in legacy_pending if r.get("r2_key")]
        if valid:
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
                print(f"[FINALIZE] Legacy: {len(created)} TikTok(s), {remaining} en buffer")
            except Exception as e:
                print(f"[FINALIZE] Erreur legacy: {e}")

def _run_bulk_async(session_id, items, user, resolution):
    """Phase 1 : Gemini en parallèle. Phase 2 : Replicate séquentiel → zéro 429"""
    import gc
    session = _get_or_create_session(session_id, len(items))

    for i, item in enumerate(items):
        item["_index"] = i
        item["_gemini_result"] = None  # résultat Gemini avant upscale

    # ── Phase 1 : Gemini en parallèle (WORKER_COUNT workers) ─────────────
    print(f"[BULK] Phase 1 — Gemini sur {len(items)} images avec {WORKER_COUNT} workers...")

    # ── Calculer TOUS les carousels du bulk avant les workers ────────────────
    # Chaque carousel est une unité indépendante avec sa propre taille et son propre plan S/A/B.
    # L'anti-répétition est mis à jour carousel par carousel pour rester cohérent.
    n_items = len(items)

    # Charger catégories et recent_used UNE SEULE FOIS pour tout le bulk
    _tmpl_cats_bulk = _load_templates_categories()
    _floc_cats_bulk = _load_flocages_categories()
    _recent_state   = _get_recent_used()  # état initial, mis à jour carousel par carousel

    all_carousels  = []     # [{carousel_idx, target_size, plan, assets, global_start, global_end}, ...]
    _assets_by_idx = {}     # {global_idx: asset_dict} — couvre tous les indices du bulk

    # ── Partitionner le bulk en carousels de tailles valides (somme == n_items) ──
    import random as _rng_mod
    carousel_sizes, leftover_count = partition_bulk_into_carousels(n_items, _rng_mod.Random())

    print(f"[BULK] {n_items} images demandées → {len(carousel_sizes)} carousel(s): {carousel_sizes}")

    global_idx   = 0
    carousel_idx = 0
    for c_size in carousel_sizes:
        c_plan  = build_carousel_plan(c_size)
        c_assets = select_carousel_assets(
            c_plan,
            tmpl_cats=_tmpl_cats_bulk,
            floc_cats=_floc_cats_bulk,
            recent_override=_recent_state,
        )
        c_global_end = global_idx + c_size - 1  # exact — pas de troncature

        if c_assets is None:
            # MODE 2, config insuffisante pour ce carousel : skipper proprement
            print(f"[BULK] ⚠️ Carousel {carousel_idx} annulé (config_insuffisante) — indices {global_idx}→{c_global_end} sans assets")
            for g in range(global_idx, c_global_end + 1):
                _assets_by_idx[g] = None  # sentinelle explicite
        else:
            # Mapper chaque asset sur son index global dans le bulk
            for local_pos, asset in enumerate(c_assets):
                g_idx = global_idx + local_pos
                _assets_by_idx[g_idx] = {**asset, "carousel_idx": carousel_idx}

            # Anti-répétition incrémental : mettre à jour recent_state carousel par carousel
            _recent_state["templates"] = (
                _recent_state["templates"] + [a["template_key"] for a in c_assets if a.get("template_key")]
            )[-ANTI_REPEAT_PENALTY_LAST_N:]
            _recent_state["flocages"] = (
                _recent_state["flocages"] + [a["flocage"] for a in c_assets if a.get("flocage")]
            )[-ANTI_REPEAT_PENALTY_LAST_N:]

        print(f"[BULK] Carousel {carousel_idx} = {c_size} images (indices {global_idx}→{c_global_end})")

        all_carousels.append({
            "carousel_idx":  carousel_idx,
            "target_size":   c_size,               # taille exacte réelle du carousel
            "plan":          c_plan,
            "assets":        c_assets or [],
            "global_start":  global_idx,
            "global_end":    c_global_end,          # exact, pas de min()
            "ok":            c_assets is not None,
        })
        global_idx   += c_size
        carousel_idx += 1

    # ── Traçabilité des images leftover (non plaçables en carousel complet) ──
    leftover_indices = list(range(global_idx, n_items))  # indices au-delà des carousels
    if leftover_count > 0 or leftover_indices:
        # leftover_count vient du partitionnement (zone morte) ; leftover_indices = indices réels
        print(f"[BULK] ⚠️ {len(leftover_indices)} image(s) non plaçable(s) : minimum carousel = 7")
        print(f"[BULK] ⚠️ Indices leftover : {leftover_indices}")
        # Marquer ces indices comme sentinelle leftover (rejetés proprement dans gemini_one)
        for g in leftover_indices:
            _assets_by_idx[g] = "__LEFTOVER__"

    print(f"[BULK] {carousel_idx} carousel(s) planifié(s) pour {n_items} images: " +
          ", ".join(f"C{c['carousel_idx']}={c['target_size']}img({'OK' if c['ok'] else 'SKIP'})"
                    for c in all_carousels))

    # Mettre à jour la session avec le plan complet multi-carousel + traçabilité leftover
    with _job_sessions_lock:
        s = _job_sessions.get(session_id)
        if s:
            s["all_carousels"]    = all_carousels
            s["carousel_size"]    = all_carousels[0]["target_size"] if all_carousels else 7
            s["leftover_count"]   = len(leftover_indices)
            s["leftover_indices"] = leftover_indices

    def gemini_one(item):
        import random as _rnd
        idx = item["_index"]
        try:
            # Échelonner le démarrage — évite de spammer 50 requêtes simultanées
            time.sleep(idx * 0.1 % 3)  # délai 0-3s selon l'index

            # Lire l'asset depuis l'index global pré-calculé
            asset = _assets_by_idx.get(idx)

            # Image leftover : non plaçable en carousel complet → rejetée proprement
            if asset == "__LEFTOVER__":
                print(f"[BULK] ⏭️ Image idx={idx} leftover — non générée (minimum carousel = 7)")
                _update_session(session_id, False,
                                error="Image leftover : non plaçable dans un carousel complet (min 7)",
                                idx=idx)
                return

            if asset is None:
                # Sentinelle explicite : carousel annulé pour config_insuffisante
                print(f"[BULK] ❌ Image idx={idx} rejetée — carousel annulé (config_insuffisante)")
                _update_session(session_id, False,
                                error="Carousel annulé : configuration S/A/B insuffisante pour ce tier",
                                idx=idx)
                return

            if asset and asset.get("flocage"):
                floc_str = asset["flocage"]
                # Utiliser la template pré-sélectionnée si l'item n'en a pas déjà une
                if not item.get("template_key") and asset.get("template_key"):
                    item["template_key"] = asset["template_key"]
            else:
                # Sécurité : asset présent mais flocage vide (ne devrait pas arriver)
                print(f"[BULK] ⚠️ Asset idx={idx} sans flocage — fallback pépites/normaux")
                try:
                    _floc_data    = r2_get_json("meta/flocages.json") or {}
                    _pepites_list = _floc_data.get("pepites", PEPITE_FLOCAGES) or PEPITE_FLOCAGES
                    _all_flocs    = _floc_data.get("flocages", DEFAULT_FLOCAGES) or DEFAULT_FLOCAGES
                    _pepites_set  = {p.lower().strip() for p in _pepites_list}
                    _normaux_list = [f for f in _all_flocs if f.lower().strip() not in _pepites_set]
                except Exception:
                    _pepites_list = PEPITE_FLOCAGES
                    _normaux_list = [f for f in DEFAULT_FLOCAGES
                                     if f.lower().strip() not in {p.lower().strip() for p in PEPITE_FLOCAGES}]
                c_size_fallback = asset.get("carousel_idx") and all_carousels[asset["carousel_idx"]]["target_size"] or 7
                pos_in_carousel = idx % max(c_size_fallback, 7)
                if pos_in_carousel < 4 and _pepites_list:
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
FIXED_CAPTION  = "3 Maillot Acheté 1 Offert 🎁 #volakits #ete #foot"

# ── Taille carousel dynamique ──────────────────────────────────────────────
# Distribution en % — doit sommer à 100
CAROUSEL_SIZE_DISTRIBUTION = {
    7:  5,
    8:  20,
    9:  30,
    10: 30,
    11: 10,
    12: 5,
}

# Segmentation S/A/B stricte par taille de carousel
CAROUSEL_SEGMENT_PLAN = {
    7:  {"S": 4, "A": 2, "B": 1},
    8:  {"S": 4, "A": 2, "B": 2},
    9:  {"S": 4, "A": 3, "B": 2},
    10: {"S": 4, "A": 3, "B": 3},
    11: {"S": 4, "A": 4, "B": 3},
    12: {"S": 4, "A": 4, "B": 4},
}

# Anti-répétition : nb de derniers éléments pénalisés (pas exclus)
ANTI_REPEAT_PENALTY_LAST_N = 20

# ── Scheduler : fenêtres horaires en HEURE DE PARIS (locale) ──────────────
# 3 publications/jour : matin ~10h, après-midi ~16h, soir ~21h30 (heure française)
# La conversion vers UTC est AUTOMATIQUE selon la saison (été UTC+2 / hiver UTC+1)
# via ZoneInfo("Europe/Paris") dans get_or_create_slot_time(). On garde donc les
# mêmes horaires français été comme hiver, sans recalcul manuel.
SCHEDULE_WINDOWS_BY_ACCOUNT = {
    "Volakits Main (wael)": [
        {"start": "09:30", "end": "10:30"},   # matin ~10h Paris
        {"start": "15:30", "end": "16:30"},   # après-midi ~16h Paris
        {"start": "21:00", "end": "22:00"},   # soir ~21h30 Paris
    ],
    "Volakits 1 (seik)": [
        {"start": "09:30", "end": "10:30"},
        {"start": "15:30", "end": "16:30"},
        {"start": "21:00", "end": "22:00"},
    ],
    "Volakits 2 (momo)": [
        {"start": "09:30", "end": "10:30"},
        {"start": "15:30", "end": "16:30"},
        {"start": "21:00", "end": "22:00"},
    ],
    "Volakits 6 (wassim)": [
        {"start": "09:30", "end": "10:30"},
        {"start": "15:30", "end": "16:30"},
        {"start": "21:00", "end": "22:00"},
    ],
}
SCHEDULE_WINDOWS_DEFAULT = [
    {"start": "09:30", "end": "10:30"},
    {"start": "15:30", "end": "16:30"},
    {"start": "21:00", "end": "22:00"},
]

# Conserver pour compatibilité — sera remplacé au BLOC 7
# ── [DEPRECATED] Ancien système de créneaux fixes ──────────────────────────
# Remplacé par SCHEDULE_WINDOWS_BY_ACCOUNT + get_or_create_slot_time (fenêtres
# aléatoires persistées). Conservé pour rétrocompatibilité — plus AUCUN code
# fonctionnel ne l'utilise pour la programmation réelle. Ne pas réintroduire
# d'usage : cela créerait un second système de scheduling concurrent.
SCHEDULE_TIMES_BY_ACCOUNT = {
    "Volakits Main (wael)": ["08:00", "13:00", "16:30", "19:00"],
    "Volakits 1 (seik)":    ["14:00", "18:30"],
    "Volakits 2 (momo)":    ["14:00", "18:30"],
    "Volakits 6 (wassim)":  ["14:00", "18:30"],
}
SCHEDULE_TIMES_DEFAULT = ["08:30", "15:30"]

def get_schedule_times_for_account(account):
    """[DEPRECATED] Ancien système de créneaux fixes.
    Remplacé par SCHEDULE_WINDOWS_BY_ACCOUNT + get_or_create_slot_time.
    Conservé uniquement pour rétrocompatibilité — ne plus utiliser pour la
    programmation réelle (créerait un système de scheduling concurrent)."""
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

# Comptes Instagram (même blogId que TikTok car même marque Metricool)
INSTAGRAM_ACCOUNTS = {
    "Volakits Instagram": {"blog_id": "6542376", "active": True},
}

# Créneaux Instagram (UTC) — 9h/12h/15h/18h/21h Paris (UTC+2)
INSTAGRAM_SCHEDULE_TIMES = ["05:00", "07:00", "10:00", "13:00", "16:00", "19:00", "21:00"]  # 7h/9h/12h/15h/18h/21h/23h Paris

# Préfixes R2 pour la queue Instagram
PFX_QUEUE_IG     = "queue_ig/"
PFX_SCHEDULED_IG = "scheduled_ig/"
KEY_USED_SLOTS_IG = "meta/used_slots_ig.json"


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

def add_to_buffer_and_create_tiktoks(new_images_b64, new_flockages, user, new_template_keys=None, target_size=None, atomic=False):
    # atomic=True : les images fournies constituent EXACTEMENT un carousel déjà validé
    #   (chemin multi-carousel depuis _update_session / _finalize_session).
    #   → création directe du TikTok, SANS passer par le buffer global buffer/pending.json.
    #   → garantit l'intégrité : aucun mélange possible avec un résidu ou un autre carousel.
    # atomic=False : comportement legacy (generate_single) — accumulation dans le buffer global.

    if atomic:
        # Filtrer les images valides (None = échec lecture R2) en gardant l'alignement
        valid_imgs, valid_flocs, valid_tkeys = [], [], []
        for i, img in enumerate(new_images_b64):
            if img:
                valid_imgs.append(img)
                valid_flocs.append(new_flockages[i] if i < len(new_flockages) else "")
                valid_tkeys.append(new_template_keys[i] if new_template_keys and i < len(new_template_keys) else "")

        # Sécurité : un carousel atomique doit contenir exactement sa taille cible
        expected = target_size if target_size else len(valid_imgs)
        if len(valid_imgs) != expected:
            print(f"[BUFFER] ❌ Carousel atomique incomplet ({len(valid_imgs)}/{expected}) — non créé, intégrité préservée")
            return [], 0

        # Déterminer l'utilisateur (sans écrire dans le buffer global)
        buf_user = user
        try:
            buf_ref = get_buffer()
            if buf_ref.get("user"):
                buf_user = buf_ref["user"]
        except Exception:
            pass

        tiktok_num = get_next_tiktok_number()
        print(f"[BUFFER] Création atomique TikTok {tiktok_num} ({len(valid_imgs)} images, target={expected})")
        # preserve_order=True : l'ordre S/A/B (position↔flocage↔image) ne doit PAS être réordonné
        _save_tiktok(tiktok_num, valid_imgs, buf_user, valid_flocs, valid_tkeys, preserve_order=True)
        # Anti-répétition : un seul appel par carousel (pas de double comptage)
        try:
            _update_recent_used(
                template_keys=[tk for tk in valid_tkeys if tk],
                flocage_names=[f for f in valid_flocs if f],
            )
        except Exception as e:
            print(f"[ANTI-REPEAT] Erreur update après TikTok atomique {tiktok_num}: {e}")
        return [tiktok_num], 0

    # ── Chemin legacy (generate_single) — buffer global accumulateur (INCHANGÉ) ──
    # target_size : taille du carousel courant (issu du plan S/A/B)
    # Si absent ou < 7 → 7 par défaut pour rétrocompatibilité (generate_single, etc.)
    effective_size = target_size if (target_size and target_size >= 7) else 7

    # Phase 1 : mettre à jour le buffer sous lock (rapide)
    with _buffer_lock:
        buf = get_buffer()
        if not buf.get("user"):
            buf["user"] = user
        buf["images_b64"].extend(new_images_b64)
        buf["flockages"].extend(new_flockages)
        if "template_keys" not in buf: buf["template_keys"] = []
        buf["template_keys"].extend(new_template_keys if new_template_keys else [""] * len(new_images_b64))
        print(f"[BUFFER] Now has {len(buf['images_b64'])} images (target={effective_size})")
        # Extraire les batches à créer selon la taille cible du carousel
        batches = []
        buf_user = buf["user"]
        while len(buf["images_b64"]) >= effective_size:
            batch_b64   = buf["images_b64"][:effective_size]
            batch_floc  = buf["flockages"][:effective_size]
            batch_tkeys = buf.get("template_keys", [])[:effective_size]
            tiktok_num  = get_next_tiktok_number()
            batches.append((tiktok_num, batch_b64, batch_floc, batch_tkeys))
            buf["images_b64"] = buf["images_b64"][effective_size:]
            buf["flockages"]  = buf["flockages"][effective_size:]
            if "template_keys" in buf: buf["template_keys"] = buf["template_keys"][effective_size:]
        remaining = len(buf["images_b64"])
        _save_buffer(buf)

    # Phase 2 : sauvegarder les TikToks HORS du lock (appels R2 lents)
    created = []
    for tiktok_num, batch_b64, batch_floc, batch_tkeys in batches:
        print(f"[BUFFER] Creating TikTok {tiktok_num}...")
        _save_tiktok(tiktok_num, batch_b64, buf_user, batch_floc, batch_tkeys)
        created.append(tiktok_num)
        # Anti-répétition : mettre à jour immédiatement après chaque carousel créé
        try:
            _update_recent_used(
                template_keys=[tk for tk in batch_tkeys if tk],
                flocage_names=[f for f in batch_floc if f],
            )
        except Exception as e:
            print(f"[ANTI-REPEAT] Erreur update après TikTok {tiktok_num}: {e}")

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
            
            # Convertir en JPEG et redimensionner à 1080x1920 max (limite TikTok)
            try:
                from PIL import Image
                import io
                img = Image.open(io.BytesIO(img_resp.content)).convert("RGB")
                # Redimensionner si trop grand
                max_w, max_h = 1080, 1920
                w, h = img.size
                if w > max_w or h > max_h:
                    ratio = min(max_w/w, max_h/h)
                    new_w, new_h = int(w*ratio), int(h*ratio)
                    img = img.resize((new_w, new_h), Image.LANCZOS)
                    print(f"[METRICOOL] Image {i+1} redimensionnée: {w}x{h} → {new_w}x{new_h}")
                buf = io.BytesIO()
                img.save(buf, format="JPEG", quality=92)
                img_bytes = buf.getvalue()
            except Exception as e:
                print(f"[METRICOOL] ❌ Conversion JPEG échouée image {i+1}: {e} — image ignorée")
                continue  # Skip cette image plutôt que d'envoyer du PNG
            
            # Uploader sur les serveurs Metricool
            files = {"picture": (f"image_{i+1}.jpg", img_bytes, "image/jpeg")}
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
        return {"success": False, "error": "Aucune image convertie en JPEG"}
    
    failed_count = len(image_urls) - len(media_ids)
    if len(media_ids) < 5:
        return {"success": False, "error": f"Seulement {len(media_ids)}/{len(image_urls)} images valides — TikTok renvoyé en file d'attente", "requeue": True}
    
    if failed_count > 0:
        print(f"[METRICOOL] ⚠️ {failed_count} image(s) ignorées — TikTok programmé avec {len(media_ids)} images")
    
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

def schedule_instagram(image_urls, caption, publish_time_iso, blog_id, timezone="Europe/Paris"):
    """Programme un post Instagram carrousel via Metricool"""
    if not METRICOOL_TOKEN:
        return {"success": False, "error": "METRICOOL_TOKEN manquant"}
    
    media_ids = []
    for i, url in enumerate(image_urls):
        try:
            img_resp = requests.get(url, timeout=30)
            if img_resp.status_code != 200:
                print(f"[INSTAGRAM] Erreur téléchargement image {i+1}: {img_resp.status_code}")
                continue
            # Convertir en JPEG et redimensionner à 1080x1920
            try:
                from PIL import Image
                import io
                img = Image.open(io.BytesIO(img_resp.content)).convert("RGB")
                max_w, max_h = 1080, 1920
                w, h = img.size
                if w > max_w or h > max_h:
                    ratio = min(max_w/w, max_h/h)
                    img = img.resize((int(w*ratio), int(h*ratio)), Image.LANCZOS)
                buf = io.BytesIO()
                img.save(buf, format="JPEG", quality=92)
                img_bytes = buf.getvalue()
            except Exception as e:
                print(f"[INSTAGRAM] ❌ Conversion échouée image {i+1}: {e}")
                continue
            
            files = {"picture": (f"image_{i+1}.jpg", img_bytes, "image/jpeg")}
            data = {"userId": METRICOOL_USER_ID, "blogId": blog_id}
            upload_resp = requests.post(
                "https://app.metricool.com/api/utils/upload",
                headers={"X-Mc-Auth": METRICOOL_TOKEN},
                files=files,
                data=data,
                timeout=60
            )
            if upload_resp.status_code == 200:
                resp_text = upload_resp.text.strip()
                if resp_text.startswith("http"):
                    media_ids.append(resp_text)
                    print(f"[INSTAGRAM] Upload image {i+1}: OK")
        except Exception as e:
            print(f"[INSTAGRAM] Erreur upload image {i+1}: {e}")
    
    if len(media_ids) < 5:
        return {"success": False, "error": f"Seulement {len(media_ids)} images valides"}
    
    payload = {
        "publicationDate": {"dateTime": publish_time_iso, "timezone": timezone},
        "text": caption,
        "firstCommentText": "",
        "providers": [{"network": "instagram"}],
        "media": media_ids,
        "mediaAltText": [None] * len(media_ids),
        "autoPublish": True,
        "shortener": False,
        "draft": False,
        "hasNotReadNotes": False,
        "instagramData": {
            "autoPublish": True,
            "isAiGenerated": False,
            "type": "POST"
        }
    }
    
    try:
        resp = requests.post(
            f"https://app.metricool.com/api/v2/scheduler/posts?userId={METRICOOL_USER_ID}&blogId={blog_id}",
            headers={"X-Mc-Auth": METRICOOL_TOKEN, "Content-Type": "application/json"},
            json=payload,
            timeout=60
        )
        print(f"[INSTAGRAM] Response {resp.status_code}: {resp.text[:300]}")
        if resp.status_code in (200, 201):
            data = resp.json()
            post_id = (data.get("data") or {}).get("id")
            return {"success": True, "post_id": post_id}
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

# ══════════════════════════════════════════════════════════════════════════════
# BLOC 2 — Catégories S/A/B : lecture/écriture + anti-répétition
#
# IMPORTANT — état initial :
# Les catégories S/A/B ne sont pas encore remplies manuellement.
# Tant qu'elles sont vides, toutes les fonctions retombent sur le
# comportement existant (pépites/normaux ou sélection aléatoire globale).
# Le bot ne plante pas si meta/templates_categories.json n'existe pas.
# ══════════════════════════════════════════════════════════════════════════════

def _load_templates_categories():
    """
    Lit meta/templates_categories.json depuis R2.
    Format : {"S": ["templates/img01.jpg", ...], "A": [...], "B": [...]}
    Si le fichier n'existe pas ou est vide → retourne {"S":[],"A":[],"B":[]}
    sans erreur. Le bot continue avec le fallback aléatoire global.
    """
    try:
        data = r2_get_json("meta/templates_categories.json") or {}
        return {
            "S": data.get("S", []),
            "A": data.get("A", []),
            "B": data.get("B", []),
        }
    except Exception as e:
        print(f"[CATEGORIES] Erreur lecture templates_categories.json: {e}")
        return {"S": [], "A": [], "B": []}

def _save_templates_categories(cats):
    """Persiste meta/templates_categories.json dans R2."""
    try:
        r2_put_json("meta/templates_categories.json", {
            "S": cats.get("S", []),
            "A": cats.get("A", []),
            "B": cats.get("B", []),
        })
        return True
    except Exception as e:
        print(f"[CATEGORIES] Erreur écriture templates_categories.json: {e}")
        return False

def _categories_are_empty(cats):
    """Retourne True si toutes les catégories S/A/B sont vides."""
    return not cats.get("S") and not cats.get("A") and not cats.get("B")

def _load_flocages_categories():
    """
    Lit les catégories S/A/B des flocages depuis meta/flocages.json.

    Priorité :
      1. Si "categories" présent et non vide → utiliser S/A/B directement
      2. Si "categories" absent mais "pepites" présent → pepites=S, reste=B, A vide
      3. Si rien → {"S":[],"A":[],"B":[]} — le fallback global prendra le relais

    Retourne un dict {"S":[...],"A":[...],"B":[...]}.
    Ne plante jamais.
    """
    try:
        data = r2_get_json("meta/flocages.json") or {}
        if "categories" in data:
            cats = data["categories"]
            result = {
                "S": cats.get("S", []),
                "A": cats.get("A", []),
                "B": cats.get("B", []),
            }
            # Si categories existe mais est vide → essayer l'ancien format pepites
            if _categories_are_empty(result) and data.get("pepites"):
                all_flocs   = data.get("flocages", DEFAULT_FLOCAGES)
                pepites     = data.get("pepites", [])
                pepites_set = {p.lower().strip() for p in pepites}
                normaux     = [f for f in all_flocs if f.lower().strip() not in pepites_set]
                print("[CATEGORIES] categories vides → fallback pepites→S, normaux→B")
                return {"S": pepites, "A": [], "B": normaux}
            return result
        elif data.get("pepites"):
            # Ancien format uniquement — migration transparente à la volée
            all_flocs   = data.get("flocages", DEFAULT_FLOCAGES)
            pepites     = data.get("pepites", PEPITE_FLOCAGES)
            pepites_set = {p.lower().strip() for p in pepites}
            normaux     = [f for f in all_flocs if f.lower().strip() not in pepites_set]
            print("[CATEGORIES] flocages.json ancien format → pepites=S, normaux=B")
            return {"S": pepites, "A": [], "B": normaux}
        else:
            # Rien de disponible — le fallback global prendra le relais
            print("[CATEGORIES] Aucune catégorie flocages disponible → fallback global")
            return {"S": [], "A": [], "B": []}
    except Exception as e:
        print(f"[CATEGORIES] Erreur lecture flocages categories: {e}")
        return {"S": [], "A": [], "B": []}

def _save_flocages_categories(cats):
    """
    Persiste les catégories S/A/B dans meta/flocages.json
    en conservant les champs existants (flocages, pepites) pour rétrocompatibilité.
    """
    try:
        data = r2_get_json("meta/flocages.json") or {}
        data["categories"] = {
            "S": cats.get("S", []),
            "A": cats.get("A", []),
            "B": cats.get("B", []),
        }
        r2_put_json("meta/flocages.json", data)
        return True
    except Exception as e:
        print(f"[CATEGORIES] Erreur écriture flocages categories: {e}")
        return False

# ── Anti-répétition ──────────────────────────────────────────────────────────
_recent_used_lock = threading.Lock()

def _get_recent_used():
    """
    Lit meta/recent_used.json depuis R2.
    Format : {"templates": [...], "flocages": [...]}
    Les éléments les plus récents sont en fin de liste.
    Retourne des listes vides si le fichier n'existe pas encore.
    """
    try:
        data = r2_get_json("meta/recent_used.json") or {}
        return {
            "templates": data.get("templates", []),
            "flocages":  data.get("flocages",  []),
        }
    except Exception:
        return {"templates": [], "flocages": []}

def _update_recent_used(template_keys, flocage_names):
    """
    Ajoute les éléments d'un carousel terminé dans l'historique anti-répétition.
    Conserve uniquement les ANTI_REPEAT_PENALTY_LAST_N derniers de chaque type.
    Appelé après création d'un carousel.
    """
    with _recent_used_lock:
        try:
            data = _get_recent_used()
            data["templates"] = (data["templates"] + list(template_keys))[-ANTI_REPEAT_PENALTY_LAST_N:]
            data["flocages"]  = (data["flocages"]  + list(flocage_names))[-ANTI_REPEAT_PENALTY_LAST_N:]
            r2_put_json("meta/recent_used.json", data)
        except Exception as e:
            print(f"[ANTI-REPEAT] Erreur mise à jour recent_used: {e}")

def _penalized_sample(pool, n, recent_used, already_used_in_carousel):
    """
    Sélectionne n éléments parmi pool avec pénalisation des récemment utilisés.

    Règles :
    - Évite les doublons intra-carousel (already_used_in_carousel)
    - Éléments dans recent_used → poids 0.1 (pénalisés, pas exclus)
    - Si pool trop petit après exclusion doublons → utilise tout le pool
    - Retourne toujours quelque chose si pool non vide
    """
    import random as _rnd

    # Exclure d'abord les doublons intra-carousel
    available = [x for x in pool if x not in already_used_in_carousel]
    if len(available) < n:
        # Pool insuffisant après exclusion doublons → utiliser tout le pool
        available = list(pool)
    if not available:
        return []
    if len(available) <= n:
        return list(available)

    recent_set = set(recent_used)
    weights = [0.1 if x in recent_set else 1.0 for x in available]

    chosen = []
    remaining = list(available)
    remaining_weights = list(weights)

    for _ in range(min(n, len(remaining))):
        if not remaining:
            break
        total = sum(remaining_weights)
        if total <= 0:
            pick = _rnd.choice(remaining)
        else:
            r = _rnd.random() * total
            cumul = 0.0
            pick = remaining[-1]
            for elem, w in zip(remaining, remaining_weights):
                cumul += w
                if r <= cumul:
                    pick = elem
                    break
        chosen.append(pick)
        idx = remaining.index(pick)
        remaining.pop(idx)
        remaining_weights.pop(idx)

    return chosen

# ══════════════════════════════════════════════════════════════════════════════
# BLOC 3 — Sélection contrôlée S/A/B : taille carousel, plan, assets
#
# FALLBACK COMPLET si catégories vides :
# - get_carousel_size()      → fonctionne toujours (pas de dépendance aux catégories)
# - build_carousel_plan()    → fonctionne toujours
# - select_carousel_assets() → si toutes les catégories sont vides :
#     templates : sélection aléatoire parmi toutes les templates R2
#     flocages  : ancien système pépites+normaux
#   Le bot se comporte exactement comme avant jusqu'au remplissage des catégories.
# ══════════════════════════════════════════════════════════════════════════════

def get_carousel_size():
    """
    Tire aléatoirement une taille de carousel selon CAROUSEL_SIZE_DISTRIBUTION.
    Distribution pondérée — 8/9/10 images représentent 80% des cas.
    Fonctionne toujours, indépendamment des catégories S/A/B.
    """
    import random as _rnd
    sizes = list(CAROUSEL_SIZE_DISTRIBUTION.keys())
    weights = [CAROUSEL_SIZE_DISTRIBUTION[s] for s in sizes]
    total = sum(weights)
    r = _rnd.random() * total
    cumul = 0
    for size, w in zip(sizes, weights):
        cumul += w
        if r <= cumul:
            return size
    return sizes[-1]

def partition_bulk_into_carousels(n_items, rng=None):
    """
    Découpe n_items en tailles de carousels ∈ [7,12] avec somme == n_items.

    Retourne (sizes, leftover) :
      - sizes    : liste de tailles, chacune dans [7,12]
      - leftover : nombre d'images non plaçables (< 7), jamais transformées en carousel tronqué

    La distribution CAROUSEL_SIZE_DISTRIBUTION est une cible statistique, pas une
    contrainte stricte par bulk. On tire les tailles selon cette distribution puis
    on corrige l'écart pour que la somme soit exactement n_items.

    Cas "zone morte" : certaines valeurs (ex: 13) ne sont pas décomposables en
    tailles [7,12]. On place alors autant de carousels complets que possible et
    on signale le reste via leftover (jamais de carousel < 7).
    """
    import random as _random
    if rng is None:
        rng = _random.Random()
    if n_items < 7:
        return [], n_items

    avg = sum(s * w for s, w in CAROUSEL_SIZE_DISTRIBUTION.items()) / sum(CAROUSEL_SIZE_DISTRIBUTION.values())

    # k faisable = nombre de carousels tel que n_items ∈ [k*7, k*12]
    feasible_k = [k for k in range(1, n_items // 7 + 1) if k * 7 <= n_items <= k * 12]

    if not feasible_k:
        # Zone morte : placer au mieux, signaler le reste en leftover
        k = max(1, round(n_items / avg))
        sizes, remaining = [], n_items
        for _ in range(k):
            if remaining < 7:
                break
            s = min(12, max(7, min(remaining, get_carousel_size())))
            if remaining - s < 0:
                s = remaining
            if s < 7:
                break
            sizes.append(s)
            remaining -= s
        return sizes, remaining

    # Choisir k le plus proche du ratio idéal n_items/avg
    ideal_k = n_items / avg
    k = min(feasible_k, key=lambda kk: abs(kk - ideal_k))

    # Tirer k tailles selon la distribution
    sizes = [get_carousel_size() for _ in range(k)]

    # Corriger l'écart pour que sum(sizes) == n_items (en restant dans [7,12])
    diff = n_items - sum(sizes)
    guard = 0
    while diff != 0 and guard < 100000:
        guard += 1
        idx = rng.randrange(len(sizes))
        if diff > 0 and sizes[idx] < 12:
            sizes[idx] += 1
            diff -= 1
        elif diff < 0 and sizes[idx] > 7:
            sizes[idx] -= 1
            diff += 1

    rng.shuffle(sizes)
    return sizes, 0

def build_carousel_plan(carousel_size):
    """
    Construit le plan S/A/B strict pour un carousel de taille donnée.
    Retourne une liste de dicts : [{"pos": 1, "tier": "S"}, ...]

    La segmentation est stricte : toutes les S d'abord, puis A, puis B.
    Les counts viennent de CAROUSEL_SEGMENT_PLAN.
    Fonctionne toujours, indépendamment des catégories S/A/B.
    """
    plan_counts = CAROUSEL_SEGMENT_PLAN.get(carousel_size, {"S": 4, "A": 2, "B": 1})
    plan = []
    pos = 1
    for tier in ("S", "A", "B"):
        for _ in range(plan_counts.get(tier, 0)):
            plan.append({"pos": pos, "tier": tier})
            pos += 1
    return plan

def select_carousel_assets(plan, tmpl_cats=None, floc_cats=None, recent_override=None):
    """
    Pour chaque slot du plan, sélectionne un template et un flocage du tier correspondant.

    Paramètres optionnels (pour éviter des appels R2 répétés dans un bulk multi-carousel) :
      tmpl_cats      : résultat de _load_templates_categories() déjà chargé
      floc_cats      : résultat de _load_flocages_categories() déjà chargé
      recent_override: état recent_used courant (dict {"templates":[],"flocages":[]})

    DEUX MODES EXCLUSIFS — aucun fallback inter-tier :

    MODE 1 — catégorisation inactive (toutes S/A/B vides) :
      templates → sélection aléatoire parmi toutes les clés R2 templates/
      flocages  → ancien système _draw_pepites/_draw_normaux selon position
      Comportement identique à l'ancien bot.

    MODE 2 — catégorisation active (au moins une catégorie non vide) :
      position S → pool S UNIQUEMENT
      position A → pool A UNIQUEMENT
      position B → pool B UNIQUEMENT
      Si un tier nécessaire est vide → carousel marqué config_insuffisante,
      retourne None. Aucun fallback inter-tier, aucun DEFAULT_FLOCAGES.

    Retourne une liste de dicts ou None si config insuffisante en MODE 2.
    """
    import random as _rnd

    # Charger les données si non fournies (appels existants sans paramètres)
    if tmpl_cats is None:
        tmpl_cats = _load_templates_categories()
    if floc_cats is None:
        floc_cats = _load_flocages_categories()
    recent = recent_override if recent_override is not None else _get_recent_used()

    tmpl_cats_empty = _categories_are_empty(tmpl_cats)
    floc_cats_empty = _categories_are_empty(floc_cats)

    # Détecter le mode actif — STRICT GLOBAL
    # MODE 1 : les DEUX côtés (templates ET flocages) sont totalement vides
    #          → comportement legacy ancien bot (fallback global des deux côtés).
    # MODE 2 : dès qu'AU MOINS un côté est configuré → catégorisation active.
    #          → les DEUX côtés doivent être configurés en S/A/B stricts.
    #          → si un côté (ou un tier) est vide → config_insuffisante, aucun fallback.
    mode1 = tmpl_cats_empty and floc_cats_empty

    if mode1:
        print("[ASSETS] MODE 1 — S/A/B non configurés → comportement ancien bot (legacy)")
    else:
        print(f"[ASSETS] MODE 2 STRICT — catégorisation active "
              f"(templates_vides={tmpl_cats_empty}, flocages_vides={floc_cats_empty})")
        # STRICT GLOBAL : en MODE 2, si un côté entier est vide → config insuffisante immédiate
        if tmpl_cats_empty:
            print("[ASSETS] ❌ CONFIG INSUFFISANTE: catégorisation active mais templates S/A/B totalement vides")
            return None
        if floc_cats_empty:
            print("[ASSETS] ❌ CONFIG INSUFFISANTE: catégorisation active mais flocages S/A/B totalement vides")
            return None

    # ── Fallback MODE 1 uniquement ────────────────────────────────────────────
    _all_template_keys_cache = None
    def _get_all_template_keys():
        nonlocal _all_template_keys_cache
        if _all_template_keys_cache is None:
            try:
                _all_template_keys_cache = r2_list_keys(PFX_TEMPLATES) or []
            except Exception:
                _all_template_keys_cache = []
        return _all_template_keys_cache

    def _get_fallback_flocage_mode1(pos_0based):
        """MODE 1 uniquement : ancien comportement pépites/normaux selon position."""
        if pos_0based < 4:
            drawn = _draw_pepites(1)
            return drawn[0] if drawn else (_draw_normaux(1) or [DEFAULT_FLOCAGES[0]])[0]
        else:
            drawn = _draw_normaux(1)
            return drawn[0] if drawn else (_draw_pepites(1) or [DEFAULT_FLOCAGES[0]])[0]

    used_templates = []
    used_flocages  = []

    # Regrouper par tier pour une sélection groupée (meilleure diversité)
    slots_by_tier = {"S": [], "A": [], "B": []}
    for slot in plan:
        slots_by_tier[slot["tier"]].append(slot)

    selected_by_tier = {}
    for tier in ("S", "A", "B"):
        n = len(slots_by_tier[tier])
        if n == 0:
            selected_by_tier[tier] = {"templates": [], "flocages": [], "ok": True}
            continue

        # ── Sélection templates ──────────────────────────────────────────────
        if mode1:
            # MODE 1 : aléatoire parmi toutes les templates R2
            all_keys = _get_all_template_keys()
            if all_keys:
                chosen_tmpls = _penalized_sample(all_keys, n, recent["templates"], used_templates)
                while len(chosen_tmpls) < n:
                    chosen_tmpls.append(_rnd.choice(all_keys))
            else:
                chosen_tmpls = [""] * n
        else:
            # MODE 2 : pool du tier UNIQUEMENT, aucun fallback inter-tier
            tmpl_pool = tmpl_cats.get(tier, [])
            if not tmpl_pool:
                print(f"[ASSETS] ❌ CONFIG INSUFFISANTE: templates tier={tier} vide en MODE 2")
                selected_by_tier[tier] = {"templates": [], "flocages": [], "ok": False, "error": f"templates_{tier}_vide"}
                continue
            chosen_tmpls = _penalized_sample(tmpl_pool, n, recent["templates"], used_templates)
            while len(chosen_tmpls) < n:
                chosen_tmpls.append(_rnd.choice(tmpl_pool))

        # ── Sélection flocages ───────────────────────────────────────────────
        if mode1:
            # MODE 1 : ancien système pépites/normaux selon position
            chosen_flocs = []
            for slot in slots_by_tier[tier]:
                chosen_flocs.append(_get_fallback_flocage_mode1(slot["pos"] - 1))
        else:
            # MODE 2 : pool du tier UNIQUEMENT, aucun fallback inter-tier
            floc_pool = floc_cats.get(tier, [])
            if not floc_pool:
                print(f"[ASSETS] ❌ CONFIG INSUFFISANTE: flocages tier={tier} vide en MODE 2")
                selected_by_tier[tier] = {"templates": [], "flocages": [], "ok": False, "error": f"flocages_{tier}_vide"}
                continue
            chosen_flocs = _penalized_sample(floc_pool, n, recent["flocages"], used_flocages)
            while len(chosen_flocs) < n:
                chosen_flocs.append(_rnd.choice(floc_pool))

        used_templates.extend(chosen_tmpls)
        used_flocages.extend(chosen_flocs)
        selected_by_tier[tier] = {"templates": chosen_tmpls, "flocages": chosen_flocs, "ok": True}

    # Vérifier qu'aucun tier nécessaire n'a échoué (MODE 2 uniquement)
    if not mode1:
        for tier in ("S", "A", "B"):
            n = len(slots_by_tier[tier])
            if n > 0 and not selected_by_tier.get(tier, {}).get("ok", True):
                err = selected_by_tier[tier].get("error", f"tier_{tier}_vide")
                print(f"[ASSETS] ❌ Carousel annulé — config_insuffisante: {err}")
                return None  # Signal explicite : carousel ne peut pas être créé

    # Assembler dans l'ordre du plan
    assets = []
    tier_cursors = {"S": 0, "A": 0, "B": 0}
    for slot in plan:
        tier = slot["tier"]
        i    = tier_cursors[tier]
        tier_data = selected_by_tier.get(tier, {"templates": [], "flocages": []})
        tmpl = tier_data["templates"][i] if i < len(tier_data["templates"]) else ""
        floc = tier_data["flocages"][i]  if i < len(tier_data["flocages"])  else ""
        assets.append({"pos": slot["pos"], "tier": tier, "template_key": tmpl, "flocage": floc})
        tier_cursors[tier] += 1

    return assets

def _save_tiktok(num, images_b64, user, flockages=None, template_keys=None, preserve_order=False):
    r2 = get_r2()
    if not r2: return False

    # ── Ordre des flocages ────────────────────────────────────────────────────
    # preserve_order=True (carousels atomiques S/A/B) : l'ordre position↔flocage↔image
    #   a déjà été construit par select_carousel_assets() et trié par pos dans
    #   _update_session. On NE réordonne PAS — sinon on casserait la segmentation
    #   S/A/B et l'alignement image↔flocage.
    # preserve_order=False (legacy generate_single) : ancien comportement — pépites
    #   d'abord, normaux ensuite.
    import random as _rnd
    provided = [f for f in (flockages or []) if f]

    if preserve_order:
        # Nouveau système : conserver l'ordre exact (déjà S puis A puis B par position)
        if provided:
            final_flockages = provided
        else:
            # Sécurité : si aucun flocage fourni pour un carousel atomique, ne pas inventer d'ordre
            final_flockages = []
    else:
        # ── Legacy : réordonner pépites en premier, normaux ensuite ──
        try:
            floc_data = r2_get_json("meta/flocages.json") or {}
            pepites_raw = floc_data.get("pepites", PEPITE_FLOCAGES)
            pepites_set_lower = {p.lower().strip() for p in pepites_raw}
        except Exception:
            pepites_set_lower = {p.lower().strip() for p in PEPITE_FLOCAGES}

        if provided:
            pepites_in = [f for f in provided if f.lower().strip() in pepites_set_lower]
            normaux_in = [f for f in provided if f.lower().strip() not in pepites_set_lower]
            final_flockages = pepites_in + normaux_in
        else:
            pepites_chosen = _draw_pepites(4)
            normaux_chosen = _draw_normaux(3)
            final_flockages = pepites_chosen + normaux_chosen
    print(f"[TIKTOK {num}] {len(final_flockages)} flocages (preserve_order={preserve_order}): {final_flockages[:2]}...")

    image_keys = []
    for i, b64 in enumerate(images_b64):
        if not b64: continue
        # Convertir en JPEG directement pour compatibilité TikTok
        try:
            from PIL import Image
            import io
            img_data = base64.b64decode(b64)
            img = Image.open(io.BytesIO(img_data)).convert("RGB")
            buf = io.BytesIO()
            img.save(buf, format="JPEG", quality=95)
            img_bytes = buf.getvalue()
            ext = "jpg"
        except Exception as e:
            print(f"[JPEG] ⚠️ Conversion JPEG échouée: {e} — fallback PNG (risque TikTok)")
            img_bytes = base64.b64decode(b64)
            ext = "png"
        k = f"queue/imgs/tiktok_{num:04d}_{i+1:02d}.{ext}"
        r2_put_image(k, img_bytes)
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
    r2_put_json(f"{PFX_QUEUE}tiktok_{num:04d}.json", meta)
    # Copie automatique dans la file d'attente Instagram
    ig_meta = {**meta, "platform": "instagram", "ig_status": "pending"}
    r2_put_json(f"{PFX_QUEUE_IG}tiktok_{num:04d}.json", ig_meta)
    return True

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

@app.route("/categories")
def categories_page(): return render_template("categories.html")

# ── Module de suivi des influenceurs ──────────────────────────────────────
INFLUENCEURS_R2_KEY = "meta/influenceurs.json"
INFLUENCEURS_BACKUP_PREFIX = "meta/influenceurs_backups/"
INFLUENCEURS_BACKUP_KEEP = 5  # nombre de sauvegardes de secours conservées
_influenceurs_lock = threading.Lock()

@app.route("/influenceurs")
def influenceurs_page(): return render_template("influenceurs.html")

@app.route("/api/influenceurs", methods=["GET"])
def api_get_influenceurs():
    """Retourne la liste complète des influenceurs depuis R2 + le numéro de version."""
    try:
        data = r2_get_json(INFLUENCEURS_R2_KEY) or {}
        influenceurs = data.get("influenceurs", [])
        if not isinstance(influenceurs, list):
            influenceurs = []
        return jsonify({
            "influenceurs": influenceurs,
            "version": data.get("version", 0),
            "updated_at": data.get("updated_at", ""),
        })
    except Exception as e:
        print(f"[INFLU] Erreur lecture: {e}")
        return jsonify({"influenceurs": [], "version": 0, "updated_at": ""})

@app.route("/api/influenceurs", methods=["POST"])
def api_save_influenceurs():
    """
    Sauvegarde la liste complète des influenceurs dans R2.

    Protection anti-perte de données :
    - Contrôle de version optimiste : le client envoie la version qu'il a chargée
      (base_version). Si la version serveur a changé entre-temps (autre onglet),
      on refuse l'écrasement et on renvoie 409 avec les données serveur à jour.
    - Le client peut forcer avec force=true (après avoir prévenu l'utilisateur).
    - Une sauvegarde de secours horodatée est créée à chaque écriture réussie
      (les INFLUENCEURS_BACKUP_KEEP dernières sont conservées).
    """
    try:
        payload = request.json or {}
        influenceurs = payload.get("influenceurs", [])
        if not isinstance(influenceurs, list):
            return jsonify({"success": False, "error": "format invalide"}), 400

        base_version = payload.get("base_version")   # version que le client avait au chargement
        force        = bool(payload.get("force", False))

        with _influenceurs_lock:
            current = r2_get_json(INFLUENCEURS_R2_KEY) or {}
            server_version = current.get("version", 0)

            # Contrôle de version optimiste (sauf si force=true ou premier enregistrement)
            if (not force) and (base_version is not None) and (base_version != server_version):
                # Un autre onglet/appareil a sauvegardé entre-temps → refuser l'écrasement
                print(f"[INFLU] ⚠️ Conflit de version: client base={base_version}, serveur={server_version}")
                return jsonify({
                    "success": False,
                    "conflict": True,
                    "server_version": server_version,
                    "server_influenceurs": current.get("influenceurs", []),
                    "server_updated_at": current.get("updated_at", ""),
                }), 409

            new_version = server_version + 1
            now_iso = datetime.now(timezone.utc).isoformat()
            record = {
                "influenceurs": influenceurs,
                "version": new_version,
                "updated_at": now_iso,
            }

            # Sauvegarde de secours de l'ANCIEN état avant écrasement (si non vide)
            if current.get("influenceurs"):
                try:
                    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
                    r2_put_json(f"{INFLUENCEURS_BACKUP_PREFIX}{ts}.json", current)
                    # Ne garder que les N dernières sauvegardes
                    backups = sorted(r2_list_keys(INFLUENCEURS_BACKUP_PREFIX, suffix=".json"))
                    for old_key in backups[:-INFLUENCEURS_BACKUP_KEEP]:
                        r2_delete(old_key)
                except Exception as e:
                    print(f"[INFLU] Backup non critique échoué: {e}")

            ok = r2_put_json(INFLUENCEURS_R2_KEY, record)
            if not ok:
                return jsonify({"success": False, "error": "écriture R2 échouée"}), 500

        return jsonify({"success": True, "count": len(influenceurs), "version": new_version, "updated_at": now_iso})
    except Exception as e:
        print(f"[INFLU] Erreur sauvegarde: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/influenceurs/backups", methods=["GET"])
def api_influenceurs_backups():
    """Liste les sauvegardes de secours disponibles (pour récupération manuelle)."""
    try:
        keys = sorted(r2_list_keys(INFLUENCEURS_BACKUP_PREFIX, suffix=".json"), reverse=True)
        backups = []
        for k in keys:
            name = k.replace(INFLUENCEURS_BACKUP_PREFIX, "").replace(".json", "")
            d = r2_get_json(k) or {}
            backups.append({
                "key": k,
                "name": name,
                "count": len(d.get("influenceurs", [])),
                "updated_at": d.get("updated_at", ""),
            })
        return jsonify({"backups": backups})
    except Exception as e:
        return jsonify({"backups": [], "error": str(e)})

@app.route("/api/influenceurs/restore", methods=["POST"])
def api_influenceurs_restore():
    """Restaure une sauvegarde de secours (devient la version courante)."""
    try:
        key = (request.json or {}).get("key")
        if not key or not key.startswith(INFLUENCEURS_BACKUP_PREFIX):
            return jsonify({"success": False, "error": "clé invalide"}), 400
        backup = r2_get_json(key)
        if not backup:
            return jsonify({"success": False, "error": "sauvegarde introuvable"}), 404
        with _influenceurs_lock:
            current = r2_get_json(INFLUENCEURS_R2_KEY) or {}
            new_version = current.get("version", 0) + 1
            record = {
                "influenceurs": backup.get("influenceurs", []),
                "version": new_version,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            r2_put_json(INFLUENCEURS_R2_KEY, record)
        return jsonify({"success": True, "count": len(record["influenceurs"]), "version": new_version})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

# ── Bibliothèque vidéos des influenceurs ───────────────────────────────────
# Les fichiers vidéo sont stockés dans R2 sous influenceurs_videos/ et gardés
# de façon permanente (même si l'influenceur supprime sa vidéo sur la plateforme).
# Les métadonnées (type unboxing/playback, lien original, statut ads, influenceur)
# sont dans meta/influenceurs_videos.json.
INFLUENCEURS_VIDEOS_PFX = "influenceurs_videos/"
INFLUENCEURS_VIDEOS_META = "meta/influenceurs_videos.json"
INFLUENCEURS_VIDEO_MAX_BYTES = 100 * 1024 * 1024  # 100 MB
_influ_videos_lock = threading.Lock()

def _load_influ_videos():
    """Retourne la liste des métadonnées vidéos (jamais None)."""
    try:
        data = r2_get_json(INFLUENCEURS_VIDEOS_META) or {}
        vids = data.get("videos", [])
        return vids if isinstance(vids, list) else []
    except Exception:
        return []

def _save_influ_videos(videos):
    try:
        return r2_put_json(INFLUENCEURS_VIDEOS_META, {"videos": videos})
    except Exception as e:
        print(f"[INFLU_VID] Erreur écriture méta: {e}")
        return False

@app.route("/api/influenceurs/videos", methods=["GET"])
def api_influ_videos_list():
    """Liste toutes les vidéos (pour la bibliothèque globale) ou celles d'un influenceur."""
    influ_id = request.args.get("influ_id")
    videos = _load_influ_videos()
    if influ_id:
        videos = [v for v in videos if v.get("influ_id") == influ_id]
    # Générer une URL présignée fraîche pour chaque vidéo (lecture/téléchargement)
    for v in videos:
        if v.get("r2_key"):
            v["url"] = r2_presigned(v["r2_key"], expires=604800)  # 7 jours
    # Trier par date décroissante
    videos.sort(key=lambda x: x.get("uploaded_at", ""), reverse=True)
    return jsonify({"videos": videos})

@app.route("/api/influenceurs/videos/upload", methods=["POST"])
def api_influ_videos_upload():
    """Upload un fichier vidéo vers R2 + enregistre les métadonnées."""
    f = request.files.get("video")
    if not f:
        return jsonify({"success": False, "error": "aucun fichier"}), 400

    influ_id    = request.form.get("influ_id", "").strip()
    influ_name  = request.form.get("influ_name", "").strip()
    vtype       = request.form.get("type", "").strip()        # unboxing / playback
    orig_link   = request.form.get("orig_link", "").strip()
    ads_status  = request.form.get("ads_status", "").strip()  # "" / a_utiliser / utilise
    if not influ_id:
        return jsonify({"success": False, "error": "influ_id requis"}), 400
    if vtype not in ("unboxing", "playback"):
        return jsonify({"success": False, "error": "type invalide"}), 400

    # Lire le fichier en mémoire avec contrôle de taille
    data = f.read()
    if len(data) == 0:
        return jsonify({"success": False, "error": "fichier vide"}), 400
    if len(data) > INFLUENCEURS_VIDEO_MAX_BYTES:
        return jsonify({"success": False, "error": f"fichier trop lourd (max 100 Mo, reçu {len(data)//(1024*1024)} Mo)"}), 400

    # Déterminer l'extension à partir du nom / mimetype
    fname = (f.filename or "video.mp4")
    ext = fname.rsplit(".", 1)[-1].lower() if "." in fname else "mp4"
    if ext not in ("mp4", "mov", "webm", "avi", "mkv", "m4v"):
        ext = "mp4"

    vid_id = f"vid_{int(time.time())}_{uuid.uuid4().hex[:8]}"
    r2_key = f"{INFLUENCEURS_VIDEOS_PFX}{influ_id}/{vid_id}.{ext}"
    mime = f.mimetype or "video/mp4"

    # Écrire dans R2
    r2 = get_r2()
    if not r2:
        return jsonify({"success": False, "error": "R2 non configuré"}), 500
    try:
        r2.put_object(Bucket=R2_BUCKET, Key=r2_key, Body=data, ContentType=mime)
    except Exception as e:
        print(f"[INFLU_VID] Erreur upload R2: {e}")
        return jsonify({"success": False, "error": "échec upload R2"}), 500

    meta = {
        "id": vid_id,
        "influ_id": influ_id,
        "influ_name": influ_name,
        "type": vtype,
        "orig_link": orig_link,
        "ads_status": ads_status,
        "r2_key": r2_key,
        "filename": fname,
        "size": len(data),
        "uploaded_at": datetime.now(timezone.utc).isoformat(),
    }
    with _influ_videos_lock:
        videos = _load_influ_videos()
        videos.append(meta)
        _save_influ_videos(videos)

    out = dict(meta)
    out["url"] = r2_presigned(r2_key, expires=604800)
    return jsonify({"success": True, "video": out})

@app.route("/api/influenceurs/videos/update", methods=["POST"])
def api_influ_videos_update():
    """Met à jour les métadonnées d'une vidéo (type, lien, statut ads)."""
    payload = request.json or {}
    vid_id = payload.get("id")
    if not vid_id:
        return jsonify({"success": False, "error": "id requis"}), 400
    with _influ_videos_lock:
        videos = _load_influ_videos()
        found = False
        for v in videos:
            if v.get("id") == vid_id:
                if "type" in payload and payload["type"] in ("unboxing", "playback"):
                    v["type"] = payload["type"]
                if "orig_link" in payload:
                    v["orig_link"] = payload["orig_link"]
                if "ads_status" in payload and payload["ads_status"] in ("", "a_utiliser", "utilise"):
                    v["ads_status"] = payload["ads_status"]
                found = True
                break
        if not found:
            return jsonify({"success": False, "error": "vidéo introuvable"}), 404
        _save_influ_videos(videos)
    return jsonify({"success": True})

@app.route("/api/influenceurs/videos/delete", methods=["POST"])
def api_influ_videos_delete():
    """Supprime une vidéo (fichier R2 + métadonnées)."""
    vid_id = (request.json or {}).get("id")
    if not vid_id:
        return jsonify({"success": False, "error": "id requis"}), 400
    with _influ_videos_lock:
        videos = _load_influ_videos()
        target = next((v for v in videos if v.get("id") == vid_id), None)
        if not target:
            return jsonify({"success": False, "error": "vidéo introuvable"}), 404
        # Supprimer le fichier R2
        if target.get("r2_key"):
            try:
                r2_delete(target["r2_key"])
            except Exception as e:
                print(f"[INFLU_VID] Erreur suppression R2: {e}")
        videos = [v for v in videos if v.get("id") != vid_id]
        _save_influ_videos(videos)
    return jsonify({"success": True})

# ══════════════════════════════════════════════════════════════════════════════
# ESPACE INFLUENCEUR PUBLIC — /espace/<slug>
#
# Vue publique individuelle pour chaque influenceur (programme ambassadeur).
# L'influenceur y consulte sa progression, ses missions, son colis, son code promo,
# et y renseigne lui-même son profil (réseaux, livraison, maillots, vidéos).
#
# IMPORTANT — Gamification : les paliers, seuils et récompenses ci-dessous sont
# VOLONTAIREMENT configurables et provisoires. Les valeurs définitives seront
# définies plus tard. Toute la structure est pensée pour les modifier sans
# toucher au reste du code.
# ══════════════════════════════════════════════════════════════════════════════

# ── Configuration des paliers (PROVISOIRE — à ajuster) ────────────────────
INFLUENCER_TIERS = [
    {
        "id": "decouverte",
        "name": "Découverte",
        "icon": "🥉",
        "color": "#94A3B8",
        # Critères pour ATTEINDRE ce palier (le premier est acquis d'office)
        "requirements": {},
        "perks": [
            "2 maillots offerts",
            "Code promo personnalisé -15%",
            "Commission 10% sur tes ventes",
        ],
    },
    {
        "id": "ambassadeur",
        "name": "Ambassadeur",
        "icon": "🥈",
        "color": "#6366F1",
        "requirements": {"videos": 3, "views": 50000, "sales": 5},
        "perks": [
            "Commission augmentée à 15%",
            "Nouveaux maillots chaque mois",
            "Accès anticipé aux nouveautés",
            "Mise en avant sur nos comptes",
        ],
    },
    {
        "id": "partenaire",
        "name": "Partenaire",
        "icon": "🥇",
        "color": "#C9A227",
        "requirements": {"videos": 10, "views": 250000, "sales": 30},
        "perks": [
            "Rémunération fixe + commission 20%",
            "Co-création d'un maillot signature",
            "Collaboration longue durée",
            "Budget contenu dédié",
        ],
    },
]

# Missions par défaut (PROVISOIRE — configurable)
DEFAULT_MISSIONS = [
    {"id": "profil",    "label": "Compléter ton profil",         "desc": "Réseaux sociaux et statistiques",     "auto": True},
    {"id": "maillots",  "label": "Choisir tes 2 maillots",       "desc": "Depuis le catalogue Volakits",        "auto": True},
    {"id": "livraison", "label": "Renseigner ton adresse",       "desc": "Pour l'expédition de ton colis",      "auto": True},
    {"id": "unboxing",  "label": "Publier ton unboxing",         "desc": "Une vidéo de réception du colis",     "auto": False},
    {"id": "video1",    "label": "Publier ta 1ʳᵉ vidéo",         "desc": "Playback ou selon ton contenu",       "auto": False},
    {"id": "video2",    "label": "Publier ta 2ᵉ vidéo",          "desc": "En portant le second maillot",        "auto": False},
]

def _slugify(text):
    """Transforme un pseudo en slug URL-safe."""
    import unicodedata, re as _re
    t = unicodedata.normalize("NFKD", str(text or "")).encode("ascii", "ignore").decode()
    t = _re.sub(r"[^a-zA-Z0-9]+", "-", t).strip("-").lower()
    return t or "influenceur"

def _get_influencer_by_slug(slug):
    """Retrouve un influenceur via son slug public (pseudo-hash)."""
    try:
        data = r2_get_json(INFLUENCEURS_R2_KEY) or {}
        for inf in data.get("influenceurs", []):
            if _public_slug(inf) == slug:
                return inf
    except Exception as e:
        print(f"[ESPACE] Erreur lecture influenceur: {e}")
    return None

def _public_slug(inf):
    """Slug public stable : pseudo-slugifié + suffixe de l'id (non devinable)."""
    base = _slugify(inf.get("pseudo") or "influenceur")
    suffix = str(inf.get("id", ""))[-6:] or "000000"
    return f"{base}-{suffix}"

def _compute_tier_progress(stats):
    """
    Détermine le palier courant et la progression vers le suivant.
    stats : {"videos": int, "views": int, "sales": int}
    Retourne (tier_index, next_tier_or_None, progress_pct, details[])
    """
    videos = int(stats.get("videos", 0) or 0)
    views  = int(stats.get("views", 0) or 0)
    sales  = int(stats.get("sales", 0) or 0)

    # Palier atteint = le plus haut dont tous les critères sont remplis
    tier_idx = 0
    for i, tier in enumerate(INFLUENCER_TIERS):
        req = tier.get("requirements") or {}
        if not req:
            tier_idx = max(tier_idx, i)
            continue
        if (videos >= req.get("videos", 0) and
            views  >= req.get("views", 0) and
            sales  >= req.get("sales", 0)):
            tier_idx = max(tier_idx, i)

    next_tier = INFLUENCER_TIERS[tier_idx + 1] if tier_idx + 1 < len(INFLUENCER_TIERS) else None

    details, pct = [], 100
    if next_tier:
        req = next_tier.get("requirements") or {}
        ratios = []
        mapping = [("videos", "Vidéos publiées", videos), ("views", "Vues cumulées", views), ("sales", "Ventes générées", sales)]
        for key, label, current in mapping:
            target = req.get(key, 0)
            if target > 0:
                r = min(1.0, current / target)
                ratios.append(r)
                details.append({
                    "key": key, "label": label,
                    "current": current, "target": target,
                    "pct": round(r * 100), "done": current >= target,
                })
        pct = round(sum(ratios) / len(ratios) * 100) if ratios else 0

    return tier_idx, next_tier, pct, details

def _espace_payload(inf):
    """Construit toutes les données affichées dans l'espace influenceur."""
    stats = dict(inf.get("stats") or {})

    # Vidéos de la bibliothèque appartenant à cet influenceur
    try:
        all_vids = _load_influ_videos()
        my_vids = [v for v in all_vids if v.get("influ_id") == inf.get("id")]
    except Exception:
        my_vids = []

    # Le nombre de vidéos publiées est TOUJOURS dérivé des vidéos réelles,
    # pas d'une valeur saisie à la main : c'est la seule source fiable.
    stats["videos"] = len(my_vids)

    tier_idx, next_tier, pct, details = _compute_tier_progress(stats)
    current_tier = INFLUENCER_TIERS[tier_idx]

    # Missions : état calculé (auto) ou déclaré
    done_map = inf.get("missions_done") or {}
    missions = []
    for m in DEFAULT_MISSIONS:
        if m["id"] == "profil":
            done = bool((inf.get("platforms") or {}))
        elif m["id"] == "maillots":
            done = len(inf.get("jerseys") or []) >= 2
        elif m["id"] == "livraison":
            done = bool(inf.get("address"))
        elif m["id"] == "unboxing":
            done = any(v.get("type") == "unboxing" for v in my_vids)
        elif m["id"] == "video1":
            done = len([v for v in my_vids if v.get("type") == "playback"]) >= 1
        elif m["id"] == "video2":
            done = len([v for v in my_vids if v.get("type") == "playback"]) >= 2
        else:
            done = bool(done_map.get(m["id"]))
        missions.append({**m, "done": done})

    return {
        "influencer": {
            "id": inf.get("id"),
            "pseudo": inf.get("pseudo") or "",
            "platform": inf.get("platform") or "",
            "promo_code": inf.get("promo_code") or "",
            "address": inf.get("address") or "",
            "platforms": inf.get("platforms") or {},
            "jerseys": inf.get("jerseys") or [],
            "shipping": inf.get("shipping") or {},
            "tracking": inf.get("tracking") or "",
            "tracking_status": inf.get("trackingStatus") or "",
            "quota": inf.get("quota") or "",
            "status": inf.get("status", 0),
        },
        "stats": {
            "views":  int(stats.get("views", 0) or 0),
            "sales":  int(stats.get("sales", 0) or 0),
            "revenue": float(stats.get("revenue", 0) or 0),
            "videos": len(my_vids),
            "commission": float(stats.get("commission", 0) or 0),
        },
        "tier": {
            "current": current_tier,
            "next": next_tier,
            "progress": pct,
            "details": details,
            "all": INFLUENCER_TIERS,
            "index": tier_idx,
        },
        "missions": missions,
        "videos": [{
            "id": v.get("id"), "type": v.get("type"), "orig_link": v.get("orig_link", ""),
            "uploaded_at": v.get("uploaded_at", ""), "filename": v.get("filename", ""),
        } for v in my_vids],
    }

@app.route("/espace/<slug>")
def espace_influenceur(slug):
    """Page publique de l'espace influenceur."""
    inf = _get_influencer_by_slug(slug)
    if not inf:
        return render_template("espace_404.html"), 404
    return render_template("espace.html", slug=slug)

@app.route("/api/espace/<slug>")
def api_espace_data(slug):
    """Données de l'espace influenceur (lecture publique)."""
    inf = _get_influencer_by_slug(slug)
    if not inf:
        return jsonify({"error": "introuvable"}), 404
    return jsonify(_espace_payload(inf))

@app.route("/api/espace/<slug>/update", methods=["POST"])
def api_espace_update(slug):
    """
    L'influenceur met à jour son propre profil (champs autorisés uniquement).
    Champs modifiables : platforms, shipping, jerseys.
    Tout le reste (stats, palier, tracking) reste sous contrôle admin.
    """
    ALLOWED = {"platforms", "shipping", "jerseys"}
    payload = request.json or {}
    updates = {k: v for k, v in payload.items() if k in ALLOWED}
    if not updates:
        return jsonify({"success": False, "error": "aucun champ modifiable"}), 400

    with _influenceurs_lock:
        data = r2_get_json(INFLUENCEURS_R2_KEY) or {}
        influenceurs = data.get("influenceurs", [])
        found = None
        for inf in influenceurs:
            if _public_slug(inf) == slug:
                found = inf
                break
        if not found:
            return jsonify({"success": False, "error": "introuvable"}), 404

        found.update(updates)
        found["lastModified"] = datetime.now(timezone.utc).isoformat()
        # Miroir de l'adresse principale pour le back-office
        if "shipping" in updates:
            sh = updates["shipping"] or {}
            parts = [sh.get("address",""), sh.get("postal",""), sh.get("city",""), sh.get("country","")]
            found["address"] = ", ".join([p for p in parts if p])

        data["influenceurs"] = influenceurs
        data["version"] = data.get("version", 0) + 1
        data["updated_at"] = datetime.now(timezone.utc).isoformat()
        r2_put_json(INFLUENCEURS_R2_KEY, data)

    return jsonify({"success": True})

@app.route("/api/espace/<slug>/video", methods=["POST"])
def api_espace_add_video(slug):
    """L'influenceur déclare une vidéo publiée (lien + type + stats déclarées)."""
    inf = _get_influencer_by_slug(slug)
    if not inf:
        return jsonify({"success": False, "error": "introuvable"}), 404
    payload = request.json or {}
    vtype = payload.get("type")
    link  = (payload.get("link") or "").strip()
    if vtype not in ("unboxing", "playback") or not link:
        return jsonify({"success": False, "error": "type ou lien invalide"}), 400

    meta = {
        "id": f"vid_{int(time.time())}_{uuid.uuid4().hex[:8]}",
        "influ_id": inf.get("id"),
        "influ_name": inf.get("pseudo", ""),
        "type": vtype,
        "orig_link": link,
        "ads_status": "",
        "r2_key": "",
        "filename": f"{vtype} déclaré",
        "size": 0,
        "declared_views": int(payload.get("views", 0) or 0),
        "declared_likes": int(payload.get("likes", 0) or 0),
        "validated": False,
        "uploaded_at": datetime.now(timezone.utc).isoformat(),
    }
    with _influ_videos_lock:
        videos = _load_influ_videos()
        videos.append(meta)
        _save_influ_videos(videos)
    return jsonify({"success": True, "video": meta})

@app.route("/api/espace/<slug>/link")
def api_espace_link(slug):
    """Retourne l'URL publique complète (pour partage côté admin)."""
    return jsonify({"url": request.host_url.rstrip("/") + "/espace/" + slug})

@app.route("/api/influenceurs/slugs")
def api_influenceurs_slugs():
    """Liste des slugs publics (back-office : récupérer le lien de chaque influenceur)."""
    try:
        data = r2_get_json(INFLUENCEURS_R2_KEY) or {}
        out = []
        for inf in data.get("influenceurs", []):
            out.append({
                "id": inf.get("id"),
                "pseudo": inf.get("pseudo", ""),
                "slug": _public_slug(inf),
                "url": request.host_url.rstrip("/") + "/espace/" + _public_slug(inf),
            })
        return jsonify({"influenceurs": out})
    except Exception as e:
        return jsonify({"influenceurs": [], "error": str(e)})

# ══════════════════════════════════════════════════════════════════════════════
# CATALOGUE GIFTING — maillots proposés aux influenceurs
#
# Deux modes :
#   "stock"     → sélection restreinte gérée à la main (stock physique réel)
#   "catalogue" → recherche large dans le catalogue Shopify (quand stock épuisé)
# Le mode est stocké dans le catalogue lui-même et bascule depuis le back-office.
# ══════════════════════════════════════════════════════════════════════════════
GIFTING_CATALOG_KEY = "meta/gifting_catalog.json"
GIFTING_IMG_PFX     = "gifting/"
GIFTING_IMG_MAX     = 8 * 1024 * 1024   # 8 Mo par photo
_gifting_lock = threading.Lock()

def _load_gifting_catalog():
    """Retourne {"mode": str, "jerseys": [...]} — jamais None."""
    try:
        data = r2_get_json(GIFTING_CATALOG_KEY) or {}
        return {
            "mode": data.get("mode", "stock"),
            "jerseys": data.get("jerseys", []) if isinstance(data.get("jerseys"), list) else [],
        }
    except Exception as e:
        print(f"[GIFTING] Erreur lecture catalogue: {e}")
        return {"mode": "stock", "jerseys": []}

def _save_gifting_catalog(cat):
    try:
        return r2_put_json(GIFTING_CATALOG_KEY, {
            "mode": cat.get("mode", "stock"),
            "jerseys": cat.get("jerseys", []),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        })
    except Exception as e:
        print(f"[GIFTING] Erreur écriture catalogue: {e}")
        return False

def _gifting_public_view(cat):
    """Version publique : uniquement les maillots actifs avec au moins une taille dispo."""
    out = []
    for j in cat.get("jerseys", []):
        if not j.get("active", True):
            continue
        sizes = {s: q for s, q in (j.get("sizes") or {}).items() if int(q or 0) > 0}
        if not sizes:
            continue
        out.append({
            "id": j.get("id"),
            "name": j.get("name", ""),
            "sub": j.get("sub", ""),
            "sizes": sizes,
            "image": r2_presigned(j["r2_key"], expires=604800) if j.get("r2_key") else "",
        })
    return out

@app.route("/catalogue")
def catalogue_page():
    return render_template("catalogue.html")

@app.route("/api/gifting/catalog", methods=["GET"])
def api_gifting_catalog_get():
    """Catalogue complet (back-office) avec URLs d'images signées."""
    cat = _load_gifting_catalog()
    for j in cat["jerseys"]:
        j["image"] = r2_presigned(j["r2_key"], expires=604800) if j.get("r2_key") else ""
    return jsonify(cat)

@app.route("/api/gifting/catalog", methods=["POST"])
def api_gifting_catalog_save():
    """Sauvegarde le catalogue (mode + liste des maillots)."""
    payload = request.json or {}
    jerseys = payload.get("jerseys")
    if jerseys is not None and not isinstance(jerseys, list):
        return jsonify({"success": False, "error": "format invalide"}), 400
    with _gifting_lock:
        cat = _load_gifting_catalog()
        if "mode" in payload and payload["mode"] in ("stock", "catalogue", "both"):
            cat["mode"] = payload["mode"]
        if jerseys is not None:
            # On ne conserve que les champs attendus (pas d'URL signée en base)
            clean = []
            for j in jerseys:
                clean.append({
                    "id":     j.get("id") or f"jr_{uuid.uuid4().hex[:10]}",
                    "name":   (j.get("name") or "").strip(),
                    "sub":    (j.get("sub") or "").strip(),
                    "r2_key": j.get("r2_key", ""),
                    "sizes":  {k: int(v or 0) for k, v in (j.get("sizes") or {}).items()},
                    "active": bool(j.get("active", True)),
                })
            cat["jerseys"] = clean
        ok = _save_gifting_catalog(cat)
    return jsonify({"success": ok, "count": len(cat["jerseys"])})

@app.route("/api/gifting/upload", methods=["POST"])
def api_gifting_upload():
    """Upload d'une photo de maillot vers R2."""
    f = request.files.get("image")
    if not f:
        return jsonify({"success": False, "error": "aucun fichier"}), 400
    data = f.read()
    if not data:
        return jsonify({"success": False, "error": "fichier vide"}), 400
    if len(data) > GIFTING_IMG_MAX:
        return jsonify({"success": False, "error": "image trop lourde (max 8 Mo)"}), 400

    fname = (f.filename or "maillot.png")
    ext = fname.rsplit(".", 1)[-1].lower() if "." in fname else "png"
    if ext not in ("png", "jpg", "jpeg", "webp"):
        ext = "png"
    base = _slugify(fname.rsplit(".", 1)[0]) or "maillot"
    key = f"{GIFTING_IMG_PFX}{base}-{uuid.uuid4().hex[:6]}.{ext}"

    if not r2_put_image(key, data, mime=f.mimetype or "image/png"):
        return jsonify({"success": False, "error": "échec upload R2"}), 500

    return jsonify({
        "success": True,
        "r2_key": key,
        "image": r2_presigned(key, expires=604800),
        "suggested_name": fname.rsplit(".", 1)[0].replace("-", " ").replace("_", " ").title(),
    })

@app.route("/api/gifting/delete_image", methods=["POST"])
def api_gifting_delete_image():
    """Supprime une photo de maillot du stockage."""
    key = (request.json or {}).get("r2_key")
    if not key or not key.startswith(GIFTING_IMG_PFX):
        return jsonify({"success": False, "error": "clé invalide"}), 400
    r2_delete(key)
    return jsonify({"success": True})

@app.route("/api/espace/<slug>/jerseys", methods=["GET"])
def api_espace_jerseys(slug):
    """Catalogue visible par l'influenceur (maillots actifs et en stock)."""
    inf = _get_influencer_by_slug(slug)
    if not inf:
        return jsonify({"error": "introuvable"}), 404
    cat = _load_gifting_catalog()
    return jsonify({"mode": cat["mode"], "jerseys": _gifting_public_view(cat)})

@app.route("/queue_ig")
def page_queue_ig(): return render_template("queue_ig.html")

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
    return jsonify({"pending": pending, "needed": max(0, 7 - pending), "note": "target_size dynamique selon carousel"})

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
    total_slots = sum(len(SCHEDULE_WINDOWS_BY_ACCOUNT.get(a, SCHEDULE_WINDOWS_DEFAULT)) for a in all_accounts)
    
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
        ratio = len(SCHEDULE_WINDOWS_BY_ACCOUNT.get(acc, SCHEDULE_WINDOWS_DEFAULT)) / total_slots
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
                # Persister dans R2 pour survivre aux restarts Railway
                try: r2_put_json("meta/last_schedule_result.json", _schedule_result["last"])
                except Exception: pass
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
    # Si c'est un objet Response Flask, extraire les données
    if hasattr(result, 'get_json'):
        return result
    if hasattr(result, 'json'):
        return result
    return jsonify(result)

def get_or_create_slot_time(account, window_index, date_str):
    """
    Retourne l'heure UTC (HH:MM) pour un créneau donné (compte + index fenêtre + date).

    Les fenêtres SCHEDULE_WINDOWS_* sont définies en HEURE DE PARIS. On tire une heure
    aléatoire dans la fenêtre en heure de Paris, puis on convertit en UTC selon la saison
    de la date concernée (été UTC+2 / hiver UTC+1) via ZoneInfo("Europe/Paris").
    Ainsi les horaires français restent constants été comme hiver.

    - Si une heure a déjà été persistée pour ce créneau → la réutiliser TELLE QUELLE
      (les créneaux déjà dans meta/scheduled_slots_plan.json ne sont jamais recalculés).
    - Sinon → tirer aléatoirement dans la fenêtre Paris, convertir en UTC, persister.

    L'espacement minimum de 90 min entre créneaux est calculé en HEURE DE PARIS.
    """
    import random as _rnd
    from zoneinfo import ZoneInfo
    paris_tz = ZoneInfo("Europe/Paris")

    plan_r2_key = "meta/scheduled_slots_plan.json"
    persist_key = f"{account}|{date_str}|{window_index}"

    # Lire le plan persisté depuis R2
    try:
        plan = r2_get_json(plan_r2_key) or {}
    except Exception:
        plan = {}

    # Si déjà calculé → réutiliser sans recalcul (protège l'existant)
    if persist_key in plan:
        return plan[persist_key]

    # Trouver la fenêtre correspondante (heures en Paris)
    windows = SCHEDULE_WINDOWS_BY_ACCOUNT.get(account, SCHEDULE_WINDOWS_DEFAULT)
    w_idx = window_index % len(windows)
    window = windows[w_idx]

    start_h, start_m = map(int, window["start"].split(":"))
    end_h,   end_m   = map(int, window["end"].split(":"))
    start_total = start_h * 60 + start_m   # minutes Paris
    end_total   = end_h   * 60 + end_m     # minutes Paris

    # Collecter les heures Paris déjà planifiées pour ce compte/date (espacement 90 min)
    # Les valeurs persistées sont en UTC → on les reconvertit en Paris pour comparer.
    y, mo, d = map(int, date_str.split("-"))
    existing_minutes_paris = []
    for k, v in plan.items():
        if k.startswith(f"{account}|{date_str}|"):
            try:
                uh, um = map(int, v.split(":"))
                # v est une heure UTC → reconvertir en Paris pour ce jour
                utc_dt = datetime(y, mo, d, uh, um, tzinfo=timezone.utc)
                p_dt   = utc_dt.astimezone(paris_tz)
                existing_minutes_paris.append(p_dt.hour * 60 + p_dt.minute)
            except Exception:
                pass

    # Tirer une heure Paris respectant l'espacement minimum de 90 min (en Paris)
    chosen_total = None
    for _ in range(20):
        candidate = _rnd.randint(start_total, end_total)
        too_close = any(abs(candidate - et) < 90 for et in existing_minutes_paris)
        if not too_close:
            chosen_total = candidate
            break

    if chosen_total is None:
        # Impossible d'éviter les conflits → milieu de la fenêtre
        chosen_total = (start_total + end_total) // 2
        print(f"[SCHEDULER] ⚠️ Conflit inévitable {account}/{date_str}/{window_index} → milieu fenêtre")

    # Convertir l'heure Paris tirée → UTC pour la date concernée (gère été/hiver)
    paris_dt = datetime(y, mo, d, chosen_total // 60, chosen_total % 60, tzinfo=paris_tz)
    utc_dt   = paris_dt.astimezone(timezone.utc)
    time_str = utc_dt.strftime("%H:%M")   # heure UTC finale, persistée

    # Persister pour survivre aux redémarrages Railway
    try:
        plan[persist_key] = time_str
        r2_put_json(plan_r2_key, plan)
        print(f"[SCHEDULER] Créneau persisté: {account} {date_str} fenêtre {window_index} → "
              f"{chosen_total//60:02d}:{chosen_total%60:02d} Paris = {time_str} UTC")
    except Exception as e:
        print(f"[SCHEDULER] Erreur persistance créneau: {e}")

    return time_str


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

        # Forcer un rebuild complet pour avoir les vrais créneaux occupés
        used_slots_idx = rebuild_used_slots_index()
        used_slots = set(used_slots_idx.get(account, []))
        account_windows = SCHEDULE_WINDOWS_BY_ACCOUNT.get(account, SCHEDULE_WINDOWS_DEFAULT)
        n_windows = len(account_windows)

        # Repartir depuis maintenant pour trouver les trous
        scan_date = start_date
        scan_index = 0

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
                # Scanner depuis le début pour trouver le premier trou disponible
                # Utilise get_or_create_slot_time → heure aléatoire dans la fenêtre, persistée
                while True:
                    window_index = scan_index % n_windows
                    date_str     = scan_date.isoformat()
                    time_str     = get_or_create_slot_time(account, window_index, date_str)
                    h, m         = map(int, time_str.split(":"))
                    slot_dt      = datetime(scan_date.year, scan_date.month, scan_date.day, h, m, tzinfo=timezone.utc)
                    slot_iso     = slot_dt.isoformat()
                    is_future_enough = slot_dt > now + timedelta(minutes=30)
                    if is_future_enough and slot_iso not in used_slots:
                        used_slots.add(slot_iso)
                        break
                    scan_index += 1
                    if scan_index % n_windows == 0:
                        scan_date += timedelta(days=1)

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
                    if result.get("requeue"):
                        # Remettre dans la file d'attente avec badge d'erreur
                        tiktok_data["status"] = "pending"
                        tiktok_data["account"] = None
                        tiktok_data["image_error"] = result["error"]
                        r2_put_json(tiktok["r2_key"], tiktok_data)
                        print(f"[METRICOOL] TikTok {tiktok.get('number','')} renvoyé en file d'attente: {result['error']}")
                        return {"error": f"TikTok {tiktok.get('number','')}: {result['error']}"}
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
    total_jobs = len(jobs)
    completed = {"count": 0, "errors": [], "details": []}
    completed_lock = threading.Lock()

    def process_and_update(job):
        result = process_schedule_job(job)
        with completed_lock:
            completed["count"] += 1
            if result.get("error"):
                completed["errors"].append(result["error"])
            else:
                completed["details"].append({"tiktok": result["tiktok"], "account": result["account"], "time": result["time"]})
            # Mise à jour progressive visible par le frontend
            _schedule_result["last"] = {
                "pending": True,  # toujours True pendant la progression
                "scheduled": len(completed["details"]),
                "total": total_jobs,
                "errors": completed["errors"][:],
                "progress": f"{completed['count']}/{total_jobs}"
            }
        return result

    with ThreadPoolExecutor(max_workers=5) as ex:
        results_jobs = list(ex.map(process_and_update, jobs))

    for r in results_jobs:
        if r.get("error"):
            errors.append(r["error"])
        else:
            scheduled_count += 1
            scheduled_details.append({"tiktok": r["tiktok"], "account": r["account"], "time": r["time"]})

    final_result = {
        "success": True,
        "pending": False,
        "scheduled": scheduled_count,
        "details": scheduled_details,
        "errors": errors,
        "completed_at": datetime.now(timezone.utc).strftime("%d/%m/%Y à %Hh%M")
    }
    # Persister dans R2 pour affichage même si l'utilisateur revient plus tard
    try: r2_put_json("meta/last_schedule_result.json", final_result)
    except Exception: pass
    return jsonify(final_result)

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

    # Recalculer les créneaux avec le système de fenêtres aléatoires persistées
    account_windows = SCHEDULE_WINDOWS_BY_ACCOUNT.get(account, SCHEDULE_WINDOWS_DEFAULT)
    n_windows = len(account_windows)
    print(f"[FIX_SLOTS] {account}: {len(tiktoks)} TikToks, {n_windows} fenêtres/jour")

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

        # Trouver le prochain créneau disponible (fenêtres aléatoires persistées)
        while True:
            window_index = slot_index % n_windows
            date_str = slot_date.isoformat()
            time_str = get_or_create_slot_time(account, window_index, date_str)
            h, m = map(int, time_str.split(":"))
            slot_dt = datetime(slot_date.year, slot_date.month, slot_date.day, h, m, tzinfo=timezone.utc)
            slot_iso = slot_dt.isoformat()
            if slot_dt > now + timedelta(minutes=30) and slot_iso not in used_slots:
                break
            slot_index += 1
            if slot_index % n_windows == 0:
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
        if slot_index % n_windows == 0:
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
        tiktok["real_status"] = None
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
        slots_per_day = len(SCHEDULE_WINDOWS_BY_ACCOUNT.get(acc, SCHEDULE_WINDOWS_DEFAULT))
        tiktoks_needed = slots_per_day * days
        already_scheduled = scheduled_by_acc.get(acc, 0)
        in_queue = queue_by_acc.get(acc, 0)
        missing = max(0, tiktoks_needed - already_scheduled - in_queue)
        plan[acc] = {
            "needed": tiktoks_needed,
            "scheduled": already_scheduled,
            "in_queue": in_queue,
            "missing": missing,
            "images_to_generate": missing * 9  # estimation : taille moyenne carousel (distribution 7-12)
        }
        total_needed += missing * 9  # estimation : taille moyenne carousel
    
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

@app.route("/api/queue_ig/all")
def api_queue_ig_all():
    """Retourne tous les posts Instagram en attente"""
    keys = sorted(r2_list_keys(PFX_QUEUE_IG))
    keys = [k for k in keys if "/imgs/" not in k]
    tiktoks = []
    for k in keys:
        d = r2_get_json(k)
        if d:
            d["r2_key"] = k
            tiktoks.append(_enrich_tiktok(d, k))
    return jsonify({"tiktoks": tiktoks, "total": len(tiktoks)})

@app.route("/api/queue_ig/schedule", methods=["POST"])
def api_schedule_ig():
    """Programme des posts Instagram"""
    if not _schedule_lock.acquire(blocking=False):
        return jsonify({"error": "Programmation en cours"}), 429
    
    data = request.json or {}
    _schedule_result_ig["last"] = {"pending": True}
    
    def run_schedule_ig():
        try:
            with app.app_context():
                result = _do_schedule_ig(data)
                _schedule_result_ig["last"] = result.get_json() if hasattr(result, 'get_json') else result
        except Exception as e:
            import traceback; traceback.print_exc()
            _schedule_result_ig["last"] = {"scheduled": 0, "errors": [str(e)], "pending": False}
        finally:
            _schedule_lock.release()
    
    threading.Thread(target=run_schedule_ig, daemon=True).start()
    return jsonify({"success": True, "background": True, "message": "Programmation Instagram lancée"})

@app.route("/api/queue_ig/schedule/status")
def api_schedule_ig_status():
    result = _schedule_result_ig.get("last")
    if result is None:
        return jsonify({"pending": True})
    if hasattr(result, 'get_json'):
        return result
    return jsonify(result)

def _do_schedule_ig(data=None):
    """Scheduling Instagram"""
    if data is None: data = {}
    start_date_str = data.get("start_date")
    now = datetime.now(timezone.utc)
    
    try:
        if start_date_str:
            from datetime import date as _date
            y,mo,d = map(int, start_date_str.split("-"))
            start_date = _date(y,mo,d)
        else:
            start_date = now.date()
    except Exception:
        start_date = now.date()
    
    # Récupérer les posts Instagram assignés
    keys = sorted(r2_list_keys(PFX_QUEUE_IG))
    keys = [k for k in keys if "/imgs/" not in k]
    queue = []
    for k in keys:
        d = r2_get_json(k)
        if d and d.get("account") and d.get("ig_status","pending") == "pending":
            d["r2_key"] = k
            queue.append(d)
    
    if not queue:
        return jsonify({"success": True, "pending": False, "scheduled": 0, "errors": [], "details": []})
    
    # Reconstruire les créneaux utilisés Instagram
    ig_used = set()
    for sk in r2_list_keys(PFX_SCHEDULED_IG):
        if "/imgs/" in sk: continue
        sd = r2_get_json(sk)
        if sd and sd.get("scheduled_at"):
            ig_used.add(sd["scheduled_at"])
    
    scheduled_count = 0
    errors = []
    details = []
    
    for post in queue:
        account = post.get("account", "Volakits Instagram")
        ig_acc = INSTAGRAM_ACCOUNTS.get(account) or list(INSTAGRAM_ACCOUNTS.values())[0]
        
        # Trouver le prochain créneau libre
        scan_date = start_date
        scan_idx = 0
        while True:
            h, m = map(int, INSTAGRAM_SCHEDULE_TIMES[scan_idx % len(INSTAGRAM_SCHEDULE_TIMES)].split(":"))
            slot_dt = datetime(scan_date.year, scan_date.month, scan_date.day, h, m, tzinfo=timezone.utc)
            slot_iso = slot_dt.isoformat()
            if slot_dt > now + timedelta(minutes=30) and slot_iso not in ig_used:
                ig_used.add(slot_iso)
                break
            scan_idx += 1
            if scan_idx % len(INSTAGRAM_SCHEDULE_TIMES) == 0:
                from datetime import timedelta as _td
                scan_date = scan_date + _td(days=1)
        
        # Convertir en heure Paris
        from zoneinfo import ZoneInfo
        paris_tz = ZoneInfo("Europe/Paris")
        dt_paris = slot_dt.astimezone(paris_tz)
        publish_time = dt_paris.strftime("%Y-%m-%dT%H:%M:%S")
        display_time = dt_paris.strftime("%d/%m/%Y à %Hh%M")
        
        image_keys = post.get("image_keys", [])
        image_urls = [f"{R2_PUBLIC_URL}/{k}" for k in image_keys if k]
        
        result = schedule_instagram(
            image_urls=image_urls,
            caption=FIXED_CAPTION,
            publish_time_iso=publish_time,
            blog_id=ig_acc["blog_id"]
        )
        
        if result["success"]:
            # Déplacer vers scheduled_ig
            post["ig_status"] = "scheduled"
            post["ig_post_id"] = result.get("post_id")
            post["scheduled_at"] = slot_iso
            r2_put_json(post["r2_key"].replace(PFX_QUEUE_IG, PFX_SCHEDULED_IG), post)
            try: get_r2().delete_object(Bucket=R2_BUCKET, Key=post["r2_key"])
            except Exception: pass
            scheduled_count += 1
            details.append({"account": account, "time": display_time})
            print(f"[INSTAGRAM] ✅ Post programmé pour {display_time}")
        else:
            errors.append(f"Post {post.get('number','?')}: {result['error']}")
    
    final = {
        "success": True, "pending": False,
        "scheduled": scheduled_count, "errors": errors, "details": details,
        "completed_at": datetime.now(timezone.utc).strftime("%d/%m/%Y à %Hh%M")
    }
    try: r2_put_json("meta/last_ig_schedule_result.json", final)
    except Exception: pass
    return jsonify(final)

_schedule_result_ig = {}

@app.route("/api/queue_ig/dispatch_smart", methods=["POST"])
def api_dispatch_ig_smart():
    """Auto-dispatch Instagram — assigne tous les posts non assignés au compte Instagram"""
    keys = [k for k in r2_list_keys(PFX_QUEUE_IG) if "/imgs/" not in k]
    ig_account = list(INSTAGRAM_ACCOUNTS.keys())[0]
    count = 0
    for k in keys:
        t = r2_get_json(k)
        if t and not t.get("account"):
            t["account"] = ig_account
            r2_put_json(k, t)
            count += 1
    return jsonify({"success": True, "count": count, "by_account": {ig_account: count}})

@app.route("/api/queue_ig/copy_from_tiktok", methods=["POST"])
def api_copy_tiktok_to_ig():
    """Copie tous les TikToks de la file d'attente vers la file d'attente Instagram"""
    keys = [k for k in r2_list_keys(PFX_QUEUE) if "/imgs/" not in k]
    copied = 0
    skipped = 0
    for k in keys:
        t = r2_get_json(k)
        if not t: continue
        ig_key = k.replace(PFX_QUEUE, PFX_QUEUE_IG)
        # Vérifier si déjà dans queue_ig
        existing = r2_get_json(ig_key)
        if existing:
            skipped += 1
            continue
        ig_meta = {**t, "platform": "instagram", "ig_status": "pending", "account": None}
        r2_put_json(ig_key, ig_meta)
        copied += 1
    return jsonify({"success": True, "copied": copied, "skipped": skipped})

@app.route("/api/queue_ig/unassign_all", methods=["POST"])
def api_unassign_ig_all():
    keys = [k for k in r2_list_keys(PFX_QUEUE_IG) if "/imgs/" not in k]
    done = 0
    for k in keys:
        t = r2_get_json(k)
        if t and t.get("account"):
            t["account"] = None
            r2_put_json(k, t)
            done += 1
    return jsonify({"success": True, "unassigned": done})

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
    # Merger au lieu d'écraser — préserve les catégories S/A/B et les pepites existants
    existing = r2_get_json("meta/flocages.json") or {}
    existing["flocages"] = data.get("flocages", [])
    r2_put_json("meta/flocages.json", existing)
    return jsonify({"success": True})

# ── Routes catégories templates S/A/B (BLOC 2) ──────────────────────────────
@app.route("/api/templates/categories", methods=["GET"])
def api_get_templates_categories():
    """Retourne les catégories S/A/B des templates"""
    return jsonify(_load_templates_categories())

@app.route("/api/templates/categories", methods=["POST"])
def api_save_templates_categories():
    """Sauvegarde les catégories S/A/B des templates"""
    data = request.json or {}
    ok = _save_templates_categories({"S": data.get("S", []), "A": data.get("A", []), "B": data.get("B", [])})
    return jsonify({"success": ok})

# ── Routes catégories flocages S/A/B (BLOC 2) ───────────────────────────────
@app.route("/api/flocages/categories", methods=["GET"])
def api_get_flocages_categories():
    """Retourne les catégories S/A/B des flocages"""
    return jsonify(_load_flocages_categories())

@app.route("/api/flocages/categories", methods=["POST"])
def api_save_flocages_categories():
    """Sauvegarde les catégories S/A/B des flocages"""
    data = request.json or {}
    ok = _save_flocages_categories({"S": data.get("S", []), "A": data.get("A", []), "B": data.get("B", [])})
    return jsonify({"success": ok})

# ── Routes anti-répétition (BLOC 2) ─────────────────────────────────────────
@app.route("/api/recent_used", methods=["GET"])
def api_get_recent_used_route():
    """Retourne les éléments récemment utilisés (debug/info)"""
    return jsonify(_get_recent_used())

@app.route("/api/recent_used/reset", methods=["POST"])
def api_reset_recent_used():
    """Remet à zéro l'historique anti-répétition"""
    try:
        r2_put_json("meta/recent_used.json", {"templates": [], "flocages": []})
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

# ── Routes scheduler plan persisté (BLOC 7) ─────────────────────────────────
@app.route("/api/scheduler/slots_plan", methods=["GET"])
def api_get_slots_plan():
    """Retourne le plan d'heures persistées par créneau (debug/info)"""
    try:
        plan = r2_get_json("meta/scheduled_slots_plan.json") or {}
        return jsonify(plan)
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route("/api/scheduler/slots_plan/reset", methods=["POST"])
def api_reset_slots_plan():
    """Vide le plan d'heures persistées (force recalcul des créneaux)"""
    try:
        r2_put_json("meta/scheduled_slots_plan.json", {})
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

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
        # schedule_times : conservé pour rétrocompat frontend (calendar.html lit .length)
        # Généré depuis les fenêtres → .length = nb réel de publications/jour (3)
        "schedule_times": {
            acc: [f'{w["start"]}-{w["end"]}' for w in SCHEDULE_WINDOWS_BY_ACCOUNT.get(acc, SCHEDULE_WINDOWS_DEFAULT)]
            for acc in accounts_list
        },
        # schedule_windows : nouveau format complet (fenêtres), pour évolution frontend
        "schedule_windows": {
            acc: SCHEDULE_WINDOWS_BY_ACCOUNT.get(acc, SCHEDULE_WINDOWS_DEFAULT)
            for acc in accounts_list
        },
    })


def _auto_check_statuses():
    """Vérifie automatiquement les statuts Metricool toutes les heures"""
    import time as _time
    _time.sleep(60)  # attendre 1 minute au démarrage
    while True:
        try:
            with app.app_context():
                if METRICOOL_TOKEN:
                    keys = [k for k in r2_list_keys(PFX_SCHEDULED) if "/imgs/" not in k]
                    updated = 0
                    for key in keys:
                        tiktok = r2_get_json(key)
                        if not tiktok: continue
                        mc_id = tiktok.get("metricool_post_id")
                        if not mc_id: continue
                        acc = tiktok.get("account","")
                        mc_acc = METRICOOL_ACCOUNTS.get(acc, {})
                        if not mc_acc.get("active"): continue
                        try:
                            resp = requests.get(
                                f"https://app.metricool.com/api/v2/scheduler/posts/{mc_id}?userId={METRICOOL_USER_ID}&blogId={mc_acc['blog_id']}",
                                headers={"X-Mc-Auth": METRICOOL_TOKEN},
                                timeout=10
                            )
                            if resp.status_code == 200:
                                data = resp.json().get("data", {})
                                providers = data.get("providers", [])
                                new_status = providers[0].get("status","") if providers else ""
                                if new_status and new_status != tiktok.get("real_status"):
                                    tiktok["real_status"] = new_status
                                    r2_put_json(key, tiktok)
                                    updated += 1
                            elif resp.status_code == 404:
                                # Post publié — plus dans scheduler
                                tiktok["real_status"] = "PUBLISHED"
                                r2_put_json(key, tiktok)
                                updated += 1
                        except Exception:
                            pass
                    if updated:
                        print(f"[AUTO_STATUS] {updated} statuts mis à jour")
        except Exception as e:
            print(f"[AUTO_STATUS] Erreur: {e}")
        _time.sleep(3600)  # toutes les heures

# Lancer le thread de vérification automatique des statuts
threading.Thread(target=_auto_check_statuses, daemon=True).start()

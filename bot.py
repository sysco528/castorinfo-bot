# -*- coding: utf-8 -*-
"""
Castor Info — bot de veille et de republication d'actualité
============================================================
1. Lit les nouveaux tweets des comptes sources (via twitterapi.io, recherche avancée)
2. Les reformule en dépêches Castor Info (via Claude Haiku)
3. Publie sur @castorinfo (via l'API X officielle)

Conçu pour tourner toutes les 15 minutes sur GitHub Actions.
L'état (dernier passage, tweets déjà traités, compteur du jour)
est conservé dans state.json, commité par le workflow.
"""

import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
import tweepy
import anthropic

# ============================================================
# RÉGLAGES — c'est ici qu'on ajuste le comportement du bot
# ============================================================
COMPTES_SOURCES = ["CerfiaFR", "AlertesInfos", "ImpactMediaFR"]

MAX_POSTS_PAR_RUN = 2     # dépêches max publiées par passage (toutes les 15 min)
MAX_POSTS_PAR_JOUR = 10   # plafond quotidien — MONTÉE EN CHARGE :
                          # semaine 1-2 : 10 | semaine 3-4 : 20 | ensuite : 30
FENETRE_MAX_MINUTES = 45  # au 1er lancement (ou après une panne), on ne remonte
                          # pas plus loin que ça dans le passé
MODELE_CLAUDE = "claude-haiku-4-5-20251001"
FICHIER_ETAT = "state.json"
# ============================================================

# --- Clés lues depuis les secrets GitHub (jamais en clair dans le code) ---
TWITTERAPI_KEY = os.environ["TWITTERAPI_KEY"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
X_API_KEY = os.environ["X_API_KEY"]
X_API_SECRET = os.environ["X_API_SECRET"]
X_ACCESS_TOKEN = os.environ["X_ACCESS_TOKEN"]
X_ACCESS_SECRET = os.environ["X_ACCESS_SECRET"]

PARIS = ZoneInfo("Europe/Paris")

PROMPT_SYSTEME = """Tu es le rédacteur en chef du compte X « Castor Info » (@castorinfo), \
un média de dépêches qui couvre l'actualité française et internationale.

À partir des tweets bruts fournis, tu produis des dépêches prêtes à publier.

RÈGLES ÉDITORIALES :
- Fusionne les doublons : si plusieurs tweets couvrent la même information, produis UNE seule dépêche.
- Sélectionne uniquement les informations les plus importantes ou marquantes. Ignore : sondages, \
auto-promotion, jeux-concours, réactions sans fait nouveau, contenus sans valeur d'actualité.
- Reformule ENTIÈREMENT avec tes propres mots. Ne copie jamais la formulation d'origine : \
les faits sont libres, la formulation ne l'est pas.
- Format d'une dépêche : signal en tête, puis 1 à 3 phrases courtes, factuelles, percutantes.
  Signaux possibles :
  « 🔴 ALERTE — » : breaking majeur (mort, attentat, catastrophe, démission, décision historique)
  « ⚡ FLASH — » : information chaude importante
  Sinon : drapeau du pays concerné (🇫🇷 🇺🇸 🇷🇺 🇨🇳 🇮🇱 …) ou emoji du thème (⚽ 💰 🎬 ⚖️ ✈️ 🏛️)
- Si le tweet d'origine cite un média (AFP, BFMTV, Le Parisien, Reuters…), termine la dépêche \
par la source entre parenthèses, ex. (AFP). Sinon, pas de source.
- STRICTEMENT INTERDIT : liens/URL, hashtags, mentions @, dépasser 275 caractères.

RÉPONDS UNIQUEMENT avec un JSON valide, sans aucun texte autour, au format exact :
{"depeches": [{"texte": "..."}]}
Si aucune information ne mérite publication : {"depeches": []}"""


# ------------------------------------------------------------
# Gestion de l'état
# ------------------------------------------------------------
def charger_etat():
    if os.path.exists(FICHIER_ETAT):
        try:
            with open(FICHIER_ETAT, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠️ state.json illisible ({e}), on repart de zéro.")
    return {"depuis_utc": None, "deja_vus": [], "compteur": {"date": "", "publies": 0}}


def sauver_etat(etat):
    with open(FICHIER_ETAT, "w", encoding="utf-8") as f:
        json.dump(etat, f, ensure_ascii=False, indent=2)


# ------------------------------------------------------------
# Étape 1 — Lecture des nouveaux tweets (twitterapi.io)
# ------------------------------------------------------------
def recuperer_nouveaux_tweets(depuis, jusqua):
    """Recherche avancée : ne facture que les tweets réellement retournés."""
    comptes = " OR ".join(f"from:{c}" for c in COMPTES_SOURCES)
    query = (
        f"({comptes}) "
        f"since:{depuis.strftime('%Y-%m-%d_%H:%M:%S_UTC')} "
        f"until:{jusqua.strftime('%Y-%m-%d_%H:%M:%S_UTC')}"
    )
    url = "https://api.twitterapi.io/twitter/tweet/advanced_search"
    headers = {"X-API-Key": TWITTERAPI_KEY}

    tweets, cursor, pages = [], "", 0
    while pages < 3:  # garde-fou : 3 pages max (60 tweets), largement assez pour 15 min
        params = {"query": query, "queryType": "Latest"}
        if cursor:
            params["cursor"] = cursor
        r = requests.get(url, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        lot = data.get("tweets") or (data.get("data") or {}).get("tweets") or []
        tweets.extend(lot)
        if not data.get("has_next_page") or not data.get("next_cursor"):
            break
        cursor = data["next_cursor"]
        pages += 1
    return tweets


def nettoyer_texte(texte):
    """Retire les liens t.co et les espaces superflus avant envoi à Claude."""
    texte = re.sub(r"https?://\S+", "", texte)
    return re.sub(r"\s+", " ", texte).strip()


# ------------------------------------------------------------
# Étape 2 — Reformulation (Claude Haiku)
# ------------------------------------------------------------
def reformuler(tweets_bruts, nb_max):
    client_ia = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    lignes = []
    for t in tweets_bruts:
        auteur = (t.get("author") or {}).get("userName", "inconnu")
        lignes.append(f"— @{auteur} : {nettoyer_texte(t.get('text', ''))}")

    demande = (
        f"Voici les nouveaux tweets des comptes sources. Produis au MAXIMUM {nb_max} "
        f"dépêche(s), en ne gardant que le plus important :\n\n" + "\n".join(lignes)
    )

    msg = client_ia.messages.create(
        model=MODELE_CLAUDE,
        max_tokens=1500,
        system=PROMPT_SYSTEME,
        messages=[{"role": "user", "content": demande}],
    )
    brut = msg.content[0].text.strip()
    brut = re.sub(r"^```(?:json)?|```$", "", brut, flags=re.MULTILINE).strip()

    try:
        depeches = json.loads(brut).get("depeches", [])
    except json.JSONDecodeError:
        print(f"⚠️ Réponse IA non parsable, run ignoré :\n{brut[:300]}")
        return []

    # Garde-fous de sécurité (coût et format)
    valides = []
    for d in depeches[:nb_max]:
        texte = (d.get("texte") or "").strip()
        if not texte:
            continue
        if "http" in texte.lower() or "#" in texte or "@" in texte:
            print(f"⛔ Dépêche rejetée (lien/hashtag/mention) : {texte[:80]}")
            continue  # un lien coûterait 0,20 $ le post au lieu de 0,015 $ !
        if len(texte) > 280:
            texte = texte[:277] + "…"
        valides.append(texte)
    return valides


# ------------------------------------------------------------
# Étape 3 — Publication sur X
# ------------------------------------------------------------
def publier(depeches):
    client_x = tweepy.Client(
        consumer_key=X_API_KEY,
        consumer_secret=X_API_SECRET,
        access_token=X_ACCESS_TOKEN,
        access_token_secret=X_ACCESS_SECRET,
    )
    publiees = 0
    for texte in depeches:
        try:
            client_x.create_tweet(text=texte)
            publiees += 1
            print(f"✅ Publié : {texte}")
            time.sleep(3)
        except tweepy.TooManyRequests:
            print("⛔ Limite de débit X atteinte, on arrête ce passage.")
            break
        except tweepy.TweepyException as e:
            print(f"⚠️ Échec de publication ({e}) : {texte[:80]}")
    return publiees


# ------------------------------------------------------------
# Programme principal
# ------------------------------------------------------------
def main():
    etat = charger_etat()
    maintenant = datetime.now(timezone.utc)
    aujourd_hui = datetime.now(PARIS).strftime("%Y-%m-%d")

    # Compteur quotidien (heure de Paris)
    if etat["compteur"].get("date") != aujourd_hui:
        etat["compteur"] = {"date": aujourd_hui, "publies": 0}
    quota_restant = MAX_POSTS_PAR_JOUR - etat["compteur"]["publies"]

    # Fenêtre de lecture
    if etat.get("depuis_utc"):
        depuis = datetime.fromisoformat(etat["depuis_utc"])
    else:
        depuis = maintenant - timedelta(minutes=FENETRE_MAX_MINUTES)
        print("🦫 Premier lancement : lecture des dernières "
              f"{FENETRE_MAX_MINUTES} minutes seulement.")
    plancher = maintenant - timedelta(minutes=FENETRE_MAX_MINUTES)
    if depuis < plancher:
        depuis = plancher
    depuis_requete = depuis - timedelta(minutes=2)  # chevauchement anti-trou

    # Lecture
    try:
        bruts = recuperer_nouveaux_tweets(depuis_requete, maintenant)
    except Exception as e:
        print(f"⛔ Erreur de lecture twitterapi.io : {e}")
        sys.exit(0)  # on réessaiera au prochain passage

    deja_vus = set(etat.get("deja_vus", []))
    nouveaux = [
        t for t in bruts
        if t.get("id") and t["id"] not in deja_vus
        and not t.get("isReply")
        and not t.get("text", "").startswith("RT @")
    ]
    print(f"📥 {len(bruts)} tweet(s) reçus, {len(nouveaux)} nouveau(x) à traiter. "
          f"Quota du jour restant : {quota_restant}.")

    # Marquer comme vus (même si non publiés) pour ne jamais retraiter
    ids_traites = [t["id"] for t in nouveaux]
    etat["deja_vus"] = (etat.get("deja_vus", []) + ids_traites)[-400:]
    etat["depuis_utc"] = maintenant.isoformat()

    if not nouveaux or quota_restant <= 0:
        if quota_restant <= 0:
            print("💤 Plafond quotidien atteint, rien ne sera publié aujourd'hui.")
        sauver_etat(etat)
        return

    # Reformulation + publication
    nb_max = min(MAX_POSTS_PAR_RUN, quota_restant)
    try:
        depeches = reformuler(nouveaux, nb_max)
    except Exception as e:
        print(f"⛔ Erreur Claude : {e}")
        sauver_etat(etat)
        sys.exit(0)

    if not depeches:
        print("🤷 Rien d'assez important à publier ce passage.")
        sauver_etat(etat)
        return

    publiees = publier(depeches)
    etat["compteur"]["publies"] += publiees
    sauver_etat(etat)
    print(f"🦫 Passage terminé : {publiees} dépêche(s) publiée(s), "
          f"{etat['compteur']['publies']}/{MAX_POSTS_PAR_JOUR} aujourd'hui.")


if __name__ == "__main__":
    main()

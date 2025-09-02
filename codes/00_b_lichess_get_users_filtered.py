import os
import gzip
import time
import json
import random
import requests
from itertools import cycle
from datetime import datetime, timezone

# =========================
# CONFIG
# Selezioni utenti con almeno 3 anni di iscrizione e almeno 500 partite rated
# Possibilità di filtrare utenti che abbiano compilato il real name
# =========================

INPUT_USERS_GZ   = r"output\lichess_list.txt.gz"          # lista da 00_lichess_get_users.py
OUTPUT_USERS_GZ  = r"output\lichess_list_filtered.txt.gz" # lista filtrata
TOKEN_FILE       = r"input\TOKEN.txt"                     # un token per riga

MIN_ACCOUNT_YEARS = 3
MIN_TOTAL_GAMES   = 500
COUNT_FIELD       = "rated"  # "all" oppure "rated" # utente abbia giocato un certo numero di partite con rating 

INCLUDE_ONLY_WITH_REAL_NAME = False  # ON/OFF filtro per real name

REQUEST_TIMEOUT = 30
MAX_RETRIES     = 4
BACKOFF_BASE    = 1.8
POLITE_DELAY_S  = 0.10

# =========================
USER_AGENT      = "ChessResearchFilter/1.2 (contact: master.aida.chess00@gmail.com)"

"""per molte chiamate API:

aiuta il team di Lichess a capire chi sta facendo le richieste

se c’è un problema o abuso, possono usare la mail tra parentesi.

evita che il traffico sia visto come “anonimo” o potenzialmente sospetto"""

# =========================
# UTILS
# =========================

def load_tokens(path):
    with open(path, "r", encoding="utf-8") as f:
        toks = [t.strip() for t in f if t.strip()]
    if not toks:
        raise RuntimeError("Nessun token trovato.")
    return cycle(toks)

def read_users_gz(path):
    users = []
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            u = line.strip()
            if u:
                users.append(u)
    return users

def write_users_gz(path, users):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8", newline="\n") as f:
        for u in users:
            f.write(u + "\n")

def account_age_years(created_at_ms):
    if not isinstance(created_at_ms, int):
        return 0.0
    created = datetime.fromtimestamp(created_at_ms / 1000.0, tz=timezone.utc)
    now = datetime.now(timezone.utc)
    return (now - created).days / 365.2425

def has_real_name_profile(profile: dict) -> bool:
    if not isinstance(profile, dict):
        return False
    for key in ("realName", "firstName", "lastName", "name", "fullName"):
        val = profile.get(key)
        if isinstance(val, str) and val.strip():
            return True
    bio = profile.get("bio")
    if isinstance(bio, str):
        tokens = bio.strip().split()
        if len(tokens) >= 2 and any(c.isalpha() for c in bio):
            return True
    return False

# =========================
# API
# =========================

def fetch_user_public(username, token):
    url = f"https://lichess.org/api/user/{username}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "User-Agent": USER_AGENT,
    }
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 404:
                return None
            if r.status_code == 429:
                wait_s = 60 + random.uniform(0, 15)
                time.sleep(wait_s)
                continue
            backoff = (BACKOFF_BASE ** (attempt - 1)) + random.uniform(0, 0.5)
            time.sleep(backoff)
        except requests.RequestException:
            backoff = (BACKOFF_BASE ** (attempt - 1)) + random.uniform(0, 0.5)
            time.sleep(backoff)
    return None

# =========================
# FILTRO
# =========================

def user_passes_filters(user_obj) -> bool:
    if not user_obj:
        return False
    if account_age_years(user_obj.get("createdAt")) < MIN_ACCOUNT_YEARS:
        return False
    cnt = user_obj.get("count") or {}
    total = cnt.get(COUNT_FIELD)
    if not isinstance(total, int) or total < MIN_TOTAL_GAMES:
        return False
    if INCLUDE_ONLY_WITH_REAL_NAME:
        if not has_real_name_profile(user_obj.get("profile", {})):
            return False
    return True

# =========================
# MAIN
# =========================

def main():
    users = read_users_gz(INPUT_USERS_GZ)
    print(f"Utenti in input: {len(users)}")

    token_cycle = load_tokens(TOKEN_FILE)

    kept = []
    for i, username in enumerate(users, 1):
        token = next(token_cycle)
        data  = fetch_user_public(username, token)
        if user_passes_filters(data):
            kept.append(username)

        if i % 50 == 0:
            print(f"[{i}/{len(users)}] passano finora: {len(kept)}")

        time.sleep(POLITE_DELAY_S)

    write_users_gz(OUTPUT_USERS_GZ, kept)
    print(f"Completato. Utenti filtrati: {len(kept)}")
    print(f"Salvato in: {OUTPUT_USERS_GZ}")

if __name__ == "__main__":
    main()

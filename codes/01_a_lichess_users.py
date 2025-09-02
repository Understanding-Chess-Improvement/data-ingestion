import requests
import time
import json
from datetime import datetime, UTC
from itertools import cycle
import gzip
import csv
import os
import re

# === CONFIGURAZIONI ===
TOKEN = r"input\TOKEN.txt"  # File con lista token
FILE_INPUT = r"output\lichess_list_filtered_survey.txt.gz"
FILE_OUTPUT_JSONL = r"output\lichess_users.jsonl"
SURVEY_CSV = r"input\survey.csv"  # SURVEY

RATE_LIMIT = 0.1
MAX_USERS = None  # None per tutti
CATEGORIES = ['ultrabullet', 'bullet', 'blitz', 'rapid', 'puzzle']
oggi = datetime.now(UTC)

# === CARICA TOKEN ===
with open(TOKEN, 'r', encoding='utf-8') as f:
    TOKENS = [line.strip() for line in f if line.strip()]
token_cycle = cycle(TOKENS)

# === CARICA UTENTI DA FILE ===
with gzip.open(FILE_INPUT, 'rt', encoding='utf-8') as f:
    utenti = [line.strip() for line in f if line.strip()]
print(f"Utenti caricati: {len(utenti)}")
utenti = utenti if MAX_USERS is None else utenti[:MAX_USERS]

# === FUNZIONI API ===
def fetch_json(url, headers):
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

def fetch_perf_data(username, perf_type):
    url = f"https://lichess.org/api/user/{username}/perf/{perf_type}"
    response = requests.get(url, headers=HEADERS)
    if response.status_code == 200:
        return response.json()

def initialize_user(username, data):
    created_at = datetime.fromtimestamp(data.get('createdAt', 0) / 1000, tz=UTC)
    anni_account = round((oggi - created_at).days / 365.25, 2)
    profile = data.get('profile', {})
    total_games = data.get('count', {}).get('all', 0)

    user_info = {
        'Username': username,
        'id': data.get('id'),
        'YearsActive': anni_account,
        'CreatedAt': created_at.strftime('%Y-%m-%d'),
        'TotalGames': total_games,
        'flag': profile.get('flag'),
        'location': profile.get('location'),
        'realName': profile.get('realName'),
        'social_links': profile.get('links'),
        'bio': profile.get('bio'),
        'fide_rating': profile.get('fideRating')
    }

    user_record = {
        'user_info': user_info,
        'count': data.get('count', {})
    }

    perfs = data.get('perfs', {})
    for cat in CATEGORIES:
        user_record[cat] = perfs.get(cat, {}) or {}
        user_record[cat]['rating_history'] = {}

    return user_record

def process_rating_history(user_record, history):
    for h in history:
        name = h.get('name', '').lower()
        if name in CATEGORIES:
            rating_history = user_record[name]['rating_history']
            for point in h.get('points', []):
                year, month, day, rating = point
                data_str = f'{day}-{month}-{year}'
                rating_history[data_str] = rating

# === FUNZIONI SURVEY ===
def _norm_header(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r'\([^)]*\)', '', s)
    s = re.sub(r'[^a-z0-9]+', '_', s)
    return s.strip('_')

def _detect_cols(headers):
    norm_headers = [_norm_header(h) for h in headers]
    user_keys  = {'username', 'lichess_username', 'username_lichess', 'user', 'account'}
    first_keys = {'nome', 'first_name', 'firstname', 'first', 'name'}
    last_keys  = {'cognome', 'last_name', 'lastname', 'last', 'surname'}

    def find_one(cands):
        for i, h in enumerate(norm_headers):
            if h in cands:
                return i
        return None

    return (
        find_one(user_keys),
        find_one(first_keys),
        find_one(last_keys),
        norm_headers
    )

def load_survey_map(survey_csv_path):
    survey_map = {}
    if not os.path.exists(survey_csv_path):
        print(f"[Survey] File non trovato: {survey_csv_path} (proseguo senza survey)")
        return survey_map

    with open(survey_csv_path, 'r', encoding='utf-8-sig', newline='') as f:
        reader = csv.reader(f)
        try:
            raw_header = next(reader)
        except StopIteration:
            print("[Survey] CSV vuoto. Proseguo senza survey.")
            return survey_map

        u_idx, f_idx, l_idx, norm_headers = _detect_cols(raw_header)
        if u_idx is None or f_idx is None or l_idx is None:
            print("[Survey] Colonne richieste non trovate (username/nome/cognome). Header normalizzati:",
                  norm_headers, ". Proseguo senza survey.")
            return survey_map

        for row in reader:
            if not row:
                continue
            u  = (row[u_idx] if u_idx < len(row) else '').strip()
            fn = (row[f_idx] if f_idx < len(row) else '').strip()
            ln = (row[l_idx] if l_idx < len(row) else '').strip()
            if u:
                survey_map[u.lower()] = (fn or None, ln or None)

    print(f"[Survey] Nominativi dalla survey caricati: {len(survey_map)}")
    return survey_map

# === CARICA MAPPATURA SURVEY ===
survey_map = load_survey_map(SURVEY_CSV)

# === ELABORAZIONE ===
with open(FILE_OUTPUT_JSONL, 'w', encoding='utf-8') as fout:
    for username in utenti:
        token_corrente = next(token_cycle)
        HEADERS = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {token_corrente}'
        }
        print(f"Controllo utente: {username}, token_corrente = {token_corrente}")

        try:
            data = fetch_json(f"https://lichess.org/api/user/{username}", HEADERS)
            user_record = initialize_user(username, data)

            # 🔹 Se username in survey, aggiorna solo firstName e lastName
            if username.lower() in survey_map:
                fn, ln = survey_map[username.lower()]
                user_record['user_info']['firstName'] = fn
                user_record['user_info']['lastName'] = ln

            time.sleep(RATE_LIMIT)

            history = fetch_json(f"https://lichess.org/api/user/{username}/rating-history", HEADERS)
            process_rating_history(user_record, history)
            time.sleep(RATE_LIMIT)

            for cat in CATEGORIES:
                perf_key = 'ultraBullet' if cat == 'ultrabullet' else cat
                perf_data = fetch_perf_data(username, perf_key)
                if perf_data:
                    user_record[cat]['perf_details'] = perf_data

            fout.write(json.dumps({username: user_record}, ensure_ascii=False) + "\n")

        except Exception as e:
            print(f"Errore per {username}: {e}")

print(f"Utenti filtrati salvati in {FILE_OUTPUT_JSONL}")

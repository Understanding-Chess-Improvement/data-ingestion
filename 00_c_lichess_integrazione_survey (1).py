import csv
import os
import re
import gzip

# =========================
# CONFIG
# =========================

# Lista di partenza (filtrata) da integrare con la survey
INPUT_FILTERED_GZ = r"output\lichess_list_filtered.txt.gz"

# CSV della survey (solo colonna username; header flessibile)
SURVEY_CSV = r"input\survey.csv"

# Output finale integrato
OUTPUT_INTEGRATED_GZ = r"output\lichess_list_filtered_survey.txt.gz"

# =========================
# HELPERS
# =========================

def _norm_header(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r'\([^)]*\)', '', s)
    s = re.sub(r'[^a-z0-9]+', '_', s)
    return s.strip('_')

def _detect_username_col(headers):
    norm_headers = [_norm_header(h) for h in headers]
    user_keys = {'username', 'lichess_username', 'username_lichess', 'user', 'account'}
    for i, h in enumerate(norm_headers):
        if h in user_keys:
            return i, norm_headers
    return None, norm_headers

def read_users_gz(path):
    users = []
    with gzip.open(path, 'rt', encoding='utf-8') as f:
        for line in f:
            u = line.strip()
            if u:
                users.append(u)
    return users

def write_users_gz(path, users):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with gzip.open(path, 'wt', encoding='utf-8', newline='\n') as f:
        for u in users:
            f.write(u + '\n')

def load_survey_usernames(survey_csv_path):
    usernames = []
    if not os.path.exists(survey_csv_path):
        print(f"[Survey] File non trovato: {survey_csv_path} (proseguo senza survey)")
        return usernames

    with open(survey_csv_path, 'r', encoding='utf-8-sig', newline='') as f:
        reader = csv.reader(f)
        try:
            raw_header = next(reader)
        except StopIteration:
            print("[Survey] CSV vuoto. Proseguo senza survey.")
            return usernames

        u_idx, norm_headers = _detect_username_col(raw_header)
        if u_idx is None:
            print("[Survey] Colonna 'username' non trovata. Header normalizzati:",
                  norm_headers, " (proseguo senza survey)")
            return usernames

        for row in reader:
            if not row or u_idx >= len(row):
                continue
            u = (row[u_idx] or '').strip()
            if u:
                usernames.append(u)

    print(f"[Survey] Utenti dalla survey caricati: {len(usernames)}")
    return usernames

# =========================
# MAIN
# =========================

def main():
    base_users = read_users_gz(INPUT_FILTERED_GZ)
    print(f"[Base] Utenti in input filtrato: {len(base_users)}")

    # Costruiamo la mappa dedup: lower -> display (mantieni il case della lista filtrata)
    ordered_users = {}
    for u in base_users:
        ordered_users[u.lower()] = u

    survey_usernames = load_survey_usernames(SURVEY_CSV)

    added_from_survey = 0
    skipped_case_diff = 0
    identical_overwrites = 0

    for u in survey_usernames:
        lu = u.lower()
        if lu in ordered_users:
            # Esiste già (case-insensitive).
            if ordered_users[lu] == u:
                # Identico, no-op
                identical_overwrites += 1
            else:
                # Differenza solo di case: NON sovrascrivere
                skipped_case_diff += 1
        else:
            ordered_users[lu] = u
            added_from_survey += 1

    print(f"[Survey] Aggiunti nuovi: {added_from_survey} | "
          f"identical_overwrites(no-op): {identical_overwrites} | "
          f"saltati_per_case_diff: {skipped_case_diff}")

    # Scrivi output integrato
    write_users_gz(OUTPUT_INTEGRATED_GZ, list(ordered_users.values()))
    print(f"Salvato {len(ordered_users)} username in {OUTPUT_INTEGRATED_GZ}")

if __name__ == "__main__":
    main()

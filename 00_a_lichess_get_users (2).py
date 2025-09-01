import requests
import zstandard as zstd
import io
import chess.pgn
import gzip
import os

# =========================
# CONFIG
# =========================

# Limite su UTENTI UNICI presi dai PGN (None => tutti)
MAX_USERS = 50000  # None per tutti

# File di input con gli URL dei PGN zstd (uno per riga)
urls_file = r"input\Chess_Games_Lichess_2025.txt"

# Output: lista unica di username (uno per riga) .txt.gz
output_file = r"output\lichess_list.txt.gz"

# Header HTTP (per i dump pubblici non serve token)
HEADERS = {"Accept": "*/*"}

# =========================
# RACCOLTA DA PGN (ZSTD)
# =========================

# Carica gli URL dei dump
with open(urls_file, 'r', encoding='utf-8') as f:
    urls = [line.strip() for line in f if line.strip()]

# Mappa ordinata: chiave = username lower (per dedup), valore = display dal PGN
ordered_users = {}   # lower_username -> display_username (PGN)
total_from_games = 0 #contatore di utenti unici trovati nei PGN

for url in urls:
    print(f"Processing file: {url}")
    try:
        with requests.get(url, stream=True, headers=HEADERS) as response:
            response.raise_for_status()
            dctx = zstd.ZstdDecompressor()
            stream_reader = dctx.stream_reader(response.raw)
            text_stream = io.TextIOWrapper(stream_reader, encoding='utf-8')

            while True:
                # Stop se raggiunto il limite di UTENTI UNICI (solo PGN)
                if MAX_USERS is not None and total_from_games >= MAX_USERS:
                    print(f"Reached limit of {MAX_USERS} unique users from PGN.")
                    break

                game = chess.pgn.read_game(text_stream)
                if game is None:
                    print("End of file reached.")
                    break

                w = (game.headers.get("White") or "").strip()
                b = (game.headers.get("Black") or "").strip()

                if w:
                    lw = w.lower()
                    if lw not in ordered_users:
                        ordered_users[lw] = w  # mantieni il case così com'è dai PGN
                        total_from_games += 1
                        if MAX_USERS is not None and total_from_games >= MAX_USERS:
                            pass

                if b and (MAX_USERS is None or total_from_games < MAX_USERS):
                    lb = b.lower()
                    if lb not in ordered_users:
                        ordered_users[lb] = b
                        total_from_games += 1

        if MAX_USERS is not None and total_from_games >= MAX_USERS:
            break

    except Exception as e:
        print(f"Error with file {url}: {e}")

print(f"[Games] Utenti UNICI raccolti dai PGN: {total_from_games}")

# =========================
# SCRITTURA OUTPUT
# =========================

os.makedirs(os.path.dirname(output_file), exist_ok=True)
with gzip.open(output_file, 'wt', encoding='utf-8', newline='\n') as f_out:
    for display_username in ordered_users.values():
        f_out.write(display_username + '\n')

print(f"Saved {len(ordered_users)} unique names to {output_file} (games_unique: {total_from_games})")

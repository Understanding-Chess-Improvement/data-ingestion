import requests
import time
import json
import gzip
from itertools import cycle
from datetime import datetime, timezone, timedelta

# =========================
# CONFIG for Games
# =========================

# Token Lichess (uno per riga)
TOKEN_FILE = r"input\TOKEN.txt"

# Input: lista utenti (uno per riga) in .gz
FILE_INPUT = r"output\lichess_list_matched_fide.txt.gz"

# Output games (UNA RIGA PER UTENTE)
OUT_GAMES_JSONL = r"output\lichess_games_matched.jsonl"

# Filtro temporale
SINCE_DATE = "2023-01-01"
UNTIL_DATE = None          # None = adesso (UTC)

# Filtri server-side
RATED_ONLY    = True
ANALYSED_ONLY = True

# Perf consentite (server + client side)
VALID_PERF_LOWER = ["ultrabullet", "bullet", "blitz", "rapid"]
PERF_TYPES_CAMEL = ["ultraBullet", "bullet", "blitz", "rapid"]
PERF_TYPES_PARAM = ",".join(PERF_TYPES_CAMEL)

# Finestre di download in giorni
WINDOW_DAYS       = 365
GROUP_WINDOW_DAYS = 1      # aggregazione output (1=giornaliera, 7=settim., 30=mensile)

# Rate control
RATE_LIMIT = 0.1
MAX_USERS  = 5          # None = tutti

# Retry / timeout
MAX_RETRIES     = 4
BACKOFF_BASE    = 1.8
REQUEST_TIMEOUT = 60

# =========================
# PROXY DECODO
# =========================
PROXY_ENABLED   = True
DECODO_HOST     = "dc.decodo.com"
DECODO_USERNAME     = "<your-username>"
DECODO_PASSWORD     = "<your-password>"
DECODO_PORTS    = list(range(10001, 10501))

_DECODO_POOL = [
    {
        "http":  f"http://{DECODO_USERNAME}:{DECODO_PASSWORD}@{DECODO_HOST}:{port}",
        "https": f"http://{DECODO_USERNAME}:{DECODO_PASSWORD}@{DECODO_HOST}:{port}",
    }
    for port in DECODO_PORTS
]
from itertools import cycle as _cycle_decodo
_decodo_cycle = _cycle_decodo(_DECODO_POOL)

def _next_decodo_proxies():
    if not PROXY_ENABLED or not _DECODO_POOL:
        return None
    return next(_decodo_cycle)

# >>> MOD CONFIG: opzioni di gestione token/proxy allineate ad activity
FIX_TOKEN_PER_USER     = False   # True = stesso token per tutte le finestre dello stesso user
ROTATE_PROXY_ON_RETRY  = True    # >>> MOD ROTATE_PROXY: cambia IP a ogni retry finché non trovi un proxy buono
LOCK_PROXY_FOR_USER    = True    # >>> MOD LOCK_PROXY: appena una finestra OK, blocca quel proxy per tutto lo user

# =========================
# DEBUG helpers
# =========================
DEBUG = True

def _mask_token(tok: str) -> str:
    if not tok:
        return "None"
    if len(tok) <= 8:
        return tok[0] + "***" + tok[-1]
    return tok[:4] + "..." + tok[-4:]

def _proxy_str(proxies: dict | None) -> str:
    if not proxies:
        return "no-proxy"
    for k in ("http", "https"):
        if k in proxies and isinstance(proxies[k], str):
            try:
                return proxies[k].split("@")[1]  # host:port
            except Exception:
                return proxies[k]
    return "proxy-unknown"

# =========================
# Utility tempo / headers
# =========================

def to_epoch_ms(date_str_or_none):
    if not date_str_or_none:
        return None
    dt = datetime.strptime(date_str_or_none, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)

SINCE_MS = to_epoch_ms(SINCE_DATE)
UNTIL_MS = to_epoch_ms(UNTIL_DATE) or int(datetime.now(timezone.utc).timestamp() * 1000)

def headers_for(token):
    return {
        "Accept": "application/x-ndjson",
        "Authorization": f"Bearer {token}",
    }

# Carica token + utenti
with open(TOKEN_FILE, "r", encoding="utf-8") as f:
    TOKENS = [line.strip() for line in f if line.strip()]
if not TOKENS:
    raise RuntimeError("Nessun token trovato in TOKEN_FILE.")
token_cycle = cycle(TOKENS)

with gzip.open(FILE_INPUT, "rt", encoding="utf-8") as f:
    USERS = [line.strip() for line in f if line.strip()]
if isinstance(MAX_USERS, int):
    USERS = USERS[:MAX_USERS]

# =========================
# >>> MOD CHUNK + ROTATE_PROXY + LOCK_PROXY (come in activity)
# Scarica per finestre; durante i retry ruota l'IP finché non trova un proxy “buono”;
# appena una finestra va a buon fine, blocca quel proxy per lo user (finestre successive incluse).
# =========================
def fetch_games_for_user(username, since_ms, until_ms, proxies_initial, allow_no_proxy_fallback=True):
    """
    Restituisce una LISTA di partite (g invariato) su finestre temporali.
    - pgnInJson + check NDJSON
    - retry/backoff
    - ROTATE_PROXY_ON_RETRY: ruota proxy ad ogni retry, finché non trova un proxy “buono”
    - LOCK_PROXY_FOR_USER: appena una finestra riesce, usa sempre quel proxy per il resto dello user
    - fallback no-proxy sull’ultimo giro di tentativi
    """
    url = f"https://lichess.org/api/games/user/{username}"
    params_base = {
        "perfType": PERF_TYPES_PARAM,
        "pgnInJson": "true",
        "moves": "true",
        "evals": "true",
        "accuracy": "true",
        "clocks": "true",
        "opening": "true",
        "division": "true",
    }
    if RATED_ONLY:
        params_base["rated"] = "true"
    if ANALYSED_ONLY:
        params_base["analysed"] = "true"

    ms_per_day = 24 * 60 * 60 * 1000
    window_ms  = WINDOW_DAYS * ms_per_day
    start = min(since_ms, until_ms)
    end   = max(since_ms, until_ms)

    fixed_token = next(token_cycle) if FIX_TOKEN_PER_USER else None
    if DEBUG and FIX_TOKEN_PER_USER:
        print(f"[{username}] FIX_TOKEN_PER_USER=ON | token_fixed={_mask_token(fixed_token)} | proxy_initial={_proxy_str(proxies_initial)}")

    used_no_proxy = False
    locked_proxy = None      # >>> MOD LOCK_PROXY
    last_exc = None

    all_games = []
    cur = start

    while True:
        win_start = cur
        win_end   = min(cur + window_ms - 1, end)

        attempt_idx = 0
        total_tries = MAX_RETRIES

        while attempt_idx < total_tries:
            attempt_idx += 1
            token = fixed_token if FIX_TOKEN_PER_USER else next(token_cycle)

            # >>> MOD ROTATE_PROXY + LOCK_PROXY: scelta proxy per il tentativo
            if used_no_proxy:
                proxies = None
            else:
                if LOCK_PROXY_FOR_USER and (locked_proxy is not None):
                    proxies = locked_proxy
                else:
                    if ROTATE_PROXY_ON_RETRY and PROXY_ENABLED:
                        # ruota ad ogni retry finché non c'è un locked_proxy
                        proxies = _next_decodo_proxies() or proxies_initial
                    else:
                        proxies = proxies_initial

            if DEBUG:
                print(f"[{username}] ATTEMPT {attempt_idx}/{total_tries} | "
                      f"token={_mask_token(token)} | proxy={_proxy_str(proxies)} | "
                      f"window={win_start}->{win_end} | locked={_proxy_str(locked_proxy)}")

            try:
                params = dict(params_base, since=win_start, until=win_end)
                resp = requests.get(
                    url,
                    headers=headers_for(token),
                    params=params,
                    stream=True,
                    timeout=REQUEST_TIMEOUT,
                    proxies=proxies
                )

                if DEBUG:
                    ct_dbg = resp.headers.get("Content-Type", "N/A")
                    print(f"[{username}] HTTP {resp.status_code} | CT={ct_dbg} | window={win_start}->{win_end}")

                ct = (resp.headers.get("Content-Type") or "").lower()
                if "ndjson" not in ct:
                    raise requests.HTTPError(f"CT_MISMATCH: {ct}")

                if resp.status_code in (429,) or resp.status_code >= 500:
                    raise requests.HTTPError(f"HTTP_{resp.status_code} {resp.reason}")

                resp.raise_for_status()

                # >>> MOD LOCK_PROXY: risposta valida -> blocca il proxy usato, se non siamo in no-proxy
                if (not used_no_proxy) and LOCK_PROXY_FOR_USER and (locked_proxy is None):
                    locked_proxy = proxies
                    if DEBUG:
                        print(f"[{username}] LOCK_PROXY -> { _proxy_str(locked_proxy) }")

                seen = 0
                for line in resp.iter_lines(decode_unicode=True):
                    if not line:
                        continue
                    try:
                        g = json.loads(line)
                    except Exception as e:
                        raise requests.HTTPError(f"BAD_NDJSON: {e}")

                    perf = (g.get("perf") or "").lower()
                    if perf in VALID_PERF_LOWER:
                        g["perf"] = "ultraBullet" if perf == "ultrabullet" else perf
                        all_games.append(g)
                        seen += 1

                if seen == 0 and DEBUG:
                    print(f"[{username}] STREAM_EMPTY | token={_mask_token(token)} | proxy={_proxy_str(proxies)} | window={win_start}->{win_end}")

                break  # finestra OK (anche se vuota)

            except requests.exceptions.ProxyError as e:
                last_exc = f"PROXY_ERROR: {e}"
                if DEBUG:
                    print(f"[{username}] {last_exc} | proxy={_proxy_str(proxies)} | window={win_start}->{win_end}")
            except (requests.exceptions.ConnectTimeout, requests.exceptions.ReadTimeout) as e:
                last_exc = f"TIMEOUT: {e}"
                if DEBUG:
                    print(f"[{username}] {last_exc} | proxy={_proxy_str(proxies)} | window={win_start}->{win_end}")
            except requests.exceptions.ConnectionError as e:
                last_exc = f"CONNECTION_ERROR: {e}"
                if DEBUG:
                    print(f"[{username}] {last_exc} | proxy={_proxy_str(proxies)} | window={win_start}->{win_end}")
            except requests.HTTPError as e:
                last_exc = f"{e}"
                if DEBUG:
                    print(f"[{username}] HTTP_ERROR: {last_exc} | proxy={_proxy_str(proxies)} | window={win_start}->{win_end}")
            except Exception as e:
                last_exc = f"UNKNOWN_ERROR: {e}"
                if DEBUG:
                    print(f"[{username}] {last_exc} | proxy={_proxy_str(proxies)} | window={win_start}->{win_end}")

            # retry / fallback
            if attempt_idx < total_tries:
                sleep_s = (BACKOFF_BASE ** (attempt_idx - 1)) + RATE_LIMIT
                print(f"[{username}] retry {attempt_idx}/{total_tries} -> {last_exc} | sleep {sleep_s:.1f}s")
                time.sleep(sleep_s)
            else:
                if allow_no_proxy_fallback and not used_no_proxy and PROXY_ENABLED:
                    if DEBUG:
                        print(f"[{username}] FALLBACK_NO_PROXY (window) {win_start}->{win_end}")
                    used_no_proxy = True
                    attempt_idx = 0  # ricomincia tentativi senza proxy
                else:
                    print(f"[{username}] errore definitivo finestra: {last_exc}")
                    break

        # prossima finestra
        if win_end >= end:
            break
        cur = win_end + 1
        time.sleep(RATE_LIMIT)

    # diagnosi
    fetch_games_for_user._last_error = last_exc if 'last_exc' in locals() else None
    return all_games

# =========================
# Aggregazione per intervalli (una riga per utente)
# =========================
def day_floor_utc(ms: int) -> datetime:
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc)

def window_floor_utc(ms: int, window_days: int) -> datetime:
    d0 = day_floor_utc(ms)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    days_since_epoch = (d0 - epoch).days
    base_days = days_since_epoch - (days_since_epoch % window_days)
    return epoch + timedelta(days=base_days)

def fmt_window_label(start_dt: datetime, window_days: int) -> tuple[int, int, str]:
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms   = int((start_dt + timedelta(days=window_days)).timestamp() * 1000) - 1
    label    = f"{start_dt.strftime('%Y-%m-%d 00:00:00+00:00')} -> {(start_dt + timedelta(days=window_days)).strftime('%Y-%m-%d 00:00:00+00:00')}"
    return start_ms, end_ms, label

# fallback a user.id + lowercase (allineato ad activity)
def extract_side_and_result(game: dict, username_lower: str) -> tuple[str | None, str | None]:
    players = game.get("players", {})
    w = players.get("white", {})
    b = players.get("black", {})
    w_user = ((w.get("user") or {}).get("name") or (w.get("user") or {}).get("id") or "").lower()
    b_user = ((b.get("user") or {}).get("name") or (b.get("user") or {}).get("id") or "").lower()
    if w_user == username_lower:
        side = "white"
    elif b_user == username_lower:
        side = "black"
    else:
        return None, None
    winner = game.get("winner")
    if winner is None:
        result = "draw"
    else:
        result = "win" if winner == side else "loss"
    return side, result

def extract_rating_pair(game: dict, side: str) -> tuple[int | None, int | None]:
    p = (game.get("players") or {}).get(side, {})
    r = p.get("rating")
    rd = p.get("ratingDiff")
    if r is None or rd is None:
        return None, None
    try:
        before = int(r) - int(rd)
        after = int(r)
        return before, after
    except Exception:
        return None, None

def build_user_buckets(username: str, games: list, group_days: int):
    username_lower = username.lower()
    buckets: dict[int, dict] = {}

    for g in games:
        ts = g.get("createdAt") or g.get("lastMoveAt")
        if ts is None:
            continue

        w_start_dt = window_floor_utc(ts, group_days)
        start_ms, end_ms, label = fmt_window_label(w_start_dt, group_days)

        bucket = buckets.get(start_ms)
        if bucket is None:
            bucket = {
                "interval": {"start": start_ms, "end": end_ms},
                "window": label,
                "games": {},
                "details": []
            }
            buckets[start_ms] = bucket

        perf_lower = (g.get("perf") or "").lower()
        if perf_lower not in VALID_PERF_LOWER:
            continue
        perf_key = "ultraBullet" if perf_lower == "ultrabullet" else perf_lower

        side, result = extract_side_and_result(g, username_lower)
        if side is None:
            continue

        node = bucket["games"].get(perf_key)
        if node is None:
            node = {"win": 0, "loss": 0, "draw": 0, "rp": {"before": None, "after": None}}
            bucket["games"][perf_key] = node
        node[result] += 1

        before, after = extract_rating_pair(g, side)
        if before is not None and node["rp"]["before"] is None:
            node["rp"]["before"] = before
        if after is not None:
            node["rp"]["after"] = after

        # dettaglio per-partita (mantiene tutti i campi del JSON originario)
        rec = {"username": username}
        rec.update(g)
        bucket["details"].append(rec)

    buckets_sorted = [buckets[k] for k in sorted(buckets.keys())]
    return {username: buckets_sorted}

# =========================
# MAIN
# =========================
def main():
    print(f"Utenti caricati: {len(USERS)} | Intervallo: {SINCE_DATE} -> "
          f"{(datetime.utcfromtimestamp(UNTIL_MS/1000).strftime('%Y-%m-%d') if UNTIL_DATE else 'oggi')} "
          f"| rated={RATED_ONLY} | analysed={ANALYSED_ONLY}")
    print(f"Output (1 riga per utente) -> {OUT_GAMES_JSONL} | Aggregazione={GROUP_WINDOW_DAYS} giorni")

    with open(OUT_GAMES_JSONL, "w", encoding="utf-8") as fout:
        for idx, u in enumerate(USERS, 1):
            print(f"[{idx}/{len(USERS)}] Scarico games per: {u}")

            # proxy iniziale per-utente (base)
            user_proxies = _next_decodo_proxies()
            if user_proxies:
                print(f"  proxy_initial={_proxy_str(user_proxies)}")
            else:
                print("  proxy_initial=no-proxy")

            try:
                games = fetch_games_for_user(
                    u, SINCE_MS, UNTIL_MS, proxies_initial=user_proxies, allow_no_proxy_fallback=True
                )
                user_buckets = build_user_buckets(u, games, GROUP_WINDOW_DAYS)

                # diagnostica se vuoto
                if not user_buckets.get(u):
                    last_err = getattr(fetch_games_for_user, "_last_error", None)
                    print(f"[{u}] RESULT_EMPTY | last_error={last_err} | proxy_initial={_proxy_str(user_proxies)}")

            except Exception as e:
                print(f"  Errore per {u}: {e}")
                user_buckets = {u: []}

            fout.write(json.dumps(user_buckets, ensure_ascii=False) + "\n")
            fout.flush()
            time.sleep(RATE_LIMIT)

    print(f"Salvato games JSONL: {OUT_GAMES_JSONL} (una riga per utente)")

if __name__ == "__main__":
    main()


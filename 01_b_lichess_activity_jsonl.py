import os
import gzip
import json
import time
import requests
from itertools import cycle
from datetime import datetime, timezone, timedelta

# ============================================================================
#                          SEZIONE PARAMETRI (LOCALE)
# ============================================================================

# Input lista utenti (uno per riga) in .gz o .txt (locale)
INPUT_USERS_PATH = r"output\lichess_list_matched_fide_splitted\lichess_list_matched_fide_part_03.txt.gz"

# Output file JSONL (locale, singolo file)
OUTPUT_JSONL_PATH = r"output\lichess_activity_matched_part\lichess_activity_matched_part_03.jsonl"

# Token Lichess
TOKENS_FILE     = r"input\TOKEN.txt"   # un token per riga
TOKENS_INLINE   = None                 # es: "tok1,tok2"

# Intervallo (UTC)
SINCE_DATE      = "2023-01-01"   # YYYY-MM-DD (incluso)
UNTIL_DATE      = None           # None -> adesso (UTC)

# Filtri server-side
RATED_ONLY      = True
ANALYSED_ONLY   = False

# Perf consentite (server + client side)
VALID_PERF_LOWER = ["ultrabullet", "bullet", "blitz", "rapid"]  
PERF_TYPES_CAMEL = ["ultraBullet", "bullet", "blitz", "rapid"]
PERF_TYPES_PARAM = ",".join(PERF_TYPES_CAMEL)

# Retry / pacing
MAX_RETRIES     = 4
BACKOFF_BASE    = 1.8
REQUEST_TIMEOUT = 60
RATE_LIMIT      = 0.01
MAX_USERS       = None  # None = tutti

# Proxy Decodo (opzionale)
PROXY_ENABLED       = True
DECODO_HOST         = "dc.decodo.com"
DECODO_USERNAME     = "sp182yfwi6"
DECODO_PASSWORD     = "z4mu25ws~bQWC7oNtj"
DECODO_PORT_START   = 10001
DECODO_PORT_END     = 10500

# >>> MOD CONFIG: Chunking e controllo token/proxy
WINDOW_DAYS         = 365        # >>> dimensione finestra
FIX_TOKEN_PER_USER  = False      # >>> True = stesso token per tutto lo user
ROTATE_PROXY_ON_RETRY = True     # >>> MOD ROTATE_PROXY: ruota IP a ogni retry finché non trovi un proxy buono
# ============================================================================

DEBUG = True  # log diagnostici

# -----------------------------
# Helpers tempo
# -----------------------------
def to_epoch_ms(date_str_or_none):
    if not date_str_or_none:
        return None
    dt = datetime.strptime(date_str_or_none, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)

SINCE_MS = to_epoch_ms(SINCE_DATE)
UNTIL_MS = to_epoch_ms(UNTIL_DATE) or int(datetime.now(timezone.utc).timestamp() * 1000)

def fmt_window_utc(day_start_dt):
    d0 = day_start_dt.strftime("%Y-%m-%d 00:00:00+00:00")
    d1 = (day_start_dt + timedelta(days=1)).strftime("%Y-%m-%d 00:00:00+00:00")
    return f"{d0} -> {d1}"

def day_utc_floor(ms):
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc)

# -----------------------------
# Caricamento tokens
# -----------------------------
def load_tokens():
    tokens = []
    if TOKENS_FILE and os.path.exists(TOKENS_FILE):
        with open(TOKENS_FILE, "r", encoding="utf-8") as f:
            tokens = [t.strip() for t in f if t.strip()]
    elif TOKENS_INLINE:
        tokens = [t.strip() for t in TOKENS_INLINE.split(",") if t.strip()]
    else:
        raise RuntimeError("Devi specificare TOKENS_FILE o TOKENS_INLINE")
    if not tokens:
        raise RuntimeError("Nessun token disponibile.")
    return tokens

TOKENS_LIST = load_tokens()
TOKEN_CYCLE = cycle(TOKENS_LIST)

# -----------------------------
# Config “broadcast locale”
# -----------------------------
CONF = {
    "RATED_ONLY": RATED_ONLY,
    "ANALYSED_ONLY": ANALYSED_ONLY,
    "VALID_PERF_LOWER": VALID_PERF_LOWER,
    "PERF_TYPES_PARAM": PERF_TYPES_PARAM,
    "MAX_RETRIES": MAX_RETRIES,
    "BACKOFF_BASE": BACKOFF_BASE,
    "REQUEST_TIMEOUT": REQUEST_TIMEOUT,
    "SINCE_MS": SINCE_MS,
    "UNTIL_MS": UNTIL_MS,
    "RATE_LIMIT": RATE_LIMIT,
    # >>> MOD CHUNK
    "WINDOW_DAYS": WINDOW_DAYS,
    "FIX_TOKEN_PER_USER": FIX_TOKEN_PER_USER,
    # >>> MOD ROTATE_PROXY
    "ROTATE_PROXY_ON_RETRY": ROTATE_PROXY_ON_RETRY,
}

PROXY_CONF = {
    "enabled": PROXY_ENABLED,
    "host": DECODO_HOST,
    "user": DECODO_USERNAME,
    "pwd": DECODO_PASSWORD,
    "port_start": DECODO_PORT_START,
    "port_end": DECODO_PORT_END,
}

# -----------------------------
# Utils logging sicuro
# -----------------------------
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
                return proxies[k].split("@")[1]  # "host:port"
            except Exception:
                return proxies[k]
    return "proxy-unknown"

# -----------------------------
# Funzioni dominio aggregazione
# -----------------------------
def perf_key_from_lower(perf_lower):
    return "ultraBullet" if perf_lower == "ultrabullet" else perf_lower

def extract_side_and_result(game, username_lower):
    players = game.get("players", {})
    w = players.get("white", {})
    b = players.get("black", {})
    w_user = ((w.get("user") or {}).get("name") or "").lower()
    b_user = ((b.get("user") or {}).get("name") or "").lower()
    if w_user == username_lower:
        side = "white"
    elif b_user == username_lower:
        side = "black"
    else:
        return None, None
    winner = game.get("winner")
    return side, ("draw" if winner is None else ("win" if winner == side else "loss"))

def extract_rating_pair(game, side):
    p = (game.get("players") or {}).get(side, {})
    r = p.get("rating"); rd = p.get("ratingDiff")
    if r is None or rd is None:
        return None, None
    try:
        return int(r) - int(rd), int(r)
    except Exception:
        return None, None

def build_activity_from_games(username, games_iter, conf):
    username_lower = username.lower()
    day_buckets = {}
    for g in games_iter:
        ts = g.get("createdAt") or g.get("lastMoveAt")
        if ts is None:
            continue
        day0_dt = day_utc_floor(ts)
        day_start_ms = int(day0_dt.timestamp() * 1000)
        day_end_ms   = day_start_ms + (24*60*60*1000) - 1
        day_label    = fmt_window_utc(day0_dt)
        if day_start_ms not in day_buckets:
            day_buckets[day_start_ms] = {
                "interval": {"start": day_start_ms, "end": day_end_ms},
                "window": day_label,
                "games": {}
            }
        bucket = day_buckets[day_start_ms]
        perf = (g.get("perf") or "").lower()
        if perf not in conf["VALID_PERF_LOWER"]:
            continue
        perf_key = perf_key_from_lower(perf)
        side, result = extract_side_and_result(g, username_lower)
        if side is None:
            continue
        b_games = bucket["games"]
        if perf_key not in b_games:
            b_games[perf_key] = {"win": 0, "loss": 0, "draw": 0, "rp": {"before": None, "after": None}}
        b_games[perf_key][result] += 1
        before, after = extract_rating_pair(g, side)
        if before is not None and b_games[perf_key]["rp"]["before"] is None:
            b_games[perf_key]["rp"]["before"] = before
        if after is not None:
            b_games[perf_key]["rp"]["after"] = after
    return {username: [day_buckets[k] for k in sorted(day_buckets.keys())]}

# -----------------------------
# Proxy helpers
# -----------------------------
def make_proxy_pool(proxy_conf):
    if not proxy_conf["enabled"]:
        return []
    ports = list(range(proxy_conf["port_start"], proxy_conf["port_end"] + 1))
    pool = []
    user = proxy_conf["user"]; pwd = proxy_conf["pwd"]; host = proxy_conf["host"]
    if not user or not pwd or not host or not ports:
        return []
    for p in ports:
        url = f"http://{user}:{pwd}@{host}:{p}"
        pool.append({"http": url, "https": url})
    return pool

PROXY_POOL = make_proxy_pool(PROXY_CONF)
PROXY_CYCLE = cycle(PROXY_POOL) if PROXY_POOL else None

# -----------------------------
# >>> MOD CHUNK + ROTATE_PROXY + LOCK_PROXY
# Scarica a finestre; ruota IP ad ogni retry finché non trova un proxy buono;
# appena una finestra va a buon fine, BLOCCA quel proxy per tutto lo user.
# -----------------------------
def fetch_games_for_user_windowed(username, conf, token_cycle, user_proxy, session, allow_no_proxy_fallback=True):
    url = f"https://lichess.org/api/games/user/{username}"
    params_base = {
        "perfType": conf["PERF_TYPES_PARAM"],
        "pgnInJson": "true",
    }
    if conf["RATED_ONLY"]:
        params_base["rated"] = "true"
    if conf["ANALYSED_ONLY"]:
        params_base["analysed"] = "true"

    ms_per_day = 24 * 60 * 60 * 1000
    window_ms  = conf["WINDOW_DAYS"] * ms_per_day
    start = min(conf["SINCE_MS"], conf["UNTIL_MS"])
    end   = max(conf["SINCE_MS"], conf["UNTIL_MS"])

    fixed_token = next(token_cycle) if conf.get("FIX_TOKEN_PER_USER") else None
    if DEBUG and conf.get("FIX_TOKEN_PER_USER"):
        print(f"[{username}] FIX_TOKEN_PER_USER=ON | token_fixed={_mask_token(fixed_token)} | proxy_initial={_proxy_str(user_proxy)}")

    used_no_proxy = False
    locked_proxy = None   # >>> MOD LOCK_PROXY: None finché non troviamo un proxy “buono”
    last_exc = None

    cur = start
    while True:
        win_start = cur
        win_end   = min(cur + window_ms - 1, end)

        total_tries = conf["MAX_RETRIES"]
        attempt_idx = 0

        while attempt_idx < total_tries:
            attempt_idx += 1
            token = fixed_token if fixed_token else next(token_cycle)

            # >>> MOD ROTATE_PROXY + LOCK_PROXY: scelta proxy per questo tentativo
            if used_no_proxy:
                proxies = None
            else:
                if locked_proxy is not None:
                    # abbiamo già un proxy “buono”: resta fisso per tutte le finestre
                    proxies = locked_proxy
                else:
                    # finché non abbiamo un proxy “buono”, ruotiamo a ogni retry (se abilitato)
                    if conf.get("ROTATE_PROXY_ON_RETRY") and PROXY_CYCLE is not None:
                        proxies = next(PROXY_CYCLE) or user_proxy
                    else:
                        proxies = user_proxy

            if DEBUG:
                print(f"[{username}] ATTEMPT {attempt_idx}/{total_tries} | "
                      f"token={_mask_token(token)} | proxy={_proxy_str(proxies)} | "
                      f"window={win_start}->{win_end} | locked={_proxy_str(locked_proxy)}")

            try:
                params = dict(params_base, since=win_start, until=win_end)
                resp = session.get(
                    url, headers={"Accept": "application/x-ndjson", "Authorization": f"Bearer {token}"},
                    params=params, stream=True, timeout=conf["REQUEST_TIMEOUT"], proxies=proxies
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

                # >>> MOD LOCK_PROXY: appena abbiamo una risposta valida, fissiamo il proxy usato
                if not used_no_proxy and locked_proxy is None:
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
                    if perf in conf["VALID_PERF_LOWER"]:
                        g["perf"] = "ultraBullet" if perf == "ultrabullet" else perf
                        seen += 1
                        yield g

                if seen == 0 and DEBUG:
                    print(f"[{username}] STREAM_EMPTY | token={_mask_token(token)} | "
                          f"proxy={_proxy_str(proxies)} | window={win_start}->{win_end}")

                break  # finestra OK (anche se vuota): passa alla successiva

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

            # retry/fallback
            if attempt_idx < total_tries:
                sleep_s = (conf["BACKOFF_BASE"] ** (attempt_idx - 1)) + conf["RATE_LIMIT"]
                print(f"[{username}] retry {attempt_idx}/{total_tries} -> {last_exc} | sleep {sleep_s:.1f}s")
                time.sleep(sleep_s)
            else:
                if allow_no_proxy_fallback and not used_no_proxy and PROXY_ENABLED:
                    if DEBUG:
                        print(f"[{username}] FALLBACK_NO_PROXY (window) {win_start}->{win_end}")
                    used_no_proxy = True
                    attempt_idx = 0  # ricomincia i tentativi *senza proxy* per questa finestra
                else:
                    print(f"[{username}] errore definitivo finestra: {last_exc}")
                    break

        # prossima finestra
        if win_end >= end:
            break
        cur = win_end + 1
        time.sleep(conf["RATE_LIMIT"])

    # ultimo errore utile per diagnostica
    fetch_games_for_user_windowed._last_error = last_exc if 'last_exc' in locals() else None

# -----------------------------
# Lettura utenti e scrittura JSONL
# -----------------------------
def iter_users(path, max_users=None):
    count = 0
    if path.lower().endswith(".gz"):
        with gzip.open(path, "rt", encoding="utf-8") as f:
            for line in f:
                u = (line or "").strip()
                if not u:
                    continue
                yield u
                count += 1
                if max_users is not None and count >= max_users:
                    return
    else:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                u = (line or "").strip()
                if not u:
                    continue
                yield u
                count += 1
                if max_users is not None and count >= max_users:
                    return

def main():
    os.makedirs(os.path.dirname(OUTPUT_JSONL_PATH), exist_ok=True)
    session = requests.Session()
    processed = 0

    with open(OUTPUT_JSONL_PATH, "w", encoding="utf-8") as out:
        for u in iter_users(INPUT_USERS_PATH, max_users=MAX_USERS):
            # proxy iniziale scelto per-utente (usato come base)
            user_proxy = next(PROXY_CYCLE) if PROXY_CYCLE else None
            if DEBUG:
                print(f"[{u}] START | proxy_initial={_proxy_str(user_proxy)}")

            try:
                games_iter = fetch_games_for_user_windowed(
                    u, CONF, TOKEN_CYCLE, user_proxy, session, allow_no_proxy_fallback=True
                )
                user_activity = build_activity_from_games(u, games_iter, CONF)

                if not user_activity.get(u):
                    last_err = getattr(fetch_games_for_user_windowed, "_last_error", None)
                    print(f"[{u}] RESULT_EMPTY | last_error={last_err} | proxy_initial={_proxy_str(user_proxy)}")

            except Exception as e:
                print(f"[{u}] errore: {e}")
                user_activity = {u: []}

            out.write(json.dumps(user_activity, ensure_ascii=False) + "\n")
            out.flush()

            processed += 1
            if CONF["RATE_LIMIT"] > 0:
                time.sleep(CONF["RATE_LIMIT"])

    print(f"Completato. Utenti processati: {processed}. Output: {OUTPUT_JSONL_PATH}")

if __name__ == "__main__":
    main()

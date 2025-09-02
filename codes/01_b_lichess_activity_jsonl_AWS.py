# glue_01_b_lichess_activity_jsonl_hardcoded.py
# Output: directory S3 con part-* contenenti linee JSONL
# Ogni riga = {"<username>":[{bucket_giornaliero_per_perf...}, ...]}

import os
import json
import time
import requests
from itertools import cycle
from datetime import datetime, timezone, timedelta

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
# ⇨ MOD: ci serve l'ID della partizione Spark per shardare proxy e staggherare i token
from pyspark import TaskContext

# ============================================================================
#                          SEZIONE PARAMETRI (HARDCODED)
# ============================================================================
JOB_NAME        = "lichess-activity"

# Input lista utenti (uno per riga) GZIP su S3
INPUT_S3_GZ     = "s3://lichess-raw-data/lichess_list_filtered_survey.txt.gz"

# Output directory S3 (verrà creata): contiene part-* in formato JSONL
OUTPUT_S3_DIR   = "s3://lichess-dwh/lichess_activity_jsonl/run_dt=2025-08-18"

# Token Lichess
TOKENS_S3       = "s3://lichess-raw-data/TOKEN.txt"  # file con un token per riga (qui porterai a 13 righe)
TOKENS_INLINE   = None  # es: "tok1,tok2"

# Intervallo (UTC) — userai un’unica richiesta per tutto l’intervallo
SINCE_DATE      = "2023-01-01"   # YYYY-MM-DD (incluso)
UNTIL_DATE      = "2023-12-31"   # None -> adesso (UTC)

# Filtri server-side
RATED_ONLY      = True
ANALYSED_ONLY   = False

# Perf consentite (server + client side)
VALID_PERF_LOWER = ["ultrabullet", "bullet", "blitz", "rapid"]  # aggiungi "classical" se ti serve
PERF_TYPES_CAMEL = ["ultraBullet", "bullet", "blitz", "rapid"]
PERF_TYPES_PARAM = ",".join(PERF_TYPES_CAMEL)

# Retry / pacing
MAX_RETRIES     = 4
BACKOFF_BASE    = 1.8
REQUEST_TIMEOUT = 60
RATE_LIMIT      = 0.01  # nessun timing obbligatorio; lascia 0.0 se vuoi davvero zero pause
MAX_USERS       = None #None per tutti

# Parallelismo Spark
N_PARTITIONS        = 25
OUTPUT_COALESCE     = 0
OUTPUT_SINGLE_FILE  = False

# Proxy Decodo
PROXY_ENABLED       = True
DECODO_HOST         = "dc.decodo.com"
DECODO_USERNAME     = "<your-username>"
DECODO_PASSWORD     = "<your-password>"
DECODO_PORT_START   = 10001
DECODO_PORT_END     = 10500
# ============================================================================

# -----------------------------
# Glue / Spark setup
# -----------------------------
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, {})

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
    dt0 = datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc)
    return dt0

# -----------------------------
# Caricamento tokens
# -----------------------------
def load_tokens():
    if TOKENS_S3:
        tokens = [t for t in sc.textFile(TOKENS_S3).collect() if t.strip()]
    elif TOKENS_INLINE:
        tokens = [t.strip() for t in TOKENS_INLINE.split(",") if t.strip()]
    else:
        raise RuntimeError("Devi specificare TOKENS_S3 o TOKENS_INLINE")
    if not tokens:
        raise RuntimeError("Nessun token disponibile.")
    return tokens

TOKENS_LIST = load_tokens()
TOKENS_BC = sc.broadcast(TOKENS_LIST)

# -----------------------------
# Broadcast configurazioni
# -----------------------------
CONF_BC = sc.broadcast({
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
})

PROXY_CONF_BC = sc.broadcast({
    "enabled": PROXY_ENABLED,
    "host": DECODO_HOST,
    "user": DECODO_USERNAME,
    "pwd": DECODO_PASSWORD,
    "port_start": DECODO_PORT_START,
    "port_end": DECODO_PORT_END,
})

# -----------------------------
# Funzioni di dominio
# -----------------------------
def perf_key_from_lower(perf_lower):
    if perf_lower == "ultrabullet":
        return "ultraBullet"
    return perf_lower

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
    if winner is None:
        result = "draw"
    else:
        result = "win" if winner == side else "loss"
    return side, result

def extract_rating_pair(game, side):
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

def build_activity_from_games(username, games_iter, conf):
    username_lower = username.lower()
    day_buckets = {}

    for g in games_iter:
        ts = g.get("createdAt") or g.get("lastMoveAt")
        if ts is None:
            continue

        day0_dt = day_utc_floor(ts)
        day_start_ms = int(day0_dt.timestamp() * 1000)
        day_end_ms   = day_start_ms + (24 * 60 * 60 * 1000) - 1
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
            b_games[perf_key] = {
                "win": 0, "loss": 0, "draw": 0,
                "rp": {"before": None, "after": None}
            }

        b_games[perf_key][result] += 1

        before, after = extract_rating_pair(g, side)
        if before is not None and b_games[perf_key]["rp"]["before"] is None:
            b_games[perf_key]["rp"]["before"] = before
        if after is not None:
            b_games[perf_key]["rp"]["after"] = after

    buckets_sorted = [day_buckets[k] for k in sorted(day_buckets.keys())]
    return {username: buckets_sorted}

# -----------------------------
# Proxy helpers
# -----------------------------
def make_proxy_pool(proxy_conf):
    if not proxy_conf["enabled"]:
        return []
    ports = list(range(proxy_conf["port_start"], proxy_conf["port_end"] + 1))
    pool = []
    user = proxy_conf["user"]
    pwd  = proxy_conf["pwd"]
    host = proxy_conf["host"]
    if not user or not pwd or not host or not ports:
        return []
    for p in ports:
        url = f"http://{user}:{pwd}@{host}:{p}"
        pool.append({"http": url, "https": url})
    return pool

# -----------------------------
# Download “singolo” (IP fisso per utente, no finestre)
# -----------------------------
def fetch_games_for_user_single(username, conf, token_cycle, user_proxy, session, allow_no_proxy_fallback=True):
    """
    Esegue UNA chiamata per tutto l'intervallo since/until.
    - Mantiene lo stesso proxy per l'intero utente.
    - Se lo stream risulta vuoto, ritenta (stesso proxy).
    - Ultimo tentativo, facoltativo, senza proxy.
    """
    url = f"https://lichess.org/api/games/user/{username}"
    params = {
        "perfType": conf["PERF_TYPES_PARAM"],
        "since": min(conf["SINCE_MS"], conf["UNTIL_MS"]),
        "until": max(conf["SINCE_MS"], conf["UNTIL_MS"]),
        "pgnInJson": "true",   # >>> FIX: forza formato NDJSON JSON-compatibile
    }
    if conf["RATED_ONLY"]:
        params["rated"] = "true"
    if conf["ANALYSED_ONLY"]:
        params["analysed"] = "true"

    last_exc = None
    used_no_proxy = False

    total_tries = conf["MAX_RETRIES"]
    attempt_idx = 0

    while attempt_idx < total_tries:
        attempt_idx += 1
        token = next(token_cycle)
        headers = {
            "Accept": "application/x-ndjson",
            "Authorization": f"Bearer {token}",
        }
        proxies = None if used_no_proxy else user_proxy

        try:
            resp = session.get(
                url, headers=headers, params=params, stream=True,
                timeout=conf["REQUEST_TIMEOUT"], proxies=proxies
            )

            # >>> FIX: controllo Content-Type, deve contenere "ndjson"
            ct = (resp.headers.get("Content-Type") or "").lower()
            if "ndjson" not in ct:
                raise requests.HTTPError(f"Unexpected Content-Type: {ct}")

            # 429/5xx => retry
            if resp.status_code in (429,) or resp.status_code >= 500:
                raise requests.HTTPError(f"{resp.status_code} {resp.reason}")
            resp.raise_for_status()

            seen = 0
            for line in resp.iter_lines(decode_unicode=True):
                if not line:
                    continue
                try:
                    g = json.loads(line)
                except Exception as e:
                    # >>> FIX: se la linea non è JSON valido, forza retry (stream corrotto)
                    raise requests.HTTPError(f"Bad NDJSON line: {e}")

                perf = (g.get("perf") or "").lower()
                if perf in conf["VALID_PERF_LOWER"]:
                    if perf == "ultrabullet":
                        g["perf"] = "ultraBullet"
                    else:
                        g["perf"] = perf
                    seen += 1
                    yield g

            # stream terminato “pulito”: se zero righe può essere filtro o stream troncato
            if seen == 0:
                # riprova (stesso proxy). Se è l’ultimo tentativo e consentito, prova senza proxy.
                if attempt_idx < total_tries:
                    print(f"[{username}] stream vuoto, retry {attempt_idx}/{total_tries}")
                    time.sleep(conf["RATE_LIMIT"])
                    continue
                elif allow_no_proxy_fallback and not used_no_proxy and PROXY_ENABLED:
                    print(f"[{username}] stream vuoto con proxy: ultimo tentativo senza proxy")
                    used_no_proxy = True
                    attempt_idx = 0  # ricomincia il ciclo di retry senza proxy
                    continue
            break  # successo o abbiamo prodotto 0 righe ma finiti i retry: esci

        except Exception as e:
            last_exc = e
            if attempt_idx < total_tries:
                sleep_s = (conf["BACKOFF_BASE"] ** (attempt_idx - 1)) + conf["RATE_LIMIT"]
                print(f"[{username}] retry {attempt_idx}/{total_tries} -> {e} | sleep {sleep_s:.1f}s")
                time.sleep(sleep_s)
            else:
                if allow_no_proxy_fallback and not used_no_proxy and PROXY_ENABLED:
                    print(f"[{username}] error con proxy: fallback finale senza proxy -> {e}")
                    used_no_proxy = True
                    attempt_idx = 0  # ricomincia i tentativi senza proxy
                else:
                    print(f"[{username}] errore definitivo: {last_exc}")
                    break

# -----------------------------
# Funzione per partizione Spark
# -----------------------------
def process_partition(user_iter):
    users = list(user_iter)
    if not users:
        return iter([])

    conf = CONF_BC.value

    # ⇨ MOD: identità della partizione Spark
    try:
        pid = TaskContext.get().partitionId()
    except Exception:
        pid = 0

    # --- TOKENS ---
    tokens = TOKENS_BC.value or []
    # ⇨ MOD: "stagger" dei token: ogni partizione parte da un offset diverso
    if tokens:
        offset = pid % len(tokens)
        tokens_staggered = tokens[offset:] + tokens[:offset]
    else:
        tokens_staggered = tokens
    token_cycle = cycle(tokens_staggered)

    # --- PROXY ---
    proxy_conf = PROXY_CONF_BC.value
    proxy_pool = make_proxy_pool(proxy_conf)

    # ⇨ MOD: "shard" del pool di proxy: ogni partizione usa un sottoinsieme (disgiunto dove possibile)
    if proxy_pool:
        # numero desiderato di proxy per partizione (equidistribuzione)
        shard_size = max(1, len(proxy_pool) // max(1, N_PARTITIONS))
        start = (pid * shard_size) % len(proxy_pool)
        end = start + shard_size
        if end <= len(proxy_pool):
            partition_proxies = proxy_pool[start:end]
        else:
            # wrap-around
            partition_proxies = proxy_pool[start:] + proxy_pool[:(end % len(proxy_pool))]
        proxy_cycle = cycle(partition_proxies)
    else:
        proxy_cycle = None

    session = requests.Session()
    out_lines = []

    for idx, u in enumerate(users, 1):
        u = (u or "").strip()
        if not u:
            continue
        try:
            # ⇨ MOD: scegliamo un proxy SOLO dallo shard della partizione
            user_proxy = next(proxy_cycle) if proxy_cycle else None

            games_iter = fetch_games_for_user_single(
                u, conf, token_cycle, user_proxy, session, allow_no_proxy_fallback=True
            )
            user_activity = build_activity_from_games(u, games_iter, conf)
        except Exception as e:
            print(f"[{u}] errore: {e}")
            user_activity = {u: []}

        out_lines.append(json.dumps(user_activity, ensure_ascii=False))

        if conf["RATE_LIMIT"] > 0:
            time.sleep(conf["RATE_LIMIT"])

    return iter(out_lines)

# -----------------------------
# Pipeline Spark
# -----------------------------
users_rdd = sc.textFile(INPUT_S3_GZ)
users_rdd = users_rdd.filter(lambda s: s is not None and s.strip() != "")

if MAX_USERS is not None and MAX_USERS > 0:
    limited = users_rdd.zipWithIndex().filter(lambda kv: kv[1] < MAX_USERS).keys()
    users_rdd = limited

users_rdd = users_rdd.repartition(N_PARTITIONS)
lines_rdd = users_rdd.mapPartitions(process_partition)

target_rdd = lines_rdd
if OUTPUT_SINGLE_FILE:
    target_rdd = target_rdd.coalesce(1)
elif OUTPUT_COALESCE > 0:
    target_rdd = target_rdd.coalesce(OUTPUT_COALESCE)

target_rdd.saveAsTextFile(OUTPUT_S3_DIR)

job.commit()


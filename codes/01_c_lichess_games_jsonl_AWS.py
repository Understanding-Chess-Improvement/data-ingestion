# glue_01_c_lichess_games_jsonl_hardcoded.py
# Output: directory S3 con part-* (NDJSON). Ogni riga = {"<username>": [bucket_aggregato, ...]}
# Logica allineata a "activity": pgnInJson=true, check Content-Type ndjson,
# fallback user.id per il match del lato, e VINCOLO proxy→partizione.

import json
import time
import requests
from itertools import cycle
from datetime import datetime, timezone, timedelta

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark import TaskContext  # >>> MOD: per ottenere l'ID della partizione

# ============================================================================
#                          PARAMETRI (HARDCODED)
# ============================================================================
JOB_NAME = "01_c_lichess_games_jsonl"

# Input: lista utenti GZIP (uno per riga) su S3
INPUT_S3_GZ = "s3://lichess-raw-data/lichess_list_filtered_survey.txt.gz"

# Output: cartella S3 con part-* (NDJSON: una riga per utente)
OUTPUT_S3_DIR = "s3://lichess-dwh/lichess_games_jsonl/run_dt=2025-08-17"

# Token Lichess
TOKENS_S3     = "s3://lichess-fide-raw-data/TOKEN.txt"  # uno per riga
TOKENS_INLINE = None  # "tok1,tok2,..."

# Intervallo temporale (UTC) — una SOLA richiesta copre tutto l'intervallo
SINCE_DATE = "2023-01-01"   
UNTIL_DATE = None           #None = adesso

# Filtri server-side
RATED_ONLY    = True
ANALYSED_ONLY = True     

# Perf consentite
VALID_PERF_LOWER = ["ultrabullet", "bullet", "blitz", "rapid"]
PERF_TYPES_CAMEL = ["ultraBullet", "bullet", "blitz", "rapid"]
PERF_TYPES_PARAM = ",".join(PERF_TYPES_CAMEL)

# Aggregazione output (NON riguarda il download): 1=giorno, 7=settimana, 30=mese
GROUP_WINDOW_DAYS = 1

# Retry / pacing
RATE_LIMIT        = 0.01    
MAX_RETRIES       = 4
BACKOFF_BASE      = 1.8
REQUEST_TIMEOUT   = 60
MAX_USERS         = None    #None = tutti

# Parallelismo Spark
N_PARTITIONS        = 25
OUTPUT_COALESCE     = 0
OUTPUT_SINGLE_FILE  = False

# Proxy Decodo
PROXY_ENABLED     = True
DECODO_HOST       = "dc.decodo.com"
DECODO_USERNAME     = "<your-username>"
DECODO_PASSWORD     = "<your-password>"
DECODO_PORT_START = 10001
DECODO_PORT_END   = 10500
# ============================================================================

# ----------------------------- Glue / Spark setup ---------------------------
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, {})

# ----------------------------- Utility tempo --------------------------------
def to_epoch_ms(date_str_or_none):
    if not date_str_or_none:
        return None
    dt = datetime.strptime(date_str_or_none, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)

SINCE_MS = to_epoch_ms(SINCE_DATE)
UNTIL_MS = to_epoch_ms(UNTIL_DATE) or int(datetime.now(timezone.utc).timestamp() * 1000)

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

# ----------------------------- Tokens ---------------------------------------
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

# ----------------------------- Config broadcast -----------------------------
CONF_BC = sc.broadcast({
    "RATED_ONLY": RATED_ONLY,
    "ANALYSED_ONLY": ANALYSED_ONLY,
    "VALID_PERF_LOWER": VALID_PERF_LOWER,
    "PERF_TYPES_PARAM": PERF_TYPES_PARAM,
    "GROUP_WINDOW_DAYS": GROUP_WINDOW_DAYS,
    "RATE_LIMIT": RATE_LIMIT,
    "MAX_RETRIES": MAX_RETRIES,
    "BACKOFF_BASE": BACKOFF_BASE,
    "REQUEST_TIMEOUT": REQUEST_TIMEOUT,
    "SINCE_MS": min(SINCE_MS, UNTIL_MS),
    "UNTIL_MS": max(SINCE_MS, UNTIL_MS),
})

PROXY_CONF_BC = sc.broadcast({
    "enabled": PROXY_ENABLED,
    "host": DECODO_HOST,
    "user": DECODO_USERNAME,
    "pwd": DECODO_PASSWORD,
    "port_start": DECODO_PORT_START,
    "port_end": DECODO_PORT_END,
})

# ----------------------------- Domain helpers -------------------------------
def extract_side_and_result(game: dict, username_lower: str):
    players = game.get("players", {}) or {}
    w = players.get("white", {}) or {}
    b = players.get("black", {}) or {}
    # >>> MOD: fallback a user.id + lowercase per robustezza (prima solo user.name)
    w_user = ((w.get("user") or {}).get("name") or (w.get("user") or {}).get("id") or "").lower()
    b_user = ((b.get("user") or {}).get("name") or (b.get("user") or {}).get("id") or "").lower()
    if w_user == username_lower:
        side = "white"
    elif b_user == username_lower:
        side = "black"
    else:
        return None, None
    winner = game.get("winner")
    result = "draw" if winner is None else ("win" if winner == side else "loss")
    return side, result

def extract_rating_pair(game: dict, side: str):
    p = (game.get("players") or {}).get(side, {})
    r = p.get("rating"); rd = p.get("ratingDiff")
    if r is None or rd is None:
        return None, None
    try:
        before = int(r) - int(rd)
        after = int(r)
        return before, after
    except Exception:
        return None, None

def perf_key_from_lower(perf_lower):
    return "ultraBullet" if perf_lower == "ultrabullet" else perf_lower

# ----------------------------- Proxy helpers --------------------------------
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

# ----------------------------- Core (IP fisso per utente) -------------------
def stream_user_games(username, conf, token_cycle, user_proxy, session, allow_no_proxy_fallback=True):
    """
    Una sola richiesta full-range per utente.
    - Mantiene lo stesso proxy per tutto lo stream.
    - Retry sullo stesso proxy; fallback finale senza proxy.
    Ritorna un generatore di record JSON (partite) già filtrati per perf consentite.
    """
    url = f"https://lichess.org/api/games/user/{username}"
    params = {
        "perfType": conf["PERF_TYPES_PARAM"],
        "since": conf["SINCE_MS"],
        "until": conf["UNTIL_MS"],
        # fields utili per i games
        "moves": "true",
        "evals": "true",
        "accuracy": "true",
        "clocks": "true",
        "opening": "true",
        "division": "true",
        "pgnInJson": "true",  
    }
    if conf["RATED_ONLY"]:
        params["rated"] = "true"
    if conf["ANALYSED_ONLY"]:
        params["analysed"] = "true"

    last_exc = None
    used_no_proxy = False
    attempt_idx = 0
    total_tries = conf["MAX_RETRIES"]

    while attempt_idx < total_tries:
        attempt_idx += 1
        token = next(token_cycle)
        headers = {"Accept": "application/x-ndjson", "Authorization": f"Bearer {token}"}
        proxies = None if used_no_proxy else user_proxy

        try:
            resp = session.get(
                url, headers=headers, params=params, stream=True,
                timeout=conf["REQUEST_TIMEOUT"], proxies=proxies
            )

            # Controllo Content-Type ndjson (come activity)
            ct = (resp.headers.get("Content-Type") or "").lower()
            if "ndjson" not in ct:
                raise requests.HTTPError(f"Unexpected Content-Type: {ct}")  # forza retry/fallback

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
                    # linea NDJSON non parsabile => stream corrotto, retry
                    raise requests.HTTPError(f"Bad NDJSON line: {e}")

                perf_lower = (g.get("perf") or "").lower()
                if perf_lower not in conf["VALID_PERF_LOWER"]:
                    continue
                g["perf"] = perf_key_from_lower(perf_lower)
                seen += 1
                yield g

            # Stream terminato; se 0 righe, riproviamo (stesso IP), poi senza proxy
            if seen == 0:
                if attempt_idx < total_tries:
                    print(f"[{username}] stream vuoto, retry {attempt_idx}/{total_tries}")
                    time.sleep(conf["RATE_LIMIT"])
                    continue
                elif allow_no_proxy_fallback and not used_no_proxy and PROXY_ENABLED:
                    print(f"[{username}] stream vuoto con proxy: ultimo tentativo senza proxy")
                    used_no_proxy = True
                    attempt_idx = 0
                    continue
            break  # ok

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
                    attempt_idx = 0
                else:
                    print(f"[{username}] errore definitivo: {last_exc}")
                    break

# ----------------------------- Build buckets (immutato) ---------------------
def build_buckets_from_stream(username, games_iter, conf):
    """
    Aggrega in bucket per finestra GROUP_WINDOW_DAYS e aggiunge details.
    """
    username_lower = username.lower()
    buckets = {}

    for g in games_iter:
        ts = g.get("createdAt") or g.get("lastMoveAt")
        if ts is None:
            continue

        w_start_dt = window_floor_utc(ts, conf["GROUP_WINDOW_DAYS"])
        start_ms, end_ms, label = fmt_window_label(w_start_dt, conf["GROUP_WINDOW_DAYS"])

        bucket = buckets.get(start_ms)
        if bucket is None:
            bucket = {
                "interval": {"start": start_ms, "end": end_ms},
                "window": label,
                "games": {},
                "details": []
            }
            buckets[start_ms] = bucket

        side, result = extract_side_and_result(g, username_lower)
        if side is None:
            continue

        perf_key = g["perf"]
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

        # dettagli completi per-partita
        rec = {"username": username}
        rec.update(g)
        bucket["details"].append(rec)

    buckets_sorted = [buckets[k] for k in sorted(buckets.keys())]
    return {username: buckets_sorted}

# ----------------------------- Process partition ----------------------------
def process_partition(user_iter):
    users = list(user_iter)
    if not users:
        return iter([])

    conf = CONF_BC.value

    # --- TOKENS (stagger per partizione, come 01_b) -------------------------
    tokens = TOKENS_BC.value or []
    if tokens:
        try:
            pid = TaskContext.get().partitionId()  # >>> MOD
        except Exception:
            pid = 0
        offset = pid % len(tokens)                # >>> MOD
        tokens_staggered = tokens[offset:] + tokens[:offset]  # >>> MOD
    else:
        tokens_staggered = tokens
    token_cycle = cycle(tokens_staggered)

    # --- PROXY (shard per partizione, come 01_b) ----------------------------
    proxy_conf = PROXY_CONF_BC.value
    proxy_pool = make_proxy_pool(proxy_conf)

    if proxy_pool:
        shard_size = max(1, len(proxy_pool) // max(1, N_PARTITIONS))  # >>> MOD
        try:
            pid = TaskContext.get().partitionId()                      # >>> MOD
        except Exception:
            pid = 0
        start = (pid * shard_size) % len(proxy_pool)                   # >>> MOD
        end = start + shard_size                                       # >>> MOD
        if end <= len(proxy_pool):
            partition_proxies = proxy_pool[start:end]
        else:
            partition_proxies = proxy_pool[start:] + proxy_pool[:(end % len(proxy_pool))]
        proxy_cycle = cycle(partition_proxies)
    else:
        proxy_cycle = None

    session = requests.Session()
    out_lines = []

    for u in users:
        u = (u or "").strip()
        if not u:
            continue
        try:
            user_proxy = next(proxy_cycle) if proxy_cycle else None
            stream_iter = stream_user_games(
                u, conf, token_cycle, user_proxy, session, allow_no_proxy_fallback=True
            )
            result = build_buckets_from_stream(u, stream_iter, conf)
        except Exception as e:
            print(f"[{u}] errore: {e}")
            result = {u: []}

        out_lines.append(json.dumps(result, ensure_ascii=False))

        if conf["RATE_LIMIT"] > 0:
            time.sleep(conf["RATE_LIMIT"])

    return iter(out_lines)

# ----------------------------- Pipeline Spark -------------------------------
users_rdd = sc.textFile(INPUT_S3_GZ).filter(lambda s: s is not None and s.strip() != "")

if isinstance(MAX_USERS, int) and MAX_USERS > 0:
    users_rdd = users_rdd.zipWithIndex().filter(lambda kv: kv[1] < MAX_USERS).keys()

users_rdd = users_rdd.repartition(N_PARTITIONS)
lines_rdd = users_rdd.mapPartitions(process_partition)

target_rdd = lines_rdd
if OUTPUT_SINGLE_FILE:
    target_rdd = target_rdd.coalesce(1)
elif OUTPUT_COALESCE > 0:
    target_rdd = target_rdd.coalesce(OUTPUT_COALESCE)

target_rdd.saveAsTextFile(OUTPUT_S3_DIR)
job.commit()


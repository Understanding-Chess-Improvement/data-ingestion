# 02_lichess_fide_match_FIXED.py
import json
import unicodedata
import re
import io
import os
import zipfile
import requests
import xml.etree.ElementTree as ET
from typing import Dict, Any, Iterable, Tuple, Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# CONFIG
# =========================
LICHESS_USERS_JSONL = r"output\lichess_users.jsonl"
OUT_MATCH_JSONL     = r"output\lichess_fide_match.jsonl"
FIDE_URL_ZIP        = "http://ratings.fide.com/download/players_list_xml.zip"
FIDE_LOCAL_ZIP      = None
MAX_WORKERS         = 1 #Threads per matvhing in parallelo
PROGRESS_EVERY      = 500

# =========================
# Utils ISO country (alpha2/alpha3)
# =========================
ALPHA2_TO_ALPHA3 = {
    "IT":"ITA","US":"USA","GB":"GBR","FR":"FRA","DE":"DEU","ES":"ESP","PT":"PRT","RU":"RUS","UA":"UKR",
    "PL":"POL","CZ":"CZE","SK":"SVK","RO":"ROU","HU":"HUN","AT":"AUT","CH":"CHE","NL":"NLD","BE":"BEL",
    "SE":"SWE","NO":"NOR","DK":"DNK","FI":"FIN","IS":"ISL","IE":"IRL","GR":"GRC","TR":"TUR","IL":"ISR",
    "EG":"EGY","ZA":"ZAF","CA":"CAN","MX":"MEX","BR":"BRA","AR":"ARG","CL":"CHL","CO":"COL","PE":"PER",
    "VE":"VEN","AU":"AUS","NZ":"NZL","IN":"IND","CN":"CHN","JP":"JPN","KR":"KOR","SG":"SGP","PH":"PHL",
    "ID":"IDN","TH":"THA","MY":"MYS","VN":"VNM","IR":"IRN","IQ":"IRQ","SA":"SAU","AE":"ARE","QA":"QAT",
    "KW":"KWT","BH":"BHR","OM":"OMN","MA":"MAR","TN":"TUN","DZ":"DZA","NG":"NGA","KE":"KEN","ET":"ETH",
    "RS":"SRB","ME":"MNE","BA":"BIH","MK":"MKD","AL":"ALB","BG":"BGR","LT":"LTU","LV":"LVA","EE":"EST",
    "MD":"MDA","GE":"GEO","AM":"ARM","AZ":"AZE","BY":"BLR"
}
A2_NAME = {
    "IT":"Italy","US":"United States","GB":"United Kingdom","FR":"France","DE":"Germany","ES":"Spain","PT":"Portugal",
    "RU":"Russia","UA":"Ukraine","PL":"Poland","CZ":"Czech Republic","SK":"Slovakia","RO":"Romania","HU":"Hungary",
    "AT":"Austria","CH":"Switzerland","NL":"Netherlands","BE":"Belgium","SE":"Sweden","NO":"Norway","DK":"Denmark",
    "FI":"Finland","IS":"Iceland","IE":"Ireland","GR":"Greece","TR":"Turkey","IL":"Israel","EG":"Egypt",
    "ZA":"South Africa","CA":"Canada","MX":"Mexico","BR":"Brazil","AR":"Argentina","CL":"Chile","CO":"Colombia",
    "PE":"Peru","VE":"Venezuela","AU":"Australia","NZ":"New Zealand","IN":"India","CN":"China","JP":"Japan",
    "KR":"South Korea","SG":"Singapore","PH":"Philippines","ID":"Indonesia","TH":"Thailand","MY":"Malaysia",
    "VN":"Vietnam","IR":"Iran","IQ":"Iraq","SA":"Saudi Arabia","AE":"United Arab Emirates","QA":"Qatar",
    "KW":"Kuwait","BH":"Bahrain","OM":"Oman","MA":"Morocco","TN":"Tunisia","DZ":"Algeria","NG":"Nigeria",
    "KE":"Kenya","ET":"Ethiopia","RS":"Serbia","ME":"Montenegro","BA":"Bosnia and Herzegovina","MK":"North Macedonia",
    "AL":"Albania","BG":"Bulgaria","LT":"Lithuania","LV":"Latvia","EE":"Estonia","MD":"Moldova","GE":"Georgia",
    "AM":"Armenia","AZ":"Azerbaijan","BY":"Belarus"
}
A3_NAME = {
    "ITA":"Italy","USA":"United States","GBR":"United Kingdom","FRA":"France","DEU":"Germany","ESP":"Spain","PRT":"Portugal",
    "RUS":"Russia","UKR":"Ukraine","POL":"Poland","CZE":"Czech Republic","SVK":"Slovakia","ROU":"Romania","HUN":"Hungary",
    "AUT":"Austria","CHE":"Switzerland","NLD":"Netherlands","BEL":"Belgium","SWE":"Sweden","NOR":"Norway","DNK":"Denmark",
    "FIN":"Finland","ISL":"Iceland","IRL":"Ireland","GRC":"Greece","TUR":"Turkey","ISR":"Israel","EGY":"Egypt",
    "ZAF":"South Africa","CAN":"Canada","MEX":"Mexico","BRA":"Brazil","ARG":"Argentina","CHL":"Chile","COL":"Colombia",
    "PER":"Peru","VEN":"Venezuela","AUS":"Australia","NZL":"New Zealand","IND":"India","CHN":"China","JPN":"Japan",
    "KOR":"South Korea","SGP":"Singapore","PHL":"Philippines","IDN":"Indonesia","THA":"Thailand","MYS":"Malaysia",
    "VNM":"Vietnam","IRN":"Iran","IRQ":"Iraq","SAU":"Saudi Arabia","ARE":"United Arab Emirates","QAT":"Qatar",
    "KWT":"Kuwait","BHR":"Bahrain","OMN":"Oman","MAR":"Morocco","TUN":"Tunisia","DZA":"Algeria","NGA":"Nigeria",
    "KEN":"Kenya","ETH":"Ethiopia","SRB":"Serbia","MNE":"Montenegro","BIH":"Bosnia and Herzegovina","MKD":"North Macedonia",
    "ALB":"Albania","BGR":"Bulgaria","LTU":"Lithuania","LVA":"Latvia","EST":"Estonia","MDA":"Moldova","GEO":"Georgia",
    "ARM":"Armenia","AZE":"Azerbaijan","BLR":"Belarus"
}
def alpha2_to_alpha3(alpha2: str) -> str:
    if not alpha2:
        return ""
    return ALPHA2_TO_ALPHA3.get(alpha2.strip().upper(), "")

# =========================
# Normalizzazioni testo
# =========================
def normalize_simple(s: Any) -> str:
    if s is None:
        return ""
    return unicodedata.normalize('NFKD', str(s)).encode('ascii', 'ignore').decode('utf-8').strip()

def normalize_aggressive(s: Any) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize('NFKD', str(s)).encode('ascii', 'ignore').decode('utf-8')
    s = re.sub(r"[-']", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip().lower()

# =========================
# Lettura NDJSON Lichess
# =========================
def iter_jsonl(path: str) -> Iterable[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue

def unwrap_username_record(rec: Dict[str, Any]) -> Tuple[Optional[str], Dict[str, Any]]:
    if isinstance(rec, dict) and len(rec) == 1:
        username = next(iter(rec))
        payload  = rec[username] if isinstance(rec[username], dict) else {}
        if "user_info" in payload and isinstance(payload["user_info"], dict):
            payload["user_info"].setdefault("Username", username)
        return username, payload
    username = rec.get("username") or rec.get("Username") or rec.get("id")
    return username, rec

def extract_profile_fields(rec: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    username, payload = unwrap_username_record(rec)
    user_info = payload.get("user_info", {})
    username = (
        username
        or user_info.get("Username")
        or payload.get("username")
        or payload.get("id")
    )
    if not username:
        return None

    real_name = (
        user_info.get("realName")
        or " ".join(x for x in [user_info.get("firstName"), user_info.get("lastName")] if x)
    )
    real_name = normalize_simple(real_name)
    first_name = normalize_simple(user_info.get("firstName"))
    last_name  = normalize_simple(user_info.get("lastName"))

    flag = normalize_simple(user_info.get("flag") or user_info.get("country")).upper()
    fide_rating = normalize_simple(user_info.get("fide_rating") or user_info.get("fideRating") or "")

    nome, cognome, inversione = "", "", False
    if first_name and last_name:
        nome, cognome = first_name, last_name
    elif real_name:
        parts = real_name.split()
        if len(parts) == 1:
            nome = parts[0]
        elif len(parts) == 2:
            nome, cognome = parts
            inversione = True
        else:
            nome, cognome = parts[0], " ".join(parts[1:])

    if not (nome and cognome):
        return None

    out = {
        "Username": username,
        "Nome": nome,
        "Cognome": cognome,
        "Federazione_Lichess_Alpha2": flag,
        "Federazione_Lichess_Alpha3": alpha2_to_alpha3(flag),
        "FIDE_Rating": fide_rating,
        "Inversione_Possibile": inversione,
    }
    out["Cognome_clean"] = normalize_aggressive(out["Cognome"])
    out["Nome_clean"]    = normalize_aggressive(out["Nome"])
    return out

# ---- MINIMA MODIFICA: trasformo in generatore (no lista in RAM)
def load_lichess_candidates(jsonl_path: str) -> Iterable[Dict[str, Any]]:
    for rec in iter_jsonl(jsonl_path):
        out = extract_profile_fields(rec)
        if out:
            yield out

# =========================
# Caricamento elenco FIDE (XML dentro ZIP)
# =========================
def load_fide_list() -> List[Dict[str, Any]]:
    if FIDE_LOCAL_ZIP and os.path.exists(FIDE_LOCAL_ZIP):
        print(f"Carico lista FIDE da locale: {FIDE_LOCAL_ZIP}")
        with zipfile.ZipFile(FIDE_LOCAL_ZIP, "r") as z:
            xml_filename = z.namelist()[0]
            with z.open(xml_filename) as xml_file:
                tree = ET.parse(xml_file)
    else:
        print(f"Scaricamento file ZIP da {FIDE_URL_ZIP} ...")
        resp = requests.get(FIDE_URL_ZIP, timeout=120)
        resp.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
            xml_filename = z.namelist()[0]
            print(f"File XML nel pacchetto: {xml_filename}")
            with z.open(xml_filename) as xml_file:
                print("Parsing XML in memoria ...")
                tree = ET.parse(xml_file)

    root = tree.getroot()
    fide_list: List[Dict[str, Any]] = []
    for player in root.findall(".//player"):
        name_raw = (player.findtext("name", default="") or "").strip()
        if "," in name_raw:
            cognome, nome_full = [part.strip() for part in name_raw.split(",", 1)]
            nome = nome_full.strip()
        else:
            cognome, nome = "", name_raw

        item = {
            "FIDE_ID":      player.findtext("fideid",   default=""),
            "Nome":         nome,
            "Cognome":      cognome,
            "Federazione":  player.findtext("country",  default=""),
            "Elo":          player.findtext("rating",   default=""),
            "AnnoNascita":  player.findtext("birthday", default=""),
            "Genere":       player.findtext("sex",      default="")
        }
        item["Cognome_clean"] = normalize_aggressive(item["Cognome"])
        item["Nome_clean"]    = normalize_aggressive(item["Nome"])
        fide_list.append(item)
    return fide_list

# =========================
# Matching helpers
# =========================
def filter_by_exact_surname_and_name_include(fide_list: List[Dict[str, Any]],
                                             cognome_clean: str,
                                             nome_clean: str) -> List[Dict[str, Any]]:
    out = []
    for p in fide_list:
        if p["Cognome_clean"] == cognome_clean:
            n = p["Nome_clean"]
            if (nome_clean in n) or (n in nome_clean):
                out.append(p)
    return out

def match_user(row: Dict[str, Any], fide_list: List[Dict[str, Any]]) -> Optional[Tuple[str, Dict[str, Any]]]:
    cognome_query = row.get("Cognome_clean","")
    nome_query    = row.get("Nome_clean","")
    inversione    = row.get("Inversione_Possibile", False)

    if not cognome_query or not nome_query:
        return None

    matches = filter_by_exact_surname_and_name_include(fide_list, cognome_query, nome_query)
    if not matches and inversione:
        matches = filter_by_exact_surname_and_name_include(fide_list, nome_query, cognome_query)
    if not matches:
        return None

    omonimi = len(matches)
    if len(matches) == 1:
        sel = matches[0]
        conf = "Altissimo"
    else:
        candidates = matches[:]
        fed_from_flag = (row.get("Federazione_Lichess_Alpha3") or "").upper()
        if fed_from_flag:
            fed_match = [c for c in candidates if (c.get("Federazione","").upper() == fed_from_flag)]
            if fed_match:
                candidates = fed_match
        rating = row.get("FIDE_Rating", "")
        if rating and str(rating).isdigit():
            candidates = [c for c in candidates if c.get("Elo","") == str(rating)]
        sel = candidates[0] if candidates else matches[0]
        conf = "Disambiguato"

    username = row.get("Username") or row.get("username") or ""
    if not username:
        return None

    alpha3 = (sel.get("Federazione") or "").upper()
    fed_name = A3_NAME.get(alpha3, alpha3 if alpha3 else "")
    flag2 = (row.get("Federazione_Lichess_Alpha2") or "").upper()
    flag_name = A2_NAME.get(flag2, flag2)

    record = {
        "FIDE_ID":                sel.get("FIDE_ID",""),
        "Nome_FIDE":              sel.get("Nome",""),
        "Cognome_FIDE":           sel.get("Cognome",""),
        "Nome_Lichess":           row.get("Nome",""),
        "Cognome_Lichess":        row.get("Cognome",""),
        "Federazione_sigla_FIDE": alpha3,
        "Federazione_FIDE":       fed_name,
        "Flag_Lichess":           flag_name,
        "Elo_FIDE":               sel.get("Elo",""),
        "AnnoNascita_FIDE":       sel.get("AnnoNascita",""),
        "Genere":                 sel.get("Genere",""),
        "Confidence_Level":       conf,
        "MatchType":              "Exact-Includes"
    }
    if omonimi > 1:
        record["Omonimi_Trovati"] = omonimi

    return (username, record)

# =========================
# MAIN (streaming senza accumulare in RAM)
# =========================
def main():
    print(f"Caricamento candidati da: {LICHESS_USERS_JSONL}")

    # Carica FIDE (in RAM, come prima: necessario per matching veloce)
    fide_list = load_fide_list()
    if not fide_list:
        print("Lista FIDE vuota o non caricata. Stop.")
        return
    print(f"Giocatori FIDE caricati: {len(fide_list)}")

    os.makedirs(os.path.dirname(OUT_MATCH_JSONL), exist_ok=True)
    total_written = 0
    submitted = 0
    inflight = set()
    INFLIGHT_MAX = MAX_WORKERS * 4  # piccolo buffer per evitare picchi RAM

    print("Avvio matching multi-thread (streaming input/output)...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex, \
         open(OUT_MATCH_JSONL, "w", encoding="utf-8") as f_out:

        # Sottomettiamo i candidati a scorrimento, senza accumularli in lista
        for row in load_lichess_candidates(LICHESS_USERS_JSONL):
            fut = ex.submit(match_user, row, fide_list)
            inflight.add(fut)
            submitted += 1

            # Progresso
            if submitted % PROGRESS_EVERY == 0:
                print(f"...progresso: {submitted} candidati inviati")

            # Quando il buffer è pieno, svuota un future completato e scrivi subito
            if len(inflight) >= INFLIGHT_MAX:
                done = next(as_completed(inflight))
                inflight.remove(done)
                res = done.result()
                if res is not None:
                    username, rec = res
                    f_out.write(json.dumps({username: rec}, ensure_ascii=False) + "\n")
                    total_written += 1

        # Svuota i future rimanenti
        for done in as_completed(inflight):
            res = done.result()
            if res is not None:
                username, rec = res
                f_out.write(json.dumps({username: rec}, ensure_ascii=False) + "\n")
                total_written += 1

    print(f"Salvati {total_written} match in NDJSON: {OUT_MATCH_JSONL}")

if __name__ == "__main__":
    main()

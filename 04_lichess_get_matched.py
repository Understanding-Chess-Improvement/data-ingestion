import json
import gzip
import os

# === File di input/output (percorsi relativi come stringhe) ===
INPUT_JSONL  = r"output\lichess_fide_match.jsonl"
OUTPUT_GZTXT = r"output\lichess_list_matched_fide.txt.gz"

def main():
    os.makedirs(os.path.dirname(OUTPUT_GZTXT) or ".", exist_ok=True)
    total_lines = 0
    wrote = 0

    with open(INPUT_JSONL, "r", encoding="utf-8") as fin, \
         gzip.open(OUTPUT_GZTXT, "wt", encoding="utf-8", newline="\n") as fout:

        for line in fin:
            total_lines += 1
            line = line.strip()
            if not line:
                continue

            try:
                obj = json.loads(line)
            except Exception:
                continue  # linea non valida, skip

            if isinstance(obj, dict) and obj:
                # Struttura: {"UsernameEsatto": {...}}
                username = next(iter(obj.keys()))
                if not isinstance(username, str) and "username" in obj:
                    username = obj["username"]

                if isinstance(username, str) and username:
                    fout.write(username + "\n")
                    wrote += 1

    print(f"Righe lette: {total_lines} | User scritti: {wrote}")
    print(f"Output: {OUTPUT_GZTXT}")

if __name__ == "__main__":
    main()

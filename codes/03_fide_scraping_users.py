import json
import time
import random
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from tqdm import tqdm

INPUT_FILE = r"output\lichess_fide_match.jsonl"
OUTPUT_FILE_JSONL = r"output\fide_scraping_user.jsonl"
DELAY_RANGE = (1.5, 5.0)
MAX_USERS = None  # None = tutti

# Se esiste un vecchio file di output, lo rimuoviamo
if os.path.exists(OUTPUT_FILE_JSONL):
    os.remove(OUTPUT_FILE_JSONL)

# === Setup Selenium ===
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
driver = webdriver.Chrome(options=chrome_options)
wait = WebDriverWait(driver, 30)

# === Funzione scraping singolo utente ===
def scrape_fide_profile(username, fide_id, base_data):
    url = f"https://ratings.fide.com/profile/{fide_id}/chart"
    driver.get(url)
    time.sleep(random.uniform(*DELAY_RANGE))

    federation = driver.find_element(By.CLASS_NAME, "profile-info-country").text.strip()
    birth_year = driver.find_element(By.CLASS_NAME, "profile-info-byear").text.strip()
    gender = driver.find_element(By.CLASS_NAME, "profile-info-sex").text.strip()
    fide_title = driver.find_element(By.CLASS_NAME, "profile-info-title").text.strip()

    std_rating = driver.find_element(By.CLASS_NAME, "profile-standart").text.split("\n")[0]
    rapid_rating = driver.find_element(By.CLASS_NAME, "profile-rapid").text.split("\n")[0]
    blitz_rating = driver.find_element(By.CLASS_NAME, "profile-blitz").text.split("\n")[0]

    # Vai nella scheda Progress
    progress_tab = wait.until(EC.element_to_be_clickable((By.ID, 'chart_button')))
    progress_tab.click()
    time.sleep(2)

    table = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'profile-table_calc')))
    rows = table.find_elements(By.TAG_NAME, 'tr')
    rating_history = []

    for row in rows[1:]:
        cols = row.find_elements(By.TAG_NAME, 'td')
        if len(cols) >= 5:
            rating_history.append({
                'Period': cols[0].text.strip(),
                'Standard': {'Rating': cols[1].text.split("\n")[0], 'Games': cols[2].text.strip()},
                'Rapid':    {'Rating': cols[3].text.split("\n")[0], 'Games': cols[4].text.strip()},
                'Blitz':    {'Rating': cols[5].text.split("\n")[0] if len(cols) > 5 else "", 
                             'Games': cols[6].text.strip() if len(cols) > 6 else ""}
            })

    return {
        **base_data,
        'FIDE_Profile': {
            'Federation': federation,
            'BirthYear': birth_year,
            'Gender': gender,
            'Title': fide_title,
            'Elo': {
                'Standard': std_rating,
                'Rapid': rapid_rating,
                'Blitz': blitz_rating
            },
            'RatingHistory': rating_history
        }
    }

# === Lettura e scraping progressivo ===
with open(INPUT_FILE, 'r', encoding='utf-8') as f_in, \
     open(OUTPUT_FILE_JSONL, 'w', encoding='utf-8') as f_out:

    count = 0
    for line in tqdm(f_in, desc="Processing users"):
        if MAX_USERS and count >= MAX_USERS:
            break

        record = json.loads(line)
        username, base_data = next(iter(record.items()))
        fide_id = base_data.get("FIDE_ID")
        if not fide_id:
            continue

        try:
            enriched_data = scrape_fide_profile(username, fide_id, base_data)
            # Scriviamo subito una riga nel JSONL
            f_out.write(json.dumps({username: enriched_data}, ensure_ascii=False) + "\n")
            count += 1
        except Exception as e:
            tqdm.write(f"Errore per FIDE_ID={fide_id} | Username={username}: {e}")

driver.quit()

print(f"Scraping completato. Output in {OUTPUT_FILE_JSONL}")


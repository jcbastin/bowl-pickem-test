import os
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("CFBD_API_KEY")
HEADERS = {"Authorization": f"Bearer {API_KEY}"}

def fetch_postseason_lines():
    url = "https://api.collegefootballdata.com/lines?season=2025&seasonType=postseason"
    r = requests.get(url, headers=HEADERS)
    print(f"[STATUS] {r.status_code}")
    return r.json()

def extract_best_spread(entry):
    lines = entry.get("lines", [])

    dk = next((l for l in lines
               if l.get("provider") == "DraftKings"
               and l.get("spread") is not None), None)
    if dk:
        return dk["spread"]

    bov = next((l for l in lines
                if l.get("provider") == "Bovada"
                and l.get("spread") is not None), None)
    if bov:
        return bov["spread"]

    return None

def main():
    data = fetch_postseason_lines()
    print("\n===== SPREAD RESULTS FROM CFBD =====\n")

    found = 0
    for entry in data:
        gid = entry.get("id")
        spread = extract_best_spread(entry)

        if spread is not None:
            found += 1
            print(f"game_id={gid}   spread={spread}")

    print(f"\n[INFO] Total spreads found: {found}")
    print("\n====================================\n")

if __name__ == "__main__":
    main()

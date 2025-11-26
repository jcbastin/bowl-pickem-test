import pandas as pd
import requests
import time
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("CFBD_API_KEY")
HEADERS = {"Authorization": f"Bearer {API_KEY}"}

CSV_PATH = "data/test_games.csv"

# ---------------------------
# Helper: Safe API fetch
# ---------------------------
def fetch_games_with_backoff():
    delay = 3
    max_retries = 2
    attempt = 0

    while attempt <= max_retries:
        try:
            resp = requests.get(
                "https://api.collegefootballdata.com/games",
                params={"year": 2025, "seasonType": "regular", "week": 14},
                headers=HEADERS,
                timeout=10
            )

            if resp.status_code == 429:
                print(f"⚠️ Rate limited — retrying in {delay}s…")
                time.sleep(delay)
                delay = min(delay * 2, 60)  # exponential backoff, capped
                attempt += 1
                continue

            resp.raise_for_status()
            data = resp.json()

            # CFBD sometimes returns ["Too many requests"]
            if isinstance(data, list) and len(data) > 0 and isinstance(data[0], str):
                print(f"⚠️ API rate limit string — waiting {delay}s…")
                time.sleep(delay)
                delay = min(delay * 2, 60)
                attempt += 1
                continue

            return data

        except Exception as e:
            print(f"⚠️ Error contacting API: {e} — retrying in {delay}s")
            time.sleep(delay)
            delay = min(delay * 2, 60)
            attempt += 1

    return None

# ---------------------------
# Manual overrides for buggy API games
# ---------------------------
MANUAL_WINNERS = {
    ("Northwestern", "Michigan"): "Michigan",
}

def apply_manual_winner(home, away):
    pair = (home, away)
    rev_pair = (away, home)

    if pair in MANUAL_WINNERS:
        return MANUAL_WINNERS[pair]

    if rev_pair in MANUAL_WINNERS:
        return MANUAL_WINNERS[rev_pair]

    return None

# ---------------------------
# Main update loop
# ---------------------------
while True:
    print("\nChecking for completed games...")

    df = pd.read_csv(CSV_PATH)
    df["winner"] = df["winner"].astype("object")
    df["completed"] = df["completed"].astype("bool")  # Ensure correct dtype for 'completed'
    df.reset_index(drop=True, inplace=True)  # Reset index to ensure compatibility

    games = fetch_games_with_backoff()

    if games is None:
        print("⚠️ Skipping update — API unreachable after retry limit.")
        continue

    for idx, row in df.iterrows():
        home = row["home_team"].strip()
        away = row["away_team"].strip()

        # Manual fix first
        manual = apply_manual_winner(home, away)
        if manual:
            df.loc[idx, "winner"] = manual
            df.loc[idx, "completed"] = True
            print(f"✔ MANUAL OVERRIDE: {home} vs {away} → {manual}")
            continue

        # Match game from API
        match = None
        for g in games:
            if not isinstance(g, dict):
                continue

            gh = g.get("homeTeam", "").strip()
            ga = g.get("awayTeam", "").strip()

            if (gh == home and ga == away) or (gh == away and ga == home):
                match = g
                break

        if not match:
            continue

        if not match.get("completed", False):
            continue

        # Determine winner
        home_pts = match.get("homePoints")
        away_pts = match.get("awayPoints")

        if home_pts is None or away_pts is None:
            continue

        if home_pts > away_pts:
            winner = match["homeTeam"]
        else:
            winner = match["awayTeam"]

        if row["winner"] != winner:
            print(f"✔ UPDATED FROM API: {home} vs {away} → {winner}")
            df.loc[idx, "winner"] = winner
            df.loc[idx, "completed"] = True

    df.to_csv(CSV_PATH, index=False)
    print("CSV updated.\n")

    time.sleep(900)  # safe interval

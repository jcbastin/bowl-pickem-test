import os
import csv
import requests
import pandas as pd
from datetime import datetime

API_KEY = os.getenv("CFBD_API_KEY")
HEADERS = {"Authorization": f"Bearer {API_KEY}"}

# Path to your seed CSV (modify if using a different path)
CSV_PATH = "./storage_seed/games.csv"

# --------------------------------------------------------------------------
# Helper: Normalize team names (CFBD sometimes uses abbreviations)
# --------------------------------------------------------------------------
def normalize_team_name(name):
    if not name:
        return ""
    return (
        name.replace("St.", "State")
        .replace("UConn", "Connecticut")
        .replace("Miami (OH)", "Miami (OH)")
        .strip()
    )

# --------------------------------------------------------------------------
# Fetch all postseason games for 2025 and 2026
# --------------------------------------------------------------------------
def fetch_postseason_games():
    all_games = []
    for year in [2025, 2026]:
        url = f"https://api.collegefootballdata.com/games?year={year}&seasonType=postseason"
        resp = requests.get(url, headers=HEADERS)
        if resp.status_code != 200:
            print(f"[WARN] CFBD returned {resp.status_code} for {url}")
            continue

        year_games = resp.json()
        for g in year_games:
            if g.get("id"):
                all_games.append(g)

    print(f"[OK] Retrieved {len(all_games)} postseason games from CFBD")
    return all_games

# --------------------------------------------------------------------------
# Fetch betting lines by matching teams (NOT by CFBD gameId)
# --------------------------------------------------------------------------
def fetch_spread_by_teams(year, home, away):
    url = f"https://api.collegefootballdata.com/lines?year={year}&seasonType=postseason"
    resp = requests.get(url, headers=HEADERS)
    if resp.status_code != 200:
        return None

    data = resp.json()
    if not data:
        return None

    home = home.lower()
    away = away.lower()

    for entry in data:
        for line in entry.get("lines", []):
            h = line.get("homeTeam", "").lower()
            a = line.get("awayTeam", "").lower()

            if h == home and a == away:
                return line.get("spread")

    return None

# --------------------------------------------------------------------------
# Update your CSV rows using CFBD data
# --------------------------------------------------------------------------
def update_games_csv():
    df = pd.read_csv(CSV_PATH)

    # Fix dtype issues
    df["home_team"] = df["home_team"].astype("string")
    df["away_team"] = df["away_team"].astype("string")

    cfbd_games = fetch_postseason_games()
    cfbd_by_id = {g["id"]: g for g in cfbd_games}

    updated_rows = 0
    unmatched_rows = 0

    for idx, row in df.iterrows():
        game_id = row.get("cfbd_game_id")

        if pd.isna(game_id):
            unmatched_rows += 1
            continue

        game_id = int(game_id)

        if game_id not in cfbd_by_id:
            unmatched_rows += 1
            continue

        g = cfbd_by_id[game_id]

        # Extract real fields
        home = normalize_team_name(g.get("home_team"))
        away = normalize_team_name(g.get("away_team"))
        network = g.get("tv")
        venue = g["venue"] if g.get("venue") else None

        # Year for spread lookup
        year = int(str(row["kickoff_datetime"])[:4])

        # Correct spread lookup
        spread = fetch_spread_by_teams(year, home, away)

        # Update CSV row
        df.at[idx, "home_team"] = home
        df.at[idx, "away_team"] = away
        df.at[idx, "network"] = network if network else row["network"]
        df.at[idx, "location"] = venue if venue else row["location"]

        if spread is not None:
            df.at[idx, "spread"] = spread

        updated_rows += 1

    df.to_csv(CSV_PATH, index=False)

    print(f"[DONE] Updated {updated_rows} rows from CFBD.")
    print(f"[INFO] {unmatched_rows} rows did not match any CFBD game_id.")

if __name__ == "__main__":
    update_games_csv()

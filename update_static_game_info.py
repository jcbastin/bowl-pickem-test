import os
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

CFBD_KEY = os.getenv("CFBD_API_KEY")
HEADERS = {"Authorization": f"Bearer " + CFBD_KEY}

# Determine the correct storage path
if os.getenv("RENDER"):
    DISK_DIR = "/opt/render/project/src/storage"
else:
    DISK_DIR = "./storage"

GAMES_CSV = f"{DISK_DIR}/games.csv"

# --------------------------------------------------
# LOAD CSV + FIX COLUMN DTYPES
# --------------------------------------------------
df = pd.read_csv(GAMES_CSV)

# These columns must ALWAYS be strings to avoid dtype issues
string_columns = ["away_record", "home_record", "away_rank", "home_rank", "cfbd_game_id"]
for col in string_columns:
    if col in df.columns:
        df[col] = df[col].astype("object")  # object = string-like


# --------------------------------------------------
# NORMALIZATION HELPER
# --------------------------------------------------
def normalize(name):
    """Normalize team names for matching"""
    return str(name).lower().replace("&", "and").replace(".", "").strip()


# --------------------------------------------------
# STEP 1 — FETCH CFBD WEEK 15 CONFERENCE CHAMPIONSHIP GAMES
# --------------------------------------------------
print("Fetching CFBD Week 15 games...")

resp = requests.get(
    "https://api.collegefootballdata.com/games?year=2025&seasonType=regular&week=15",
    headers=HEADERS
)

week15_games = resp.json()
print(f"Found {len(week15_games)} Week 15 CFBD games.")

# Build lookup table of normalized teams
cfbd_lookup = []
for g in week15_games:
    cfbd_lookup.append({
        "id": g["id"],
        "home": normalize(g["homeTeam"]),
        "away": normalize(g["awayTeam"]),
        "notes": g.get("notes", ""),
        "date": g.get("startDate", "")[:10]
    })


# --------------------------------------------------
# STEP 2 — FETCH TEAM RECORDS
# --------------------------------------------------
print("Fetching team records...")

records_resp = requests.get(
    "https://api.collegefootballdata.com/records?year=2025",
    headers=HEADERS
)
team_records = records_resp.json()

record_map = {}
for team in team_records:
    name = normalize(team["team"])
    wins = team["total"]["wins"]
    losses = team["total"]["losses"]
    record_map[name] = f"{wins}-{losses}"


# --------------------------------------------------
# STEP 3 — FETCH TEAM RANKINGS
# --------------------------------------------------
print("Fetching team rankings...")

rank_resp = requests.get(
    "https://api.collegefootballdata.com/rankings?year=2025",
    headers=HEADERS
)
rankings = rank_resp.json()

rank_map = {}
if rankings and rankings[-1]["polls"]:
    # Use most recent poll available
    latest_poll = rankings[-1]["polls"][0]["ranks"]
    for entry in latest_poll:
        rank_map[normalize(entry["school"])] = str(entry["rank"])  # force string


# --------------------------------------------------
# STEP 4 — MATCH CSV GAMES TO CFBD WEEK 15 GAMES
# --------------------------------------------------
print("Matching CSV games to CFBD Week 15 games...")

matched = 0

for idx, row in df.iterrows():
    home = normalize(row["home_team"])
    away = normalize(row["away_team"])

    # Match based on normalized home + away names
    match = next(
        (
            g for g in cfbd_lookup
            if g["home"] == home and g["away"] == away
        ),
        None
    )

    # Fill CFBD game ID if found
    if match:
        df.at[idx, "cfbd_game_id"] = str(match["id"])  # ensure string
        matched += 1
    else:
        print(f"⚠️ No match found for: {row['away_team']} vs {row['home_team']}")

    # Populate record data
    df.at[idx, "away_record"] = record_map.get(away, "")
    df.at[idx, "home_record"] = record_map.get(home, "")

    # Populate ranking data
    df.at[idx, "away_rank"] = rank_map.get(away, "")
    df.at[idx, "home_rank"] = rank_map.get(home, "")

print(f"\nMatched {matched} of {len(df)} games.")


# --------------------------------------------------
# SAVE UPDATED CSV
# --------------------------------------------------
df.to_csv(GAMES_CSV, index=False)
print(f"🎉 update_static_game_info.py completed. Saved to {GAMES_CSV}")

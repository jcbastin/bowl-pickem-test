import os
import pandas as pd
import requests
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

CFBD_API_KEY = os.getenv("CFBD_API_KEY")

YEAR = 2025
WEEK = 14
SEASON_TYPE = "regular"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEST_GAMES_OUT = os.path.join(BASE_DIR, "data", "test_games.csv")

HEADERS = {
    "Authorization": f"Bearer {CFBD_API_KEY}",
    "Accept": "application/json",
}

# ---------------------------------------------------------
# 1. The exact 15 rivalry/crossover games YOU selected
# ---------------------------------------------------------
USER_GAMES = {
    frozenset(["Clemson", "South Carolina"]): 1,
    frozenset(["Iowa", "Nebraska"]): 1,
    frozenset(["UNLV", "Nevada"]): 1,
    frozenset(["UCLA", "USC"]): 1,
    frozenset(["Indiana", "Purdue"]): 1,

    frozenset(["LSU", "Oklahoma"]): 2,
    frozenset(["Arizona", "Arizona State"]): 2,
    frozenset(["Ole Miss", "Mississippi State"]): 2,
    frozenset(["Kentucky", "Louisville"]): 2,

    frozenset(["Georgia", "Georgia Tech"]): 3,
    frozenset(["Vanderbilt", "Tennessee"]): 3,
    frozenset(["Oregon", "Washington"]): 3,

    frozenset(["Texas", "Texas A&M"]): 4,
    frozenset(["Auburn", "Alabama"]): 4,

    frozenset(["Michigan", "Ohio State"]): 5,
}

# ---------------------------------------------------------
# 2. Pull CFBD Week 14 (2025)
# ---------------------------------------------------------
def fetch_cfbd_games():
    url = "https://api.collegefootballdata.com/games"
    params = {
        "year": YEAR,
        "week": WEEK,
        "seasonType": SEASON_TYPE,
    }
    print("Fetching 2025 Week 14 games from CFBD...")
    resp = requests.get(url, headers=HEADERS, params=params)
    resp.raise_for_status()
    return resp.json()

# ---------------------------------------------------------
# 3. Build Test 3 test_games.csv
# ---------------------------------------------------------
def main():
    if not CFBD_API_KEY:
        raise RuntimeError("CFBD_API_KEY missing in .env")

    cfbd_games = fetch_cfbd_games()

    rows = []
    game_id = 1

    for g in cfbd_games:

        cfbd_home = g.get("homeTeam")
        cfbd_away = g.get("awayTeam")

        if not cfbd_home or not cfbd_away:
            continue

        # Build teamset for matching
        game_teamset = frozenset([cfbd_home, cfbd_away])

        # Only include games YOU specified
        if game_teamset not in USER_GAMES:
            continue

        pick_value = USER_GAMES[game_teamset]
        cfbd_id = g.get("id")

        # Kickoff datetime clean formatting
        start_raw = g.get("startDate")
        try:
            clean = start_raw.replace("Z", "").split(".")[0]
            kickoff_dt = datetime.fromisoformat(clean)
            kickoff_str = kickoff_dt.strftime("%Y-%m-%d %H:%M:%S")
        except:
            kickoff_str = ""

        # Add row
        rows.append({
            "game_id": game_id,
            "bowl_name": f"Test Game {game_id}",
            "kickoff_datetime": kickoff_str,
            "point_value": pick_value,
            "away_team": cfbd_away,
            "home_team": cfbd_home,
            "away_record": "",
            "home_record": "",
            "spread": "",
            "status": "NOT_STARTED",
            "winner": "",
            "completed": False,
            "away_score": "",
            "home_score": "",
            "cfbd_game_id": cfbd_id,
        })

        print(f"MATCHED: Test Game {game_id} → {cfbd_away} @ {cfbd_home} (CFBD ID {cfbd_id})")
        game_id += 1

    # Write CSV
    df = pd.DataFrame(rows)
    df.to_csv(TEST_GAMES_OUT, index=False)

    print("\n✓ Test 3 test_games.csv created successfully!")
    print(f"Saved to: {TEST_GAMES_OUT}")


if __name__ == "__main__":
    main()

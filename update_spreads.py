import os
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

CFBD_KEY = os.getenv("CFBD_API_KEY")
HEADERS = {"Authorization": f"Bearer {CFBD_KEY}"}

YEAR = 2025
WEEK = 15     # Conference Championship Week for your test
SEASON_TYPE = "regular"

# Paths
if os.getenv("RENDER"):
    DISK_DIR = "/opt/render/project/src/storage"
else:
    DISK_DIR = "./storage"

GAMES_CSV = f"{DISK_DIR}/games.csv"


# ----------------------------------------------
# Fetch spreads from CFBD using cfbd_game_id
# ----------------------------------------------
def fetch_spread_for_game(game):
    """
    Extract the best spread from DraftKings > Bovada > ESPN Bet.
    Returns "home spread" (positive if home is the favorite).
    """

    lines = game.get("lines", [])
    if not lines:
        return None

    # Preferred provider order
    providers = ["DraftKings", "Bovada", "ESPN Bet"]

    for provider in providers:
        for book in lines:
            if book.get("provider") == provider and book.get("spread") is not None:
                return float(book["spread"])

    # If no provider had a spread
    return None


# ----------------------------------------------
# Pull spreads for the entire week
# ----------------------------------------------
def fetch_all_spreads():
    url = (
        f"https://api.collegefootballdata.com/lines"
        f"?year={YEAR}&week={WEEK}&seasonType={SEASON_TYPE}"
    )

    print(f"Fetching spreads from: {url}")
    resp = requests.get(url, headers=HEADERS)

    try:
        data = resp.json()
    except Exception:
        print("❌ ERROR: CFBD did not return JSON")
        print(resp.text)
        return pd.DataFrame()

    if not data:
        print("⚠️ No spread data returned from CFBD.")
        return pd.DataFrame()

    results = []
    for g in data:
        spread = fetch_spread_for_game(g)
        results.append({
            "cfbd_game_id": g["id"],
            "spread": spread
        })

    df = pd.DataFrame(results)
    print(f"✅ Retrieved spreads for {len(df)} games.")
    return df


# ----------------------------------------------
# Update local games.csv with spread values
# ----------------------------------------------
def update_spreads():
    print(f"📂 Loading games from {GAMES_CSV}")
    games = pd.read_csv(GAMES_CSV)

    spreads = fetch_all_spreads()
    if spreads.empty:
        print("⚠️ No spreads pulled. Leaving CSV unchanged.")
        return

    # Make sure cfbd_game_id matches dtype
    games["cfbd_game_id"] = games["cfbd_game_id"].astype(int)
    spreads["cfbd_game_id"] = spreads["cfbd_game_id"].astype(int)

    # Merge spreads directly using cfbd_game_id
    merged = games.merge(spreads, on="cfbd_game_id", how="left")

    # Overwrite spread column
    merged["spread"] = merged["spread_y"].fillna(merged["spread_x"])
    merged = merged.drop(columns=["spread_x", "spread_y"])

    # Save updated games.csv
    merged.to_csv(GAMES_CSV, index=False)
    print(f"✅ Spreads updated successfully! Saved to {GAMES_CSV}")


# ----------------------------------------------
# MAIN
# ----------------------------------------------
if __name__ == "__main__":
    update_spreads()

import pandas as pd
import requests
import os

# Load API key from environment
API_KEY = os.getenv("CFBD_API_KEY")
HEADERS = {"Authorization": f"Bearer {API_KEY}"}

# Render disk directory
DEFAULT_DISK_DIR = "/opt/render/project/src/storage"
DISK_DIR = os.getenv("DISK_DIR", DEFAULT_DISK_DIR)

# Path to CSV
CSV_PATH = os.path.join(DISK_DIR, "games.csv")   # <-- now using main games.csv

def fetch_all_games_for_week():
    """
    Fetch all CFBD games for Week 15 (conference championships) in 2025.
    We will match strictly by CFBD game ID.
    """
    try:
        resp = requests.get(
            "https://api.collegefootballdata.com/games",
            params={"year": 2025, "seasonType": "regular", "week": 15},
            headers=HEADERS,
            timeout=10
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"⚠️ Error contacting CFBD API: {e}")
        return None


def main():
    print("🔄 Running update_winners_live...")

    # Load existing CSV
    try:
        df = pd.read_csv(CSV_PATH)
    except Exception as e:
        print(f"❌ Failed to read CSV: {CSV_PATH} → {e}")
        return

    df["winner"] = df["winner"].astype("object")
    df["completed"] = df["completed"].astype("bool")

    # Fetch CFBD week 15 games
    games = fetch_all_games_for_week()
    if not games:
        print("⚠️ No API data returned.")
        return

    # Build a lookup dict by cfbd_game_id for fast matching
    game_lookup = {g["id"]: g for g in games}

    updated_any = False

    for idx, row in df.iterrows():
        cfbd_id = row.get("cfbd_game_id")

        if pd.isna(cfbd_id):
            print(f"⚠️ Row {idx} has no cfbd_game_id — skipping.")
            continue

        cfbd_id = int(cfbd_id)

        match = game_lookup.get(cfbd_id)
        if not match:
            print(f"⚠️ CFBD game not found for ID {cfbd_id}")
            continue

        # Check completion
        if not match.get("completed", False):
            continue

        home = match["homeTeam"]
        away = match["awayTeam"]
        home_pts = match.get("homePoints")
        away_pts = match.get("awayPoints")

        # Safety fallback
        if home_pts is None or away_pts is None:
            continue

        winner = home if home_pts > away_pts else away

        # Update CSV only if changed
        if df.loc[idx, "winner"] != winner:
            print(f"✔ UPDATED: {away} vs {home} → {winner}")

            df.loc[idx, "winner"] = winner
            df.loc[idx, "completed"] = True
            df.loc[idx, "away_score"] = away_pts
            df.loc[idx, "home_score"] = home_pts

            updated_any = True

    # Save updates
    if updated_any:
        try:
            df.to_csv(CSV_PATH, index=False)
            print("💾 CSV updated successfully.")
        except Exception as e:
            print(f"❌ Failed to save CSV: {e}")
    else:
        print("ℹ️ No updates needed.")

    print("✅ update_winners_live completed.")


if __name__ == "__main__":
    main()

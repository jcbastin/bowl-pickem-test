import pandas as pd
import requests
import os

# Load API key from Render environment variable
API_KEY = os.getenv("CFBD_API_KEY")
HEADERS = {"Authorization": f"Bearer {API_KEY}"}

# Shared directories for Render
CSV_DIR = "/opt/render/project/src/data"
DISK_DIR = "/opt/render/project/src/storage"

# Path to your CSV (on Render this will be on the persistent disk)
CSV_PATH = f"{DISK_DIR}/test_games.csv"

def fetch_games():
    """Fetch games for 2025 Week 14 (regular season) with retries removed for cron simplicity."""
    try:
        resp = requests.get(
            "https://api.collegefootballdata.com/games",
            params={"year": 2025, "seasonType": "regular", "week": 14},
            headers=HEADERS,
            timeout=10
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print("⚠️ Error contacting API:", e)
        return None


def main():
    print("🔄 Running update_winners_live cron job...")

    # Load CSV from the persistent disk
    try:
        df = pd.read_csv(CSV_PATH)
    except Exception as e:
        print(f"❌ Failed to read CSV at {CSV_PATH}: {e}")
        return

    df["winner"] = df["winner"].astype("object")
    df["completed"] = df["completed"].astype("bool")

    games = fetch_games()
    if not games:
        print("⚠️ No API data returned. Exiting.")
        return

    updated_any = False

    for idx, row in df.iterrows():
        home = row["home_team"].strip()
        away = row["away_team"].strip()

        # Find matching game from CFBD response
        match = None
        for g in games:
            if (
                g.get("homeTeam", "").strip() == home and
                g.get("awayTeam", "").strip() == away
            ):
                match = g
                break

        if not match:
            continue

        # Skip if not completed
        if not match.get("completed", False):
            continue

        home_pts = match.get("homePoints")
        away_pts = match.get("awayPoints")

        if home_pts is None or away_pts is None:
            continue

        # Determine winner
        winner = match["homeTeam"] if home_pts > away_pts else match["awayTeam"]

        if df.loc[idx, "winner"] != winner:
            print(f"✔ UPDATED: {away} @ {home} → {winner}")
            df.loc[idx, "winner"] = winner
            df.loc[idx, "completed"] = True
            updated_any = True

    # Save only if something changed
    if updated_any:
        try:
            df.to_csv(f"{DISK_DIR}/test_games.csv", index=False)
            print("💾 CSV updated successfully.")
        except Exception as e:
            print(f"❌ Failed to write updated CSV: {e}")
    else:
        print("ℹ️ No updates needed.")

    print("✅ Cron job completed.")


if __name__ == "__main__":
    main()

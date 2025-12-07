import pandas as pd
import requests
import os

# Load API key
API_KEY = os.getenv("CFBD_API_KEY")
HEADERS = {"Authorization": f"Bearer {API_KEY}"}

# Correct Render storage path
CSV_PATH = "/opt/render/project/src/storage/games.csv"


def fetch_postseason_games():
    """
    Fetch ALL postseason games for 2025.
    CFBD assigns postseason games a seasonType of 'postseason'
    and spreads them across multiple 'weeks'.
    """
    try:
        resp = requests.get(
            "https://api.collegefootballdata.com/games",
            params={"year": 2025, "seasonType": "postseason"},
            headers=HEADERS,
            timeout=10
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"⚠️ Error contacting CFBD API: {e}")
        return None


def main():
    print("🔄 Running update_winners_live for POSTSEASON 2025...")

    # Load games.csv
    try:
        df = pd.read_csv(CSV_PATH)
    except Exception as e:
        print(f"❌ Failed to read CSV: {CSV_PATH} → {e}")
        return

    # Ensure correct types
    df["winner"] = df["winner"].astype("object")
    df["completed"] = df["completed"].astype(bool)

    # Fetch all postseason results
    games = fetch_postseason_games()
    if not games:
        print("⚠️ No API data returned.")
        return

    # Lookup by CFBD ID
    cfbd_lookup = {g["id"]: g for g in games}

    updated_any = False

    for idx, row in df.iterrows():
        cfbd_id = row.get("cfbd_game_id")

        if pd.isna(cfbd_id):
            print(f"⚠️ Row {idx} has NO cfbd_game_id — skipping.")
            continue

        cfbd_id = int(cfbd_id)

        match = cfbd_lookup.get(cfbd_id)
        if match is None:
            print(f"⚠️ CFBD game NOT FOUND for ID {cfbd_id}")
            continue

        # Skip unplayed games
        if not match.get("completed", False):
            continue

        # Extract data
        home = match["homeTeam"]
        away = match["awayTeam"]
        home_pts = match.get("homePoints")
        away_pts = match.get("awayPoints")

        if home_pts is None or away_pts is None:
            continue  # not complete

        # Determine winner
        winner = home if home_pts > away_pts else away

        # Update only if changed
        if (
            df.loc[idx, "winner"] != winner
            or df.loc[idx, "completed"] is False
            or df.loc[idx, "home_score"] != home_pts
            or df.loc[idx, "away_score"] != away_pts
        ):
            print(f"✔ UPDATED: {away} vs {home} → {winner}")

            df.loc[idx, "winner"] = winner
            df.loc[idx, "completed"] = True
            df.loc[idx, "home_score"] = home_pts
            df.loc[idx, "away_score"] = away_pts

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

    print("✅ update_winners_live POSTSEASON completed.")


if __name__ == "__main__":
    main()

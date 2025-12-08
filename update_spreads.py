import os
import requests
import pandas as pd

# ---- Paths ----
CSV_PATH = "/opt/render/project/src/storage/games.csv"

# ---- CFBD Key ----
CFBD_KEY = os.getenv("CFBD_API_KEY")
HEADERS = {"Authorization": f"Bearer {CFBD_KEY}"}

# ---- Provider Priority ----
PROVIDER_PRIORITY = ["DraftKings", "Bovada"]


def choose_spread(lines):
    """
    Select spread using priority (DraftKings -> Bovada -> first available).
    Each 'line' item is a dict with keys: provider, spread, formattedSpread, etc.
    """
    # Try priority list first
    for provider in PROVIDER_PRIORITY:
        for item in lines:
            if item.get("provider") == provider and item.get("spread") is not None:
                return item["spread"]

    # Otherwise choose first available spread
    for item in lines:
        if item.get("spread") is not None:
            return item["spread"]

    return None


def update_spreads():
    """
    Fetch CFBD lines for each game in games.csv and update the 'spread' column.
    This function is designed so Flask can import it cleanly.
    """
    if CFBD_KEY is None:
        raise RuntimeError("CFBD_API_KEY is missing in environment.")

    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV not found at {CSV_PATH}")

    df = pd.read_csv(CSV_PATH)

    updated_count = 0

    for idx, row in df.iterrows():
        game_id = row.get("cfbd_game_id")

        # Skip if no CFBD ID
        if pd.isna(game_id):
            print(f"Skipping row {idx}: No CFBD ID")
            continue

        game_id = int(game_id)
        url = f"https://api.collegefootballdata.com/lines?gameId={game_id}"

        try:
            response = requests.get(url, headers=HEADERS, timeout=10)
        except Exception as e:
            print(f"[{game_id}] Request error: {e}")
            continue

        if response.status_code != 200:
            print(f"[{game_id}] CFBD ERROR {response.status_code}: {response.text[:200]}")
            continue

        data = response.json()

        if not data:
            print(f"[{game_id}] No data returned")
            continue

        game_obj = data[0]

        if "lines" not in game_obj or not game_obj["lines"]:
            print(f"[{game_id}] No lines available")
            continue

        lines = game_obj["lines"]

        spread = choose_spread(lines)

        print(f"{row['away_team']} vs {row['home_team']} -> spread chosen: {spread}")

        df.at[idx, "spread"] = spread
        updated_count += 1

    # Write back to CSV
    df.to_csv(CSV_PATH, index=False)
    print(f"Completed spread update for {updated_count} games.")

    return {"updated": updated_count}


# Allow running manually from command line
if __name__ == "__main__":
    update_spreads()

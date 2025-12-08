import os
import csv
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("CFBD_API_KEY")
HEADERS = {"Authorization": f"Bearer {API_KEY}"}

# Render STORAGE directory
DISK_DIR = "/opt/render/project/src/storage"
CSV_PATH = os.path.join(DISK_DIR, "games.csv")

def fetch_postseason_lines():
    url = "https://api.collegefootballdata.com/lines?year=2025&seasonType=postseason"
    resp = requests.get(url, headers=HEADERS)

    if resp.status_code != 200:
        print(f"[ERROR] Failed to fetch spreads. Status {resp.status_code}")
        return []

    return resp.json()


def extract_best_spread(lines_entry):
    if "lines" not in lines_entry:
        return None

    dk = next((l for l in lines_entry["lines"] if l.get("provider") == "DraftKings" and l.get("spread") is not None), None)
    if dk:
        return dk["spread"]

    bov = next((l for l in lines_entry["lines"] if l.get("provider") == "Bovada" and l.get("spread") is not None), None)
    if bov:
        return bov["spread"]

    return None


def load_games():
    return pd.read_csv(CSV_PATH)


def update_spreads():
    print("[INFO] Fetching postseason lines...")
    lines_data = fetch_postseason_lines()

    if not lines_data:
        print("[WARN] No lines returned. Exiting.")
        return

    # --- DEBUG: PRINT RAW SPREADS RETURNED ---
    print("\n[DEBUG] Raw spreads returned from CFBD:")
    for entry in lines_data:
        gid = entry.get("id")
        spread = extract_best_spread(entry)
        print(f"  game_id={gid}, spread={spread}")
    print("[DEBUG] End raw spread dump\n")

    df = load_games()

    if "cfbd_game_id" not in df.columns:
        print("[ERROR] cfbd_game_id column missing from games.csv")
        return

    updated_count = 0
    df["cfbd_game_id"] = pd.to_numeric(df["cfbd_game_id"], errors="coerce")

    spread_map = {}
    for entry in lines_data:
        game_id = entry.get("id")
        if game_id:
            best_spread = extract_best_spread(entry)
            if best_spread is not None:
                spread_map[game_id] = best_spread

    print(f"[INFO] Found {len(spread_map)} spreads.")

    for idx, row in df.iterrows():
        gid = row["cfbd_game_id"]
        if gid in spread_map:
            df.at[idx, "spread"] = spread_map[gid]
            updated_count += 1
            print(f"[UPDATE] Game {gid}: spread → {spread_map[gid]}")

    # --- DEBUG: PRINT DF BEFORE SAVING ---
    print("\n[DEBUG] DataFrame spreads before saving:")
    print(df[["cfbd_game_id", "spread"]].head(20))
    print("[DEBUG] End DataFrame dump\n")

    # Remove unnamed junk columns
    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]

    # Ensure spread column exists
    if "spread" not in df.columns:
        df["spread"] = None

    # Force spread to be the last column
    cols = [c for c in df.columns if c != "spread"] + ["spread"]
    df = df[cols]

    print("\n[DEBUG] Final columns:", df.columns.tolist())

    # Save clean CSV
    df.to_csv(CSV_PATH, index=False)


    print(f"[DONE] Updated {updated_count} spreads.")
    print(f"[DONE] Saved to {CSV_PATH}")


if __name__ == "__main__":
    update_spreads()

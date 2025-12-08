import csv
import requests
from datetime import datetime, timedelta
import os

API_KEY = os.getenv("CFBD_API_KEY")
HEADERS = {"Authorization": f"Bearer {API_KEY}"}

CSV_PATH = "/opt/render/project/src/storage/games.csv"

# Allowable time difference when matching kickoff times
TIME_TOLERANCE = timedelta(minutes=10)


def parse_csv_datetime(dt_str):
    # CSV uses "2025-12-31 19:30:00"
    return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")


def parse_cfbd_datetime(dt_str):
    # CFBD uses ISO format "2025-12-31T19:30:00.000Z"
    return datetime.fromisoformat(dt_str.replace("Z", "+00:00")).replace(tzinfo=None)


def fetch_postseason_games():
    url = "https://api.collegefootballdata.com/games?year=2025&seasonType=postseason"
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    return resp.json()


def fetch_postseason_lines():
    url = "https://api.collegefootballdata.com/lines?year=2025&seasonType=postseason"
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    return resp.json()


def build_lines_lookup(lines_data):
    """Return a dict: cfbd_game_id → spread"""
    lookup = {}
    for game in lines_data:
        game_id = game.get("id")
        if not game_id:
            continue
        if not game.get("lines"):
            continue

        provider_line = game["lines"][0]  # typically DraftKings / default provider
        lookup[game_id] = provider_line.get("spread")

    return lookup


def match_game_by_time(csv_kickoff, cfbd_start):
    delta = abs(csv_kickoff - cfbd_start)
    return delta <= TIME_TOLERANCE


def main():
    print("Fetching CFBD postseason games...")
    cfbd_games = fetch_postseason_games()

    print("Fetching CFBD postseason lines...")
    cfbd_lines = fetch_postseason_lines()
    lines_lookup = build_lines_lookup(cfbd_lines)

    # Build list of CFBD games with valid data
    cfbd_candidates = []
    for g in cfbd_games:
        if g.get("id") and g.get("homeTeam") and g.get("awayTeam"):
            try:
                start_dt = parse_cfbd_datetime(g["startDate"])
                cfbd_candidates.append({
                    "id": g["id"],
                    "home": g["homeTeam"],
                    "away": g["awayTeam"],
                    "start": start_dt,
                })
            except:
                pass

    print(f"Found {len(cfbd_candidates)} real CFBD games.")

    # Read CSV
    rows = []
    with open(CSV_PATH, "r", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    updated = False
    print("Matching CFP + missing games...")

    for row in rows:
        if row["cfbd_game_id"].strip() != "":
            continue  # already filled in

        try:
            csv_time = parse_csv_datetime(row["kickoff_datetime"])
        except:
            continue

        # Attempt match based on time
        for cfbd in cfbd_candidates:
            if match_game_by_time(csv_time, cfbd["start"]):
                row["cfbd_game_id"] = str(cfbd["id"])
                print(f"Matched: game_id {row['game_id']} → {cfbd['id']}")
                updated = True
                break

        # Update spread if available
        if row["cfbd_game_id"] in lines_lookup:
            row["spread"] = lines_lookup[row["cfbd_game_id"]]
            updated = True

    # Write updated CSV
    if updated:
        print("Writing updated CSV...")
        with open(CSV_PATH, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        print("Update complete.")
    else:
        print("No updates needed.")


if __name__ == "__main__":
    main()

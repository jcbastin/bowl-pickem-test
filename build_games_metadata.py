import requests
import csv
import os

API_KEY = os.getenv("CFBD_API_KEY")
HEADERS = {"Authorization": f"Bearer {API_KEY}"}

OUTPUT_FILE = "cfbd_postseason_2025_ids.csv"

def fetch_postseason_games():
    url = (
        "https://api.collegefootballdata.com/games?"
        "year=2025&seasonType=postseason"
    )
    response = requests.get(url, headers=HEADERS)

    if response.status_code != 200:
        raise Exception(f"CFBD API Error {response.status_code}: {response.text}")

    return response.json()

def write_to_csv(games):
    fieldnames = [
        "cfbd_game_id",
        "season",
        "seasonType",
        "week",
        "startDate",
        "homeTeam",
        "awayTeam",
        "homeConference",
        "awayConference",
        "homePoints",
        "awayPoints",
        "venue",
        "neutralSite",
        "conferenceGame"
    ]

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for g in games:
            writer.writerow({
                "cfbd_game_id": g.get("id"),
                "season": g.get("season"),
                "seasonType": g.get("seasonType"),
                "week": g.get("week"),
                "startDate": g.get("startDate"),
                "homeTeam": g.get("homeTeam"),
                "awayTeam": g.get("awayTeam"),
                "homeConference": g.get("homeConference"),
                "awayConference": g.get("awayConference"),
                "homePoints": g.get("homePoints"),
                "awayPoints": g.get("awayPoints"),
                "venue": g.get("venue"),
                "neutralSite": g.get("neutralSite"),
                "conferenceGame": g.get("conferenceGame"),
            })

def main():
    print("Fetching 2025 postseason games from CFBD…")
    games = fetch_postseason_games()
    print(f"Retrieved {len(games)} games.")

    print(f"Writing results to {OUTPUT_FILE}…")
    write_to_csv(games)

    print("Done. Open the CSV and spot-check the IDs and teams.")

if __name__ == "__main__":
    main()

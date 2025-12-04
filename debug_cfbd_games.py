import os
import requests
from dotenv import load_dotenv

load_dotenv()

CFBD_KEY = os.getenv("CFBD_API_KEY")
HEADERS = {"Authorization": f"Bearer {CFBD_KEY}"}

print("\n=== Testing CFBD games endpoint ===\n")

# Try postseason first
url = "https://api.collegefootballdata.com/games?year=2025&seasonType=postseason"
resp = requests.get(url, headers=HEADERS)
print("POSTSEASON STATUS:", resp.status_code)
print("POSTSEASON COUNT:", len(resp.json()))
print("POSTSEASON SAMPLE:", resp.json()[:3], "\n")

# Try regular season
url = "https://api.collegefootballdata.com/games?year=2025&seasonType=regular"
resp = requests.get(url, headers=HEADERS)
print("REGULAR STATUS:", resp.status_code)
print("REGULAR COUNT:", len(resp.json()))
print("REGULAR SAMPLE:", resp.json()[:3], "\n")

# Try SEASONTYPE=both
url = "https://api.collegefootballdata.com/games?year=2025&seasonType=both"
resp = requests.get(url, headers=HEADERS)
print("BOTH STATUS:", resp.status_code)
print("BOTH COUNT:", len(resp.json()))
print("BOTH SAMPLE:", resp.json()[:3], "\n")

# Try filtering by week (Championship week = Week 14 or 15)
for week in range(13, 18):
    url = f"https://api.collegefootballdata.com/games?year=2025&week={week}"
    resp = requests.get(url, headers=HEADERS)
    games = resp.json()
    print(f"WEEK {week}: {len(games)} games")
    if games:
        print("  SAMPLE:", games[:2])
    print()

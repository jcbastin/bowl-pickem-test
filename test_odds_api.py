import requests
import json
from datetime import datetime

API_KEY = "571d3d0514d6d69830a8b9530f9d0922"

url = "https://api.the-odds-api.com/v4/sports/americanfootball_ncaaf/odds/"

params = {
    "regions": "us",                   # US sportsbooks
    "markets": "spreads",              # only spreads
    "oddsFormat": "decimal",
    "dateFormat": "iso",
    "apiKey": API_KEY
}

print("Fetching odds...")
response = requests.get(url, params=params)

print("Status Code:", response.status_code)

if response.status_code != 200:
    print("Error:", response.text)
    exit()

data = response.json()

print("\nNumber of games returned:", len(data))
print("\n=== Raw JSON sample (first game) ===\n")
print(json.dumps(data[0], indent=2))

# Optional: save full data for inspection
with open("odds_output.json", "w") as f:
    json.dump(data, f, indent=2)

print("\nSaved full output to odds_output.json")

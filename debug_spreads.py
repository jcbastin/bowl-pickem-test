import os
import requests
from dotenv import load_dotenv
load_dotenv()

API_KEY = os.getenv("CFBD_API_KEY")

url = "https://api.collegefootballdata.com/lines?year=2025&week=11"
headers = {"Authorization": f"Bearer " + API_KEY}

resp = requests.get(url, headers=headers)
print("Status:", resp.status_code)

data = resp.json()
print("Number of games:", len(data))

# Print the first game's structure
import json
print(json.dumps(data[0], indent=2))

import os
import requests
import pandas as pd
from dotenv import load_dotenv

# Load API key
load_dotenv()
API_KEY = os.getenv("CFBD_API_KEY")

YEAR = 2025  # adjust if needed

def fetch_team_records():
    """Fetch team win/loss records from the CollegeFootballData API."""
    url = f"https://api.collegefootballdata.com/records?year={YEAR}"
    headers = {"Authorization": f"Bearer {API_KEY}"}

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f"❌ Failed to fetch team records: {response.status_code}")
        print(response.text)
        return

    data = response.json()
    df = pd.json_normalize(data)

    # Extract needed columns
    df = df[[
        "team",
        "total.wins",
        "total.losses",
        "total.ties"
    ]]

    # Rename for clarity
    df.rename(columns={
        "total.wins": "wins",
        "total.losses": "losses",
        "total.ties": "ties"
    }, inplace=True)

    # Build W-L record string
    df["record"] = df["wins"].astype(str) + "-" + df["losses"].astype(str)

    # Save file
    df.to_csv("data/team_records.csv", index=False)
    print("✅ Team records updated successfully!")


if __name__ == "__main__":
    fetch_team_records()

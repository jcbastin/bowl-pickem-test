import os
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("CFBD_API_KEY")

YEAR = 2025
WEEK = 14   # <-- Week for Test 3

# ---------- NAME CLEANING ----------
def clean_name(name: str) -> str:
    """
    Normalize team names so matching works even when formatting differs.
    """
    return (
        str(name)
        .lower()
        .replace("&", "and")
        .replace("state", "st")
        .replace(" ", "")
        .replace(".", "")
    )


# ---------- FETCH SPREADS FROM CFBD ----------
def fetch_spreads():
    """Fetch betting lines from CFBD API."""
    url = f"https://api.collegefootballdata.com/lines?year={YEAR}&week={WEEK}"
    headers = {"Authorization": f"Bearer {API_KEY}"}

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"❌ Failed to fetch spreads ({response.status_code})")
        print(response.text)
        return pd.DataFrame()

    data = response.json()
    rows = []

    for game in data:
        home = game.get("homeTeam")
        away = game.get("awayTeam")

        # Find *FIRST* sportsbook with a spread value
        spread_value = None
        for book in game.get("lines", []):
            if book.get("spread") is not None:
                spread_value = book["spread"]
                break

        # Skip if no sportsbook has a spread
        if spread_value is None:
            continue

        rows.append({
            "home_team": home,
            "away_team": away,
            "spread": float(spread_value)
        })

    if not rows:
        print("⚠️ No spreads found in API response.")
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    print(f"✅ Retrieved {len(df)} spread entries from API")
    return df


# ---------- MATCH SPREADS TO LOCAL TEST GAMES ----------
def update_local_spreads():
    games = pd.read_csv("data/test_games.csv")
    spreads = fetch_spreads()

    if spreads.empty:
        print("⚠️ No spreads retrieved. CSV unchanged.")
        return

    # Prepare cleaned names for matching
    games["home_clean"] = games["home_team"].apply(clean_name)
    games["away_clean"] = games["away_team"].apply(clean_name)

    spreads["home_clean"] = spreads["home_team"].apply(clean_name)
    spreads["away_clean"] = spreads["away_team"].apply(clean_name)

    results = []

    for _, g in games.iterrows():
        gid = g["game_id"]
        gh = g["home_clean"]
        ga = g["away_clean"]

        # --- Direct home/away match ---
        direct = spreads[
            (spreads["home_clean"] == gh) &
            (spreads["away_clean"] == ga)
        ]

        # --- Reversed match (invert spread) ---
        reversed_match = spreads[
            (spreads["home_clean"] == ga) &
            (spreads["away_clean"] == gh)
        ]

        if not direct.empty:
            sp = direct.iloc[0]["spread"]
            results.append((gid, sp))

        elif not reversed_match.empty:
            sp = -reversed_match.iloc[0]["spread"]
            results.append((gid, sp))

        else:
            # No spread matched for this game
            results.append((gid, None))

    df_out = pd.DataFrame(results, columns=["game_id", "spread"])
    df_out.to_csv("data/spreads.csv", index=False)

    print("✅ Spreads updated successfully!")


# ---------- MAIN ----------
if __name__ == "__main__":
    update_local_spreads()

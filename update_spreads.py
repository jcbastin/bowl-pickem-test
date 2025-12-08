import os
import re
import pandas as pd
import requests

# ---------------------------------------------------------
# ENV + CONFIG
# ---------------------------------------------------------

API_KEY = os.getenv("ODDS_API_KEY")  # must be set in Render
ODDS_URL = "https://api.the-odds-api.com/v4/sports/americanfootball_ncaaf/odds/"

DISK_DIR = os.getenv("DISK_DIR", "/opt/render/project/src/storage")
CSV_PATH = os.path.join(DISK_DIR, "games.csv")

# ---------------------------------------------------------
# TEAM NAME NORMALIZATION
# ---------------------------------------------------------

def school_key(name: str) -> str:
    """
    Extracts only the school name portion of a team identity by removing mascots.
    Safe for fuzzy matching pairwise (avoids Arizona vs Arizona State, etc.)
    """
    if not name:
        return ""
    
    # lowercase
    n = name.lower()

    # normalize punctuation
    n = n.replace("&", "and")
    n = re.sub(r"[^a-z0-9\s]", "", n)

    tokens = n.split()

    mascots = {
        "aggies","broncos","buckeyes","tigers","wildcats","panthers","knights",
        "huskies","spartans","trojans","mountaineers","cougars","cardinal",
        "bulldogs","jayhawks","rebels","volunteers","gators","ducks","bruins",
        "longhorns","seminoles","hurricanes","blazers","sun","devils","devil",
        "gamecocks","pirates","cowboys","raiders","wolfpack","csv","crusaders",
        "paladins","hog","horned","frogs","patriots","wolverines"
    }

    # keep only non-mascot tokens
    filtered = [t for t in tokens if t not in mascots]

    return " ".join(filtered).strip()


def pair_matches(csv_home, csv_away, api_home, api_away):
    """
    Determines if the CSV home/away pair matches the Odds API home/away pair.
    This prevents incorrect fuzzy matches (e.g., Arizona vs Arizona State).
    """
    ch = school_key(csv_home)
    ca = school_key(csv_away)
    ah = school_key(api_home)
    aa = school_key(api_away)

    # Direct (home/away)
    if ch == ah and ca == aa:
        return True
    
    # Reversed (away/home)
    if ch == aa and ca == ah:
        return True

    return False


# ---------------------------------------------------------
# SPREAD EXTRACTION
# ---------------------------------------------------------

def extract_consensus_spread(bookmakers, home_team, away_team):
    """
    Returns the average spread (home spread) across all sportsbooks.
    Spread is negative if home is favored.
    """
    home_key = school_key(home_team)
    away_key = school_key(away_team)

    spread_values = []

    for b in bookmakers:
        for m in b.get("markets", []):
            if m.get("key") != "spreads":
                continue

            outcomes = m.get("outcomes", [])
            home_point = None
            away_point = None

            for o in outcomes:
                okey = school_key(o["name"])
                point = o.get("point")

                if okey == home_key:
                    home_point = point
                elif okey == away_key:
                    away_point = point

            if home_point is not None and away_point is not None:
                spread_values.append(home_point)

    if not spread_values:
        return ""

    return round(sum(spread_values) / len(spread_values), 1)


# ---------------------------------------------------------
# MAIN SCRIPT
# ---------------------------------------------------------

def main():
    print(f"Loading CSV: {CSV_PATH}")
    games = pd.read_csv(CSV_PATH)

    print("Fetching spreads from The Odds API...")
    params = {
        "apiKey": API_KEY,
        "regions": "us",
        "markets": "spreads",
        "oddsFormat": "decimal",
        "dateFormat": "iso"
    }

    res = requests.get(ODDS_URL, params=params)

    if res.status_code != 200:
        print("ERROR fetching Odds API:", res.text)
        return

    odds = res.json()
    print(f"Retrieved {len(odds)} Odds API games.\n")

    # Process each game in CSV
    for idx, row in games.iterrows():
        csv_home = row["home_team"]
        csv_away = row["away_team"]

        matched_game = None

        # Try to find corresponding API game
        for api_game in odds:
            api_home = api_game["home_team"]
            api_away = api_game["away_team"]

            if pair_matches(csv_home, csv_away, api_home, api_away):
                matched_game = api_game
                break

        if matched_game is None:
            print(f"[NO MATCH] {csv_away} vs {csv_home}")
            continue

        # Extract consensus spread
        bookmakers = matched_game.get("bookmakers", [])
        spread_value = extract_consensus_spread(bookmakers, csv_home, csv_away)

        if spread_value == "":
            print(f"[MATCHED but no spread] {csv_away} vs {csv_home}")
        else:
            print(f"[UPDATED] {csv_away} vs {csv_home} → spread {spread_value}")

        games.at[idx, "spread"] = spread_value

    # Save final CSV
    print("\nSaving updated CSV...")
    games.to_csv(CSV_PATH, index=False)
    print("DONE.")


if __name__ == "__main__":
    main()

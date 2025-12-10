import pandas as pd
import requests
import os
from datetime import datetime

# ======================================================
#                CONFIGURATION
# ======================================================

API_KEY = os.getenv("CFBD_API_KEY")
HEADERS = {"Authorization": f"Bearer {API_KEY}"}

CSV_PATH = "/opt/render/project/src/storage/games.csv"

LOGO_PATH = "/static/logos"  # Adjust if needed


# ======================================================
#                HELPER FUNCTIONS
# ======================================================

def normalize(s):
    """Return lowercase, punctuation-stripped string for matching."""
    if not isinstance(s, str):
        return ""
    return s.lower().replace("'", "").replace(",", "").strip()


def parse_dt(dt_str):
    """Parse datetime safely."""
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    except:
        return None


def fetch_postseason_games():
    """Fetch all postseason games for the year."""
    try:
        resp = requests.get(
            "https://api.collegefootballdata.com/games",
            params={"year": 2025, "seasonType": "postseason"},
            headers=HEADERS,
            timeout=10
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"⚠️ Error fetching CFBD postseason games: {e}")
        return []


# ======================================================
#          MATCHING + ID ASSIGNMENT SECTION
# ======================================================

def assign_cfbd_ids(df, games):
    """
    Assign missing CFBD game IDs by matching kickoff datetime,
    bowl name (via notes/venue), or location.
    """
    updated = False

    for idx, row in df.iterrows():
        existing = row.get("cfbd_game_id")

        # Skip rows that already have a valid CFBD ID
        if pd.notna(existing) and int(existing) != 0:
            continue

        csv_bowl = normalize(row["bowl_name"])
        csv_loc = normalize(row["location"])
        csv_dt = parse_dt(row["kickoff_datetime"])

        if csv_dt is None:
            print(f"⚠️ Row {idx}: invalid kickoff datetime → {row['kickoff_datetime']}")
            continue

        for g in games:
            cfbd_dt = parse_dt(g.get("startDate"))
            if cfbd_dt is None:
                continue

            # 1) Match on datetime
            datetime_match = abs((cfbd_dt - csv_dt).total_seconds()) < 5

            # 2) Match on bowl name or venue
            notes = normalize(g.get("notes", ""))
            venue = normalize(g.get("venue", ""))

            bowl_match = (
                csv_bowl in notes or notes in csv_bowl or
                csv_bowl in venue or venue in csv_loc
            )

            if datetime_match or bowl_match:
                new_id = g["id"]
                print(f"✔ Assigned CFBD ID {new_id} → {row['bowl_name']}")
                df.loc[idx, "cfbd_game_id"] = int(new_id)
                updated = True
                break

    return updated


# ======================================================
#      TEAM UPDATE SECTION (Playoff Matchups)
# ======================================================

def update_teams_from_cfbd(df, games):
    """
    After CFBD game IDs are known, update playoff matchups
    with real team names, records, and logos automatically.
    """
    updated = False
    lookup = {g["id"]: g for g in games}

    for idx, row in df.iterrows():
        cfbd_id = row.get("cfbd_game_id")

        if pd.isna(cfbd_id) or int(cfbd_id) == 0:
            continue  # No CFBD ID yet

        match = lookup.get(int(cfbd_id))
        if not match:
            continue

        # CFBD fields
        home = match.get("homeTeam")
        away = match.get("awayTeam")

        # Skip until teams are known
        if not home or not away:
            continue

        # Only update if different from CSV
        if row["home_team"] != home or row["away_team"] != away:
            print(f"✔ Updating matchup for {row['bowl_name']}: {away} vs {home}")

            # Team names
            df.loc[idx, "home_team"] = home
            df.loc[idx, "away_team"] = away

            # Records if available
            df.loc[idx, "home_record"] = match.get("homeRecord", "")
            df.loc[idx, "away_record"] = match.get("awayRecord", "")

            # Logos (assumes team name = logo file name)
            df.loc[idx, "home_logo"] = f"{LOGO_PATH}/{home.replace(' ', '_')}.png"
            df.loc[idx, "away_logo"] = f"{LOGO_PATH}/{away.replace(' ', '_')}.png"

            updated = True

    return updated


# ======================================================
#                MAIN ENTRYPOINT
# ======================================================

def main():
    print("🔄 Running CFBD ID + team updater...")

    try:
        df = pd.read_csv(CSV_PATH)
    except Exception as e:
        print(f"❌ Could not read games.csv: {e}")
        return {"status": "error", "details": str(e)}

    games = fetch_postseason_games()
    if not games:
        print("⚠️ No postseason games returned from API.")
        return {"status": "no_api_data"}

    id_updates = assign_cfbd_ids(df, games)
    team_updates = update_teams_from_cfbd(df, games)

    if id_updates or team_updates:
        try:
            df.to_csv(CSV_PATH, index=False)
            print("💾 Saved updates to games.csv")
            return {"status": "updated"}
        except Exception as e:
            print(f"❌ Failed to save CSV: {e}")
            return {"status": "save_error", "details": str(e)}

    print("ℹ️ No changes needed.")
    return {"status": "no_changes"}


if __name__ == "__main__":
    main()

from flask import Flask, request, send_from_directory
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime
import pytz
from functools import wraps
from flask_cors import CORS



# ======================================================
#               ENV + APP SETUP
# ======================================================

load_dotenv()

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})
app.secret_key = os.getenv("FLASK_SECRET_KEY", "pickem_secret_key")

if os.getenv("RENDER"):
    DISK_DIR = "/opt/render/project/src/storage"
    CSV_DIR = "/opt/render/project/src/data"
else:
    DISK_DIR = "./storage"
    CSV_DIR = "./data"
    os.makedirs(DISK_DIR, exist_ok=True)
    os.makedirs(CSV_DIR, exist_ok=True)

# ------------------------------------------------------
# LOCK DEADLINE — 9 AM PST, DECEMBER 13, 2025
# ------------------------------------------------------
PICK_DEADLINE_PST = datetime(
    2025, 12, 13, 9, 0, 0, tzinfo=pytz.timezone("US/Pacific")
)


def picks_locked() -> bool:
    """Return True if the global pick deadline has passed."""
    now_pst = datetime.now(pytz.timezone("US/Pacific"))
    return now_pst >= PICK_DEADLINE_PST

# ======================================================
#               DISK SEEDING LOGIC
# ======================================================

def seed_disk():
    """
    Copy initial CSVs into the Render persistent disk ONLY if they do not exist.
    Prevents overwriting live data on redeploys.
    """

    seed_dir = "./storage_seed"
    if not os.path.exists(seed_dir):
        print("⚠️ No seed directory found — skipping seed step.")
        return

    os.makedirs(DISK_DIR, exist_ok=True)

    for filename in ["games.csv", "groups.csv", "picks.csv"]:
        dst = f"{DISK_DIR}/{filename}"
        src = f"{seed_dir}/{filename}"

        # Only seed if disk file does NOT exist
        if not os.path.exists(dst):
            if os.path.exists(src):
                import shutil
                shutil.copy(src, dst)
                print(f"🌱 Seeded {filename} → {dst}")
            else:
                print(f"⚠️ Seed file missing: {src}")
        else:
            print(f"✔ {filename} already exists on disk — not overwriting.")


# Run seeding at startup (only once per deploy)
seed_disk()



# ======================================================
#               GROUP SUPPORT
# ======================================================

def load_groups():
    """Load list of allowed groups from groups.csv in DISK_DIR."""
    path = f"{DISK_DIR}/groups.csv"
    if not os.path.exists(path):
        return set()

    df = pd.read_csv(path)
    if "group_name" not in df.columns:
        return set()

    return set(df["group_name"].astype(str).str.strip())


ALLOWED_GROUPS = {g.lower(): g for g in load_groups()}  # map lower → real name

def require_group(f):
    @wraps(f)
    def wrapper(group_name, *args, **kwargs):
        key = group_name.replace("%20", " ").lower()
        if key not in ALLOWED_GROUPS:
            return {"error": f"Unknown group '{group_name}'"}, 404
        real_name = ALLOWED_GROUPS[key]
        return f(real_name, *args, **kwargs)
    return wrapper


# ======================================================
#               DATA HELPERS
# ======================================================

def load_games() -> pd.DataFrame:
    """Load games metadata from games.csv, with safe defaults."""
    path = f"{DISK_DIR}/games.csv"
    if not os.path.exists(path):
        return pd.DataFrame(
            columns=[
                "game_id",
                "point_value",
                "winner",
                "completed",
                "away_team",
                "home_team",
                "bowl_name",
                "kickoff_datetime",
                "away_record",
                "home_record",
                "away_logo",
                "home_logo",
                "location",
                "network",
                "status",
                "spread",
            ]
        )

    df = pd.read_csv(path)
    df = df.fillna("")
    if "game_id" not in df.columns:
        df["game_id"] = ""
    df["game_id"] = df["game_id"].astype(str)
    return df


def load_picks() -> pd.DataFrame:
    """Load picks from picks.csv and normalize schema."""
    path = f"{DISK_DIR}/picks.csv"
    if not os.path.exists(path):
        return pd.DataFrame(
            columns=[
                "group_name",
                "username",
                "name",
                "game_id",
                "selected_team",
                "point_value",
            ]
        )

    df = pd.read_csv(path)

    # Ensure required columns exist
    required_columns = [
        "group_name",
        "username",
        "name",
        "game_id",
        "selected_team",
        "point_value",
    ]
    for col in required_columns:
        if col not in df.columns:
            if col == "point_value":
                df[col] = 0
            else:
                df[col] = ""

    df["game_id"] = df["game_id"].astype(str)
    return df


def user_has_submitted(username: str, group_name: str) -> bool:
    """Check if a user has already submitted final picks for this group."""
    picks_df = load_picks()
    return (
        (picks_df["group_name"] == group_name)
        & (picks_df["username"] == username)
    ).any()


# ======================================================
#               API ROUTES
# ======================================================

# ------------------------------
# Get bowl games
# ------------------------------
@app.route("/api/<group_name>/games")
@require_group
def api_games(group_name):
    df = load_games()
    return df.to_dict(orient="records")


# ------------------------------
# Save in-progress picks to session_picks.csv
# (not used by scoring, just backup / in-progress)
# ------------------------------
@app.route("/api/<group_name>/save_session_picks", methods=["POST"])
@require_group
def api_save_session_picks(group_name):
    data = request.get_json()

    if not data:
        return {"error": "Missing JSON body"}, 400

    username = data.get("username", "").strip()
    point_value = data.get("point_value")
    raw_picks = data.get("picks")

    if not username or point_value is None or raw_picks is None:
        return {"error": "Missing required fields"}, 400

    picks_path = f"{DISK_DIR}/session_picks.csv"

    if os.path.exists(picks_path):
        df = pd.read_csv(picks_path)
    else:
        df = pd.DataFrame(
            columns=["group_name", "username", "point_value", "picks"]
        )

    # Store raw_picks as a string for now (we never read it back in this app)
    formatted_picks = str(raw_picks)

    df = pd.concat(
        [
            df,
            pd.DataFrame(
                [
                    {
                        "group_name": group_name,
                        "username": username,
                        "point_value": point_value,
                        "picks": formatted_picks,
                    }
                ]
            ),
        ],
        ignore_index=True,
    )

    df.to_csv(picks_path, index=False)

    return {"success": True}, 200


# ------------------------------
# Final submission — writes canonical picks to picks.csv
# ------------------------------
@app.route("/api/<group_name>/confirm_picks", methods=["POST"])
@require_group
def api_confirm_picks(group_name):
    data = request.get_json()

    username = data.get("username", "").strip()
    name = data.get("name", "").strip()
    picks = data.get("picks")  # dict: game_id → selected_team

    if not username or not picks:
        return {"error": "Missing username or picks"}, 400

    games_df = load_games()
    games_df["game_id"] = games_df["game_id"].astype(str)

    picks_path = f"{DISK_DIR}/picks.csv"

    if os.path.exists(picks_path):
        picks_df = pd.read_csv(picks_path)
    else:
        picks_df = pd.DataFrame(
            columns=[
                "group_name",
                "username",
                "name",
                "game_id",
                "selected_team",
                "point_value",
            ]
        )

    # Drop old rows for this user & group
    if "group_name" not in picks_df.columns:
        picks_df["group_name"] = ""
    if "username" not in picks_df.columns:
        picks_df["username"] = ""

    picks_df = picks_df[
        ~(
            (picks_df["group_name"] == group_name)
            & (picks_df["username"] == username)
        )
    ]

    # Build new rows
    new_rows = []
    for game_id, team_name in picks.items():
        # Look up point_value from games
        match = games_df.loc[games_df["game_id"] == str(game_id)]
        if match.empty:
            # Ignore invalid game ids instead of crashing
            continue
        point_val = match["point_value"].values[0]

        new_rows.append(
            {
                "group_name": group_name,
                "username": username,
                "name": name or username,
                "game_id": str(game_id),
                "selected_team": team_name,
                "point_value": int(point_val),
            }
        )

    if new_rows:
        new_df = pd.DataFrame(new_rows)
        final_df = pd.concat([picks_df, new_df], ignore_index=True)
    else:
        final_df = picks_df

    # Normalize and deduplicate
    final_df = final_df.drop_duplicates(
        subset=["group_name", "username", "game_id"], keep="last"
    )
    final_df["game_id"] = final_df["game_id"].astype(str)

    final_df.to_csv(picks_path, index=False)

    return {"success": True}, 200


# ------------------------------
# Username availability (per group)
# ------------------------------
@app.route("/api/<group_name>/check_username")
@require_group
def api_check_username(group_name):
    username = request.args.get("username", "").strip()

    if not username:
        return {"available": False}

    picks_df = load_picks()
    collision = picks_df[
        (picks_df["group_name"] == group_name)
        & (picks_df["username"] == username)
    ]

    return {"available": collision.empty}


# ------------------------------
# Get user picks (for "Your Picks" page)
# ------------------------------
@app.route("/api/<group_name>/get_user_picks")
@require_group
def api_get_user_picks(group_name):
    username = request.args.get("username", "").strip()
    if not username:
        return {"error": "Missing username"}, 400

    picks_df = load_picks()

    filtered = picks_df[
        (picks_df["group_name"] == group_name)
        & (picks_df["username"] == username)
    ]

    return filtered.to_dict(orient="records")


# ------------------------------
# User status — has submitted? is locked?
# ------------------------------
@app.route("/api/<group_name>/user_status")
@require_group
def api_user_status(group_name):
    username = request.args.get("username", "").strip()

    if not username:
        return {"submitted": False, "locked": picks_locked()}

    submitted = user_has_submitted(username, group_name)
    locked = picks_locked()

    return {"submitted": submitted, "locked": locked}


# ------------------------------
# Top 5 leaderboard (per group)
# ------------------------------
@app.route("/api/<group_name>/leaderboard_top5")
@require_group
def api_leaderboard_top5(group_name):
    picks_path = f"{DISK_DIR}/picks.csv"
    if not os.path.exists(picks_path):
        return {"leaderboard": []}

    picks_df = pd.read_csv(picks_path)

    required_columns = [
        "group_name",
        "username",
        "selected_team",
        "point_value",
        "game_id",
    ]
    for col in required_columns:
        if col not in picks_df.columns:
            picks_df[col] = 0 if col == "point_value" else ""

    picks_df = picks_df[picks_df["group_name"] == group_name]

    if picks_df.empty:
        return {"leaderboard": []}

    games_df = load_games()

    picks_df["game_id"] = picks_df["game_id"].astype(str)
    games_df["game_id"] = games_df["game_id"].astype(str)

    merged = picks_df.merge(
        games_df[["game_id", "winner", "completed"]],
        on="game_id",
        how="left",
    )

    merged["completed"] = merged["completed"].fillna(False)
    merged["correct"] = (merged["completed"] == True) & (
        merged["selected_team"] == merged["winner"]
    )

    merged["score"] = merged["correct"].astype(int) * merged["point_value"]

    totals = (
        merged.groupby("username", as_index=False)["score"]
        .sum()
        .rename(columns={"score": "total_points"})
    )

    totals = totals.sort_values("total_points", ascending=False)
    top5 = totals.head(5)

    return {"leaderboard": top5.to_dict(orient="records")}


# ------------------------------
# Full leaderboard (per group)
# ------------------------------
@app.route("/api/<group_name>/leaderboard")
@require_group
def api_leaderboard(group_name):
    picks_path = f"{DISK_DIR}/picks.csv"
    if not os.path.exists(picks_path):
        return {"leaderboard": []}

    picks_df = pd.read_csv(picks_path)

    required_columns = [
        "group_name",
        "username",
        "selected_team",
        "point_value",
        "game_id",
    ]
    for col in required_columns:
        if col not in picks_df.columns:
            picks_df[col] = 0 if col == "point_value" else ""

    picks_df = picks_df[picks_df["group_name"] == group_name]

    if picks_df.empty:
        return {"leaderboard": []}

    games_df = load_games()

    picks_df["game_id"] = picks_df["game_id"].astype(str)
    games_df["game_id"] = games_df["game_id"].astype(str)

    merged = picks_df.merge(
        games_df[["game_id", "winner", "completed"]],
        on="game_id",
        how="left",
    )

    merged["completed"] = merged["completed"].fillna(False)
    merged["correct"] = (merged["completed"] == True) & (
        merged["selected_team"] == merged["winner"]
    )

    merged["score"] = merged["correct"].astype(int) * merged["point_value"]

    totals = (
        merged.groupby("username", as_index=False)["score"]
        .sum()
        .rename(columns={"score": "total_points"})
    )

    totals = totals.sort_values("total_points", ascending=False)
    totals["rank"] = totals["total_points"].rank(
        method="min", ascending=False
    ).astype(int)

    return {"leaderboard": totals.to_dict(orient="records")}


# ------------------------------
# Picks board — comparison grid across all users in a group
# ------------------------------
@app.route("/api/<group_name>/picks_board")
@require_group
def api_picks_board(group_name):
    print("\n===== PICKS BOARD DEBUG =====", flush=True)
    print("URL group_name:", repr(group_name), flush=True)

    picks_path = f"{DISK_DIR}/picks.csv"
    print("picks.csv exists?", os.path.exists(picks_path), flush=True)

    if os.path.exists(picks_path):
        print("First 5 lines of picks.csv:", flush=True)
        with open(picks_path, "r") as f:
            for i in range(5):
                line = f.readline().rstrip("\n")
                print("  ", line, flush=True)
    else:
        print("picks.csv missing!", flush=True)

    picks_df = pd.read_csv(picks_path)

    # DEBUG: show what group_name values exist in the file
    unique_groups = picks_df["group_name"].astype(str).unique()
    print("DEBUG groups in picks_df:", [repr(g) for g in unique_groups], flush=True)

    # Ensure required columns exist (no crash)
    required_columns = [
        "group_name",
        "username",
        "selected_team",
        "point_value",
        "game_id",
    ]
    for col in required_columns:
        if col not in picks_df.columns:
            picks_df[col] = 0 if col == "point_value" else ""

    # Normalize strings
    picks_df["group_name"] = picks_df["group_name"].astype(str).str.strip()
    group_name = group_name.strip()

    # Filter by group
    picks_df = picks_df[picks_df["group_name"] == group_name]

    if picks_df.empty:
        print("No picks for group—return empty.", flush=True)
        return {"games": [], "users": []}

    # Load games dataframe
    games_df = load_games()
    print("GAMES_DF COLUMNS:", list(games_df.columns), flush=True)

    # Convert IDs to strings
    picks_df["game_id"] = picks_df["game_id"].astype(str)
    games_df["game_id"] = games_df["game_id"].astype(str)

    # FIX: Rename game point_value → game_point_value to avoid collision
    games_df = games_df.rename(columns={"point_value": "game_point_value"})

    # Build ordered games list
    games_meta = []
    for _, row in games_df.sort_values("game_id").iterrows():
        games_meta.append(
            {
                "game_id": row["game_id"],
                "label": f"{row['away_team']} @ {row['home_team']}",
                "winner": row.get("winner", ""),
                "completed": bool(row.get("completed", False)),
                "point_value": int(row.get("game_point_value", 0)),
            }
        )

    # Merge picks with game scoring data
    merged = picks_df.merge(
        games_df[["game_id", "winner", "completed", "game_point_value"]],
        on="game_id",
        how="left",
    )

    # Fill missing fields
    merged["completed"] = merged["completed"].fillna(False)
    merged["game_point_value"] = merged["game_point_value"].fillna(0).astype(int)

    # Determine correct picks
    merged["correct"] = (merged["completed"] == True) & (
        merged["selected_team"] == merged["winner"]
    )

    # Score = correct * game point value
    merged["score"] = merged["correct"].astype(int) * merged["game_point_value"]

    # Aggregate totals by user
    totals = (
        merged.groupby("username", as_index=False)["score"]
        .sum()
        .rename(columns={"score": "total_points"})
        .sort_values("total_points", ascending=False)
    )

    # Build output user list
    users_output = []
    for _, user_row in totals.iterrows():
        username = user_row["username"]
        total_points = int(user_row["total_points"])

        user_picks_df = merged[merged["username"] == username]

        pick_map = {}
        for _, r in user_picks_df.iterrows():
            pick_map[r["game_id"]] = {
                "pick": r["selected_team"],
                "correct": bool(r["correct"]),
                "completed": bool(r["completed"]),
                "point_value": int(r["game_point_value"]),
            }

        users_output.append(
            {
                "username": username,
                "total_points": total_points,
                "picks": pick_map,
            }
        )

    print("Returning picks board successfully.", flush=True)
    return {"games": games_meta, "users": users_output}


@app.route("/api/test/update_results", methods=["GET", "POST"])
def api_test_update_results():
    """
    Simulates a live results feed by randomly selecting winners
    for all games that are not yet completed.
    """
    import random

    games_path = f"{DISK_DIR}/games.csv"

    # Load games CSV
    try:
        df = pd.read_csv(games_path)
    except Exception as e:
        return {"error": f"Failed to load games.csv: {e}"}, 500

    updated = 0

    # Loop through games
    for idx, row in df.iterrows():
        # Only update games that are not completed
        completed = str(row.get("completed", "")).lower() == "true"
        if completed:
            continue

        away = row.get("away_team", "")
        home = row.get("home_team", "")

        if away and home:
            winner = random.choice([away, home])
        else:
            winner = ""

        df.at[idx, "winner"] = winner
        df.at[idx, "completed"] = True
        updated += 1

    # Save back to CSV
    try:
        df.to_csv(games_path, index=False)
    except Exception as e:
        return {"error": f"Failed to write games.csv: {e}"}, 500

    return {
        "status": "ok",
        "updated_games": updated,
        "message": f"Updated {updated} games with randomly assigned winners."
    }


# ======================================================
#               LOGOS
# ======================================================
@app.route('/static/<path:filename>')
def static_files(filename):
    static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static')
    return send_from_directory(static_dir, filename)


# ======================================================
#               MAIN
# ======================================================

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

from flask import Flask, request, send_from_directory, jsonify
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime
import pytz
from functools import wraps
from flask_cors import CORS
import uuid
import requests  # needed for update_spreads


# ======================================================
#               ENV + APP SETUP
# ======================================================

load_dotenv()

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})
app.secret_key = os.getenv("FLASK_SECRET_KEY", "pickem_secret_key")

if os.getenv("RENDER"):
    DISK_DIR = "/opt/render/project/src/storage"
    CSV_DIR = DISK_DIR
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

# ------------------------------------------------------
# CHAMPIONSHIP DEADLINE — 9:30 PM PST, JANUARY 19, 2026
# ------------------------------------------------------
CHAMPIONSHIP_END_PST = datetime(
    2026, 1, 19, 21, 30, 0, tzinfo=pytz.timezone("US/Pacific")
)


def picks_locked() -> bool:
    """Return True if the global pick deadline has passed."""
    now_pst = datetime.now(pytz.timezone("US/Pacific"))
    return now_pst >= PICK_DEADLINE_PST


def championship_complete() -> bool:
    now_pst = datetime.now(pytz.timezone("US/Pacific"))
    return now_pst >= CHAMPIONSHIP_END_PST


def generate_user_token():
    return uuid.uuid4().hex


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

# Ensure user_tokens.csv exists
TOKENS_PATH = f"{DISK_DIR}/user_tokens.csv"
if not os.path.exists(TOKENS_PATH):
    pd.DataFrame(columns=["token", "group", "username"]).to_csv(TOKENS_PATH, index=False)


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
                "away_score",
                "home_score",
                "cfbd_game_id",
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


def load_tiebreakers() -> pd.DataFrame:
    path = f"{DISK_DIR}/tiebreakers.csv"
    if not os.path.exists(path):
        return pd.DataFrame(columns=["group_name", "username", "name", "tiebreaker"])
    df = pd.read_csv(path)
    return df


def save_tiebreaker(group_name: str, username: str, name: str, tb_value: int):
    """Save or update a user's tiebreaker guess."""
    path = f"{DISK_DIR}/tiebreakers.csv"

    if os.path.exists(path):
        df = pd.read_csv(path)
    else:
        df = pd.DataFrame(columns=["group_name", "username", "name", "tiebreaker"])

    # Remove old values for this user
    df = df[
        ~(
            (df["group_name"] == group_name)
            & (df["username"] == username)
        )
    ]

    # Add new row
    new_row = pd.DataFrame([{
        "group_name": group_name,
        "username": username,
        "name": name,
        "tiebreaker": tb_value
    }])

    df = pd.concat([df, new_row], ignore_index=True)
    df.to_csv(path, index=False)


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
    name = data.get("name", "").strip()  # necessary for session save (kept for future use)
    point_value = data.get("point_value")
    raw_picks = data.get("picks")

    # DO NOT save tiebreaker here. Only final submission should save it.

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
# Create user (pre-seed picks.csv rows)
# ------------------------------
@app.route("/api/create-user", methods=["POST"])
def api_create_user():
    data = request.get_json()
    group = data.get("group", "").strip()
    username = data.get("username", "").strip()
    name = data.get("name", "").strip()

    if not group or not username or not name:
        return {"error": "Missing required fields"}, 400

    # Load groups to validate
    groups_path = os.path.join(DISK_DIR, "groups.csv")
    groups_df = pd.read_csv(groups_path)

    if group not in groups_df["group_name"].values:
        return {"error": f"Group '{group}' does not exist"}, 400

    # Load picks.csv to ensure username is not already taken
    picks_path = os.path.join(DISK_DIR, "picks.csv")
    picks_df = pd.read_csv(picks_path)

    existing = picks_df[
        (picks_df["group_name"] == group) &
        (picks_df["username"] == username)
    ]

    if not existing.empty:
        return {"error": "Username already exists"}, 400

    # Create the user by writing a placeholder row for every game
    games_path = os.path.join(DISK_DIR, "games.csv")
    games_df = pd.read_csv(games_path)

    new_rows = []
    for _, game in games_df.iterrows():
        new_rows.append({
            "group_name": group,
            "username": username,
            "name": name,
            "game_id": game["game_id"],
            "selected_team": "",
            "point_value": game["point_value"]
        })

    # Append to picks.csv
    new_df = pd.DataFrame(new_rows)
    updated_df = pd.concat([picks_df, new_df], ignore_index=True)
    updated_df.to_csv(picks_path, index=False)

    return {"success": True, "message": "User created"}, 200


# ------------------------------
# Final submission — writes canonical picks to picks.csv
# and generates permalink token + tiebreaker
# ------------------------------
@app.route("/api/<group_name>/confirm_picks", methods=["POST"])
@require_group
def api_confirm_picks(group_name):
    data = request.get_json()

    username = data.get("username", "").strip()
    name = data.get("name", "").strip()
    picks = data.get("picks")  # dict: game_id → selected_team
    tiebreaker = data.get("tiebreaker")

    if not username or not picks:
        return {"error": "Missing username or picks"}, 400

    # ======================================================
    # 1. Generate permalink token
    # ======================================================
    token_path = f"{DISK_DIR}/user_tokens.csv"

    if os.path.exists(token_path):
        token_df = pd.read_csv(token_path)
    else:
        token_df = pd.DataFrame(columns=["token", "group", "username"])

    # Remove old tokens for this user/group
    token_df = token_df[
        ~(
            (token_df["group"] == group_name) &
            (token_df["username"] == username)
        )
    ]

    # Create new token
    new_token = uuid.uuid4().hex

    token_df = pd.concat([
        token_df,
        pd.DataFrame([{
            "token": new_token,
            "group": group_name,
            "username": username
        }])
    ], ignore_index=True)

    # Save token CSV
    token_df.to_csv(token_path, index=False)

    # ======================================================
    # 2. Save tiebreaker (if present)
    # ======================================================
    if tiebreaker is not None:
        try:
            tiebreaker = int(tiebreaker)
            save_tiebreaker(group_name, username, name, tiebreaker)
        except Exception:
            pass

    # ======================================================
    # 3. Save picks to picks.csv
    # ======================================================
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

    # Remove this user's old picks
    picks_df = picks_df[
        ~(
            (picks_df["group_name"] == group_name) &
            (picks_df["username"] == username)
        )
    ]

    # Build new pick rows
    new_rows = []
    for game_id, team_name in picks.items():
        match = games_df.loc[games_df["game_id"] == str(game_id)]
        if match.empty:
            continue
        point_val = match["point_value"].values[0]

        new_rows.append({
            "group_name": group_name,
            "username": username,
            "name": name or username,
            "game_id": str(game_id),
            "selected_team": team_name,
            "point_value": int(point_val),
        })

    # Append & dedupe
    if new_rows:
        new_df = pd.DataFrame(new_rows)
        final_df = pd.concat([picks_df, new_df], ignore_index=True)
    else:
        final_df = picks_df

    final_df = final_df.drop_duplicates(
        subset=["group_name", "username", "game_id"],
        keep="last"
    )

    final_df.to_csv(picks_path, index=False)

    # ======================================================
    # 4. Return success with token
    # ======================================================
    return {"success": True, "token": new_token}, 200


# ======================================================
# GET TIEBREAKER
# ======================================================
@app.route("/api/<group_name>/get_tiebreaker")
@require_group
def api_get_tiebreaker(group_name):
    username = request.args.get("username", "").strip()
    if not username:
        return {"error": "Missing username"}, 400

    df = load_tiebreakers()

    row = df[
        (df["group_name"] == group_name) &
        (df["username"] == username)
    ]

    if row.empty:
        return {"tiebreaker": None}

    tb = row.iloc[0]["tiebreaker"]
    try:
        tb = int(tb)
    except Exception:
        pass

    return {"tiebreaker": tb}


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

    # Get username → name mapping
    name_map = picks_df[["username", "name"]].drop_duplicates()

    totals = (
        merged.groupby("username", as_index=False)["score"]
        .sum()
        .rename(columns={"score": "total_points"})
        .merge(name_map, on="username", how="left")
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

    # Add username → name mapping
    name_map = picks_df[["username", "name"]].drop_duplicates()

    totals = (
        merged.groupby("username", as_index=False)["score"]
        .sum()
        .rename(columns={"score": "total_points"})
        .merge(name_map, on="username", how="left")
    )

    totals = totals.sort_values("total_points", ascending=False)
    totals["rank"] = totals["total_points"].rank(
        method="min", ascending=False
    ).astype(int)

    return {"leaderboard": totals.to_dict(orient="records")}


# ------------------------------
# Has user submitted picks? 
# ------------------------------
@app.route("/<group>/has_submitted_picks", methods=["GET"])
def has_submitted_picks(group):
    username = request.args.get("username", "").strip()
    if not username:
        return {"submitted": False}, 200

    # Load games for the group
    games_csv = os.path.join(DISK_DIR, f"{group}_games.csv")
    if not os.path.exists(games_csv):
        return {"submitted": False}, 200

    games_df = pd.read_csv(games_csv)
    total_games = len(games_df)

    # Load picks for this user
    picks_csv = os.path.join(DISK_DIR, f"{group}_picks.csv")
    if not os.path.exists(picks_csv):
        return {"submitted": False}, 200

    picks_df = pd.read_csv(picks_csv)

    # Count user’s picks
    user_picks = picks_df[picks_df["username"] == username]

    submitted = len(user_picks) == total_games

    return {"submitted": submitted}, 200

@app.route("/api/<group>/<username>/has-submitted", methods=["GET"])
def api_has_submitted(group, username):
    picks_path = os.path.join(DISK_DIR, "picks.csv")

    try:
        df = pd.read_csv(picks_path)
    except Exception as e:
        return {"error": str(e)}, 500

    user_picks = df[(df["group_name"] == group) & (df["username"] == username)]

    # Count required picks
    games_path = os.path.join(DISK_DIR, "games.csv")
    games_df = pd.read_csv(games_path)
    required_picks = len(games_df)

    submitted = len(user_picks) == required_picks

    return {"submitted": bool(submitted)}


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

    # Load tiebreakers for all users in this group
    tiebreaker_df = load_tiebreakers()
    tiebreaker_df = tiebreaker_df[tiebreaker_df["group_name"] == group_name]

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

    # Rename game point_value → game_point_value to avoid collision
    games_df = games_df.rename(columns={"point_value": "game_point_value"})

    # Build ordered games list
    games_meta = []
    for _, row in games_df.sort_values("game_id").iterrows():
        games_meta.append(
            {
                "game_id": row["game_id"],
                "label": row.get("bowl_name", ""),
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

        # Look up real name
        real_name = ""
        if "name" in user_picks_df.columns:
            real_name = str(user_picks_df["name"].iloc[0])

        # Lookup user's tiebreaker (may be missing)
        tb_row = tiebreaker_df[tiebreaker_df["username"] == username]
        if not tb_row.empty:
            tiebreaker_value = tb_row.iloc[0]["tiebreaker"]
            try:
                tiebreaker_value = int(tiebreaker_value)
            except Exception:
                pass
        else:
            tiebreaker_value = None

        users_output.append(
            {
                "username": username,
                "name": real_name,
                "display_name": f"{username} ({real_name})" if real_name else username,
                "total_points": total_points,
                "picks": pick_map,
                "tiebreaker": tiebreaker_value,
            }
        )

    print("Returning picks board successfully.", flush=True)
    return {"games": games_meta, "users": users_output}



# ======================================================
#               PICK LOCKING
# ======================================================
@app.route("/pick-lock-status")
def api_pick_lock_status():
    return {
        "picks_locked": picks_locked(),
        "deadline_iso": PICK_DEADLINE_PST.isoformat()
    }


# ======================================================
#               UPDATE WINNERS (CFBD live winners)
# ======================================================
@app.post("/internal/update_winners")
def internal_update_winners():
    # Only allow from cron, but skip security for now
    import update_winners_live
    update_winners_live.main()
    return {"status": "ok"}


# ======================================================
#               WINNER (AFTER CHAMPIONSHIP)
# ======================================================
@app.route("/api/<group_name>/winner")
@require_group
def api_winner(group_name):
    # If championship not finished, do not declare winner yet
    if not championship_complete():
        return {"winner": None}

    picks_df = load_picks()
    picks_df = picks_df[picks_df["group_name"] == group_name]

    if picks_df.empty:
        return {"winner": None}

    games_df = load_games()
    games_df["game_id"] = games_df["game_id"].astype(str)
    picks_df["game_id"] = picks_df["game_id"].astype(str)

    # Merge picks with results
    merged = picks_df.merge(
        games_df[["game_id", "winner", "completed", "spread", "home_team", "away_team"]],
        on="game_id",
        how="left"
    )

    # Score correct picks
    merged["correct"] = (merged["completed"] == True) & (
        merged["selected_team"] == merged["winner"]
    )
    merged["score"] = merged["correct"].astype(int) * merged["point_value"]

    # Compute total points per user
    totals = merged.groupby(["username", "name"], as_index=False)["score"].sum()
    totals = totals.rename(columns={"score": "total_points"})

    # Determine championship actual total points
    # Find the national championship game (bowl_name contains "National Championship")
    champ_game = games_df[
        games_df["bowl_name"].str.contains("National Championship", case=False, na=False)
    ]

    if champ_game.empty:
        return {"winner": None}

    champ_row = champ_game.iloc[0]

    # Use home_score and away_score from games.csv
    try:
        champ_home = int(champ_row["home_score"])
        champ_away = int(champ_row["away_score"])
        champ_total_points = champ_home + champ_away
    except Exception:
        # If data not ready, do not declare a winner
        return {"winner": None}

    # Load all user tiebreakers
    tb_df = load_tiebreakers()
    tb_df = tb_df[tb_df["group_name"] == group_name]

    # Merge totals with tiebreakers
    totals = totals.merge(tb_df[["username", "tiebreaker"]], on="username", how="left")

    # Replace missing tiebreakers with very large error (they lose the tiebreaker)
    totals["tiebreaker"] = totals["tiebreaker"].fillna(9999)

    # Compute tiebreaker error
    totals["tb_error"] = (totals["tiebreaker"] - champ_total_points).abs()

    # Sort:
    # 1. Highest total_points
    # 2. Lowest tb_error
    totals = totals.sort_values(["total_points", "tb_error"], ascending=[False, True])

    winner = totals.iloc[0]

    return {
        "winner": {
            "username": winner["username"],
            "name": winner["name"],
            "total_points": int(winner["total_points"]),
            "tiebreaker": int(winner["tiebreaker"]),
        }
    }


# ======================================================
#               UPDATE SPREADS (CFBD odds)
# ======================================================
from update_spreads import update_spreads

@app.post("/internal/update_spreads")
def internal_update_spreads():
    result = update_spreads()
    return {"status": "success", "result": result}, 200




# ======================================================
#       PUBLIC PERMALINK FOR USER PICKS BY TOKEN
# ======================================================
@app.route('/api/p/<token>')
def api_get_picks_by_token(token):
    token_path = f"{DISK_DIR}/user_tokens.csv"

    if not os.path.exists(token_path):
        return jsonify({"error": "Token storage missing"}), 500

    tokens = pd.read_csv(token_path)

    row = tokens[tokens["token"] == token]
    if row.empty:
        return jsonify({"error": "Invalid link"}), 404

    group = row.iloc[0]["group"]
    username = row.iloc[0]["username"]
    name = row.iloc[0].get("name", "")
    tiebreaker = row.iloc[0].get("tiebreaker", "")

    picks_df = load_picks()
    picks_df = picks_df[
        (picks_df["group_name"] == group) &
        (picks_df["username"] == username)
    ]

    return jsonify({
        "group": group,
        "username": username,
        "name": name,
        "tiebreaker": tiebreaker,
        "picks": picks_df.to_dict(orient="records")
    })



# ======================================================
#               LOGOS / STATIC FILES
# ======================================================
@app.route("/static/<path:filename>")
def static_files(filename):
    static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
    return send_from_directory(static_dir, filename)


# ======================================================
#               MAIN
# ======================================================

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

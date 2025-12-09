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
import uuid


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

# File paths
USERS_PATH = os.path.join(DISK_DIR, "users.csv")
PICKS_PATH = os.path.join(DISK_DIR, "picks.csv")
GAMES_PATH = os.path.join(DISK_DIR, "games.csv")
GROUPS_PATH = os.path.join(DISK_DIR, "groups.csv")


def load_users() -> pd.DataFrame:
    """
    Load users.csv into a DataFrame. If the file does not exist,
    create it with the correct header structure.
    """
    if not os.path.exists(USERS_PATH):
        pd.DataFrame(
            columns=["group_name", "username", "name", "token", "has_submitted", "tiebreaker"]
        ).to_csv(USERS_PATH, index=False)
    return pd.read_csv(USERS_PATH)


def save_users(df: pd.DataFrame) -> None:
    """
    Write the DataFrame back to users.csv.
    """
    df.to_csv(USERS_PATH, index=False)

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
        group_key = group_name.lower()

        # Make sure group exists
        if group_key not in ALLOWED_GROUPS:
            print(f"[ERROR] Invalid group requested: {group_name}", flush=True)
            return {"error": "invalid_group"}, 404

        # Normalize to canonical case (e.g., "Test")
        real_group = ALLOWED_GROUPS[group_key]

        return f(real_group, *args, **kwargs)

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
# Group Info
# ------------------------------
@app.get("/group_info/<group_name>")
def get_group_info(group_name):
    import csv
    file_path = os.path.join(DISK_DIR, "group_info.csv")

    try:
        with open(file_path, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row["group_name"].strip().lower() == group_name.strip().lower():
                    return row
        return {"error": "Group not found"}, 404
    except Exception as e:
        return {"error": str(e)}, 500

@app.get("/group_pot/<group_name>")
def get_group_pot(group_name):
    import csv
    picks_path = os.path.join(DISK_DIR, "picks.csv")
    info_path = os.path.join(DISK_DIR, "group_info.csv")

    # Load buy_in
    buy_in = 0
    with open(info_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["group_name"].strip().lower() == group_name.strip().lower():
                buy_in = float(row["buy_in"])
                break

    # Count unique users in picks.csv
    unique_users = set()
    with open(picks_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["group_name"].strip().lower() == group_name.strip().lower():
                unique_users.add(row["username"])

    pot = len(unique_users) * buy_in

    return {"pot": pot, "num_players": len(unique_users)}


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
@app.route("/api/<group_name>/create-user", methods=["POST"])
@require_group
def api_create_user(group_name):
    data = request.get_json()
    username = data.get("username", "").strip()
    name = data.get("name", "").strip()

    if not username or not name:
        return {"error": "Missing username or name"}, 400

    users_df = load_users()
    picks_df = load_picks()

    group_lower = group_name.lower()
    username_lower = username.lower()

    existing_user = users_df[
        (users_df["group_name"].str.lower() == group_lower) &
        (users_df["username"].str.lower() == username_lower)
    ]

    # CASE 1: Username does not exist → create new user
    if existing_user.empty:
        token = generate_token()
        new_row = {
            "group_name": group_name,
            "username": username,
            "name": name,
            "token": token
        }

        users_df = pd.concat([users_df, pd.DataFrame([new_row])], ignore_index=True)
        save_users(users_df)

        return {"token": token, "new": True}, 200

    # CASE 2: Username DOES exist → check picks
    user_picks = picks_df[
        (picks_df["group_name"].str.lower() == group_lower) &
        (picks_df["username"].str.lower() == username_lower)
    ]

    # CASE 2a: User submitted picks → block new creation
    if len(user_picks) > 0:
        return {"error": "Username already exists and has submitted picks"}, 400

    # CASE 2b: User exists but no picks → allow resume
    token = existing_user.iloc[0]["token"]
    return {"token": token, "new": False, "resume": True}, 200



# ------------------------------
# Final submission — writes canonical picks to picks.csv
# and generates permalink token + tiebreaker
# ------------------------------
@app.route("/api/<group_name>/confirm_picks", methods=["POST"])
@require_group
def api_confirm_picks(group_name):
    data = request.get_json() or {}

    username = data.get("username", "").strip()
    name = data.get("name", "").strip()
    picks = data.get("picks")  # dict: game_id → selected_team
    tiebreaker = data.get("tiebreaker")

    if not username or not picks:
        return {"error": "Missing username or picks"}, 400

    # ======================================================
    # 1. Load users.csv and confirm user exists
    # ======================================================
    users_df = load_users()

    mask = (
        (users_df["group_name"].str.lower() == group_name.lower()) &
        (users_df["username"].str.lower() == username.lower())
    )

    if not mask.any():
        return {"error": "User does not exist"}, 400

    user_token = users_df.loc[mask, "token"].iloc[0]

    # ======================================================
    # 2. Update user submission fields
    # ======================================================
    users_df.loc[mask, "has_submitted"] = True

    if tiebreaker is not None:
        try:
            users_df.loc[mask, "tiebreaker"] = int(tiebreaker)
        except Exception:
            users_df.loc[mask, "tiebreaker"] = tiebreaker

    save_users(users_df)

    # ======================================================
    # 2b. ALSO save tiebreaker in tiebreakers.csv (for picks_board)
    # ======================================================
    if tiebreaker is not None:
        try:
            save_tiebreaker(group_name, username, name, int(tiebreaker))
        except Exception:
            save_tiebreaker(group_name, username, name, tiebreaker)

    # ======================================================
    # 3. Save final picks to picks.csv
    # ======================================================
    picks_path = f"{DISK_DIR}/picks.csv"
    games_df = load_games()
    games_df["game_id"] = games_df["game_id"].astype(str)

    if os.path.exists(picks_path):
        picks_df = pd.read_csv(picks_path)
    else:
        picks_df = pd.DataFrame(
            columns=["group_name", "username", "name", "game_id",
                     "selected_team", "point_value"]
        )

    # Remove previous picks for user
    picks_df = picks_df[
        ~(
            (picks_df["group_name"].str.lower() == group_name.lower()) &
            (picks_df["username"].str.lower() == username.lower())
        )
    ]

    # Add new picks
    new_rows = []
    for game_id, selected_team in picks.items():
        game_row = games_df[games_df["game_id"] == str(game_id)]
        if game_row.empty:
            continue

        point_val = int(game_row.iloc[0]["point_value"])

        new_rows.append({
            "group_name": group_name,
            "username": username,
            "name": name or username,
            "game_id": str(game_id),
            "selected_team": selected_team,
            "point_value": point_val,
        })

    if new_rows:
        picks_df = pd.concat([picks_df, pd.DataFrame(new_rows)], ignore_index=True)

    picks_df = picks_df.drop_duplicates(
        subset=["group_name", "username", "game_id"], keep="last"
    )

    picks_df.to_csv(picks_path, index=False)

    # ======================================================
    # 4. Success
    # ======================================================
    return {"success": True, "token": user_token}, 200


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
# Picks board — comparison grid across all users in a group
# ------------------------------
@app.route("/api/<group_name>/picks_board")
@require_group
def api_picks_board(group_name):
    print("\n===== PICKS BOARD DEBUG =====", flush=True)
    print("URL group_name:", repr(group_name), flush=True)

    # ---------------------------
    # Load picks
    # ---------------------------
    picks_path = f"{DISK_DIR}/picks.csv"
    print("picks.csv exists?", os.path.exists(picks_path), flush=True)

    if os.path.exists(picks_path):
        print("First 5 lines of picks.csv:", flush=True)
        with open(picks_path, "r") as f:
            for i in range(5):
                print("  ", f.readline().rstrip("\n"), flush=True)
    else:
        print("picks.csv missing!", flush=True)

    picks_df = pd.read_csv(picks_path)

    # normalize usernames for matching
    picks_df["username"] = picks_df["username"].astype(str).str.lower()

    # ---------------------------
    # Load users.csv (for tiebreakers + real_name)
    # ---------------------------
    users_df = load_users()
    users_df = users_df[users_df["group_name"] == group_name]
    users_df["username"] = users_df["username"].astype(str).str.lower()
    users_df["tiebreaker"] = users_df["tiebreaker"].fillna("").astype(str)

    # ---------------------------
    # Validate picks.csv columns
    # ---------------------------
    required_columns = ["group_name", "username", "selected_team", "point_value", "game_id"]
    for col in required_columns:
        if col not in picks_df.columns:
            picks_df[col] = 0 if col == "point_value" else ""

    picks_df["group_name"] = picks_df["group_name"].astype(str).str.strip()
    group_name = group_name.strip()

    # Filter to this group
    picks_df = picks_df[picks_df["group_name"] == group_name]

    if picks_df.empty:
        print("No picks for group — return empty.", flush=True)
        return {"games": [], "users": []}

    # ---------------------------
    # Load games
    # ---------------------------
    games_df = load_games()
    print("GAMES_DF COLUMNS:", list(games_df.columns), flush=True)

    # Ensure numeric sorting
    picks_df["game_id"] = picks_df["game_id"].astype(int)
    games_df["game_id"] = games_df["game_id"].astype(int)

    games_df = games_df.rename(columns={"point_value": "game_point_value"})

    # ---------------------------
    # Build ordered games list (NUMERIC SORT)
    # ---------------------------
    games_meta = []
    for _, row in games_df.sort_values("game_id").iterrows():
        games_meta.append(
            {
                "game_id": str(row["game_id"]),
                "label": row.get("bowl_name", ""),
                "winner": row.get("winner", ""),
                "completed": bool(row.get("completed", False)),
                "point_value": int(row.get("game_point_value", 0)),
            }
        )

    # ---------------------------
    # Merge picks with game scoring
    # ---------------------------
    merged = picks_df.merge(
        games_df[["game_id", "winner", "completed", "game_point_value"]],
        on="game_id",
        how="left",
    )

    merged["completed"] = merged["completed"].fillna(False)
    merged["game_point_value"] = merged["game_point_value"].fillna(0).astype(int)

    merged["correct"] = (merged["completed"] == True) & (
        merged["selected_team"] == merged["winner"]
    )

    merged["score"] = merged["correct"].astype(int) * merged["game_point_value"]

    totals = (
        merged.groupby("username", as_index=False)["score"]
        .sum()
        .rename(columns={"score": "total_points"})
        .sort_values("total_points", ascending=False)
    )

    # ---------------------------
    # Build users output
    # ---------------------------
    users_output = []

    for _, row in totals.iterrows():
        username = row["username"]
        total_points = int(row["total_points"])

        user_picks_df = merged[merged["username"] == username]

        # numeric → string keys
        pick_map = {
            str(r["game_id"]): {
                "pick": r["selected_team"],
                "correct": bool(r["correct"]),
                "completed": bool(r["completed"]),
                "point_value": int(r["game_point_value"]),
            }
            for _, r in user_picks_df.iterrows()
        }

        # real name
        real_name = ""
        if "name" in user_picks_df.columns:
            real_name = str(user_picks_df["name"].iloc[0])

        # tiebreaker (from users.csv ONLY)
        tb_row = users_df[users_df["username"] == username]
        if not tb_row.empty:
            raw_tb = str(tb_row.iloc[0]["tiebreaker"]).strip()
            if raw_tb and raw_tb.lower() not in ("nan", "none", ""):
                try:
                    tiebreaker_value = int(float(raw_tb))
                except:
                    tiebreaker_value = raw_tb
            else:
                tiebreaker_value = None
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

@app.route("/api/<group_name>/pick-lock-status")
@require_group
def api_group_pick_lock_status(group_name):
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

@app.route("/api/<group_name>/<username>/has-submitted", methods=["GET"])
@require_group
def api_has_submitted(group_name, username):
    username = username.strip()
    if not username:
        return {"submitted": False}

    picks_df = load_picks()

    # Filter just this group + username
    user_picks = picks_df[
        (picks_df["group_name"].str.lower() == group_name.lower()) &
        (picks_df["username"].str.lower() == username.lower())
    ]

    # Check number of games
    games_df = load_games()
    total_games = len(games_df)

    # Full submission = they have a pick for every game
    submitted = len(user_picks) == total_games

    return {"submitted": bool(submitted)}


# ======================================================
#               UPDATE SPREADS (CFBD odds)
# ======================================================
from update_spreads import update_spreads

@app.post("/internal/update_spreads")
def internal_update_spreads():
    result = update_spreads()
    return {"status": "success", "result": result}, 200




# ===== PUBLIC PERMALINK LOOKUP =====
@app.route("/api/p/<token>")
def api_get_picks_by_token(token):
    users_df = load_users()

    row = users_df[users_df["token"] == token]

    if row.empty:
        return jsonify({"error": "Invalid link"}), 404

    group_name = row.iloc[0]["group_name"]
    username = row.iloc[0]["username"]
    name = row.iloc[0]["name"]
    tiebreaker = row.iloc[0]["tiebreaker"]

    picks_df = load_picks()
    user_picks = picks_df[
        (picks_df["group_name"] == group_name) &
        (picks_df["username"] == username)
    ]

    return jsonify({
        "group": group_name,
        "username": username,
        "name": name,
        "tiebreaker": tiebreaker,
        "picks": user_picks.to_dict(orient="records")
    })


@app.route("/p/<token>")
def permalink_redirect(token):
    return api_get_picks_by_token(token)



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

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
    now_pst = datetime.now(pytz.timezone("US/Pacific"))
    return now_pst >= PICK_DEADLINE_PST


# ======================================================
#               GROUP SUPPORT
# ======================================================

def load_groups():
    path = f"{DISK_DIR}/groups.csv"
    if not os.path.exists(path):
        return set()

    df = pd.read_csv(path)
    if "group_name" not in df.columns:
        return set()

    return set(df["group_name"].astype(str).str.strip())

ALLOWED_GROUPS = {g.lower(): g for g in load_groups()}

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
#               DATA LOADERS
# ======================================================

def load_games() -> pd.DataFrame:
    path = f"{DISK_DIR}/games.csv"
    if not os.path.exists(path):
        return pd.DataFrame(
            columns=[
                "game_id", "point_value", "winner", "completed",
                "away_team", "home_team", "bowl_name",
                "kickoff_datetime", "away_record", "home_record",
                "away_logo", "home_logo", "location", "network",
                "status", "spread"
            ]
        )

    df = pd.read_csv(path).fillna("")
    df["game_id"] = df["game_id"].astype(str)
    return df


def load_picks() -> pd.DataFrame:
    path = f"{DISK_DIR}/picks.csv"
    if not os.path.exists(path):
        return pd.DataFrame(columns=[
            "group_name", "username", "name", "game_id",
            "selected_team", "point_value"
        ])

    df = pd.read_csv(path)

    required = ["group_name", "username", "name", "game_id", "selected_team", "point_value"]
    for col in required:
        if col not in df.columns:
            df[col] = "" if col != "point_value" else 0

    df["game_id"] = df["game_id"].astype(str)
    return df


def load_tiebreakers() -> pd.DataFrame:
    path = f"{DISK_DIR}/tiebreakers.csv"
    if not os.path.exists(path):
        return pd.DataFrame(columns=["group_name", "username", "name", "tiebreaker"])
    return pd.read_csv(path)


def save_tiebreaker(group_name: str, username: str, name: str, tb_value: int):
    path = f"{DISK_DIR}/tiebreakers.csv"

    if os.path.exists(path):
        df = pd.read_csv(path)
    else:
        df = pd.DataFrame(columns=["group_name", "username", "name", "tiebreaker"])

    df = df[
        ~((df["group_name"] == group_name) & (df["username"] == username))
    ]

    new = pd.DataFrame([{
        "group_name": group_name,
        "username": username,
        "name": name,
        "tiebreaker": tb_value
    }])

    df = pd.concat([df, new], ignore_index=True)
    df.to_csv(path, index=False)


def user_has_submitted(username: str, group_name: str) -> bool:
    picks_df = load_picks()
    return (
        (picks_df["group_name"] == group_name) &
        (picks_df["username"] == username)
    ).any()


# ======================================================
#               API ROUTES
# ======================================================

@app.route("/api/<group_name>/games")
@require_group
def api_games(group_name):
    return load_games().to_dict(orient="records")


# ------------------------------
# Save session picks (no tiebreakers here)
# ------------------------------
@app.route("/api/<group_name>/save_session_picks", methods=["POST"])
@require_group
def api_save_session_picks(group_name):
    data = request.get_json()

    username = data.get("username", "").strip()
    point_value = data.get("point_value")
    raw_picks = data.get("picks")

    if not username or point_value is None or raw_picks is None:
        return {"error": "Missing required fields"}, 400

    path = f"{DISK_DIR}/session_picks.csv"

    if os.path.exists(path):
        df = pd.read_csv(path)
    else:
        df = pd.DataFrame(columns=["group_name", "username", "point_value", "picks"])

    df = pd.concat([
        df,
        pd.DataFrame([{
            "group_name": group_name,
            "username": username,
            "point_value": point_value,
            "picks": str(raw_picks)
        }])
    ], ignore_index=True)

    df.to_csv(path, index=False)

    return {"success": True}, 200


# ------------------------------
# Create user
# ------------------------------
@app.route("/api/create-user", methods=["POST"])
def api_create_user():
    data = request.get_json()

    group = data.get("group", "").strip()
    username = data.get("username", "").strip()
    name = data.get("name", "").strip()

    if not group or not username or not name:
        return {"error": "Missing required fields"}, 400

    groups_df = pd.read_csv(f"{DISK_DIR}/groups.csv")

    if group not in groups_df["group_name"].values:
        return {"error": f"Group '{group}' does not exist"}, 400

    picks_df = load_picks()
    exists = picks_df[
        (picks_df["group_name"] == group) &
        (picks_df["username"] == username)
    ]

    if not exists.empty:
        return {"error": "Username already exists"}, 400

    games = load_games()

    new_rows = [{
        "group_name": group,
        "username": username,
        "name": name,
        "game_id": row["game_id"],
        "selected_team": "",
        "point_value": row["point_value"]
    } for _, row in games.iterrows()]

    updated = pd.concat([picks_df, pd.DataFrame(new_rows)], ignore_index=True)
    updated.to_csv(f"{DISK_DIR}/picks.csv", index=False)

    return {"success": True}, 200


# ------------------------------
# Final submission (picks + tiebreaker)
# ------------------------------
@app.route("/api/<group_name>/confirm_picks", methods=["POST"])
@require_group
def api_confirm_picks(group_name):
    data = request.get_json()

    username = data.get("username", "").strip()
    name = data.get("name", "").strip()
    picks = data.get("picks", {})

    if not username or not picks:
        return {"error": "Missing username or picks"}, 400

    # Save tiebreaker if included
    tiebreaker = data.get("tiebreaker", None)
    if tiebreaker is not None:
        try:
            save_tiebreaker(group_name, username, name, int(tiebreaker))
        except:
            pass

    games_df = load_games()

    picks_path = f"{DISK_DIR}/picks.csv"
    if os.path.exists(picks_path):
        picks_df = pd.read_csv(picks_path)
    else:
        picks_df = pd.DataFrame(columns=[
            "group_name", "username", "name",
            "game_id", "selected_team", "point_value"
        ])

    # Remove old rows for user
    picks_df = picks_df[
        ~((picks_df["group_name"] == group_name) &
          (picks_df["username"] == username))
    ]

    new_rows = []
    for gid, team in picks.items():
        match = games_df[games_df["game_id"] == str(gid)]
        if match.empty:
            continue
        point_val = int(match.iloc[0]["point_value"])

        new_rows.append({
            "group_name": group_name,
            "username": username,
            "name": name or username,
            "game_id": str(gid),
            "selected_team": team,
            "point_value": point_val,
        })

    final = pd.concat([picks_df, pd.DataFrame(new_rows)], ignore_index=True)
    final = final.drop_duplicates(subset=["group_name", "username", "game_id"])
    final.to_csv(picks_path, index=False)

    return {"success": True}, 200


# ------------------------------
# Get tiebreaker (per user)
# ------------------------------
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
    except:
        pass

    return {"tiebreaker": tb}


# ------------------------------
# Picks board (now includes tiebreakers)
# ------------------------------
@app.route("/api/<group_name>/picks_board")
@require_group
def api_picks_board(group_name):

    picks_df = load_picks()
    picks_df = picks_df[picks_df["group_name"] == group_name]

    if picks_df.empty:
        return {"games": [], "users": []}

    tiebreakers = load_tiebreakers()
    tiebreakers = tiebreakers[tiebreakers["group_name"] == group_name]

    games_df = load_games()
    games_df["game_id"] = games_df["game_id"].astype(str)
    picks_df["game_id"] = picks_df["game_id"].astype(str)

    games_df = games_df.rename(columns={"point_value": "game_point_value"})

    games_meta = [{
        "game_id": row["game_id"],
        "label": row.get("bowl_name", ""),
        "winner": row.get("winner", ""),
        "completed": bool(row.get("completed", False)),
        "point_value": int(row.get("game_point_value", 0))
    } for _, row in games_df.sort_values("game_id").iterrows()]

    merged = picks_df.merge(
        games_df[["game_id", "winner", "completed", "game_point_value"]],
        on="game_id",
        how="left"
    )

    merged["completed"] = merged["completed"].fillna(False)
    merged["correct"] = (merged["selected_team"] == merged["winner"]) & merged["completed"]
    merged["score"] = merged["correct"].astype(int) * merged["game_point_value"]

    totals = merged.groupby("username")["score"].sum().reset_index()
    totals = totals.sort_values("score", ascending=False)

    users_output = []
    for _, row in totals.iterrows():
        username = row["username"]
        user_rows = merged[merged["username"] == username]

        picks_map = {
            r["game_id"]: {
                "pick": r["selected_team"],
                "correct": bool(r["correct"]),
                "completed": bool(r["completed"]),
                "point_value": int(r["game_point_value"])
            }
            for _, r in user_rows.iterrows()
        }

        name = user_rows.iloc[0]["name"] if "name" in user_rows.columns else username

        tb_row = tiebreakers[tiebreakers["username"] == username]
        if not tb_row.empty:
            try:
                tiebreak_value = int(tb_row.iloc[0]["tiebreaker"])
            except:
                tiebreak_value = tb_row.iloc[0]["tiebreaker"]
        else:
            tiebreak_value = None

        users_output.append({
            "username": username,
            "name": name,
            "display_name": f"{username} ({name})",
            "total_points": int(row["score"]),
            "picks": picks_map,
            "tiebreaker": tiebreak_value
        })

    return {"games": games_meta, "users": users_output}


# ------------------------------
# Pick locking status
# ------------------------------
@app.route("/pick-lock-status")
def api_pick_lock_status():
    return {
        "picks_locked": picks_locked(),
        "deadline_iso": PICK_DEADLINE_PST.isoformat()
    }


# ------------------------------
# Static logos
# ------------------------------
@app.route('/static/<path:filename>')
def static_files(filename):
    static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static')
    return send_from_directory(static_dir, filename)



# ======================================================
#               MAIN
# ======================================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

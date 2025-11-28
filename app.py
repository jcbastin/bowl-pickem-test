from flask import Flask, render_template, request, redirect, session, url_for
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime
import pytz
from scheduler import start_scheduler

# ---------- ENV & APP SETUP ---------- #

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "pickem_secret_key")  # fallback for local

# Shared directories for Render
CSV_DIR = "/opt/render/project/src/data"
DISK_DIR = "/opt/render/project/src/storage"

# Global pick deadline
PICK_DEADLINE_PST = datetime(2025, 11, 28, 9, 0, 0, tzinfo=pytz.timezone("US/Pacific"))

def picks_locked():
    now_pst = datetime.now(pytz.timezone("US/Pacific"))
    return now_pst >= PICK_DEADLINE_PST

# ---------- HELPER FUNCTIONS ---------- #

@app.before_request
def log_request():
    print("➡️ Incoming request:", request.method, request.path)


@app.route("/check-key")
def check_key():
    key = os.getenv("CFBD_API_KEY")
    if key:
        return "API key loaded successfully."
    else:
        return "API key NOT loaded."

def load_games():
    """Load all test games from CSV."""
    return pd.read_csv(f"{DISK_DIR}/test_games.csv")


def load_team_records():
    df = pd.read_csv(f"{DISK_DIR}/team_records.csv")
    records = dict(zip(df["team"], df["record"]))

    aliases = {
        "NM State": ["New Mexico State", "NMSU", "NM St"],
        "UCF": ["Central Florida"],
        "Ole Miss": ["Mississippi"],
        "Pitt": ["Pittsburgh"],
        "Georgia Tech": ["Georgia Institute of Technology"],
    }

    for display_name, possible_keys in aliases.items():
        for key in possible_keys:
            if key in records:
                records[display_name] = records[key]

    return records


def load_spreads():
    df = pd.read_csv(f"{DISK_DIR}/spreads.csv")
    return dict(zip(df["game_id"], df["spread"]))


def user_has_submitted(username: str) -> bool:
    """
    Check if the user has already submitted picks.
    """
    picks_path = f"{DISK_DIR}/picks.csv"
    if not os.path.exists(picks_path):
        return False

    df = pd.read_csv(picks_path)
    return username in df["username"].values


def write_final_picks_to_csv(username: str, picks_by_page: dict):
    """
    Flatten session picks structure and append to data/picks.csv.
    picks_by_page looks like:
    {1: {game_id: team, ...}, 2: {...}, ...}
    """
    rows = []

    for point_value, games_dict in picks_by_page.items():
        for game_id, selected_team in games_dict.items():
            rows.append({
                "username": username,
                "game_id": str(game_id),
                "selected_team": selected_team,
                "point_value": int(point_value),
            })

    if not rows:
        return

    picks_path = f"{DISK_DIR}/picks.csv"
    new_df = pd.DataFrame(rows)

    if not os.path.exists(picks_path):
        new_df.to_csv(picks_path, index=False)
    else:
        new_df.to_csv(picks_path, mode="a", header=False, index=False)


# ---------- ROUTES ---------- #

@app.route('/')
def home():
    username = session.get('username')
    has_submitted = user_has_submitted(username) if username else False
    return render_template(
        'index.html',
        username=username,
        has_submitted=has_submitted
    )


@app.route('/enter_name', methods=['GET'])
def enter_name():
    return render_template('enter_name.html')


@app.route('/set_name', methods=['POST'])
def set_name():
    username = request.form['username'].strip()
    session['username'] = username
    session['picks'] = {}       # reset in-session picks
    session['finalized'] = False

    # If this user already has final picks in CSV, do not allow new ones
    if user_has_submitted(username):
        return redirect(url_for('picks_board'))

    return redirect('/picks/1')


@app.route('/picks/<int:points>', methods=['GET'])
def picks_page(points):
    if picks_locked():
        return render_template("locked.html")

    if 'username' not in session:
        return redirect('/enter_name')

    username = session['username']

    # If already finalized or already in CSV, no more editing
    if session.get('finalized') or user_has_submitted(username):
        return redirect(url_for('picks_board'))

    games = load_games()
    subset = games[games['point_value'] == points]

    # Define a robust spread formatting function
    def format_spread(row):
        sp = row.get("spread")
        home = row.get("home_team", "")

        if pd.isna(sp):
            return ""

        try:
            sp = float(sp)
        except:
            return ""

        sign = "+" if sp > 0 else ""
        return f"— {home} {sign}{sp}"

    # Apply the formatting function to the subset
    subset["spread"] = subset.apply(format_spread, axis=1)

    records = load_team_records()
    spreads = load_spreads()

    picks_by_page = session.get('picks', {})
    page_picks = picks_by_page.get(str(points), {})

    return render_template(
        f'picks_{points}.html',
        games=subset.to_dict(orient='records'),
        username=username,
        records=records,
        spreads=spreads,
        points=points,
        page_picks=page_picks
    )


@app.route('/submit_picks/<int:points>', methods=['POST'])
def submit_picks(points):
    if picks_locked():
        return render_template("locked.html")

    if 'username' not in session:
        return redirect('/enter_name')

    username = session['username']

    # If already done → block editing
    if session.get('finalized') or user_has_submitted(username):
        return redirect('/already_submitted')

    form = request.form
    direction = form.get('direction', 'next')

    picks_by_page = session.get('picks', {})
    page_key = str(points)
    page_picks = picks_by_page.get(page_key, {})

    # ONLY update picks when user clicks NEXT
    if direction == 'next':
        for game_id, selected_team in form.items():
            if game_id == "direction":
                continue
            page_picks[game_id] = selected_team

        picks_by_page[page_key] = page_picks
        session['picks'] = picks_by_page

    # NAVIGATION
    if direction == 'back':
        prev_points = max(1, points - 1)
        return redirect(f'/picks/{prev_points}')
    else:
        if points < 5:
            next_points = points + 1
            return redirect(f'/picks/{next_points}')
        else:
            return redirect(url_for('review_picks'))




@app.route('/review_picks')
def review_picks():
    if 'username' not in session:
        return redirect('/enter_name')

    username = session['username']

    # If this user already has final picks in CSV,
    # don't let them review/edit again
    if user_has_submitted(username):
        return redirect(url_for('picks_board'))

    picks_by_page = session.get('picks', {})
    if not picks_by_page:
        # No picks in session yet – send them to first page
        return redirect('/picks/1')

    # Flatten in-session picks into a list of rows
    rows = []
    for point_value_str, games_dict in picks_by_page.items():
        for game_id, selected_team in games_dict.items():
            rows.append({
                "game_id": str(game_id),
                "selected_team": selected_team,
                "point_value": int(point_value_str)  # "1" -> 1
            })

    if not rows:
        return redirect('/picks/1')

    picks_df = pd.DataFrame(rows)

    # Load game metadata for matchup display
    games_df = pd.read_csv(f'{DISK_DIR}/test_games.csv')

    # Make sure game_id types match
    games_df['game_id'] = games_df['game_id'].astype(str)
    picks_df['game_id'] = picks_df['game_id'].astype(str)

    # Merge picks with home/away teams
    merged = picks_df.merge(
        games_df[['game_id', 'home_team', 'away_team']],
        on='game_id',
        how='left'
    )

    # Sort nicely by point value then game_id
    if 'point_value' in merged.columns:
        merged = merged.sort_values(['point_value', 'game_id'])
    else:
        merged = merged.sort_values(['game_id'])

    return render_template(
        'review_picks.html',
        username=username,
        picks=merged.to_dict(orient='records')
    )


@app.route('/confirm_picks', methods=['POST'])
def confirm_picks():
    if 'username' not in session:
        return redirect('/enter_name')

    username = session['username']

    # If picks already saved for this user, don't save again
    if user_has_submitted(username):
        session['finalized'] = True
        return redirect(url_for('picks_board'))

    picks_by_page = session.get('picks', {})
    if not picks_by_page:
        # No picks in session – send them back to start
        return redirect('/picks/1')

    # Write all in-session picks to CSV exactly once
    write_final_picks_to_csv(username, picks_by_page)

    # Mark as finalized for this browser session
    session['finalized'] = True

    # Optionally clear in-session picks (not required, but tidy)
    session['picks'] = {}

    # Go to picks board after confirming
    return redirect(url_for('picks_board'))



@app.route('/leaderboard')
def leaderboard():
    games = pd.read_csv(f'{DISK_DIR}/test_games.csv')
    picks = pd.read_csv(f'{DISK_DIR}/picks.csv')

    # Ensure matching merge keys
    games['game_id'] = games['game_id'].astype(str)
    picks['game_id'] = picks['game_id'].astype(str)

    # Merge picks with game results
    merged = picks.merge(
        games[['game_id', 'winner', 'completed', 'point_value']],
        on='game_id',
        how='left'
    )

    # If point_value came in as point_value_y or point_value_x, fix it
    if 'point_value' not in merged.columns:
        if 'point_value_x' in merged.columns:
            merged.rename(columns={'point_value_x': 'point_value'}, inplace=True)
        elif 'point_value_y' in merged.columns:
            merged.rename(columns={'point_value_y': 'point_value'}, inplace=True)
        else:
            merged['point_value'] = 0

    # Correctness ONLY when the game is completed
    merged['correct'] = merged['selected_team'] == merged['winner']

    # Final score for each pick
    merged['score'] = merged['correct'] * merged['point_value']

    # Summarize user total points
    leaderboard_df = (
        merged.groupby('username', as_index=False)
        .agg({'score': 'sum'})
        .sort_values(by='score', ascending=False)
    )

    # Dense ranking
    leaderboard_df['rank'] = (
        leaderboard_df['score'].rank(method='dense', ascending=False).astype(int)
    )

    # Display sorted by rank then name
    leaderboard_df = leaderboard_df.sort_values(['rank', 'username'])

    return render_template(
        'leaderboard.html',
        leaderboard=leaderboard_df.to_dict(orient='records')
    )


@app.route('/user/<username>')
def user_picks(username):
    # Load data
    games = pd.read_csv(f'{DISK_DIR}/test_games.csv')
    picks = pd.read_csv(f'{DISK_DIR}/picks.csv')

    # Ensure consistent types
    games['game_id'] = games['game_id'].astype(str)
    picks['game_id'] = picks['game_id'].astype(str)

    # Merge picks with required columns from games_df
    merged = picks.merge(
        games[['game_id', 'home_team', 'away_team', 'winner', 'completed', 'point_value']],
        on='game_id',
        how='left'
    )

    # Ensure all required columns exist
    required_cols = ['winner', 'completed', 'point_value']
    for col in required_cols:
        if col not in merged.columns:
            if col == 'completed':
                merged[col] = False
            elif col == 'point_value':
                merged[col] = 0
            else:
                merged[col] = None

    # Update correctness and scoring logic
    merged['correct'] = (merged['completed'] == True) & (merged['selected_team'] == merged['winner'])
    merged['score'] = merged['correct'].astype(int) * merged['point_value']

    # Add results column for display
    def compute_results(row):
        if not row['completed']:
            return "—"
        if row['correct']:
            return f"✔ {row['winner']} won"
        return f"✘ {row['winner']} won"

    merged['results'] = merged.apply(compute_results, axis=1)

    # Pass point_value, score, and results to the template
    return render_template(
        'user_picks.html',
        username=username,
        picks=merged.to_dict(orient='records'),
        total_score=int(merged['score'].sum())
    )


@app.route('/picks_board')
def picks_board():
    # User must be logged in
    username = session.get('username')
    if not username:
        return redirect('/')

    picks_df = pd.read_csv(f'{DISK_DIR}/picks.csv')
    games_df = pd.read_csv(f'{DISK_DIR}/test_games.csv')

    # If user hasn't submitted picks, redirect back to home
    if username not in picks_df['username'].unique():
        return redirect('/')

    # Ensure correct datatypes
    games_df['game_id'] = games_df['game_id'].astype(str)
    picks_df['game_id'] = picks_df['game_id'].astype(str)

    # Build metadata for display in table header
    games_meta = []
    for _, row in games_df.sort_values('game_id').iterrows():
        games_meta.append({
            "game_id": row['game_id'],
            "label": f"{row['away_team']} @ {row['home_team']}",
            "completed": row['completed'],
            "winner": row['winner']
        })
    # Compute per-user total points and competition-style rank (1,1,3,4)
    # We reuse the scoring logic from /leaderboard and /user/<username>
    import numpy as np

    # Clean winner + completed fields
    games_df['winner'] = games_df['winner'].replace({np.nan: None, "nan": None, "": None})
    games_df['completed'] = games_df['completed'].replace({"True": True, "False": False})
    games_df['completed'] = games_df['completed'].astype(bool)

    # Ensure consistent types
    games_df['game_id'] = games_df['game_id'].astype(str)
    picks_df['game_id'] = picks_df['game_id'].astype(str)

    # Merge picks with necessary game info to compute scores
    score_merged = picks_df.merge(
        games_df[['game_id', 'winner', 'completed', 'point_value']],
        on='game_id',
        how='left'
    )

    # Fix point_value column just in case
    if 'point_value' not in score_merged.columns:
        if 'point_value_x' in score_merged.columns:
            score_merged.rename(columns={'point_value_x': 'point_value'}, inplace=True)
        elif 'point_value_y' in score_merged.columns:
            score_merged.rename(columns={'point_value_y': 'point_value'}, inplace=True)
        else:
            score_merged['point_value'] = 0

    # Correctness should only count when game is completed
    score_merged['correct'] = (score_merged['completed'] == True) & (score_merged['selected_team'] == score_merged['winner'])

    # Score is point_value only when correct
    score_merged['score'] = score_merged['correct'] * score_merged['point_value']

    # Aggregate total per user
    totals = (
        score_merged.groupby('username', as_index=False)
        .agg({'score': 'sum'})
        .rename(columns={'score': 'total_points'})
    )

    # If a user somehow has no rows (shouldn't happen), ensure they are present via picks_df
    # Compute competition-style ranks ('min' => 1,1,3,4)
    if not totals.empty:
        totals['rank'] = totals['total_points'].rank(method='min', ascending=False).astype(int)
    else:
        totals['rank'] = []

    # Order users by rank then username
    totals = totals.sort_values(['rank', 'username'])

    # Build a structure: list of rows each with username, rank, total_points, and their picks mapping
    picks_rows = []
    for _, urow in totals.iterrows():
        user = urow['username']
        user_picks = picks_df[picks_df['username'] == user]
        user_row = {}

        for _, up in user_picks.iterrows():
            gid = up['game_id']
            pick = up['selected_team']

            # Default no color
            cell_class = ""

            # Find game info
            game_row = games_df[games_df['game_id'] == gid].iloc[0]
            completed = bool(game_row['completed'])
            winner = game_row['winner']

            # Only color if completed = True AND winner is not missing
            if completed and isinstance(winner, str) and winner.strip() != "":
                if pick == winner:
                    cell_class = "correct"
                else:
                    cell_class = "incorrect"

            user_row[gid] = {
                "pick": pick,
                "class": cell_class
            }

        picks_rows.append({
            'username': user,
            'rank': int(urow['rank']),
            'total_points': int(urow['total_points']),
            'picks': user_row
        })

    return render_template(
        'picks_board.html',
        games=games_meta,
        picks=picks_rows
    )


# ---------- MAIN ---------- #

# Start scheduler on all deployments (including Gunicorn)
start_scheduler()

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True)


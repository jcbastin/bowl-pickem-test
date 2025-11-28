import threading
import subprocess
import time
import os

def run_update_script():
    """Run the update_winners_live.py script every 15 minutes."""
    while True:
        try:
            print("🔄 Running update_winners_live.py...")
            # Ensure the script runs from the correct working directory
            subprocess.run(
                ["python3", "update_winners_live.py"],
                cwd=os.path.dirname(os.path.abspath(__file__)),
                check=True
            )
            print("✅ update_winners_live.py completed successfully.")
        except subprocess.CalledProcessError as e:
            print(f"❌ Error running update_winners_live.py: {e}")
        except Exception as e:
            print(f"⚠️ Unexpected error: {e}")
        
        # Sleep for 15 minutes (900 seconds)
        print("⏳ Sleeping for 15 minutes...")
        time.sleep(900)

def start_scheduler():
    """Start the scheduler in a background thread."""
    scheduler_thread = threading.Thread(target=run_update_script, daemon=True)
    scheduler_thread.start()
    print("🕒 Scheduler started.")
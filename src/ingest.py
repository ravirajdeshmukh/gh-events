#%%
import time
import argparse
import requests
import os
import json
from datetime import datetime
from collections import deque
from utils.defaults import (
    INTERESTED_TYPES, EVENT_DUMP_FILE,
    BRONZE_DIR, BASE_STORAGE_PATH
)
from utils.file_ops import write_json_to_file

#%%
# Buffers to temporarily hold fetched events before writing to disk
watch_event_buffer = deque()
issues_event_buffer = deque()
pr_event_buffer = deque()

# Constants
EVENT_THRESHOLD = 10                 # Events per type before flush
FETCH_INTERVAL_SECONDS = 10          # Frequency of GitHub API requests
RUN_DURATION = 300                   # Default run time (in seconds) in batch mode

# Mapping event types to their respective buffers
event_router = {
    "WatchEvent": watch_event_buffer,
    "PullRequestEvent": pr_event_buffer,
    "IssuesEvent": issues_event_buffer
}

#%%
def fetch_github_events(duration: int = RUN_DURATION, live: bool = False):
    """
    Fetches GitHub events and stores them in a bronze layer.

    Args:
        duration (int): Time to run the ingestion in batch mode (ignored in live mode).
        live (bool): If True, runs indefinitely. Otherwise, runs for the given duration.

    Fetches events from the GitHub API every FETCH_INTERVAL_SECONDS, stores
    them in memory until the EVENT_THRESHOLD is met, then flushes to disk.
    """
    try:
        start_time = time.time()
        elapsed = time.time() - start_time

        while live or elapsed < duration:        
            print("[INFO] Fetching GitHub events...")

            try:
                res = requests.get(
                    "https://api.github.com/events",
                    headers={"User-Agent": "GitMonitor"}
                )

                if res.status_code == 200:
                    for event in res.json():
                        event_type = event.get("type")
                        if event_type in INTERESTED_TYPES:
                            event_router[event_type].append(event)

                    print(f"Buffer lengths | WatchEvent: {len(watch_event_buffer)}, "
                          f"PullRequestEvent: {len(pr_event_buffer)}, "
                          f"IssuesEvent: {len(issues_event_buffer)}")

                    # Flush to bronze layer when buffer hits threshold
                    if len(watch_event_buffer) >= EVENT_THRESHOLD:
                        flush_to_bronze(watch_event_buffer, "WatchEvent")
                    if len(pr_event_buffer) >= EVENT_THRESHOLD:
                        flush_to_bronze(pr_event_buffer, "PullRequestEvent")
                    if len(issues_event_buffer) >= EVENT_THRESHOLD:
                        flush_to_bronze(issues_event_buffer, "IssuesEvent")
                
                elif res.status_code == 403:
                    print("GH API limit hit or the connection is unreachable.")

                time.sleep(FETCH_INTERVAL_SECONDS)
                elapsed = time.time() - start_time

            except Exception as e:
                print(f"[ERROR] Failed to fetch events: {e}")

    finally:
        # Ensure all buffers are flushed on exit
        print("[INFO] Fetching completed. Flushing remaining events...")
        if len(watch_event_buffer) > 0:
            flush_to_bronze(watch_event_buffer, "WatchEvent")
        if len(pr_event_buffer) > 0:
            flush_to_bronze(pr_event_buffer, "PullRequestEvent")
        if len(issues_event_buffer) > 0:
            flush_to_bronze(issues_event_buffer, "IssuesEvent")

#%%
def flush_to_bronze(event_buffer, event_type):
    """
    Writes buffered events to a timestamped JSON file in the bronze directory.

    Args:
        event_buffer (deque): The buffer containing event dictionaries.
        event_type (str): Type of GitHub event (used in file naming and pathing).
    """
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        file_name = f"{event_type}_dump_{timestamp}.json"
        file_path = os.path.join(BASE_STORAGE_PATH, BRONZE_DIR, event_type, file_name)

        write_json_to_file(list(event_buffer), file_path)
        event_buffer.clear()

        print(f"[INFO] Flushed {event_type} buffer to {file_path}")
    except Exception as e:
        print(f"[ERROR] Failed to write JSON for {event_type}: {e}")

#%%
if __name__ == "__main__":
    # CLI for choosing between batch or live ingestion
    parser = argparse.ArgumentParser()
    parser.add_argument("--live", action="store_true", help="Run ingestion continuously")
    parser.add_argument("--duration", type=int, default=300, help="Duration in seconds if not live")

    args = parser.parse_args()

    fetch_github_events(duration=args.duration, live=args.live)

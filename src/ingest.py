#%%
import time
import requests
import os
from datetime import datetime
from collections import deque
from utils.defaults import (INTERESTED_TYPES, EVENT_DUMP_FILE,
                      BRONZE_DIR, BASE_STORAGE_PATH) 
import json
from utils.file_ops import write_json_to_file

#%%
watch_event_buffer = deque()
issues_event_buffer = deque()
pr_event_buffer = deque()
EVENT_THRESHOLD = 10
FETCH_INTERVAL_SECONDS = 10
RUN_DURATION = 300 

event_router = {
    "WatchEvent": watch_event_buffer,
    "PullRequestEvent": pr_event_buffer,
    "IssuesEvent": issues_event_buffer
}
#%%
def fetch_github_events(duration=RUN_DURATION):
    try:
        start_time = time.time()
        elapsed = time.time() - start_time
        while elapsed < duration:        
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
                    print(f"Buffer lengths | WatchEvent: {len(watch_event_buffer)}, PullRequestEvent: {len(pr_event_buffer)}, IssuesEvent: {len(issues_event_buffer)}")
                    if len(watch_event_buffer) >= EVENT_THRESHOLD:
                        flush_to_bronze(watch_event_buffer, "WatchEvent")
                    if len(pr_event_buffer) >= EVENT_THRESHOLD:
                        flush_to_bronze(pr_event_buffer, "PullRequestEvent")
                    if len(issues_event_buffer) >= EVENT_THRESHOLD:
                        flush_to_bronze(issues_event_buffer, "IssuesEvent")
                time.sleep(FETCH_INTERVAL_SECONDS)
                elapsed = time.time() - start_time
            except Exception as e:
                print(f"[ERROR] Failed to fetch: {e}")
    finally:
        print("[INFO] Fetching completed. Flushing remaining events...")
        if len(watch_event_buffer) > 0:
            flush_to_bronze(watch_event_buffer, "WatchEvent")
        if len(pr_event_buffer) > 0:
            flush_to_bronze(pr_event_buffer, "PullRequestEvent")
        if len(issues_event_buffer) > 0:
            flush_to_bronze(issues_event_buffer, "IssuesEvent")
    
        


def flush_to_bronze(event_buffer, event_type):
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
    fetch_github_events()

import os
from pathlib import Path

INTERESTED_TYPES = ["WatchEvent", "PullRequestEvent", "IssuesEvent"]
STORAGE_FOLDER= "data"
EVENT_DUMP_FILE = "events_dump"
BRONZE_DIR = "bronze"
SILVER_DIR = "silver"
BASE_STORAGE_PATH = os.path.join(os.getcwd(), STORAGE_FOLDER)
BASE_PATH=Path("src").resolve()
CONFIG_FOLDER= "config"
BASE_CONFIG_PATH=os.path.join(BASE_PATH, CONFIG_FOLDER)

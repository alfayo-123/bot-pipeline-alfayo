"""
schedule_config.py — Schedules run_pipeline.py daily at 2am.
Run this once on the server: python scheduler/schedule_config.py
It will keep running and trigger the pipeline every day at 2am.
"""

import schedule
import time
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))

from scheduler.run_pipeline import run_pipeline
from utils.logger import log_event

LOG_DIR = str(BASE / "logs")


def scheduled_run():
    log_event(LOG_DIR, "INFO", "scheduler",
              "ALL", "Scheduled 2am pipeline run triggered")
    run_pipeline()


# Schedule daily at 2am
schedule.every().day.at("02:00").do(scheduled_run)

log_event(LOG_DIR, "INFO", "scheduler",
          "ALL", "Scheduler started — pipeline will run daily at 02:00")

print("✅ Scheduler running — pipeline executes daily at 2:00 AM")
print("   Press Ctrl+C to stop")

while True:
    schedule.run_pending()
    time.sleep(60)

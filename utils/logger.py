"""
logger.py — Audit logging utility
Used by every pipeline file starting from Phase 3.
Format: TIMESTAMP | LEVEL | STAGE | INSTITUTION | MESSAGE
"""

import os
from datetime import datetime


def get_log_path(log_dir):
    today = datetime.now().strftime("%Y%m%d")
    return f"{log_dir}/pipeline_{today}.log"


def log_event(log_dir, level, stage, institution, message):
    os.makedirs(log_dir, exist_ok=True)
    log_path = get_log_path(log_dir)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{timestamp} | {level} | {stage} | {institution} | {message}\n"
    with open(log_path, "a") as f:
        f.write(line)

"""
backup.py — Creates Parquet backups before each pipeline run.
Protects against data loss if a run corrupts existing tables.
Works alongside Delta Lake Time Travel as a second recovery layer.
"""

import os
import shutil
from pathlib import Path
from datetime import datetime
from utils.logger import log_event


def backup_layer(source_path, backup_base, dataset_name, log_dir=None):
    """
    Creates a timestamped backup of a Delta table folder
    before the pipeline overwrites it.

    Args:
        source_path : path to the Delta table folder to back up
        backup_base : base folder where backups are stored
        dataset_name: name of the dataset (used in backup folder name)
        log_dir     : path to log folder (optional)
    """
    source = Path(source_path)
    if not source.exists():
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = Path(backup_base) / f"{dataset_name}_{timestamp}"

    try:
        shutil.copytree(source, backup_path)
        if log_dir:
            log_event(log_dir, "INFO", "backup",
                      dataset_name, f"Backup created at {backup_path}")
    except Exception as e:
        if log_dir:
            log_event(log_dir, "ERROR", "backup",
                      dataset_name, f"Backup failed: {str(e)}")


def cleanup_old_backups(backup_base, dataset_name, keep_last=3, log_dir=None):
    """
    Keeps only the last N backups for a dataset.
    Deletes older ones to save disk space.

    Args:
        backup_base : base folder where backups are stored
        dataset_name: name of the dataset
        keep_last   : number of most recent backups to keep
        log_dir     : path to log folder (optional)
    """
    backup_dir = Path(backup_base)
    if not backup_dir.exists():
        return

    backups = sorted([
        d for d in backup_dir.iterdir()
        if d.is_dir() and d.name.startswith(dataset_name)
    ])

    to_delete = backups[:-keep_last] if len(backups) > keep_last else []

    for old_backup in to_delete:
        try:
            shutil.rmtree(old_backup)
            if log_dir:
                log_event(log_dir, "INFO", "backup",
                          dataset_name, f"Deleted old backup: {old_backup.name}")
        except Exception as e:
            if log_dir:
                log_event(log_dir, "ERROR", "backup",
                          dataset_name, f"Failed to delete backup: {str(e)}")

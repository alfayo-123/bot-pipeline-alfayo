"""
folder_connector.py — Reads CSV/Excel files from a shared local folder.
Used when institutions drop files into a shared network drive or folder.
"""

import os
import pandas as pd
from pathlib import Path
from utils.logger import log_event


class FolderConnector:
    """
    Reads data files from a local or network shared folder.
    Supports CSV and Excel formats.
    """

    def __init__(self, folder_path, log_dir):
        self.folder_path = Path(folder_path)
        self.log_dir = log_dir

    def list_files(self, extensions=[".csv", ".xlsx", ".xls"]):
        """Returns list of all data files in the folder."""
        files = []
        for ext in extensions:
            files.extend(list(self.folder_path.glob(f"*{ext}")))
        log_event(self.log_dir, "INFO", "folder_connector",
                  "ALL", f"Found {len(files)} files in {self.folder_path}")
        return files

    def read_file(self, file_path):
        """Reads a single CSV or Excel file into a pandas DataFrame."""
        file_path = Path(file_path)
        try:
            if file_path.suffix == ".csv":
                df = pd.read_csv(file_path)
            else:
                df = pd.read_excel(file_path)

            log_event(self.log_dir, "INFO", "folder_connector",
                      "ALL", f"Read {len(df)} rows from {file_path.name}")
            return df, file_path.name

        except Exception as e:
            log_event(self.log_dir, "ERROR", "folder_connector",
                      "ALL", f"Failed to read {file_path.name}: {str(e)}")
            return None, file_path.name

    def read_all(self):
        """Reads all files in the folder and returns list of DataFrames."""
        results = []
        for file_path in self.list_files():
            df, name = self.read_file(file_path)
            if df is not None:
                results.append({"dataframe": df, "source_file": name,
                                 "source_type": "folder"})
        return results

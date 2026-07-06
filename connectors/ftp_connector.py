"""
ftp_connector.py — Reads files from FTP or SFTP servers.
Used when institutions upload files to a central FTP server.
"""

import os
import ftplib
import pandas as pd
from io import BytesIO
from pathlib import Path
from utils.logger import log_event


class FTPConnector:
    """
    Connects to an FTP or SFTP server and downloads data files.
    Credentials are read from environment variables via .env file.
    """

    def __init__(self, host, username, password, remote_dir, log_dir, port=21):
        self.host = host
        self.username = username
        self.password = password
        self.remote_dir = remote_dir
        self.log_dir = log_dir
        self.port = port

    def connect(self):
        """Establishes FTP connection."""
        try:
            ftp = ftplib.FTP()
            ftp.connect(self.host, self.port)
            ftp.login(self.username, self.password)
            ftp.cwd(self.remote_dir)
            log_event(self.log_dir, "INFO", "ftp_connector",
                      "ALL", f"Connected to FTP: {self.host}")
            return ftp
        except Exception as e:
            log_event(self.log_dir, "ERROR", "ftp_connector",
                      "ALL", f"FTP connection failed: {str(e)}")
            return None

    def list_files(self, ftp, extensions=[".csv", ".xlsx"]):
        """Lists all data files in the remote directory."""
        try:
            all_files = ftp.nlst()
            return [f for f in all_files
                    if any(f.endswith(ext) for ext in extensions)]
        except Exception as e:
            log_event(self.log_dir, "ERROR", "ftp_connector",
                      "ALL", f"Failed to list FTP files: {str(e)}")
            return []

    def download_file(self, ftp, filename):
        """Downloads a single file and returns as DataFrame."""
        try:
            buffer = BytesIO()
            ftp.retrbinary(f"RETR {filename}", buffer.write)
            buffer.seek(0)

            if filename.endswith(".csv"):
                df = pd.read_csv(buffer)
            else:
                df = pd.read_excel(buffer)

            log_event(self.log_dir, "INFO", "ftp_connector",
                      "ALL", f"Downloaded {filename}: {len(df)} rows")
            return df

        except Exception as e:
            log_event(self.log_dir, "ERROR", "ftp_connector",
                      "ALL", f"Failed to download {filename}: {str(e)}")
            return None

    def read_all(self):
        """Connects, lists files, downloads all and returns DataFrames."""
        ftp = self.connect()
        if not ftp:
            return []

        results = []
        for filename in self.list_files(ftp):
            df = self.download_file(ftp, filename)
            if df is not None:
                results.append({"dataframe": df, "source_file": filename,
                                 "source_type": "ftp"})
        ftp.quit()
        return results

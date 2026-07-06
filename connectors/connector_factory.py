"""
connector_factory.py — Auto-detects which connector to use.
Reads connector type from .env and returns the right connector.
This is the single entry point for all data ingestion.
"""

import os
from dotenv import load_dotenv
from connectors.folder_connector import FolderConnector
from connectors.ftp_connector import FTPConnector
from connectors.api_connector import APIConnector
from connectors.database_connector import DatabaseConnector
from connectors.email_connector import EmailConnector
from utils.logger import log_event

load_dotenv()


class ConnectorFactory:
    """
    Auto-detects and instantiates the correct connector
    based on SOURCE_TYPE in the .env file.

    Supported types:
        folder   — shared local/network folder
        ftp      — FTP or SFTP server
        api      — REST API endpoint
        database — Oracle, MySQL, PostgreSQL
        email    — Email inbox attachments
    """

    def __init__(self, log_dir):
        self.log_dir = log_dir
        self.source_type = os.getenv("SOURCE_TYPE", "folder").lower()

    def get_connector(self):
        """Returns the correct connector based on SOURCE_TYPE."""
        log_event(self.log_dir, "INFO", "connector_factory",
                  "ALL", f"Using connector type: {self.source_type}")

        if self.source_type == "folder":
            return FolderConnector(
                folder_path=os.getenv("FOLDER_PATH", "raw_data"),
                log_dir=self.log_dir
            )

        elif self.source_type == "ftp":
            return FTPConnector(
                host=os.getenv("FTP_HOST"),
                username=os.getenv("FTP_USERNAME"),
                password=os.getenv("FTP_PASSWORD"),
                remote_dir=os.getenv("FTP_REMOTE_DIR", "/"),
                log_dir=self.log_dir,
                port=int(os.getenv("FTP_PORT", 21))
            )

        elif self.source_type == "api":
            return APIConnector(
                base_url=os.getenv("API_BASE_URL"),
                log_dir=self.log_dir,
                api_key=os.getenv("API_KEY"),
                token=os.getenv("API_TOKEN")
            )

        elif self.source_type == "database":
            return DatabaseConnector(
                db_type=os.getenv("DB_TYPE", "mysql"),
                host=os.getenv("DB_HOST"),
                port=int(os.getenv("DB_PORT", 3306)),
                database=os.getenv("DB_NAME"),
                username=os.getenv("DB_USERNAME"),
                password=os.getenv("DB_PASSWORD"),
                log_dir=self.log_dir
            )

        elif self.source_type == "email":
            return EmailConnector(
                host=os.getenv("EMAIL_HOST"),
                username=os.getenv("EMAIL_USERNAME"),
                password=os.getenv("EMAIL_PASSWORD"),
                log_dir=self.log_dir,
                port=int(os.getenv("EMAIL_PORT", 993))
            )

        else:
            log_event(self.log_dir, "ERROR", "connector_factory",
                      "ALL", f"Unknown SOURCE_TYPE: {self.source_type}")
            raise ValueError(f"Unknown SOURCE_TYPE: {self.source_type}")

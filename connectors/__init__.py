"""
connectors/__init__.py
Exports all connector classes for easy importing.
"""
from connectors.folder_connector import FolderConnector
from connectors.ftp_connector import FTPConnector
from connectors.api_connector import APIConnector
from connectors.database_connector import DatabaseConnector
from connectors.email_connector import EmailConnector
from connectors.connector_factory import ConnectorFactory

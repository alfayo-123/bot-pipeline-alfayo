"""
database_connector.py — Reads data from Oracle, MySQL or PostgreSQL.
Used when institutions expose their data via a database connection.
Security: credentials stored in .env file only — never in code.
"""

import pandas as pd
from utils.logger import log_event


class DatabaseConnector:
    """
    Connects to Oracle, MySQL or PostgreSQL databases.
    Reads tables or executes SQL queries and returns DataFrames.
    """

    def __init__(self, db_type, host, port, database,
                 username, password, log_dir):
        self.db_type = db_type.lower()
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.log_dir = log_dir
        self.connection = None

    def connect(self):
        """Establishes database connection based on db_type."""
        try:
            if self.db_type == "mysql":
                import mysql.connector
                self.connection = mysql.connector.connect(
                    host=self.host, port=self.port,
                    database=self.database,
                    user=self.username, password=self.password
                )
            elif self.db_type == "postgresql":
                import psycopg2
                self.connection = psycopg2.connect(
                    host=self.host, port=self.port,
                    dbname=self.database,
                    user=self.username, password=self.password
                )
            elif self.db_type == "oracle":
                import cx_Oracle
                dsn = cx_Oracle.makedsn(self.host, self.port,
                                        service_name=self.database)
                self.connection = cx_Oracle.connect(
                    self.username, self.password, dsn
                )

            log_event(self.log_dir, "INFO", "database_connector",
                      "ALL", f"Connected to {self.db_type} at {self.host}")
            return True

        except Exception as e:
            log_event(self.log_dir, "ERROR", "database_connector",
                      "ALL", f"DB connection failed: {str(e)}")
            return False

    def read_table(self, table_name, where_clause=None):
        """Reads a full table or filtered rows into a DataFrame."""
        try:
            query = f"SELECT * FROM {table_name}"
            if where_clause:
                query += f" WHERE {where_clause}"

            df = pd.read_sql(query, self.connection)
            log_event(self.log_dir, "INFO", "database_connector",
                      "ALL", f"Read {len(df)} rows from {table_name}")
            return df

        except Exception as e:
            log_event(self.log_dir, "ERROR", "database_connector",
                      "ALL", f"Failed to read {table_name}: {str(e)}")
            return None

    def read_query(self, sql_query):
        """Executes a custom SQL query and returns DataFrame."""
        try:
            df = pd.read_sql(sql_query, self.connection)
            log_event(self.log_dir, "INFO", "database_connector",
                      "ALL", f"Query returned {len(df)} rows")
            return df
        except Exception as e:
            log_event(self.log_dir, "ERROR", "database_connector",
                      "ALL", f"Query failed: {str(e)}")
            return None

    def disconnect(self):
        """Closes the database connection."""
        if self.connection:
            self.connection.close()
            log_event(self.log_dir, "INFO", "database_connector",
                      "ALL", "Database connection closed")

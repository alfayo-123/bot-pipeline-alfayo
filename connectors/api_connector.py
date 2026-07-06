"""
api_connector.py — Reads data from REST API endpoints.
Used when institutions expose their data via HTTP APIs.
"""

import requests
import pandas as pd
from utils.logger import log_event


class APIConnector:
    """
    Connects to a REST API endpoint and fetches data.
    Supports Bearer token and API key authentication.
    """

    def __init__(self, base_url, log_dir, api_key=None, token=None):
        self.base_url = base_url.rstrip("/")
        self.log_dir = log_dir
        self.headers = {"Content-Type": "application/json"}

        if token:
            self.headers["Authorization"] = f"Bearer {token}"
        if api_key:
            self.headers["X-API-Key"] = api_key

    def fetch(self, endpoint, params=None):
        """Fetches data from a single API endpoint."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        try:
            response = requests.get(url, headers=self.headers,
                                    params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict) and "data" in data:
                df = pd.DataFrame(data["data"])
            else:
                df = pd.DataFrame([data])

            log_event(self.log_dir, "INFO", "api_connector",
                      "ALL", f"Fetched {len(df)} rows from {url}")
            return df

        except Exception as e:
            log_event(self.log_dir, "ERROR", "api_connector",
                      "ALL", f"API fetch failed from {url}: {str(e)}")
            return None

    def read_all(self, endpoints):
        """
        Fetches data from multiple endpoints.

        Args:
            endpoints: list of dicts with keys:
                       endpoint, dataset_name, params (optional)
        """
        results = []
        for ep in endpoints:
            df = self.fetch(ep["endpoint"], ep.get("params"))
            if df is not None:
                results.append({
                    "dataframe": df,
                    "source_file": ep["dataset_name"],
                    "source_type": "api"
                })
        return results

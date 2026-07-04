#!/bin/bash
echo "Starting BoT ICN Data Pipeline..."

# Set Airflow home
export AIRFLOW_HOME=/app/airflow

# Start Airflow webserver in background
airflow webserver --port 8080 --daemon

# Start Airflow scheduler in background
airflow scheduler --daemon

# Start Streamlit dashboard
streamlit run dashboard/app.py --server.port 8501 --server.address 0.0.0.0

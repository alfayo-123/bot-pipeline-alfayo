# BoT Automated ICN Data Pipeline
# Base image: Python 3.11 slim
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install Java (required for Apache Spark)
RUN apt-get update && apt-get install -y \
    default-jdk \
    wget \
    curl \
    git \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$PATH:$JAVA_HOME/bin

# Copy requirements first (for Docker layer caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy all project files into the image
COPY . .

# Create necessary directories
RUN mkdir -p warehouse/bronze warehouse/silver warehouse/gold \
    warehouse/quarantine logs airflow/dags

# Copy DAG to Airflow dags folder
RUN cp dags/bot_pipeline_dag.py airflow/dags/

# Initialize Airflow database
RUN airflow db init

# Create Airflow admin user
RUN airflow users create \
    --username admin \
    --firstname BoT \
    --lastname Group16 \
    --role Admin \
    --email admin@bot.go.tz \
    --password BotPipeline2025!

# Expose ports
# 8080 = Airflow webserver
# 8501 = Streamlit dashboard
EXPOSE 8080 8501

# Copy startup script
COPY start.sh .
RUN chmod +x start.sh

# Start services
CMD ["./start.sh"]

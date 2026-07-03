"""
bot_pipeline_dag.py — Phase 7 Airflow Orchestration
Schedules the full BoT ICN data pipeline to run daily at 2am.
28 tasks across 7 datasets — Bronze, Validate, Transform, Gold per dataset.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, "/content/drive/MyDrive/bot_pipeline")

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql import functions as F

# ── Paths ──────────────────────────────────────────────────
BASE = "/content/drive/MyDrive/bot_pipeline"
RAW  = f"{BASE}/raw_data"
BRONZE = f"{BASE}/warehouse/bronze"
SILVER = f"{BASE}/warehouse/silver"
GOLD   = f"{BASE}/warehouse/gold"
QUARANTINE = f"{BASE}/warehouse/quarantine"

FILES = {
    "atm_distribution":  f"{RAW}/Geographical_ATM_distribution_2025.csv",
    "pos_distribution":  f"{RAW}/Geographical_merchant_POS_distribution_2025.csv",
    "internet_banking":  f"{RAW}/Internet_banking_transactions_2025.csv",
    "card_transactions": f"{RAW}/Local transactions by locally_issued_card_transactions_2025.csv",
    "mno_balances":      f"{RAW}/MNO_bank_trust_Acct_balances_2025.csv",
    "mobile_banking":    f"{RAW}/Mobile_banking_transactions.csv",
    "money_remittance":  f"{RAW}/Money_Remittance_transactions_2025.csv",
}

# ── Default args ────────────────────────────────────────────
default_args = {
    "owner": "BoT_Group16",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": False,
}

# ── Spark session factory ───────────────────────────────────
def get_spark():
    builder = SparkSession.builder \
        .appName("BoT_Pipeline_DAG") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    return configure_spark_with_delta_pip(builder).getOrCreate()

# ── Task functions ──────────────────────────────────────────
def run_bronze(dataset_name, **kwargs):
    from pipeline.ingestor import ingest_to_bronze
    spark = get_spark()
    result = ingest_to_bronze(spark, dataset_name, FILES[dataset_name], BRONZE)
    print(f"[Bronze] {dataset_name}: {result['bronze_rows']} rows ingested")

def run_validate(dataset_name, **kwargs):
    from pipeline.validator import VALIDATORS
    from pyspark.sql import functions as F
    spark = get_spark()
    df = spark.read.format("delta").load(f"{BRONZE}/{dataset_name}")
    flagged = VALIDATORS[dataset_name](df)
    silver_df = flagged.filter(F.col("_quarantine_reason") == "")
    quarantine_df = flagged.filter(F.col("_quarantine_reason") != "") \
        .withColumn("_quarantine_layer", F.lit("SILVER")) \
        .withColumn("_quarantine_time", F.current_timestamp())
    silver_df.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true").save(f"{SILVER}/{dataset_name}")
    quarantine_df.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true").save(f"{QUARANTINE}/{dataset_name}")
    print(f"[Validate] {dataset_name}: {silver_df.count()} Silver, {quarantine_df.count()} Quarantine")

def run_transform(dataset_name, **kwargs):
    from pipeline.transformer import transform_dataset, canonical_mapping
    from pipeline.deduplicator import deduplicate
    spark = get_spark()
    df = spark.read.format("delta").load(f"{SILVER}/{dataset_name}")
    transformed = transform_dataset(df, dataset_name, canonical_mapping[dataset_name])
    deduped, removed = deduplicate(transformed, dataset_name)
    deduped.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true").save(f"{SILVER}/{dataset_name}")
    print(f"[Transform] {dataset_name}: transformed and deduplicated, {removed} duplicates removed")

def run_gold(dataset_name, **kwargs):
    from pipeline.aggregator import AGGREGATORS
    spark = get_spark()
    df = spark.read.format("delta").load(f"{SILVER}/{dataset_name}")
    gold_df = AGGREGATORS[dataset_name](df) \
        .withColumn("_aggregated_at", F.current_timestamp()) \
        .withColumn("dataset_type", F.lit(dataset_name))
    gold_df.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true").save(f"{GOLD}/{dataset_name}")
    print(f"[Gold] {dataset_name}: {gold_df.count()} Gold rows saved")

# ── DAG definition ──────────────────────────────────────────
with DAG(
    dag_id="bot_icn_data_pipeline",
    default_args=default_args,
    description="BoT ICN Automated Data Pipeline — 7 datasets, 28 tasks",
    schedule_interval="0 2 * * *",
    catchup=False,
    tags=["BoT", "ICN", "pipeline"],
) as dag:

    datasets = list(FILES.keys())

    for dataset in datasets:
        bronze_task = PythonOperator(
            task_id=f"bronze_{dataset}",
            python_callable=run_bronze,
            op_kwargs={"dataset_name": dataset},
        )

        validate_task = PythonOperator(
            task_id=f"validate_{dataset}",
            python_callable=run_validate,
            op_kwargs={"dataset_name": dataset},
        )

        transform_task = PythonOperator(
            task_id=f"transform_{dataset}",
            python_callable=run_transform,
            op_kwargs={"dataset_name": dataset},
        )

        gold_task = PythonOperator(
            task_id=f"gold_{dataset}",
            python_callable=run_gold,
            op_kwargs={"dataset_name": dataset},
        )

        # Chain: bronze → validate → transform → gold
        bronze_task >> validate_task >> transform_task >> gold_task

"""
run_pipeline.py — Runs the full pipeline end to end.
Called by schedule_config.py daily at 2am.
Also callable manually: python scheduler/run_pipeline.py
"""

import os
import sys
import time
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))

from connectors.connector_factory import ConnectorFactory
from pipeline.ingestor import ingest_to_bronze
from pipeline.validator import VALIDATORS
from pipeline.transformer import transform_dataset, canonical_mapping
from pipeline.deduplicator import deduplicate
from pipeline.aggregator import AGGREGATORS
from utils.logger import log_event
from utils.metadata import save_metadata
from utils.backup import backup_layer

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip

# ── Paths ──────────────────────────────────────────────────
DATA_DIR   = BASE / "data"
BRONZE     = DATA_DIR / "bronze"
SILVER     = DATA_DIR / "silver"
GOLD       = DATA_DIR / "gold"
QUARANTINE = DATA_DIR / "quarantine"
LOG_DIR    = BASE / "logs"

DATASET_NAMES = [
    "atm_distribution", "pos_distribution", "internet_banking",
    "card_transactions", "mno_balances", "mobile_banking", "money_remittance"
]


def get_spark():
    builder = SparkSession.builder \
        .appName("BoT_Pipeline_Run") \
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    return configure_spark_with_delta_pip(builder).getOrCreate()


def run_pipeline():
    """Runs the complete Bronze → Silver → Gold pipeline."""
    start_time = time.time()
    run_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    log_event(str(LOG_DIR), "INFO", "scheduler",
              "ALL", f"Pipeline run started at {run_date}")

    spark = get_spark()

    # Step 1: Get data from connectors
    factory = ConnectorFactory(log_dir=str(LOG_DIR))
    connector = factory.get_connector()
    data_sources = connector.read_all()

    log_event(str(LOG_DIR), "INFO", "scheduler",
              "ALL", f"Connector returned {len(data_sources)} files")

    for source in data_sources:
        df_pandas = source["dataframe"]
        source_file = source["source_file"]
        dataset_name = detect_dataset_type(source_file)

        if not dataset_name:
            log_event(str(LOG_DIR), "WARNING", "scheduler",
                      "ALL", f"Could not detect dataset type for {source_file}")
            continue

        t0 = time.time()

        try:
            # Step 2: Bronze
            backup_layer(str(BRONZE / dataset_name), str(DATA_DIR / "backups"), dataset_name)
            df_spark = spark.createDataFrame(df_pandas)
            df_bronze = df_spark \
                .withColumn("_ingested_at", F.current_timestamp()) \
                .withColumn("_source_file", F.lit(source_file)) \
                .withColumn("_dataset_type", F.lit(dataset_name))
            df_bronze.write.format("delta").mode("append") \
                .option("mergeSchema", "true") \
                .save(str(BRONZE / dataset_name))

            # Step 3: Validate → Silver + Quarantine
            backup_layer(str(SILVER / dataset_name), str(DATA_DIR / "backups"), dataset_name)
            df_flagged = VALIDATORS[dataset_name](df_bronze)
            silver_df = df_flagged.filter(F.col("_quarantine_reason") == "")
            quarantine_df = df_flagged.filter(F.col("_quarantine_reason") != "") \
                .withColumn("_quarantine_layer", F.lit("SILVER")) \
                .withColumn("_quarantine_time", F.current_timestamp())

            silver_df.write.format("delta").mode("overwrite") \
                .option("overwriteSchema", "true") \
                .save(str(SILVER / dataset_name))
            quarantine_df.write.format("delta").mode("overwrite") \
                .option("overwriteSchema", "true") \
                .save(str(QUARANTINE / dataset_name))

            # Step 4: Transform + Deduplicate
            transformed = transform_dataset(silver_df, dataset_name,
                                            canonical_mapping[dataset_name])
            deduped, removed = deduplicate(transformed, dataset_name)
            deduped.write.format("delta").mode("overwrite") \
                .option("overwriteSchema", "true") \
                .save(str(SILVER / dataset_name))

            # Step 5: Gold
            backup_layer(str(GOLD / dataset_name), str(DATA_DIR / "backups"), dataset_name)
            gold_df = AGGREGATORS[dataset_name](deduped) \
                .withColumn("_aggregated_at", F.current_timestamp()) \
                .withColumn("dataset_type", F.lit(dataset_name))
            gold_df.write.format("delta").mode("overwrite") \
                .option("overwriteSchema", "true") \
                .save(str(GOLD / dataset_name))

            duration = time.time() - t0
            save_metadata(str(BASE), "full_pipeline", dataset_name,
                          "SUCCESS", deduped.count(), duration)
            log_event(str(LOG_DIR), "INFO", "scheduler", dataset_name,
                      f"Pipeline complete in {duration:.1f}s — "
                      f"Silver: {silver_df.count()} | "
                      f"Quarantine: {quarantine_df.count()} | "
                      f"Gold: {gold_df.count()} | "
                      f"Duplicates removed: {removed}")

        except Exception as e:
            duration = time.time() - t0
            save_metadata(str(BASE), "full_pipeline", dataset_name,
                          "FAILED", 0, duration, str(e))
            log_event(str(LOG_DIR), "ERROR", "scheduler", dataset_name,
                      f"Pipeline FAILED: {str(e)}")

    total_duration = time.time() - start_time
    log_event(str(LOG_DIR), "INFO", "scheduler",
              "ALL", f"Full pipeline completed in {total_duration:.1f}s")


def detect_dataset_type(filename):
    """Detects dataset type from filename keywords."""
    filename = filename.lower()
    if "atm" in filename:       return "atm_distribution"
    if "pos" in filename or "merchant" in filename: return "pos_distribution"
    if "internet" in filename:  return "internet_banking"
    if "card" in filename:      return "card_transactions"
    if "mno" in filename or "trust" in filename:    return "mno_balances"
    if "mobile" in filename:    return "mobile_banking"
    if "remit" in filename or "money" in filename:  return "money_remittance"
    return None


if __name__ == "__main__":
    run_pipeline()

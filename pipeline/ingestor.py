"""
ingestor.py — Phase 3 Bronze Layer
Reads all 7 raw BoT CSV files using Apache Spark and writes them
to Delta Lake exactly as received, with metadata columns added.
No cleaning or transformation happens here.
"""

import os
from pyspark.sql import functions as F


def ingest_to_bronze(spark, dataset_name, file_path, bronze_base):
    df = spark.read.csv(file_path, header=True, inferSchema=False)

    df_with_meta = df \
        .withColumn("_ingested_at", F.current_timestamp()) \
        .withColumn("_source_file", F.lit(os.path.basename(file_path))) \
        .withColumn("_dataset_type", F.lit(dataset_name))

    output_path = f"{bronze_base}/{dataset_name}"

    df_with_meta.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save(output_path)

    bronze_count = spark.read.format("delta").load(output_path).count()
    csv_count = df.count()

    return {
        "dataset": dataset_name,
        "csv_rows": csv_count,
        "bronze_rows": bronze_count,
        "match": csv_count == bronze_count,
    }


def run_all_bronze_ingestion(spark, files, bronze_base):
    results = []
    for name, path in files.items():
        print(f"Ingesting {name}...")
        result = ingest_to_bronze(spark, name, path, bronze_base)
        results.append(result)
        status = "PASS" if result["match"] else "FAIL"
        print(f"  [{status}] CSV rows: {result['csv_rows']} | Bronze rows: {result['bronze_rows']}")
    return results

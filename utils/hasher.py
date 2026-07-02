"""
hasher.py — SHA-256 one-way PII hashing utility
Used in transformer.py before saving sensitive columns to Silver.
"""

from pyspark.sql import functions as F


def hash_column(df, colname, new_colname=None):
    """
    Applies SHA-256 hashing to a column using Spark's built-in sha2 function.
    Returns a 64-character hex string. One-way - cannot be reversed.
    """
    target = new_colname if new_colname else colname
    return df.withColumn(target, F.sha2(F.col(colname).cast("string"), 256))

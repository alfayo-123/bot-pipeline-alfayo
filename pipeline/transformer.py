"""
transformer.py — Phase 5 Silver Layer Transformation
Renames columns to canonical schema, standardizes dates,
cleans text, casts numeric types, and hashes institution codes.
"""

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from utils.hasher import hash_column


def clean_numeric_value(colname):
    cleaned = F.trim(F.col(colname))
    cleaned = F.regexp_replace(cleaned, ",", "")
    cleaned = F.when(cleaned == "-", F.lit("0")).otherwise(cleaned)
    return cleaned.cast("double")


canonical_mapping = {
    "atm_distribution": {
        "INSTITUTIONCODE": "institution_id",
        "REPORTINGDATE": "date",
        "DESCRIPTIONNO": "description_no",
        "REGION": "region",
        "NUMBEROFATM": "number_of_atm",
        "TRANSACTIONVOLUME": "trans_volume",
        "AMOUNT_VALUE": "amount_value",
    },
    "pos_distribution": {
        "INSTITUTIONCODE": "institution_id",
        "REPORTINGDATE": "date",
        "DESCRIPTIONNO": "description_no",
        "REGION": "region",
        "FOREX_NO_OF_POS": "forex_no_of_pos",
        "FOREX_TRANS_VOLUME": "forex_trans_volume",
        "FOREX_AMOUNT_VALUE": "forex_amount_value",
        "LOCAL_NO_OF_POS": "local_no_of_pos",
        "LOCAL_TRANS_VOLUME": "local_trans_volume",
        "LOCAL_AMOUNT_VALUE": "local_amount_value",
    },
    "internet_banking": {
        "INSTITUTIONCODE": "institution_id",
        "REPORTINGDATE": "date",
        "DESCRIPTIONNO": "description_no",
        "CATEGORY": "category",
        "DOMESTIC_CUSTOMER": "domestic_customer",
        "DOMESTIC_VOLUME": "domestic_volume",
        "DOMESTIC_VALUE": "domestic_value",
        "INT_CUSTOMER": "int_customer",
        "INT_VOLUME": "int_volume",
        "INT_VALUE": "int_value",
    },
    "card_transactions": {
        "INSTITUTIONCODE": "institution_id",
        "REPORTINGDATE": "date",
        "DESCRIPTIONNO": "description_no",
        "CARD_TYPE": "card_type",
        "DOMESTIC_LIVE_CARD": "domestic_live_card",
        "DOMESTIC_ACTIVE_CARD": "domestic_active_card",
        "DOMESTIC_VOLUME": "domestic_volume",
        "DOMESTIC_VALUE": "domestic_value",
        "INT_LIVE_CARD": "int_live_card",
        "INT_ACTIVE_CARD": "int_active_card",
        "INT_VOLUME": "int_volume",
        "INT_VALUE": "int_value",
    },
    "mno_balances": {
        "INSTITUTIONCODE": "institution_id",
        "REPORTINGDATE": "date",
        "DESCRIPTIONNO": "description_no",
        "COMPANY": "company",
        "ACCOUNT_BALANCE": "account_balance",
        "INTEREST": "interest",
    },
    "mobile_banking": {
        "INSTITUTIONCODE": "institution_id",
        "REPORTINGDATE": "date",
        "DESCRIPTIONNO": "description_no",
        "CATEGORY": "category",
        "NUMBER_OF_CUSTOMER": "number_of_customer",
        "TRANS_VOLUME": "trans_volume",
        "TRANS_VALUE": "trans_value",
    },
    "money_remittance": {
        "INSTITUTIONCODE": "institution_id",
        "REPORTINGDATE": "date",
        "DESCRIPTIONNO": "description_no",
        "COMPANY": "company",
        "INFLOW_VOLUME": "inflow_volume",
        "INFLOW_VALUE": "inflow_value",
        "OUTFLOW_VOLUME": "outflow_volume",
        "OUTLOW_VALUE": "outflow_value",
    },
}


def transform_dataset(df, dataset_name, mapping):
    result = df

    result = result.withColumn(
        "_std_date",
        F.coalesce(
            F.to_date(F.col("REPORTINGDATE"), "M/d/yyyy"),
            F.to_date(F.col("REPORTINGDATE"), "yyyy-MM-dd")
        )
    )
    result = result.drop("REPORTINGDATE").withColumnRenamed("_std_date", "date")

    result = hash_column(result, "INSTITUTIONCODE", "institution_id_hash")

    for original, canonical in mapping.items():
        if original == "REPORTINGDATE":
            continue
        if original in result.columns:
            result = result.withColumnRenamed(original, canonical)

    if "description_no" in result.columns:
        result = result.withColumn("description_no", F.col("description_no").cast("int"))

    text_cols = ["region", "category", "company", "card_type"]
    for col in text_cols:
        if col in result.columns:
            result = result.withColumn(col, F.initcap(F.trim(F.col(col))))

    numeric_string_cols = {
        "atm_distribution": ["trans_volume", "amount_value"],
        "pos_distribution": ["forex_trans_volume", "forex_amount_value",
                             "local_trans_volume", "local_amount_value"],
        "internet_banking": ["domestic_customer", "domestic_volume", "domestic_value",
                             "int_customer", "int_volume", "int_value"],
        "mno_balances": ["account_balance", "interest"],
        "money_remittance": ["inflow_volume", "inflow_value", "outflow_value"],
    }

    if dataset_name in numeric_string_cols:
        for col in numeric_string_cols[dataset_name]:
            if col in result.columns:
                result = result.withColumn(
                    col, clean_numeric_value(col).cast(DecimalType(20, 2))
                )

    plain_int_cols = {
        "atm_distribution": ["number_of_atm"],
        "pos_distribution": ["forex_no_of_pos", "local_no_of_pos"],
        "card_transactions": ["domestic_live_card", "domestic_active_card",
                              "domestic_volume", "domestic_value",
                              "int_live_card", "int_active_card",
                              "int_volume", "int_value"],
        "mobile_banking": ["number_of_customer", "trans_volume", "trans_value"],
        "money_remittance": ["outflow_volume"],
    }

    if dataset_name in plain_int_cols:
        for col in plain_int_cols[dataset_name]:
            if col in result.columns:
                result = result.withColumn(
                    col, F.col(col).cast(DecimalType(20, 2))
                )

    result = result.withColumn("dataset_type", F.lit(dataset_name))

    return result

"""
validator.py — Phase 4 Silver Layer Validation
Applies 9 validation rules to Bronze data for all 7 BoT datasets,
splitting records into clean Silver and flagged Quarantine tables.
"""

from pyspark.sql import functions as F


def apply_common_rules(df, date_col="REPORTINGDATE"):
    """Applies rules common to all datasets: institution code, date, format, descriptionno."""
    flags = df

    flags = flags.withColumn(
        "_flag_rule1",
        F.when(
            F.col("INSTITUTIONCODE").isNull() | (F.trim(F.col("INSTITUTIONCODE")) == ""),
            F.lit("Null institution_id")
        ).otherwise(F.lit(""))
    )

    flags = flags.withColumn(
        "_parsed_date",
        F.coalesce(
            F.to_date(F.col(date_col), "M/d/yyyy"),
            F.to_date(F.col(date_col), "yyyy-MM-dd")
        )
    )
    flags = flags.withColumn(
        "_flag_rule2",
        F.when(F.col("_parsed_date").isNull(), F.lit("Invalid date"))
         .when(F.col("_parsed_date") > F.current_date(), F.lit("Future date"))
         .otherwise(F.lit(""))
    )

    flags = flags.withColumn(
        "_flag_rule7",
        F.when(
            ~F.col("INSTITUTIONCODE").rlike("^[A-Za-z]{3}-[0-9]{3}$"),
            F.lit("Invalid institution_id format")
        ).otherwise(F.lit(""))
    )

    flags = flags.withColumn(
        "_flag_rule8",
        F.when(
            F.col("DESCRIPTIONNO").cast("int") <= 0,
            F.lit("Invalid DESCRIPTIONNO")
        ).otherwise(F.lit(""))
    )

    return flags


def clean_numeric(colname):
    """Removes commas and dashes, returns cleaned double column expression."""
    cleaned = F.trim(F.col(colname))
    cleaned = F.regexp_replace(cleaned, ",", "")
    cleaned = F.when(cleaned == "-", F.lit("0")).otherwise(cleaned)
    return cleaned.cast("double")


def finalize_quarantine_reason(df, rule_cols):
    """Combines all rule flag columns into one clean _quarantine_reason column."""
    df = df.withColumn(
        "_quarantine_reason",
        F.concat_ws(" | ", *[F.col(c) for c in rule_cols])
    )
    df = df.withColumn(
        "_quarantine_reason",
        F.regexp_replace(F.col("_quarantine_reason"), r"^( \| )+|( \| )+$", "")
    )
    df = df.withColumn(
        "_quarantine_reason",
        F.regexp_replace(F.col("_quarantine_reason"), r"( \| ){2,}", " | ")
    )
    return df


def validate_atm(df):
    flags = apply_common_rules(df)
    flags = flags.withColumn("_clean_volume", clean_numeric("TRANSACTIONVOLUME"))
    flags = flags.withColumn("_clean_amount", clean_numeric("AMOUNT_VALUE"))
    flags = flags.withColumn(
        "_flag_rule6",
        F.when(F.col("_clean_volume") < 0, F.lit("Negative value in TRANSACTIONVOLUME"))
         .when(F.col("_clean_amount") < 0, F.lit("Negative value in AMOUNT_VALUE"))
         .when(F.col("_clean_volume").isNull(), F.lit("Non-numeric TRANSACTIONVOLUME"))
         .when(F.col("_clean_amount").isNull(), F.lit("Non-numeric AMOUNT_VALUE"))
         .otherwise(F.lit(""))
    )
    return finalize_quarantine_reason(flags, ["_flag_rule1", "_flag_rule2", "_flag_rule6", "_flag_rule7", "_flag_rule8"])


def validate_pos(df):
    flags = apply_common_rules(df)
    flags = flags.withColumn("_clean_forex_vol", clean_numeric("FOREX_TRANS_VOLUME"))
    flags = flags.withColumn("_clean_forex_amt", clean_numeric("FOREX_AMOUNT_VALUE"))
    flags = flags.withColumn("_clean_local_vol", clean_numeric("LOCAL_TRANS_VOLUME"))
    flags = flags.withColumn("_clean_local_amt", clean_numeric("LOCAL_AMOUNT_VALUE"))
    flags = flags.withColumn(
        "_flag_rule6",
        F.when(F.col("_clean_forex_vol") < 0, F.lit("Negative FOREX_TRANS_VOLUME"))
         .when(F.col("_clean_forex_amt") < 0, F.lit("Negative FOREX_AMOUNT_VALUE"))
         .when(F.col("_clean_local_vol") < 0, F.lit("Negative LOCAL_TRANS_VOLUME"))
         .when(F.col("_clean_local_amt") < 0, F.lit("Negative LOCAL_AMOUNT_VALUE"))
         .when(F.col("_clean_forex_vol").isNull(), F.lit("Non-numeric FOREX_TRANS_VOLUME"))
         .when(F.col("_clean_forex_amt").isNull(), F.lit("Non-numeric FOREX_AMOUNT_VALUE"))
         .when(F.col("_clean_local_vol").isNull(), F.lit("Non-numeric LOCAL_TRANS_VOLUME"))
         .when(F.col("_clean_local_amt").isNull(), F.lit("Non-numeric LOCAL_AMOUNT_VALUE"))
         .otherwise(F.lit(""))
    )
    return finalize_quarantine_reason(flags, ["_flag_rule1", "_flag_rule2", "_flag_rule6", "_flag_rule7", "_flag_rule8"])


def validate_internet(df):
    flags = apply_common_rules(df)
    flags = flags.withColumn("_clean_dom_vol", clean_numeric("DOMESTIC_VOLUME"))
    flags = flags.withColumn("_clean_dom_val", clean_numeric("DOMESTIC_VALUE"))
    flags = flags.withColumn("_clean_int_vol", clean_numeric("INT_VOLUME"))
    flags = flags.withColumn("_clean_int_val", clean_numeric("INT_VALUE"))
    flags = flags.withColumn(
        "_flag_rule6",
        F.when(F.col("_clean_dom_vol") < 0, F.lit("Negative DOMESTIC_VOLUME"))
         .when(F.col("_clean_dom_val") < 0, F.lit("Negative DOMESTIC_VALUE"))
         .when(F.col("_clean_int_vol") < 0, F.lit("Negative INT_VOLUME"))
         .when(F.col("_clean_int_val") < 0, F.lit("Negative INT_VALUE"))
         .when(F.col("_clean_dom_vol").isNull(), F.lit("Non-numeric DOMESTIC_VOLUME"))
         .when(F.col("_clean_dom_val").isNull(), F.lit("Non-numeric DOMESTIC_VALUE"))
         .when(F.col("_clean_int_vol").isNull(), F.lit("Non-numeric INT_VOLUME"))
         .when(F.col("_clean_int_val").isNull(), F.lit("Non-numeric INT_VALUE"))
         .otherwise(F.lit(""))
    )
    return finalize_quarantine_reason(flags, ["_flag_rule1", "_flag_rule2", "_flag_rule6", "_flag_rule7", "_flag_rule8"])


def validate_card(df):
    flags = apply_common_rules(df)
    numeric_cols = ["DOMESTIC_LIVE_CARD", "DOMESTIC_ACTIVE_CARD", "DOMESTIC_VOLUME", "DOMESTIC_VALUE",
                     "INT_LIVE_CARD", "INT_ACTIVE_CARD", "INT_VOLUME", "INT_VALUE"]
    for col in numeric_cols:
        flags = flags.withColumn(f"_clean_{col}", F.col(col).cast("double"))
    flag_expr = F.lit("")
    for col in numeric_cols:
        flag_expr = F.when(F.col(f"_clean_{col}") < 0, F.lit(f"Negative {col}")).otherwise(flag_expr)
    flags = flags.withColumn("_flag_rule6", flag_expr)
    return finalize_quarantine_reason(flags, ["_flag_rule1", "_flag_rule2", "_flag_rule6", "_flag_rule7", "_flag_rule8"])


def validate_mno(df):
    flags = apply_common_rules(df)
    flags = flags.withColumn("_clean_balance", clean_numeric("ACCOUNT_BALANCE"))
    flags = flags.withColumn("_clean_interest", clean_numeric("INTEREST"))
    flags = flags.withColumn("_is_balance_dash", F.trim(F.col("ACCOUNT_BALANCE")) == "-")
    flags = flags.withColumn("_is_interest_dash", F.trim(F.col("INTEREST")) == "-")
    flags = flags.withColumn(
        "_flag_rule9",
        F.when(
            F.col("_is_balance_dash") != F.col("_is_interest_dash"),
            F.lit("Inconsistent dash values between ACCOUNT_BALANCE and INTEREST")
        ).otherwise(F.lit(""))
    )
    flags = flags.withColumn(
        "_flag_rule6",
        F.when(F.col("_clean_balance") < 0, F.lit("Negative ACCOUNT_BALANCE"))
         .when(F.col("_clean_interest") < 0, F.lit("Negative INTEREST"))
         .when(F.col("_clean_balance").isNull(), F.lit("Non-numeric ACCOUNT_BALANCE"))
         .when(F.col("_clean_interest").isNull(), F.lit("Non-numeric INTEREST"))
         .otherwise(F.lit(""))
    )
    return finalize_quarantine_reason(flags, ["_flag_rule1", "_flag_rule2", "_flag_rule6", "_flag_rule7", "_flag_rule8", "_flag_rule9"])


def validate_mobile(df):
    flags = apply_common_rules(df, date_col="REPORTINGDATE")
    flags = flags.withColumn("_clean_vol", F.col("TRANS_VOLUME").cast("double"))
    flags = flags.withColumn("_clean_val", F.col("TRANS_VALUE").cast("double"))
    flags = flags.withColumn(
        "_flag_rule6",
        F.when(F.col("_clean_vol") < 0, F.lit("Negative TRANS_VOLUME"))
         .when(F.col("_clean_val") < 0, F.lit("Negative TRANS_VALUE"))
         .otherwise(F.lit(""))
    )
    return finalize_quarantine_reason(flags, ["_flag_rule1", "_flag_rule2", "_flag_rule6", "_flag_rule7", "_flag_rule8"])


def validate_remittance(df):
    flags = apply_common_rules(df)
    flags = flags.withColumn("_clean_inflow_vol", clean_numeric("INFLOW_VOLUME"))
    flags = flags.withColumn("_clean_inflow_val", clean_numeric("INFLOW_VALUE"))
    flags = flags.withColumn("_clean_outflow_vol", F.col("OUTFLOW_VOLUME").cast("double"))
    flags = flags.withColumn("_clean_outflow_val", clean_numeric("OUTLOW_VALUE"))
    flags = flags.withColumn(
        "_flag_rule6",
        F.when(F.col("_clean_inflow_vol") < 0, F.lit("Negative INFLOW_VOLUME"))
         .when(F.col("_clean_inflow_val") < 0, F.lit("Negative INFLOW_VALUE"))
         .when(F.col("_clean_outflow_vol") < 0, F.lit("Negative OUTFLOW_VOLUME"))
         .when(F.col("_clean_outflow_val") < 0, F.lit("Negative OUTFLOW_VALUE"))
         .when(F.col("_clean_inflow_vol").isNull(), F.lit("Non-numeric INFLOW_VOLUME"))
         .when(F.col("_clean_inflow_val").isNull(), F.lit("Non-numeric INFLOW_VALUE"))
         .when(F.col("_clean_outflow_val").isNull(), F.lit("Non-numeric OUTFLOW_VALUE"))
         .otherwise(F.lit(""))
    )
    return finalize_quarantine_reason(flags, ["_flag_rule1", "_flag_rule2", "_flag_rule6", "_flag_rule7", "_flag_rule8"])


VALIDATORS = {
    "atm_distribution": validate_atm,
    "pos_distribution": validate_pos,
    "internet_banking": validate_internet,
    "card_transactions": validate_card,
    "mno_balances": validate_mno,
    "mobile_banking": validate_mobile,
    "money_remittance": validate_remittance,
}

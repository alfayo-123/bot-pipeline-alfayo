"""
deduplicator.py — Phase 5 Silver Layer Deduplication
Removes duplicate rows from transformed Silver data
using composite primary key per dataset.
"""


PRIMARY_KEYS = {
    "atm_distribution":  ["institution_id", "date", "description_no"],
    "pos_distribution":  ["institution_id", "date", "description_no"],
    "internet_banking":  ["institution_id", "date", "description_no"],
    "card_transactions": ["institution_id", "date", "description_no"],
    "mno_balances":      ["institution_id", "date", "description_no"],
    "mobile_banking":    ["institution_id", "date", "description_no", "category"],
    "money_remittance":  ["institution_id", "date", "description_no"],
}


def deduplicate(df, dataset_name):
    """
    Removes duplicate rows using the composite primary key for the dataset.
    Returns deduplicated DataFrame and count of removed rows.
    """
    keys = PRIMARY_KEYS[dataset_name]
    before = df.count()
    deduped = df.dropDuplicates(keys)
    after = deduped.count()
    removed = before - after
    return deduped, removed

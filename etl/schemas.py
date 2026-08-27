"""
Expected structure for each Synthea table - what "valid" means for FR-3.1.
"""

# Columns that must be non-null for a row to be considered valid.
REQUIRED_COLUMNS = {
    "patients": ["Id", "BIRTHDATE", "SSN", "FIRST", "LAST"],
    "encounters": ["Id", "PATIENT", "START"],
    "conditions": ["PATIENT", "ENCOUNTER", "CODE"],
    "medications": ["PATIENT", "ENCOUNTER", "CODE"],
    "observations": ["PATIENT", "ENCOUNTER", "CODE"],
    "procedures": ["PATIENT", "ENCOUNTER", "CODE"],
}

# Column(s) used to identify and drop duplicate rows.
# Tables with an Id column use it directly; the rest dedupe on the full row.
DEDUPE_KEYS = {
    "patients": ["Id"],
    "encounters": ["Id"],
}


def get_required_columns(table: str) -> list[str]:
    return REQUIRED_COLUMNS.get(table, [])


def get_dedupe_keys(table: str):
    """Returns dedupe key columns, or None to mean 'dedupe on full row'."""
    return DEDUPE_KEYS.get(table)
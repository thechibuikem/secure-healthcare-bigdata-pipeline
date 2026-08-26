"""
Shared list of PHI (Protected Health Information) columns, per table.

Every other part of the pipeline (ETL, encryption, access control) imports
from here. This is the ONLY place PHI columns are defined - never redefine
this list anywhere else.
"""

PHI_FIELDS = {
    "patients": [
        "SSN",
        "FIRST",
        "LAST",
        "BIRTHDATE",
        "ADDRESS",
        "PHONE",
    ],
    "encounters": [
        # encounters link to a patient ID, which is itself sensitive
        "PATIENT",
    ],
    "conditions": [
        "PATIENT",
    ],
    "medications": [
        "PATIENT",
    ],
    "observations": [
        "PATIENT",
    ],
    "procedures": [
        "PATIENT",
    ],
}


def get_phi_fields(table: str) -> list[str]:
    """Return the list of PHI columns for a given table. Empty list if none."""
    return PHI_FIELDS.get(table, [])


def is_phi_field(table: str, column: str) -> bool:
    """Check whether a specific column on a table is considered PHI."""
    return column in PHI_FIELDS.get(table, [])
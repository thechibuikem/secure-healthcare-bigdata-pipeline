"""
Shared role -> permission mapping.

Defines what each role is allowed to see. The access control layer
(security/access_control.py) checks this before returning any data -
especially before decrypting any PHI column.
"""

ROLES = {
    "clinician": {
        "can_view_phi": True,
        "allowed_tables": "*",
    },
      "analyst": {
        "can_view_phi": False,
        "allowed_tables": "*",
    }
}

def role_exists(role: str) -> bool:
    return role in ROLES

def can_view_phi(role: str) -> bool:
    """Whether this role is allowed to see decrypted PHI at all."""
    if role not in ROLES:
        return False
    return ROLES[role]["can_view_phi"]

def can_access_table(role: str, table: str) -> bool:
    """Whether this role is allowed to query this table (PHI or not)."""
    if role not in ROLES:
        return False
    allowed = ROLES[role]["allowed_tables"]
    return allowed == "*" or table in allowed
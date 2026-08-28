"""
The ONLY sanctioned way to decrypt PHI. Everything else - ETL, aggregates,
the API - only ever writes or reads encrypted values. Decryption always
goes through here, so it can always be permission-checked and logged.
"""

from shared.config.roles import can_view_phi
from security.encryption import decrypt_value


def decrypt_if_allowed(role: str, value: str) -> str:
    """
    Returns the decrypted value if this role is allowed to see PHI,
    otherwise returns None. Never raises on a denied role - denial is
    a normal outcome here, not an error.
    """
    if not can_view_phi(role):
        return None
    return decrypt_value(value)
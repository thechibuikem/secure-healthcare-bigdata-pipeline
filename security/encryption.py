"""
Encrypt/decrypt PHI (Protected Health Information) field values. 
This is the single source of truth for all encryption logic in the pipeline:
- The ETL pipeline calls this to encrypt sensitive fields before writing data to the /curated zone.
- The access control layer (TASK-4) calls decrypt_value only through verified, permission-checked paths.
"""

import os
from cryptography.fernet import Fernet
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from shared.config.phi_fields import get_phi_fields


def get_key() -> bytes:
    """
    Retrieves and validates the secret Fernet encryption key from environment variables.
    
    Fernet requires the key to be in bytes format. If the environment variable 
    is missing, it raises a descriptive ValueError with instructions on how to generate one.
    """
    key = os.environ.get("ENCRYPTION_KEY")
    
    if not key:
        raise ValueError(
            "ENCRYPTION_KEY not set. Generate one with: "
            "python -c \"from cryptography.fernet import Fernet; "
            "print(Fernet.generate_key().decode())\" "
            "and put it in your .env file."
        )
        
    # Fernet expects a bytes object, so we encode the string key
    return key.encode()


def encrypt_value(value: str) -> str:
    """
    Encrypts a single plaintext string value using Fernet symmetric encryption.
    
    - Handles null/None values safely by returning None.
    - Converts arbitrary data types to string, encodes them to bytes for encryption, 
      and decodes the resulting token back to a string for Spark compatibility.
    """
    if value is None:
        return None
        
    # Initialize Fernet cipher suite with our secret key
    fernet = Fernet(get_key())
    
    # Convert value to string, encode to bytes, encrypt, then decode to string token
    return fernet.encrypt(str(value).encode()).decode()


def decrypt_value(token: str) -> str:
    """
    Decrypts a Fernet-encrypted ciphertext token back into its original plaintext value.
    
    - Handles null/None tokens safely.
    - Used strictly by authorized access-control layers (TASK-4).
    """
    if token is None:
        return None
        
    # Initialize Fernet cipher suite with our secret key
    fernet = Fernet(get_key())
    
    # Encode token back to bytes, decrypt, and decode back to a standard string
    return fernet.decrypt(token.encode()).decode()


def get_encrypt_udf():
    """
    Wraps the Python encrypt_value function into a PySpark User-Defined Function (UDF).
    
    This allows the encryption logic to be mapped across distributed rows 
    in a PySpark DataFrame, explicitly returning a StringType.
    """
    return udf(encrypt_value, StringType())


def encrypt_phi_columns(df, table: str):
    """
    Takes a PySpark DataFrame and a table name, returning a new DataFrame 
    where all designated PHI columns are fully encrypted.
    
    - Looks up configured PHI fields dynamically via get_phi_fields(table).
    - Safely checks if a PHI column exists in the DataFrame schema before applying the UDF to prevent runtime errors.
    - Leaves non-PHI columns untouched.
    """
    # Fetch the list of sensitive columns that require encryption for this specific table
    phi_columns = get_phi_fields(table)
    
    # Get the PySpark UDF instance
    encrypt_udf = get_encrypt_udf()

    # Iterate through each defined PHI column and apply encryption if present in the DataFrame
    for column in phi_columns:
        if column in df.columns:
            df = df.withColumn(column, encrypt_udf(df[column]))

    return df
import os
import sys
import pandas as pd
import hashlib
from sqlalchemy import create_engine, text, inspect
import pymysql
from urllib import parse
import urllib


# Database configuration and credentials - passing these credentials through encrypted file will be more secure
# --
    # I need to develop an encrypted file to include all these credentials
    # Also, I need to change the password to more ambigous one
# --

DB_USER = 'root'
DB_PASS = 'root@1234'
DB_HOST = 'localhost'
DB_NAME = 'HealthcareADT_DW'

if not all([DB_USER, DB_PASS, DB_HOST, DB_NAME]):
    # We assuming that we already have our credentials stored in .env file.
    print("Error: Database credentials are not set in .env file.")
    sys.exit(1)

# This step is to encode the password instead of being exposed
encoded_password = urllib.parse.quote(DB_PASS)

# Create a database connection engine
try:
    connection_string = f"mysql+pymysql://{DB_USER}:{encoded_password}@{DB_HOST}/{DB_NAME}"
    engine = create_engine(connection_string)
    print("Database connection engine created successfully.")
except Exception as e:
    print(f"Error creating database engine: {e}")
    sys.exit(1)

print(inspect(engine).get_table_names())
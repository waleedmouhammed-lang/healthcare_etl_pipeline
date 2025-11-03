import os
import sys
import pandas as pd
import hashlib
from sqlalchemy import create_engine, text
import urllib
from dotenv import load_dotenv



# Passing the server credentials form .env configuration file
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")

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


def extract_and_validate_csv(file_object): 
    # In this step we expect that the user will upload a file object
    # The file object will be passed directly from Streamlit's file uploader
    """
    Extracts data from CSV, validates, cleans, transforms, and
    creates a unique business key.
    """
    print(f"Starting extraction and validation from uploaded file...")
    
    try:
        df = pd.read_csv(file_object)
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return None


    # --- Amending the original dataset column names to fit the script names
    df.columns = [col.replace(" ", "_") for col in df.columns]

    # --- 1. Data Cleaning ---
    # Trim all string columns
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].str.strip()

    # Convert numeric and date columns, handling errors
    df['Billing_Amount'] = pd.to_numeric(
        df['Billing_Amount'].astype(str).str.replace(r'[$,]', '', regex=True),
        errors='coerce'  # Bad values become NaN (NULL)
    )
    df['Age'] = pd.to_numeric(df['Age'], errors='coerce')
    
    # Enforce date format
    df['Date_of_Admission'] = pd.to_datetime(
        df['Date_of_Admission'], 
        format='%m/%d/%Y',
        errors='coerce'  # Bad dates become NaT (NULL)
    )
    df['Discharge_Date'] = pd.to_datetime(
        df['Discharge_Date'], 
        format='%m/%d/%Y', 
        errors='coerce' # Bad dates become NaT (NULL)
    )

    # --- 2. Validation: Drop rows with critical missing data ---
    initial_rows = len(df)
    df.dropna(subset=['Name', 'Date_of_Admission'], inplace=True)
    if initial_rows > len(df):
        print(f"Dropped {initial_rows - len(df)} rows due to missing Name or Admission Date.")

    # --- 3. Deduplication (within the batch) ---
    # Remove fully duplicated rows
    initial_rows = len(df)
    df.drop_duplicates(inplace=True)
    if initial_rows > len(df):
        print(f"Dropped {initial_rows - len(df)} fully duplicate rows.")

    # Keep min age for rows duplicated on all other fields
    initial_rows = len(df)
    group_cols = [col for col in df.columns if col not in ['Age']]
    
    if group_cols:
        # We must re-aggregate all non-group columns
        agg_dict = {col: 'first' for col in df.columns if col not in group_cols}
        agg_dict['Age'] = 'min'
        
        df = df.groupby(group_cols, as_index=False).agg(agg_dict)

    if initial_rows > len(df):
        print(f"Consolidated {initial_rows - len(df)} rows based on minimum age.")

    # --- 4. Create the Unique Business Key (SourceAdmissionID) ---
    # This key uniquely identifies an admission event for idempotency.
    key_cols = ['Name', 'Date_of_Admission', 'Doctor', 'Hospital', 'Medical_Condition']
    
    # Create a stable composite key string
    df['composite_key'] = df[key_cols].fillna('').astype(str).apply(lambda x: '|'.join(x), axis=1)
    
    # Create a SHA-256 hash (64 chars)
    df['SourceAdmissionID'] = df['composite_key'].apply(
        lambda x: hashlib.sha256(x.encode()).hexdigest()
    )
    
    # Drop the temporary helper column
    df.drop(columns=['composite_key'], inplace=True)

    print(f"Validation complete. {len(df)} clean rows ready for staging.")
    return df


def load_to_staging(df, db_engine):
    """
    Loads the clean DataFrame into the Staging_Admissions table.
    This process is a full TRUNCATE and load of the *batch*.
    """
    print("Loading data to staging table...")
    try:
        with db_engine.connect() as conn:
            # We use a transaction for the staging load
            with conn.begin():
                conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
                conn.execute(text("TRUNCATE TABLE Staging_Admissions;"))
                conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))
                
                # Load the *cleaned* dataframe
                df.to_sql('Staging_Admissions', con=conn, if_exists='append', index=False)
        
        print(f"Successfully loaded {len(df)} rows to Staging_Admissions.")
    except Exception as e:
        print(f"Error loading to staging: {e}")
        raise


def run_data_warehouse_load(db_engine):
    """
    Executes the incremental stored procedure in MySQL.
    """
    print("Calling stored procedure sp_LoadDataWarehouse_Incremental...")
    try:
        with db_engine.connect() as conn:
            with conn.begin():
                # The stored procedure handles its own transaction
                conn.execute(text("CALL sp_LoadDataWarehouse_Incremental();"))
        
        print("Data warehouse load procedure executed successfully.")
    except Exception as e:
        print(f"Error running stored procedure: {e}")
        raise


# --- NEW MAIN FUNCTION ---
def main_etl_process(uploaded_file, db_engine):
    """
    Runs the full ETL pipeline for a given uploaded file.
    This function will be called by Streamlit.
    """
    try:
        # 1. Extract, Transform, Validate
        clean_df = extract_and_validate_csv(uploaded_file)
        
        if clean_df is not None and not clean_df.empty:
            # 2. Load to Staging
            load_to_staging(clean_df, db_engine)
            
            # 3. Load to Data Warehouse
            run_data_warehouse_load(db_engine)
            
            print("\nETL process completed successfully!")
            return True, f"Successfully loaded {len(clean_df)} new admission records."
        
        else:
            print("ETL process halted: No valid data to load.")
            return False, "ETL process halted: No valid data to load."

    except Exception as e:
        print(f"\nETL process FAILED: {e}")
        return False, f"ETL process FAILED: {e}"


# --- MODIFIED: The main execution block ---
# This part now only runs if you execute this file directly.
# It's left here for testing.
if __name__ == "__main__":
    
    csv_file_path = 'generated_admissions_batch_1.csv' 
    print(f"Running ETL in standalone mode for: {csv_file_path}")
    
    # We must create the engine here for standalone runs
    try:
        local_engine = create_engine(connection_string)
        with open(csv_file_path, 'rb') as f: # Open file in binary read mode
            success, message = main_etl_process(f, local_engine)
        print(message)
    except FileNotFoundError:
        print(f"Error: Test file not found at {csv_file_path}")
    except Exception as e:
        print(f"Error in standalone run: {e}")
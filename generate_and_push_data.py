import os
import sys
import pandas as pd
import hashlib
import random
from datetime import date, timedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

print("Starting daily data generation job...")

# --- 1. DATA GENERATION LOGIC ---
# (Re-using the functions we built to make test files)
first_names = ['John', 'Jane', 'Robert', 'Emily', 'Michael', 'Sarah', 'David', 'Laura']
last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller']
hospital_names = ['City', 'General', 'Mercy', 'St. Jude', 'Community']
hospital_suffixes = ['Hospital', 'Clinic', 'Medical Center']
blood_types = ['A+', 'A-', 'B+', 'B-', 'AB+', 'AB-', 'O+', 'O-']
medical_conditions = ['Diabetes', 'Cancer', 'Obesity', 'Arthritis', 'Hypertension']
insurance_providers = ['Medicare', 'Aetna', 'Blue Cross', 'Cigna', 'UnitedHealthcare']
admission_types = ['Urgent', 'Emergency', 'Elective']
medications = ['Paracetamol', 'Ibuprofen', 'Aspirin', 'Penicillin', 'Lipitor']
test_results = ['Normal', 'Abnormal', 'Inconclusive']
genders = ['Male', 'Female']

def get_random_date(start_date, end_date):
    days_between = (end_date - start_date).days
    random_days = random.randint(0, days_between)
    return start_date + timedelta(days=random_days)

def create_row():
    start_date = date(2023, 1, 1) # Generate data for the last couple of years
    end_date = date.today()
    admission_date = get_random_date(start_date, end_date)
    discharge_date = admission_date + timedelta(days=random.randint(1, 15))
    
    return {
        'Name': f"{random.choice(first_names)} {random.choice(last_names)}",
        'Age': random.randint(18, 90),
        'Gender': random.choice(genders),
        'Blood_Type': random.choice(blood_types),
        'Medical_Condition': random.choice(medical_conditions),
        'Date_of_Admission': admission_date, # Generate as date objects
        'Doctor': f"Dr. {random.choice(first_names)} {random.choice(last_names)}",
        'Hospital': f"{random.choice(hospital_names)} {random.choice(hospital_suffixes)}",
        'Insurance_Provider': random.choice(insurance_providers),
        'Billing_Amount': round(random.uniform(1000.0, 50000.0), 2),
        'Room_Number': str(random.randint(100, 599)), # As string
        'Admission_Type': random.choice(admission_types),
        'Discharge_Date': discharge_date, # As date objects
        'Medication': random.choice(medications),
        'Test_Results': random.choice(test_results)
    }

# --- 2. VALIDATION & KEY GENERATION ---
# (This is a simplified version of your et_pipeline.py logic)
def validate_and_key(df):
    print(f"Validating {len(df)} generated rows...")
    
    # Create the unique business key (SourceAdmissionID)
    key_cols = ['Name', 'Date_of_Admission', 'Doctor', 'Hospital', 'Medical_Condition']
    
    # Create a stable composite key string
    df['composite_key'] = df[key_cols].fillna('').astype(str).apply(lambda x: '|'.join(x), axis=1)
    
    # Create a SHA-256 hash (64 chars)
    df['SourceAdmissionID'] = df['composite_key'].apply(
        lambda x: hashlib.sha256(x.encode()).hexdigest()
    )
    df.drop(columns=['composite_key'], inplace=True)
    return df

# --- 3. DATABASE CONNECTION & LOAD ---
def connect_to_db():
    print("Loading database URL...")
    load_dotenv() # It will now look for NEON_DATABASE_URL
    
    DB_URL = os.getenv("NEON_DATABASE_URL")

    if not DB_URL:
        print("Error: NEON_DATABASE_URL not found. Halting.")
        sys.exit(1)
    
    print("Connecting to Neon database...")
    
    # The URL from Neon already includes all SSL requirements
    engine = create_engine(DB_URL)
    return engine

# ... (The rest of your generate_and_push_data.py is 100% THE SAME) ...

def load_to_staging(df, db_engine):
    print(f"Loading {len(df)} rows to staging table...")
    try:
        with db_engine.connect() as conn:
            with conn.begin():
                #conn.execute(text("TRUNCATE TABLE Staging_Admissions;"))
                # Load the cleaned dataframe
                df.to_sql('Staging_Admissions', con=conn, if_exists='replace', index=False)
        print("Staging load complete.")
    except Exception as e:
        print(f"Error loading to staging: {e}")
        raise

def run_data_warehouse_load(db_engine):
    print("Calling incremental load stored procedure...")
    try:
        with db_engine.connect() as conn:
            with conn.begin():
                conn.execute(text("CALL sp_LoadDataWarehouse_Incremental();"))
        print("Incremental load procedure executed successfully.")
    except Exception as e:
        print(f"Error running stored procedure: {e}")
        raise

# --- 4. MAIN EXECUTION ---
if __name__ == "__main__":
    
    # 1. Generate Data
    num_rows = 10000
    print(f"Generating {num_rows} new rows of data...")
    data = [create_row() for _ in range(num_rows)]
    main_df = pd.DataFrame(data)

    # 2. Validate and Create Keys
    clean_df = validate_and_key(main_df)
    
    try:
        # 3. Connect to DB
        db_engine = connect_to_db()
        
        # 4. Load to Staging
        load_to_staging(clean_df, db_engine)
        
        # 5. Load to Data Warehouse
        run_data_warehouse_load(db_engine)
        
        print(f"Successfully generated and loaded {num_rows} rows.")
        
    except Exception as e:
        print(f"ETL Job FAILED: {e}")
        sys.exit(1)

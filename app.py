import streamlit as st
from sqlalchemy import create_engine
import os
import urllib
from urllib import parse
import dotenv
from dotenv import load_dotenv

# Import the functions from your other script
from et_pipeline import main_etl_process

# --- Database Connection ---
# We need to create the engine here, in the app's main scope
# (Streamlit re-runs the script, so we use @st.cache_resource
#  to create the engine only once)
@st.cache_resource
def get_db_engine():
    """
    Creates and caches the database connection engine.
    """
    try:
        DB_USER = os.getenv("DB_USER")
        DB_PASS = os.getenv("DB_PASS")
        DB_HOST = os.getenv("DB_HOST")
        DB_NAME = os.getenv("DB_NAME")

        encoded_password = urllib.parse.quote(DB_PASS)
        
        connection_string = f"mysql+pymysql://{DB_USER}:{encoded_password}@{DB_HOST}/{DB_NAME}"
        engine = create_engine(connection_string)
        return engine
    except Exception as e:
        st.error(f"Failed to create database connection: {e}")
        return None

engine = get_db_engine()

# --- Page Configuration ---
st.set_page_config(
    page_title="Healthcare ETL Uploader",
    page_icon="🏥",
    layout="centered"
)

# --- The App UI ---
st.title("🏥 Healthcare Admissions ETL Uploader")

st.info("Upload a new batch of admissions data (CSV) to load into the Data Warehouse.")

uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

if uploaded_file is not None:
    st.success(f"File '{uploaded_file.name}' selected. Click 'Submit' to start the ETL process.")
    
    # The 'Submit' button
    if st.button("Submit and Run ETL"):
        
        if engine is None:
            st.error("Database connection is not available. Cannot run ETL.")
        else:
            # This is the "magic starts" part
            # It shows a spinner while the function runs
            with st.spinner("ETL process in progress... This may take a moment."):
                
                # Call your main ETL function
                success, message = main_etl_process(uploaded_file, engine)
            
            # Show the result
            if success:
                st.success(message)
                # st.balloons() # A little celebration
            else:
                st.error(message)

else:
    st.warning("Please upload a CSV file to begin.")
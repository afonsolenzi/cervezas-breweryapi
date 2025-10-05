import streamlit as st
import pandas as pd
import sqlite3

# Path to the database file created by your Airflow DAG
DB_FILE_PATH = "./include/data/brewery.db"

def load_data(table_name: str):
    """Function to load data from a specific table in the SQLite database."""
    try:
        conn = sqlite3.connect(DB_FILE_PATH)
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        conn.close()
        return df
    except Exception:
        # Return an empty DataFrame if the table doesn't exist yet
        return pd.DataFrame()

# --- Dashboard Layout ---
st.set_page_config(page_title="Brewery Dashboard", layout="wide")
st.title("🍺 Brewery Data Dashboard")

# --- Load All Data ---
raw_data = load_data("breweries")
clean_data = load_data("clean_breweries")
quality_data = load_data("data_quality_checks")

# --- Data Quality Section ---
st.subheader("📊 Data Quality Monitoring")

if not quality_data.empty:
    last_check_time = pd.to_datetime(quality_data['check_timestamp'].iloc[0]).strftime('%Y-%m-%d %H:%M:%S')
    st.info(f"Last DQ Check Run: **{last_check_time}**")

    for index, row in quality_data.iterrows():
        if row['passed']:
            st.success(f"✅ **{row['check_name']}:** PASS", icon="✅")
        else:
            st.error(f"❌ **{row['check_name']}:** FAIL", icon="❌")
        st.caption(row['description'])
    st.markdown("---")
else:
    st.warning("No data quality results found. Please run the Airflow DAG to generate them.")
    st.markdown("---")

# --- Main Dashboard Section with Tabs ---
if not raw_data.empty and not clean_data.empty:
    
    tab1, tab2 = st.tabs(["Clean Dataset ✨", "Raw Dataset (Unfiltered)"])

    # --- TAB 1: CLEAN DATA ---
    with tab1:
        st.header("Clean Brewery Data")
        st.info("This dataset contains only the records that pass all data quality checks.")
        
        # KPIs for Clean Data
        st.subheader("Key Metrics")
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Clean Breweries", len(clean_data))
        col2.metric("Number of States", clean_data['state'].nunique())
        col3.metric("Number of Brewery Types", clean_data['brewery_type'].nunique())

        # Charts for Clean Data
        st.subheader("Clean Breweries by Type")
        brewery_type_counts = clean_data['brewery_type'].value_counts()
        st.bar_chart(brewery_type_counts)
        
        # MAP FOR CLEAN DATA (MOVED HERE)
        st.subheader("Brewery Locations Map")
        map_data = clean_data.dropna(subset=['latitude', 'longitude'])
        if not map_data.empty:
            st.map(map_data)
        else:
            st.warning("No breweries with valid location data to display on the map.")

        # Data Table for Clean Data
        st.subheader("Clean Data Records")
        st.dataframe(clean_data)

    # --- TAB 2: RAW DATA ---
    with tab2:
        st.header("Raw Brewery Data")
        st.warning("This is the original, unfiltered dataset fetched from the API.")
        
        # KPIs for Raw Data
        st.subheader("Key Metrics")
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Raw Breweries", len(raw_data))
        col2.metric("Number of States", raw_data['state'].nunique())
        col3.metric("Number of Brewery Types", raw_data['brewery_type'].nunique())

        # Charts for Raw Data
        st.subheader("Raw Breweries by Type")
        brewery_type_counts = raw_data['brewery_type'].value_counts()
        st.bar_chart(brewery_type_counts)
        
        # Data Table for Raw Data
        st.subheader("Raw Data Records")
        st.dataframe(raw_data)

else:
    st.info("No brewery data found. Run the Airflow DAG to load data.")
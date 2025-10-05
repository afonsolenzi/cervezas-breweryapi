import json
import pandas as pd
import requests
import sqlite3
from airflow.decorators import dag, task
import pendulum

# Define the path for your SQLite database file
DB_FILE_PATH = "/usr/local/airflow/include/data/brewery.db"
API_URL = "https://api.openbrewerydb.org/v1/breweries"

# Define the accepted brewery types for consistency
ACCEPTED_BREWERY_TYPES = ['micro', 'nano', 'regional', 'brewpub', 'large', 'planning', 'bar', 'contract', 'proprietor', 'closed']

@dag(
    dag_id="brewery_api_to_sqlite_poc",
    start_date=pendulum.datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["poc", "api", "quality"],
)
def brewery_etl():
    """
    A POC DAG to fetch brewery data, run quality checks, clean the data, and save all results to SQLite.
    """

    @task
    def get_brewery_data() -> list[dict]:
        """
        Fetches all data from the brewery API by paginating through the results.
        """
        all_breweries = []
        page = 1
        per_page = 200
        while True:
            params = {"page": page, "per_page": per_page}
            response = requests.get(API_URL, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            if not data:
                break
            all_breweries.extend(data)
            page += 1
        print(f"Finished fetching data. Total breweries found: {len(all_breweries)}")
        return all_breweries

    @task
    def save_raw_data_to_sqlite(brewery_data: list[dict]):
        """Saves the raw fetched data into a SQLite database table."""
        if not brewery_data:
            print("No raw data to save.")
            return
        df = pd.DataFrame(brewery_data)
        conn = sqlite3.connect(DB_FILE_PATH)
        df.to_sql("breweries", conn, if_exists="replace", index=False)
        conn.close()
        print(f"Successfully saved {len(df)} raw records to 'breweries' table.")

    @task
    def run_data_quality_checks(brewery_data: list[dict]) -> list[dict]:
        """Performs data quality checks and returns the results."""
        if not brewery_data:
            return []
        df = pd.DataFrame(brewery_data)
        checks = []
        
        # 1. Null Check
        null_ids = df['id'].isnull().sum()
        checks.append({'check_name': 'ID Not Null', 'passed': int(null_ids == 0), 'description': f'Found {null_ids} null values in the id column.'})

        # 2. Uniqueness Check
        duplicate_ids = df.duplicated(subset=['id']).sum()
        checks.append({'check_name': 'ID is Unique', 'passed': int(duplicate_ids == 0), 'description': f'Found {duplicate_ids} duplicate ids.'})

        # 3. Range Check
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        valid_latitudes = df['latitude'].dropna()
        is_passed = valid_latitudes.between(-90, 90).all()
        if is_passed:
            description = 'Success: All valid latitudes are within the [-90, 90] range.'
        else:
            out_of_range_count = valid_latitudes[~valid_latitudes.between(-90, 90)].count()
            description = f'Found {out_of_range_count} latitudes outside the valid [-90, 90] range.'
        checks.append({'check_name': 'Latitude in Range', 'passed': int(is_passed), 'description': description})

        # 4. Accepted Values Check
        unexpected_types = df[~df['brewery_type'].isin(ACCEPTED_BREWERY_TYPES)]['brewery_type'].nunique()
        checks.append({'check_name': 'Brewery Type is Valid', 'passed': int(unexpected_types == 0), 'description': f'Found {unexpected_types} brewery types not in the accepted list.'})

        return checks

    @task
    def save_quality_results(quality_checks: list[dict]):
        """Saves the data quality check results to a new table."""
        if not quality_checks:
            return
        df_checks = pd.DataFrame(quality_checks)
        df_checks['check_timestamp'] = pendulum.now()
        conn = sqlite3.connect(DB_FILE_PATH)
        df_checks.to_sql("data_quality_checks", conn, if_exists="replace", index=False)
        conn.close()
        print(f"Successfully saved {len(df_checks)} quality checks.")
        
    @task
    def clean_and_save_data(brewery_data: list[dict]):
        """Removes rows that fail data quality checks and saves the result."""
        if not brewery_data:
            print("No data to clean.")
            return
            
        df = pd.DataFrame(brewery_data)
        
        # Apply cleaning rules
        # 1. Fix Null/Duplicate IDs
        clean_df = df.dropna(subset=['id']).drop_duplicates(subset=['id'])
        
        # 2. Fix Latitude Range
        clean_df['latitude'] = pd.to_numeric(clean_df['latitude'], errors='coerce')
        clean_df = clean_df[clean_df['latitude'].between(-90, 90) | clean_df['latitude'].isnull()]
        
        # 3. Fix Brewery Type
        clean_df = clean_df[clean_df['brewery_type'].isin(ACCEPTED_BREWERY_TYPES)]
        
        print(f"Original row count: {len(df)}. Cleaned row count: {len(clean_df)}.")
        
        # Save the clean dataframe to a new table
        conn = sqlite3.connect(DB_FILE_PATH)
        clean_df.to_sql("clean_breweries", conn, if_exists="replace", index=False)
        conn.close()
        print(f"Successfully saved {len(clean_df)} clean records to 'clean_breweries' table.")

    # Define task dependencies
    raw_data = get_brewery_data()
    save_raw_data_to_sqlite(raw_data)
    quality_results = run_data_quality_checks(raw_data)
    save_quality_results(quality_results)
    clean_and_save_data(raw_data)

brewery_etl()
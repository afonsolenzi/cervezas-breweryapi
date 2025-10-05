import sqlite3
import pandas as pd

# Make sure this path points to your local copy of the database file
DB_FILE_PATH = "./include/data/brewery.db"

# This query has been corrected (the "5;" at the end is removed)
query = """
SELECT
  id,
  name,
  brewery_type,
  city,
  state
FROM
  breweries
WHERE
  brewery_type NOT IN ('micro', 'nano', 'regional', 'brewpub', 'large', 'planning', 'bar', 'contract', 'proprietor', 'closed');
"""

try:
    # Connect to the database
    conn = sqlite3.connect(DB_FILE_PATH)

    # Use pandas to execute the query
    invalid_rows_df = pd.read_sql_query(query, conn)

    if not invalid_rows_df.empty:
        print("Found the following rows with brewery types not in the accepted list:")
        print(invalid_rows_df)
    else:
        print("Success: No rows with invalid brewery types were found in the 'breweries' table.")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Always close the connection
    if 'conn' in locals() and conn:
        conn.close()
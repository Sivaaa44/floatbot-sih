import duckdb
import os

# Initialize DuckDB connection
conn = duckdb.connect('argo_floats.db')

# Create tables and import data from CSVs
try:
    # Create and load FLOAT table with NULL handling
    conn.execute("""
        CREATE TABLE IF NOT EXISTS float (
            FLOAT_ID VARCHAR PRIMARY KEY,
            PLATFORM_NUMBER VARCHAR,
            PLATFORM_TYPE VARCHAR,
            PLATFORM_MAKER VARCHAR,
            FLOAT_SERIAL_NO VARCHAR,
            PROJECT_NAME VARCHAR,
            PI_NAME VARCHAR,
            LAUNCH_DATE VARCHAR,  -- Keep as VARCHAR initially
            LAUNCH_LATITUDE DOUBLE,
            LAUNCH_LONGITUDE DOUBLE,
            START_DATE VARCHAR,   -- Keep as VARCHAR initially
            END_MISSION_DATE VARCHAR,  -- Keep as VARCHAR initially
            BATTERY_TYPE VARCHAR,
            FIRMWARE_VERSION VARCHAR,
            DEPLOYMENT_PLATFORM VARCHAR,
            DEPLOYMENT_CRUISE_ID VARCHAR,
            FLOAT_OWNER VARCHAR,
            OPERATING_INSTITUTION VARCHAR,
            DATA_CENTRE VARCHAR,
            WMO_INST_TYPE VARCHAR
        )
    """)
    # Load data directly into float table
    conn.execute("COPY float FROM 'FLOAT.csv' (DELIMITER ',', HEADER TRUE, NULL_PADDING TRUE, QUOTE '\"', NULL '');")

    # Create and load PROFILES table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS profiles (
            FLOAT_ID VARCHAR,
            PROFILE_NUMBER INTEGER,
            CYCLE_NUMBER DOUBLE,
            JULD TIMESTAMP,
            LATITUDE DOUBLE,
            LONGITUDE DOUBLE,
            POSITION_QC VARCHAR,  -- Changed to VARCHAR to match data
            DIRECTION VARCHAR,
            DATA_MODE VARCHAR,
            PROFILE_PRES_QC VARCHAR,
            PROFILE_TEMP_QC VARCHAR,
            PROFILE_PSAL_QC VARCHAR,
            PRIMARY KEY (FLOAT_ID, PROFILE_NUMBER),
            FOREIGN KEY (FLOAT_ID) REFERENCES float(FLOAT_ID)
        )
    """)
    conn.execute("COPY profiles FROM 'PROFILES.csv' (DELIMITER ',', HEADER TRUE, NULL_PADDING TRUE, QUOTE '\"', NULL '');")

    # Create and load MEASUREMENTS table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS measurements (
            FLOAT_ID VARCHAR,
            PROFILE_NUMBER INTEGER,
            LEVEL INTEGER,
            PRES DOUBLE,
            TEMP DOUBLE,
            PSAL DOUBLE,
            PRES_QC VARCHAR,  -- Changed to VARCHAR to match data
            TEMP_QC VARCHAR,  -- Changed to VARCHAR to match data
            PSAL_QC VARCHAR,  -- Changed to VARCHAR to match data
            PRES_ADJUSTED DOUBLE,
            TEMP_ADJUSTED DOUBLE,
            PSAL_ADJUSTED DOUBLE,
            PRES_ADJUSTED_QC VARCHAR,
            TEMP_ADJUSTED_QC VARCHAR,
            PSAL_ADJUSTED_QC VARCHAR,
            FOREIGN KEY (FLOAT_ID, PROFILE_NUMBER) REFERENCES profiles(FLOAT_ID, PROFILE_NUMBER)
        )
    """)
    conn.execute("COPY measurements FROM 'MEASUREMENTS.csv' (DELIMITER ',', HEADER TRUE, NULL_PADDING TRUE, QUOTE '\"', NULL '');")

    print("Database setup completed successfully!")

except Exception as e:
    print(f"Error during database setup: {e}")

finally:
    conn.close()

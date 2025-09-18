from argopy import ArgoFloat, DataFetcher as Argo
import os
import pandas as pd
import numpy as np
import xarray as xr
from datetime import datetime
import json
import psycopg2
from psycopg2.extras import execute_batch
from sqlalchemy import create_engine, text
import warnings

# Database connection string
DATABASE_URL = "postgresql://neondb_owner:npg_TkEgrt0xJs6p@ep-misty-dew-a1a7stss-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

def create_database_tables(connection_string=DATABASE_URL):
    """
    Create the database tables if they don't exist
    """
    print("🔧 Setting up database tables...")
    
    schema_sql = """
    -- Argo Float Database Schema
    -- 3-table design: Floats -> Cycles -> Measurements

    -- 1. FLOATS TABLE (One record per float)
    CREATE TABLE IF NOT EXISTS floats (
        id SERIAL PRIMARY KEY,
        wmo_id INTEGER UNIQUE NOT NULL,
        platform_type VARCHAR(50),
        platform_maker VARCHAR(100),
        float_serial_no VARCHAR(50),
        project_name VARCHAR(100),
        pi_name VARCHAR(100),
        launch_date TIMESTAMP,
        launch_latitude REAL,
        launch_longitude REAL,
        end_mission_date TIMESTAMP,
        data_centre VARCHAR(10),
        operating_institution VARCHAR(100),
        deployment_platform VARCHAR(50),
        wmo_inst_type VARCHAR(10),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- 2. CYCLES TABLE (One record per profile/cycle)
    CREATE TABLE IF NOT EXISTS cycles (
        id SERIAL PRIMARY KEY,
        float_id INTEGER NOT NULL REFERENCES floats(id) ON DELETE CASCADE,
        float_wmo INTEGER NOT NULL,
        cycle_number INTEGER NOT NULL,
        profile_date TIMESTAMP,
        latitude REAL NOT NULL,
        longitude REAL NOT NULL,
        data_mode VARCHAR(1),
        direction VARCHAR(1),
        position_qc INTEGER,
        num_measurements INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(float_wmo, cycle_number)
    );

    -- 3. MEASUREMENTS TABLE (Multiple records per cycle)
    CREATE TABLE IF NOT EXISTS measurements (
        id SERIAL PRIMARY KEY,
        cycle_id INTEGER NOT NULL REFERENCES cycles(id) ON DELETE CASCADE,
        float_wmo INTEGER NOT NULL,
        cycle_number INTEGER NOT NULL,
        measurement_index INTEGER NOT NULL,
        pressure REAL,
        temperature REAL,
        salinity REAL,
        pressure_qc INTEGER,
        temperature_qc INTEGER,
        salinity_qc INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(float_wmo, cycle_number, measurement_index)
    );

    -- INDEXES for performance
    CREATE INDEX IF NOT EXISTS idx_floats_wmo ON floats(wmo_id);
    CREATE INDEX IF NOT EXISTS idx_floats_launch_date ON floats(launch_date);
    CREATE INDEX IF NOT EXISTS idx_floats_location ON floats(launch_latitude, launch_longitude);

    CREATE INDEX IF NOT EXISTS idx_cycles_float_id ON cycles(float_id);
    CREATE INDEX IF NOT EXISTS idx_cycles_wmo ON cycles(float_wmo);
    CREATE INDEX IF NOT EXISTS idx_cycles_date ON cycles(profile_date);
    CREATE INDEX IF NOT EXISTS idx_cycles_location ON cycles(latitude, longitude);
    CREATE INDEX IF NOT EXISTS idx_cycles_wmo_cycle ON cycles(float_wmo, cycle_number);

    CREATE INDEX IF NOT EXISTS idx_measurements_cycle_id ON measurements(cycle_id);
    CREATE INDEX IF NOT EXISTS idx_measurements_wmo ON measurements(float_wmo);
    CREATE INDEX IF NOT EXISTS idx_measurements_pressure ON measurements(pressure);
    CREATE INDEX IF NOT EXISTS idx_measurements_temperature ON measurements(temperature);
    CREATE INDEX IF NOT EXISTS idx_measurements_salinity ON measurements(salinity);
    CREATE INDEX IF NOT EXISTS idx_measurements_wmo_cycle ON measurements(float_wmo, cycle_number);
    """
    
    try:
        # Create engine for SQLAlchemy
        engine = create_engine(connection_string)
        
        with engine.connect() as conn:
            # Execute schema creation - split into individual statements
            statements = [stmt.strip() for stmt in schema_sql.split(';') if stmt.strip()]
            for statement in statements:
                if statement:
                    conn.execute(text(statement))
            conn.commit()
            
        print("✅ Database tables created successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Error creating database tables: {str(e)}")
        return False

def extract_float_metadata(float_wmo):
    """
    Extract float metadata for the Float table using ArgoFloat
    """
    print(f"\n=== Extracting metadata for Float {float_wmo} ===")
    
    try:
        # Create ArgoFloat instance
        af = ArgoFloat(float_wmo)
        
        # Load metadata
        meta_ds = af.open_dataset('meta')
        
        # Extract metadata for Float table
        float_metadata = {
            'wmo_id': int(float_wmo),
            'platform_type': str(meta_ds['PLATFORM_TYPE'].data).strip(),
            'platform_maker': str(meta_ds['PLATFORM_MAKER'].data).strip(),
            'float_serial_no': str(meta_ds['FLOAT_SERIAL_NO'].data).strip(),
            'project_name': str(meta_ds['PROJECT_NAME'].data).strip(),
            'pi_name': str(meta_ds['PI_NAME'].data).strip(),
            'launch_date': pd.to_datetime(str(meta_ds['LAUNCH_DATE'].data)),
            'launch_latitude': float(meta_ds['LAUNCH_LATITUDE'].data),
            'launch_longitude': float(meta_ds['LAUNCH_LONGITUDE'].data),
            'data_centre': str(meta_ds['DATA_CENTRE'].data).strip(),
            'operating_institution': str(meta_ds['OPERATING_INSTITUTION'].data).strip(),
            'deployment_platform': str(meta_ds['DEPLOYMENT_PLATFORM'].data).strip() if 'DEPLOYMENT_PLATFORM' in meta_ds else None,
            'wmo_inst_type': str(meta_ds['WMO_INST_TYPE'].data).strip(),
        }
        
        # Handle optional fields
        try:
            if 'END_MISSION_DATE' in meta_ds:
                end_date = str(meta_ds['END_MISSION_DATE'].data)
                if end_date and end_date.strip():
                    float_metadata['end_mission_date'] = pd.to_datetime(end_date)
                else:
                    float_metadata['end_mission_date'] = None
            else:
                float_metadata['end_mission_date'] = None
        except:
            float_metadata['end_mission_date'] = None
        
        print(f"✅ Float metadata extracted: {float_metadata['platform_type']} from {float_metadata['operating_institution']}")
        print(f"   Launch: {float_metadata['launch_date'].strftime('%Y-%m-%d')} at {float_metadata['launch_latitude']:.2f}°N, {float_metadata['launch_longitude']:.2f}°E")
        
        return float_metadata
        
    except Exception as e:
        print(f"❌ Error extracting metadata for float {float_wmo}: {str(e)}")
        return None

def insert_float_metadata(float_metadata, connection_string=DATABASE_URL):
    """
    Insert float metadata into the floats table
    """
    print("💾 Inserting float metadata into database...")
    
    try:
        engine = create_engine(connection_string)
        
        # Create DataFrame and insert
        float_df = pd.DataFrame([float_metadata])
        
        with engine.connect() as conn:
            # Try to insert, ignore if already exists
            float_df.to_sql('floats', conn, if_exists='append', index=False, method='multi')
            
            # Get the float ID
            result = conn.execute(
                text("SELECT id FROM floats WHERE wmo_id = :wmo_id"), 
                {"wmo_id": float_metadata['wmo_id']}
            )
            float_id = result.fetchone()[0]
            
        print(f"✅ Float metadata inserted successfully (ID: {float_id})")
        return float_id
        
    except Exception as e:
        if "duplicate key value" in str(e).lower():
            print(f"ℹ️  Float {float_metadata['wmo_id']} already exists in database")
            # Get existing float ID
            try:
                engine = create_engine(connection_string)
                with engine.connect() as conn:
                    result = conn.execute(
                        text("SELECT id FROM floats WHERE wmo_id = :wmo_id"), 
                        {"wmo_id": float_metadata['wmo_id']}
                    )
                    float_id = result.fetchone()[0]
                return float_id
            except:
                return None
        else:
            print(f"❌ Error inserting float metadata: {str(e)}")
            return None

def extract_and_insert_profile_data(float_wmo, float_id, connection_string=DATABASE_URL, batch_size=50):
    """
    Extract profile data and insert into cycles and measurements tables
    """
    print(f"\n=== Extracting and inserting profile data for Float {float_wmo} ===")
    
    try:
        # Use DataFetcher to get profile data
        ds = Argo().float(float_wmo).to_xarray()
        
        # Get unique cycles
        unique_cycles = np.unique(ds.CYCLE_NUMBER.values)
        unique_cycles = unique_cycles[~np.isnan(unique_cycles)]
        
        print(f"Found {len(unique_cycles)} profiles")
        
        engine = create_engine(connection_string)
        
        # First, insert all cycles
        print("📊 Inserting cycles...")
        cycles_data = []
        
        for i, cycle_num in enumerate(unique_cycles):
            cycle_num = int(cycle_num)
            
            # Filter data for this cycle
            cycle_mask = ds.CYCLE_NUMBER == cycle_num
            cycle_data = ds.where(cycle_mask, drop=True)
            
            if len(cycle_data.N_POINTS) == 0:
                continue
            
            # Extract cycle-level information - handle Julian day format
            try:
                # Argo uses Julian day format (days since 1950-01-01)
                julian_day = cycle_data.TIME.values[0]
                if not np.isnan(julian_day):
                    # Convert from Julian day to datetime
                    profile_time = pd.to_datetime('1950-01-01') + pd.Timedelta(days=float(julian_day))
                else:
                    profile_time = None
            except:
                profile_time = None
            
            cycle_info = {
                'float_id': float_id,
                'float_wmo': int(float_wmo),
                'cycle_number': cycle_num,
                'profile_date': profile_time,
                'latitude': float(cycle_data.LATITUDE.values[0]),
                'longitude': float(cycle_data.LONGITUDE.values[0]),
                'data_mode': str(cycle_data.DATA_MODE.values[0]),
                'direction': str(cycle_data.DIRECTION.values[0]),
                'position_qc': int(cycle_data.POSITION_QC.values[0]) if not np.isnan(cycle_data.POSITION_QC.values[0]) else None,
                'num_measurements': len(cycle_data.N_POINTS)
            }
            
            cycles_data.append(cycle_info)
            
            if (i + 1) % 20 == 0:
                print(f"  Progress: {i + 1}/{len(unique_cycles)} cycles prepared")
        
        # Insert all cycles at once
        cycles_df = pd.DataFrame(cycles_data)
        with engine.connect() as conn:
            cycles_df.to_sql('cycles', conn, if_exists='append', index=False, method='multi')
        print(f"✅ Inserted {len(cycles_data)} cycles")
        
        # Get cycle_id mapping
        print("🔗 Getting cycle ID mappings...")
        cycle_id_map = {}
        with engine.connect() as conn:
            for cycle in cycles_data:
                result = conn.execute(
                    text("SELECT id FROM cycles WHERE float_wmo = :float_wmo AND cycle_number = :cycle_number"),
                    {"float_wmo": cycle['float_wmo'], "cycle_number": cycle['cycle_number']}
                )
                cycle_id_result = result.fetchone()
                if cycle_id_result:
                    cycle_id_map[(cycle['float_wmo'], cycle['cycle_number'])] = cycle_id_result[0]
        
        print(f"✅ Mapped {len(cycle_id_map)} cycle IDs")
        
        # Now insert measurements in batches
        print("📊 Inserting measurements...")
        measurements_data = []
        total_measurements = 0
        
        for i, cycle_num in enumerate(unique_cycles):
            cycle_num = int(cycle_num)
            
            # Filter data for this cycle
            cycle_mask = ds.CYCLE_NUMBER == cycle_num
            cycle_data = ds.where(cycle_mask, drop=True)
            
            if len(cycle_data.N_POINTS) == 0:
                continue
            
            # Get cycle_id for this cycle
            cycle_id = cycle_id_map.get((int(float_wmo), cycle_num))
            if not cycle_id:
                continue
            
            # Extract measurements for this cycle
            pressure_sort_idx = np.argsort(cycle_data.PRES.values)
            
            cycle_measurements = []
            for idx in pressure_sort_idx:
                measurement = {
                    'cycle_id': cycle_id,
                    'float_wmo': int(float_wmo),
                    'cycle_number': cycle_num,
                    'measurement_index': int(idx),
                    'pressure': float(cycle_data.PRES.values[idx]) if not np.isnan(cycle_data.PRES.values[idx]) else None,
                    'temperature': float(cycle_data.TEMP.values[idx]) if not np.isnan(cycle_data.TEMP.values[idx]) else None,
                    'salinity': float(cycle_data.PSAL.values[idx]) if not np.isnan(cycle_data.PSAL.values[idx]) else None,
                    'pressure_qc': int(cycle_data.PRES_QC.values[idx]) if not np.isnan(cycle_data.PRES_QC.values[idx]) else None,
                    'temperature_qc': int(cycle_data.TEMP_QC.values[idx]) if not np.isnan(cycle_data.TEMP_QC.values[idx]) else None,
                    'salinity_qc': int(cycle_data.PSAL_QC.values[idx]) if not np.isnan(cycle_data.PSAL_QC.values[idx]) else None,
                }
                
                if any([measurement['pressure'] is not None, 
                       measurement['temperature'] is not None, 
                       measurement['salinity'] is not None]):
                    cycle_measurements.append(measurement)
            
            measurements_data.extend(cycle_measurements)
            total_measurements += len(cycle_measurements)
            
            # Insert measurements in batches
            if len(measurements_data) >= batch_size or i == len(unique_cycles) - 1:
                if measurements_data:
                    measurements_df = pd.DataFrame(measurements_data)
                    with engine.connect() as conn:
                        measurements_df.to_sql('measurements', conn, if_exists='append', index=False, method='multi')
                    
                    print(f"  ✅ Inserted batch of {len(measurements_data)} measurements")
                    measurements_data = []
            
            if (i + 1) % 20 == 0:
                print(f"  Progress: {i + 1}/{len(unique_cycles)} cycles processed")
        
        print(f"✅ Profile data insertion complete!")
        print(f"   Cycles inserted: {len(cycles_data)}")
        print(f"   Measurements inserted: {total_measurements}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error extracting/inserting profile data: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def clear_existing_float_data(float_wmo, connection_string=DATABASE_URL):
    """
    Clear existing data for a float (useful for re-processing)
    """
    print(f"🧹 Clearing existing data for float {float_wmo}...")
    
    try:
        engine = create_engine(connection_string)
        
        with engine.connect() as conn:
            # Delete in reverse order due to foreign key constraints
            conn.execute(text("DELETE FROM measurements WHERE float_wmo = :wmo_id"), {"wmo_id": float_wmo})
            conn.execute(text("DELETE FROM cycles WHERE float_wmo = :wmo_id"), {"wmo_id": float_wmo})
            conn.execute(text("DELETE FROM floats WHERE wmo_id = :wmo_id"), {"wmo_id": float_wmo})
            conn.commit()
            
        print(f"✅ Cleared existing data for float {float_wmo}")
        return True
        
    except Exception as e:
        print(f"❌ Error clearing data: {str(e)}")
        return False

def process_float_to_database(float_wmo, connection_string=DATABASE_URL, clear_existing=False):
    """
    Complete pipeline to process a float and insert all data into PostgreSQL
    """
    print(f"\n🌊 PROCESSING ARGO FLOAT {float_wmo} TO DATABASE 🌊")
    print("=" * 80)
    
    # Step 0: Clear existing data if requested
    if clear_existing:
        clear_existing_float_data(float_wmo, connection_string)
    
    # Step 1: Create tables if they don't exist
    if not create_database_tables(connection_string):
        return False
    
    # Step 2: Extract and insert float metadata
    float_metadata = extract_float_metadata(float_wmo)
    if not float_metadata:
        return False
    
    float_id = insert_float_metadata(float_metadata, connection_string)
    if not float_id:
        return False
    
    # Step 3: Extract and insert profile data
    success = extract_and_insert_profile_data(float_wmo, float_id, connection_string)
    
    if success:
        # Verify the data
        verify_data_insertion(float_wmo, connection_string)
    
    return success

def verify_data_insertion(float_wmo, connection_string=DATABASE_URL):
    """
    Verify that the data was inserted correctly
    """
    print(f"\n🔍 Verifying data insertion for Float {float_wmo}...")
    
    try:
        engine = create_engine(connection_string)
        
        with engine.connect() as conn:
            # Check floats table
            float_result = conn.execute(
                text("SELECT COUNT(*) FROM floats WHERE wmo_id = :wmo_id"), {"wmo_id": float_wmo}
            ).fetchone()
            
            # Check cycles table
            cycles_result = conn.execute(
                text("SELECT COUNT(*) FROM cycles WHERE float_wmo = :wmo_id"), {"wmo_id": float_wmo}
            ).fetchone()
            
            # Check measurements table
            measurements_result = conn.execute(
                text("SELECT COUNT(*) FROM measurements WHERE float_wmo = :wmo_id"), {"wmo_id": float_wmo}
            ).fetchone()
            
            # Get some sample data
            sample_query = """
            SELECT f.wmo_id, f.platform_type, f.launch_date,
                   COUNT(DISTINCT c.cycle_number) as num_cycles,
                   COUNT(m.id) as num_measurements,
                   MIN(c.profile_date) as first_profile,
                   MAX(c.profile_date) as last_profile
            FROM floats f
            LEFT JOIN cycles c ON f.id = c.float_id
            LEFT JOIN measurements m ON c.id = m.cycle_id
            WHERE f.wmo_id = :wmo_id
            GROUP BY f.wmo_id, f.platform_type, f.launch_date
            """
            
            sample_result = conn.execute(text(sample_query), {"wmo_id": float_wmo}).fetchone()
            
        print(f"✅ DATA VERIFICATION RESULTS:")
        print(f"   Floats: {float_result[0]} record")
        print(f"   Cycles: {cycles_result[0]} records")
        print(f"   Measurements: {measurements_result[0]} records")
        
        if sample_result:
            print(f"\n📊 FLOAT SUMMARY:")
            print(f"   WMO ID: {sample_result[0]}")
            print(f"   Platform: {sample_result[1]}")
            print(f"   Launch Date: {sample_result[2].strftime('%Y-%m-%d') if sample_result[2] else 'Unknown'}")
            print(f"   Profiles: {sample_result[3]}")
            print(f"   Total Measurements: {sample_result[4]}")
            print(f"   Mission Duration: {sample_result[5].strftime('%Y-%m-%d') if sample_result[5] else 'Unknown'} to {sample_result[6].strftime('%Y-%m-%d') if sample_result[6] else 'Unknown'}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error verifying data: {str(e)}")
        return False

def query_float_data(float_wmo, connection_string=DATABASE_URL):
    """
    Example function to query float data from the database
    """
    print(f"\n🔍 QUERYING FLOAT {float_wmo} DATA...")
    
    try:
        engine = create_engine(connection_string)
        
        # Example query: Get temperature profile for the latest cycle
        query = """
        SELECT c.cycle_number, c.profile_date, c.latitude, c.longitude,
               m.pressure, m.temperature, m.salinity
        FROM floats f
        JOIN cycles c ON f.id = c.float_id
        JOIN measurements m ON c.id = m.cycle_id
        WHERE f.wmo_id = :wmo_id
        AND c.profile_date = (
            SELECT MAX(profile_date) FROM cycles WHERE float_wmo = :wmo_id2
        )
        ORDER BY m.pressure
        LIMIT 20
        """
        
        df = pd.read_sql(text(query), engine, params={"wmo_id": float_wmo, "wmo_id2": float_wmo})
        
        if not df.empty:
            print(f"Latest profile data (first 20 measurements):")
            print(df.to_string(index=False))
        else:
            print("No data found")
            
        return df
        
    except Exception as e:
        print(f"❌ Error querying data: {str(e)}")
        return None

# Example usage
if __name__ == "__main__":
    # Process a single float
    float_wmo = 6903569
    
    # Process the float and insert into database (clear existing data first)
    success = process_float_to_database(float_wmo, clear_existing=True)
    
    if success:
        print(f"\n🎉 SUCCESS! Float {float_wmo} processed and inserted into database!")
        
        # Query some sample data
        sample_data = query_float_data(float_wmo)
        
        print(f"\n🔗 Your data is now available in your Neon PostgreSQL database!")
        print(f"   Connection: {DATABASE_URL.split('@')[1].split('/')[0]}")
        print(f"   Tables: floats, cycles, measurements")
        
    else:
        print(f"❌ Failed to process float {float_wmo}")
    
    # Example: Process multiple floats
    # float_list = [6903569, 6903570, 6903571]  # Add more float IDs
    # for wmo in float_list:
    #     print(f"\n" + "="*80)
    #     process_float_to_database(wmo)
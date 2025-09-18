from argopy import DataFetcher as Argo
import os
import pandas as pd
import numpy as np
import xarray as xr
from datetime import datetime

def analyze_profile_structure(ds):
    """
    Analyze the structure of a profile dataset
    """
    print(f"\n=== Profile Structure Analysis ===")
    print(f"Total measurements: {len(ds.N_POINTS)}")
    
    # Check unique cycles in this dataset
    unique_cycles = np.unique(ds.CYCLE_NUMBER.values)
    print(f"Unique cycles: {unique_cycles}")
    
    # Analyze pressure levels
    pressures = ds.PRES.values
    valid_pressures = pressures[~np.isnan(pressures)]
    print(f"Pressure range: {valid_pressures.min():.1f} to {valid_pressures.max():.1f} dbar")
    print(f"Valid pressure measurements: {len(valid_pressures)}")
    
    # Check coordinates
    lats = np.unique(ds.LATITUDE.values)
    lons = np.unique(ds.LONGITUDE.values)
    print(f"Unique positions: {len(lats)} latitudes, {len(lons)} longitudes")
    if len(lats) == 1 and len(lons) == 1:
        print(f"Single position: {lats[0]:.4f}°N, {lons[0]:.4f}°E")
    
    return unique_cycles

def numpy_to_python(obj):
    """
    Convert numpy types to Python native types for JSON serialization
    """
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {key: numpy_to_python(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [numpy_to_python(item) for item in obj]
    else:
        return obj

def create_proper_profile_structure(ds, cycle_num):
    """
    Convert flattened argopy data to a proper profile structure
    """
    # Filter for this specific cycle
    cycle_mask = ds.CYCLE_NUMBER == cycle_num
    cycle_data = ds.where(cycle_mask, drop=True)
    
    if len(cycle_data.N_POINTS) == 0:
        return None
    
    # Sort by pressure (ascending - surface to depth)
    pressure_sort_idx = np.argsort(cycle_data.PRES.values)
    
    # Create a properly structured profile dataset
    profile_data = {}
    
    # Basic profile info (single values) - convert to Python types
    profile_data['cycle_number'] = int(cycle_data.CYCLE_NUMBER.values[0])
    profile_data['platform_number'] = int(cycle_data.PLATFORM_NUMBER.values[0])
    profile_data['latitude'] = float(cycle_data.LATITUDE.values[0])
    profile_data['longitude'] = float(cycle_data.LONGITUDE.values[0])
    
    # Try to decode time properly
    try:
        time_val = cycle_data.TIME.values[0]
        if not np.isnan(time_val):
            # Convert from Julian days or other format if needed
            profile_data['profile_datetime'] = pd.to_datetime(time_val, unit='s', origin='1970-01-01')
        else:
            profile_data['profile_datetime'] = None
    except:
        profile_data['profile_datetime'] = None
    
    profile_data['data_mode'] = str(cycle_data.DATA_MODE.values[0])
    profile_data['direction'] = str(cycle_data.DIRECTION.values[0])
    
    # Measurement arrays (sorted by pressure)
    measurements = {}
    for var in ['PRES', 'TEMP', 'PSAL', 'PRES_ERROR', 'TEMP_ERROR', 'PSAL_ERROR', 
                'PRES_QC', 'TEMP_QC', 'PSAL_QC']:
        if var in cycle_data:
            measurements[var] = cycle_data[var].values[pressure_sort_idx]
    
    # Count valid measurements - convert to Python int
    valid_temp = int(np.sum(~np.isnan(measurements.get('TEMP', []))))
    valid_psal = int(np.sum(~np.isnan(measurements.get('PSAL', []))))
    valid_pres = int(np.sum(~np.isnan(measurements.get('PRES', []))))
    
    profile_data['valid_measurements'] = {
        'temperature': valid_temp,
        'salinity': valid_psal,
        'pressure': valid_pres
    }
    
    return profile_data, measurements

def download_and_organize_float(float_id, base_dir="argo_data"):
    """
    Download float data and organize it properly for database ingestion
    """
    
    float_dir = os.path.join(base_dir, f"float_{float_id}")
    os.makedirs(float_dir, exist_ok=True)
    
    profiles_dir = os.path.join(float_dir, "profiles")
    os.makedirs(profiles_dir, exist_ok=True)
    
    print(f"\n=== Downloading and organizing float {float_id} ===")
    
    try:
        # Fetch complete dataset
        ds = Argo().float(float_id).to_xarray()
        
        # Analyze structure
        unique_cycles = analyze_profile_structure(ds)
        
        # Process each profile
        all_profiles_info = []
        
        for cycle in unique_cycles:
            print(f"\nProcessing cycle {cycle}...")
            
            profile_info, measurements = create_proper_profile_structure(ds, cycle)
            
            if profile_info is None:
                continue
            
            # Save individual profile data
            profile_file = os.path.join(profiles_dir, f"cycle_{cycle:03d}.json")
            
            # Prepare data for JSON serialization
            json_data = {
                'profile_info': profile_info.copy(),
                'measurements': {k: v.tolist() if isinstance(v, np.ndarray) else v 
                               for k, v in measurements.items()}
            }
            
            # Handle datetime serialization
            if profile_info['profile_datetime']:
                json_data['profile_info']['profile_datetime'] = profile_info['profile_datetime'].isoformat()
            
            import json
            with open(profile_file, 'w') as f:
                json.dump(json_data, f, indent=2)
            
            # Prepare summary for CSV
            summary_row = {
                'float_id': float_id,
                'cycle_number': profile_info['cycle_number'],
                'profile_date': profile_info['profile_datetime'].strftime('%Y-%m-%d %H:%M:%S') if profile_info['profile_datetime'] else None,
                'latitude': profile_info['latitude'],
                'longitude': profile_info['longitude'],
                'data_mode': profile_info['data_mode'],
                'direction': profile_info['direction'],
                'temp_measurements': profile_info['valid_measurements']['temperature'],
                'psal_measurements': profile_info['valid_measurements']['salinity'],
                'pres_measurements': profile_info['valid_measurements']['pressure'],
                'max_pressure': float(np.nanmax(measurements['PRES'])),
                'min_temp': float(np.nanmin(measurements['TEMP'])),
                'max_temp': float(np.nanmax(measurements['TEMP'])),
                'min_salinity': float(np.nanmin(measurements['PSAL'])),
                'max_salinity': float(np.nanmax(measurements['PSAL'])),
                'profile_file': profile_file
            }
            
            all_profiles_info.append(summary_row)
            
            print(f"  ✅ Cycle {cycle}: {summary_row['temp_measurements']} temp, {summary_row['psal_measurements']} salinity measurements")
        
        # Save summary CSV
        summary_df = pd.DataFrame(all_profiles_info)
        summary_file = os.path.join(float_dir, f"float_{float_id}_profiles_summary.csv")
        summary_df.to_csv(summary_file, index=False)
        
        # Save original NetCDF for reference
        original_file = os.path.join(float_dir, f"float_{float_id}_original.nc")
        ds.to_netcdf(original_file)
        
        print(f"\n=== Summary ===")
        print(f"Float {float_id}: {len(all_profiles_info)} profiles processed")
        print(f"Date range: {summary_df['profile_date'].min()} to {summary_df['profile_date'].max()}")
        print(f"Lat range: {summary_df['latitude'].min():.2f} to {summary_df['latitude'].max():.2f}")
        print(f"Lon range: {summary_df['longitude'].min():.2f} to {summary_df['longitude'].max():.2f}")
        print(f"Summary saved to: {summary_file}")
        
        return float_dir, summary_df
        
    except Exception as e:
        print(f"❌ Error processing float {float_id}: {str(e)}")
        import traceback
        traceback.print_exc()
        return None, None

def load_profile_data(profile_json_file):
    """
    Load a specific profile from JSON file
    """
    import json
    with open(profile_json_file, 'r') as f:
        data = json.load(f)
    
    profile_info = data['profile_info']
    measurements = data['measurements']
    
    # Convert back to numpy arrays
    for key in measurements:
        measurements[key] = np.array(measurements[key])
    
    return profile_info, measurements

def create_database_tables_sql(csv_file):
    """
    Generate SQL to create database tables from the CSV structure
    """
    df = pd.read_csv(csv_file)
    
    sql = """
-- Argo Float Profiles Table
CREATE TABLE IF NOT EXISTS argo_profiles (
    id SERIAL PRIMARY KEY,
    float_id INTEGER NOT NULL,
    cycle_number INTEGER NOT NULL,
    profile_date TIMESTAMP,
    latitude REAL NOT NULL,
    longitude REAL NOT NULL,
    data_mode VARCHAR(1),
    direction VARCHAR(1),
    temp_measurements INTEGER,
    psal_measurements INTEGER,
    pres_measurements INTEGER,
    max_pressure REAL,
    min_temp REAL,
    max_temp REAL,
    min_salinity REAL,
    max_salinity REAL,
    profile_file TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(float_id, cycle_number)
);

-- Index for common queries
CREATE INDEX IF NOT EXISTS idx_argo_float_id ON argo_profiles(float_id);
CREATE INDEX IF NOT EXISTS idx_argo_location ON argo_profiles(latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_argo_date ON argo_profiles(profile_date);
"""
    
    return sql

# Example usage
if __name__ == "__main__":
    float_id = 6903569
    base_dir = "argo_organized_data"
    
    # Download and organize
    float_dir, summary_df = download_and_organize_float(float_id, base_dir)
    
    if float_dir:
        # Generate SQL for database creation
        summary_file = os.path.join(float_dir, f"float_{float_id}_profiles_summary.csv")
        sql = create_database_tables_sql(summary_file)
        
        sql_file = os.path.join(float_dir, "create_tables.sql")
        with open(sql_file, 'w') as f:
            f.write(sql)
        
        print(f"\n✅ Database SQL saved to: {sql_file}")
        
        # Example: Load a specific profile
        import glob
        profile_files = glob.glob(os.path.join(float_dir, "profiles", "*.json"))
        if profile_files:
            print(f"\n=== Example: Loading first profile ===")
            profile_info, measurements = load_profile_data(profile_files[0])
            print(f"Profile {profile_info['cycle_number']} at {profile_info['latitude']:.2f}, {profile_info['longitude']:.2f}")
            print(f"Temperature range: {measurements['TEMP'].min():.2f} to {measurements['TEMP'].max():.2f}°C")
            print(f"Depth range: {measurements['PRES'].min():.1f} to {measurements['PRES'].max():.1f} dbar")

"""
Multi-Float Argo Data Extractor
Extracts data from multiple Argo floats and organizes into 3 CSV files:
1. FLOAT.csv - Float metadata
2. PROFILES.csv - Profile/cycle information per float
3. MEASUREMENTS.csv - Detailed measurement data per profile
"""

from argopy import gdacfs
import pandas as pd
import numpy as np
import xarray as xr
from datetime import datetime
import os

def safe_extract_value(data_array, index=None):
    """Safely extract values from xarray DataArray, handling different data types"""
    try:
        if index is not None:
            if hasattr(data_array, 'values') and len(data_array.shape) > 0:
                if isinstance(index, tuple):
                    return data_array.values[index]
                else:
                    return data_array.values[index]
            else:
                return None
        else:
            value = data_array.values
        
        # Handle different data types
        if hasattr(value, 'item'):
            value = value.item()
        
        # Convert bytes to string
        if isinstance(value, bytes):
            value = value.decode('utf-8').strip()
        elif isinstance(value, str):
            value = value.strip()
        elif isinstance(value, (np.ndarray, list)) and len(value) > 0:
            if isinstance(value[0], bytes):
                value = ''.join([b.decode('utf-8') if isinstance(b, bytes) else str(b) for b in value]).strip()
        
        # Handle NaN values
        if pd.isna(value) or (isinstance(value, str) and value == ''):
            return None
            
        return value
    except Exception as e:
        return None

def extract_float_metadata(fs, float_id, dac="coriolis"):
    """Extract float metadata from meta.nc file"""
    print(f"\n=== Extracting Float Metadata for {float_id} ===")
    
    meta_file_path = f"dac/{dac}/{float_id}/{float_id}_meta.nc"
    
    try:
        ds_meta = fs.open_dataset(meta_file_path)
        
        # Define the specific metadata columns we want
        metadata_columns = [
            'PLATFORM_NUMBER', 'PLATFORM_TYPE', 'PLATFORM_MAKER', 'FLOAT_SERIAL_NO',
            'PROJECT_NAME', 'PI_NAME', 'LAUNCH_DATE', 'LAUNCH_LATITUDE', 'LAUNCH_LONGITUDE',
            'START_DATE', 'END_MISSION_DATE', 'BATTERY_TYPE', 'FIRMWARE_VERSION',
            'DEPLOYMENT_PLATFORM', 'DEPLOYMENT_CRUISE_ID', 'FLOAT_OWNER', 'OPERATING_INSTITUTION',
            'DATA_CENTRE', 'DAC_FORMAT_ID', 'WMO_INST_TYPE', 'PLATFORM_FAMILY'
        ]
        
        float_metadata = {'FLOAT_ID': float_id}
        
        for var_name in metadata_columns:
            if var_name in ds_meta.variables:
                try:
                    var_data = ds_meta[var_name]
                    value = safe_extract_value(var_data)
                    float_metadata[var_name] = value
                except Exception as e:
                    print(f"Warning: Could not extract {var_name}: {e}")
                    float_metadata[var_name] = None
            else:
                float_metadata[var_name] = None
        
        ds_meta.close()
        print(f"✓ Float metadata extracted for {float_id}")
        return float_metadata
        
    except Exception as e:
        print(f"✗ Error extracting float metadata for {float_id}: {e}")
        return None

def extract_profile_data(fs, float_id, profile_number, dac="coriolis"):
    """Extract data from a single profile file"""
    profile_file_path = f"dac/{dac}/{float_id}/profiles/R{float_id}_{profile_number:03d}.nc"
    
    try:
        ds_profile = fs.open_dataset(profile_file_path)
        
        # Extract profile-level information
        profile_info = {
            'FLOAT_ID': float_id,
            'PROFILE_NUMBER': profile_number,
            'CYCLE_NUMBER': safe_extract_value(ds_profile.get('CYCLE_NUMBER', None)),
            'JULD': safe_extract_value(ds_profile.get('JULD', None)),
            'LATITUDE': safe_extract_value(ds_profile.get('LATITUDE', None)),
            'LONGITUDE': safe_extract_value(ds_profile.get('LONGITUDE', None)),
            'POSITION_QC': safe_extract_value(ds_profile.get('POSITION_QC', None)),
            'DIRECTION': safe_extract_value(ds_profile.get('DIRECTION', None)),
            'DATA_MODE': safe_extract_value(ds_profile.get('DATA_MODE', None)),
            'PROFILE_DOXY_QC': safe_extract_value(ds_profile.get('PROFILE_DOXY_QC', None)),
            'PROFILE_PRES_QC': safe_extract_value(ds_profile.get('PROFILE_PRES_QC', None)),
            'PROFILE_PSAL_QC': safe_extract_value(ds_profile.get('PROFILE_PSAL_QC', None)),
            'PROFILE_TEMP_QC': safe_extract_value(ds_profile.get('PROFILE_TEMP_QC', None))
        }
        
        # Extract measurement data
        measurements = []
        n_levels = ds_profile.dims.get('N_LEVELS', 0)
        
        # Key measurement variables
        measurement_vars = ['PRES', 'TEMP', 'PSAL', 'DOXY', 'PRES_QC', 'TEMP_QC', 'PSAL_QC', 'DOXY_QC',
                           'PRES_ADJUSTED', 'TEMP_ADJUSTED', 'PSAL_ADJUSTED', 'DOXY_ADJUSTED',
                           'PRES_ADJUSTED_QC', 'TEMP_ADJUSTED_QC', 'PSAL_ADJUSTED_QC', 'DOXY_ADJUSTED_QC']
        
        for level_idx in range(n_levels):
            measurement_row = {
                'FLOAT_ID': float_id,
                'PROFILE_NUMBER': profile_number,
                'LEVEL': level_idx
            }
            
            for var_name in measurement_vars:
                if var_name in ds_profile.variables:
                    var_data = ds_profile[var_name]
                    if 'N_LEVELS' in var_data.dims:
                        value = safe_extract_value(var_data, level_idx)
                        measurement_row[var_name] = value
                    else:
                        # Scalar variable, same for all levels
                        value = safe_extract_value(var_data)
                        measurement_row[var_name] = value
                else:
                    measurement_row[var_name] = None
            
            measurements.append(measurement_row)
        
        ds_profile.close()
        return profile_info, measurements
        
    except Exception as e:
        print(f"Warning: Could not extract profile {profile_number} for float {float_id}: {e}")
        return None, []

def process_multiple_floats(float_ids, dac="coriolis", max_profiles=10):
    """Process multiple floats and extract all data"""
    print("=== Multi-Float Argo Data Extractor ===")
    
    # Initialize file system
    fs = gdacfs("https://data-argo.ifremer.fr")
    
    # Storage for all data
    all_float_metadata = []
    all_profile_data = []
    all_measurements = []
    
    # Process each float
    for float_id in float_ids:
        print(f"\n{'='*60}")
        print(f"Processing Float: {float_id}")
        print(f"{'='*60}")
        
        # Extract float metadata
        float_metadata = extract_float_metadata(fs, float_id, dac)
        if float_metadata:
            all_float_metadata.append(float_metadata)
        
        # Extract profile data
        successful_profiles = 0
        for profile_num in range(1, max_profiles + 1):
            print(f"  Processing profile {profile_num}/{max_profiles}...")
            
            profile_info, measurements = extract_profile_data(fs, float_id, profile_num, dac)
            
            if profile_info:
                all_profile_data.append(profile_info)
                all_measurements.extend(measurements)
                successful_profiles += 1
                print(f"    ✓ Profile {profile_num}: {len(measurements)} measurements")
            else:
                print(f"    ✗ Profile {profile_num}: No data or file not found")
        
        print(f"Float {float_id} summary: {successful_profiles}/{max_profiles} profiles extracted")
    
    return all_float_metadata, all_profile_data, all_measurements

def save_to_csv(all_float_metadata, all_profile_data, all_measurements):
    """Save all extracted data to CSV files"""
    print(f"\n{'='*60}")
    print("Saving Data to CSV Files")
    print(f"{'='*60}")
    
    # Save FLOAT.csv
    if all_float_metadata:
        df_floats = pd.DataFrame(all_float_metadata)
        df_floats.to_csv("FLOAT.csv", index=False)
        print(f"✓ FLOAT.csv saved: {len(df_floats)} floats")
    else:
        print("✗ No float metadata to save")
    
    # Save PROFILES.csv
    if all_profile_data:
        df_profiles = pd.DataFrame(all_profile_data)
        df_profiles.to_csv("PROFILES.csv", index=False)
        print(f"✓ PROFILES.csv saved: {len(df_profiles)} profiles")
    else:
        print("✗ No profile data to save")
    
    # Save MEASUREMENTS.csv
    if all_measurements:
        df_measurements = pd.DataFrame(all_measurements)
        df_measurements.to_csv("MEASUREMENTS.csv", index=False)
        print(f"✓ MEASUREMENTS.csv saved: {len(df_measurements)} measurements")
    else:
        print("✗ No measurement data to save")
    
    return len(all_float_metadata), len(all_profile_data), len(all_measurements)

def main():
    """Main function to extract data from multiple floats"""
    # Float IDs to process
    float_ids = ["6903091", "6903092", "6903093"]


    
    # Maximum profiles per float
    max_profiles = 10
    
    print(f"Target floats: {float_ids}")
    print(f"Max profiles per float: {max_profiles}")
    
    # Process all floats
    all_float_metadata, all_profile_data, all_measurements = process_multiple_floats(
        float_ids, max_profiles=max_profiles
    )
    
    # Save to CSV files
    num_floats, num_profiles, num_measurements = save_to_csv(
        all_float_metadata, all_profile_data, all_measurements
    )
    
    # Final summary
    print(f"\n{'='*60}")
    print("EXTRACTION COMPLETE")
    print(f"{'='*60}")
    print(f"Floats processed: {num_floats}")
    print(f"Profiles extracted: {num_profiles}")
    print(f"Measurements recorded: {num_measurements}")
    print(f"\nFiles created:")
    print(f"  - FLOAT.csv ({num_floats} records)")
    print(f"  - PROFILES.csv ({num_profiles} records)")
    print(f"  - MEASUREMENTS.csv ({num_measurements} records)")
    
    if num_measurements > 0:
        avg_measurements_per_profile = num_measurements / num_profiles if num_profiles > 0 else 0
        print(f"\nAverage measurements per profile: {avg_measurements_per_profile:.1f}")

if __name__ == "__main__":
    main()
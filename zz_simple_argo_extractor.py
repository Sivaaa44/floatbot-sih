#!/usr/bin/env python3
"""
Simple Argo Float Data Extractor
Reads all 4 main Argo files and extracts data to CSV files
"""

from argopy import gdacfs
import pandas as pd
import numpy as np
import xarray as xr

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

def extract_metadata_simple(ds, float_id):
    """Extract specific metadata columns from meta.nc file"""
    print(f"\n=== Extracting Metadata for Float {float_id} ===")
    
    # Define the specific columns you want
    metadata_columns = [
        'PLATFORM_NUMBER', 'PLATFORM_TYPE', 'PLATFORM_MAKER', 'FLOAT_SERIAL_NO',
        'PROJECT_NAME', 'PI_NAME', 'LAUNCH_DATE', 'LAUNCH_LATITUDE', 'LAUNCH_LONGITUDE',
        'START_DATE', 'END_MISSION_DATE', 'BATTERY_TYPE', 'FIRMWARE_VERSION',
        'DEPLOYMENT_PLATFORM', 'DEPLOYMENT_CRUISE_ID', 'FLOAT_OWNER', 'OPERATING_INSTITUTION',
        'DATA_CENTRE', 'DAC_FORMAT_ID', 'WMO_INST_TYPE', 'PLATFORM_FAMILY'
    ]
    
    meta_data = []
    
    for var_name in metadata_columns:
        if var_name in ds.variables:
            try:
                var_data = ds[var_name]
                value = safe_extract_value(var_data)
                meta_data.append({
                    'variable': var_name,
                    'value': value,
                    'units': var_data.attrs.get('units', ''),
                    'long_name': var_data.attrs.get('long_name', ''),
                    'description': var_data.attrs.get('description', '')
                })
            except Exception as e:
                print(f"Warning: Could not extract {var_name}: {e}")
        else:
            print(f"Warning: Variable {var_name} not found in dataset")
    
    # Create DataFrame and save to CSV
    df_meta = pd.DataFrame(meta_data)
    df_meta.to_csv(f"{float_id}_metadata.csv", index=False)
    print(f"✓ Metadata saved to {float_id}_metadata.csv ({len(df_meta)} records)")
    
    return df_meta


def extract_profiles_simple(ds, float_id):
    """Extract profile data from prof.nc file"""
    print(f"\n=== Extracting Profile Data for Float {float_id} ===")
    
    profile_data = []
    
    # Get profile dimensions
    n_profiles = ds.dims.get('N_PROF', 0)
    n_levels = ds.dims.get('N_LEVELS', 0)
    print(f"Number of profiles: {n_profiles}")
    print(f"Number of levels per profile: {n_levels}")
    
    if n_profiles > 0 and n_levels > 0:
        # Extract key profile variables
        key_vars = ['CYCLE_NUMBER', 'JULD', 'LATITUDE', 'LONGITUDE', 'PRES', 'TEMP', 'PSAL',
                   'PRES_QC', 'TEMP_QC', 'PSAL_QC']
        
        for var_name in key_vars:
            if var_name in ds.variables:
                var_data = ds[var_name]
                
                if 'N_PROF' in var_data.dims and 'N_LEVELS' in var_data.dims:
                    # 2D variable (profile x level)
                    for prof_idx in range(min(n_profiles, 5)):  # Limit to first 5 profiles
                        for level_idx in range(min(n_levels, 50)):  # Limit to first 50 levels
                            value = safe_extract_value(var_data, (prof_idx, level_idx))
                            profile_data.append({
                                'profile': prof_idx,
                                'level': level_idx,
                                'variable': var_name,
                                'value': value,
                                'units': var_data.attrs.get('units', '')
                            })
                elif 'N_PROF' in var_data.dims:
                    # 1D variable (profile level)
                    for prof_idx in range(min(n_profiles, 5)):
                        value = safe_extract_value(var_data, prof_idx)
                        profile_data.append({
                            'profile': prof_idx,
                            'level': None,
                            'variable': var_name,
                            'value': value,
                            'units': var_data.attrs.get('units', '')
                        })
    
    # Create DataFrame and save to CSV
    df_prof = pd.DataFrame(profile_data)
    df_prof.to_csv(f"{float_id}_profiles.csv", index=False)
    print(f"✓ Profile data saved to {float_id}_profiles.csv ({len(df_prof)} records)")
    
    return df_prof

def extract_single_profile_complete(ds, float_id, profile_idx=0):
    """Extract complete data for a single profile"""
    print(f"\n=== Extracting Complete Profile {profile_idx} Data for Float {float_id} ===")
    
    # Select the specific profile
    profile = ds.isel(N_PROF=profile_idx)
    
    profile_detail_data = []
    
    # Get profile dimensions
    n_levels = ds.dims.get('N_LEVELS', 0)
    print(f"Profile {profile_idx} has {n_levels} levels")
    
    # Extract all variables for this profile
    for var_name in profile.variables:
        var_data = profile[var_name]
        
        if 'N_LEVELS' in var_data.dims:
            # Variable with depth levels
            for level_idx in range(min(n_levels, 100)):  # Limit to first 100 levels
                value = safe_extract_value(var_data, level_idx)
                profile_detail_data.append({
                    'level': level_idx,
                    'variable': var_name,
                    'value': value,
                    'units': var_data.attrs.get('units', ''),
                    'long_name': var_data.attrs.get('long_name', '')
                })
        else:
            # Scalar variable for this profile
            value = safe_extract_value(var_data)
            profile_detail_data.append({
                'level': None,
                'variable': var_name,
                'value': value,
                'units': var_data.attrs.get('units', ''),
                'long_name': var_data.attrs.get('long_name', '')
            })
    
    # Create DataFrame and save to CSV
    df_profile_detail = pd.DataFrame(profile_detail_data)
    df_profile_detail.to_csv(f"{float_id}_profile_{profile_idx}_complete.csv", index=False)
    print(f"✓ Individual profile data saved to {float_id}_profile_{profile_idx}_complete.csv ({len(df_profile_detail)} records)")
    
    return df_profile_detail

def main():
    """Main function to extract all Argo data"""
    print("=== Simple Argo Float Data Extractor ===")
    
    # Initialize file system
    fs = gdacfs("https://data-argo.ifremer.fr")
    
    # Float ID
    float_id = "6903091"
    dac = "coriolis"
    
    print(f"Analyzing float {float_id} from DAC {dac}")
    
    # File paths
    files = {
        'meta': f"dac/{dac}/{float_id}/{float_id}_meta.nc",
        'prof': f"dac/{dac}/{float_id}/{float_id}_prof.nc"
    }
    
    # Check which files exist
    print("\n=== Checking Available Files ===")
    available_files = {}
    for file_type, file_path in files.items():
        try:
            info = fs.info(file_path)
            available_files[file_type] = file_path
            print(f"✓ {file_type.upper()}: {file_path}")
        except Exception as e:
            print(f"✗ {file_type.upper()}: {file_path} - {e}")
    
    # Extract data from available files
    results = {}
    
    # 1. Metadata
    if 'meta' in available_files:
        try:
            ds_meta = fs.open_dataset(available_files['meta'])
            results['meta'] = extract_metadata_simple(ds_meta, float_id)
            ds_meta.close()
        except Exception as e:
            print(f"Error processing metadata: {e}")
    
    # 2. Profiles
    if 'prof' in available_files:
        try:
            ds_prof = fs.open_dataset(available_files['prof'])
            results['prof'] = extract_profiles_simple(ds_prof, float_id)
            
            # Extract individual profile data
            if ds_prof.dims.get('N_PROF', 0) > 0:
                results['profile_0'] = extract_single_profile_complete(ds_prof, float_id, 0)
            
            ds_prof.close()
        except Exception as e:
            print(f"Error processing profiles: {e}")
    
    # Summary
    print(f"\n=== Extraction Summary ===")
    print(f"Float ID: {float_id}")
    print(f"Files processed: {len(results)}")
    for data_type, df in results.items():
        print(f"  {data_type}: {len(df)} records")
    
    print(f"\n✓ Data extraction completed!")
    print(f"Files created:")
    for data_type in results.keys():
        print(f"  - {float_id}_{data_type}.csv")

if __name__ == "__main__":
    main()

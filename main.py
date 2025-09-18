from argopy import DataFetcher as Argo
import os
import pandas as pd
import numpy as np
import xarray as xr

def download_float_data(float_id, base_dir="argo_data"):
    """
    Download all profile data for a specific float and organize it properly
    """
    
    # Create directory structure
    float_dir = os.path.join(base_dir, f"float_{float_id}")
    os.makedirs(float_dir, exist_ok=True)
    
    profiles_dir = os.path.join(float_dir, "profiles")
    os.makedirs(profiles_dir, exist_ok=True)
    
    print(f"\n=== Downloading all profiles for float {float_id} ===")
    
    try:
        # Fetch all data for this float
        ds = Argo().float(float_id).to_xarray()
        
        print(f"Total data points: {len(ds.N_POINTS)}")
        
        # Get unique cycles to understand the profile structure
        cycles = np.unique(ds.CYCLE_NUMBER.values)
        print(f"Number of profiles (cycles): {len(cycles)}")
        
        # Save the complete dataset
        complete_file = os.path.join(float_dir, f"float_{float_id}_complete.nc")
        ds.to_netcdf(complete_file)
        print(f"✅ Saved complete dataset: {complete_file}")
        
        # Create a summary CSV for easy database ingestion later
        summary_data = []
        
        # Process each profile/cycle separately
        for cycle in cycles:
            cycle_data = ds.where(ds.CYCLE_NUMBER == cycle, drop=True)
            
            if len(cycle_data.N_POINTS) > 0:  # Skip empty cycles
                # Save individual profile
                profile_file = os.path.join(profiles_dir, f"profile_cycle_{cycle:03d}.nc")
                cycle_data.to_netcdf(profile_file)
                
                # Extract summary info for database
                summary_info = {
                    'float_id': float_id,
                    'cycle_number': int(cycle),
                    'profile_date': pd.to_datetime(cycle_data.TIME.values[0]).strftime('%Y-%m-%d %H:%M:%S'),
                    'latitude': float(cycle_data.LATITUDE.values[0]),
                    'longitude': float(cycle_data.LONGITUDE.values[0]),
                    'num_measurements': int(len(cycle_data.N_POINTS)),
                    'max_pressure': float(cycle_data.PRES.max().values),
                    'min_pressure': float(cycle_data.PRES.min().values),
                    'temp_range': f"{float(cycle_data.TEMP.min().values):.2f} to {float(cycle_data.TEMP.max().values):.2f}",
                    'psal_range': f"{float(cycle_data.PSAL.min().values):.2f} to {float(cycle_data.PSAL.max().values):.2f}",
                    'data_mode': cycle_data.DATA_MODE.values[0],
                    'file_path': profile_file
                }
                summary_data.append(summary_info)
        
        # Save summary CSV for database ingestion
        summary_df = pd.DataFrame(summary_data)
        summary_file = os.path.join(float_dir, f"float_{float_id}_summary.csv")
        summary_df.to_csv(summary_file, index=False)
        print(f"✅ Saved profile summary: {summary_file}")
        
        # Create a metadata file
        metadata = {
            'float_id': float_id,
            'total_profiles': len(cycles),
            'total_measurements': len(ds.N_POINTS),
            'date_range': f"{pd.to_datetime(ds.TIME.min().values).strftime('%Y-%m-%d')} to {pd.to_datetime(ds.TIME.max().values).strftime('%Y-%m-%d')}",
            'lat_range': f"{float(ds.LATITUDE.min().values):.2f} to {float(ds.LATITUDE.max().values):.2f}",
            'lon_range': f"{float(ds.LONGITUDE.min().values):.2f} to {float(ds.LONGITUDE.max().values):.2f}",
            'pressure_range': f"{float(ds.PRES.min().values):.1f} to {float(ds.PRES.max().values):.1f} dbar",
            'download_date': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'),
            'data_source': str(ds.attrs.get('Fetched_from', 'Unknown'))
        }
        
        metadata_file = os.path.join(float_dir, f"float_{float_id}_metadata.json")
        import json
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
        print(f"✅ Saved metadata: {metadata_file}")
        
        print(f"\n=== Summary for Float {float_id} ===")
        print(f"Total profiles: {len(cycles)}")
        print(f"Date range: {metadata['date_range']}")
        print(f"Geographic range: Lat {metadata['lat_range']}, Lon {metadata['lon_range']}")
        print(f"Pressure range: {metadata['pressure_range']}")
        
        return float_dir, summary_df
        
    except Exception as e:
        print(f"❌ Error downloading float {float_id}: {str(e)}")
        return None, None

def download_multiple_floats(float_ids, base_dir="argo_data"):
    """
    Download data for multiple floats
    """
    all_summaries = []
    successful_downloads = []
    
    for float_id in float_ids:
        float_dir, summary_df = download_float_data(float_id, base_dir)
        if float_dir and summary_df is not None:
            successful_downloads.append(float_id)
            summary_df['download_status'] = 'success'
            all_summaries.append(summary_df)
        else:
            # Add failed download to summary
            failed_summary = pd.DataFrame([{
                'float_id': float_id,
                'download_status': 'failed',
                'cycle_number': None,
                'profile_date': None,
                'latitude': None,
                'longitude': None
            }])
            all_summaries.append(failed_summary)
    
    # Create master summary
    if all_summaries:
        master_summary = pd.concat(all_summaries, ignore_index=True)
        master_file = os.path.join(base_dir, "all_floats_summary.csv")
        master_summary.to_csv(master_file, index=False)
        print(f"\n✅ Master summary saved: {master_file}")
        print(f"Successfully downloaded: {len(successful_downloads)} floats")
        
    return successful_downloads

# Example usage
if __name__ == "__main__":
    # Single float download
    float_id = 6903569  # Your Indian Ocean float
    base_dir = "argo_indian_ocean_data"
    
    # Download single float
    download_float_data(float_id, base_dir)
    
    # Example: Download multiple floats
    # float_ids = [6903569, 6903570, 6903571]  # Add more float IDs as needed
    # successful = download_multiple_floats(float_ids, base_dir)pip
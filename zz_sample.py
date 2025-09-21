#!/usr/bin/env python3
"""
Argo Data Structure Inspector
Analyzes the structure of Argo netCDF files to understand data organization
"""

from argopy import gdacfs
import pandas as pd
import numpy as np
import xarray as xr

def inspect_dataset_structure(ds, file_type):
    """Inspect and print the structure of an Argo dataset"""
    print(f"\n{'='*80}")
    print(f"INSPECTING {file_type.upper()} FILE STRUCTURE")
    print(f"{'='*80}")
    
    # Dataset dimensions
    print(f"\nDIMENSIONS:")
    for dim_name, dim_size in ds.dims.items():
        print(f"  {dim_name}: {dim_size}")
    
    # Dataset coordinates
    print(f"\nCOORDINATES:")
    for coord_name in ds.coords:
        coord = ds.coords[coord_name]
        print(f"  {coord_name}: {coord.dims}, shape={coord.shape}, dtype={coord.dtype}")
    
    # Dataset variables
    print(f"\nVARIABLES:")
    for var_name in sorted(ds.variables):
        if var_name not in ds.coords:  # Skip coordinates
            var = ds.variables[var_name]
            print(f"  {var_name}:")
            print(f"    dims: {var.dims}")
            print(f"    shape: {var.shape}")
            print(f"    dtype: {var.dtype}")
            if hasattr(var, 'attrs'):
                if 'long_name' in var.attrs:
                    print(f"    long_name: {var.attrs['long_name']}")
                if 'units' in var.attrs:
                    print(f"    units: {var.attrs['units']}")
    
    # Global attributes
    print(f"\nGLOBAL ATTRIBUTES:")
    for attr_name, attr_value in ds.attrs.items():
        if isinstance(attr_value, str) and len(attr_value) < 100:
            print(f"  {attr_name}: {attr_value}")
        elif isinstance(attr_value, (int, float)):
            print(f"  {attr_name}: {attr_value}")

def sample_variable_data(ds, var_name, max_samples=5):
    """Sample data from a variable to understand its structure"""
    if var_name not in ds.variables:
        print(f"Variable {var_name} not found")
        return
    
    var = ds.variables[var_name]
    print(f"\n--- SAMPLING {var_name} ---")
    print(f"Shape: {var.shape}, Dims: {var.dims}")
    
    try:
        if len(var.shape) == 0:
            # Scalar
            value = var.values
            print(f"Scalar value: {value} (type: {type(value)})")
        elif len(var.shape) == 1:
            # 1D array
            values = var.values
            print(f"1D array (first {max_samples}): {values[:max_samples]}")
            print(f"Data types in array: {set(type(x).__name__ for x in values[:max_samples] if x is not None)}")
        elif len(var.shape) == 2:
            # 2D array
            values = var.values
            print(f"2D array shape: {values.shape}")
            print(f"Sample from [0, :5]: {values[0, :max_samples]}")
            if values.shape[0] > 1:
                print(f"Sample from [1, :5]: {values[1, :max_samples]}")
        
        # Check for fill values or NaNs
        if hasattr(var, '_FillValue'):
            print(f"Fill value: {var._FillValue}")
        
    except Exception as e:
        print(f"Error sampling {var_name}: {e}")

def inspect_profile_file(fs, float_id, profile_num, dac="coriolis"):
    """Inspect a single profile file"""
    print(f"\n{'#'*80}")
    print(f"INSPECTING PROFILE FILE: R{float_id}_{profile_num:03d}.nc")
    print(f"{'#'*80}")
    
    profile_file_path = f"dac/{dac}/{float_id}/profiles/R{float_id}_{profile_num:03d}.nc"
    
    try:
        ds = fs.open_dataset(profile_file_path)
        
        # Basic structure
        inspect_dataset_structure(ds, "PROFILE")
        
        # Sample key variables
        key_vars = ['CYCLE_NUMBER', 'JULD', 'LATITUDE', 'LONGITUDE', 'PRES', 'TEMP', 'PSAL', 'DOXY']
        
        print(f"\n{'='*60}")
        print("SAMPLING KEY VARIABLES")
        print(f"{'='*60}")
        
        for var_name in key_vars:
            sample_variable_data(ds, var_name)
        
        ds.close()
        return True
        
    except Exception as e:
        print(f"Error inspecting profile {profile_num}: {e}")
        return False

def inspect_meta_file(fs, float_id, dac="coriolis"):
    """Inspect the metadata file"""
    print(f"\n{'#'*80}")
    print(f"INSPECTING META FILE: {float_id}_meta.nc")
    print(f"{'#'*80}")
    
    meta_file_path = f"dac/{dac}/{float_id}/{float_id}_meta.nc"
    
    try:
        ds = fs.open_dataset(meta_file_path)
        
        # Basic structure
        inspect_dataset_structure(ds, "METADATA")
        
        # Sample some metadata variables
        meta_vars = ['PLATFORM_NUMBER', 'LAUNCH_DATE', 'LAUNCH_LATITUDE', 'LAUNCH_LONGITUDE', 'PROJECT_NAME']
        
        print(f"\n{'='*60}")
        print("SAMPLING METADATA VARIABLES")
        print(f"{'='*60}")
        
        for var_name in meta_vars:
            sample_variable_data(ds, var_name)
        
        ds.close()
        return True
        
    except Exception as e:
        print(f"Error inspecting metadata: {e}")
        return False

def inspect_prof_file(fs, float_id, dac="coriolis"):
    """Inspect the prof.nc file (contains all profiles)"""
    print(f"\n{'#'*80}")
    print(f"INSPECTING PROF FILE: {float_id}_prof.nc")
    print(f"{'#'*80}")
    
    prof_file_path = f"dac/{dac}/{float_id}/{float_id}_prof.nc"
    
    try:
        ds = fs.open_dataset(prof_file_path)
        
        # Basic structure
        inspect_dataset_structure(ds, "PROF")
        
        # Sample key variables from the prof file
        prof_vars = ['CYCLE_NUMBER', 'JULD', 'LATITUDE', 'LONGITUDE', 'PRES', 'TEMP', 'PSAL']
        
        print(f"\n{'='*60}")
        print("SAMPLING PROF FILE VARIABLES")
        print(f"{'='*60}")
        
        for var_name in prof_vars:
            sample_variable_data(ds, var_name, max_samples=3)
        
        ds.close()
        return True
        
    except Exception as e:
        print(f"Error inspecting prof file: {e}")
        return False

def main():
    """Main inspection function"""
    print("ARGO DATA STRUCTURE INSPECTOR")
    print("="*80)
    
    # Initialize file system
    fs = gdacfs("https://data-argo.ifremer.fr")
    
    # Test with one float first
    float_id = "6903091"
    dac = "coriolis"
    
    print(f"Analyzing float {float_id} from DAC {dac}")
    
    # 1. Inspect metadata file
    print("\n" + "🔍 " * 20)
    print("STEP 1: METADATA FILE INSPECTION")
    print("🔍 " * 20)
    inspect_meta_file(fs, float_id, dac)
    
    # 2. Inspect prof file (if exists)
    print("\n" + "🔍 " * 20)
    print("STEP 2: PROF FILE INSPECTION")
    print("🔍 " * 20)
    inspect_prof_file(fs, float_id, dac)
    
    # 3. Inspect individual profile files
    print("\n" + "🔍 " * 20)
    print("STEP 3: INDIVIDUAL PROFILE FILE INSPECTION")
    print("🔍 " * 20)
    
    # Try to inspect first few profile files
    for profile_num in range(1, 4):  # Check first 3 profiles
        success = inspect_profile_file(fs, float_id, profile_num, dac)
        if success:
            break  # We got one working profile, that's enough for structure analysis
    
    print(f"\n{'='*80}")
    print("INSPECTION COMPLETE")
    print(f"{'='*80}")
    print("Now run the improved extractor based on this analysis!")

if __name__ == "__main__":
    main()
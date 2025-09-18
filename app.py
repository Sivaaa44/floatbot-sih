# from argopy import ArgoIndex
# import os

# # Save directory
# out_dir = "argo_indian_ocean"
# os.makedirs(out_dir, exist_ok=True)

# # Bounding box for the Indian Ocean [lon_min, lon_max, lat_min, lat_max]
# indian_ocean_box = [20, 120, -40, 25]

# # Query the Argo index (increase nrows for more results)
# idx = ArgoIndex().query.lon_lat(indian_ocean_box, nrows=1000)

# print("\n=== Searching Indian Ocean floats launched after 2020 ===")
# downloaded = 0

# for a_float in idx.iterfloats():
#     # Metadata
#     meta = a_float.open_dataset("meta")
#     launch_date = str(meta["LAUNCH_DATE"].data)
    
#     # Check launch date
#     if launch_date >= "2020-01-01":
#         print(f"\n✅ Float {a_float.WMO} launched on {launch_date}")

#         # Download all profiles
#         ds = a_float.open_dataset("profile")
#         print(f"Profiles available: {ds.dims['N_PROF']}")

#         # Save NetCDF locally
#         filename = os.path.join(out_dir, f"float_{a_float.WMO}_profiles.nc")
#         ds.to_netcdf(filename)
#         print(f"Saved: {filename}")

#         downloaded += 1
#         if downloaded >= 2:   # Stop after at least 2 floats for testing
#             break
#     else:
#         print(f"Skipping float {a_float.WMO}, launched on {launch_date}")

# print(f"\n=== Finished. Downloaded {downloaded} floats. ===")


from argopy import DataFetcher as Argo
import os

# Example float in the Indian Ocean (WMO ID) 
# Replace with another ID if needed
float_id = 6903569   # this is an Indian Ocean float launched after 2020

# Output directory
out_dir = "argo_test"
os.makedirs(out_dir, exist_ok=True)

print(f"\n=== Downloading all profiles for float {float_id} ===")

# Fetch all profiles for this float
ds = Argo().float(float_id).to_xarray()

print("\n=== Dataset summary ===")
print(ds)

# Save locally
filename = os.path.join(out_dir, f"float_{float_id}_profiles.nc")
ds.to_netcdf(filename)

print(f"\n✅ Saved NetCDF: {filename}")
print(f"Profiles available: {ds.dims['N_PROF']}")

# direct_gdac_access.py
"""
Direct GDAC access fallback module
Provides direct HTTP/FTP access to GDAC files when argopy fails
"""

import requests
import xarray as xr
import tempfile
import os
import logging
from typing import Optional, Dict, Any, List
from urllib.parse import urljoin
import ftplib
from io import BytesIO

logger = logging.getLogger(__name__)

class DirectGDACAccess:
    """Direct access to GDAC servers bypassing argopy"""
    
    def __init__(self):
        self.gdac_servers = [
            {
                'name': 'Ifremer HTTPS',
                'base_url': 'https://data-argo.ifremer.fr/',
                'type': 'http'
            },
            {
                'name': 'US GODAE HTTPS', 
                'base_url': 'https://usgodae.org/pub/outgoing/argo/',
                'type': 'http'
            },
            {
                'name': 'Ifremer FTP',
                'base_url': 'ftp://ftp.ifremer.fr/ifremer/argo/',
                'type': 'ftp'
            }
        ]
    
    def find_float_dac(self, wmo_id: str, server: Dict[str, str]) -> Optional[str]:
        """
        Find which DAC (Data Assembly Center) contains the float
        
        Args:
            wmo_id: WMO identifier
            server: Server configuration
            
        Returns:
            DAC name if found, None otherwise
        """
        common_dacs = [
            'coriolis', 'aoml', 'bodc', 'csio', 'csiro', 'incois', 
            'jma', 'kma', 'kordi', 'meds', 'nmdis'
        ]
        
        for dac in common_dacs:
            try:
                if server['type'] == 'http':
                    url = f"{server['base_url']}dac/{dac}/{wmo_id}/"
                    response = requests.head(url, timeout=10)
                    if response.status_code == 200:
                        logger.info(f"Found float {wmo_id} in DAC: {dac}")
                        return dac
                        
                elif server['type'] == 'ftp':
                    # FTP check is more complex, skip for now
                    continue
                    
            except Exception as e:
                logger.debug(f"DAC {dac} check failed for {wmo_id}: {e}")
                continue
        
        return None
    
    def download_netcdf_file(self, wmo_id: str, file_type: str = 'meta') -> Optional[xr.Dataset]:
        """
        Download and open NetCDF file directly from GDAC
        
        Args:
            wmo_id: WMO identifier
            file_type: Type of file ('meta', 'prof', 'tech', etc.)
            
        Returns:
            xarray Dataset or None if failed
        """
        for server in self.gdac_servers:
            try:
                logger.info(f"Trying {server['name']} for float {wmo_id}")
                
                # Find DAC
                dac = self.find_float_dac(wmo_id, server)
                if not dac:
                    logger.warning(f"Could not find DAC for {wmo_id} on {server['name']}")
                    continue
                
                # Construct file URL
                filename = f"{wmo_id}_{file_type}.nc"
                if server['type'] == 'http':
                    file_url = f"{server['base_url']}dac/{dac}/{wmo_id}/{filename}"
                    
                    # Download file
                    response = requests.get(file_url, timeout=30)
                    response.raise_for_status()
                    
                    # Save to temporary file and open with xarray
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.nc') as tmp_file:
                        tmp_file.write(response.content)
                        tmp_file.flush()
                        
                        # Open with xarray
                        dataset = xr.open_dataset(tmp_file.name)
                        
                        # Clean up temp file
                        os.unlink(tmp_file.name)
                        
                        logger.info(f"Successfully downloaded {filename} from {server['name']}")
                        return dataset
                
            except Exception as e:
                logger.warning(f"Failed to download from {server['name']}: {e}")
                continue
        
        logger.error(f"Failed to download {file_type}.nc for float {wmo_id} from all servers")
        return None
    
    def get_float_metadata_direct(self, wmo_id: str) -> Optional[Dict[str, Any]]:
        """
        Get float metadata using direct GDAC access
        
        Args:
            wmo_id: WMO identifier
            
        Returns:
            Metadata dictionary or None
        """
        try:
            meta_ds = self.download_netcdf_file(wmo_id, 'meta')
            if meta_ds is None:
                return None
            
            # Extract basic metadata
            def safe_extract(var_name, index=None):
                try:
                    if var_name not in meta_ds:
                        return None
                    value = meta_ds[var_name]
                    if index is not None and len(value) > index:
                        value = value[index]
                    
                    if hasattr(value, 'values'):
                        value = value.values
                    if hasattr(value, 'item'):
                        value = value.item()
                    
                    if isinstance(value, bytes):
                        value = value.decode('utf-8').strip()
                    elif isinstance(value, str):
                        value = value.strip()
                    
                    return value if value else None
                except Exception as e:
                    logger.debug(f"Error extracting {var_name}: {e}")
                    return None
            
            metadata = {
                'wmo_id': str(wmo_id),
                'platform_type': safe_extract('PLATFORM_TYPE'),
                'platform_maker': safe_extract('PLATFORM_MAKER'),
                'float_serial_no': safe_extract('FLOAT_SERIAL_NO'),
                'project_name': safe_extract('PROJECT_NAME'),
                'pi_name': safe_extract('PI_NAME'),
                'launch_latitude': safe_extract('LAUNCH_LATITUDE'),
                'launch_longitude': safe_extract('LAUNCH_LONGITUDE'),
                'deployment_platform': safe_extract('DEPLOYMENT_PLATFORM'),
                'deployment_cruise_id': safe_extract('DEPLOYMENT_CRUISE_ID'),
                'battery_type': safe_extract('BATTERY_TYPE'),
                'firmware_version': safe_extract('FIRMWARE_VERSION'),
                'float_owner': safe_extract('FLOAT_OWNER'),
                'operating_institution': safe_extract('OPERATING_INSTITUTION'),
            }
            
            # Handle dates
            try:
                launch_date = meta_ds.get('LAUNCH_DATE')
                if launch_date is not None:
                    metadata['launch_date'] = launch_date.values
            except:
                metadata['launch_date'] = None
            
            try:
                start_date = meta_ds.get('START_DATE')
                if start_date is not None:
                    metadata['start_date'] = start_date.values
            except:
                metadata['start_date'] = None
            
            meta_ds.close()
            logger.info(f"Successfully extracted metadata for {wmo_id} using direct access")
            return metadata
            
        except Exception as e:
            logger.error(f"Error extracting metadata using direct access for {wmo_id}: {e}")
            return None

# Add this to the FloatDataProcessor class as a fallback method
def add_direct_access_fallback_to_processor():
    """Add direct access methods to FloatDataProcessor"""
    
    def extract_float_metadata_with_direct_fallback(self, wmo_id: str) -> Optional[Dict[str, Any]]:
        """Enhanced metadata extraction with direct access fallback"""
        
        # First try the normal argopy method
        metadata = self.extract_float_metadata_original(wmo_id)
        if metadata is not None:
            return metadata
        
        # If that fails, try direct access
        logger.info(f"Falling back to direct GDAC access for float {wmo_id}")
        direct_access = DirectGDACAccess()
        metadata = direct_access.get_float_metadata_direct(wmo_id)
        
        if metadata:
            # Check if it's an Indian Ocean float
            lat = metadata.get('launch_latitude')
            lon = metadata.get('launch_longitude')
            
            if lat is not None and lon is not None:
                if self.is_indian_ocean_float(lat, lon):
                    ocean_region = "Indian Ocean"
                else:
                    ocean_region = "Other Ocean"
                    # Temporarily allow non-Indian Ocean floats for testing
                    # return None
                    
                logger.info(f"Direct access: Float {wmo_id} found in {ocean_region} ({lat:.2f}°N, {lon:.2f}°E)")
            
            # Add missing fields
            metadata.update({
                'dac_name': 'Unknown',
                'network_type': 'Unknown',
                'end_mission_date': None
            })
            
            return metadata
        
        return None
    
    # Monkey patch the method
    FloatDataProcessor.extract_float_metadata_original = FloatDataProcessor.extract_float_metadata
    FloatDataProcessor.extract_float_metadata = extract_float_metadata_with_direct_fallback
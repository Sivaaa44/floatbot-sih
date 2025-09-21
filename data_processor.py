# data_processor.py
"""
Float data processing module using argopy
Handles data extraction, transformation, and loading from GDAC
"""

import numpy as np
import pandas as pd
import xarray as xr
from datetime import datetime, timezone
import logging
from typing import Dict, List, Any, Optional, Tuple
import argopy
from argopy import ArgoFloat
import warnings

# Suppress xarray warnings
warnings.filterwarnings('ignore')

# Configure logging
logger = logging.getLogger(__name__)

class FloatDataProcessor:
    """Processes Argo float data from GDAC using argopy"""
    
    def __init__(self, gdac_host: str = "https"):
        """
        Initialize the data processor
        
        Args:
            gdac_host: GDAC host shortcut or full URL
        """
        self.gdac_host = gdac_host
        # Define fallback hosts in order of preference
        self.fallback_hosts = [
            "https://data-argo.ifremer.fr",  # Primary Ifremer
            "ftp://ftp.ifremer.fr/ifremer/argo",  # FTP fallback
            "https://usgodae.org/pub/outgoing/argo",  # US GODAE
            "s3://argo-gdac-sandbox/pub",  # AWS S3
        ]
        self.current_host_index = 0
        logger.info(f"Initialized FloatDataProcessor with GDAC host: {gdac_host}")
    
    def get_argo_float_with_fallback(self, wmo_id: str, max_retries: int = 3):
        """
        Create ArgoFloat instance with fallback mechanism
        
        Args:
            wmo_id: WMO identifier
            max_retries: Maximum number of host fallbacks to try
            
        Returns:
            ArgoFloat instance or None if all fail
        """
        hosts_to_try = [self.gdac_host] + self.fallback_hosts
        
        for i, host in enumerate(hosts_to_try[:max_retries + 1]):
            try:
                logger.info(f"Attempting to connect to host: {host}")
                
                # Disable ERDDAP for this attempt by setting offline mode
                argopy.set_options(mode='offline')
                
                af = ArgoFloat(int(wmo_id), host=host)
                
                # Test connection by trying to list datasets
                datasets = af.ls_dataset()
                if datasets:
                    logger.info(f"Successfully connected to {host}")
                    return af
                    
            except Exception as e:
                logger.warning(f"Failed to connect to {host}: {e}")
                continue
        
        logger.error(f"All GDAC hosts failed for float {wmo_id}")
        return None
    
    def is_indian_ocean_float(self, launch_lat: float, launch_lon: float) -> bool:
        """
        Check if float is deployed in Indian Ocean region
        
        Args:
            launch_lat: Launch latitude
            launch_lon: Launch longitude
            
        Returns:
            bool: True if in Indian Ocean region
        """
        # Indian Ocean approximate bounds
        return (20 <= launch_lon <= 120) and (-60 <= launch_lat <= 30)
    
    def safe_get_value(self, dataset: xr.Dataset, var_name: str, index: int = None) -> Any:
        """Safely extract value from xarray dataset"""
        try:
            if var_name not in dataset:
                return None
            
            value = dataset[var_name]
            
            if index is not None:
                if len(value.dims) > 0 and len(value) > index:
                    value = value[index]
            
            # Convert to Python native types
            if hasattr(value, 'values'):
                value = value.values
            
            if hasattr(value, 'item'):
                value = value.item()
            
            # Handle bytes strings
            if isinstance(value, bytes):
                value = value.decode('utf-8').strip()
            elif isinstance(value, str):
                value = value.strip()
            elif isinstance(value, (np.ndarray, list)) and len(value) > 0 and isinstance(value[0], bytes):
                value = ''.join([b.decode('utf-8') if isinstance(b, bytes) else str(b) for b in value]).strip()
            
            # Handle NaN values
            if pd.isna(value) or (isinstance(value, str) and value == ''):
                return None
                
            return value
            
        except Exception as e:
            logger.warning(f"Error extracting {var_name}: {e}")
            return None
    
    def safe_get_datetime(self, dataset: xr.Dataset, var_name: str, index: int = None) -> Optional[datetime]:
        """Safely extract datetime from xarray dataset"""
        try:
            value = self.safe_get_value(dataset, var_name, index)
            if value is None:
                return None
            
            if isinstance(value, (np.datetime64, pd.Timestamp)):
                # Convert to timezone-aware datetime
                dt = pd.to_datetime(value)
                if dt.tz is None:
                    dt = dt.tz_localize(timezone.utc)
                return dt.to_pydatetime()
            elif isinstance(value, str):
                # Try to parse string datetime
                try:
                    dt = pd.to_datetime(value)
                    if dt.tz is None:
                        dt = dt.tz_localize(timezone.utc)
                    return dt.to_pydatetime()
                except:
                    return None
            
            return None
            
        except Exception as e:
            logger.warning(f"Error extracting datetime {var_name}: {e}")
            return None
    
    def extract_float_metadata(self, wmo_id: str) -> Optional[Dict[str, Any]]:
        """
        Extract float metadata from meta.nc file with fallback mechanism
        
        Args:
            wmo_id: WMO identifier of the float
            
        Returns:
            Dict containing float metadata or None if error
        """
        af = None
        try:
            # Create ArgoFloat instance with fallback
            af = self.get_argo_float_with_fallback(wmo_id)
            if af is None:
                logger.error(f"Could not establish connection to any GDAC for float {wmo_id}")
                return None
            
            # Load metadata
            meta_ds = af.open_dataset('meta')
            
            # Check if it's an Indian Ocean float
            launch_lat = self.safe_get_value(meta_ds, 'LAUNCH_LATITUDE')
            launch_lon = self.safe_get_value(meta_ds, 'LAUNCH_LONGITUDE')
            
            if launch_lat is None or launch_lon is None:
                logger.warning(f"Missing launch coordinates for float {wmo_id}")
                return None
            
            # Temporarily disable Indian Ocean filtering for testing
            # if not self.is_indian_ocean_float(launch_lat, launch_lon):
            #     logger.info(f"Float {wmo_id} is not in Indian Ocean region")
            #     return None
            
            # Extract metadata
            metadata = {
                'wmo_id': str(wmo_id),
                'platform_type': self.safe_get_value(meta_ds, 'PLATFORM_TYPE'),
                'platform_maker': self.safe_get_value(meta_ds, 'PLATFORM_MAKER'),
                'float_serial_no': self.safe_get_value(meta_ds, 'FLOAT_SERIAL_NO'),
                'project_name': self.safe_get_value(meta_ds, 'PROJECT_NAME'),
                'pi_name': self.safe_get_value(meta_ds, 'PI_NAME'),
                'launch_date': self.safe_get_datetime(meta_ds, 'LAUNCH_DATE'),
                'launch_latitude': float(launch_lat) if launch_lat is not None else None,
                'launch_longitude': float(launch_lon) if launch_lon is not None else None,
                'deployment_platform': self.safe_get_value(meta_ds, 'DEPLOYMENT_PLATFORM'),
                'deployment_cruise_id': self.safe_get_value(meta_ds, 'DEPLOYMENT_CRUISE_ID'),
                'start_date': self.safe_get_datetime(meta_ds, 'START_DATE'),
                'end_mission_date': self.safe_get_datetime(meta_ds, 'END_MISSION_DATE'),
                'battery_type': self.safe_get_value(meta_ds, 'BATTERY_TYPE'),
                'firmware_version': self.safe_get_value(meta_ds, 'FIRMWARE_VERSION'),
                'dac_name': af.DAC if hasattr(af, 'DAC') else None,
                'network_type': str(af.networks) if hasattr(af, 'networks') else None,
                'float_owner': self.safe_get_value(meta_ds, 'FLOAT_OWNER'),
                'operating_institution': self.safe_get_value(meta_ds, 'OPERATING_INSTITUTION')
            }
            
            meta_ds.close()
            
            # Log location info
            ocean_region = "Indian Ocean" if self.is_indian_ocean_float(launch_lat, launch_lon) else "Other Ocean"
            logger.info(f"Successfully extracted metadata for float {wmo_id} in {ocean_region} ({launch_lat:.2f}°N, {launch_lon:.2f}°E)")
            return metadata
            
        except Exception as e:
            logger.error(f"Error extracting metadata for float {wmo_id}: {e}")
            return None
        finally:
            # Clean up
            if af:
                try:
                    af.clear_cache()
                except:
                    pass
    
    def extract_cycle_data(self, wmo_id: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Extract cycle and measurement data from prof.nc file with fallback mechanism
        
        Args:
            wmo_id: WMO identifier of the float
            
        Returns:
            Tuple of (cycles_data, measurements_data)
        """
        cycles_data = []
        measurements_data = []
        af = None
        
        try:
            # Create ArgoFloat instance with fallback
            af = self.get_argo_float_with_fallback(wmo_id)
            if af is None:
                logger.error(f"Could not establish connection to any GDAC for float {wmo_id}")
                return [], []
            
            # Load profile data
            prof_ds = af.open_dataset('prof')
            
            n_prof = len(prof_ds.N_PROF)
            logger.info(f"Processing {n_prof} profiles for float {wmo_id}")
            
            for i in range(n_prof):
                try:
                    # Extract cycle information
                    cycle_data = {
                        'wmo_id': str(wmo_id),
                        'cycle_number': int(self.safe_get_value(prof_ds, 'CYCLE_NUMBER', i) or 0),
                        'profile_date': self.safe_get_datetime(prof_ds, 'JULD', i),
                        'profile_latitude': self.safe_get_value(prof_ds, 'LATITUDE', i),
                        'profile_longitude': self.safe_get_value(prof_ds, 'LONGITUDE', i),
                        'ocean_area': 'Indian Ocean',
                        'positioning_system': self.safe_get_value(prof_ds, 'POSITIONING_SYSTEM', i),
                        'profile_pres_qc': self.safe_get_value(prof_ds, 'PROFILE_PRES_QC', i),
                        'profile_temp_qc': self.safe_get_value(prof_ds, 'PROFILE_TEMP_QC', i),
                        'profile_psal_qc': self.safe_get_value(prof_ds, 'PROFILE_PSAL_QC', i),
                        'vertical_sampling_scheme': self.safe_get_value(prof_ds, 'VERTICAL_SAMPLING_SCHEME', i),
                        'config_mission_number': self.safe_get_value(prof_ds, 'CONFIG_MISSION_NUMBER', i),
                        'data_mode': self.safe_get_value(prof_ds, 'DATA_MODE', i),
                        'direction': self.safe_get_value(prof_ds, 'DIRECTION', i),
                        'data_centre': self.safe_get_value(prof_ds, 'DATA_CENTRE', i),
                        'dc_reference': self.safe_get_value(prof_ds, 'DC_REFERENCE', i),
                        'data_state_indicator': self.safe_get_value(prof_ds, 'DATA_STATE_INDICATOR', i),
                    }
                    
                    cycles_data.append(cycle_data)
                    
                    # Extract measurements for this cycle
                    cycle_measurements = self.extract_measurements_for_profile(prof_ds, wmo_id, cycle_data['cycle_number'], i)
                    measurements_data.extend(cycle_measurements)
                    
                except Exception as e:
                    logger.error(f"Error processing profile {i} for float {wmo_id}: {e}")
                    continue
            
            prof_ds.close()
            logger.info(f"Successfully extracted {len(cycles_data)} cycles and {len(measurements_data)} measurements for float {wmo_id}")
            return cycles_data, measurements_data
            
        except Exception as e:
            logger.error(f"Error extracting cycle data for float {wmo_id}: {e}")
            return [], []
        finally:
            # Clean up
            if af:
                try:
                    af.clear_cache()
                except:
                    pass
    
    def extract_measurements_for_profile(self, prof_ds: xr.Dataset, wmo_id: str, cycle_number: int, profile_index: int) -> List[Dict[str, Any]]:
        """Extract individual measurements for a profile"""
        measurements = []
        
        try:
            n_levels = len(prof_ds.N_LEVELS)
            
            for level in range(n_levels):
                # Check if this measurement level has valid data
                pressure = self.safe_get_value(prof_ds, 'PRES', (profile_index, level))
                if pressure is None or pd.isna(pressure):
                    continue
                
                measurement = {
                    'wmo_id': str(wmo_id),
                    'cycle_number': cycle_number,
                    'measurement_level': level,
                    
                    # Core measurements
                    'pressure': self.safe_get_value(prof_ds, 'PRES', (profile_index, level)),
                    'pressure_qc': self.safe_get_value(prof_ds, 'PRES_QC', (profile_index, level)),
                    'pressure_adjusted': self.safe_get_value(prof_ds, 'PRES_ADJUSTED', (profile_index, level)),
                    'pressure_adjusted_qc': self.safe_get_value(prof_ds, 'PRES_ADJUSTED_QC', (profile_index, level)),
                    'pressure_adjusted_error': self.safe_get_value(prof_ds, 'PRES_ADJUSTED_ERROR', (profile_index, level)),
                    
                    'temperature': self.safe_get_value(prof_ds, 'TEMP', (profile_index, level)),
                    'temperature_qc': self.safe_get_value(prof_ds, 'TEMP_QC', (profile_index, level)),
                    'temperature_adjusted': self.safe_get_value(prof_ds, 'TEMP_ADJUSTED', (profile_index, level)),
                    'temperature_adjusted_qc': self.safe_get_value(prof_ds, 'TEMP_ADJUSTED_QC', (profile_index, level)),
                    'temperature_adjusted_error': self.safe_get_value(prof_ds, 'TEMP_ADJUSTED_ERROR', (profile_index, level)),
                    
                    'salinity': self.safe_get_value(prof_ds, 'PSAL', (profile_index, level)),
                    'salinity_qc': self.safe_get_value(prof_ds, 'PSAL_QC', (profile_index, level)),
                    'salinity_adjusted': self.safe_get_value(prof_ds, 'PSAL_ADJUSTED', (profile_index, level)),
                    'salinity_adjusted_qc': self.safe_get_value(prof_ds, 'PSAL_ADJUSTED_QC', (profile_index, level)),
                    'salinity_adjusted_error': self.safe_get_value(prof_ds, 'PSAL_ADJUSTED_ERROR', (profile_index, level)),
                    
                    # Biogeochemical parameters (if available)
                    'doxy': self.safe_get_value(prof_ds, 'DOXY', (profile_index, level)),
                    'doxy_qc': self.safe_get_value(prof_ds, 'DOXY_QC', (profile_index, level)),
                    'doxy_adjusted': self.safe_get_value(prof_ds, 'DOXY_ADJUSTED', (profile_index, level)),
                    'doxy_adjusted_qc': self.safe_get_value(prof_ds, 'DOXY_ADJUSTED_QC', (profile_index, level)),
                    
                    'chla': self.safe_get_value(prof_ds, 'CHLA', (profile_index, level)),
                    'chla_qc': self.safe_get_value(prof_ds, 'CHLA_QC', (profile_index, level)),
                    'chla_adjusted': self.safe_get_value(prof_ds, 'CHLA_ADJUSTED', (profile_index, level)),
                    'chla_adjusted_qc': self.safe_get_value(prof_ds, 'CHLA_ADJUSTED_QC', (profile_index, level)),
                    
                    'bbp700': self.safe_get_value(prof_ds, 'BBP700', (profile_index, level)),
                    'bbp700_qc': self.safe_get_value(prof_ds, 'BBP700_QC', (profile_index, level)),
                    'bbp700_adjusted': self.safe_get_value(prof_ds, 'BBP700_ADJUSTED', (profile_index, level)),
                    'bbp700_adjusted_qc': self.safe_get_value(prof_ds, 'BBP700_ADJUSTED_QC', (profile_index, level)),
                    
                    'nitrate': self.safe_get_value(prof_ds, 'NITRATE', (profile_index, level)),
                    'nitrate_qc': self.safe_get_value(prof_ds, 'NITRATE_QC', (profile_index, level)),
                    'nitrate_adjusted': self.safe_get_value(prof_ds, 'NITRATE_ADJUSTED', (profile_index, level)),
                    'nitrate_adjusted_qc': self.safe_get_value(prof_ds, 'NITRATE_ADJUSTED_QC', (profile_index, level)),
                    
                    'ph_in_situ_total': self.safe_get_value(prof_ds, 'PH_IN_SITU_TOTAL', (profile_index, level)),
                    'ph_in_situ_total_qc': self.safe_get_value(prof_ds, 'PH_IN_SITU_TOTAL_QC', (profile_index, level)),
                    'ph_in_situ_total_adjusted': self.safe_get_value(prof_ds, 'PH_IN_SITU_TOTAL_ADJUSTED', (profile_index, level)),
                    'ph_in_situ_total_adjusted_qc': self.safe_get_value(prof_ds, 'PH_IN_SITU_TOTAL_ADJUSTED_QC', (profile_index, level)),
                }
                
                measurements.append(measurement)
                
        except Exception as e:
            logger.error(f"Error extracting measurements for profile {profile_index}: {e}")
        
        return measurements
    
    def process_float_complete(self, wmo_id: str) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Process complete float data (metadata, cycles, and measurements)
        
        Args:
            wmo_id: WMO identifier of the float
            
        Returns:
            Tuple of (metadata, cycles_data, measurements_data)
        """
        logger.info(f"Starting complete processing for float {wmo_id}")
        
        # Extract metadata
        metadata = self.extract_float_metadata(wmo_id)
        if metadata is None:
            logger.error(f"Failed to extract metadata for float {wmo_id}")
            return None, [], []
        
        # Extract cycle and measurement data
        cycles_data, measurements_data = self.extract_cycle_data(wmo_id)
        
        logger.info(f"Completed processing for float {wmo_id}: "
                   f"metadata={'✓' if metadata else '✗'}, "
                   f"cycles={len(cycles_data)}, "
                   f"measurements={len(measurements_data)}")
        
        return metadata, cycles_data, measurements_data
    
    def validate_float_data(self, metadata: Dict[str, Any], cycles: List[Dict[str, Any]], 
                           measurements: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Validate extracted float data and return validation report
        
        Args:
            metadata: Float metadata
            cycles: Cycle data
            measurements: Measurement data
            
        Returns:
            Dictionary with validation results
        """
        validation_report = {
            'valid': True,
            'warnings': [],
            'errors': [],
            'stats': {
                'cycles_count': len(cycles),
                'measurements_count': len(measurements),
                'avg_measurements_per_cycle': len(measurements) / len(cycles) if cycles else 0
            }
        }
        
        # Check metadata
        if not metadata.get('wmo_id'):
            validation_report['errors'].append("Missing WMO ID")
            validation_report['valid'] = False
        
        if not metadata.get('launch_date'):
            validation_report['warnings'].append("Missing launch date")
        
        if metadata.get('launch_latitude') is None or metadata.get('launch_longitude') is None:
            validation_report['errors'].append("Missing launch coordinates")
            validation_report['valid'] = False
        
        # Check cycles
        if not cycles:
            validation_report['errors'].append("No cycles found")
            validation_report['valid'] = False
        else:
            cycle_numbers = [c['cycle_number'] for c in cycles]
            if len(set(cycle_numbers)) != len(cycle_numbers):
                validation_report['warnings'].append("Duplicate cycle numbers found")
        
        # Check measurements
        if not measurements:
            validation_report['warnings'].append("No measurements found")
        else:
            # Check for essential measurements
            temp_count = sum(1 for m in measurements if m.get('temperature') is not None)
            sal_count = sum(1 for m in measurements if m.get('salinity') is not None)
            pres_count = sum(1 for m in measurements if m.get('pressure') is not None)
            
            validation_report['stats'].update({
                'temperature_measurements': temp_count,
                'salinity_measurements': sal_count,
                'pressure_measurements': pres_count
            })
            
            if temp_count == 0:
                validation_report['warnings'].append("No temperature measurements found")
            if sal_count == 0:
                validation_report['warnings'].append("No salinity measurements found")
            if pres_count == 0:
                validation_report['errors'].append("No pressure measurements found")
                validation_report['valid'] = False
        
        return validation_report
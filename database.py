# database.py
"""
Database connection and operations module for FloatBot
Handles PostgreSQL connections and CRUD operations for ocean float data
"""

import psycopg2
import psycopg2.extras
from datetime import datetime
import logging
from typing import Dict, List, Optional, Tuple, Any
from contextlib import contextmanager
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FloatDatabase:
    """Database handler for ocean float data operations"""
    
    def __init__(self, connection_string: str):
        """
        Initialize database connection
        
        Args:
            connection_string: PostgreSQL connection string
        """
        self.connection_string = connection_string
        self._test_connection()
    
    def _test_connection(self):
        """Test database connection"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT version();")
                    version = cur.fetchone()
                    logger.info(f"Connected to PostgreSQL: {version[0]}")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = None
        try:
            conn = psycopg2.connect(self.connection_string)
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def create_tables(self):
        """Create database tables from schema file"""
        # Read and execute the schema file
        schema_path = os.path.join(os.path.dirname(__file__), 'schema.sql')
        try:
            with open(schema_path, 'r') as f:
                schema_sql = f.read()
            
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(schema_sql)
                conn.commit()
                logger.info("Database tables created successfully")
        except FileNotFoundError:
            logger.error("Schema file not found. Please create schema.sql with the database schema.")
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            raise
    
    # ===== FLOAT OPERATIONS =====
    
    def insert_float_metadata(self, float_data: Dict[str, Any]) -> bool:
        """
        Insert float metadata into floats table
        
        Args:
            float_data: Dictionary containing float metadata
            
        Returns:
            bool: Success status
        """
        insert_query = """
        INSERT INTO floats (
            wmo_id, platform_type, platform_maker, float_serial_no, project_name,
            pi_name, launch_date, launch_latitude, launch_longitude, 
            deployment_platform, deployment_cruise_id, start_date, end_mission_date,
            battery_type, firmware_version, dac_name, network_type, 
            float_owner, operating_institution
        ) VALUES (
            %(wmo_id)s, %(platform_type)s, %(platform_maker)s, %(float_serial_no)s, 
            %(project_name)s, %(pi_name)s, %(launch_date)s, %(launch_latitude)s, 
            %(launch_longitude)s, %(deployment_platform)s, %(deployment_cruise_id)s,
            %(start_date)s, %(end_mission_date)s, %(battery_type)s, %(firmware_version)s,
            %(dac_name)s, %(network_type)s, %(float_owner)s, %(operating_institution)s
        ) ON CONFLICT (wmo_id) DO UPDATE SET
            updated_at = CURRENT_TIMESTAMP,
            platform_type = EXCLUDED.platform_type,
            platform_maker = EXCLUDED.platform_maker,
            end_mission_date = EXCLUDED.end_mission_date
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(insert_query, float_data)
                conn.commit()
                logger.info(f"Float {float_data['wmo_id']} metadata inserted/updated successfully")
                return True
        except Exception as e:
            logger.error(f"Error inserting float metadata: {e}")
            return False
    
    def get_float_metadata(self, wmo_id: str) -> Optional[Dict[str, Any]]:
        """Get float metadata by WMO ID"""
        query = "SELECT * FROM floats WHERE wmo_id = %s"
        
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(query, (wmo_id,))
                    result = cur.fetchone()
                    return dict(result) if result else None
        except Exception as e:
            logger.error(f"Error getting float metadata: {e}")
            return None
    
    # ===== CYCLE OPERATIONS =====
    
    def insert_cycle_data(self, cycle_data: Dict[str, Any]) -> bool:
        """Insert cycle data into cycles table"""
        insert_query = """
        INSERT INTO cycles (
            wmo_id, cycle_number, profile_date, profile_latitude, profile_longitude,
            ocean_area, positioning_system, profile_pres_qc, profile_temp_qc, 
            profile_psal_qc, vertical_sampling_scheme, config_mission_number,
            data_mode, direction, data_centre, dc_reference, data_state_indicator
        ) VALUES (
            %(wmo_id)s, %(cycle_number)s, %(profile_date)s, %(profile_latitude)s,
            %(profile_longitude)s, %(ocean_area)s, %(positioning_system)s,
            %(profile_pres_qc)s, %(profile_temp_qc)s, %(profile_psal_qc)s,
            %(vertical_sampling_scheme)s, %(config_mission_number)s, %(data_mode)s,
            %(direction)s, %(data_centre)s, %(dc_reference)s, %(data_state_indicator)s
        ) ON CONFLICT (wmo_id, cycle_number) DO UPDATE SET
            updated_at = CURRENT_TIMESTAMP,
            profile_date = EXCLUDED.profile_date,
            profile_latitude = EXCLUDED.profile_latitude,
            profile_longitude = EXCLUDED.profile_longitude,
            data_mode = EXCLUDED.data_mode
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(insert_query, cycle_data)
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Error inserting cycle data: {e}")
            return False
    
    def get_cycles_for_float(self, wmo_id: str) -> List[Dict[str, Any]]:
        """Get all cycles for a specific float"""
        query = """
        SELECT * FROM cycles 
        WHERE wmo_id = %s 
        ORDER BY cycle_number
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(query, (wmo_id,))
                    results = cur.fetchall()
                    return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"Error getting cycles for float {wmo_id}: {e}")
            return []
    
    # ===== MEASUREMENT OPERATIONS =====
    
    def insert_measurements_batch(self, measurements: List[Dict[str, Any]]) -> bool:
        """Insert multiple measurements efficiently"""
        if not measurements:
            return True
            
        insert_query = """
        INSERT INTO measurements (
            wmo_id, cycle_number, measurement_level, pressure, pressure_qc,
            pressure_adjusted, pressure_adjusted_qc, pressure_adjusted_error,
            temperature, temperature_qc, temperature_adjusted, temperature_adjusted_qc,
            temperature_adjusted_error, salinity, salinity_qc, salinity_adjusted,
            salinity_adjusted_qc, salinity_adjusted_error, doxy, doxy_qc,
            doxy_adjusted, doxy_adjusted_qc, chla, chla_qc, chla_adjusted,
            chla_adjusted_qc, bbp700, bbp700_qc, bbp700_adjusted, bbp700_adjusted_qc,
            nitrate, nitrate_qc, nitrate_adjusted, nitrate_adjusted_qc,
            ph_in_situ_total, ph_in_situ_total_qc, ph_in_situ_total_adjusted,
            ph_in_situ_total_adjusted_qc
        ) VALUES (
            %(wmo_id)s, %(cycle_number)s, %(measurement_level)s, %(pressure)s, %(pressure_qc)s,
            %(pressure_adjusted)s, %(pressure_adjusted_qc)s, %(pressure_adjusted_error)s,
            %(temperature)s, %(temperature_qc)s, %(temperature_adjusted)s, %(temperature_adjusted_qc)s,
            %(temperature_adjusted_error)s, %(salinity)s, %(salinity_qc)s, %(salinity_adjusted)s,
            %(salinity_adjusted_qc)s, %(salinity_adjusted_error)s, %(doxy)s, %(doxy_qc)s,
            %(doxy_adjusted)s, %(doxy_adjusted_qc)s, %(chla)s, %(chla_qc)s, %(chla_adjusted)s,
            %(chla_adjusted_qc)s, %(bbp700)s, %(bbp700_qc)s, %(bbp700_adjusted)s, %(bbp700_adjusted_qc)s,
            %(nitrate)s, %(nitrate_qc)s, %(nitrate_adjusted)s, %(nitrate_adjusted_qc)s,
            %(ph_in_situ_total)s, %(ph_in_situ_total_qc)s, %(ph_in_situ_total_adjusted)s,
            %(ph_in_situ_total_adjusted_qc)s
        ) ON CONFLICT (wmo_id, cycle_number, measurement_level) DO UPDATE SET
            pressure = EXCLUDED.pressure,
            temperature = EXCLUDED.temperature,
            salinity = EXCLUDED.salinity,
            doxy = EXCLUDED.doxy,
            chla = EXCLUDED.chla,
            bbp700 = EXCLUDED.bbp700,
            nitrate = EXCLUDED.nitrate,
            ph_in_situ_total = EXCLUDED.ph_in_situ_total
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.executemany(insert_query, measurements)
                conn.commit()
                logger.info(f"Inserted {len(measurements)} measurements successfully")
                return True
        except Exception as e:
            logger.error(f"Error inserting measurements: {e}")
            return False
    
    def get_measurements_for_cycle(self, wmo_id: str, cycle_number: int) -> List[Dict[str, Any]]:
        """Get all measurements for a specific cycle"""
        query = """
        SELECT * FROM measurements 
        WHERE wmo_id = %s AND cycle_number = %s
        ORDER BY measurement_level
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(query, (wmo_id, cycle_number))
                    results = cur.fetchall()
                    return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"Error getting measurements: {e}")
            return []
    
    # ===== UTILITY OPERATIONS =====
    
    def get_indian_ocean_floats(self) -> List[Dict[str, Any]]:
        """Get all floats in the Indian Ocean region"""
        query = "SELECT * FROM indian_ocean_floats ORDER BY wmo_id"
        
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(query)
                    results = cur.fetchall()
                    return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"Error getting Indian Ocean floats: {e}")
            return []
    
    def get_float_summary(self, wmo_id: str) -> Optional[Dict[str, Any]]:
        """Get comprehensive summary of a float's data"""
        query = """
        SELECT 
            f.*,
            COUNT(DISTINCT c.cycle_number) as total_cycles,
            COUNT(m.id) as total_measurements,
            MIN(c.profile_date) as first_profile,
            MAX(c.profile_date) as last_profile,
            AVG(c.profile_latitude) as avg_latitude,
            AVG(c.profile_longitude) as avg_longitude
        FROM floats f
        LEFT JOIN cycles c ON f.wmo_id = c.wmo_id
        LEFT JOIN measurements m ON c.wmo_id = m.wmo_id AND c.cycle_number = m.cycle_number
        WHERE f.wmo_id = %s
        GROUP BY f.wmo_id
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(query, (wmo_id,))
                    result = cur.fetchone()
                    return dict(result) if result else None
        except Exception as e:
            logger.error(f"Error getting float summary: {e}")
            return None
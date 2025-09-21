# floatbot_main.py
"""
FloatBot main application
Orchestrates data processing and database operations for ocean floats
"""

import os
import sys
import logging
from datetime import datetime
from typing import List, Optional, Dict, Any
import argparse

from database import FloatDatabase
from data_processor import FloatDataProcessor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('floatbot.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class FloatBot:
    """Main FloatBot application class"""
    
    def __init__(self, connection_string: str, gdac_host: str = "https"):
        """
        Initialize FloatBot
        
        Args:
            connection_string: PostgreSQL connection string
            gdac_host: GDAC host for argopy
        """
        self.db = FloatDatabase(connection_string)
        self.processor = FloatDataProcessor(gdac_host)
        logger.info("FloatBot initialized successfully")
    
    def setup_database(self):
        """Create database tables if they don't exist"""
        logger.info("Setting up database tables...")
        try:
            self.db.create_tables()
            logger.info("Database setup completed successfully")
        except Exception as e:
            logger.error(f"Database setup failed: {e}")
            raise
    
    def process_single_float(self, wmo_id: str, force_update: bool = False) -> bool:
        """
        Process a single float and store in database
        
        Args:
            wmo_id: WMO identifier of the float
            force_update: Force update even if float exists in database
            
        Returns:
            bool: Success status
        """
        logger.info(f"Processing float {wmo_id}")
        
        # Check if float already exists (unless force update)
        if not force_update:
            existing_float = self.db.get_float_metadata(wmo_id)
            if existing_float:
                logger.info(f"Float {wmo_id} already exists in database. Use force_update=True to reprocess.")
                return True
        
        try:
            # Extract data using argopy
            metadata, cycles_data, measurements_data = self.processor.process_float_complete(wmo_id)
            
            if metadata is None:
                logger.error(f"Failed to extract data for float {wmo_id}")
                return False
            
            # Validate data
            validation_report = self.processor.validate_float_data(metadata, cycles_data, measurements_data)
            
            if not validation_report['valid']:
                logger.error(f"Validation failed for float {wmo_id}: {validation_report['errors']}")
                return False
            
            if validation_report['warnings']:
                logger.warning(f"Validation warnings for float {wmo_id}: {validation_report['warnings']}")
            
            # Store in database
            success = True
            
            # Insert metadata
            if not self.db.insert_float_metadata(metadata):
                logger.error(f"Failed to insert metadata for float {wmo_id}")
                success = False
            
            # Insert cycles
            for cycle_data in cycles_data:
                if not self.db.insert_cycle_data(cycle_data):
                    logger.error(f"Failed to insert cycle {cycle_data['cycle_number']} for float {wmo_id}")
                    success = False
            
            # Insert measurements in batches
            batch_size = 1000
            for i in range(0, len(measurements_data), batch_size):
                batch = measurements_data[i:i + batch_size]
                if not self.db.insert_measurements_batch(batch):
                    logger.error(f"Failed to insert measurement batch {i//batch_size + 1} for float {wmo_id}")
                    success = False
            
            if success:
                logger.info(f"Successfully processed and stored float {wmo_id}")
                logger.info(f"Stats: {validation_report['stats']}")
            
            return success
            
        except Exception as e:
            logger.error(f"Error processing float {wmo_id}: {e}")
            return False
    
    def process_multiple_floats(self, wmo_ids: List[str], force_update: bool = False) -> Dict[str, bool]:
        """
        Process multiple floats
        
        Args:
            wmo_ids: List of WMO identifiers
            force_update: Force update even if floats exist
            
        Returns:
            Dictionary mapping WMO ID to success status
        """
        results = {}
        
        logger.info(f"Processing {len(wmo_ids)} floats")
        
        for i, wmo_id in enumerate(wmo_ids, 1):
            logger.info(f"Processing float {i}/{len(wmo_ids)}: {wmo_id}")
            results[wmo_id] = self.process_single_float(wmo_id, force_update)
        
        # Summary
        successful = sum(1 for success in results.values() if success)
        logger.info(f"Processing completed: {successful}/{len(wmo_ids)} floats successful")
        
        return results
    
    def get_float_info(self, wmo_id: str) -> Optional[Dict[str, Any]]:
        """Get comprehensive float information"""
        return self.db.get_float_summary(wmo_id)
    
    def get_indian_ocean_floats(self) -> List[Dict[str, Any]]:
        """Get all Indian Ocean floats from database"""
        return self.db.get_indian_ocean_floats()
    
    def get_float_cycles(self, wmo_id: str) -> List[Dict[str, Any]]:
        """Get all cycles for a float"""
        return self.db.get_cycles_for_float(wmo_id)
    
    def get_cycle_measurements(self, wmo_id: str, cycle_number: int) -> List[Dict[str, Any]]:
        """Get measurements for a specific cycle"""
        return self.db.get_measurements_for_cycle(wmo_id, cycle_number)
    
    def generate_float_report(self, wmo_id: str) -> Optional[str]:
        """Generate a comprehensive report for a float"""
        float_info = self.get_float_info(wmo_id)
        if not float_info:
            return None
        
        report = f"""
═══════════════════════════════════════════════
FLOAT REPORT - WMO {wmo_id}
═══════════════════════════════════════════════

METADATA:
─────────────────────────────────────────────
Platform Type: {float_info.get('platform_type', 'N/A')}
Manufacturer: {float_info.get('platform_maker', 'N/A')}
Serial Number: {float_info.get('float_serial_no', 'N/A')}
Project: {float_info.get('project_name', 'N/A')}
Principal Investigator: {float_info.get('pi_name', 'N/A')}
Owner: {float_info.get('float_owner', 'N/A')}

DEPLOYMENT:
─────────────────────────────────────────────
Launch Date: {float_info.get('launch_date', 'N/A')}
Launch Position: {float_info.get('launch_latitude', 'N/A')}°N, {float_info.get('launch_longitude', 'N/A')}°E
Deployment Platform: {float_info.get('deployment_platform', 'N/A')}
Cruise ID: {float_info.get('deployment_cruise_id', 'N/A')}

OPERATIONAL STATUS:
─────────────────────────────────────────────
Start Date: {float_info.get('start_date', 'N/A')}
End Date: {float_info.get('end_mission_date', 'N/A')}
Total Cycles: {float_info.get('total_cycles', 0)}
Total Measurements: {float_info.get('total_measurements', 0)}
First Profile: {float_info.get('first_profile', 'N/A')}
Last Profile: {float_info.get('last_profile', 'N/A')}

GEOGRAPHIC COVERAGE:
─────────────────────────────────────────────
Average Position: {float_info.get('avg_latitude', 'N/A')}°N, {float_info.get('avg_longitude', 'N/A')}°E
Ocean Region: Indian Ocean

TECHNICAL:
─────────────────────────────────────────────
Battery Type: {float_info.get('battery_type', 'N/A')}
Firmware Version: {float_info.get('firmware_version', 'N/A')}
DAC: {float_info.get('dac_name', 'N/A')}
Network: {float_info.get('network_type', 'N/A')}

═══════════════════════════════════════════════
Report generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
═══════════════════════════════════════════════
"""
        return report

def main():
    """Main function for command-line interface"""
    parser = argparse.ArgumentParser(description='FloatBot - Ocean Float Data Manager')
    parser.add_argument('--connection-string', required=True, help='PostgreSQL connection string')
    parser.add_argument('--setup-db', action='store_true', help='Setup database tables')
    parser.add_argument('--wmo-id', help='Process specific float by WMO ID')
    parser.add_argument('--wmo-ids', nargs='+', help='Process multiple floats by WMO IDs')
    parser.add_argument('--force-update', action='store_true', help='Force update even if float exists')
    parser.add_argument('--list-floats', action='store_true', help='List all Indian Ocean floats in database')
    parser.add_argument('--report', help='Generate report for specific WMO ID')
    parser.add_argument('--gdac-host', default='https', help='GDAC host (default: https)')
    
    args = parser.parse_args()
    
    try:
        # Initialize FloatBot
        bot = FloatBot(args.connection_string, args.gdac_host)
        
        # Setup database if requested
        if args.setup_db:
            bot.setup_database()
            print("Database setup completed successfully!")
            return
        
        # Process single float
        if args.wmo_id:
            success = bot.process_single_float(args.wmo_id, args.force_update)
            print(f"Processing float {args.wmo_id}: {'✓ SUCCESS' if success else '✗ FAILED'}")
            return
        
        # Process multiple floats
        if args.wmo_ids:
            results = bot.process_multiple_floats(args.wmo_ids, args.force_update)
            print("\nProcessing Results:")
            for wmo_id, success in results.items():
                print(f"  {wmo_id}: {'✓ SUCCESS' if success else '✗ FAILED'}")
            return
        
        # List floats
        if args.list_floats:
            floats = bot.get_indian_ocean_floats()
            print(f"\nFound {len(floats)} Indian Ocean floats in database:")
            print("─" * 80)
            for float_data in floats:
                print(f"WMO: {float_data['wmo_id']} | "
                      f"Cycles: {float_data.get('total_cycles', 0)} | "
                      f"Last Profile: {float_data.get('last_profile_date', 'N/A')}")
            return
        
        # Generate report
        if args.report:
            report = bot.generate_float_report(args.report)
            if report:
                print(report)
            else:
                print(f"Float {args.report} not found in database")
            return
        
        # If no specific action, show help
        parser.print_help()
        
    except Exception as e:
        logger.error(f"Application error: {e}")
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
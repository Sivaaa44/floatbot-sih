"""
Example usage of FloatBot for processing ocean float data
"""
import sys
import os
from floatbot_main import FloatBot
from config import Config

def main():
    """Example usage of FloatBot"""
    
    # Example connection string for Neon PostgreSQL
    # Replace with your actual connection details
    connection_string = "host=ep-cool-fire-a5mn57ts.us-east-2.aws.neon.tech port=5432 dbname=neondb user=neondb_owner password=YOUR_PASSWORD sslmode=require"
    
    # You can also use the config class
    # connection_string = Config.get_connection_string()
    
    try:
        # Initialize FloatBot
        print("Initializing FloatBot...")
        bot = FloatBot(connection_string, gdac_host="https")
        
        # Setup database tables
        print("Setting up database...")
        bot.setup_database()
        
        # Example WMO IDs for Indian Ocean floats
        # These are real float IDs that should have data in the Indian Ocean
        example_wmo_ids = [
            "2902746",  # Indian Ocean float
            "2902747",  # Indian Ocean float  
            "2902748",  # Indian Ocean float
        ]
        
        # Process a single float
        print(f"\nProcessing single float: {example_wmo_ids[0]}")
        success = bot.process_single_float(example_wmo_ids[0])
        
        if success:
            print("✓ Float processed successfully!")
            
            # Generate report
            print("\nGenerating float report...")
            report = bot.generate_float_report(example_wmo_ids[0])
            if report:
                print(report)
            
            # Get float info
            print("\nGetting float summary...")
            float_info = bot.get_float_info(example_wmo_ids[0])
            if float_info:
                print(f"Float has {float_info.get('total_cycles', 0)} cycles "
                      f"and {float_info.get('total_measurements', 0)} measurements")
            
            # Get cycles
            print("\nGetting cycle information...")
            cycles = bot.get_float_cycles(example_wmo_ids[0])
            print(f"Found {len(cycles)} cycles")
            
            if cycles:
                # Get measurements for first cycle
                first_cycle = cycles[0]['cycle_number']
                measurements = bot.get_cycle_measurements(example_wmo_ids[0], first_cycle)
                print(f"Cycle {first_cycle} has {len(measurements)} measurements")
        
        else:
            print("✗ Failed to process float")
        
        # List all Indian Ocean floats in database
        print("\nListing all Indian Ocean floats in database...")
        indian_ocean_floats = bot.get_indian_ocean_floats()
        print(f"Found {len(indian_ocean_floats)} Indian Ocean floats in database")
        
        for float_data in indian_ocean_floats[:5]:  # Show first 5
            print(f"  WMO: {float_data['wmo_id']} | "
                  f"Cycles: {float_data.get('total_cycles', 0)} | "
                  f"Launch: {float_data.get('launch_date', 'N/A')}")
        
        print("\n✓ Example completed successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
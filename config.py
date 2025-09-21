 config.py
"""
Configuration settings for FloatBot
"""
import os
from typing import Dict, Any

class Config:
    """Configuration class for FloatBot"""
    
    # Database configuration
    DATABASE_CONFIG = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432'),
        'database': os.getenv('DB_NAME', 'floatbot'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', ''),
    }
    
    @classmethod
    def get_connection_string(cls) -> str:
        """Generate PostgreSQL connection string"""
        config = cls.DATABASE_CONFIG
        return (f"host={config['host']} "
                f"port={config['port']} "
                f"dbname={config['database']} "
                f"user={config['user']} "
                f"password={config['password']}")
    
    # GDAC configuration
    GDAC_HOST = os.getenv('GDAC_HOST', 'https')
    
    # Indian Ocean bounds (approximate)
    INDIAN_OCEAN_BOUNDS = {
        'lon_min': 20,
        'lon_max': 120,
        'lat_min': -60,
        'lat_max': 30
    }
    
    # Processing configuration
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', '1000'))
    MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
    
    # Logging configuration
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FILE = os.getenv('LOG_FILE', 'floatbot.log')
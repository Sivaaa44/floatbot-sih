
-- FloatBot Database Schema for PostgreSQL
-- This schema supports ocean float data management with three main tables

-- 1. FLOATS table - stores metadata for each unique WMO float
CREATE TABLE IF NOT EXISTS floats (
    wmo_id VARCHAR(20) PRIMARY KEY,
    platform_type VARCHAR(100),
    platform_maker VARCHAR(255),
    float_serial_no VARCHAR(50),
    project_name VARCHAR(100),
    pi_name VARCHAR(100),
    launch_date TIMESTAMP,
    launch_latitude DECIMAL(10, 6),
    launch_longitude DECIMAL(10, 6),
    deployment_platform VARCHAR(100),
    deployment_cruise_id VARCHAR(50),
    start_date TIMESTAMP,
    end_mission_date TIMESTAMP,
    battery_type VARCHAR(100),
    firmware_version VARCHAR(50),
    dac_name VARCHAR(50),
    network_type VARCHAR(50),
    float_owner VARCHAR(100),
    operating_institution VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. CYCLES table - stores cycle/profile information for each float
CREATE TABLE IF NOT EXISTS cycles (
    id SERIAL PRIMARY KEY,
    wmo_id VARCHAR(20) NOT NULL,
    cycle_number INTEGER NOT NULL,
    profile_date TIMESTAMP,
    profile_latitude DECIMAL(10, 6),
    profile_longitude DECIMAL(10, 6),
    ocean_area VARCHAR(50),
    positioning_system VARCHAR(20),
    profile_pres_qc CHAR(1),
    profile_temp_qc CHAR(1),
    profile_psal_qc CHAR(1),
    vertical_sampling_scheme VARCHAR(50),
    config_mission_number INTEGER,
    data_mode CHAR(1),
    direction CHAR(1), -- A: ascending, D: descending
    data_centre VARCHAR(10),
    dc_reference VARCHAR(50),
    data_state_indicator VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (wmo_id) REFERENCES floats(wmo_id) ON DELETE CASCADE,
    UNIQUE(wmo_id, cycle_number)
);

-- 3. MEASUREMENTS table - stores actual sensor measurements
CREATE TABLE IF NOT EXISTS measurements (
    id SERIAL PRIMARY KEY,
    wmo_id VARCHAR(20) NOT NULL,
    cycle_number INTEGER NOT NULL,
    measurement_level INTEGER NOT NULL, -- depth level index
    pressure DECIMAL(10, 3),
    pressure_qc CHAR(1),
    pressure_adjusted DECIMAL(10, 3),
    pressure_adjusted_qc CHAR(1),
    pressure_adjusted_error DECIMAL(10, 3),
    temperature DECIMAL(10, 4),
    temperature_qc CHAR(1),
    temperature_adjusted DECIMAL(10, 4),
    temperature_adjusted_qc CHAR(1),
    temperature_adjusted_error DECIMAL(10, 4),
    salinity DECIMAL(10, 4),
    salinity_qc CHAR(1),
    salinity_adjusted DECIMAL(10, 4),
    salinity_adjusted_qc CHAR(1),
    salinity_adjusted_error DECIMAL(10, 4),
    -- Additional biogeochemical parameters (for BGC floats)
    doxy DECIMAL(10, 3), -- dissolved oxygen
    doxy_qc CHAR(1),
    doxy_adjusted DECIMAL(10, 3),
    doxy_adjusted_qc CHAR(1),
    chla DECIMAL(10, 4), -- chlorophyll-a
    chla_qc CHAR(1),
    chla_adjusted DECIMAL(10, 4),
    chla_adjusted_qc CHAR(1),
    bbp700 DECIMAL(10, 6), -- backscattering
    bbp700_qc CHAR(1),
    bbp700_adjusted DECIMAL(10, 6),
    bbp700_adjusted_qc CHAR(1),
    nitrate DECIMAL(10, 3),
    nitrate_qc CHAR(1),
    nitrate_adjusted DECIMAL(10, 3),
    nitrate_adjusted_qc CHAR(1),
    ph_in_situ_total DECIMAL(10, 4),
    ph_in_situ_total_qc CHAR(1),
    ph_in_situ_total_adjusted DECIMAL(10, 4),
    ph_in_situ_total_adjusted_qc CHAR(1),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (wmo_id, cycle_number) REFERENCES cycles(wmo_id, cycle_number) ON DELETE CASCADE,
    UNIQUE(wmo_id, cycle_number, measurement_level)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_floats_launch_date ON floats(launch_date);
CREATE INDEX IF NOT EXISTS idx_floats_location ON floats(launch_latitude, launch_longitude);
CREATE INDEX IF NOT EXISTS idx_cycles_wmo_date ON cycles(wmo_id, profile_date);
CREATE INDEX IF NOT EXISTS idx_cycles_location ON cycles(profile_latitude, profile_longitude);
CREATE INDEX IF NOT EXISTS idx_measurements_wmo_cycle ON measurements(wmo_id, cycle_number);
CREATE INDEX IF NOT EXISTS idx_measurements_pressure ON measurements(pressure);
CREATE INDEX IF NOT EXISTS idx_measurements_temperature ON measurements(temperature);
CREATE INDEX IF NOT EXISTS idx_measurements_salinity ON measurements(salinity);

-- Create a view for Indian Ocean floats (approximate bounds)
CREATE OR REPLACE VIEW indian_ocean_floats AS
SELECT f.*, 
       COUNT(c.cycle_number) as total_cycles,
       MAX(c.profile_date) as last_profile_date,
       MIN(c.profile_date) as first_profile_date
FROM floats f
LEFT JOIN cycles c ON f.wmo_id = c.wmo_id
WHERE f.launch_longitude BETWEEN 20 AND 120 
  AND f.launch_latitude BETWEEN -60 AND 30
GROUP BY f.wmo_id;
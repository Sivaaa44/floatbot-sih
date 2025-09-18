
-- Argo Float Profiles Table
CREATE TABLE IF NOT EXISTS argo_profiles (
    id SERIAL PRIMARY KEY,
    float_id INTEGER NOT NULL,
    cycle_number INTEGER NOT NULL,
    profile_date TIMESTAMP,
    latitude REAL NOT NULL,
    longitude REAL NOT NULL,
    data_mode VARCHAR(1),
    direction VARCHAR(1),
    temp_measurements INTEGER,
    psal_measurements INTEGER,
    pres_measurements INTEGER,
    max_pressure REAL,
    min_temp REAL,
    max_temp REAL,
    min_salinity REAL,
    max_salinity REAL,
    profile_file TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(float_id, cycle_number)
);

-- Index for common queries
CREATE INDEX IF NOT EXISTS idx_argo_float_id ON argo_profiles(float_id);
CREATE INDEX IF NOT EXISTS idx_argo_location ON argo_profiles(latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_argo_date ON argo_profiles(profile_date);

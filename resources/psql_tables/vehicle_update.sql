CREATE TABLE IF NOT EXISTS vehicle_update (
    uid SERIAL PRIMARY KEY,
    vehicle_id TEXT NOT NULL,
    latitude NUMERIC(10,6) NOT NULL,
    longitude NUMERIC(10,6) NOT NULL,
    location_time TIMESTAMP WITH TIME ZONE NOT NULL,
    event_time TIMESTAMP WITH TIME ZONE NOT NULL,
    organization_id VARCHAR(50) NOT NULL,
    correlation_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS vehicle_registration (
    uid SERIAL PRIMARY KEY,
    vehicle_id TEXT NOT NULL,
    event TEXT NOT NULL,
    event_time TIMESTAMP WITH TIME ZONE NOT NULL,
    organization_id VARCHAR(50) NOT NULL,
    correlation_id TEXT NOT NULL
);
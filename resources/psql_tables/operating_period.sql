CREATE TABLE IF NOT EXISTS operating_period (
    uid SERIAL PRIMARY KEY,
    operating_period_id TEXT NOT NULL UNIQUE,
    vehicle_id TEXT,
    start TIMESTAMP WITH TIME ZONE NOT NULL,
    finish TIMESTAMP WITH TIME ZONE NOT NULL,
    event TEXT NOT NULL,
    event_time TIMESTAMP WITH TIME ZONE NOT NULL,
    organization_id VARCHAR(50) NOT NULL,
    correlation_id TEXT NOT NULL
);
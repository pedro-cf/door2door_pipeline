CREATE TABLE IF NOT EXISTS operating_period_metrics (
    operating_period text PRIMARY KEY,
    time_elapsed INTERVAL,
    distance_travelled FLOAT,
    correlation_id TEXT NOT NULL
);
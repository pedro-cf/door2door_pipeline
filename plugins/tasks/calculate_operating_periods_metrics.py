import logging
import os

from utils.psql_utils import PSQL_connect
from utils.utils import generate_correlation_id

def calculate_operating_periods_metrics(**context):
    """
    Calculate metrics for operating periods.

    This function inserts data into the `operating_period_metrics` table in the
    database. The table contains the time elapsed and distance travelled for each
    operating period, as well as a correlation ID to link the data to the original
    run of the DAG.

    Parameters
    ----------
    context : dict
        The context dictionary, which is provided by Airflow and contains information
        about the current DAG run.

    Returns
    -------
    None
    """
    correlation_id = generate_correlation_id(context['dag_run'].run_id)
    logging.info(f"Running calculate_operating_periods_metrics for {correlation_id=}")
    
    with PSQL_connect() as (connect, cursor):
        cursor.execute(f"""
            INSERT INTO public.operating_period_metrics (operating_period, time_elapsed, correlation_id)
            SELECT
                operating_period_id,
                age(finish, start) AS time_elapsed,
                correlation_id
            FROM
                operating_period
            WHERE
                correlation_id = '{correlation_id}'
            ON CONFLICT (operating_period) DO UPDATE
            SET
                time_elapsed = EXCLUDED.time_elapsed;
        """)
        
        cursor.execute(f"""
            INSERT INTO operating_period_metrics (operating_period, time_elapsed, distance_travelled, correlation_id)
            SELECT
                o.operating_period_id AS operating_period,
                age(o.finish, o.start) AS time_elapsed,
                sum(v.distance) AS distance_travelled,
                o.correlation_id
            FROM
                operating_period o
                JOIN (
                    SELECT
                        vehicle_id,
                        location_time,
                        correlation_id,
                        ST_Distance(
                            ST_Point(longitude, latitude)::geography,
                            LAG(ST_Point(longitude, latitude)) OVER (PARTITION BY vehicle_id, correlation_id ORDER BY location_time)::geography
                        ) AS distance
                    FROM
                        vehicle_update
                ) v ON o.vehicle_id = v.vehicle_id AND v.location_time BETWEEN o.start AND o.finish AND o.correlation_id = v.correlation_id
            WHERE
                o.correlation_id = '{correlation_id}'
            GROUP BY
                o.operating_period_id, o.start, o.finish, o.correlation_id
            ON CONFLICT (operating_period) DO UPDATE SET
                distance_travelled = EXCLUDED.distance_travelled;
        """)

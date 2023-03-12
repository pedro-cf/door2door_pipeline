import logging
import os

from utils.psql_utils import PSQL_connect
from utils.utils import generate_correlation_id

def calculate_operating_periods(**context):
    """
    Creates operating periods for registered vehicles based on registration and deregistration events in the database.

    Args:
        context (dict): The context dictionary provided by Airflow.

    Returns:
        None

    """
    correlation_id = generate_correlation_id(context['dag_run'].run_id)
    logging.info(f"Running calculate_operating_periods for {correlation_id=}")
    
    with PSQL_connect() as (connect, cursor):
        cursor.execute(f"""
            CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
            INSERT INTO operating_period (operating_period_id, vehicle_id, start, finish, event, event_time, organization_id, correlation_id)
            SELECT 
                uuid_generate_v4() AS operating_period_id,
                r.vehicle_id,
                r.event_time AS start,
                d.event_time AS finish,
                'create' AS event,
                r.event_time,
                r.organization_id,
                r.correlation_id
            FROM 
                vehicle_registration r
                INNER JOIN vehicle_registration d ON r.vehicle_id = d.vehicle_id AND d.event = 'deregister' AND d.event_time > r.event_time
            WHERE 
                r.event = 'register' AND r.correlation_id = '{correlation_id}';
        """)

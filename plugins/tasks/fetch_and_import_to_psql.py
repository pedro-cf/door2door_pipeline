import logging
from minio import Minio
from minio.error import S3Error
import os, sys
import tempfile
import jsonschema
import json
from dateutil import parser

from utils.json_utils import read_json_file
from utils.psql_utils import PSQL_connect
from utils.utils import generate_correlation_id

def import_json_to_psql(file_path, correlation_id, connection, cursor):
    data = read_json_file(file_path)
    
    for obj in data:
        on = obj.get("on")
        event = obj.get("event")
        if on == "vehicle" and event == "update":
            vehicle_id = obj['data']['id']
            latitude = obj['data']['location']['lat']
            longitude = obj['data']['location']['lng']
            location_time = obj['data']['location']['at']
            event_time = obj['at']
            organization_id = obj['organization_id']
            
            cursor.execute(f"""
                INSERT INTO vehicle_update (vehicle_id, latitude, longitude, location_time, event_time, organization_id, correlation_id)
                VALUES ('{vehicle_id}', {latitude}, {longitude}, '{location_time}', '{event_time}', '{organization_id}', '{correlation_id}');
            """)
        elif on == "vehicle" and (event == "register" or event == "deregister"):
            vehicle_id = obj['data']['id']
            event = obj['event']
            event_time = obj['at']
            organization_id = obj['organization_id']
            
            cursor.execute(f"""
                INSERT INTO vehicle_registration (vehicle_id, event, event_time, organization_id, correlation_id)
                VALUES ('{vehicle_id}', '{event}', '{event_time}', '{organization_id}', '{correlation_id}');
            """)
        elif on == "operating_period" and (event == "create" or event == "delete"):
            operating_period_id = obj['data']['id']
            start = obj['data']['start']
            finish = obj['data']['finish']
            event = obj['event']
            event_time = obj['at']
            organization_id = obj['organization_id']
            
            cursor.execute(f"""
                INSERT INTO operating_period (operating_period_id, vehicle_id, start, finish, event, event_time, organization_id, correlation_id)
                VALUES ('{operating_period_id}', NULL, '{start}', '{finish}', '{event}', '{event_time}', '{organization_id}', '{correlation_id}');
            """)

def fetch_and_import_to_psql(**context):
    correlation_id = generate_correlation_id(context['dag_run'].run_id)
    logging.info(f"Running fetch_and_import_to_psql for {correlation_id=}")

    endpoint = os.environ.get("MINIO_ENDPOINT")
    user = os.environ.get("MINIO_USER")
    password = os.environ.get("MINIO_PASSWORD")

    # create a MinIO client
    client = Minio(
        endpoint=endpoint, access_key=user, secret_key=password, secure=False
    )

    target_date_str = context['ti'].xcom_pull(task_ids='fetch_and_validate_bucket')
    bucket_name = "datalake"
    prefix = f"{target_date_str}/"
    
    # get list of all files in the bucket
    result = list(client.list_objects(bucket_name, prefix=prefix, recursive=True))
    
    keys_to_download = [obj.object_name for obj in result]

    # loop, download and proceess each matching file
    with PSQL_connect() as (connection, cursor):
        for key in keys_to_download:
            with tempfile.NamedTemporaryFile(suffix=".json", delete=True) as tmp_file:
                try:
                    client.fget_object(bucket_name, key, tmp_file.name)
                except Exception as err:
                    print(err)
                    sys.exit(1)
                
                import_json_to_psql(tmp_file.name, correlation_id, connection, cursor)
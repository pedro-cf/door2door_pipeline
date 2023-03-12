import logging
from minio import Minio
from minio.error import S3Error
import os, sys
import tempfile
import jsonschema
import json
from dateutil import parser

from utils.json_utils import read_json_file, validate_jsonschema
from utils.utils import generate_correlation_id

VEHICLE_UPDATE_VALIDATOR = jsonschema.Draft7Validator(json.load(open("/opt/airflow/resources/jsonschemas/vehicle_update.json")))
VEHICLE_REGISTRATION_VALIDATOR = jsonschema.Draft7Validator(json.load(open("/opt/airflow/resources/jsonschemas/vehicle_registration.json")))
OPERATING_PERIOD_VALIDATOR = jsonschema.Draft7Validator(json.load(open("/opt/airflow/resources/jsonschemas/operating_period.json")))

def valid_file(file_path):
    data = read_json_file(file_path)
    res = True
    for obj in data:
        on = obj.get("on")
        event = obj.get("event")
        if on == "vehicle" and event == "update":
            if not validate_jsonschema(obj, validator=VEHICLE_UPDATE_VALIDATOR):
                print(f"Invalid schema for {obj} in {file_path}")
                res = False
        elif on == "vehicle" and (event == "register" or event == "deregister"):
            if not validate_jsonschema(obj, validator=VEHICLE_REGISTRATION_VALIDATOR):
                print(f"Invalid schema for {obj} in {file_path}")
                res = False
        elif on == "operating_period" and (event == "create" or event == "delete"):
            if not validate_jsonschema(obj, validator=OPERATING_PERIOD_VALIDATOR):
                print(f"Invalid schema for {obj} in {file_path}")
                res = False
    return res


def send_file_to_minio(file_path, endpoint, user, password, bucket, prefix, object_name=None):
    # Set up MinIO client
    minio_client = Minio(
        endpoint, access_key=user, secret_key=password, secure=False  # Disable SSL/TLS
    )

    # Set up bucket name and object name
    if not object_name:
        object_name = os.path.join(prefix, os.path.basename(file_path))

    # Upload the file to MinIO
    try:
        with open(file_path, "rb") as file:
            minio_client.put_object(
                bucket, object_name, file, os.stat(file_path).st_size
            )
        print(f"{file_path} uploaded to MinIO with {object_name}")
    except Exception as e:
        pass
        print(f"Error uploading file {file_path} to MinIO: {e}")

def fetch_and_validate_bucket(target_date, **context):
    correlation_id = generate_correlation_id(context['dag_run'].run_id)
    logging.info(f"Running fetch_and_validate_bucket_data for {target_date=} and {correlation_id=}")
    if type(target_date) is str:
        target_date = parser.parse(target_date)
    endpoint = os.environ.get("MINIO_ENDPOINT")
    user = os.environ.get("MINIO_USER")
    password = os.environ.get("MINIO_PASSWORD")

    # create a MinIO client
    client = Minio(
        endpoint=endpoint, access_key=user, secret_key=password, secure=False
    )
    
    bucket_name = "de-tech-assessment-2022"
    prefix = "data/"
    
    # get list of all files in the bucket
    result = list(client.list_objects(bucket_name, prefix=prefix, recursive=True))
    
    target_date_str = target_date.strftime("%Y-%m-%d")
    # filter files that match the given date
    keys_to_download = [
        obj.object_name
        for obj in result
        if os.path.basename(obj.object_name).startswith(
            target_date_str
        )
    ]
    
    # loop, download and proceess each matching file
    for key in keys_to_download:
        with tempfile.NamedTemporaryFile(suffix=".json", delete=True) as tmp_file:
            try:
                client.fget_object(bucket_name, key, tmp_file.name)
            except Exception as err:
                print(err)
                sys.exit(1)
                
            basename = os.path.basename(key)
            
            if valid_file(tmp_file.name):
                send_file_to_minio(
                    file_path=tmp_file.name,
                    endpoint=endpoint,
                    user=user,
                    password=password,
                    bucket="datalake",
                    prefix=f"{target_date_str}/",
                    object_name=f"{target_date_str}/{basename}"
                )
                
    return target_date_str
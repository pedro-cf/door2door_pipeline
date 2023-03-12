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
    """
    Validate the JSON schema of the given file against the appropriate schema based on the `on` and `event` fields.
    
    Args:
    - file_path (str): Path to the JSON file to validate.
    
    Returns:
    - bool: True if the schema is valid, False otherwise.
    """
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
    """
    Upload the given file to MinIO at the specified bucket and prefix with the given object name.
    
    Args:
    - file_path (str): Path to the file to upload.
    - endpoint (str): The endpoint URL of the MinIO server.
    - user (str): The access key for the MinIO server.
    - password (str): The secret key for the MinIO server.
    - bucket (str): The name of the bucket to upload the file to.
    - prefix (str): The prefix of the object key to use.
    - object_name (str): Optional. The name of the object to use for the uploaded file. Defaults to the base name of the file at `file_path`.
    
    Returns:
    - None
    """
    
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
    """
    Fetches all files from the "de-tech-assessment-2022" bucket that match the given `target_date` and validates their schemas. 
    If a schema is valid, the corresponding file is uploaded to the "datalake" bucket on MinIO.
    
    Args:
    - target_date (str or datetime.datetime): The target date to match files against. If a string, it must be in the format "%Y-%m-%d".
    - **context: Additional context that can be passed to the function.
    
    Returns:
    - str: The target date in the format "%Y-%m-%d".
    """
    
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
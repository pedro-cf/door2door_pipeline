import json
from typing import List
import logging
import jsonschema

def banana(instance, schema=None, validator=None):
    print(111111)


def read_json_file(file_path: str) -> List[dict]:
    with open(file_path, "r") as f:
        file_contents = f.read()

    # Check if the file contains a single JSON object
    try:
        single_json = json.loads(file_contents)
        return [single_json]
    except json.JSONDecodeError:
        pass

    # Check if the file contains an array of JSON objects
    try:
        array_json = json.loads(f"[{file_contents}]")
        return array_json
    except json.JSONDecodeError:
        pass

    # Otherwise, assume that the file contains multiple JSON objects with no array or comma separation
    lines = file_contents.strip().split("\n")
    return [json.loads(line) for line in lines]


def validate_jsonschema(instance, schema=None, validator=None):
    try:
        if validator:
            validator.validate(instance)
            return True
        elif schema:
            if type(schema) == str:
                schema = json.loads(schema)
            jsonschema.validate(instance=instance, schema=schema)
            return True
        else:
            raise (
                Exception("Must specify schema or validator for validate_jsonschema()")
            )
    except (
        jsonschema.exceptions.ValidationError,
        jsonschema.exceptions.SchemaError,
    ) as e:
        return False

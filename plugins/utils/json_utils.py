import json
from typing import List
import logging
import jsonschema


def read_json_file(file_path: str) -> List[dict]:
    """
    Reads a JSON file and returns a list of JSON objects.

    Args:
        file_path (str): Path to the JSON file to read.

    Returns:
        List[dict]: A list of JSON objects from the file.

    Raises:
        FileNotFoundError: If the file does not exist.
        json.JSONDecodeError: If the file is not valid JSON.
    """
    
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
    """
    Validates a JSON object against a JSON schema.

    Args:
        instance: The JSON object to validate.
        schema (Union[str, dict], optional): The JSON schema to validate against.
            Can be either a string or a dictionary representing the schema.
            If a string is passed, it will be parsed as JSON.
        validator (jsonschema.validators.validator.ValidatingDraft7Validator,
            optional): The validator to use for validation. If a validator is passed,
            it will take precedence over the schema argument.

    Returns:
        bool: True if the JSON object is valid according to the schema or validator,
            False otherwise.

    Raises:
        Exception: If neither schema nor validator is passed to the function.
    """
    
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

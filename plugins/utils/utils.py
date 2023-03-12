import uuid
import hashlib

def generate_correlation_id(run_id : str):
    return hashlib.md5(run_id.encode()).hexdigest()
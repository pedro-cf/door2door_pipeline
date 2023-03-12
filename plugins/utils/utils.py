import uuid
import hashlib

def generate_correlation_id(run_id : str):
    """
    Generates a correlation ID using the provided `run_id`.

    Parameters:
    -----------
    run_id : str
        The ID of the current run.

    Returns:
    --------
    str
        The generated correlation ID, which is a string representation of the MD5 hash
        of the provided `run_id`.
    """
    
    return hashlib.md5(run_id.encode()).hexdigest()
import os
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime

def setup_logger(log_path, max_bytes=1000000, backup_count=5):
    """
    Set up a rotating logger with detailed source information.

    Args:
        log_path (str): Path to the log file.
        max_bytes (int): Maximum size of each log file in bytes.
        backup_count (int): Number of backup files to keep.

    Returns:
        logging.Logger: Configured logger object.
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    os.makedirs(os.path.dirname(log_path), exist_ok=True)

    handler = RotatingFileHandler(log_path, maxBytes=max_bytes, backupCount=backup_count)
    
    # Create a custom formatter with additional details
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s - %(message)s'
    )
    handler.setFormatter(formatter)

    # Remove any existing handlers to avoid duplicate logs
    if logger.handlers:
        logger.handlers.clear()

    logger.addHandler(handler)

    # Add a stream handler to print logs to console as well
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger
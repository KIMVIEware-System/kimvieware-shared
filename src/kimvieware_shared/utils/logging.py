"""
Logging Configuration
"""
import logging
import sys

def setup_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Setup structured logger"""
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.handlers.clear()
    
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        f'[{name}] %(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger

def log_message_received(logger, job_id: str, queue: str):
    logger.info(f" Received job: {job_id} from {queue}")

def log_message_published(logger, job_id: str, queue: str):
    logger.info(f" Published job: {job_id} to {queue}")

def log_processing_time(logger, job_id: str, duration: float):
    logger.info(f"  Job {job_id} processed in {duration:.2f}s")

def log_error(logger, job_id: str, error: Exception):
    logger.error(f" Job {job_id} failed: {str(error)}")

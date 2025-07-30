import logging
import os
from datetime import datetime
from functools import wraps
import time

def setup_logger(name='data_pipeline'):
    """Centralized logger for Data Pipeline"""

    # Create logs directory if not exists
    os.makedirs('logs', exist_ok=True)
    
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Prevent duplicate handlers if logger already exists
    if logger.handlers:
        return logger
    
    # Create formatters
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    )
    console_formatter = logging.Formatter(
        '%(levelname)s - %(funcName)s - %(message)s'
    )
    
    # File handler - dengan timestamp + PID untuk uniqueness
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    pid = os.getpid()
    file_handler = logging.FileHandler(f'logs/pipeline_{timestamp}_{pid}.log', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(file_formatter)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger

def log_execution_time(func):
    """Decorator for logging execution time of functions"""

    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = datetime.now()
        # Get the logger from the function's module
        logger = logging.getLogger(func.__module__)
        
        start_time = time.time()
        logger.info(f"⏱️ Start timing {func.__name__} process")
        
        try:
            result = func(*args, **kwargs)
            end_time = time.time()
            duration = end_time - start_time
            logger.info(f"✅ {func.__name__} completed in {duration:.2f} seconds")
            return result
        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time
            logger.error(f"❌ {func.__name__} failed after {duration:.2f} seconds: {str(e)}")
            raise
    return wrapper
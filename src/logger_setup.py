import logging
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
LOG_FILE = os.path.join(DATA_DIR, "scraper.log")

def setup_logging():
    """
    Configures logging to write to both the console and a file.
    The log file is overwritten on each run.
    """
    os.makedirs(DATA_DIR, exist_ok=True)

    # Get the root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Clear existing handlers to avoid duplicate logs if run multiple times
    if logger.hasHandlers():
        logger.handlers.clear()

    # Create file handler (mode='w' overwrites the file)
    # This handler will have a detailed format for the log file.
    file_handler = logging.FileHandler(LOG_FILE, mode='w')
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)

    # Create console handler
    # This handler will have a simple format to mimic print() for the console.
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter('%(message)s')
    console_handler.setFormatter(console_formatter)

    # Add handlers to the root logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
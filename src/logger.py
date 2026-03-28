import logging
import logging.handlers
import os
import sys

from src import config


def setup_logging():
    # Create log directory if not exists
    os.makedirs(config.LOG_DIR, exist_ok=True)

    # Main log file path
    log_file_path = os.path.join(config.LOG_DIR, "etl.log")

    # Log format: [time] [level] [module] — message
    log_format = "[%(asctime)s] [%(levelname)-8s] [%(name)s] %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    formatter = logging.Formatter(fmt=log_format, datefmt=date_format)

    # Handler 1: Write to console (stdout)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    # Handler 2: Write to file, rotate when reaching 5MB, keep 3 old files
    file_handler = logging.handlers.RotatingFileHandler(
        filename=log_file_path,
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=3,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Remove old handlers if any (avoid duplicate logs when setup is called multiple times)
    root_logger.handlers.clear()

    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)

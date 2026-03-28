import os
from src import config
from src.logger import get_logger

logger = get_logger(__name__)


def pre_check_dir():
    dirs = [
        config.AUTO_LOADER_DIR,
        os.path.join(config.AUTO_LOADER_DIR, "s"),
        os.path.join(config.AUTO_LOADER_DIR, "f"),
        config.REJECTED_DIR,
        config.CHECKPOINT_DIR,
        config.LOG_DIR,
    ]
    for directory in dirs:
        os.makedirs(directory, exist_ok=True)
        logger.debug(f"Directory ready: {directory}")

    logger.info("All directories are ready.")




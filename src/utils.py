"""
utils.py — Các hàm tiện ích dùng chung cho toàn project

Hiện tại chứa:
  - Hàm tạo các thư mục cần thiết khi khởi động
  - Hàm in banner thông tin khi khởi động ứng dụng
"""

import os
from src.config import config
from src.log.logger import get_logger

logger = get_logger(__name__)


def pre_check_dir():
    dirs = [
        config.OUTBOX_DIR,
        config.SUCCESS_DIR,
        config.FAILURE_DIR,
        config.REJECTED_DIR,
        config.CHECKPOINT_DIR,
        config.LOG_DIR,
    ]
    for directory in dirs:
        os.makedirs(directory, exist_ok=True)
        logger.debug(f"Directory ready: {directory}")

    logger.info("All directories are ready.")




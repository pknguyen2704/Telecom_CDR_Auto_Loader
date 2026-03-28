"""
logger.py — Cấu hình hệ thống ghi log cho toàn bộ project

Mỗi lần chạy sẽ ghi log ra:
  1. Console (stdout) — để xem trực tiếp khi chạy local hoặc docker logs
  2. File log xoay vòng (rotating file) — mỗi file tối đa 5MB, giữ 3 file cũ

Cách dùng ở các file khác:
    from src.logger import get_logger
    logger = get_logger(__name__)
    logger.info("Thông tin gì đó")
"""

import logging
import logging.handlers
import os
import sys

from src import config


def setup_logging():
    """
    Thiết lập logging cho toàn bộ ứng dụng.
    """
    # Tạo thư mục log nếu chưa có
    os.makedirs(config.LOG_DIR, exist_ok=True)

    # Đường dẫn file log chính
    log_file_path = os.path.join(config.LOG_DIR, "etl.log")

    # Định dạng log: [thời gian] [mức độ] [tên module] — nội dung
    log_format = "[%(asctime)s] [%(levelname)-8s] [%(name)s] %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    formatter = logging.Formatter(fmt=log_format, datefmt=date_format)

    # Handler 1: Ghi ra console (stdout)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    # Handler 2: Ghi ra file, xoay vòng khi đạt 5MB, giữ 3 file cũ
    file_handler = logging.handlers.RotatingFileHandler(
        filename=log_file_path,
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=3,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    # Cấu hình logger gốc (root logger)
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Xóa handlers cũ nếu có (tránh ghi đôi khi gọi setup nhiều lần)
    root_logger.handlers.clear()

    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)


def get_logger(name: str) -> logging.Logger:
    """
    Lấy logger theo tên module.
    """
    return logging.getLogger(name)

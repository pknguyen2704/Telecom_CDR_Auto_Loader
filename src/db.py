"""
db.py — Manages connection to PostgreSQL

This module only does one thing: create and return a connection to PostgreSQL.
If the connection fails, it will retry 3 times before raising an error.
"""

import time
import psycopg2
from psycopg2.extensions import connection as PgConnection

from src import config
from src.logger import get_logger

logger = get_logger(__name__)

# Số lần thử lại khi kết nối thất bại
MAX_RETRY = 3
# Thời gian chờ (giây) giữa mỗi lần thử lại
RETRY_DELAY_SECONDS = 5


def get_connection() -> PgConnection:
    """
    Tạo kết nối đến PostgreSQL.

    Nếu kết nối lỗi, thử lại tối đa MAX_RETRY lần.
    Sau khi vượt số lần thử, raise Exception để dừng chương trình.
    """
    # Thử kết nối nhiều lần để chịu được lỗi mạng tạm thời
    for attempt in range(1, MAX_RETRY + 1):
        try:
            logger.info(f"Đang kết nối PostgreSQL (lần thử {attempt}/{MAX_RETRY})...")

            conn = psycopg2.connect(
                host=config.POSTGRES_HOST,
                port=config.POSTGRES_PORT,
                dbname=config.POSTGRES_DB,
                user=config.POSTGRES_USER,
                password=config.POSTGRES_PASSWORD,
                # Timeout kết nối: nếu server không phản hồi sau 10s thì bỏ qua
                connect_timeout=10,
            )

            logger.info("Connect successfully!")
            return conn

        except psycopg2.OperationalError as e:
            logger.warning(f"Connect failed {attempt}: {e}")

            # If not yet retried, wait and try again
            if attempt < MAX_RETRY:
                logger.info(f"Waiting {RETRY_DELAY_SECONDS}s then retry...")
                time.sleep(RETRY_DELAY_SECONDS)
            else:
                # Out of retries — report error to stop ETL run
                logger.error("Cannot connect to PostgreSQL after many retries!")
                raise

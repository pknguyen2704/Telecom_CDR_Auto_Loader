"""
scheduler.py — Vòng lặp lập lịch chạy ETL định kỳ

Dùng thư viện `schedule` (đơn giản, dễ hiểu):
  - Mỗi X phút chạy hàm run_etl_job() một lần
  - Nếu một lần chạy bị lỗi, ghi log và tiếp tục chờ lần tiếp theo
  - Không chạy nhiều job song song (vì chỉ có một thread)

GIẢI THÍCH VỀ `schedule`:
  Thư viện schedule không tự chạy nền — nó chỉ đăng ký job.
  Ta phải tự gọi schedule.run_pending() trong vòng while True
  để nó kiểm tra và kích hoạt các job đến hạn.
"""

import time
import schedule

from src.logger import get_logger
from src import config

logger = get_logger(__name__)


def start_scheduler(etl_job_fn):
    """
    Đăng ký và bắt đầu vòng lặp lập lịch.

    Tham số:
        etl_job_fn: hàm ETL sẽ được gọi mỗi lần lịch kích hoạt
                    (thường là hàm run_etl_job từ main.py)

    Vòng lặp này chạy mãi mãi (cho đến khi bị kill hoặc Ctrl+C).
    """
    interval = config.SCHEDULE_INTERVAL_MINUTES

    # Đăng ký job: chạy etl_job_fn mỗi X phút
    schedule.every(interval).minutes.do(etl_job_fn)

    logger.info(f"Scheduler started: will run ETL every {interval} minutes.")
    logger.info("Running ETL for the first time to test connection...")

    etl_job_fn()

    logger.info(f"Next run in {interval} minutes. Waiting...")

    # Main loop: check if any jobs are due
    while True:
        schedule.run_pending()
        time.sleep(30)
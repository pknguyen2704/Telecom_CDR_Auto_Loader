"""
main.py — Điểm khởi động của ETL service

File này làm 3 việc:
  1. Thiết lập logging, thư mục, checkpoint
  2. Định nghĩa hàm run_etl_job() — một lần chạy ETL đầy đủ
  3. Giao cho scheduler gọi run_etl_job() định kỳ

Checkpoint được cập nhật CHỈ SAU KHI ghi CSV thành công.
Nếu ghi CSV thất bại, checkpoint không đổi → lần sau sẽ thử lại đúng batch đó.
"""

import sys

from src.logger import setup_logging, get_logger
from src import checkpoint, config
from src.scheduler import start_scheduler
from src.utils import pre_check_dir
from src.etl_job import run_etl_job

# Log setup
setup_logging()
logger = get_logger(__name__)


def main():
    config_details = f"""
    - PostgreSQL   : {config.POSTGRES_HOST}:{config.POSTGRES_PORT}
    - Database     : {config.POSTGRES_DB}
    - Table        : {config.SOURCE_TABLE}
    - Batch size   : {config.BATCH_SIZE} records/run
    - Job interval : {config.SCHEDULE_INTERVAL_SECONDS} seconds
    - Auto Loader  : {config.AUTO_LOADER_DIR}
    """
    print(config_details)
    
    # Pre-check directories
    pre_check_dir()

    # Initialize checkpoint database
    checkpoint.init_checkpoint_db()
 
    logger.info("[ETL Service] Start")

    try:
        start_scheduler(run_etl_job)
    except KeyboardInterrupt:
        logger.info("[ETL Service] Stop")
        sys.exit(0)


if __name__ == "__main__":
    main()

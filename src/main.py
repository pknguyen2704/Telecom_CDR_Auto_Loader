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
import traceback

from src.logger import setup_logging, get_logger
from src import checkpoint, config
from src.db import get_connection
from src.extract import fetch_new_records
from src.transform import transform_batch
from src.csv_writer import write_csv, write_rejected_csv
from src.scheduler import start_scheduler
from src.utils import pre_check_dir

# Thiết lập logging trước tiên để có thể ghi log ngay từ đầu
setup_logging()
logger = get_logger(__name__)


def run_etl_job():
    """
    Một lần chạy ETL đầy đủ: extract → transform → load (ghi CSV).

    Hàm này được scheduler gọi định kỳ theo lịch cấu hình.
    Bất kỳ lỗi nào xảy ra đều được log và hàm kết thúc bình thường
    (không raise để không làm crash vòng lặp scheduler).
    """

    # Connect to PostgreSQL
    conn = None
    try:
        conn = get_connection()
    except Exception as e:
        logger.error(f"Can't connect to PostgreSQL: {e}")
        return
    
    # Read checkpoint
    try:
        current_checkpoint = checkpoint.read_checkpoint()
        last_id = current_checkpoint["last_id"]
        last_event_time = current_checkpoint["last_event_time"]
        logger.info(f"Current checkpoint: last_id={last_id}, last_event_time={last_event_time}")
    except Exception as e:
        logger.error(f"Can't read checkpoint: {e}")
        return 

    # Extract data from PostgreSQL
    try:
        raw_records = fetch_new_records(conn, last_id=last_id)
        logger.info(f"Total records: {len(raw_records)}")
    except Exception as e:
        logger.error(f"Error when extract data: {e}")
        traceback.print_exc()
        return
    finally:
        conn.close()

    if not raw_records:
        logger.info("No new records. End of this run.")
        return

    # Transform data
    try:
        valid_records, rejected_records = transform_batch(raw_records)
        logger.info(f"Valid records: {len(valid_records)}")
        logger.info(f"Rejected records: {len(rejected_records)}")
    except Exception as e:
        logger.error(f"Error when transform data: {e}")
        traceback.print_exc()
        return

    # Handling rejected records
    if rejected_records:
        try:
            write_rejected_csv(rejected_records)
        except Exception as e:
            # Error when writing rejected CSV should not stop the entire ETL
            logger.error(f"Error when writing rejected CSV (ignore): {e}")

    # Handling valid records
    if valid_records:
        try:
            output_path = write_csv(valid_records)  
            logger.info(f"File CSV output: {output_path}")
        except Exception as e:
            logger.error(f"Error when writing CSV output: {e}")
            traceback.print_exc()
            # DO NOT update checkpoint if CSV writing fails
            # → Next run will retry the same batch
            logger.warning("Checkpoint NOT updated due to CSV writing error.")
            return

        # Update checkpoint ONLY after successful CSV writing
        try:
            # Tìm id lớn nhất trong batch vừa xử lý thành công
            # Đây là giá trị checkpoint mới để lần sau dùng
            new_last_id = max(r["id"] for r in valid_records)
            new_last_event_time = max(r["event_time_utc"] for r in valid_records)
            checkpoint.save_checkpoint(new_last_id, new_last_event_time)
        except Exception as e:
            logger.error(f"Error when saving checkpoint: {e}")
            return
    else:
        logger.info("No valid records to write CSV.")

    # Summary of this run
    logger.info("─" * 60)
    logger.info("📊 ETL RUN RESULT:")
    logger.info(f"   - Previous checkpoint: last_id={last_id}")
    logger.info(f"   - Total records: {len(raw_records)}")
    logger.info(f"   - Valid records: {len(valid_records)}")
    logger.info(f"   - Rejected records: {len(rejected_records)}")
    if valid_records:
        new_last_id = max(r["id"] for r in valid_records)
        logger.info(f"   - New checkpoint: last_id={new_last_id}")
    logger.info("=" * 60)


def main():
    config = f"""
    - PostgreSQL   : {config.POSTGRES_HOST}:{config.POSTGRES_PORT}
    - Database     : {config.POSTGRES_DB}
    - Table        : {config.SOURCE_TABLE}
    - Batch size   : {config.BATCH_SIZE} records/run
    - Job interval : {config.SCHEDULE_INTERVAL_MINUTES} minutes
    - Outbox dir   : {config.OUTBOX_DIR}
    """
    print(config)

    # pre_check_directories
    pre_check_dir()

    # Khởi tạo checkpoint database (tạo bảng nếu chưa có)
    checkpoint.init_checkpoint_db()
 
    logger.info("[ETL Service] Start")

    try:
        start_scheduler(run_etl_job)
    except KeyboardInterrupt:
        logger.info("[ETL Service] Stop")
        sys.exit(0)


if __name__ == "__main__":
    main()

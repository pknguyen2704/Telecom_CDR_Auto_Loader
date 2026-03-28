import traceback
from src import checkpoint
from src.logger import get_logger
from src.db import get_connection
from src.extract import fetch_new_records
from src.transform import transform_batch
from src.csv_writer import write_csv, write_rejected_csv

logger = get_logger(__name__)

def run_etl_job():
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

    # Extract
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

    # Transform
    try:
        valid_records, rejected_records = transform_batch(raw_records)
        logger.info(f"Valid records: {len(valid_records)}")
        logger.info(f"Rejected records: {len(rejected_records)}")
    except Exception as e:
        logger.error(f"Error when transform data: {e}")
        traceback.print_exc()
        return

    # Loading
    # Rejected records
    if rejected_records:
        try:
            write_rejected_csv(rejected_records)
        except Exception as e:
            # Error when writing rejected CSV should not stop the entire ETL
            logger.error(f"Error when writing rejected CSV (ignore): {e}")

    # Valid records
    if valid_records:
        try:
            output_path = write_csv(valid_records)  
            logger.info(f"File CSV output: {output_path}")
        except Exception as e:
            logger.error(f"Error when writing CSV output: {e}")
            traceback.print_exc()
            # DO NOT update checkpoint if CSV writing fails
            logger.warning("Checkpoint NOT updated due to CSV writing error.")
            return

        # Update checkpoint ONLY after successful CSV writing
        try:
            # Find max id in the batch just processed successfully
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
    logger.info("ETL RUN RESULT:")
    logger.info(f"Previous checkpoint: last_id={last_id}")
    logger.info(f"Total records: {len(raw_records)}")
    logger.info(f"Valid records: {len(valid_records)}")
    logger.info(f"Rejected records: {len(rejected_records)}")
    if valid_records:
        new_last_id = max(r["id"] for r in valid_records)
        logger.info(f"New checkpoint: last_id={new_last_id}")
    logger.info("=" * 60)

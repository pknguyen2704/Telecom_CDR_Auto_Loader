import csv
import os
import shutil
import datetime

from src import config
from src.logger import get_logger

logger = get_logger(__name__)

# Column order
CSV_COLUMNS = [
    "id",
    "caller",
    "receiver",
    "device_imei",
    "event_time_unix",
    "event_time_utc",
    "duration_seconds",
    "duration_minutes",
    "call_type_code",
    "call_type_name",
    "tower_lat",
    "tower_lng",
    "country",
    "created_at",
]


def _make_filename(prefix: str, extension: str = ".csv") -> str:
    # File name using timestamp, example: telecom_cdr_20260327_143022.csv
    timestamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{timestamp}{extension}"


def write_csv(records: list[dict]) -> str | None:
    if not records:
        logger.info("No valid records, skipping CSV writing.")
        return None

    # Create directory if not exists
    os.makedirs(config.AUTO_LOADER_DIR, exist_ok=True)

    # Final filename
    final_filename = _make_filename("telecom_cdr")
    
    # Auto loader directory
    auto_loader_path = os.path.join(config.AUTO_LOADER_DIR, final_filename)
    auto_loader_tmp_path = auto_loader_path + ".tmp"

    try:
        # Write to temp file directly in autoloader directory with .tmp extension
        logger.info(f"Writing {len(records)} records to temp file: {auto_loader_tmp_path}")

        with open(auto_loader_tmp_path, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(records)

            f.flush()
            os.fsync(f.fileno())

        # Replace from .tmp to .csv to activate Auto Loader
        os.replace(auto_loader_tmp_path, auto_loader_path)
        logger.info(f"CSV file is ready for Auto Loader: {auto_loader_path}")
        
        return auto_loader_path

    except Exception as e:
        logger.error(f"Error writing CSV: {e}")
        if locals().get("auto_loader_tmp_path") and os.path.exists(auto_loader_tmp_path):
            os.remove(auto_loader_tmp_path)
            
        raise


def write_rejected_csv(rejected_records: list[dict]) -> str | None:
    if not rejected_records:
        return None

    os.makedirs(config.REJECTED_DIR, exist_ok=True)

    final_filename = _make_filename("rejected_telecom_cdr")
    final_path = os.path.join(config.REJECTED_DIR, final_filename)
    tmp_path = final_path + ".tmp"

    all_keys = list(rejected_records[0].keys()) if rejected_records else []
    if "reject_reason" not in all_keys:
        all_keys.append("reject_reason")

    try:
        with open(tmp_path, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=all_keys, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rejected_records)
            f.flush()
            os.fsync(f.fileno())

        os.replace(tmp_path, final_path)
        logger.info(f"Write {len(rejected_records)} rejected records to: {final_path}")
        return final_path

    except Exception as e:
        logger.error(f"Error writing rejected CSV: {e}")
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise

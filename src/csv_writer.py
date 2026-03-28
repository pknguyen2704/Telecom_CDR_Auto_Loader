"""
csv_writer.py — Ghi dữ liệu đã biến đổi ra file CSV

QUAN TRỌNG — TRÁNH AUTO LOADER ĐỌC FILE CHƯA HOÀN CHỈNH:
  Vấn đề: Nếu ghi thẳng vào file CSV cuối rồi quá trình ghi bị gián đoạn
  (crash, hết pin, mất điện), Auto Loader có thể đọc được file chưa đầy đủ.

  Giải pháp sử dụng MẪU: GHI VÀO FILE TẠM → ĐỔI TÊN NGUYÊN TỬ
    Bước 1: Ghi hoàn chỉnh ra file .tmp trong cùng thư mục
    Bước 2: flush() + fsync() để đảm bảo dữ liệu thực sự đã ra đĩa
    Bước 3: os.replace() — đổi tên file tạm thành file thật
            Lệnh này là nguyên tử trên Linux/macOS:
            Auto Loader hoặc thấy file cũ, hoặc thấy file mới hoàn chỉnh,
            KHÔNG BAO GIỜ thấy file đang ghi dở.
"""

import csv
import os
import datetime

from src import config
from src.logger import get_logger

logger = get_logger(__name__)

# Danh sách cột theo thứ tự muốn xuất ra CSV
# Phải khớp với dict trả về từ transform.py
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
    """
    Tạo tên file có dạng: <prefix>_YYYYMMDD_HHMMSS.csv
    Ví dụ: telecom_cdr_20240315_143022.csv
    """
    timestamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{timestamp}{extension}"


def write_csv(records: list[dict]) -> str | None:
    """
    Ghi danh sách bản ghi đã biến đổi ra file CSV trong thư mục outbox.
    Dùng mẫu: ghi file .tmp trước, xong mới đổi tên thành .csv thật.

    Tham số:
        records (list[dict]): Danh sách bản ghi đã qua transform

    Trả về:
        str: Đường dẫn file CSV cuối cùng (đã đổi tên xong)
        None: Nếu records rỗng (không ghi file)
    """
    if not records:
        logger.info("Không có bản ghi hợp lệ, bỏ qua bước ghi CSV.")
        return None

    # Tạo thư mục nếu chưa có
    os.makedirs(config.OUTBOX_DIR, exist_ok=True)

    # Tên file chính thức (Auto Loader sẽ đọc file này)
    final_filename = _make_filename("telecom_cdr")
    final_path = os.path.join(config.OUTBOX_DIR, final_filename)

    # Tên file tạm — KHÔNG để Auto Loader quét thư mục thấy file này
    # Đặt cùng thư mục với final_path để os.replace() hoạt động nguyên tử
    tmp_path = final_path + ".tmp"

    try:
        # -----------------------------------------------
        # BƯỚC 1: Ghi toàn bộ dữ liệu ra file tạm (.tmp)
        # -----------------------------------------------
        logger.info(f"Đang ghi {len(records)} bản ghi ra file tạm: {tmp_path}")

        with open(tmp_path, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(records)

            # -----------------------------------------------
            # BƯỚC 2: flush() + fsync() — đẩy data xuống đĩa vật lý
            # -----------------------------------------------
            # flush(): xả buffer của Python ra OS buffer
            f.flush()
            # fsync(): buộc OS đẩy buffer xuống ổ đĩa cứng
            # Không có bước này, khi máy crash, data vẫn có thể mất
            os.fsync(f.fileno())

        # -----------------------------------------------
        # BƯỚC 3: Đổi tên nguyên tử (atomic rename)
        # -----------------------------------------------
        # os.replace() là hoạt động nguyên tử trên cùng filesystem:
        # Auto Loader sẽ thấy file .csv CHỈ KHI bước này hoàn thành.
        os.replace(tmp_path, final_path)
        logger.info(f"✅ File CSV đã sẵn sàng cho Auto Loader: {final_path}")
        return final_path

    except Exception as e:
        # Nếu xảy ra lỗi giữa chừng, xóa file tạm nếu còn tồn tại
        logger.error(f"Lỗi khi ghi CSV: {e}")
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
            logger.info(f"Đã xóa file tạm: {tmp_path}")
        raise


def write_rejected_csv(rejected_records: list[dict]) -> str | None:
    """
    Ghi các bản ghi bị loại ra file CSV riêng trong thư mục rejected.
    Áp dụng cùng mẫu file tạm để an toàn.

    Tham số:
        rejected_records (list[dict]): Bản ghi gốc + trường 'reject_reason'

    Trả về:
        str: Đường dẫn file rejected CSV
        None: Nếu không có bản ghi bị loại
    """
    if not rejected_records:
        return None

    os.makedirs(config.REJECTED_DIR, exist_ok=True)

    final_filename = _make_filename("rejected_telecom_cdr")
    final_path = os.path.join(config.REJECTED_DIR, final_filename)
    tmp_path = final_path + ".tmp"

    # Lấy tất cả key từ bản ghi đầu tiên để làm header
    # (bản ghi rejected là raw record nên schema có thể khác với valid records)
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
        logger.info(f"⚠️  Ghi {len(rejected_records)} bản ghi bị loại ra: {final_path}")
        return final_path

    except Exception as e:
        logger.error(f"Lỗi khi ghi rejected CSV: {e}")
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise

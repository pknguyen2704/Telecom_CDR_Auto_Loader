"""
transform.py — Áp dụng các biến đổi dữ liệu và lọc bản ghi không hợp lệ

Bước này thực hiện:
  1. Chuyển event_time từ Unix timestamp (số) sang chuỗi datetime UTC
  2. Map call_type: "MO" → "Outgoing", "MT" → "Incoming"
  3. Tính duration_minutes = duration_seconds / 60
  4. Chuyển tower_lat và tower_lng sang dạng số thực (float)
  5. Cắt khoảng trắng thừa ở các cột chuỗi quan trọng
  6. Kiểm tra các trường bắt buộc — bản ghi thiếu thì bỏ qua và log lại
"""

import datetime
from src.logger import get_logger

logger = get_logger(__name__)

# Bảng map call_type — thêm vào đây nếu có loại cuộc gọi mới
CALL_TYPE_MAP = {
    "MO": "Outgoing",   # Mobile Originating — cuộc gọi đi
    "MT": "Incoming",   # Mobile Terminating — cuộc gọi đến
}

# Danh sách cột bắt buộc phải có giá trị
# Bản ghi thiếu bất kỳ cột nào trong danh sách này sẽ bị loại
REQUIRED_FIELDS = ["id", "caller", "receiver", "event_time", "call_type"]


def transform_one(raw: dict) -> dict | None:
    """
    Biến đổi một bản ghi thô từ PostgreSQL thành bản ghi đã xử lý.

    Tham số:
        raw (dict): Một bản ghi thô từ bảng telecom_cdr

    Trả về:
        dict: Bản ghi đã biến đổi, sẵn sàng để ghi CSV
        None: Nếu bản ghi không hợp lệ (thiếu trường, sai kiểu dữ liệu,...)
    """

    # -------------------------------------------------------
    # BƯỚC 1: Kiểm tra các trường bắt buộc
    # -------------------------------------------------------
    for field in REQUIRED_FIELDS:
        value = raw.get(field)
        # Coi None và chuỗi rỗng đều là thiếu dữ liệu
        if value is None or str(value).strip() == "":
            logger.warning(
                f"Bỏ qua bản ghi id={raw.get('id', 'UNKNOWN')}: "
                f"Thiếu trường bắt buộc '{field}'"
            )
            return None

    # -------------------------------------------------------
    # BƯỚC 2: Cắt khoảng trắng các trường chuỗi quan trọng
    # -------------------------------------------------------
    caller   = str(raw.get("caller", "")).strip()
    receiver = str(raw.get("receiver", "")).strip()
    device_imei  = str(raw.get("device_imei", "")).strip()
    call_type_code = str(raw.get("call_type", "")).strip().upper()
    country  = str(raw.get("country", "")).strip()

    # -------------------------------------------------------
    # BƯỚC 3: Chuyển event_time từ Unix timestamp sang datetime UTC
    # -------------------------------------------------------
    # event_time trong nguồn là số nguyên (Unix timestamp, tính bằng giây)
    # Ví dụ: 1700000000 → "2023-11-14 22:13:20"
    try:
        event_time_unix = int(raw["event_time"])
        # fromtimestamp(ts, tz=UTC) đảm bảo không bị ảnh hưởng bởi múi giờ máy chủ
        event_time_utc_dt = datetime.datetime.fromtimestamp(
            event_time_unix, tz=datetime.timezone.utc
        )
        # Định dạng chuỗi dễ đọc cho người
        event_time_utc_str = event_time_utc_dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError, OSError) as e:
        logger.warning(
            f"Bỏ qua bản ghi id={raw.get('id')}: "
            f"event_time không hợp lệ: {raw.get('event_time')} — {e}"
        )
        return None

    # -------------------------------------------------------
    # BƯỚC 4: Map call_type code sang tên tiếng Anh dễ hiểu
    # -------------------------------------------------------
    # Nếu không tìm thấy trong bảng map, giữ nguyên giá trị gốc
    call_type_name = CALL_TYPE_MAP.get(call_type_code, call_type_code)

    # -------------------------------------------------------
    # BƯỚC 5: Tính duration_minutes (làm tròn 2 chữ số thập phân)
    # -------------------------------------------------------
    try:
        duration_seconds = int(raw.get("duration_seconds", 0) or 0)
        duration_minutes = round(duration_seconds / 60, 2)
    except (ValueError, TypeError):
        duration_seconds = 0
        duration_minutes = 0.0

    # -------------------------------------------------------
    # BƯỚC 6: Chuyển tower_lat, tower_lng sang float
    # -------------------------------------------------------
    # Nếu không chuyển được, để None (sẽ ghi thành chuỗi rỗng trong CSV)
    tower_lat = _safe_float(raw.get("tower_lat"), field_name="tower_lat", record_id=raw.get("id"))
    tower_lng = _safe_float(raw.get("tower_lng"), field_name="tower_lng", record_id=raw.get("id"))

    # -------------------------------------------------------
    # BƯỚC 7: Lấy created_at — nếu không có thì dùng thời điểm hiện tại
    # -------------------------------------------------------
    created_at_raw = raw.get("created_at")
    if created_at_raw is not None:
        # Chuyển sang chuỗi nếu là datetime object (psycopg2 auto-parse datetime)
        created_at_str = str(created_at_raw)
    else:
        # Dùng thời điểm ETL chạy làm created_at
        created_at_str = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # -------------------------------------------------------
    # BƯỚC 8: Ghép kết quả lại thành dict
    # -------------------------------------------------------
    # Tên cột theo đúng yêu cầu DM
    return {
        "id":                   raw["id"],
        "caller":               caller,
        "receiver":             receiver,
        "device_imei":          device_imei,
        "event_time_unix":      event_time_unix,
        "event_time_utc":       event_time_utc_str,
        "duration_seconds":     duration_seconds,
        "duration_minutes":     duration_minutes,
        "call_type_code":       call_type_code,
        "call_type_name":       call_type_name,
        "tower_lat":            tower_lat if tower_lat is not None else "",
        "tower_lng":            tower_lng if tower_lng is not None else "",
        "country":              country,
        "created_at":           created_at_str,
    }


def transform_batch(raw_records: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Biến đổi toàn bộ một batch bản ghi thô.

    Trả về một tuple gồm:
        - valid_records: danh sách bản ghi đã biến đổi hợp lệ → ghi vào CSV chính
        - rejected_records: danh sách bản ghi không hợp lệ → ghi vào CSV rejected
    """
    valid_records = []
    rejected_records = []

    for raw in raw_records:
        result = transform_one(raw)
        if result is not None:
            valid_records.append(result)
        else:
            # Thêm thông tin reject vào bản ghi gốc để ghi vào file rejected
            raw_copy = dict(raw)
            raw_copy["reject_reason"] = "Validation failed — xem log để biết chi tiết"
            rejected_records.append(raw_copy)

    return valid_records, rejected_records


def _safe_float(value, field_name: str = "", record_id=None) -> float | None:
    """
    Hàm tiện ích: chuyển giá trị sang float, trả về None nếu không chuyển được.
    Log cảnh báo nhưng không raise Exception — bản ghi vẫn được xử lý tiếp.
    """
    if value is None or str(value).strip() == "":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        logger.warning(
            f"Không thể chuyển '{field_name}' = '{value}' sang float "
            f"(id={record_id}). Để trống."
        )
        return None

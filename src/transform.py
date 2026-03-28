import datetime
from src.logger import get_logger

logger = get_logger(__name__)

# Call map table
CALL_TYPE_MAP = {
    "MO": "Outgoing",
    "MT": "Incoming",
}

# Required fields
REQUIRED_FIELDS = ["id", "caller", "receiver", "event_time", "call_type"]

def _safe_float(value, field_name: str = "", record_id=None) -> float | None:
    if value is None or str(value).strip() == "":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        logger.warning(
            f"Cannot convert '{field_name}' = '{value}' to float "
            f"(id={record_id}). Leave empty."
        )
        return None

def transform_one(raw: dict) -> dict | None:
    # Check require field
    for field in REQUIRED_FIELDS:
        value = raw.get(field)
        # Check None and empty string
        if value is None or str(value).strip() == "":
            logger.warning(
                f"Bỏ qua bản ghi id={raw.get('id', 'UNKNOWN')}: "
                f"Thiếu trường bắt buộc '{field}'"
            )
            return None
    # Strip whitespace from important string fields
    caller   = str(raw.get("caller", "")).strip()
    receiver = str(raw.get("receiver", "")).strip()
    device_imei  = str(raw.get("device_imei", "")).strip()
    call_type_code = str(raw.get("call_type", "")).strip().upper()
    country  = str(raw.get("country", "")).strip()

    # Convert event_time from Unix timestamp to datetime UTC
    try:
        event_time_unix = int(raw["event_time"])
        event_time_utc_dt = datetime.datetime.fromtimestamp(
            event_time_unix, tz=datetime.timezone.utc
        )
        # Format string for readability
        event_time_utc_str = event_time_utc_dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError, OSError) as e:
        logger.warning(
            f"Bỏ qua bản ghi id={raw.get('id')}: "
            f"event_time không hợp lệ: {raw.get('event_time')} — {e}"
        )
        return None

    # Map call_type code
    # If not found in map, keep original value
    call_type_name = CALL_TYPE_MAP.get(call_type_code, call_type_code)

    # Calculate duration_minutes (rounded to 2 decimal places)
    try:
        duration_seconds = int(raw.get("duration_seconds", 0) or 0)
        duration_minutes = round(duration_seconds / 60, 2)
    except (ValueError, TypeError):
        duration_seconds = 0
        duration_minutes = 0.0

    # Convert tower_lat, tower_lng to float
    # If not found in map, keep original value
    tower_lat = _safe_float(raw.get("tower_lat"), field_name="tower_lat", record_id=raw.get("id"))
    tower_lng = _safe_float(raw.get("tower_lng"), field_name="tower_lng", record_id=raw.get("id"))

    # Get created_at — if not found, use current time
    created_at_raw = raw.get("created_at")
    if created_at_raw is not None:
        # Convert to string if datetime object (psycopg2 auto-parse datetime)
        created_at_str = str(created_at_raw)
    else:
        # Use ETL run time as created_at
        created_at_str = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # Return result
    # Column names follow DM requirements
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
    valid_records = []
    rejected_records = []

    for raw in raw_records:
        result = transform_one(raw)
        if result is not None:
            valid_records.append(result)
        else:
            # Add reject information to the original record to write to the rejected file
            raw_copy = dict(raw)
            raw_copy["reject_reason"] = "Validation failed — see log for details"
            rejected_records.append(raw_copy)

    return valid_records, rejected_records



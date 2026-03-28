"""
extract.py — Lấy dữ liệu mới từ PostgreSQL dựa trên checkpoint

Chỉ lấy các bản ghi có id > last_id (từ checkpoint).
Giới hạn số bản ghi mỗi lần bằng BATCH_SIZE để tránh quá tải bộ nhớ.

GHI CHÚ VỀ CỘT id:
  Khi kiểm tra bảng nguồn, nên chạy:
    SELECT column_name, data_type FROM information_schema.columns
    WHERE table_name = 'telecom_cdr';

  Nếu bảng có cột `id` SERIAL / BIGINT tăng dần, chiến lược này hoạt động tốt nhất.
  Nếu không có cột `id`, hãy thay bằng cột khác tăng dần (ví dụ: created_at).
"""

from psycopg2.extensions import connection as PgConnection

from src import config
from src.logger import get_logger

logger = get_logger(__name__)


def fetch_new_records(conn: PgConnection, last_id: int) -> list[dict]:
    """
    Lấy các bản ghi mới từ bảng nguồn PostgreSQL.

    Tham số:
        conn (PgConnection): kết nối PostgreSQL đang mở
        last_id (int): id lớn nhất đã xử lý lần trước (từ checkpoint)

    Trả về:
        list[dict]: danh sách bản ghi, mỗi bản ghi là một dict
                    với key là tên cột.
    """
    # Câu lệnh SQL: chỉ lấy bản ghi có id > last_id
    # ORDER BY id ASC đảm bảo xử lý theo thứ tự thời gian
    # LIMIT giới hạn số bản ghi mỗi lần để tránh file CSV quá lớn
    query = f"""
        SELECT *
        FROM {config.SOURCE_TABLE}
        WHERE id > %(last_id)s
        ORDER BY id ASC
        LIMIT %(batch_size)s
    """

    params = {
        "last_id": last_id,
        "batch_size": config.BATCH_SIZE,
    }

    logger.info(f"Đang truy vấn dữ liệu mới: id > {last_id}, limit = {config.BATCH_SIZE}")

    cursor = conn.cursor()
    try:
        cursor.execute(query, params)

        # Get column names from cursor
        column_names = [desc[0] for desc in cursor.description]

        # Convert each row to dict
        rows = []
        for row in cursor.fetchall():
            row_dict = dict(zip(column_names, row))
            rows.append(row_dict)

        logger.info(f"Got {len(rows)} new records from PostgreSQL")
        return rows

    finally:
        cursor.close()

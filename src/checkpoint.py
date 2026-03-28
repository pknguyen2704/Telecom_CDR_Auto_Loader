"""
checkpoint.py — Lưu và đọc checkpoint để tránh load dữ liệu trùng lặp

Checkpoint là "dấu vị trí" mà ETL đã đọc đến.
Lần chạy sau sẽ đọc từ vị trí đó trở đi, không đọc lại từ đầu.

CHIẾN LƯỢC CHECKPOINT:
  - Dùng kết hợp (last_id, last_event_time) làm checkpoint.
  - Ưu tiên dùng `id` vì đây là khóa tăng dần, ổn định, không thay đổi.
  - `event_time` dùng làm tie-breaker: nếu có nhiều bản ghi cùng id
    (lý thuyết không xảy ra, nhưng an toàn hơn khi dùng cả hai).

  Câu query sẽ là:
    WHERE id > :last_id
    ORDER BY id ASC
    LIMIT :batch_size

TẠI SAO DÙNG SQLite THAY VÌ FILE JSON?
  - SQLite viết nguyên tử (atomic), không bị hỏng khi chương trình crash giữa chừng.
  - JSON file đơn giản hơn nhưng dễ bị corrupt nếu write thất bại.
  - SQLite dễ kiểm tra bằng DB browser, tiện cho debug.

HẠN CHẾ:
  - Nếu `id` không tăng dần ổn định (ví dụ UUID ngẫu nhiên),
    chiến lược này cần điều chỉnh lại.
  - Nếu có bản ghi cũ được insert với id nhỏ hơn last_id,
    chúng sẽ bị bỏ qua. Nhưng với CDC/event table thì thường không xảy ra.
"""

import sqlite3
import os

from src import config
from src.logger import get_logger

logger = get_logger(__name__)


def _get_db_path() -> str:
    return os.path.join(config.CHECKPOINT_DIR, config.CHECKPOINT_DB_NAME)


def init_checkpoint_db():
    """
    Initialize SQLite checkpoint database if it doesn't exist.
    Create checkpoint table with a single row (id=1).
    Call this function once when starting the program.
    """
    os.makedirs(config.CHECKPOINT_DIR, exist_ok=True)
    db_path = _get_db_path()

    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        # Create checkpoint db
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS checkpoint (
                id            INTEGER PRIMARY KEY,
                last_id       INTEGER NOT NULL,
                last_event_time TEXT NOT NULL,
                updated_at    TEXT NOT NULL
            )
        """)

        # Insert default row if table is empty
        cursor.execute("""
            INSERT OR IGNORE INTO checkpoint (id, last_id, last_event_time, updated_at)
            VALUES (1, 0, '2000-01-01 00:00:00', datetime('now'))
        """)

        conn.commit()
        logger.info(f"Checkpoint DB is ready at: {db_path}")

    finally:
        conn.close()


def read_checkpoint() -> dict:
    """
    Đọc checkpoint hiện tại từ SQLite.
    Trả về dict với hai key: last_id và last_event_time.
    """
    db_path = _get_db_path()
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT last_id, last_event_time FROM checkpoint WHERE id = 1")
        row = cursor.fetchone()

        if row is None:
            return {"last_id": 0, "last_event_time": "2020-01-01 00:00:00"}

        return {
            "last_id": row[0],
            "last_event_time": row[1],
        }
    finally:
        conn.close()


def save_checkpoint(last_id: int, last_event_time: str):
    """
    Lưu checkpoint mới vào SQLite sau khi ETL xử lý thành công.

    Tham số:
        last_id (int): id lớn nhất trong batch vừa xử lý
        last_event_time (str): event_time lớn nhất dạng chuỗi ISO

    SQLite đảm bảo ghi nguyên tử nên không lo file bị corrupt.
    """
    db_path = _get_db_path()
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE checkpoint
            SET last_id = ?, last_event_time = ?, updated_at = datetime('now')
            WHERE id = 1
        """, (last_id, last_event_time))
        conn.commit()
        logger.info(f"Checkpoint updated: last_id={last_id}, last_event_time={last_event_time}")
    finally:
        conn.close()

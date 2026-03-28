import sqlite3
import os
from src import config
from src.logger import get_logger

logger = get_logger(__name__)


def _get_db_path() -> str:
    return os.path.join(config.CHECKPOINT_DIR, config.CHECKPOINT_DB_NAME)


def init_checkpoint_db():
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

from psycopg2.extensions import connection as PgConnection

from src import config
from src.logger import get_logger

logger = get_logger(__name__)


def fetch_new_records(conn: PgConnection, last_id: int) -> list[dict]:
    # SQL query: only fetch records with id > last_id
    # ORDER BY id ASC ensures processing in chronological order
    # LIMIT limits the number of records per run to avoid large CSV files
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

    logger.info(f"Extracting new records: id > {last_id}, limit = {config.BATCH_SIZE}")

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

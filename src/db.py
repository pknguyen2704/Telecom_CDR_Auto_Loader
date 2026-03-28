import time
import psycopg2
from psycopg2.extensions import connection as PgConnection

from src import config
from src.logger import get_logger

logger = get_logger(__name__)

# Setup retry connection
MAX_RETRY = 3
RETRY_DELAY_SECONDS = 5


def get_connection() -> PgConnection:
    for attempt in range(1, MAX_RETRY + 1):
        try:
            logger.info(f"Connecting to PostgreSQL (attempt {attempt}/{MAX_RETRY})...")

            conn = psycopg2.connect(
                host=config.POSTGRES_HOST,
                port=config.POSTGRES_PORT,
                dbname=config.POSTGRES_DB,
                user=config.POSTGRES_USER,
                password=config.POSTGRES_PASSWORD,
                connect_timeout=10,
            )

            logger.info("Connect successfully!")
            return conn

        except psycopg2.OperationalError as e:
            logger.warning(f"Connect failed {attempt}: {e}")

            # If not yet retried, wait and try again
            if attempt < MAX_RETRY:
                logger.info(f"Waiting {RETRY_DELAY_SECONDS}s then retry...")
                time.sleep(RETRY_DELAY_SECONDS)
            else:
                # Out of retries — report error to stop ETL run
                logger.error("Cannot connect to PostgreSQL after many retries!")
                raise

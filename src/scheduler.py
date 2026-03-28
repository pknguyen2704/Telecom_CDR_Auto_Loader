import time
import schedule

from src.logger import get_logger
from src import config

logger = get_logger(__name__)


def start_scheduler(etl_job_fn):
    interval = config.SCHEDULE_INTERVAL_SECONDS

    schedule.every(interval).seconds.do(etl_job_fn)

    logger.info(f"Scheduler started: will run ETL every {interval} seconds.")

    etl_job_fn()

    # Main loop: check if any jobs are due
    while True:
        schedule.run_pending()
        time.sleep(2)
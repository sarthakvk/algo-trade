from datetime import datetime
import os
from ticks_collector.s3_utils import upload_parquet_folder_to_s3
from ticks_collector.ticker import TICKS_DIR, Ticker, is_trading_day
from app_config import ZONEINFO
from time import sleep
import logging
from apscheduler.schedulers.background import BackgroundScheduler

logger = logging.getLogger(__name__)


def drop_ticks_from_disk_after_verification():
    """Verify that today's ticks have been uploaded to S3, then delete local files."""

    today_dir_name = f"date={(datetime.now().strftime('%Y-%m-%d'))}"
    today_dir_path = os.path.join(TICKS_DIR, today_dir_name)

    # Here you would implement verification logic to check S3 for the files.
    # For simplicity, we'll assume verification is successful.
    verification_successful = False  # TODO: Implement actual verification logic

    if verification_successful:
        import shutil

        shutil.rmtree(today_dir_path)
        logger.info(f"Deleted local ticks data at {today_dir_path} after verification.")
    else:
        logger.warning(
            f"Verification failed for ticks data at {today_dir_path}. Not deleting."
        )


def upload_ticks_to_s3(scheduler: BackgroundScheduler):
    today_dir_name = f"date={(datetime.now().strftime('%Y-%m-%d'))}"
    today_dir_path = os.path.join(TICKS_DIR, today_dir_name)
    logger.info(f"Uploading ticks from {today_dir_path} to S3...")
    upload_parquet_folder_to_s3(today_dir_path)

    scheduler.add_job(
        drop_ticks_from_disk_after_verification, id="drop_ticks_after_verification"
    )


def ticks_collector_job(scheduler: BackgroundScheduler):
    """Job to start ticker, stop ticker"""
    if not is_trading_day():
        logger.info("Today is not a trading day. Exiting ticks collector job.")
        return

    ticker = Ticker()

    now = datetime.now(ZONEINFO)
    start_cutoff = now.replace(hour=9, minute=14, second=0, microsecond=0)
    stop_cutoff = now.replace(hour=15, minute=31, second=0, microsecond=0)

    # If script started after 9:14 but before 15:31, start immediately
    start_immediately = start_cutoff < now < stop_cutoff

    if start_immediately:
        logger.info("Starting ticks collector job immediately.")
        ticker.start()
    else:
        logger.info("Waiting for scheduled start time...")
        while True:
            sleep(1)
            cur_time = datetime.now(tz=ZONEINFO).time()
            if cur_time.hour == 9 and cur_time.minute == 14:
                ticker.start()
                logger.info("Ticker started.")
                break

    while True:
        sleep(1)
        cur_time = datetime.now(tz=ZONEINFO).time()
        if cur_time.hour == 15 and cur_time.minute == 35:
            ticker.stop()
            logger.info("Ticker stopped.")
            break

    scheduler.add_job(upload_ticks_to_s3, id="upload_ticks_to_s3")

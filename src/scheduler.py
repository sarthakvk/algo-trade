from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from ticks_collector.ticker import Ticker, is_trading_day, TICKS_DIR
import logging
from logging.handlers import RotatingFileHandler
from zoneinfo import ZoneInfo
import os
from ticks_collector.s3_utils import upload_parquet_folder_to_s3

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.propagate = False

os.makedirs(os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs"), exist_ok=True)

file_handler = RotatingFileHandler(
    os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs", "scheduler.log"),
    maxBytes=5_000_000,      # 5 MB
    backupCount=15           # keep up to 15 log files
)
stream_handler = logging.StreamHandler()

formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
stream_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)

ZONEINFO = ZoneInfo("Asia/Kolkata")


def upload_ticks_to_s3():
    today_dir_name = f"date={(datetime.now().strftime('%Y-%m-%d'))}"
    today_dir_path = os.path.join(TICKS_DIR, today_dir_name)
    logger.info(f"Uploading ticks from {today_dir_path} to S3...")
    upload_parquet_folder_to_s3(today_dir_path)


def schedule_ticker_jobs(scheduler: BlockingScheduler):
    if is_trading_day():
        ticker = Ticker()
        scheduler.add_job(ticker.start, 'cron', hour=9, minute=12, second=0, timezone=ZONEINFO, id='start_ticker')
        scheduler.add_job(ticker.stop, 'cron', hour=15, minute=32, second=0, timezone=ZONEINFO, id='stop_ticker')
        scheduler.add_job(upload_ticks_to_s3, 'cron', hour=15, minute=35, second=0, timezone=ZONEINFO, id='upload_ticks')
        logger.info("Scheduled ticker start and stop jobs for today.")

def schedule_ticker_jobs_immediate(scheduler: BlockingScheduler):
    if is_trading_day():
        ticker = Ticker()
        ticker.start()
        scheduler.add_job(ticker.stop, 'cron', hour=15, minute=32, second=0, timezone=ZONEINFO, id='stop_ticker')
        scheduler.add_job(upload_ticks_to_s3, 'cron', hour=15, minute=35, second=0, timezone=ZONEINFO, id='upload_ticks')
        logger.info("Scheduled ticker stop job for today after immediate start.")

def main():
    scheduler = BlockingScheduler(timezone=ZONEINFO)
    scheduler.add_job(schedule_ticker_jobs, 'cron', args=[scheduler], hour=9, minute=11, second=0, timezone=ZONEINFO, id='daily_scheduler')
    
    now = datetime.now(ZONEINFO)
    start_cutoff = now.replace(hour=9, minute=11, second=0, microsecond=0)
    stop_cutoff = now.replace(hour=15, minute=32, second=0, microsecond=0)

    # If today is trading day and script started after 9:11 but before 15:32
    if is_trading_day() and start_cutoff < now < stop_cutoff:
        logger.info("Startup after scheduled time -> scheduling jobs for today.")
        schedule_ticker_jobs_immediate(scheduler)
    
    logger.info("Scheduler started. Waiting for jobs...")
    scheduler.start()


if __name__ == "__main__":
    main()

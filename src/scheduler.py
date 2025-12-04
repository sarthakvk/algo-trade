from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from ticks_collector.ticker import Ticker, is_trading_day
import logging
from logging.handlers import RotatingFileHandler
from zoneinfo import ZoneInfo
import os

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

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

logger.propagate = False

ZONEINFO = ZoneInfo("Asia/Kolkata")

def test_task():
    logger.info("Test task executed.")

def schedule_ticker_jobs(scheduler: BlockingScheduler):
    if is_trading_day():
        ticker = Ticker()
        scheduler.add_job(ticker.start, 'cron', hour=9, minute=12, second=0, timezone=ZONEINFO, id='start_ticker')
        scheduler.add_job(ticker.stop, 'cron', hour=15, minute=32, second=0, timezone=ZONEINFO, id='stop_ticker')
        logger.info("Scheduled ticker start and stop jobs for today.")


def main():
    scheduler = BlockingScheduler(timezone=ZONEINFO)
    scheduler.add_job(schedule_ticker_jobs, 'cron', args=[scheduler], hour=9, minute=11, second=0, timezone=ZONEINFO, id='daily_scheduler')
    
    now = datetime.now(ZONEINFO)
    start_cutoff = now.replace(hour=9, minute=11, second=0, microsecond=0)
    stop_cutoff = now.replace(hour=15, minute=32, second=0, microsecond=0)

    # If today is trading day and script started after 9:11 but before 15:32
    if is_trading_day() and start_cutoff < now < stop_cutoff:
        logger.info("Startup after scheduled time -> scheduling jobs for today.")
        schedule_ticker_jobs(scheduler)
    
    logger.info("Scheduler started. Waiting for jobs...")
    scheduler.start()


if __name__ == "__main__":
    main()

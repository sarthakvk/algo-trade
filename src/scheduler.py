from apscheduler.schedulers.background import BlockingScheduler
from ticks_collector.ticker import Ticker, is_trading_day
import logging
from zoneinfo import ZoneInfo

logging.basicConfig(level=logging.WARN)
logger = logging.getLogger(__name__)

ZONEINFO = ZoneInfo("Asia/Kolkata")

def test_task():
    logger.info("Test task executed.")

def schedule_ticker_jobs(scheduler: BlockingScheduler):
    if is_trading_day():
        ticker = Ticker()
        scheduler.add_job(ticker.start, 'cron', hour=9, minute=13, second=0, timezone=ZONEINFO, id='start_ticker')
        scheduler.add_job(ticker.stop, 'cron', hour=15, minute=32, second=0, timezone=ZONEINFO, id='stop_ticker')
        logger.info("Scheduled ticker start and stop jobs for today.")


def main():
    scheduler = BlockingScheduler(timezone=ZONEINFO)
    schedule_ticker_jobs(scheduler)

    scheduler.start()


if __name__ == "__main__":
    main()

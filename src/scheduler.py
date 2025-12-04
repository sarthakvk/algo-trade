import atexit
from datetime import datetime
import signal
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

PID_FILE = "/tmp/scheduler.pid"
LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")

os.makedirs(
    LOG_DIR, exist_ok=True
)

file_handler = RotatingFileHandler(
    os.path.join(LOG_DIR, "scheduler.log"),
    maxBytes=5_000_000,  # 5 MB
    backupCount=15,  # keep up to 15 log files
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
        scheduler.add_job(
            ticker.start,
            "cron",
            hour=9,
            minute=12,
            second=0,
            timezone=ZONEINFO,
            id="start_ticker",
        )
        scheduler.add_job(
            ticker.stop,
            "cron",
            hour=15,
            minute=32,
            second=0,
            timezone=ZONEINFO,
            id="stop_ticker",
        )
        scheduler.add_job(
            upload_ticks_to_s3,
            "cron",
            hour=15,
            minute=35,
            second=0,
            timezone=ZONEINFO,
            id="upload_ticks",
        )
        logger.info("Scheduled ticker start and stop jobs for today.")


def schedule_ticker_jobs_immediate(scheduler: BlockingScheduler):
    if is_trading_day():
        ticker = Ticker()
        ticker.start()
        scheduler.add_job(
            ticker.stop,
            "cron",
            hour=15,
            minute=32,
            second=0,
            timezone=ZONEINFO,
            id="stop_ticker",
        )
        scheduler.add_job(
            upload_ticks_to_s3,
            "cron",
            hour=15,
            minute=35,
            second=0,
            timezone=ZONEINFO,
            id="upload_ticks",
        )
        logger.info(
            "Scheduled ticker stop job and s3 upload for today after immediate start."
        )


def main():
    scheduler = BlockingScheduler(timezone=ZONEINFO)
    scheduler.add_job(
        schedule_ticker_jobs,
        "cron",
        args=[scheduler],
        hour=9,
        minute=11,
        second=0,
        timezone=ZONEINFO,
        id="daily_scheduler",
    )

    now = datetime.now(ZONEINFO)
    start_cutoff = now.replace(hour=9, minute=11, second=0, microsecond=0)
    stop_cutoff = now.replace(hour=15, minute=32, second=0, microsecond=0)

    # If today is trading day and script started after 9:11 but before 15:32
    if is_trading_day() and start_cutoff < now < stop_cutoff:
        logger.info("Startup after scheduled time -> scheduling jobs for today.")
        schedule_ticker_jobs_immediate(scheduler)

    logger.info("Scheduler started. Waiting for jobs...")
    scheduler.start()


def remove_pid():
    if os.path.exists(PID_FILE):
        os.remove(PID_FILE)


def write_pid():
    with open(PID_FILE, "w") as f:
        f.write(str(os.getpid()))


def exit_if_already_running():
    if os.path.exists(PID_FILE):
        with open(PID_FILE) as f:
            pid = int(f.read())
        try:
            os.kill(pid, 0)  # check if alive
            print("scheduler already running")
            exit(0)
        except ProcessLookupError:
            # stale pid
            os.remove(PID_FILE)


if __name__ == "__main__":
    exit_if_already_running()
    # register atexit handler to remove PID file
    atexit.register(remove_pid)

    # handle kill signals gracefully
    signal.signal(signal.SIGTERM, lambda *args: exit(0))
    signal.signal(signal.SIGINT, lambda *args: exit(0))

    write_pid()

    main()

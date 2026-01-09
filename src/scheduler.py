from app_config import ZONEINFO
from jobs import ticks_collector_job
import logging
from apscheduler.schedulers.background import BackgroundScheduler

logger = logging.getLogger(__name__)
# ---------------------------
# SCHEDULER MAIN APPLICATION LOGIC
# ---------------------------

# ---------------------------
# JOB SCHEDULING
# ---------------------------
scheduler = BackgroundScheduler(timezone=ZONEINFO)
scheduler.start()
logger.info("Scheduler started with timezone %s", ZONEINFO)

jobs_schedule = [
    {
        "func": ticks_collector_job,
        "trigger": "cron",
        "hour": 9,
        "minute": 10,
        "replace_existing": True,
        "timezone": ZONEINFO,
        "id": "ticks_collector_job",
        "args": [scheduler],
    },
]


def schedule_jobs(scheduler: BackgroundScheduler):
    """Schedule jobs in the scheduler based on the provided jobs_schedule list."""
    for job in jobs_schedule:
        if job.get("id") is not None:
            replace_existing = job.get("replace_existing", False)

            existing_job = scheduler.get_job(job["id"])

            if existing_job is not None and not replace_existing:
                logger.info(
                    f"Job with id {job['id']} already exists. Skipping scheduling."
                )
                continue

        logger.info(f"Scheduling job: {job['id']}")
        func = job.pop("func")
        scheduler.add_job(func, **job)

from contextlib import asynccontextmanager
import datetime
import fastapi
from scheduler import schedule_jobs
from ticks_collector.s3_utils import upload_parquet_folder_to_s3
from ticks_collector.ticker import TICKS_DIR
from logging_config import *
from scheduler import scheduler, schedule_jobs

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: fastapi.FastAPI):
    """Lifespan context manager to start and stop the scheduler with the FastAPI app."""

    try:
        schedule_jobs(scheduler)
        yield
    finally:
        scheduler.shutdown()
        logger.info("Scheduler shut down with FastAPI app")


app = fastapi.FastAPI(lifespan=lifespan)


@app.post("/tasks/trigger-s3-upload", tags=["Tasks"], summary="Trigger S3 upload task")
async def trigger_s3_upload(date: datetime.date | None = None):
    """Trigger upload of ticks data inside TICKS_DIR to S3"""
    # If a manual upload job already exists (pending or running), don't add another
    existing = scheduler.get_job("manual_s3_upload")
    if existing is not None:
        raise fastapi.HTTPException(
            status_code=409,
            detail="Manual S3 upload is already scheduled or running",
        )
    if date is not None:
        date_str = date.strftime("%Y-%m-%d")
        dir_to_upload = TICKS_DIR + f"/date={date_str}"
    else:
        dir_to_upload = TICKS_DIR

    job = scheduler.add_job(
        upload_parquet_folder_to_s3,
        args=[dir_to_upload],
        id="manual_s3_upload",
        max_instances=1,
        replace_existing=False,
    )
    return {"message": "S3 upload queued", "job_id": job.id}


@app.get("/jobs/{job_id}", tags=["Tasks"], summary="Get job status")
async def get_job_status(job_id: str):
    """Get the status of a scheduled job by its ID"""
    job = scheduler.get_job(job_id)
    if job is None:
        raise fastapi.HTTPException(status_code=404, detail="Job not found")

    return {
        "id": job.id,
        "next_run_time": job.next_run_time,
        "trigger": str(job.trigger),
        "max_instances": job.max_instances,
        "misfire_grace_time": job.misfire_grace_time,
        "pending": job.pending,
    }


@app.get("/jobs", tags=["Jobs"], summary="Get all scheduled jobs")
async def get_scheduled_jobs():
    """Get all scheduled jobs"""
    jobs = scheduler.get_jobs()

    jobs_json = []
    for job in jobs:
        jobs_json.append(
            {
                "id": job.id,
                "next_run_time": job.next_run_time,
                "trigger": str(job.trigger),
                "max_instances": job.max_instances,
                "misfire_grace_time": job.misfire_grace_time,
                "pending": job.pending,
            }
        )

    return {"jobs": jobs_json}

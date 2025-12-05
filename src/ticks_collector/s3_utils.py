import os
import boto3
from .ticker import TICKS_DIR
import pathlib
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

logger = logging.getLogger(__name__)


def upload_file_to_s3(file_path: str, bucket_name: str, s3_key: str):
    s3_client = boto3.client("s3")
    s3_client.upload_file(file_path, bucket_name, s3_key)
    logger.info(f"Uploaded {file_path} to s3://{bucket_name}/{s3_key}")


def upload_parquet_folder_to_s3(dir: str):
    # Implement the logic to upload the ticks folder to S3
    bucket_name = "ticks-data-bucket"
    dir: pathlib.Path = pathlib.Path(dir)
    ticks_root_dir = pathlib.Path(TICKS_DIR)

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for file_path in dir.rglob("*.parquet"):
            s3_key = file_path.relative_to(ticks_root_dir)
            futures.append(
                executor.submit(
                    upload_file_to_s3, file_path, bucket_name, s3_key.as_posix()
                )
            )
        total_files = len(futures)
        logger.info(f"Starting upload of {total_files} files to S3 from {dir}")

        for idx, future in enumerate(as_completed(futures)):
            try:
                future.result()
                logger.info(f"Uploaded {idx + 1}/{total_files} files to S3")
            except Exception as e:
                logger.error(f"Error uploading file: {e}")
                raise e

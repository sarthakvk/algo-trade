import os
import boto3
from .ticker import TICKS_DIR
import pathlib
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import hashlib

logger = logging.getLogger(__name__)


def upload_file_to_s3(file_path: str, bucket_name: str, s3_key: str):
    s3_client = boto3.client("s3")
    s3_client.upload_file(file_path, bucket_name, s3_key)
    logger.info(f"Uploaded {file_path} to s3://{bucket_name}/{s3_key}")


def _sha256_of_file(file_path: pathlib.Path) -> str:
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def upload_parquet_folder_to_s3(dir: str):
    # Implement the logic to upload the ticks folder to S3
    bucket_name = "ticks-data-bucket"
    dir: pathlib.Path = pathlib.Path(dir)
    ticks_root_dir = pathlib.Path(TICKS_DIR)

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for file_path in dir.rglob("*.parquet"):
            s3_key = file_path.relative_to(ticks_root_dir)
            # Upload parquet file
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


def verify_parquet_folder_uploaded_to_s3(dir: str) -> bool:
    """Verify all local parquet files under `dir` exist in S3.

    Keys are computed relative to `TICKS_DIR`, matching `upload_parquet_folder_to_s3`.
    Returns True if every local parquet file has a corresponding S3 object.
    """
    bucket_name = "ticks-data-bucket"
    s3_client = boto3.client("s3")

    dir_path = pathlib.Path(dir)
    ticks_root_dir = pathlib.Path(TICKS_DIR)

    local_files = list(dir_path.rglob("*.parquet"))
    if not local_files:
        logger.warning(f"No parquet files found to verify in {dir_path}")
        return False

    logger.info(f"Verifying {len(local_files)} parquet files exist in s3://{bucket_name}")

    all_ok = True
    for fp in local_files:
        key = fp.relative_to(ticks_root_dir).as_posix()
        local_sha = _sha256_of_file(fp)
        # Stream object from S3 and compute SHA-256 without loading into memory
        try:
            obj = s3_client.get_object(Bucket=bucket_name, Key=key)
            body = obj["Body"]
            h = hashlib.sha256()
            for chunk in body.iter_chunks(chunk_size=1024 * 1024):
                if chunk:
                    h.update(chunk)
            remote_sha = h.hexdigest()
            if remote_sha != local_sha:
                logger.error(
                    f"Checksum mismatch for {key}: local {local_sha} != S3 {remote_sha}"
                )
                all_ok = False
            else:
                logger.debug(f"Checksum verified for s3://{bucket_name}/{key}")
        except Exception as e:
            logger.error(f"Error streaming S3 object {key} for verification: {e}")
            all_ok = False

    if all_ok:
        logger.info(f"Verification successful for folder {dir_path}")
    else:
        logger.warning(f"Verification failed for one or more files under {dir_path}")

    return all_ok

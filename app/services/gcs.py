from dotenv import load_dotenv
import os
import sys
from pathlib import Path

from datetime import datetime
import crc32c
import base64

from google.cloud import storage
from google.cloud.exceptions import NotFound

from app.services.audit import log_audit_entry, check_audit_log
from app.services.retry import retry_with_backoff
from ..utils.logger import get_logger, log_execution

logger = get_logger(__name__)

def compute_local_crc32c(file_path: str) -> str:
    """
    Computes CRC32C hash of a file.

    Args:
        file_path (str): Path to the file for which to compute the hash

    Returns:
        str: Base-64 encoded CRC32C hash of the file
    """
    hash_crc = crc32c.crc32c(b"")
    with open(file_path, "rb") as f: #read-only binary mode
        for chunk in iter(lambda: f.read(8192), b""): # Compute hash in chunks, same end result as hashing entire file at once
            hash_crc = crc32c.crc32c(chunk, hash_crc)
    return base64.b64encode(hash_crc.to_bytes(4, byteorder="big")).decode("utf-8")

def blob_check(blob: storage.Blob, local_crc: str) -> bool:
    """
    Checks if blob exists on GCS and the CRC32C hash matches.

    Args:
        blob (storage.Blob): GCS Blob object to check
        local_crc (str): Base-64 encoded CRC32C hash of the local file

    Returns:
        bool: True if blob exists and hash matches, False otherwise

    Raises:
        NotFound: If the blob does not exist in GCS
    """
    try:
        blob.reload()
        return blob.crc32c == local_crc
    except NotFound:
        return False

@retry_with_backoff(max_retries=3, base_delay=2, max_delay=16)
def upload_single_file(blob: storage.Blob, file_path: str, local_crc: str, chunk_size_mb: int = 256) -> None:
    """
    Upload a single file to GCS with retry logic, using chunked uploads to limit memory usage.

    Args:
        blob (storage.Blob): GCS Blob object representing the destination file
        file_path (str): Path to the local file to upload
        local_crc (str): Base-64 encoded CRC32C hash of the local file for integrity check
        chunk_size_mb (int): Size of each upload chunk in megabytes (default: 256MB)

    Raises:
        ValueError: If the uploaded file's hash does not match the local hash after upload
    """
    blob.chunk_size = chunk_size_mb * 1024 * 1024
    blob.upload_from_filename(str(file_path), checksum="crc32c")

    # Check if uploaded blob's hash matches our locally calculated hash
    if not blob_check(blob, local_crc):
        raise ValueError(f"GCS hash does not match local after upload.\nFile: {file_path}\nLocal: {local_crc}\nGCS: {blob.crc32c}")

@log_execution
def upload_parquet_files(
        bucket_name: str,
        local_folder: str,
        source_system: str,
        table_name: str,
        chunk_size_mb: int = 256,
    ) -> dict:
    """
    Upload .parquet files to GCS Bucket in chunks with checksum, retry logic, logging, and audit trail.

    Args:
        bucket_name (str): Name of the GCS bucket to upload to
        local_folder (str): Path to the local folder containing .parquet files
        source_system (str): Name of the source system for audit logging
        table_name (str): Name of the table for audit logging
        chunk_size_mb (int): Size of each upload chunk in megabytes (default: 256MB)

    Returns:
        dict: Summary of upload results, including lists of uploaded, skipped, and failed files, and batch summary statistics
    
    Raises:
        FileNotFoundError: If no .parquet files are found in the local folder
    """

    # Connect to GCS, fetch bucket
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    local_path = Path(local_folder)
    start_datetime = datetime.now()
    parquet_files = list(local_path.glob("**/*.parquet"))
    results = {
        "uploaded": [],
        "skipped": [],
        "failed": [],
        "batch_summary": {
            "total_files": len(parquet_files),
            "total_bytes": 0,
            "start_time": start_datetime
        }
    }
    if not parquet_files:
        raise FileNotFoundError(f"No .parquet files found in {local_folder}")

    logger.info(f"Starting upload batch for {source_system}.{table_name}: {len(parquet_files)} files")
    for file_idx, file_path in enumerate(parquet_files, 1):
        file_name = file_path.name
        local_crc = None
        try:
            logger.debug(f"Processing file {file_idx}/{len(parquet_files)}: {file_name}")
            # Compute local CRC32C
            local_crc = compute_local_crc32c(str(file_path))
            # Check audit log to prevent duplicate processing
            if check_audit_log("audit_log.json", local_crc, source_system, table_name):
                logger.info(f"File {file_name} already processed according to audit log, skipping.")
                results["skipped"].append({"name": file_path.name, "hash": local_crc})
                continue
            # Construct GCS blob path
            relative_path = file_path.relative_to(local_path)
            partition_parts = [p for p in relative_path.parts[:-1]]  # get hive partition parts (year, month), exclude the filename
            partition = "/".join(partition_parts) if partition_parts else "none"
            blob_name = f"{source_system}/{relative_path.as_posix()}"
            blob = bucket.blob(blob_name)
            # If blob already uploaded to bucket and has matching hash, skip this file since the GCS copy is the same, don't need to reupload
            if blob_check(blob, local_crc):
                logger.info(f"File {file_name} already exists in GCS with matching hash, skipping.")
                results["skipped"].append({"name": file_path.name, "hash": local_crc})
                continue
            # Upload file with retry logic
            logger.info(f"Uploading {file_name} to {blob_name}")
            upload_single_file(blob, file_path, local_crc, chunk_size_mb)
            results["uploaded"].append({"name": file_path.name, "hash": local_crc})

            log_audit_entry("audit_log.json", {
                    "timestamp": datetime.now(),
                    "source_system": source_system,
                    "table_name": table_name,
                    "partition": partition,
                    "file_name": file_path.name,
                    "hash": local_crc,
                    "blob_name": blob_name
                })

        except Exception as e:
            # Unexpected error (e.g., can't compute CRC32C, upload failed)
            logger.error(f"Unexpected error processing {file_name}: {type(e).__name__}: {e}")
            results["failed"].append({"name": file_name, "hash": local_crc, "error": str(e)})
    
    # Add end time and summary
    
    end_datetime: datetime = datetime.now()
    time_delta = end_datetime - start_datetime
    logger.info(f"GCS Upload took {time_delta.total_seconds():.2f}s total.")

    results["batch_summary"]["end_time"] = end_datetime.isoformat()
    results["batch_summary"]["uploaded_count"] = len(results["uploaded"])
    results["batch_summary"]["skipped_count"] = len(results["skipped"])
    results["batch_summary"]["failed_count"] = len(results["failed"])
    
    logger.info(
        f"Batch upload complete for {source_system}.{table_name}: "
        f"Uploaded={results['batch_summary']['uploaded_count']}, "
        f"Skipped={results['batch_summary']['skipped_count']}, "
        f"Failed={results['batch_summary']['failed_count']}"
    )
    
    return results


def fetch_creds():
    """Load credentials to connect to GCP"""
    load_dotenv()
    creds_path = Path(os.getenv("GOOGLE_APPLICATION_CREDENTIALS")).resolve()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(creds_path)

    proceed = ""
    while True:
        proceed = input("Credentials loaded. Proceed with connection? (y/n) ").lower()
        if proceed in ["y", "yes"]:
            break
        elif proceed in ["n", "no"]:
            sys.exit()

if __name__ == "__main__":
    from app.services.conversion import convert_to_parquet

    # convert_to_parquet("data/")

    fetch_creds()

    parquets_folder_path = "data/"
    results = upload_parquet_files("jbc-sales-bucket", parquets_folder_path, "jbc", "sales")
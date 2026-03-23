from dotenv import load_dotenv
import os
import sys
from pathlib import Path

from datetime import datetime
import time
import logging
import json
import hashlib
import base64

from google.cloud import storage
from google.cloud.exceptions import NotFound

from app.services.retry import retry_with_backoff

logger = logging.getLogger(__name__)

def log_audit_entry(log_file: str, entry: dict):
    """
    Append an audit entry to a JSON log file.
    """
    try:
        log_path = Path(log_file)
        if log_path.exists():
            with open(log_path, 'r') as f:
                logs = json.load(f)
        else:
            logs = []
        logs.append(entry)
        with open(log_path, 'w') as f:
            json.dump(logs, f, indent=4, default=str)
    except Exception as e:
        logger.error(f"Failed to write audit log: {e}")

def check_audit_log(log_file: str, file_hash: str, source_system: str, table_name: str) -> bool:
    """
    Check if a file with the given hash has already been processed for the source_system and table_name.
    Returns True if already processed (skip), False otherwise.
    """
    try:
        log_path = Path(log_file)
        if not log_path.exists():
            return False
        with open(log_path, 'r') as f:
            logs = json.load(f)
        for entry in logs:
            if entry.get("source_system") == source_system and entry.get("table_name") == table_name:
                for uploaded in entry.get("uploaded_files", []):
                    if isinstance(uploaded, dict) and uploaded.get("hash") == file_hash:
                        return True
                for skipped in entry.get("skipped_files", []):
                    if isinstance(skipped, dict) and skipped.get("hash") == file_hash:
                        return True
        return False
    except Exception as e:
        logger.error(f"Failed to check audit log: {e}")
        return False

def compute_local_md5(file_path: str) -> str:
    """
    Returns base-64 encoded MD5
    """
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f: #read-only binary mode
        for chunk in iter(lambda: f.read(8192), b""):
            hash_md5.update(chunk) # according to docs, this will be the same as making a hash of the entire file at once
    return base64.b64encode(hash_md5.digest()).decode("utf-8")

def blob_check(blob, local_md5: str) -> bool:
    """
    Checks if blob exists on GCS and the MD5 hash matches
    """
    try:
        blob.reload()
        return blob.md5_hash == local_md5
    except NotFound:
        return False

def upload_parquet_files(
        bucket_name: str,
        local_folder: str,
        source_system: str,
        table_name: str,
        chunk_size_mb: int = 256,
    ) -> dict:
    """
    Upload .parquet files to GCS Bucket with checksum
    """

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    now = datetime.now(datetime.timezone.utc)
    partition = f"year={now.year}/month={now.month:02d}"

    parquet_files = list(Path(local_folder).glob("**/*.parquet"))
    results = {
        "uploaded": [],
        "skipped": [],
        "failed": [],
        "batch_summary": {
            "total_files": len(parquet_files),
            "total_bytes": 0,
            "start_time": now.isoformat()
        }
    }

    if not parquet_files:
        raise FileNotFoundError(f"No .parquet files found in {local_folder}")
    
    logger.info(f"Starting upload batch for {source_system}.{table_name}: {len(parquet_files)} files")
    
    for file_idx, file_path in enumerate(parquet_files, 1):
        file_name = file_path.name
        try:
            logger.debug(f"Processing file {file_idx}/{len(parquet_files)}: {file_name}")
            
            # Compute local MD5
            start_time = time.time()
            local_md5 = compute_local_md5(str(file_path))
            
            # Check audit log to prevent duplicate processing
            if check_audit_log("audit_log.json", local_md5, source_system, table_name):
                logger.info(f"File {file_name} already processed according to audit log, skipping.")
                results["skipped"].append({"name": file_path.name, "hash": local_md5})
                continue
            
            # Construct GCS blob path
            blob_name = f"{source_system}/{table_name}/{partition}/{file_name}"
            blob = bucket.blob(blob_name)
        except Exception as e:
            pass

        # If blob already uploaded to bucket and has matching hash, skip this file since the GCS copy is the same, don't need to reupload
        if blob_check(blob, local_md5):
            # TODO log it
            results["skipped"].append({"name": file_path.name, "hash": local_md5})
            continue

        # Upload file with retry logic
        try:
            logger.info(f"Uploading {file_name} to {blob_name}")

            # after upload verify new GCS MD5 still matches, otherwise the file on GCS does not match our local copy
            blob.reload()
            if blob.md5_hash != local_md5:
                raise ValueError(f"GCS hash does not match local, filepath: {file_path}"
                                 f"Local: {local_md5}\nGCS: {blob.md5_hash}")
            results["uploaded"].append({"name": file_path.name, "hash": local_md5})

        except ValueError as e:
            # TODO log it, mismatched hash
            results["failed"].append({"name": file_path.name, "hash": local_md5, "error": str(e)})
        except Exception as e:
            # Unexpected error (e.g., can't compute MD5)
            logger.error(f"Unexpected error processing {file_name}: {type(e).__name__}: {e}")
            results["failed"].append({"name": file_name, "hash": local_md5 if 'local_md5' in locals() else None, "error": str(e)})
    
    # Add end time and summary
    results["batch_summary"]["end_time"] = datetime.utcnow().isoformat()
    results["batch_summary"]["uploaded_count"] = len(results["uploaded"])
    results["batch_summary"]["skipped_count"] = len(results["skipped"])
    results["batch_summary"]["failed_count"] = len(results["failed"])
    
    logger.info(
        f"Batch upload complete for {source_system}.{table_name}: "
        f"Uploaded={results['batch_summary']['uploaded_count']}, "
        f"Skipped={results['batch_summary']['skipped_count']}, "
        f"Failed={results['batch_summary']['failed_count']}"
    )
    
    # Audit log entry
    audit_entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "source_system": source_system,
        "table_name": table_name,
        "bucket_name": bucket_name,
        "partition": partition,
        "uploaded_files": results["uploaded"],
        "skipped_files": results["skipped"],
        "failed_files": results["failed"],
        "batch_summary": results["batch_summary"]
    }
    log_audit_entry("audit_log.json", audit_entry)
    
    return results


def main():
    """Connect to our GCS Bucket"""
    load_dotenv()
    creds_path = Path(os.getenv("GOOGLE_APPLICATION_CREDENTIALS")).resolve()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(creds_path)
    proceed = input("Proceed? ").lower()
    if proceed not in ["y", "yes"]:
        sys.exit()


if __name__ == "__main__":
    main()
    parquets_folder_path = "" #TODO Replace with parquet file locations 
    results = upload_parquet_files("jbc-sales-bucket", parquets_folder_path, "stg_sales")
    #results that should already be logged for report
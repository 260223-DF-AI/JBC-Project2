from pathlib import Path
import json
import logging

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

def check_audit_log(log_file: str, file_hash: str, source_system: str, partition: str) -> bool:
    """
    Check if a file with the given hash has already been processed for the source_system and partition.
    Returns True if already processed (skip), False otherwise.
    """
    try:
        log_path = Path(log_file)
        if not log_path.exists():
            return False
        with open(log_path, 'r') as f:
            logs = json.load(f)
        for entry in logs:
            if entry.get("source_system") == source_system and entry.get("partition") == partition:
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
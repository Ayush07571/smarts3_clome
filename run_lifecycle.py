import boto3
import yaml
from datetime import datetime, timedelta, timezone  # <-- Add missing imports

# Try importing helpers (safe fallback if not present)
try:
    from s3_manager import s3, list_objects_pagewise
    from lifecycle import older_than, matches_prefixes, move_to_archive
except ImportError:
    s3 = None
    list_objects_pagewise = None
    older_than = None
    matches_prefixes = None
    move_to_archive = None

# Load lifecycle config
with open("lifecycle_config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Connect to LocalStack
if s3 is None:
    s3 = boto3.client(
        "s3",
        endpoint_url="http://localhost:4566",  # LocalStack endpoint
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1"
    )

BUCKET = config["bucket"]
PAGE_SIZE = config.get("options", {}).get("page_size", 1000)
DRY_RUN = config.get("options", {}).get("dry_run", True)

def list_objects(bucket, prefix=""):
    """List S3 objects (fallback if s3_manager not available)."""
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj

def is_older_than(obj, days):
    """Check if object is older than given days."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    return obj["LastModified"] < cutoff

def default_move_to_archive(s3_client, bucket, key, archive_prefix):
    """Move object by copy + delete (fallback)."""
    new_key = f"{archive_prefix}{key}"
    s3_client.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": key}, Key=new_key)
    s3_client.delete_object(Bucket=bucket, Key=key)
    return new_key

def apply_lifecycle(config_path="lifecycle_config.yaml"):
    cfg = yaml.safe_load(open(config_path, "r"))
    bucket = cfg["bucket"]
    dry_run = cfg.get("options", {}).get("dry_run", True)
    page_size = cfg.get("options", {}).get("page_size", 1000)

    # Use fallback functions if helpers are missing
    _list_objects = list_objects_pagewise if list_objects_pagewise else lambda b, p, ps: list_objects(b, p)
    _older_than = older_than if older_than else is_older_than
    _matches_prefixes = matches_prefixes if matches_prefixes else lambda k, inc, exc: k.startswith(inc) and not any(k.startswith(e) for e in exc)
    _move_to_archive = move_to_archive if move_to_archive else default_move_to_archive

    for rule in cfg["rules"]:
        include_prefix = rule.get("include_prefix", "")
        exclude_prefixes = rule.get("exclude_prefixes", [])
        days = rule["older_than_days"]
        action = rule.get("action", None)
        if not action:
            continue

        archive_prefix = rule.get("archive_prefix", "archive/")

        candidates = []
        for obj in _list_objects(bucket, include_prefix, page_size):
            key = obj["Key"]
            if not _matches_prefixes(key, include_prefix, exclude_prefixes):
                continue
            if not _older_than(obj, days):
                continue

            # 🔑 NEW: check suffix filter
            suffix = rule.get("filter", {}).get("suffix")
            if suffix and not key.endswith(suffix):
                continue

            candidates.append((key, obj))

        print(f"Rule '{rule['name']}': {len(candidates)} object(s) matched")

        for key, _ in candidates:
            if dry_run:
                if action == "move":
                    print(f"[DRY-RUN] Would move {key} → {archive_prefix}{key}")
                elif action == "delete":
                    print(f"[DRY-RUN] Would delete {key}")
            else:
                if action == "move":
                    new_key = _move_to_archive(s3, bucket, key, archive_prefix)
                    print(f"Moved {key} → {new_key}")
                elif action == "delete":
                    s3.delete_object(Bucket=bucket, Key=key)
                    print(f"Deleted {key}")

if __name__ == "__main__":
    apply_lifecycle()
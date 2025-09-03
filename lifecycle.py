from datetime import datetime, timezone, timedelta  

def filter_old_files(objects, days=1):
    """Return files older than `days`"""
    cutoff = datetime.now() - timedelta(days=days)
    old_files = []
    for obj in objects:
        if obj['LastModified'].replace(tzinfo=None) < cutoff:
            old_files.append(obj['Key'])
    return old_files

def older_than(obj, days):
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    return obj["LastModified"] < cutoff

def matches_prefixes(key, include_prefix, exclude_prefixes):
    if include_prefix and not key.startswith(include_prefix):
        return False
    return not any(key.startswith(p) for p in exclude_prefixes or [])

def move_to_archive(s3, bucket, key, archive_prefix):
    copy_source = {"Bucket": bucket, "Key": key}
    archive_key = f"{archive_prefix}{key}"
    # Copy
    s3.copy_object(Bucket=bucket, CopySource=copy_source, Key=archive_key)
    # Delete original
    s3.delete_object(Bucket=bucket, Key=key)
    return archive_key

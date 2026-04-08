import os
import subprocess
import datetime
import boto3

RETENTION_DAYS = 30

def get_config():
    return {
        "DATABASE_URL": os.getenv("DATABASE_URL"),
        "R2_ENDPOINT_URL": os.getenv("R2_ENDPOINT_URL"),
        "R2_ACCESS_KEY_ID": os.getenv("R2_ACCESS_KEY_ID"),
        "R2_SECRET_ACCESS_KEY": os.getenv("R2_SECRET_ACCESS_KEY"),
        "R2_BUCKET_NAME": os.getenv("R2_BUCKET_NAME"),
    }

def list_backups():
    """Lists available backups in the R2 bucket."""
    config = get_config()
    validate_config(config, ["R2_ENDPOINT_URL", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET_NAME"])

    s3 = get_s3_client(config)
    backups = list_all_backup_objects(s3, config["R2_BUCKET_NAME"])

    # Sort by last modified descending
    backups = sorted(backups, key=lambda x: x['LastModified'], reverse=True)
    return [{
        "filename": b['Key'],
        "size": b['Size'],
        "last_modified": b['LastModified'].strftime("%Y-%m-%d %H:%M:%S")
    } for b in backups]

def validate_config(config, keys):
    missing = [k for k in keys if not config.get(k)]
    if missing:
        raise ValueError(f"Missing environment variables: {', '.join(missing)}")

def get_s3_client(config):
    return boto3.client(
        's3',
        endpoint_url=config["R2_ENDPOINT_URL"],
        aws_access_key_id=config["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=config["R2_SECRET_ACCESS_KEY"]
    )

def list_all_backup_objects(s3, bucket_name):
    backups = []
    continuation_token = None

    while True:
        params = {"Bucket": bucket_name}
        if continuation_token:
            params["ContinuationToken"] = continuation_token

        response = s3.list_objects_v2(**params)
        backups.extend(response.get("Contents", []))

        if not response.get("IsTruncated"):
            break

        continuation_token = response.get("NextContinuationToken")

    return backups

def delete_old_backups(retention_days=RETENTION_DAYS):
    """Deletes backups older than the retention window from the R2 bucket."""
    config = get_config()
    validate_config(config, ["R2_ENDPOINT_URL", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET_NAME"])

    s3 = get_s3_client(config)
    backups = list_all_backup_objects(s3, config["R2_BUCKET_NAME"])
    cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=retention_days)

    deleted_files = []
    for backup in backups:
        if backup["LastModified"] < cutoff:
            s3.delete_object(Bucket=config["R2_BUCKET_NAME"], Key=backup["Key"])
            deleted_files.append(backup["Key"])

    return {
        "deleted_count": len(deleted_files),
        "deleted_files": deleted_files,
        "retention_days": retention_days
    }

def perform_backup():
    """
    Dumps the database to a file, uploads to R2, and cleans up.
    Returns: (success: bool, message: str)
    """
    config = get_config()
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"backup_{timestamp}.sql"
    filepath = f"/tmp/{filename}"
    
    print(f"Starting backup: {filename}")

    # 1. Dump Database
    try:
        # Use pg_dump with the full URL
        command = f"pg_dump '{config['DATABASE_URL']}' -f {filepath}"
        subprocess.run(command, shell=True, check=True, capture_output=True)
    except subprocess.CalledProcessError as e:
        return False, f"Dump failed: {e.stderr.decode()}"

    # 2. Upload to R2
    try:
        validate_config(config, ["R2_ENDPOINT_URL", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET_NAME"])

        s3 = get_s3_client(config)
        
        with open(filepath, "rb") as f:
            s3.upload_fileobj(f, config["R2_BUCKET_NAME"], filename)

        file_size = os.path.getsize(filepath)
        cleanup_result = delete_old_backups()
        
        # Cleanup
        os.remove(filepath)
        return True, (
            f"Backup successful ({round(file_size/(1024*1024), 2)} MB). "
            f"Removed {cleanup_result['deleted_count']} backup(s) older than {cleanup_result['retention_days']} days."
        )
        
    except Exception as e:
        return False, f"Upload failed: {str(e)}"

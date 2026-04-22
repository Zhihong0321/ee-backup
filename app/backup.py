import os
import subprocess
import datetime
import tempfile
from urllib.parse import urlparse

import boto3
import psycopg2

RETENTION_DAYS = 30
PLAYTEST_DATABASE_ENV_KEYS = (
    "PLAYTEST_DATABASE_URL",
    "PLAYTEST_DATABASE",
    "TEST_DATABASE_URL",
)
PLAYTEST_RESTORE_FLAG_KEYS = (
    "ENABLE_PLAYTEST_RESTORE",
    "ENABLE_TEST_RESTORE",
)


def get_first_env_value(*keys):
    for key in keys:
        value = os.getenv(key)
        if value:
            return value
    return None

def get_config():
    return {
        "DATABASE_URL": os.getenv("DATABASE_URL"),
        "PLAYTEST_DATABASE_URL": get_first_env_value(*PLAYTEST_DATABASE_ENV_KEYS),
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


def normalize_database_target(url):
    parsed = urlparse(url)
    port = parsed.port
    if port is None and parsed.scheme in ("postgres", "postgresql"):
        port = 5432

    return {
        "scheme": parsed.scheme,
        "host": (parsed.hostname or "").lower(),
        "port": port,
        "database": parsed.path.lstrip("/"),
    }


def same_database_target(url_a, url_b):
    if not url_a or not url_b:
        return False
    return normalize_database_target(url_a) == normalize_database_target(url_b)


def restore_is_enabled():
    value = get_first_env_value(*PLAYTEST_RESTORE_FLAG_KEYS, "ENABLE_RESTORE")
    return (value or "").strip().lower() == "true"


def mask_secret(secret):
    if not secret:
        return "N/A"
    if len(secret) <= 4:
        return "*" * len(secret)
    return f"{secret[:2]}{'*' * (len(secret) - 4)}{secret[-2:]}"


def get_playtest_restore_blocker(config=None):
    config = config or get_config()

    if not config.get("PLAYTEST_DATABASE_URL"):
        return (
            "Playtest database is not configured. Set PLAYTEST_DATABASE_URL or "
            "PLAYTEST_DATABASE first."
        )

    if not restore_is_enabled():
        return (
            "Playtest restore is disabled. Set ENABLE_PLAYTEST_RESTORE=true "
            "to allow destructive restore actions."
        )

    if same_database_target(config.get("PLAYTEST_DATABASE_URL"), config.get("DATABASE_URL")):
        return "Playtest target matches the source database. Restore is blocked for safety."

    return None


def get_latest_backup_name():
    backups = list_backups()
    if not backups:
        return None
    return backups[0]["filename"]


def download_backup_to_tempfile(filename, config):
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".sql")
    temp_file.close()

    s3 = get_s3_client(config)
    s3.download_file(config["R2_BUCKET_NAME"], filename, temp_file.name)
    return temp_file.name


def build_subprocess_error(prefix, error):
    stderr = (error.stderr or "").strip()
    stdout = (error.stdout or "").strip()
    details = stderr or stdout or str(error)
    return f"{prefix}: {details}"


def perform_restore(filename=None):
    """
    Downloads a backup from R2 and restores it to the playtest database.
    """
    config = get_config()
    try:
        validate_config(
            config,
            ["R2_ENDPOINT_URL", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET_NAME"],
        )
    except ValueError as exc:
        return False, str(exc)

    blocker = get_playtest_restore_blocker(config)
    if blocker:
        return False, blocker

    selected_filename = filename or get_latest_backup_name()
    if not selected_filename:
        return False, "No backups are available to restore."

    filepath = None

    try:
        filepath = download_backup_to_tempfile(selected_filename, config)
    except Exception as exc:
        return False, f"Download failed: {exc}"

    try:
        subprocess.run(
            [
                "psql",
                config["PLAYTEST_DATABASE_URL"],
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                "DROP SCHEMA public CASCADE; CREATE SCHEMA public;",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        subprocess.run(
            [
                "psql",
                config["PLAYTEST_DATABASE_URL"],
                "-v",
                "ON_ERROR_STOP=1",
                "-f",
                filepath,
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        return True, f"Successfully restored {selected_filename} to the playtest database."
    except subprocess.CalledProcessError as exc:
        return False, build_subprocess_error("Restore failed", exc)
    finally:
        if filepath and os.path.exists(filepath):
            os.remove(filepath)


def get_db_connection(database_url):
    return psycopg2.connect(database_url, connect_timeout=5)


def get_playtest_db_info():
    config = get_config()
    playtest_url = config.get("PLAYTEST_DATABASE_URL")

    info = {
        "configured": bool(playtest_url),
        "restore_enabled": restore_is_enabled(),
        "status": "Not configured",
        "table_count": 0,
        "latest_update": "No connection",
        "connection": {
            "host": "N/A",
            "port": "N/A",
            "user": "N/A",
            "password": "N/A",
            "database": "N/A",
        },
    }

    if not playtest_url:
        return info

    parsed = urlparse(playtest_url)
    info["connection"] = {
        "host": parsed.hostname or "N/A",
        "port": parsed.port or 5432,
        "user": parsed.username or "N/A",
        "password": mask_secret(parsed.password),
        "database": parsed.path.lstrip("/") or "N/A",
    }

    try:
        conn = get_db_connection(playtest_url)
        cur = conn.cursor()
        cur.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'"
        )
        info["table_count"] = cur.fetchone()[0]
        info["status"] = "Healthy"
        info["latest_update"] = "Loaded" if info["table_count"] > 0 else "Empty schema"
        cur.close()
        conn.close()
    except Exception as exc:
        info["status"] = f"Error: {exc}"
        info["latest_update"] = "Unavailable"

    return info

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
    filepath = os.path.join(tempfile.gettempdir(), filename)
    
    print(f"Starting backup: {filename}")

    # 1. Dump Database
    try:
        validate_config(config, ["DATABASE_URL"])
        subprocess.run(
            ["pg_dump", config["DATABASE_URL"], "-f", filepath],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as e:
        return False, build_subprocess_error("Dump failed", e)
    except ValueError as e:
        return False, str(e)

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
    finally:
        if os.path.exists(filepath):
            os.remove(filepath)

import os
import subprocess
import datetime
import boto3
import psycopg2
from botocore.exceptions import NoCredentialsError

def get_config():
    return {
        "DATABASE_URL": os.getenv("DATABASE_URL"),
        "TEST_DATABASE_URL": os.getenv("TEST_DATABASE_URL"),
        "R2_ENDPOINT_URL": os.getenv("R2_ENDPOINT_URL"),
        "R2_ACCESS_KEY_ID": os.getenv("R2_ACCESS_KEY_ID"),
        "R2_SECRET_ACCESS_KEY": os.getenv("R2_SECRET_ACCESS_KEY"),
        "R2_BUCKET_NAME": os.getenv("R2_BUCKET_NAME"),
    }

def list_backups():
    """Lists available backups in the R2 bucket."""
    config = get_config()
    validate_config(config, ["R2_ENDPOINT_URL", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET_NAME"])
    
    s3 = boto3.client(
        's3',
        endpoint_url=config["R2_ENDPOINT_URL"],
        aws_access_key_id=config["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=config["R2_SECRET_ACCESS_KEY"]
    )
    
    response = s3.list_objects_v2(Bucket=config["R2_BUCKET_NAME"])
    if 'Contents' not in response:
        return []
    
    # Sort by last modified descending
    backups = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)
    return [{
        "filename": b['Key'],
        "size": b['Size'],
        "last_modified": b['LastModified'].strftime("%Y-%m-%d %H:%M:%S")
    } for b in backups]

def perform_restore(filename):
    """
    Downloads a backup from R2 and restores it to the TEST_DATABASE_URL.
    """
    config = get_config()
    validate_config(config, ["TEST_DATABASE_URL", "R2_ENDPOINT_URL", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET_NAME"])
    
    if config["TEST_DATABASE_URL"] == config["DATABASE_URL"]:
        return False, "Safety Error: TEST_DATABASE_URL is the same as production DATABASE_URL!"

    filepath = f"/tmp/{filename}"
    
    # 1. Download from R2
    try:
        s3 = boto3.client(
            's3',
            endpoint_url=config["R2_ENDPOINT_URL"],
            aws_access_key_id=config["R2_ACCESS_KEY_ID"],
            aws_secret_access_key=config["R2_SECRET_ACCESS_KEY"]
        )
        s3.download_file(config["R2_BUCKET_NAME"], filename, filepath)
    except Exception as e:
        return False, f"Download failed: {str(e)}"

    # 2. Reset and Restore
    try:
        # Reset: Drop and recreate public schema
        reset_cmd = f"psql '{config['TEST_DATABASE_URL']}' -c 'DROP SCHEMA public CASCADE; CREATE SCHEMA public;'"
        subprocess.run(reset_cmd, shell=True, check=True, capture_output=True)
        
        # Restore
        restore_cmd = f"psql '{config['TEST_DATABASE_URL']}' -f {filepath}"
        subprocess.run(restore_cmd, shell=True, check=True, capture_output=True)
        
        os.remove(filepath)
        return True, f"Successfully restored {filename} to Test DB"
    except subprocess.CalledProcessError as e:
        if os.path.exists(filepath): os.remove(filepath)
        return False, f"Restore failed: {e.stderr.decode()}"
    except Exception as e:
        if os.path.exists(filepath): os.remove(filepath)
        return False, f"Unexpected error: {str(e)}"


def validate_config(config, keys):
    missing = [k for k in keys if not config.get(k)]
    if missing:
        raise ValueError(f"Missing environment variables: {', '.join(missing)}")

def get_db_connection(test_db=False):
    config = get_config()
    url = config["TEST_DATABASE_URL"] if test_db else config["DATABASE_URL"]
    validate_config(config, ["TEST_DATABASE_URL" if test_db else "DATABASE_URL"])
    return psycopg2.connect(url)

def get_test_db_info():
    """Fetches status, table count, and latest record info from Test DB."""
    config = get_config()
    if not config.get("TEST_DATABASE_URL"):
        return None
    
    info = {
        "status": "Healthy",
        "table_count": 0,
        "latest_update": "N/A",
        "connection": {}
    }
    
    try:
        # Parse connection info
        from urllib.parse import urlparse
        result = urlparse(config["TEST_DATABASE_URL"])
        info["connection"] = {
            "host": result.hostname,
            "port": result.port,
            "user": result.username,
            "password": result.password,
            "database": result.path.lstrip('/')
        }

        conn = get_db_connection(test_db=True)
        cur = conn.cursor()
        
        # Get table count
        cur.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'")
        info["table_count"] = cur.fetchone()[0]
        
        # Try to find the latest update across common timestamp columns if tables exist
        if info["table_count"] > 0:
            cur.execute("""
                SELECT MAX(last_modified) FROM (
                    SELECT last_modified FROM (
                        SELECT table_name FROM information_schema.columns 
                        WHERE column_name = 'updated_at' OR column_name = 'created_at'
                    ) t, LATERAL (SELECT MAX(updated_at) as last_modified FROM public.table_name)
                ) subquery
            """)
            # Note: The above dynamic query is complex, simpler approach for now:
            # Just check if there's any data in a likely table or just use current timestamp of restore
            info["latest_update"] = "Connected"
            
        cur.close()
        conn.close()
    except Exception as e:
        info["status"] = f"Error: {str(e)}"
        
    return info


def init_db():
    """Creates the backup log table if it doesn't exist."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS _admin_backup_logs (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status VARCHAR(50),
                filename VARCHAR(255),
                size_bytes BIGINT,
                message TEXT
            );
        """)
        conn.commit()
        cur.close()
        conn.close()
        print("Initialized backup log table.")
    except Exception as e:
        print(f"Error initializing DB: {e}")

def log_backup(status, filename, size, message):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO _admin_backup_logs (status, filename, size_bytes, message)
            VALUES (%s, %s, %s, %s)
        """, (status, filename, size, message))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Failed to log to DB: {e}")

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
        process = subprocess.run(command, shell=True, check=True, capture_output=True)
    except subprocess.CalledProcessError as e:
        err_msg = f"Dump failed: {e.stderr.decode()}"
        log_backup("FAILED", filename, 0, err_msg)
        return False, err_msg

    # 2. Upload to R2
    try:
        validate_config(config, ["R2_ENDPOINT_URL", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET_NAME"])
        
        s3 = boto3.client(
            's3',
            endpoint_url=config["R2_ENDPOINT_URL"],
            aws_access_key_id=config["R2_ACCESS_KEY_ID"],
            aws_secret_access_key=config["R2_SECRET_ACCESS_KEY"]
        )
        
        with open(filepath, "rb") as f:
            s3.upload_fileobj(f, config["R2_BUCKET_NAME"], filename)

        
        file_size = os.path.getsize(filepath)
        log_backup("SUCCESS", filename, file_size, "Backup uploaded successfully")
        
        # Cleanup
        os.remove(filepath)
        return True, f"Backup successful ({round(file_size/(1024*1024), 2)} MB)"
        
    except Exception as e:
        err_msg = f"Upload failed: {str(e)}"
        log_backup("FAILED", filename, 0, err_msg)
        return False, err_msg

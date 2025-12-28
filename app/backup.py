import os
import subprocess
import datetime
import boto3
import psycopg2
from botocore.exceptions import NoCredentialsError

def get_config():
    return {
        "DATABASE_URL": os.getenv("DATABASE_URL"),
        "R2_ENDPOINT": os.getenv("R2_ENDPOINT_URL"),
        "R2_ACCESS_KEY": os.getenv("R2_ACCESS_KEY_ID"),
        "R2_SECRET_KEY": os.getenv("R2_SECRET_ACCESS_KEY"),
        "R2_BUCKET": os.getenv("R2_BUCKET_NAME"),
    }

def get_db_connection():
    config = get_config()
    return psycopg2.connect(config["DATABASE_URL"])

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
        s3 = boto3.client(
            's3',
            endpoint_url=config["R2_ENDPOINT"],
            aws_access_key_id=config["R2_ACCESS_KEY"],
            aws_secret_access_key=config["R2_SECRET_KEY"]
        )
        
        with open(filepath, "rb") as f:
            s3.upload_fileobj(f, config["R2_BUCKET"], filename)
        
        file_size = os.path.getsize(filepath)
        log_backup("SUCCESS", filename, file_size, "Backup uploaded successfully")
        
        # Cleanup
        os.remove(filepath)
        return True, f"Backup successful ({round(file_size/(1024*1024), 2)} MB)"
        
    except Exception as e:
        err_msg = f"Upload failed: {str(e)}"
        log_backup("FAILED", filename, 0, err_msg)
        return False, err_msg

from fastapi import FastAPI, Request, BackgroundTasks, Depends, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from apscheduler.schedulers.background import BackgroundScheduler
import os
import datetime
import json
from .backup import (
    perform_backup,
    init_db,
    get_db_connection,
    list_backups,
    perform_restore,
    get_test_db_info,
    delete_old_backups,
    RETENTION_DAYS,
)

app = FastAPI(title="Sentinel Backup Service")

# --- Scheduler Setup ---
scheduler = BackgroundScheduler()

def scheduled_job():
    print("Running scheduled backup...")
    perform_backup()

@app.on_event("startup")
def startup_event():
    try:
        init_db()
    except Exception as exc:
        print(f"Startup warning: init_db failed: {exc}")

    try:
        hour = int(os.getenv("BACKUP_CRON_HOUR", 3))
        minute = int(os.getenv("BACKUP_CRON_MINUTE", 0))
    except ValueError as exc:
        print(f"Startup warning: invalid backup schedule config: {exc}")
        hour = 3
        minute = 0

    try:
        if not scheduler.running:
            scheduler.add_job(
                scheduled_job,
                'cron',
                hour=hour,
                minute=minute,
                id="daily_backup",
                replace_existing=True
            )
            scheduler.start()
        print(f"Scheduler ready. Backup set for {hour:02d}:{minute:02d} daily.")
    except Exception as exc:
        print(f"Startup warning: scheduler failed to start: {exc}")

# --- UI Setup ---
templates = Jinja2Templates(directory="app/templates")

# --- Routes ---

@app.get("/schema", response_class=HTMLResponse)
async def schema_view(request: Request):
    try:
        with open("schema_metadata.json", "r") as f:
            schema_data = json.load(f)
    except FileNotFoundError:
        schema_data = {}
        
    return templates.TemplateResponse("schema.html", {"request": request, "schema": schema_data})

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    formatted_logs = []
    dashboard_errors = []

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        # Fetch last 50 logs
        cur.execute("SELECT id, timestamp, status, filename, size_bytes, message FROM _admin_backup_logs ORDER BY timestamp DESC LIMIT 50")
        logs = cur.fetchall()
        cur.close()
        conn.close()

        for log in logs:
            # Simple size formatter
            size_mb = round(log[4] / (1024 * 1024), 2) if log[4] else 0
            formatted_logs.append({
                "timestamp": log[1].strftime("%Y-%m-%d %H:%M:%S"),
                "status": log[2],
                "filename": log[3],
                "size": f"{size_mb} MB",
                "message": log[5]
            })
    except Exception as exc:
        dashboard_errors.append(f"Recent activity unavailable: {exc}")

    # Fetch available backups for restoration
    try:
        available_backups = list_backups()
    except Exception as exc:
        available_backups = []
        dashboard_errors.append(f"Backup list unavailable: {exc}")

    try:
        test_db_info = get_test_db_info()
    except Exception as exc:
        test_db_info = None
        dashboard_errors.append(f"Test DB status unavailable: {exc}")

    context = {
        "request": request,
        "logs": formatted_logs,
        "backups": available_backups,
        "test_db_info": test_db_info,
        "retention_days": RETENTION_DAYS,
        "dashboard_errors": dashboard_errors
    }

    try:
        return templates.TemplateResponse("index.html", context)
    except Exception as exc:
        error_items = "".join(
            f"<li>{error}</li>" for error in (dashboard_errors or [f"Dashboard render failed: {exc}"])
        )
        return HTMLResponse(
            f"""
            <html>
                <head><title>Sentinel Backup</title></head>
                <body style="font-family: Arial, sans-serif; padding: 24px;">
                    <h1>Sentinel Backup</h1>
                    <p>The dashboard could not render fully, but the service is running.</p>
                    <p><a href="/health">Health Check</a></p>
                    <h2>Warnings</h2>
                    <ul>{error_items}</ul>
                </body>
            </html>
            """,
            status_code=200
        )

@app.post("/restore/{filename}")
async def restore_to_test(filename: str, background_tasks: BackgroundTasks):
    # For restoration, we might want to track it in logs too, but for now just run it
    # We can do it synchronously if it's not too large, or background it.
    # Since it's for "Testing DB", background is safer to avoid UI timeout.
    background_tasks.add_task(perform_restore, filename)
    return {"message": f"Restoration of {filename} to Test DB started in background"}

@app.post("/trigger-backup")
async def trigger_backup(background_tasks: BackgroundTasks):
    background_tasks.add_task(perform_backup)
    return {"message": "Backup triggered in background"}

@app.post("/cleanup-old-backups")
async def cleanup_old_backups():
    try:
        result = delete_old_backups()
        return {
            "message": (
                f"Deleted {result['deleted_count']} backup(s) older than "
                f"{result['retention_days']} days."
            ),
            "deleted_count": result["deleted_count"],
            "deleted_files": result["deleted_files"],
            "retention_days": result["retention_days"]
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Cleanup failed: {exc}")

@app.get("/health")
def health_check():
    return {"status": "ok"}

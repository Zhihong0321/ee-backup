from fastapi import FastAPI, Request, BackgroundTasks, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from apscheduler.schedulers.background import BackgroundScheduler
import datetime
import json
import os
from .backup import (
    perform_backup,
    list_backups,
    perform_restore,
    get_latest_backup_name,
    get_playtest_db_info,
    get_playtest_restore_blocker,
    delete_old_backups,
    RETENTION_DAYS,
)

app = FastAPI(title="Sentinel Backup Service")

# --- Scheduler Setup ---
scheduler = BackgroundScheduler()


def utc_timestamp():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def default_playtest_restore_job():
    return {
        "status": "idle",
        "status_class": "status-idle",
        "message": "No playtest restore has been started yet.",
        "filename": None,
        "requested_at": None,
        "finished_at": None,
    }


def set_playtest_restore_job(**updates):
    current = default_playtest_restore_job()
    current.update(getattr(app.state, "playtest_restore_job", {}))
    current.update(updates)
    app.state.playtest_restore_job = current
    return current


def queue_playtest_restore(filename):
    set_playtest_restore_job(
        status="running",
        status_class="status-running",
        message=f"Restoring {filename} into the playtest database.",
        filename=filename,
        requested_at=utc_timestamp(),
        finished_at=None,
    )

    try:
        success, message = perform_restore(filename)
    except Exception as exc:
        success = False
        message = f"Unexpected restore failure: {exc}"

    set_playtest_restore_job(
        status="success" if success else "failed",
        status_class="status-success" if success else "status-failed",
        message=message,
        filename=filename,
        finished_at=utc_timestamp(),
    )


def ensure_playtest_restore_ready():
    blocker = get_playtest_restore_blocker()
    if blocker:
        raise HTTPException(status_code=400, detail=blocker)

def scheduled_job():
    print("Running scheduled backup...")
    perform_backup()

@app.on_event("startup")
def startup_event():
    app.state.playtest_restore_job = default_playtest_restore_job()

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

    return templates.TemplateResponse(
        request=request,
        name="schema.html",
        context={"request": request, "schema": schema_data}
    )

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    dashboard_errors = []
    available_backups = []
    playtest_db_info = get_playtest_db_info()
    restore_blocker = get_playtest_restore_blocker()
    restore_enabled = restore_blocker is None
    latest_backup_filename = None
    latest_restore_job = getattr(app.state, "playtest_restore_job", default_playtest_restore_job())

    try:
        raw_backups = list_backups()
        available_backups = [{
            "filename": backup["filename"],
            "size_mb": round(backup["size"] / (1024 * 1024), 2),
            "last_modified": backup["last_modified"]
        } for backup in raw_backups]
        if available_backups:
            latest_backup_filename = available_backups[0]["filename"]
    except Exception as exc:
        available_backups = []
        dashboard_errors.append(f"Backup list unavailable: {exc}")

    status_text = str(playtest_db_info.get("status", "Unknown"))
    if not playtest_db_info.get("configured"):
        status_class = "status-idle"
    elif status_text == "Healthy":
        status_class = "status-success"
    else:
        status_class = "status-failed"

    connection = playtest_db_info.get("connection") or {}
    normalized_playtest_db_info = {
        "configured": playtest_db_info.get("configured", False),
        "restore_enabled": playtest_db_info.get("restore_enabled", False),
        "status": status_text,
        "status_class": status_class,
        "table_count": playtest_db_info.get("table_count", 0),
        "latest_update": playtest_db_info.get("latest_update", "N/A"),
        "connection_host": connection.get("host", "N/A"),
        "connection_port": connection.get("port", "N/A"),
        "connection_user": connection.get("user", "N/A"),
        "connection_password": connection.get("password", "N/A"),
        "connection_database": connection.get("database", "N/A"),
    }

    context = {
        "request": request,
        "backups": available_backups,
        "latest_backup_filename": latest_backup_filename,
        "playtest_db_info": normalized_playtest_db_info,
        "playtest_restore_job": latest_restore_job,
        "restore_enabled": restore_enabled,
        "restore_blocker": restore_blocker,
        "retention_days": RETENTION_DAYS,
        "dashboard_errors": dashboard_errors,
    }

    try:
        return templates.TemplateResponse(
            request=request,
            name="index.html",
            context=context
        )
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

@app.post("/trigger-backup")
async def trigger_backup(background_tasks: BackgroundTasks):
    background_tasks.add_task(perform_backup)
    return {"message": "Backup triggered in background"}

@app.post("/restore-playtest/latest")
async def restore_latest_playtest(background_tasks: BackgroundTasks):
    ensure_playtest_restore_ready()

    try:
        filename = get_latest_backup_name()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Could not load latest backup: {exc}")

    if not filename:
        raise HTTPException(status_code=400, detail="No backups are available to restore.")

    background_tasks.add_task(queue_playtest_restore, filename)
    return {"message": f"Playtest restore of latest backup ({filename}) started in background."}

@app.post("/restore-playtest/{filename}")
async def restore_selected_playtest(filename: str, background_tasks: BackgroundTasks):
    ensure_playtest_restore_ready()
    background_tasks.add_task(queue_playtest_restore, filename)
    return {"message": f"Playtest restore of {filename} started in background."}

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

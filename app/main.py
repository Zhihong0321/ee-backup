from fastapi import FastAPI, Request, BackgroundTasks, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from apscheduler.schedulers.background import BackgroundScheduler
import os
import json
from .backup import (
    perform_backup,
    list_backups,
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

    try:
        raw_backups = list_backups()
        available_backups = [{
            "filename": backup["filename"],
            "size_mb": round(backup["size"] / (1024 * 1024), 2),
            "last_modified": backup["last_modified"]
        } for backup in raw_backups]
    except Exception as exc:
        available_backups = []
        dashboard_errors.append(f"Backup list unavailable: {exc}")

    context = {
        "request": request,
        "backups": available_backups,
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

from fastapi import FastAPI, Request, BackgroundTasks, Depends, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from apscheduler.schedulers.background import BackgroundScheduler
import os
import datetime
from .backup import perform_backup, init_db, get_db_connection

app = FastAPI(title="Sentinel Backup Service")

# --- Scheduler Setup ---
scheduler = BackgroundScheduler()

def scheduled_job():
    print("Running scheduled backup...")
    perform_backup()

@app.on_event("startup")
def startup_event():
    init_db()
    # Configure Schedule from Env or Default to 3 AM
    hour = int(os.getenv("BACKUP_CRON_HOUR", 3))
    minute = int(os.getenv("BACKUP_CRON_MINUTE", 0))
    
    scheduler.add_job(scheduled_job, 'cron', hour=hour, minute=minute)
    scheduler.start()
    print(f"Scheduler started. Backup set for {hour:02d}:{minute:02d} daily.")

# --- UI Setup ---
templates = Jinja2Templates(directory="app/templates")

# --- Routes ---

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    conn = get_db_connection()
    cur = conn.cursor()
    # Fetch last 50 logs
    cur.execute("SELECT id, timestamp, status, filename, size_bytes, message FROM _admin_backup_logs ORDER BY timestamp DESC LIMIT 50")
    logs = cur.fetchall()
    cur.close()
    conn.close()
    
    formatted_logs = []
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

    return templates.TemplateResponse("index.html", {"request": request, "logs": formatted_logs})

@app.post("/trigger-backup")
async def trigger_backup(background_tasks: BackgroundTasks):
    background_tasks.add_task(perform_backup)
    return {"message": "Backup triggered in background"}

@app.get("/health")
def health_check():
    return {"status": "ok"}

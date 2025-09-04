from flask import Flask, request, jsonify, redirect
import requests
import sqlite3
from datetime import datetime
import os
import logging
from werkzeug.utils import secure_filename
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from googleapiclient.errors import HttpError
import io
import time

# ---------------- CONFIG ----------------
BASE_DOMAIN = os.getenv("BASE_DOMAIN", "https://omantracking2.com")
TOKEN = os.getenv("TOKEN")
DB_FILE = os.getenv("DB_FILE", "reports.db")
STORAGE_PATH = os.getenv("STORAGE_PATH", "/opt/render/reports")

# Google Drive Configuration
GDRIVE_CREDENTIALS = os.getenv("GDRIVE_CREDENTIALS")
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID")

os.makedirs(STORAGE_PATH, exist_ok=True)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
app = Flask(__name__)

# --- Google Drive ---
def get_gdrive_service():
    try:
        secret_path = "/etc/secrets/power-bi-x-gpsgate-b793752d1634.json"
        with open(secret_path, "r") as f:
            creds_dict = json.load(f)
        creds = service_account.Credentials.from_service_account_info(
            creds_dict, scopes=['https://www.googleapis.com/auth/drive.file']
        )
        return build('drive', 'v3', credentials=creds)
    except Exception:
        logger.exception("Google Drive auth failed")
        return None

def save_to_gdrive(file_url, file_name, token_override=None):
    try:
        headers = {"Authorization": token_override or TOKEN}
        response = requests.get(file_url, headers=headers, timeout=30, stream=True)
        response.raise_for_status()
        file_content = io.BytesIO(response.content)
        service = get_gdrive_service()
        if not service:
            return None
        metadata = {'name': file_name, 'mimeType': 'text/csv'}
        if GDRIVE_FOLDER_ID:
            metadata['parents'] = [GDRIVE_FOLDER_ID]
        media = MediaIoBaseUpload(file_content, mimetype='text/csv', resumable=True)
        file = service.files().create(
            body=metadata,
            media_body=media,
            fields='id, webViewLink, webContentLink',
            supportsAllDrives=True
        ).execute()
        return file.get('webContentLink')
    except Exception:
        logger.exception("Error saving to Google Drive")
        return None

# --- Database ---
def init_db():
    """Initialize DB with safer schema and migrate old tables if needed."""
    schema = """
    CREATE TABLE IF NOT EXISTS downloaded_reports (
        id INTEGER PRIMARY KEY AUTOINCREMENT
        -- we'll add other columns in migration
    );
    """
    uniques = """
    CREATE UNIQUE INDEX IF NOT EXISTS ux_reports_unique
      ON downloaded_reports(application_id, report_id, api_render_id, file_name);
    """

    try:
        with sqlite3.connect(DB_FILE) as conn:
            cur = conn.cursor()
            # Step 1: ensure table exists
            cur.executescript(schema)

            # Step 2: migrate/add missing columns
            cur.execute("PRAGMA table_info(downloaded_reports)")
            cols = [c[1] for c in cur.fetchall()]
            
            # Add missing columns
            for col, col_type in [
                ("application_id", "TEXT"),
                ("report_id", "TEXT"),
                ("request_render_id", "TEXT"),
                ("api_render_id", "TEXT"),
                ("file_name", "TEXT"),
                ("file_path", "TEXT"),
                ("downloaded_at", "DATETIME DEFAULT CURRENT_TIMESTAMP")
            ]:
                if col not in cols:
                    cur.execute(f"ALTER TABLE downloaded_reports ADD COLUMN {col} {col_type}")

            # Step 3: create unique index safely
            cur.execute(uniques)

            conn.commit()
        cleanup_invalid_records()
        logger.info("Database initialization/migration completed successfully")
    except sqlite3.Error as e:
        logger.exception(f"SQLite error during init_db: {e}")
    except Exception:
        logger.exception("Unexpected error during init_db")


def cleanup_invalid_records():
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM downloaded_reports WHERE file_name IS NULL OR file_path IS NULL")
            cur.execute("""
                DELETE FROM downloaded_reports
                WHERE id NOT IN (
                    SELECT MAX(id) FROM downloaded_reports
                    GROUP BY application_id, report_id, api_render_id, file_name
                )
            """)
            conn.commit()
        logger.info("DB cleanup completed")
    except Exception:
        logger.exception("DB cleanup error")

def already_downloaded(application_id, report_id, request_render_id=None, api_render_id=None):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cur = conn.cursor()
            if api_render_id:
                cur.execute("""
                    SELECT file_name, file_path FROM downloaded_reports
                    WHERE application_id=? AND report_id=? AND api_render_id=?
                    ORDER BY downloaded_at DESC LIMIT 1
                """, (application_id, report_id, str(api_render_id)))
            else:
                cur.execute("""
                    SELECT file_name, file_path FROM downloaded_reports
                    WHERE application_id=? AND report_id=? AND request_render_id=?
                    ORDER BY downloaded_at DESC LIMIT 1
                """, (application_id, report_id, str(request_render_id)))
            row = cur.fetchone()
            return {"file_name": row[0], "file_path": row[1]} if row else None
    except Exception:
        logger.exception("Error checking already_downloaded")
        return None

def save_to_db(application_id, report_id, request_render_id, api_render_id, file_name, file_path):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO downloaded_reports
                  (application_id, report_id, request_render_id, api_render_id, file_name, file_path)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(application_id, report_id, api_render_id, file_name)
                DO UPDATE SET file_path=excluded.file_path, downloaded_at=CURRENT_TIMESTAMP
            """, (application_id, report_id, request_render_id, api_render_id, file_name, file_path))
            conn.commit()
    except Exception:
        logger.exception("Error saving to DB")

# --- File storage ---
def save_file_locally(file_url, file_name, token_override=None):
    gdrive_link = save_to_gdrive(file_url, file_name, token_override)
    if gdrive_link:
        return gdrive_link
    # Fallback to local storage
    try:
        local_path = os.path.join(STORAGE_PATH, file_name)
        headers = {"Authorization": token_override or TOKEN}
        r = requests.get(file_url, headers=headers, timeout=30, stream=True)
        r.raise_for_status()
        with open(local_path, "wb") as f:
            f.write(r.content)
        return local_path
    except Exception:
        logger.exception("Failed to save locally")
        return None

# --- Routes ---
@app.route("/get_report", methods=["GET"])
def get_report():
    init_db()
    application_id = request.args.get("application_id")
    report_id = request.args.get("report_id")
    request_render_id = request.args.get("render_id")

    if not application_id or not report_id or not request_render_id:
        return jsonify({"error": "application_id, report_id, and render_id required"}), 400

    token_to_use = request.headers.get("Authorization") or TOKEN
    if not token_to_use:
        return jsonify({"error": "Missing Authorization token"}), 401

    cached = already_downloaded(application_id, report_id, request_render_id=request_render_id)
    if cached:
        return jsonify({
            "message": "Report already processed",
            "application_id": application_id,
            "report_id": report_id,
            "render_id": request_render_id,
            "download_url": f"{request.host_url}download_file/{secure_filename(cached['file_name'])}",
            "file_name": cached["file_name"]
        })

    url = f"{BASE_DOMAIN}/comGpsGate/api.v.1/applications/{application_id}/reports/{report_id}/renderings/{request_render_id}"
    headers = {"Authorization": token_to_use, "Accept": "application/json"}
    logger.info(f"Calling GPSGate API: {url}")

    try:
        for attempt in range(3):  # retry loop
            response = requests.get(url, headers=headers, timeout=30)
            if response.status_code == 404:
                time.sleep(3)
                continue
            if response.status_code != 200:
                return jsonify({"error": f"Error fetching render {response.status_code}", "details": response.text}), response.status_code
            data = response.json()
            api_render_id = data.get("id")
            output_file = data.get("outputFile")
            is_ready = data.get("isReady")
            if not api_render_id or not output_file:
                return jsonify({"error": "No report file info found"}), 404
            break
        else:
            return jsonify({"error": "Render not found after retries"}), 404

        cached = already_downloaded(application_id, report_id, api_render_id=str(api_render_id))
        if cached:
            return jsonify({
                "message": "Report already processed",
                "application_id": application_id,
                "report_id": report_id,
                "render_id": api_render_id,
                "download_url": f"{request.host_url}download_file/{secure_filename(cached['file_name'])}",
                "file_name": cached["file_name"]
            })

        if is_ready:
            file_url = f"{BASE_DOMAIN}{output_file}"
            file_name = secure_filename(f"{application_id}-{report_id}-{api_render_id}-{datetime.now():%Y%m%d_%H%M%S}.csv")
            file_path = save_file_locally(file_url, file_name, token_to_use)
            if not file_path:
                return jsonify({"error": "Failed to save file"}), 500
            save_to_db(application_id, report_id, request_render_id, str(api_render_id), file_name, file_path)
            return jsonify({
                "application_id": application_id,
                "report_id": report_id,
                "render_id": api_render_id,
                "download_url": f"{request.host_url}download_file/{file_name}",
                "file_name": file_name,
                "message": "File saved successfully"
            })
        return jsonify({"message": "Report not ready yet", "status": "processing"})
    except Exception:
        logger.exception("Unexpected error in get_report")
        return jsonify({"error": "Internal server error"}), 500

@app.route("/download_file/<filename>", methods=["GET"])
def download_file(filename):
    try:
        filename = secure_filename(filename)
        with sqlite3.connect(DB_FILE) as conn:
            cur = conn.cursor()
            cur.execute("SELECT file_path FROM downloaded_reports WHERE file_name=?", (filename,))
            row = cur.fetchone()
            if row:
                return redirect(row[0])
        return jsonify({"error": "File not found"}), 404
    except Exception:
        logger.exception("Download error")
        return jsonify({"error": "Download failed"}), 500

@app.route("/list_files", methods=["GET"])
def list_files():
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cur = conn.cursor()
            cur.execute("SELECT file_name FROM downloaded_reports ORDER BY downloaded_at DESC")
            files = [r[0] for r in cur.fetchall()]
        return jsonify({"files": files, "count": len(files)})
    except Exception:
        logger.exception("List files error")
        return jsonify({"error": "Failed to list files"}), 500

@app.route("/health", methods=["GET"])
def health_check():
    gdrive_ok = get_gdrive_service() is not None
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "db_file_exists": os.path.exists(DB_FILE),
        "gdrive_configured": bool(GDRIVE_CREDENTIALS),
        "gdrive_folder_set": bool(GDRIVE_FOLDER_ID),
        "gdrive_connected": gdrive_ok
    })

@app.route("/admin/cleanup", methods=["POST"])
def admin_cleanup():
    try:
        cleanup_invalid_records()
        return jsonify({"message": "Database cleanup completed"})
    except Exception:
        logger.exception("Cleanup error")
        return jsonify({"error": "Cleanup failed"}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
if __name__ == "__main__":
    init_db()  # <-- ensure DB is created before the server starts
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))

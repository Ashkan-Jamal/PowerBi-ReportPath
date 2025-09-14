from flask import Flask, request, jsonify, redirect, send_file
from flask_cors import CORS
import requests
import sqlite3
from datetime import datetime
import os
import logging
from werkzeug.utils import secure_filename
import shutil
import json
import io
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

# ---------------- CONFIG ----------------
BASE_DOMAIN = os.getenv("BASE_DOMAIN", "https://omantracking2.com")
TOKEN = os.getenv("TOKEN")  # GPSGate API token, e.g., v2:...
DB_FILE = os.getenv("DB_FILE", "reports.db")
STORAGE_PATH = os.getenv("STORAGE_PATH", "/opt/render/reports")

GDRIVE_CREDENTIALS = os.getenv("GDRIVE_CREDENTIALS")
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID")

os.makedirs(STORAGE_PATH, exist_ok=True)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
app = Flask(__name__)
CORS(app)

# ---------------- GOOGLE DRIVE ----------------
def get_gdrive_service():
    if not GDRIVE_CREDENTIALS:
        return None
    try:
        creds_dict = json.loads(GDRIVE_CREDENTIALS)
        creds = service_account.Credentials.from_service_account_info(
            creds_dict, scopes=['https://www.googleapis.com/auth/drive.file']
        )
        return build('drive', 'v3', credentials=creds)
    except Exception:
        logger.exception("Failed to initialize Google Drive service")
        return None

def save_to_gdrive(file_url, file_name, token_override=None):
    try:
        token = token_override or TOKEN
        headers = {"Authorization": token}
        r = requests.get(file_url, headers=headers, stream=True, timeout=30)
        r.raise_for_status()
        file_content = io.BytesIO(r.content)
        service = get_gdrive_service()
        if not service:
            return None
        metadata = {"name": file_name, "mimeType": "text/csv"}
        if GDRIVE_FOLDER_ID:
            metadata["parents"] = [GDRIVE_FOLDER_ID]
        media = MediaIoBaseUpload(file_content, mimetype='text/csv', resumable=True)
        file = service.files().create(
            body=metadata,
            media_body=media,
            fields='id, webViewLink, webContentLink',
            supportsAllDrives=True
        ).execute()
        return file.get('webContentLink')
    except Exception:
        logger.exception("Failed to upload to Google Drive")
        return None

# ---------------- DATABASE ----------------
def init_db():
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS downloaded_reports (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    application_id TEXT,
                    report_id TEXT,
                    request_render_id TEXT,
                    api_render_id TEXT,
                    file_name TEXT,
                    file_path TEXT,
                    downloaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(application_id, report_id, api_render_id, file_name)
                )
            """)
            conn.commit()
        logger.info("Database initialized")
    except Exception:
        logger.exception("Failed to initialize database")

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
        logger.exception("DB check failed")
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
        logger.exception("Failed to save record in DB")

# ---------------- FILE STORAGE ----------------
def save_file_locally(file_url, file_name, token_override=None):
    # Try Google Drive first
    if GDRIVE_CREDENTIALS or GDRIVE_FOLDER_ID:
        gdrive_link = save_to_gdrive(file_url, file_name, token_override)
        if gdrive_link:
            return gdrive_link
    try:
        os.makedirs(STORAGE_PATH, exist_ok=True)
        safe_filename = secure_filename(file_name)
        local_path = os.path.join(STORAGE_PATH, safe_filename)
        if not os.path.abspath(local_path).startswith(os.path.abspath(STORAGE_PATH)):
            raise ValueError("Invalid file path")
        token = token_override or TOKEN
        headers = {"Authorization": token}
        r = requests.get(file_url, headers=headers, stream=True, timeout=30)
        r.raise_for_status()
        temp_path = local_path + ".tmp"
        with open(temp_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        shutil.move(temp_path, local_path)
        return local_path
    except Exception:
        logger.exception("Failed to save file locally")
        return None

# ---------------- ROUTES ----------------
@app.route("/get_report", methods=["GET"])
def get_report():
    init_db()
    application_id = request.args.get("application_id")
    report_id = request.args.get("report_id")
    request_render_id = request.args.get("render_id")
    if not all([application_id, report_id, request_render_id]):
        return jsonify({"error": "Missing parameters"}), 400

    token_to_use = request.headers.get("Authorization") or TOKEN
    if not token_to_use:
        return jsonify({"error": "Missing Authorization token"}), 401

    # DB first
    cached = already_downloaded(application_id, report_id, request_render_id=request_render_id)
    if cached:
        return jsonify({
            "message": "Report already cached",
            "application_id": application_id,
            "report_id": report_id,
            "render_id": request_render_id,
            "download_url": f"{request.host_url}download_file/{secure_filename(cached['file_name'])}",
            "file_name": cached["file_name"]
        })

    # GPSGate API
    url = f"{BASE_DOMAIN}/comGpsGate/api.v.1/applications/{application_id}/reports/{report_id}/renderings/{request_render_id}"
    headers = {"Authorization": token_to_use, "Accept": "application/json"}
    logger.info(f"Calling GPSGate API: {url}")

    try:
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code != 200:
            return jsonify({"error": f"API error {response.status_code}", "details": response.text}), response.status_code
        data = response.json()
        api_render_id = data.get("id")
        output_file = data.get("outputFile")
        is_ready = data.get("isReady")
        if not api_render_id or not output_file:
            return jsonify({"error": "No report file info"}), 404

        # Check DB by API render
        cached = already_downloaded(application_id, report_id, api_render_id=str(api_render_id))
        if cached:
            return jsonify({
                "message": "Report already cached",
                "application_id": application_id,
                "report_id": report_id,
                "render_id": api_render_id,
                "download_url": f"{request.host_url}download_file/{secure_filename(cached['file_name'])}",
                "file_name": cached["file_name"]
            })

        if is_ready:
            file_url = f"{BASE_DOMAIN}{output_file}"
            file_name = secure_filename(f"{application_id}-{report_id}-{api_render_id}.csv")
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
        return jsonify({"message": "Report not ready", "status": "processing"})

    except Exception:
        logger.exception("Unexpected error fetching report")
        return jsonify({"error": "Internal server error"}), 500

@app.route("/download_file/<filename>", methods=["GET"])
def download_file(filename):
    try:
        filename = secure_filename(filename)
        with sqlite3.connect(DB_FILE) as conn:
            cur = conn.cursor()
            cur.execute("SELECT file_path FROM downloaded_reports WHERE file_name=?", (filename,))
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "File not found"}), 404
            file_path = row[0]
            if file_path.startswith("http"):
                return redirect(file_path)
            if os.path.exists(file_path):
                return send_file(file_path, as_attachment=True)
            return jsonify({"error": "File missing on disk"}), 404
    except Exception:
        logger.exception("Download error")
        return jsonify({"error": "Download failed"}), 500

@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "db_exists": os.path.exists(DB_FILE),
        "storage_exists": os.path.exists(STORAGE_PATH)
    })

if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))

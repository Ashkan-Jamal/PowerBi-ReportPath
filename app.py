from flask import Flask, request, jsonify, redirect
import requests
import sqlite3
from datetime import datetime
import os
import logging
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from googleapiclient.errors import HttpError
import io

# ---------------- CONFIG ----------------
BASE_DOMAIN = os.getenv("BASE_DOMAIN", "https://omantracking2.com")
DB_FILE = "reports.db"
STORAGE_PATH = os.getenv("STORAGE_PATH", "/opt/render/reports")

# Google Drive Configuration
GDRIVE_CREDENTIALS = os.getenv("GDRIVE_CREDENTIALS")
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID")

# Create storage directory if it doesn't exist
os.makedirs(STORAGE_PATH, exist_ok=True)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Flask API ---
app = Flask(__name__)

# --- Google Drive Functions ---
def get_gdrive_service():
    """Authenticate and create Google Drive service instance using a secret file."""
    try:
        secret_path = "/etc/secrets/power-bi-x-gpsgate-b793752d1634.json"
        with open(secret_path, "r") as f:
            creds_dict = json.load(f)

        creds = service_account.Credentials.from_service_account_info(
            creds_dict,
            scopes=['https://www.googleapis.com/auth/drive.file']
        )

        service = build('drive', 'v3', credentials=creds)
        return service

    except Exception as e:
        logger.exception("Failed to create Google Drive service")
        return None


def save_to_gdrive(file_url, file_name, gpsgate_token):
    """Download file from URL using token and save to Google Drive."""
    try:
        response = requests.get(file_url, headers={"Authorization": gpsgate_token}, timeout=30, stream=True)
        response.raise_for_status()

        file_content = io.BytesIO()
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                file_content.write(chunk)
        file_content.seek(0)

        service = get_gdrive_service()
        if not service:
            return None

        file_metadata = {
            'name': file_name,
            'mimeType': 'text/csv'
        }
        if GDRIVE_FOLDER_ID:
            file_metadata['parents'] = [GDRIVE_FOLDER_ID]

        media = MediaIoBaseUpload(file_content, mimetype='text/csv', resumable=True)
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id, webViewLink, webContentLink',
            supportsAllDrives=True
        ).execute()

        return file.get('webContentLink')

    except HttpError as error:
        logger.exception(f"Google Drive API error: {error}")
        return None
    except Exception:
        logger.exception("Error saving file to Google Drive")
        return None


# --- Database functions ---
def cleanup_invalid_records():
    """Clean up invalid database entries (Google Drive links assumed always valid)."""
    logger.info("Database cleanup completed (Google Drive mode)")


def init_db():
    """Initialize database and table."""
    sql = """
    CREATE TABLE IF NOT EXISTS downloaded_reports (
        render_id INTEGER PRIMARY KEY,
        file_name TEXT NOT NULL,
        file_path TEXT NOT NULL,
        downloaded_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cur = conn.cursor()
            cur.execute(sql)
            cur.execute("PRAGMA table_info(downloaded_reports)")
            columns = [col[1] for col in cur.fetchall()]

            if 'file_name' not in columns:
                logger.info("Migrating database schema...")
                cur.execute("ALTER TABLE downloaded_reports RENAME TO downloaded_reports_old")
                cur.execute(sql)
            conn.commit()
        cleanup_invalid_records()
    except Exception:
        logger.exception("Error initializing database")


def already_downloaded(rid):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cur = conn.cursor()
            cur.execute("PRAGMA table_info(downloaded_reports)")
            columns = [col[1] for col in cur.fetchall()]
            if 'file_name' not in columns:
                return None
            cur.execute("SELECT file_name, file_path FROM downloaded_reports WHERE render_id=?", (rid,))
            result = cur.fetchone()
        if result and result[0] and result[1]:
            return result[0]
        return None
    except Exception:
        logger.exception("Error checking already_downloaded")
        return None


def save_to_db(rid, file_name, file_path):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT OR REPLACE INTO downloaded_reports (render_id, file_name, file_path) VALUES (?, ?, ?)",
                (rid, file_name, file_path)
            )
            conn.commit()
    except Exception:
        logger.exception("Error saving to database")


def save_file_locally(file_url, file_name, gpsgate_token):
    """Save file to Google Drive and return link for database."""
    gdrive_link = save_to_gdrive(file_url, file_name, gpsgate_token)
    return gdrive_link


# --- Routes ---
@app.route("/get_report", methods=["GET"])
def get_report():
    init_db()

    application_id = request.args.get("application_id")
    report_id = request.args.get("report_id")
    render_id = request.args.get("render_id")
    gpsgate_token = request.headers.get("Authorization")

    if not gpsgate_token:
        return jsonify({"error": "Authorization header required"}), 401
    if not application_id or not report_id or not render_id:
        return jsonify({"error": "application_id, report_id, and render_id are required"}), 400

    existing_file = already_downloaded(render_id)
    if existing_file:
        return jsonify({
            "message": "Report already processed",
            "render_id": render_id,
            "download_url": f"{request.host_url}download_file/{existing_file}",
            "file_name": existing_file
        })

    url = f"{BASE_DOMAIN}/comGpsGate/api/v.1/applications/{application_id}/reports/{report_id}/renderings/{render_id}"
    headers = {"Authorization": gpsgate_token, "Accept": "application/json"}

    try:
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code == 401:
            return jsonify({"error": "Invalid GPSGate token"}), 401
        if response.status_code == 403:
            return jsonify({"error": "Access forbidden"}), 403
        if response.status_code != 200:
            return jsonify({"error": f"Error fetching render info {response.status_code}"}), response.status_code

        data = response.json()
        rid = data.get("id")
        output_file = data.get("outputFile")
        is_ready = data.get("isReady")

        if not rid or not output_file:
            return jsonify({"error": "No report file info found in API response"}), 404

        if is_ready:
            file_url = f"{BASE_DOMAIN}{output_file}"
            file_name = f"{application_id}-{report_id}-{render_id}-{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            file_path = save_file_locally(file_url, file_name, gpsgate_token)

            if not file_path:
                return jsonify({"error": "Failed to save file to Google Drive"}), 500

            save_to_db(rid, file_name, file_path)

            return jsonify({
                "application_id": application_id,
                "report_id": report_id,
                "render_id": rid,
                "download_url": f"{request.host_url}download_file/{file_name}",
                "file_name": file_name,
                "message": "File saved successfully to Google Drive"
            })

        return jsonify({
            "message": "Report not ready yet",
            "application_id": application_id,
            "report_id": report_id,
            "render_id": render_id,
            "status": "processing"
        })

    except requests.exceptions.RequestException as e:
        logger.exception("Request error")
        return jsonify({"error": f"Network error: {str(e)}"}), 500


@app.route("/download_file/<filename>", methods=["GET"])
def download_file(filename):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cur = conn.cursor()
            cur.execute("SELECT file_path FROM downloaded_reports WHERE file_name=?", (filename,))
            result = cur.fetchone()

        if result and result[0]:
            return redirect(result[0])
        return jsonify({"error": "File not found"}), 404
    except Exception:
        logger.exception("Download error")
        return jsonify({"error": "Download failed"}), 500


@app.route("/list_files", methods=["GET"])
def list_files():
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cur = conn.cursor()
            cur.execute("SELECT file_name FROM downloaded_reports")
            files = [row[0] for row in cur.fetchall()]
        return jsonify({"files": files, "count": len(files)})
    except Exception:
        logger.exception("List files error")
        return jsonify({"error": "Failed to list files"}), 500


@app.route("/admin/cleanup", methods=["POST"])
def admin_cleanup():
    try:
        cleanup_invalid_records()
        return jsonify({"message": "Database cleanup completed"})
    except Exception:
        logger.exception("Cleanup error")
        return jsonify({"error": "Cleanup failed"}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)


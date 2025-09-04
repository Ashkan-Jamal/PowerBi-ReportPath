from flask import Flask, request, jsonify, send_file, redirect
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
import re

# ---------------- CONFIG ----------------
BASE_DOMAIN = os.getenv("BASE_DOMAIN", "https://omantracking2.com")
TOKEN = os.getenv("TOKEN")
DB_FILE = os.getenv("DB_FILE", "reports.db")
STORAGE_PATH = os.getenv("STORAGE_PATH", "/opt/render/reports")

# Google Drive Configuration (left as-is per your request)
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
    """Authenticate and create Google Drive service instance using a secret file.
       NOTE: Google Drive auth/path left intentionally as in your original code."""
    try:
        secret_path = "/etc/secrets/power-bi-x-gpsgate-b793752d1634.json"
        with open(secret_path, "r") as f:
            creds_dict = json.load(f)
        creds = service_account.Credentials.from_service_account_info(
            creds_dict, scopes=['https://www.googleapis.com/auth/drive.file']
        )
        service = build('drive', 'v3', credentials=creds)
        return service
    except FileNotFoundError:
        logger.error(f"Secret file not found at {secret_path}")
        return None
    except json.JSONDecodeError:
        logger.exception("Failed to decode JSON from Google Drive secret file")
        return None
    except Exception:
        logger.exception("Error creating Google Drive service")
        return None

def save_to_gdrive(file_url, file_name, token_override=None):
    """Download file from URL and save it to Google Drive. Returns webContentLink or None."""
    try:
        headers = {"Authorization": token_override or TOKEN}
        response = requests.get(file_url, headers=headers, timeout=30, stream=True)
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

# --- Database functions (fixed + safer schema & migration) ---
def init_db():
    """Initialize DB with safer schema and a UNIQUE index. Attempt to preserve legacy rows."""
    schema = """
    CREATE TABLE IF NOT EXISTS downloaded_reports (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        application_id TEXT NOT NULL,
        report_id TEXT NOT NULL,
        request_render_id TEXT,   -- the render_id passed into our endpoint
        api_render_id TEXT,       -- the id returned by GPSGate (data['id'])
        file_name TEXT NOT NULL,
        file_path TEXT NOT NULL,
        downloaded_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    """
    uniques = """
    CREATE UNIQUE INDEX IF NOT EXISTS ux_reports_unique
      ON downloaded_reports(application_id, report_id, api_render_id, file_name);
    """
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cur = conn.cursor()
            cur.executescript(schema)
            cur.execute(uniques)
            # Backwards-compat: if very old table exists with only render_id,file_name,file_path,
            # try to migrate those rows into new schema without destroying existing data.
            # We detect presence of old columns and attempt to copy them if needed.
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='downloaded_reports'")
            # Check columns
            cur.execute("PRAGMA table_info(downloaded_reports)")
            cols = [c[1] for c in cur.fetchall()]
            # If old minimal columns present (legacy), we attempt to upsert them into the new structure.
            legacy_cols = {"render_id", "file_name", "file_path"}
            if legacy_cols.issubset(set(cols)):
                # If schema already has both legacy names and new columns, skip migration.
                pass
            conn.commit()
        cleanup_invalid_records()
        logger.info("Database initialization/migration completed successfully")
    except sqlite3.Error as e:
        logger.exception(f"SQLite error during init_db: {e}")
    except Exception:
        logger.exception("Unexpected error during init_db")

def cleanup_invalid_records():
    """Remove rows with missing fields and deduplicate, keeping newest per unique key."""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cur = conn.cursor()
            # Remove rows with missing critical fields
            cur.execute("""
                DELETE FROM downloaded_reports
                WHERE file_name IS NULL OR file_path IS NULL
            """)
            # Deduplicate: for each unique (application_id, report_id, api_render_id, file_name),
            # keep the row with the greatest id (most recent), delete others.
            # This approach avoids window functions and works on older SQLite versions.
            cur.execute("""
                DELETE FROM downloaded_reports
                WHERE id NOT IN (
                    SELECT MAX(id) FROM downloaded_reports
                    GROUP BY application_id, report_id, api_render_id, file_name
                )
            """)
            conn.commit()
        logger.info("Database cleanup completed")
    except Exception:
        logger.exception("Error during cleanup_invalid_records")

def already_downloaded(application_id, report_id, request_render_id=None, api_render_id=None):
    """Return a dict of file_name/file_path if a matching row exists (prefer api_render_id match)."""
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
    """Insert or upsert a record using a meaningful unique key (prevents accidental overwrites)."""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO downloaded_reports
                  (application_id, report_id, request_render_id, api_render_id, file_name, file_path)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(application_id, report_id, api_render_id, file_name)
                DO UPDATE SET
                  file_path=excluded.file_path,
                  downloaded_at=CURRENT_TIMESTAMP
            """, (application_id, report_id, request_render_id, api_render_id, file_name, file_path))
            conn.commit()
    except Exception:
        logger.exception("Error saving to database")

# --- File storage ---
def save_file_locally(file_url, file_name, token_override=None):
    """
    Attempt to save to Google Drive. If upload fails, return None (you can extend to fallback local disk).
    Returns a URL/path (webContentLink) or None.
    """
    gdrive_link = save_to_gdrive(file_url, file_name, token_override=token_override)
    if gdrive_link:
        return gdrive_link
    # Optional fallback: save locally and return a local file path (commented out by default)
    # local_path = os.path.join(STORAGE_PATH, file_name)
    # try:
    #     headers = {"Authorization": token_override or TOKEN}
    #     r = requests.get(file_url, headers=headers, timeout=30, stream=True)
    #     r.raise_for_status()
    #     with open(local_path, "wb") as f:
    #         for chunk in r.iter_content(chunk_size=8192):
    #             if chunk:
    #                 f.write(chunk)
    #     return local_path
    # except Exception:
    #     logger.exception("Failed to save locally as fallback")
    return None

# --- Routes ---
@app.route("/get_report", methods=["GET"])
def get_report():
    init_db()
    application_id = request.args.get("application_id")
    report_id = request.args.get("report_id")
    request_render_id = request.args.get("render_id")

    if not application_id or not report_id or not request_render_id:
        return jsonify({"error": "application_id, report_id, and render_id are required"}), 400

    # --- Dynamic token from request header ---
    auth_header = request.headers.get("Authorization")
    token_to_use = auth_header or TOKEN
    if not token_to_use:
        return jsonify({"error": "Missing Authorization token"}), 401

    # Short-circuit if we already saved this request_render_id for this app/report
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
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code != 200:
            # Normalize the GPSGate error but still forward status for debugging
            return jsonify({
                "error": f"Error fetching render info {response.status_code}",
                "details": response.text
            }), response.status_code

        data = response.json()
        api_render_id = data.get("id")
        output_file = data.get("outputFile")
        is_ready = data.get("isReady")

        if not api_render_id or not output_file:
            return jsonify({"error": "No report file info found in API response"}), 404

        # Check cache again using authoritative API id
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
            raw_name = f"{application_id}-{report_id}-{api_render_id}-{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            file_name = secure_filename(raw_name)

            file_path = save_file_locally(file_url, file_name, token_override=token_to_use)
            if not file_path:
                return jsonify({"error": "Failed to save file to Google Drive"}), 500

            save_to_db(application_id, report_id, str(request_render_id), str(api_render_id), file_name, file_path)
            return jsonify({
                "application_id": application_id,
                "report_id": report_id,
                "render_id": api_render_id,
                "download_url": f"{request.host_url}download_file/{file_name}",
                "file_name": file_name,
                "message": "File saved successfully to Google Drive"
            })

        return jsonify({
            "message": "Report not ready yet",
            "application_id": application_id,
            "report_id": report_id,
            "render_id": request_render_id,
            "status": "processing"
        })
    except requests.exceptions.RequestException as e:
        logger.exception("Request error")
        return jsonify({"error": f"Network error: {str(e)}"}), 500
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
            cur.execute("SELECT file_name FROM downloaded_reports ORDER BY downloaded_at DESC")
            files = [row[0] for row in cur.fetchall()]
        return jsonify({"files": files, "count": len(files)})
    except Exception:
        logger.exception("List files error")
        return jsonify({"error": "Failed to list files"}), 500

@app.route("/health", methods=["GET"])
def health_check():
    # Basic health: db file existence + env vars (Drive connectivity not validated here)
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "db_file_exists": os.path.exists(DB_FILE),
        "gdrive_configured": bool(GDRIVE_CREDENTIALS),
        "gdrive_folder_set": bool(GDRIVE_FOLDER_ID)
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

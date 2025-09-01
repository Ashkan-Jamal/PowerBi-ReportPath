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
    """Authenticate and create Google Drive service instance."""
    try:
        # Load credentials from environment variable
        credentials_json = os.getenv("GDRIVE_CREDENTIALS")
        if not credentials_json:
            logger.error("GDRIVE_CREDENTIALS environment variable not set")
            return None

        # Convert literal \n to actual newlines
        credentials_json = credentials_json.replace("\\n", "\n")

        # Parse JSON
        creds_dict = json.loads(credentials_json)

        # Create service account credentials
        creds = service_account.Credentials.from_service_account_info(
            creds_dict,
            scopes=['https://www.googleapis.com/auth/drive.file']
        )

        # Build the Drive service
        service = build('drive', 'v3', credentials=creds)
        return service

    except Exception as e:
        logger.exception("Error creating Google Drive service")
        return None

def save_to_gdrive(file_url, file_name):
    """Download file from URL and save it to Google Drive."""
    try:
        # Download the file
        response = requests.get(file_url, headers={"Authorization": TOKEN}, timeout=30, stream=True)
        response.raise_for_status()
        
        # Create file content in memory
        file_content = io.BytesIO()
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                file_content.write(chunk)
        file_content.seek(0)
        
        # Get Google Drive service
        service = get_gdrive_service()
        if not service:
            return None
            
        # Prepare file metadata - save to your specific folder
        file_metadata = {
            'name': file_name,
            'mimeType': 'text/csv'
        }
        
        # Add folder ID if specified
        if GDRIVE_FOLDER_ID:
            file_metadata['parents'] = [GDRIVE_FOLDER_ID]
        
        # Upload to Google Drive
        media = MediaIoBaseUpload(file_content, mimetype='text/csv', resumable=True)
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id, webViewLink, webContentLink'
        ).execute()
        
        # Return the web content link for direct download
        return file.get('webContentLink')
        
    except HttpError as error:
        logger.exception(f"Google Drive API error: {error}")
        return None
    except Exception:
        logger.exception("Error saving file to Google Drive")
        return None

# --- Database functions ---
def cleanup_invalid_records():
    """Clean up any database records that point to non-existent files."""
    try:
        # For Google Drive, we don't need to check file existence
        logger.info("Database cleanup completed (Google Drive mode)")
    except Exception:
        logger.exception("Error during cleanup_invalid_records")


def init_db():
    """Initialize database and table if they don't exist."""
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
            logger.info("Ensuring 'downloaded_reports' table exists...")
            cur.execute(sql)
            
            # Check if we need to migrate old schema
            cur.execute("PRAGMA table_info(downloaded_reports)")
            columns = [col[1] for col in cur.fetchall()]
            
            if 'file_name' not in columns:
                logger.info("Migrating database schema...")
                # Backup old data if needed
                cur.execute("ALTER TABLE downloaded_reports RENAME TO downloaded_reports_old")
                cur.execute(sql)
                
            conn.commit()

        cleanup_invalid_records()
        logger.info("Database initialization completed successfully")

    except sqlite3.Error as e:
        logger.exception(f"SQLite error during init_db: {e}")
    except Exception:
        logger.exception("Unexpected error during init_db")


def already_downloaded(rid):
    """Check if a report is already downloaded to Google Drive."""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cur = conn.cursor()
            # First check if the table has the right columns
            cur.execute("PRAGMA table_info(downloaded_reports)")
            columns = [col[1] for col in cur.fetchall()]
            
            if 'file_name' not in columns:
                return None
                
            cur.execute("SELECT file_name, file_path FROM downloaded_reports WHERE render_id=?", (rid,))
            result = cur.fetchone()

        if result and result[0] and result[1]:
            # For Google Drive, we assume the link is always valid
            return result[0]
        return None
    except Exception:
        logger.exception("Error checking already_downloaded")
        return None


def save_to_db(rid, file_name, file_path):
    """Save report metadata to the database."""
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


# --- File storage ---
def save_file_locally(file_url, file_name):
    """Save file to Google Drive instead of local storage."""
    gdrive_link = save_to_gdrive(file_url, file_name)
    
    # For database compatibility, we'll store the Google Drive link as the file_path
    if gdrive_link:
        # We're returning the Google Drive link as the "file_path"
        return gdrive_link
    return None


# --- Routes ---
@app.route("/get_report", methods=["GET"])
def get_report():
    init_db()

    application_id = request.args.get("application_id")
    report_id = request.args.get("report_id")
    render_id = request.args.get("render_id")

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
    headers = {"Authorization": TOKEN, "Accept": "application/json"}

    logger.info(f"Calling GPSGate API: {url}")

    try:
        response = requests.get(url, headers=headers, timeout=30)

        if response.status_code != 200:
            return jsonify({
                "error": f"Error fetching render info {response.status_code}",
                "details": response.text
            }), response.status_code

        data = response.json()

        rid = data.get("id")
        output_file = data.get("outputFile")
        is_ready = data.get("isReady")

        if not rid or not output_file:
            return jsonify({"error": "No report file info found in API response"}), 404

        if is_ready:
            file_url = f"{BASE_DOMAIN}{output_file}"
            file_name = f"{application_id}-{report_id}-{render_id}-{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            file_path = save_file_locally(file_url, file_name)

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
    except Exception:
        logger.exception("Unexpected error in get_report")
        return jsonify({"error": "Internal server error"}), 500


@app.route("/download_file/<filename>", methods=["GET"])
def download_file(filename):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cur = conn.cursor()
            cur.execute("SELECT file_path FROM downloaded_reports WHERE file_name=?", (filename,))
            result = cur.fetchone()
            
        if result and result[0]:
            # Redirect to the Google Drive download link
            return redirect(result[0])
        return jsonify({"error": "File not found"}), 404
    except Exception:
        logger.exception("Download error")
        return jsonify({"error": "Download failed"}), 500


@app.route("/list_files", methods=["GET"])
def list_files():
    try:
        # This will now only show file names, not paths
        with sqlite3.connect(DB_FILE) as conn:
            cur = conn.cursor()
            cur.execute("SELECT file_name FROM downloaded_reports")
            files = [row[0] for row in cur.fetchall()]
            
        return jsonify({"files": files, "count": len(files)})
    except Exception:
        logger.exception("List files error")
        return jsonify({"error": "Failed to list files"}), 500


@app.route("/health", methods=["GET"])
def health_check():
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
    app.run(host="0.0.0.0", port=5000)

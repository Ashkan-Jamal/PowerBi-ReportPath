import os
import io
import json
import sqlite3
import logging
import requests
from flask import Flask, request, jsonify
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from googleapiclient.errors import HttpError

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Database file
DATABASE = "reports.db"

# Environment variables
TOKEN = os.getenv("API_TOKEN")
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID")
GDRIVE_CREDENTIALS = os.getenv("GDRIVE_CREDENTIALS")  # path to JSON file in /etc/secrets/


def init_db():
    """Initialize database."""
    conn = sqlite3.connect(DATABASE)
    c = conn.cursor()
    c.execute(
        """CREATE TABLE IF NOT EXISTS downloaded_reports (
            report_id TEXT NOT NULL,
            render_id TEXT NOT NULL,
            file_name TEXT NOT NULL,
            PRIMARY KEY (report_id, render_id)
        )"""
    )
    conn.commit()
    conn.close()


def get_gdrive_service():
    """Authenticate and create Google Drive service instance."""
    try:
        if not GDRIVE_CREDENTIALS or not os.path.exists(GDRIVE_CREDENTIALS):
            logger.error("Google Drive credentials file not found")
            return None

        with open(GDRIVE_CREDENTIALS, "r") as f:
            creds_dict = json.load(f)

        creds = service_account.Credentials.from_service_account_info(
            creds_dict,
            scopes=["https://www.googleapis.com/auth/drive.file"],
        )
        service = build("drive", "v3", credentials=creds)
        return service
    except Exception as e:
        logger.exception("Error creating Google Drive service")
        return None


def already_downloaded(report_id, render_id):
    """Check if report already downloaded and file still exists in Drive."""
    try:
        conn = sqlite3.connect(DATABASE)
        c = conn.cursor()
        c.execute(
            "SELECT file_name FROM downloaded_reports WHERE report_id=? AND render_id=?",
            (report_id, render_id),
        )
        row = c.fetchone()
        conn.close()

        if row:
            file_id = row[0]  # This is the Drive file id/name
            service = get_gdrive_service()
            if not service:
                return None
            try:
                # Validate file existence in Drive
                service.files().get(fileId=file_id, fields="id").execute()
                return file_id
            except HttpError as e:
                if e.resp.status == 404:
                    logger.warning(
                        f"Stale DB entry for report_id={report_id}, render_id={render_id}, cleaning up"
                    )
                    delete_downloaded_report(report_id, render_id)
                return None
        return None
    except Exception:
        logger.exception("Error checking if report already downloaded")
        return None


def save_to_gdrive(file_url, file_name):
    """Download file from URL and save it to Google Drive."""
    try:
        response = requests.get(
            file_url, headers={"Authorization": TOKEN}, timeout=30, stream=True
        )
        response.raise_for_status()

        file_content = io.BytesIO()
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                file_content.write(chunk)
        file_content.seek(0)

        service = get_gdrive_service()
        if not service:
            return None

        file_metadata = {"name": file_name, "mimeType": "text/csv"}
        if GDRIVE_FOLDER_ID:
            file_metadata["parents"] = [GDRIVE_FOLDER_ID]

        media = MediaIoBaseUpload(file_content, mimetype="text/csv", resumable=True)
        file = (
            service.files()
            .create(
                body=file_metadata,
                media_body=media,
                fields="id, webViewLink, webContentLink",
            )
            .execute()
        )
        return file.get("id")  # Store ID instead of link
    except HttpError as error:
        logger.exception(f"Google Drive API error: {error}")
        return None
    except Exception:
        logger.exception("Error saving file to Google Drive")
        return None


def record_download(report_id, render_id, file_id):
    """Record downloaded file in DB."""
    try:
        conn = sqlite3.connect(DATABASE)
        c = conn.cursor()
        c.execute(
            "INSERT OR REPLACE INTO downloaded_reports (report_id, render_id, file_name) VALUES (?, ?, ?)",
            (report_id, render_id, file_id),
        )
        conn.commit()
        conn.close()
    except Exception:
        logger.exception("Error recording download in DB")


def delete_downloaded_report(report_id, render_id):
    """Delete stale DB entry."""
    try:
        conn = sqlite3.connect(DATABASE)
        c = conn.cursor()
        c.execute(
            "DELETE FROM downloaded_reports WHERE report_id=? AND render_id=?",
            (report_id, render_id),
        )
        conn.commit()
        conn.close()
    except Exception:
        logger.exception("Error deleting stale DB record")


def cleanup_invalid_records():
    """Clean up stale DB records that point to missing Drive files."""
    try:
        conn = sqlite3.connect(DATABASE)
        c = conn.cursor()
        c.execute("SELECT report_id, render_id, file_name FROM downloaded_reports")
        rows = c.fetchall()
        conn.close()

        if not rows:
            return

        service = get_gdrive_service()
        if not service:
            return

        for report_id, render_id, file_id in rows:
            try:
                service.files().get(fileId=file_id, fields="id").execute()
            except HttpError as e:
                if e.resp.status == 404:
                    logger.warning(
                        f"Removing stale DB entry for report_id={report_id}, render_id={render_id}"
                    )
                    delete_downloaded_report(report_id, render_id)
    except Exception:
        logger.exception("Error cleaning up invalid DB records")


@app.route("/health", methods=["GET"])
def health_check():
    try:
        status = {"status": "ok", "database": False, "gdrive_configured": False}

        # DB check
        try:
            conn = sqlite3.connect(DATABASE)
            c = conn.cursor()
            c.execute("SELECT 1")
            conn.close()
            status["database"] = True
        except Exception:
            logger.exception("Database health check failed")

        # Check if Drive credentials file exists
        if GDRIVE_CREDENTIALS and os.path.exists(GDRIVE_CREDENTIALS):
            status["gdrive_configured"] = True

        return jsonify(status)
    except Exception:
        logger.exception("Health check failed")
        return jsonify({"status": "error"}), 500


if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=5000)

from flask import Flask, request, jsonify
import requests
import sqlite3
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import io
import os

# ---------------- CONFIG ----------------
BASE_DOMAIN = os.getenv("BASE_DOMAIN", "https://omantracking2.com")
TOKEN = os.getenv("TOKEN")  # Render Env Variable
DB_FILE = "reports.db"

# C-Google Drive setup
SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "service_account.json")
FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID")  # Your Drive folder

credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=["https://www.googleapis.com/auth/drive.file"]
)
drive_service = build("drive", "v3", credentials=credentials)
# -----------------------------------------


# --- Database functions ---
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS downloaded_reports
        (
            render_id INTEGER PRIMARY KEY,
            output_file TEXT,
            downloaded_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()


def already_downloaded(rid):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM downloaded_reports WHERE render_id=?", (rid,))
    exists = cur.fetchone() is not None
    conn.close()
    return exists


def save_to_db(rid, file_url):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO downloaded_reports (render_id, output_file) VALUES (?, ?)",
        (rid, file_url)
    )
    conn.commit()
    conn.close()


# --- Google Drive upload ---
def upload_to_drive(file_bytes, file_name):
    file_metadata = {
        "name": file_name,
        "parents": [FOLDER_ID]
    }
    media = MediaIoBaseUpload(io.BytesIO(file_bytes), mimetype="text/csv")
    uploaded_file = drive_service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id, webViewLink"
    ).execute()
    return uploaded_file["webViewLink"]


# --- Flask API ---
app = Flask(__name__)


@app.route("/get_report", methods=["GET"])
def get_report():
    init_db()

    application_id = request.args.get("application_id")
    report_id = request.args.get("report_id")
    render_id = request.args.get("render_id")

    if not application_id or not report_id or not render_id:
        return jsonify({"error": "application_id, report_id, and render_id are required"}), 400

    # Build API URL dynamically
    url = f"{BASE_DOMAIN}/comGpsGate/api/v.1/applications/{application_id}/reports/{report_id}/renderings/{render_id}"
    headers = {"Authorization": TOKEN, "Accept": "application/json"}

    # Fetch report info
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        return jsonify({"error": f"Error fetching render info {response.status_code}",
                        "details": response.text}), response.status_code

    data = response.json()
    rid = data.get("id")
    output_file = data.get("outputFile")
    is_ready = data.get("isReady")

    if not rid or not output_file:
        return jsonify({"error": "No report file info found"}), 404

    if already_downloaded(rid):
        return jsonify({"message": "Report already processed", "render_id": rid})

    if is_ready:
        file_url = f"{BASE_DOMAIN}{output_file}"
        csv_resp = requests.get(file_url, headers={"Authorization": TOKEN})
        if csv_resp.status_code != 200:
            return jsonify({"error": f"Failed to fetch CSV {csv_resp.status_code}"}), csv_resp.status_code

        # Name: appid-reportid-renderid-date.csv
        file_name = f"{application_id}-{report_id}-{render_id}-{datetime.now().strftime('%Y%m%d')}.csv"
        cloud_url = upload_to_drive(csv_resp.content, file_name)
        save_to_db(rid, cloud_url)

        return jsonify({
            "application_id": application_id,
            "report_id": report_id,
            "render_id": render_id,
            "csv_url": cloud_url
        })

    return jsonify({"message": "Report not ready yet",
                    "application_id": application_id,
                    "report_id": report_id,
                    "render_id": render_id})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

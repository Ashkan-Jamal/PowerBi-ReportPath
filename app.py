from flask import Flask, request, jsonify, send_file
import requests
import sqlite3
from datetime import datetime
import os

# ---------------- CONFIG ----------------
BASE_DOMAIN = os.getenv("BASE_DOMAIN", "https://omantracking2.com")
TOKEN = os.getenv("TOKEN")  # Render Env Variable
DB_FILE = "reports.db"
STORAGE_PATH = os.getenv("STORAGE_PATH", "/opt/render/reports")  # Render persistent disk

# Create storage directory if it doesn't exist
os.makedirs(STORAGE_PATH, exist_ok=True)
# -----------------------------------------

# --- Database functions ---
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS downloaded_reports
        (
            render_id INTEGER PRIMARY KEY,
            file_name TEXT,
            file_path TEXT,
            downloaded_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()

def already_downloaded(rid):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT file_name FROM downloaded_reports WHERE render_id=?", (rid,))
    result = cur.fetchone()
    conn.close()
    return result[0] if result else None

def save_to_db(rid, file_name, file_path):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO downloaded_reports (render_id, file_name, file_path) VALUES (?, ?, ?)",
        (rid, file_name, file_path)
    )
    conn.commit()
    conn.close()

# --- Local file storage ---
def save_file_locally(file_bytes, file_name):
    file_path = os.path.join(STORAGE_PATH, file_name)
    with open(file_path, 'wb') as f:
        f.write(file_bytes)
    return file_path

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

    # Check if already downloaded and return the download link
    existing_file = already_downloaded(rid)
    if existing_file:
        return jsonify({
            "message": "Report already processed", 
            "render_id": rid,
            "download_url": f"/download_file/{existing_file}",
            "file_name": existing_file
        })

    if is_ready:
        file_url = f"{BASE_DOMAIN}{output_file}"
        csv_resp = requests.get(file_url, headers={"Authorization": TOKEN})
        if csv_resp.status_code != 200:
            return jsonify({"error": f"Failed to fetch CSV {csv_resp.status_code}"}), csv_resp.status_code

        # Generate unique filename
        file_name = f"{application_id}-{report_id}-{render_id}-{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        file_path = save_file_locally(csv_resp.content, file_name)
        save_to_db(rid, file_name, file_path)

        return jsonify({
            "application_id": application_id,
            "report_id": report_id,
            "render_id": rid,
            "download_url": f"/download_file/{file_name}",
            "file_name": file_name,
            "message": "File saved successfully"
        })

    return jsonify({"message": "Report not ready yet",
                    "application_id": application_id,
                    "report_id": report_id,
                    "render_id": render_id})

@app.route("/download_file/<filename>", methods=["GET"])
def download_file(filename):
    file_path = os.path.join(STORAGE_PATH, filename)
    if os.path.exists(file_path):
        return send_file(file_path, as_attachment=True, download_name=filename)
    return jsonify({"error": "File not found"}), 404

@app.route("/list_files", methods=["GET"])
def list_files():
    files = os.listdir(STORAGE_PATH)
    return jsonify({"files": files, "count": len(files)})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

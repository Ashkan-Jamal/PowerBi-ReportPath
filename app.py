from flask import Flask, request, jsonify, send_file
import requests
import sqlite3
from datetime import datetime
import os
import logging

# ---------------- CONFIG ----------------
BASE_DOMAIN = os.getenv("BASE_DOMAIN", "https://omantracking2.com")
TOKEN = os.getenv("TOKEN")
DB_FILE = "reports.db"
STORAGE_PATH = os.getenv("STORAGE_PATH", "/opt/render/reports")

# Create storage directory if it doesn't exist
os.makedirs(STORAGE_PATH, exist_ok=True)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Flask API ---
app = Flask(__name__)

# --- Database functions ---
def cleanup_invalid_records():
    """Clean up any database records that point to non-existent files"""
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT render_id, file_path FROM downloaded_reports")
    records = cur.fetchall()
    
    deleted_count = 0
    for rid, file_path in records:
        if file_path and not os.path.exists(file_path):
            cur.execute("DELETE FROM downloaded_reports WHERE render_id=?", (rid,))
            deleted_count += 1
    
    conn.commit()
    conn.close()
    
    if deleted_count > 0:
        logger.info(f"Cleaned up {deleted_count} invalid database records")

def init_db():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS downloaded_reports
        (
            render_id INTEGER PRIMARY KEY,
            file_name TEXT NOT NULL,
            file_path TEXT NOT NULL,
            downloaded_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    try:
        cleanup_invalid_records()
    except Exception as e:
        logger.error(f"Cleanup error: {str(e)}")
    
    conn.commit()
    conn.close()

def already_downloaded(rid):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT file_name, file_path FROM downloaded_reports WHERE render_id=?", (rid,))
    result = cur.fetchone()
    conn.close()
    
    if result and result[0] and result[1]:
        if os.path.exists(result[1]):
            return result[0]
        else:
            conn = sqlite3.connect(DB_FILE)
            cur = conn.cursor()
            cur.execute("DELETE FROM downloaded_reports WHERE render_id=?", (rid,))
            conn.commit()
            conn.close()
            return None
    return None

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
            "download_url": f"/download_file/{existing_file}",
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
            
            csv_resp = requests.get(file_url, headers={"Authorization": TOKEN}, timeout=30)
            if csv_resp.status_code != 200:
                return jsonify({"error": f"Failed to fetch CSV {csv_resp.status_code}"}), csv_resp.status_code

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

        return jsonify({
            "message": "Report not ready yet",
            "application_id": application_id,
            "report_id": report_id,
            "render_id": render_id,
            "status": "processing"
        })

    except requests.exceptions.RequestException as e:
        logger.error(f"Request error: {str(e)}")
        return jsonify({"error": f"Network error: {str(e)}"}), 500
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500

@app.route("/download_file/<filename>", methods=["GET"])
def download_file(filename):
    try:
        file_path = os.path.join(STORAGE_PATH, filename)
        if os.path.exists(file_path):
            return send_file(file_path, as_attachment=True, download_name=filename)
        return jsonify({"error": "File not found"}), 404
    except Exception as e:
        logger.error(f"Download error: {str(e)}")
        return jsonify({"error": f"Download failed: {str(e)}"}), 500

@app.route("/list_files", methods=["GET"])
def list_files():
    try:
        files = os.listdir(STORAGE_PATH)
        return jsonify({"files": files, "count": len(files)})
    except Exception as e:
        logger.error(f"List files error: {str(e)}")
        return jsonify({"error": f"Failed to list files: {str(e)}"}), 500

@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})

@app.route("/admin/cleanup", methods=["POST"])
def admin_cleanup():
    try:
        cleanup_invalid_records()
        return jsonify({"message": "Database cleanup completed"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

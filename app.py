from flask import Flask, request, jsonify, redirect, send_file
from flask_cors import CORS
import requests
import sqlite3
from datetime import datetime
import os
import logging
from werkzeug.utils import secure_filename
import json
import time
import shutil

# ---------------- CONFIG ----------------
BASE_DOMAIN = os.getenv("BASE_DOMAIN", "https://omantracking2.com")
TOKEN = os.getenv("TOKEN")
DB_FILE = os.getenv("DB_FILE", "reports.db")
STORAGE_PATH = os.getenv("STORAGE_PATH", "/opt/render/reports")

os.makedirs(STORAGE_PATH, exist_ok=True)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
app = Flask(__name__)
CORS(app)

# --- Database ---
def init_db():
    """Initialize DB with simple schema"""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS downloaded_reports (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    application_id TEXT NOT NULL,
                    report_id TEXT NOT NULL,
                    request_render_id TEXT NOT NULL,
                    api_render_id TEXT NOT NULL,
                    file_name TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    downloaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(application_id, report_id, api_render_id, file_name)
                )
            """)
            conn.commit()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")

def already_downloaded(application_id, report_id, api_render_id=None, request_render_id=None):
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
    except Exception as e:
        logger.error(f"Error checking already_downloaded: {e}")
        return None

def save_to_db(application_id, report_id, request_render_id, api_render_id, file_name, file_path):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT OR REPLACE INTO downloaded_reports
                (application_id, report_id, request_render_id, api_render_id, file_name, file_path)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (application_id, report_id, request_render_id, api_render_id, file_name, file_path))
            conn.commit()
    except Exception as e:
        logger.error(f"Error saving to DB: {e}")

# --- File storage ---
def save_file_locally(file_url, file_name, token):
    try:
        # Ensure storage directory exists
        os.makedirs(STORAGE_PATH, exist_ok=True)
        
        safe_filename = secure_filename(file_name)
        local_path = os.path.join(STORAGE_PATH, safe_filename)
        
        # Validate path security
        if not os.path.abspath(local_path).startswith(os.path.abspath(STORAGE_PATH)):
            raise ValueError("Invalid file path")
        
        # Always use API token as-is
        headers = {
            "Authorization": token,
            "Accept": "application/json"
        }
        
        response = requests.get(file_url, headers=headers, timeout=30, stream=True)
        response.raise_for_status()
        
        # Write file
        temp_path = local_path + ".tmp"
        with open(temp_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        
        shutil.move(temp_path, local_path)
        return local_path
    except Exception as e:
        logger.error(f"Failed to save locally: {e}")
        return None

# --- Routes ---
@app.route("/", methods=["GET"])
def root():
    return jsonify({
        "message": "Report API is running",
        "endpoints": {
            "get_report": "/get_report?application_id=X&report_id=Y&render_id=Z",
            "download_file": "/download_file/<filename>",
            "health": "/health"
        }
    })

@app.route("/get_report", methods=["GET"])
def get_report():
    application_id = request.args.get("application_id")
    report_id = request.args.get("report_id")
    request_render_id = request.args.get("render_id")

    if not all([application_id, report_id, request_render_id]):
        return jsonify({"error": "application_id, report_id, and render_id are required"}), 400

    # Get token from Authorization header (NOT from query string ideally)
    auth_header = request.headers.get("Authorization")
    if auth_header:
        token = auth_header.strip()
    else:
        token = request.args.get("Authorization") or TOKEN
    
    if not token:
        return jsonify({"error": "Authorization token is required. Pass it in the Authorization header"}), 401

    logger.info(f"Request parameters: app_id={application_id}, report_id={report_id}, render_id={request_render_id}")
    logger.info(f"Using token: {token[:20]}...")  

    # Check if already downloaded
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

    url = f"{BASE_DOMAIN}/comGpsGate/api/v.1/applications/{application_id}/reports/{report_id}/renderings/{request_render_id}"
    headers = {
        "Authorization": token,  # Always API token, no prefix
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    try:
        logger.info(f"Calling GPSGate API with URL: {url}")
        logger.info(f"Using token: {token[:20]}...")
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code != 200:
            logger.error(f"GPSGate API error: {response.status_code} - {response.text}")
            return jsonify({
                "error": f"Error fetching render: {response.status_code}",
                "details": response.text
            }), response.status_code

        data = response.json()
        logger.info(f"API response: {data}")
        
        api_render_id = data.get("id")
        output_file = data.get("outputFile")
        is_ready = data.get("isReady", False)

        if not api_render_id or not output_file:
            return jsonify({"error": "No report file info found in response"}), 404

        # Check if this specific API render ID is already processed
        cached = already_downloaded(application_id, report_id, api_render_id=api_render_id)
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
            file_name = secure_filename(f"{application_id}-{report_id}-{api_render_id}.csv")
            
            file_path = save_file_locally(file_url, file_name, token)
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
        else:
            return jsonify({
                "message": "Report not ready yet", 
                "status": "processing",
                "api_render_id": api_render_id
            })

    except requests.exceptions.RequestException as e:
        logger.error(f"Network error: {e}")
        return jsonify({"error": "Network error contacting GPSGate API"}), 500
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.route("/download_file/<filename>", methods=["GET"])
def download_file(filename):
    try:
        filename = secure_filename(filename)
        if not filename:
            return jsonify({"error": "Invalid filename"}), 400
            
        with sqlite3.connect(DB_FILE) as conn:
            cur = conn.cursor()
            cur.execute("SELECT file_path FROM downloaded_reports WHERE file_name=?", (filename,))
            row = cur.fetchone()
            
            if row:
                file_path = row[0]
                
                if os.path.exists(file_path):
                    return send_file(file_path, as_attachment=True)
                else:
                    return jsonify({"error": "File not found on disk"}), 404
                    
        return jsonify({"error": "File not found in database"}), 404
    except Exception as e:
        logger.error(f"Download error: {e}")
        return jsonify({"error": "Download failed"}), 500

@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "db_file_exists": os.path.exists(DB_FILE),
        "storage_path_exists": os.path.exists(STORAGE_PATH)
    })

if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)), debug=True)

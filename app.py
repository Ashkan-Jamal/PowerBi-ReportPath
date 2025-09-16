from flask import Flask, request, jsonify
import requests
import os

app = Flask(__name__)
BASE_DOMAIN = os.getenv("BASE_DOMAIN", "https://omantracking2.com")

@app.route("/get_report", methods=["GET"])
def get_report():
    app_id = request.args.get("application_id")
    report_id = request.args.get("report_id")
    render_id = request.args.get("render_id")

    if not all([app_id, report_id, render_id]):
        return jsonify({"error": "application_id, report_id, render_id required"}), 400

    token = request.headers.get("Authorization")
    if not token:
        return jsonify({"error": "Authorization header required"}), 401

    url = f"{BASE_DOMAIN}/comGpsGate/api.v.1/applications/{app_id}/reports/{report_id}/renderings/{render_id}"
    headers = {"Authorization": token.strip(), "Accept": "application/json"}

    try:
        resp = requests.get(url, headers=headers, timeout=30)
        return jsonify({
            "status_code": resp.status_code,
            "response": resp.json() if resp.status_code == 200 else resp.text
        }), resp.status_code
    except requests.RequestException as e:
        return jsonify({"error": "Network/API error", "details": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))

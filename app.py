@app.route("/get_report", methods=["GET"])
def get_report():
    init_db()  # Ensure DB is ready

    application_id = request.args.get("application_id")
    report_id = request.args.get("report_id")
    request_render_id = request.args.get("render_id")

    if not application_id or not report_id or not request_render_id:
        return jsonify({"error": "application_id, report_id, and render_id are required"}), 400

    # --- Get token ---
    # Priority: Authorization header > query param > default TOKEN
    auth_header = request.headers.get("Authorization")
    token_to_use = auth_header or request.args.get("Authorization") or TOKEN
    if not token_to_use:
        return jsonify({"error": "Authorization token is required"}), 401

    # Log token safely (first 20 chars)
    logger.info(f"Using token: {token_to_use[:20]}...")

    # --- Check if already downloaded ---
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

    # --- Construct GPSGate API URL ---
    url = f"{BASE_DOMAIN}/comGpsGate/api.v.1/applications/{application_id}/reports/{report_id}/renderings/{request_render_id}"
    
    headers = {
        "Authorization": token_to_use,  # <--- v2 token, no Bearer
        "Accept": "application/json"
    }

    logger.info(f"Calling GPSGate API URL: {url}")
    logger.info(f"Headers: {headers}")

    try:
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 404:
            return jsonify({"error": "Render not found (404)", "details": response.text}), 404
        if response.status_code != 200:
            return jsonify({"error": f"API error {response.status_code}", "details": response.text}), response.status_code

        data = response.json()
        api_render_id = data.get("id")
        output_file = data.get("outputFile")
        is_ready = data.get("isReady", False)

        if not api_render_id or not output_file:
            return jsonify({"error": "No report file info found"}), 404

        # --- Check DB by api_render_id ---
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
            file_path = save_file_locally(file_url, file_name, token_override=token_to_use)

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
        logger.exception(f"Network error: {e}")
        return jsonify({"error": "Network error contacting GPSGate API"}), 500
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        return jsonify({"error": "Internal server error"}), 500

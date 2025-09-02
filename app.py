# --- Google Drive Functions ---
def get_gdrive_service():
    """Authenticate and create Google Drive service instance using a secret file."""
    try:
        # Path to your secret file in Render
        secret_path = "/etc/secrets/power-bi-x-gpsgate-b793752d1634.json"
        
        # Read JSON credentials from file
        with open(secret_path, "r") as f:
            creds_dict = json.load(f)

        # Create service account credentials
        creds = service_account.Credentials.from_service_account_info(
            creds_dict,
            scopes=['https://www.googleapis.com/auth/drive.file']
        )

        # Build the Drive service
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


def save_to_gdrive(file_url, file_name, gpsgate_token=None):
    """
    Download file from URL and save it to Google Drive.
    - gpsgate_token is only used for downloading the file (from GPSGate)
    - Google Drive upload uses the service account
    """
    try:
        # Download the file using either provided token or default TOKEN
        auth_header = {"Authorization": gpsgate_token} if gpsgate_token else {"Authorization": TOKEN}
        response = requests.get(file_url, headers=auth_header, timeout=30, stream=True)
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
            fields='id, webViewLink, webContentLink',
            supportsAllDrives=True
        ).execute()
        
        # Return the web content link for direct download
        return file.get('webContentLink')
        
    except HttpError as error:
        logger.exception(f"Google Drive API error: {error}")
        return None
    except Exception:
        logger.exception("Error saving file to Google Drive")
        return None


# --- File storage ---
def save_file_locally(file_url, file_name, gpsgate_token=None):
    """
    Save file to Google Drive instead of local storage.
    gpsgate_token is only needed for downloading the file from GPSGate.
    """
    gdrive_link = save_to_gdrive(file_url, file_name, gpsgate_token)
    
    # For database compatibility, store the Google Drive link as the file_path
    if gdrive_link:
        return gdrive_link
    return None

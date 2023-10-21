# pip install google-auth google-auth-oauthlib google-api-python-client

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from datetime import datetime

# Load credentials from the JSON file you downloaded
credentials = service_account.Credentials.from_service_account_file(
    './credentials.json', scopes=['https://www.googleapis.com/auth/drive']
)

name = datetime.today().strftime('%c')
# Create a Google Drive API client
drive_service = build('drive', 'v3', credentials=credentials)

def upload_file(file_path, folder_id=None):
    file_metadata = {
<<<<<<< HEAD
        'name': 'test.xlsx',  # Replace with the desired file name
=======
        'name': f"{name}.md",  # Replace with the desired file name
>>>>>>> 4bb372578b4285673bfde48618a90a682aca576b
        'parents': [folder_id] if folder_id else []
    }

    media = MediaFileUpload(file_path, resumable=True)
    file = drive_service.files().create(
        body=file_metadata, media_body=media, fields='id',
    ).execute()

    print(f'File ID: {file.get("id")}')

# Example usage:
upload_file('./test.xlsx', folder_id='')

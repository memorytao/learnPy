from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# Load credentials from the JSON file you downloaded
credentials = service_account.Credentials.from_service_account_file(
    './credentials.json', scopes=['https://www.googleapis.com/auth/drive']
)

# Create a Google Drive API client
drive_service = build('drive', 'v3', credentials=credentials)

def upload_file(file_path, folder_id=None):
    file_metadata = {
        'name': 'test.xlsx',  # Replace with the desired file name
        'parents': [folder_id] if folder_id else []
    }

    media = MediaFileUpload(file_path, resumable=True)
    file = drive_service.files().create(
        body=file_metadata, media_body=media, fields='id',
    ).execute()

    print(f'File ID: {file.get("id")}')

# Example usage:
upload_file('./test.xlsx', folder_id='')
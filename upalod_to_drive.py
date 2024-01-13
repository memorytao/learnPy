import os
import subprocess
import pandas as pd
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

FORMAT_YYYYMMDD = "%Y%m%d"
INSERT_TABLE = "CVM_CMPGN_MASTER_PROCESS_LOG"



def highlight_status(value):
  # print(value)
  if value == 'Delay':
    return "background-color:red; color:white"
  elif value == 'Normal':
    return "background-color:green; color:black"
  elif value == 'Adhoc':
     return "background-color:black; color:black"
  else:
    return ""

def convertCSVtoEexcel(file_name):
  df = pd.read_csv(f"./{file_name}.csv",sep="|", usecols=[3,4,5,6,7,8], names=["TABLE NAME", "BUSINESS DESCRIPTION", "SLA Data", "Processed Date", "Records Amount" , "Status"])
  df_styled = df.style.applymap(highlight_status, subset=['Status'])
  df_styled.to_excel("./color.xlsx", index=False)



def get_file():
    try:
        create_date = datetime.now().strftime(FORMAT_YYYYMMDD)
        file_name = f"{INSERT_TABLE}_{create_date}_1.csv"
        copy_command = f"jpt_dtac_user10@172.20.23.22:/data/CVM/table_monitoring/scripts/report/{file_name}"
        subprocess.call(['scp',copy_command,'./'])
        convertCSVtoEexcel(file_name)
        upload_file(file_name)
    except Exception as err :
        print( str(err) )


def upload_file(file_name):
    hasFile = os.path.isfile(f"{os.getcwd()}\{file_name}")

    if(hasFile):
        try:
            credentials = service_account.Credentials.from_service_account_file('./credentials.json', scopes=['https://www.googleapis.com/auth/drive'])
            drive_service = build('drive', 'v3', credentials=credentials)
            folder_id='19MTjDhfJigonI-e9tBhuGDcODiJXTWht'

            for new_name in [file_name, str(file_name).replace(".csv",".xlsx") ]:
                file_metadata = {
                    'name': f"{new_name}",  # Replace with the desired file name
                    'parents': [folder_id] if folder_id else []
                }
                file = f"{os.getcwd()}\{file_name}"
                media = MediaFileUpload(file, resumable=True)
                file = drive_service.files().create(body=file_metadata, media_body=media, fields='id',).execute()
                print('Uploaded file successfully')

           
        except Exception as err :
            print(str(err))
    else :
        print('File not found')

    

if __name__ == '__main__':
    get_file()
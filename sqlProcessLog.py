import sqlalchemy as db
import pandas as pd
from datetime import datetime
from datetime import timedelta
import sys


# Create a connection to the database
DRIVER_URL = "sqlite:///C:/Users/Tao/AppData/Roaming/DBeaverData/workspace6/.metadata/sample-database-sqlite-1/Chinook.db"
# DRIVER_URL = "sqlite:////Users/memorytao/Library/DBeaverData/workspace6/.metadata/sample-database-sqlite-1/Chinook.db"
engine = db.create_engine(DRIVER_URL)
currentTime = datetime.now()
currTimeStr = currentTime.strftime("%Y-%m-%d %H:%M:%S")


# ROUND_AT_DAY = sys.argv[1] # round of the day (eg. 1 = 9:00 , 2 = 12:00, 3 = 15:00)
ROUND_AT_DAY = 1

INSERT_TABLE = "CVM_CMPGN_MASTER_PROCESS_LOG"
SQL = 'SELECT * FROM CVM_CMPGN_MASTER_TABLE ccmt'

DAY_ID = []
ROUND = []
SCHEMA_NAME = []
TABLE_NAME = []
LATEST_DATE = []
DATA_AMOUNT = []
STATUS = []
CREATE_DTTM = []


# Query the data
i = 0;
with engine.connect() as conn:

    query = db.text(SQL)
    result = conn.execute(query)
    # Create a Pandas DataFrame from the results of the SQL query
    df = pd.DataFrame(result)
    
    # Replace None with "Unknown" in all columns
    df.replace(to_replace='None', value='', inplace=True)

    # DAY_ID.append(df['CHECK_FIELD_NAME_1'])
    # ROUND.append(ROUND_AT_DAY)
    SCHEMA_NAME.append(df['SCHEMA_NAME'])
    TABLE_NAME.append(df['TABLE_NAME'])
    # LATEST_DATE.append(ROUND_AT_DAY)
    # DATA_AMOUNT.append('')
    # STATUS.append('')
    # CREATE_DTTM.append(currTimeStr)

print(i)
data_to_process_log_table = pd.DataFrame(
    {
        'DAY_ID': DAY_ID,
        'ROUND': ROUND,
        'SCHEMA_NAME': SCHEMA_NAME,
        'TABLE_NAME': TABLE_NAME,
        'LATEST_DATE': LATEST_DATE,
        'DATA_AMOUNT': DATA_AMOUNT,
        'STATUS': STATUS,
        'CREATE_DTTM': CREATE_DTTM,
    }
)
data_to_process_log_table.to_sql(
    name=INSERT_TABLE, con=engine, index=False, if_exists='append')

import sqlalchemy as db
import pandas as pd
from datetime import datetime
import sys
import os


CSV_PATH = ""
TMP_PATH = ""
DATE_FORMAT = "%Y%m%d"
round_job = sys.argv[1]

# Data containers
DAY_ID_DATA = []
ROUND_DATA = []
SCHEMA_NAME_DATA = []
TABLE_NAME_DATA = []
LATEST_DATE_DATA = []
DATA_AMOUNT_DATA = []
STATUS_DATA = []
CREATE_DTTM_DATA = []

INSERT_TABLE = "CVM_CMPGN_MASTER_PROCESS_LOG"

# Create a connection to the database
DRIVER_URL = "netezza+nzpy://T968672:sa4+VdllVLi$UkV@10.50.78.21:5480/NZDATAMART"
engine = db.create_engine(DRIVER_URL)

SQL_CVM_CMPGN_MASTER_TABLE = """
    SELECT SCHEMA_NAME, TABLE_NAME, TABLE_DESCRIPTION, TABLE_SHORT_DESCRIPTION, TABLE_CATEGORY, SLA_DATA, SLA_TIME,
           MIN_DATA_THRESHOLD, MAX_DATA_THRESHOLD, CHECK_FIELD_NAME_1, DATA_TYPE, CHECK_FIELD_NAME_2, 
           CHECK_FIELD_NAME_3, CHECK_FIELD_NAME_4, UPDATE_DTTM, UPDATE_BY
    FROM CVM_CMPGN_MASTER_TABLE 
"""

with engine.connect() as conn:
    cvm_campaign_from_sql = conn.execute(db.text(SQL_CVM_CMPGN_MASTER_TABLE))
    cvm_master_data = pd.DataFrame(cvm_campaign_from_sql,
                                   columns=[
                                       "SCHEMA_NAME", "TABLE_NAME", "TABLE_DESCRIPTION", "TABLE_SHORT_DESCRIPTION",
                                       "TABLE_CATEGORY", "SLA_DATA", "SLA_TIME", "MIN_DATA_THRESHOLD",
                                       "MAX_DATA_THRESHOLD", "CHECK_FIELD_NAME_1", "DATA_TYPE", "CHECK_FIELD_NAME_2",
                                       "CHECK_FIELD_NAME_3", "CHECK_FIELD_NAME_4", "UPDATE_DTTM", "UPDATE_BY"
                                   ]
                                   )

    for idx, master in cvm_master_data.iterrows():
        TABLE_CATEGORY = master['TABLE_CATEGORY']
        DATA_TYPE = master['DATA_TYPE']
        CHECK_FIELD_NAME_1 = master['CHECK_FIELD_NAME_1']
        TABLE_NAME = master['TABLE_NAME']
        MAX_DATA_THRESHOLD = master['MAX_DATA_THRESHOLD']
        MIN_DATA_THRESHOLD = master['MIN_DATA_THRESHOLD']
        SCHEMA_NAME = master['SCHEMA_NAME']

        # if TABLE_NAME == 'FCT_INVC_PROD':
        #     # Skip the FCT_INVC_PROD table
        #     continue

        # SQL to get the latest date from the table
        SQL_CHECK_LATEST = """SELECT to_char(MAX(DATE({})),'YYYYMMDD') AS LOADDATE FROM {} ;"""
        db_latest_time = conn.execute(
            db.text(SQL_CHECK_LATEST.format(CHECK_FIELD_NAME_1, TABLE_NAME)))
        latest_date = db_latest_time.fetchall()[0][0]
        currentTime = datetime.strptime(
            datetime.now().strftime(DATE_FORMAT), DATE_FORMAT)
        latest_update_date = datetime.strptime(latest_date, DATE_FORMAT)

        SQL_COUNT_AMOUNT = """SELECT COUNT(1) FROM {} """.format(TABLE_NAME)

        if TABLE_CATEGORY == 'TRANSACTION':
            SQL_COUNT_AMOUNT += "WHERE to_char(DATE({}),'YYYYMMDD') = '{}' ".format(
                CHECK_FIELD_NAME_1, latest_update_date.strftime(DATE_FORMAT))

        max_sub = int(MAX_DATA_THRESHOLD)
        min_sub = int(MIN_DATA_THRESHOLD)

        db_amount = conn.execute(db.text(SQL_COUNT_AMOUNT))
        amount_sub = int(db_amount.fetchall()[0][0])

        # Determine the status based on the current date and the latest update date
        status = ""
        if (currentTime == latest_update_date and (amount_sub >= min_sub and amount_sub <= max_sub)):
            status = "Normal"
        elif (currentTime > latest_update_date):
            status = "Delay"
        else:
            status = "AbNormal"

        print('latest:', latest_date, 'check latest:',
              SQL_CHECK_LATEST.format(CHECK_FIELD_NAME_1, TABLE_NAME))
        print('amt:', SQL_COUNT_AMOUNT, 'amount:', amount_sub)
        print('table cate:', TABLE_CATEGORY, 'table:', TABLE_NAME,
              'max:', max_sub, 'min:', min_sub, 'status:', status)

        # Append data to the containers
        DAY_ID_DATA.append(currentTime.strftime(DATE_FORMAT))
        ROUND_DATA.append(round_job)
        SCHEMA_NAME_DATA.append(SCHEMA_NAME)
        TABLE_NAME_DATA.append(TABLE_NAME)
        LATEST_DATE_DATA.append(latest_date)
        DATA_AMOUNT_DATA.append(amount_sub)
        STATUS_DATA.append(status)
        CREATE_DTTM_DATA.append(datetime.now())

# Create a DataFrame with the collected data
data_to_process_log_table = pd.DataFrame({
    'DAY_ID': DAY_ID_DATA,
    'ROUND': ROUND_DATA,
    'SCHEMA_NAME': SCHEMA_NAME_DATA,
    'TABLE_NAME': TABLE_NAME_DATA,
    'LATEST_DATE': LATEST_DATE_DATA,
    'DATA_AMOUNT': DATA_AMOUNT_DATA,
    'STATUS': STATUS_DATA,
    'CREATE_DTTM': CREATE_DTTM_DATA,
})

# Insert data to the database table
data_to_process_log_table.to_sql(
    name=INSERT_TABLE, con=engine, index=False, if_exists='append')

# Save data to a CSV file
data_to_process_log_table.to_csv('./{}_{}_{}.csv'.format(INSERT_TABLE, datetime.now().strftime(DATE_FORMAT), round_job),
                                 index=False, sep="|", header=None)

# Commented out section. Check if it's required, otherwise it can be removed.
try:
    isDir = os.path.isdir(CSV_PATH)
    if (not isDir):
        os.mkdir(CSV_PATH)
    
except FileExistsError:
    print(" File or Dir error")

sys.exit(0)

#!/usr/bin/python3
import os
import sys
from datetime import datetime
import pandas as pd
import subprocess
import logging

subprocess.check_call([sys.executable, "-m", "pip",
                      "install", "nzpy", "--user"])
import nzpy as nz


CSV_PATH = ""
TMP_PATH = ""
FORMAT_YYYYMMDD = "%Y%m%d"
FORMAT_YYYYMMDD_HHMMSS = "%Y/%m/%d %H:%M:%S"
FORMAT_YYMMDD = "%y%m%d"
round_job = sys.argv[1]

# Data containers
DAY_ID_DATA = []
ROUND_DATA = []
SCHEMA_NAME_DATA = []
TABLE_NAME_DATA = []
TABLE_DESC_DATA = []
SLA_DATA_T = []
LATEST_DATE_DATA = []
DATA_AMOUNT_DATA = []
STATUS_DATA = []
CREATE_DTTM_DATA = []

INSERT_TABLE = "CVM_CMPGN_MASTER_PROCESS_LOG"

USER_NAME = "T968672"
PASSWORD = "sa4+VdllVLi$UkV"
HOST = "10.50.78.21"
POST = 5480
DATABASE = "NZDATAMART"


def send_mail():
    # Commented out section. Check if it's required, otherwise it can be removed.
    try:
        is_dir = os.path.isdir(CSV_PATH)
        if (not is_dir):
            os.mkdir(CSV_PATH)
            cmd = "(sh /data/CVM/monitor/script/file_name.sh) & "
            os.system(cmd)
    except Exception as err:
        print(f"Error occurs :::  {err=}, {type(err)=}")
    sys.exit(0)


conn = nz.connect(
    user=USER_NAME,
    password=PASSWORD,
    host=HOST,
    port=POST,
    database=DATABASE
)


def sql_cvm_cmpgn_master():
    sql = """SELECT SCHEMA_NAME,TABLE_NAME,TABLE_DESCRIPTION,TABLE_SHORT_DESCRIPTION, 
            TABLE_CATEGORY,SLA_DATA, SLA_TIME,MIN_DATA_THRESHOLD,
            MAX_DATA_THRESHOLD,CHECK_FIELD_NAME_1,DATA_TYPE,CHECK_FIELD_NAME_2, 
            CHECK_FIELD_NAME_3,CHECK_FIELD_NAME_4,UPDATE_DTTM,UPDATE_BY
            FROM CVM_CMPGN_MASTER_TABLE 
        """
    return sql

with conn.cursor() as cursor:
    SQL_CVM_CMPGN_MASTER_TABLE = """
    SELECT SCHEMA_NAME, TABLE_NAME, TABLE_DESCRIPTION, TABLE_SHORT_DESCRIPTION, TABLE_CATEGORY, SLA_DATA, SLA_TIME,
        MIN_DATA_THRESHOLD, MAX_DATA_THRESHOLD, CHECK_FIELD_NAME_1, DATA_TYPE, CHECK_FIELD_NAME_2, 
        CHECK_FIELD_NAME_3, CHECK_FIELD_NAME_4, UPDATE_DTTM, UPDATE_BY
    FROM CVM_CMPGN_MASTER_TABLE 
"""
    cursor.execute(SQL_CVM_CMPGN_MASTER_TABLE)
    cvm_campaign_from_sql = cursor.fetchall()
    cvm_master_data = pd.DataFrame(cvm_campaign_from_sql,
                                    columns=[
                                        "SCHEMA_NAME", "TABLE_NAME", "TABLE_DESCRIPTION", "TABLE_SHORT_DESCRIPTION",
                                        "TABLE_CATEGORY", "SLA_DATA", "SLA_TIME", "MIN_DATA_THRESHOLD",
                                        "MAX_DATA_THRESHOLD", "CHECK_FIELD_NAME_1", "DATA_TYPE", "CHECK_FIELD_NAME_2",
                                        "CHECK_FIELD_NAME_3", "CHECK_FIELD_NAME_4", "UPDATE_DTTM", "UPDATE_BY"
                                    ]
                                    )

    # print(cvm_master_data.to_string(index=False))
    try:
        for idx, master in cvm_master_data.iterrows():
            SCHEMA_NAME = master['SCHEMA_NAME']
            TABLE_NAME = master['TABLE_NAME']
            TABLE_DESCRIPTION = master['TABLE_DESCRIPTION']
            TABLE_SHORT_DESCRIPTION = master['TABLE_SHORT_DESCRIPTION']
            TABLE_CATEGORY = master['TABLE_CATEGORY']
            SLA_DATA = master['SLA_DATA']
            SLA_TIME = master['SLA_TIME']
            MAX_DATA_THRESHOLD = master['MAX_DATA_THRESHOLD']
            MIN_DATA_THRESHOLD = master['MIN_DATA_THRESHOLD']
            CHECK_FIELD_NAME_1 = master['CHECK_FIELD_NAME_1']
            DATA_TYPE = master['DATA_TYPE']
            CHECK_FIELD_NAME_2 = master['CHECK_FIELD_NAME_2']
            CHECK_FIELD_NAME_3 = master['CHECK_FIELD_NAME_3']
            CHECK_FIELD_NAME_4 = master['CHECK_FIELD_NAME_4']
            UPDATE_DTTM = master['UPDATE_DTTM']
            UPDATE_BY = master['UPDATE_BY']

            if TABLE_NAME == 'FCT_INVC_PROD':
                # Skip the FCT_INVC_PROD table
                continue

            # SQL to get the latest date from the table
            SQL_CHECK_LATEST = """SELECT to_char(MAX(DATE({})),'YYYYMMDD') AS LOADDATE FROM {} ;"""

            db_latest_time = cursor.execute(
                SQL_CHECK_LATEST.format(CHECK_FIELD_NAME_1, TABLE_NAME))
            latest_date = db_latest_time.fetchall()[0][0]
            current_time = datetime.strptime(
                datetime.now().strftime(FORMAT_YYYYMMDD), FORMAT_YYYYMMDD)
            latest_update_date = datetime.strptime(latest_date, FORMAT_YYYYMMDD)

            SQL_COUNT_AMOUNT = """SELECT COUNT(1) FROM {} """.format(
                TABLE_NAME)

            if TABLE_CATEGORY == 'TRANSACTION':
                SQL_COUNT_AMOUNT += "WHERE to_char(DATE({}),'YYYYMMDD') = '{}' ".format(
                    CHECK_FIELD_NAME_1, latest_update_date.strftime(FORMAT_YYYYMMDD))

            max_sub = int(MAX_DATA_THRESHOLD)
            min_sub = int(MIN_DATA_THRESHOLD)

            db_amount = cursor.execute(SQL_COUNT_AMOUNT)
            amount_sub = int(db_amount.fetchall()[0][0])

            # Determine the status based on the current date and the latest update date
            status = ""
            if (current_time == latest_update_date and (amount_sub >= min_sub and amount_sub <= max_sub)):
                status = "Normal"
            elif (current_time > latest_update_date):
                status = "Delay"
            else:
                status = "AbNormal"


            # Append data to the containers
            current_time_yyyymmdd = current_time.strftime(FORMAT_YYYYMMDD)
            create_datetime = datetime.now().strftime(FORMAT_YYYYMMDD_HHMMSS)
            DAY_ID_DATA.append(current_time_yyyymmdd)
            ROUND_DATA.append(round_job)
            SCHEMA_NAME_DATA.append(SCHEMA_NAME)
            TABLE_NAME_DATA.append(TABLE_NAME)
            TABLE_DESC_DATA.append(TABLE_DESCRIPTION)
            SLA_DATA_T.append(SLA_DATA)
            LATEST_DATE_DATA.append(latest_date)
            DATA_AMOUNT_DATA.append(amount_sub)
            STATUS_DATA.append(status)
            CREATE_DTTM_DATA.append(create_datetime)

            # Insert data to the database table
            try:
                cursor.execute("insert into {} values (?,?,?,?,?,?,?,?)".format(INSERT_TABLE),
                            (current_time_yyyymmdd, round_job, SCHEMA_NAME, TABLE_NAME, latest_date, amount_sub, status, datetime.now().strftime(FORMAT_YYYYMMDD_HHMMSS)))
            except Exception as e:
                print("Error occurred during insertion: ", e)

    except Exception as err:
        print("Error occurs ::: ", err, "error type", type(err))


# Create a DataFrame with the collected data
data_to_process_log_table = pd.DataFrame({
    'DAY_ID': DAY_ID_DATA,
    'ROUND': ROUND_DATA,
    'SCHEMA_NAME': SCHEMA_NAME_DATA,
    'TABLE_NAME': TABLE_NAME_DATA,
    'TABLE_DESCRIPTION': TABLE_DESC_DATA,
    'SLA_DATA': SLA_DATA_T,
    'LATEST_DATE': LATEST_DATE_DATA,
    'DATA_AMOUNT': DATA_AMOUNT_DATA,
    'STATUS': STATUS_DATA,
    'CREATE_DTTM': CREATE_DTTM_DATA,
})

# Save data to a CSV file
data_to_process_log_table.to_csv('./{}_{}_{}.csv'.format(INSERT_TABLE, datetime.now().strftime(FORMAT_YYYYMMDD), round_job),
                                 index=False, sep="|", header=None)





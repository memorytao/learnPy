#!/usr/bin/python3


import os
import sys
import re
import logging
import subprocess
import pandas as pd
import nzpy as nz
from datetime import datetime

subprocess.check_call([sys.executable, "-m", "pip",
                      "install", "nzpy", "--user"])

CSV_PATH = ""
TMP_PATH = ""
FORMAT_YYYYMMDD = "%Y%m%d"
FORMAT_YYYYMMDD_HHMMSS = "%Y/%m/%d %H:%M:%S"
FORMAT_YYMMDD = "%y%m%d"
CURRENT_DATE = ""
ROUND_JOB = sys.argv[1]
TARGET_DIRECTORY = "/path/to/your/csv/files/directory"

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

LIST_SLECTED_COLUMN = ["SCHEMA_NAME", "TABLE_NAME", "TABLE_DESCRIPTION", "TABLE_SHORT_DESCRIPTION",
                       "TABLE_CATEGORY", "SLA_DATA", "SLA_TIME", "MIN_DATA_THRESHOLD",
                       "MAX_DATA_THRESHOLD", "CHECK_FIELD_NAME_1", "DATA_TYPE", "CHECK_FIELD_NAME_2",
                       "CHECK_FIELD_NAME_3", "CHECK_FIELD_NAME_4", "UPDATE_DTTM", "UPDATE_BY"]

INSERT_TABLE = "CVM_CMPGN_MASTER_PROCESS_LOG"


def get_connection():
    try:
        USER_NAME = "T968672"
        PASSWORD = "sa4+VdllVLi$UkV"
        HOST = "10.50.78.21"
        POST = 5480
        DATABASE = "NZDATAMART"
        return nz.connect(user=USER_NAME, password=PASSWORD, host=HOST, port=POST, database=DATABASE).cursor()
    except Exception as err:
        print('error to connect the database ', type(err), err)
        logging.error('error to connect the database')
        logging.error('please check your connection')


def sql_cvm_cmpgn_master():
    sql = """SELECT SCHEMA_NAME,TABLE_NAME,TABLE_DESCRIPTION,TABLE_SHORT_DESCRIPTION, 
            TABLE_CATEGORY,SLA_DATA, SLA_TIME,MIN_DATA_THRESHOLD,
            MAX_DATA_THRESHOLD,CHECK_FIELD_NAME_1,DATA_TYPE,CHECK_FIELD_NAME_2, 
            CHECK_FIELD_NAME_3,CHECK_FIELD_NAME_4,UPDATE_DTTM,UPDATE_BY
            FROM CVM_CMPGN_MASTER_TABLE 
        """
    return sql


def insert_to_process_log(current_time_yyyymmdd, schema_name, table_name, latest_date, amount_sub, status):
    logging.info('inserting data to database....')
    try:
        cursor.execute("insert into {} values (?,?,?,?,?,?,?,?)".format(INSERT_TABLE),
                       (current_time_yyyymmdd, ROUND_JOB, schema_name, table_name, latest_date, amount_sub, status, datetime.now().strftime(FORMAT_YYYYMMDD_HHMMSS)))

    except Exception as e:
        print("error occurred during insertion: ", e)


def create_csv():
    logging.info('creating csv file....')
    try:
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
        create_date = datetime.now().strftime(FORMAT_YYYYMMDD)
        data_to_process_log_table.to_csv(
            './{}_{}_{}.csv'.format(INSERT_TABLE, create_date, ROUND_JOB), index=False, sep="|", header=None)

    except Exception as err:
        print('create csv error', type(err), err)
        logging.error('create csv error')
    logging.info('created csv file!')


def send_mail():
    # Commented out section. Check if it's required, otherwise it can be removed.
    logging.info('sending email.... ')
    try:
        is_dir = os.path.isdir(CSV_PATH)
        if (not is_dir):
            os.mkdir(CSV_PATH)
            cmd = "(sh /data/CVM/monitor/script/file_name.sh) & "
            os.system(cmd)
        logging.info('email has sent! ')

    except Exception as err:
        print("error sending email ", type, err)
        logging.error('send email error! ')
    sys.exit(0)


def get_file_creation_date(file_name):
    # Extract the date from the file name using the YYYYMMDD pattern
    match = re.search(r"\d{8}", file_name)
    if match:
        date_str = match.group()
        return datetime.datetime.strptime(date_str, "%Y%m%d").date()
    return None


def remove_csv_files(directory):
    try:
        # Get the current date
        current_date = datetime.date.today()
        # List all files in the directory
        files = os.listdir(directory)
        # Filter and remove CSV files older than 7 days
        for file_name in files:
            if file_name.lower().endswith(".csv"):
                creation_date = get_file_creation_date(file_name)
                if creation_date:
                    file_age = (current_date - creation_date).days
                    if file_age > 7:
                        file_path = os.path.join(directory, file_name)
                        os.remove(file_path)
                        # print("Removed ", file_name)
                        logging.info(" remove.... ", file_name)
    except Exception as err:
        print('Error while remove file', type(err), err)


def create_report_form(schema_name, table_name, table_description, sla_data, latest_date, amount_sub, status):
    # create form daily insert to CVM_CMPGN_MASTER_PROCESS_LOG
    try:
        current_time_yyyymmdd = datetime.now().strftime(FORMAT_YYYYMMDD)
        create_datetime = datetime.now().strftime(FORMAT_YYYYMMDD_HHMMSS)
        DAY_ID_DATA.append(current_time_yyyymmdd)
        ROUND_DATA.append(ROUND_JOB)
        SCHEMA_NAME_DATA.append(schema_name)
        TABLE_NAME_DATA.append(table_name)
        TABLE_DESC_DATA.append(table_description)
        SLA_DATA_T.append(sla_data)
        LATEST_DATE_DATA.append(latest_date)
        DATA_AMOUNT_DATA.append(amount_sub)
        STATUS_DATA.append(status)
        CREATE_DTTM_DATA.append(create_datetime)

        # insert to CVM_CMPGN_MASTER_PROCESS_LOG
        insert_to_process_log(current_time_yyyymmdd=current_time_yyyymmdd, schema_name=schema_name,
                              table_name=table_name, latest_date=latest_date, amount_sub=amount_sub, status=status)
    except Exception as err:
        print('create report error! ', type(err), err)
        logging.error('')


def check_latest_updated(check_field_name_1, table_name):

    logging.info('status is checking....')
    try:
        # SQL to get the latest date from the table
        res = {
            'status': '',
            'amount': 0,
            'latest_date': ''
        }
        SQL_CHECK_LATEST = """SELECT to_char(MAX(DATE({})),'YYYYMMDD') AS LOADDATE FROM {} ;"""

        db_latest_time = cursor.execute(
            SQL_CHECK_LATEST.format(check_field_name_1, table_name))
        latest_date = db_latest_time.fetchall()[0][0]
        current_time = datetime.strptime(
            datetime.now().strftime(FORMAT_YYYYMMDD), FORMAT_YYYYMMDD)
        latest_update_date = datetime.strptime(latest_date, FORMAT_YYYYMMDD)

        SQL_COUNT_AMOUNT = """SELECT COUNT(1) FROM {} """.format(table_name)

        if TABLE_CATEGORY == 'TRANSACTION':
            SQL_COUNT_AMOUNT += "WHERE to_char(DATE({}),'YYYYMMDD') = '{}' ".format(
                check_field_name_1, latest_update_date.strftime(FORMAT_YYYYMMDD))

        max_sub = int(MAX_DATA_THRESHOLD)
        min_sub = int(MIN_DATA_THRESHOLD)

        db_amount = cursor.execute(SQL_COUNT_AMOUNT)
        amount_sub = int(db_amount.fetchall()[0][0])
        res['amount'] = amount_sub
        res['latest_date'] = latest_date

        # Determine the status based on the current date and the latest update date
        if (current_time == latest_update_date and (amount_sub >= min_sub and amount_sub <= max_sub)):
            res['status'] = "Normal"
        elif (current_time > latest_update_date):
            res['status'] = "Delay"
        else:
            res['status'] = "AbNormal"

        return res
    except Exception as err:
        print('check status error!', type(err), err)
        logging.error('check status error!')


with get_connection() as cursor:
    logging.info('connected to database')

    cursor.execute(sql_cvm_cmpgn_master())
    cvm_campaign_from_sql = cursor.fetchall()
    cvm_master_data = pd.DataFrame(
        cvm_campaign_from_sql, columns=LIST_SLECTED_COLUMN)

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

            res = check_latest_updated(
                check_field_name_1=CHECK_FIELD_NAME_1, table_name=TABLE_NAME)

            create_report_form(schema_name=SCHEMA_NAME, table_name=TABLE_NAME, table_description=TABLE_DESC_DATA,
                               sla_data=SLA_DATA, latest_date=res['latest_date'],
                               amount_sub=res['amount'], status=res['status'])

            # remove_csv_files(TARGET_DIRECTORY)
    except Exception as err:
        print("Error occurs ", err, "error type", type(err))

#!/usr/bin/python3
"""
    This script will exucute every single day at 9,12,15 for each day to sent report
"""
import os
import sys
import re
import logging
from datetime import datetime
import nzpy as nz
import pandas as pd


LOG_PATH = "/data/CVM/table_monitoring/scripts/log/"
CSV_PATH = "/data/CVM/table_monitoring/scripts/report/"
TMP_PATH = ""
FORMAT_YYYYMMDD = "%Y%m%d"
FORMAT_YYYYMMDD_HHMMSS = "%Y/%m/%d %H:%M:%S"
CURRENT_DATE = datetime.strftime(datetime.now(), FORMAT_YYYYMMDD)

# Data containers
ROUND_JOB = sys.argv[1]
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


LIST_SELECTED_COLUMN = ["SCHEMA_NAME", "TABLE_NAME", "TABLE_DESCRIPTION",
                        "TABLE_SHORT_DESCRIPTION", "TABLE_CATEGORY", "SLA_DATA",
                        "SLA_TIME", "MIN_DATA_THRESHOLD", "MAX_DATA_THRESHOLD",
                        "CHECK_FIELD_NAME_1", "DATA_TYPE", "CHECK_FIELD_NAME_2",
                        "CHECK_FIELD_NAME_3", "CHECK_FIELD_NAME_4", "UPDATE_DTTM", "UPDATE_BY"]

SQL_CMPGN_MASTER_TABLE = """SELECT SCHEMA_NAME,TABLE_NAME,TABLE_DESCRIPTION,TABLE_SHORT_DESCRIPTION,
            TABLE_CATEGORY,SLA_DATA, SLA_TIME,MIN_DATA_THRESHOLD,
            MAX_DATA_THRESHOLD,CHECK_FIELD_NAME_1,DATA_TYPE,CHECK_FIELD_NAME_2,
            CHECK_FIELD_NAME_3,CHECK_FIELD_NAME_4,UPDATE_DTTM,UPDATE_BY
            FROM CVM_CMPGN_MASTER_TABLE
        """

INSERT_TABLE = "CVM_CMPGN_MASTER_PROCESS_LOG"

LOG_NAME = 'cvm_cmpgn_master_process_log.log'
LOG_FORMATTER = '%(asctime)s:%(levelname)s:%(message)s'

CONN = None

logging.disable(logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
# file_handler = logging.FileHandler('{}{}'.format(LOG_PATH, LOG_NAME))
file_handler = logging.FileHandler(f'{LOG_PATH}{LOG_NAME}')
file_handler.setFormatter(logging.Formatter(LOG_FORMATTER))
logger.addHandler(file_handler)


def get_connection():
    """ Start connection """
    try:
        user_name = "T968672"
        password = "sa4+VdllVLi$UkV"
        host = "10.50.78.21"
        port = 5480
        database = "NZDATAMART"

        conn = nz.connect(user=user_name, password=password,
                          host=host, port=port, database=database)
        logger.info('connected to database')
        return conn
    except ConnectionError as err:
        logger.error('error to connect the database %s', str(err))


def insert_to_process_log(insert_data, cursor):
    """ Insert to database """
    try:
        # cursor.execute("insert into {} values (?,?,?,?,?,?,?,?)".format(INSERT_TABLE),
        #                (insert_data['CURRENT_DATE'], ROUND_JOB, insert_data['SCHEMA_NAME'], insert_data['TABLE_NAME'],
        #                 insert_data['LATEST_DATE'], insert_data['AMOUNT'], insert_data['STATUS'], insert_data['CREATE_DT']))

        cursor.execute(f"insert into {INSERT_TABLE} values (?,?,?,?,?,?,?,?)",
                       (insert_data['CURRENT_DATE'], ROUND_JOB, insert_data['SCHEMA_NAME'], insert_data['TABLE_NAME'],
                        insert_data['LATEST_DATE'], insert_data['AMOUNT'], insert_data['STATUS'], insert_data['CREATE_DT']))

        # msg = 'inserted {} to CVM_CMPGN_MASTER_PROCESS_LOG success '.format(
        #     insert_data['TABLE_NAME'])
        logger.info('inserted %s to CVM_CMPGN_MASTER_PROCESS_LOG success',
                    insert_data['TABLE_NAME'])
    except ConnectionAbortedError as err:
        logger.error('error occurred during insertion %s', str(err))


def create_csv():
    """ create csv file after insert table """
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
        os.chdir(CSV_PATH)
        create_date = datetime.now().strftime(FORMAT_YYYYMMDD)
        # file_name = '{}_{}_{}.csv'.format(
        #     INSERT_TABLE, create_date, ROUND_JOB)
        file_name = f"{INSERT_TABLE}_{create_date}_{ROUND_JOB}.csv"
        data_to_process_log_table.to_csv(
            file_name, index=False, sep="|", header=None)

        logger.info('CSV created successfully : %s', file_name)
    except EOFError as err:
        logger.error('create csv error %s', str(err))


def send_mail():
    """ Commented out section. Check if it's required, otherwise it can be removed. """
    try:
        is_dir = os.path.isdir(CSV_PATH)
        if not is_dir:
            os.mkdir(CSV_PATH)
            cmd = "(sh /data/CVM/monitor/script/mail_cmpgn_process_log.sh.sh) & "
            os.system(cmd)
        logging.info('email has sent! ')

    except FileNotFoundError as err:
        logger.error("error to sending email %s", str(err))
    sys.exit(0)


def get_file_creation_date(file_name):
    """ Extract the date from the file name using the YYYYMMDD pattern """
    match = re.search(r"\d{8}", file_name)
    if match:
        date_str = match.group()
        return datetime.strptime(date_str, "%Y%m%d").date()
    return None


def remove_csv_files(directory):
    """ Files will removed after 7 days """
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
    except FileNotFoundError as err:
        logger.error('Error while removing file %s', str(err))


def create_report_form(master_table, latest_updated):
    """ To make form for generate csv, insert to database """
    try:
        create_datetime = datetime.now().strftime(FORMAT_YYYYMMDD_HHMMSS)
        DAY_ID_DATA.append(CURRENT_DATE)
        ROUND_DATA.append(ROUND_JOB)
        SCHEMA_NAME_DATA.append(master_table['SCHEMA_NAME'])
        TABLE_NAME_DATA.append(master_table['TABLE_NAME'])
        TABLE_DESC_DATA.append(master_table['TABLE_DESCRIPTION'])
        SLA_DATA_T.append(master_table['SLA_DATA'])
        LATEST_DATE_DATA.append(latest_updated['latest_date'])
        DATA_AMOUNT_DATA.append(latest_updated['amount'])
        STATUS_DATA.append(latest_updated['status'])
        CREATE_DTTM_DATA.append(create_datetime)

        insert_data = {
            'CURRENT_TIME': CURRENT_DATE,
            'SCHEMA_NAME': master_table['SCHEMA_NAME'],
            'TABLE_NAME': master_table['TABLE_NAME'],
            'LATEST_DATE': latest_updated['latest_date'],
            'AMOUNT': latest_updated['amount'],
            'STATUS': latest_updated['status'],
            'CREATE_DT': create_datetime
        }
        return insert_data
    except ValueError as err:
        logger.error('create_report_form %s', str(err))


def check_latest_date_table(master_table, cursor):
    """ """
    try:
        logger.info('checking latest date from %s ',
                    master_table['TABLE_NAME'])

        # sql_check_latest = "SELECT to_char(MAX(DATE({})),'YYYYMMDD') AS LOADDATE FROM {} ".format(
        #     master_table['CHECK_FIELD_NAME_1'], master_table['TABLE_NAME'])

        sql_check_latest = f"SELECT to_char(MAX(DATE({ master_table['CHECK_FIELD_NAME_1']})),'YYYYMMDD') AS LOADDATE FROM {master_table['TABLE_NAME']} "
        logger.info('sql => %s', sql_check_latest)
        # eg. value from db is [(20231230)]
        db_latest_time = cursor.execute(sql_check_latest)

        return db_latest_time.fetchall()[0][0]
    except TypeError as err:
        logger.error('check_latest_date_table %s', str(err))


def get_amount_subs(latest_date, table_name, table_category, check_field, cursor):
    """ To count number of subscription by date """
    try:
        latest_update_date = datetime.strptime(latest_date, FORMAT_YYYYMMDD)
        # sql_count_amount = 'SELECT COUNT(1) FROM {} '.format(table_name)

        sql_count_amount = f"SELECT COUNT(1) FROM {table_name} "
        if table_category == "TRANSACTION":
            # sql_count_amount += "WHERE to_char(DATE({}),'YYYYMMDD') = '{}' ".format(
            #     check_field, latest_update_date.strftime(FORMAT_YYYYMMDD))
            last_date = latest_update_date.strftime(FORMAT_YYYYMMDD)
            sql_count_amount += f"WHERE to_char(DATE({check_field}),'YYYYMMDD') = '{last_date}' "

        logger.info('sql => %s', sql_count_amount)
        db_amount = cursor.execute(sql_count_amount)
        # eg. value from db is [(70000)]
        amount_sub = int(db_amount.fetchall()[0][0])

        return amount_sub
    except TypeError as err:
        logger.error('get_amount_subs %s ', str(err))
    return None


def check_status(current_time, latest_update_date, amount_sub, min_sub, max_sub):
    """ check status (Normal,Delay,AbNormal) """

    status = ''

    if current_time == latest_update_date and (amount_sub >= min_sub and amount_sub <= max_sub):
        status = "Normal"
    elif current_time > latest_update_date:
        status = "Delay"
    else:
        status = "AbNormal"
    return status


def check_latest_updated(master_table, cursor):
    """ to check latest date of data """
    try:
        # SQL to get the latest date from the table
        latest_update = {
            'status': '',
            'amount': 0,
            'latest_date': ''
        }

        latest_date = check_latest_date_table(master_table, cursor)

        amount_subs = get_amount_subs(
            latest_date, master_table['TABLE_NAME'], master_table['TABLE_CATEGORY'],
            master_table['CHECK_FIELD_NAME_1'], cursor)

        min_sub = int(master_table['MIN_DATA_THRESHOLD'])
        max_sub = int(master_table['MAX_DATA_THRESHOLD'])

        current_time = datetime.strptime(CURRENT_DATE, FORMAT_YYYYMMDD)
        status = check_status(current_time, latest_date,
                              amount_subs, min_sub, max_sub)
        latest_update['amount'] = amount_subs
        latest_update['latest_date'] = latest_date
        latest_date['status'] = status

        return latest_update
    except ValueError as err:
        logger.error('Table: %s  check latest updated error %s',
                     master_table['TABLE_NAME'], str(err))
    return None


def create_process_log():

    with CONN.cursor() as cursor:
        logger.info(
            'JOB CVM_CMPGN_MASTER_PROCESS_LOG STARTED ROUND %s ', ROUND_JOB)
        cursor.execute(SQL_CMPGN_MASTER_TABLE)
        cvm_campaign_from_sql = cursor.fetchall()
        cvm_master_data = pd.DataFrame(
            cvm_campaign_from_sql, columns=LIST_SELECTED_COLUMN)

        for master in cvm_master_data.iterrows():

            master_table = {
                'SCHEMA_NAME': master['SCHEMA_NAME'],
                'TABLE_NAME': master['TABLE_NAME'],
                'TABLE_DESCRIPTION': master['TABLE_DESCRIPTION'],
                'TABLE_SHORT_DESCRIPTION': master['TABLE_SHORT_DESCRIPTION'],
                'TABLE_CATEGORY': master['TABLE_CATEGORY'],
                'SLA_DATA': master['SLA_DATA'],
                'SLA_TIME': master['SLA_TIME'],
                'MAX_DATA_THRESHOLD': master['MAX_DATA_THRESHOLD'],
                'MIN_DATA_THRESHOLD': master['MIN_DATA_THRESHOLD'],
                'CHECK_FIELD_NAME_1': master['CHECK_FIELD_NAME_1'],
                'DATA_TYPE': master['DATA_TYPE'],
                'CHECK_FIELD_NAME_2': master['CHECK_FIELD_NAME_2'],
                'CHECK_FIELD_NAME_3': master['CHECK_FIELD_NAME_3'],
                'CHECK_FIELD_NAME_4': master['CHECK_FIELD_NAME_4'],
                'UPDATE_DTTM': master['UPDATE_DTTM'],
                'UPDATE_BY': master['UPDATE_BY']
            }

            try:
                latest_updated = check_latest_updated(
                    master_table, cursor=cursor)
                insert_data = create_report_form(master_table, latest_updated)
                insert_to_process_log(insert_data, cursor=cursor)

            except SystemError as err:
                logger.error('Error occurred while processing table : %s detail: %s',
                             master_table['TABLE_NAME'], str(err))
                continue

    CONN.close()
    logger.info('JOB CVM_CMPGN_MASTER_PROCESS_LOG ROUND %s DONE! ', ROUND_JOB)


if __name__ == '__main__':
    CONN = get_connection()
    create_process_log()
    create_csv()
    send_mail()


#!/usr/bin/python3
"""
    This script will exucute every single day at 9,12,15 for each day to sent report

"""

import os
import sys
import re
import logging
import pandas as pd
from datetime import datetime
import cvm_table_driver
from datetime import timedelta

LOG_PATH = "/data/CVM/table_monitoring/scripts/log/"
LOG_NAME = 'cvm_cmpgn_master_process_log.log'
LOG_FORMATTER = '%(asctime)s:%(levelname)s:%(message)s'
logging.disable(logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

TIMES_OF_DAY = sys.argv[1]
# TIMES_OF_DAY = 9

CSV_PATH = "/data/CVM/table_monitoring/scripts/report/"
FULLDATE_FORMAT = "%Y%m%d"
DATETIME_FORMAT = "%Y/%m/%d %H:%M:%S"
CURRENT_DATE = datetime.strftime(datetime.now(), FULLDATE_FORMAT)
CVM_CMPGN_MASTER_PROCESS_LOG = "CVM_CMPGN_MASTER_PROCESS_LOG"
SQL_CMPGN_MASTER_TABLE = cvm_table_driver.SQL_CMPGN_MASTER_TABLE
LIST_SELECTED_COLUMN = cvm_table_driver.LIST_SELECTED_COLUMN
cursor = cvm_table_driver.get_nzdatamart()


day_id_data_list = []
round_data_list = []
schema_name_data_list = []
table_name_data_list = []
table_desc_data_list = []
sla_data_list = []
latest_date_list = []
data_amount_data_list = []
status_data_list = []
create_dttm_data_list = []
sort_by_type = []

def check_dir(path):
    """ Prepare directory to log file and CSV file """
    if not os.path.exists(path):
        try:
            os.makedirs(path)
        except OSError as err:
            logger.error('ERROR CREATE DIRECTORY %s', str(err))


def create_csv():
    try:
        data_to_process_log_table = pd.DataFrame({
            'DAY_ID': day_id_data_list,
            'ROUND': round_data_list,
            'SCHEMA_NAME': schema_name_data_list,
            'TABLE_NAME': table_name_data_list,
            'TABLE_DESCRIPTION': table_desc_data_list,
            'SLA_DATA': sla_data_list,
            'LATEST_DATE': latest_date_list,
            'DATA_AMOUNT': data_amount_data_list,
            'STATUS': status_data_list,
            'CREATE_DTTM': create_dttm_data_list,
            'SORT': sort_by_type
        })

        data_to_process_log_table = data_to_process_log_table.sort_values(
            by=["SORT","TABLE_NAME"], ascending=True)
        
        data_to_process_log_table.drop(columns="SORT")

        os.chdir(CSV_PATH)
        create_date = datetime.now().strftime(FULLDATE_FORMAT)
        file_name = f"{CVM_CMPGN_MASTER_PROCESS_LOG}_{create_date}_{TIMES_OF_DAY}.csv"
        data_to_process_log_table.to_csv(file_name, index=False, sep="|", header=None)

        logger.info(f" THE FILE HAS CREATED {file_name} SUCCESS")
        
    except EOFError as err:
        logger.error(f" ERROR WHEN CREATE CSV FILE {str(err)}")

def insert_to_master_table_log(data_to_insert):
    try:
        cursor.execute(f"insert into {CVM_CMPGN_MASTER_PROCESS_LOG} values (?,?,?,?,?,?,?,?)",
                       (data_to_insert['CURRENT_DATE'], TIMES_OF_DAY, data_to_insert['SCHEMA_NAME'], data_to_insert['TABLE_NAME'],
                        data_to_insert['LATEST_DATE'], data_to_insert['AMOUNT'], data_to_insert['STATUS'], data_to_insert['CREATE_DT']))

        logger.info(f" INSERTED {data_to_insert['TABLE_NAME']} TO CVM_CMPGN_MASTER_PROCESS_LOG SUCCESS \n")
    except (Exception) as err:
        logger.error(f"  ERROR WHILE INSERT TO TABLE LOG => {str(err)}")

def store_data_to_report(data, latest_update):

    try:

        sort = 0 if data['SLA_DATA'] == 'T+1' else 1 if data['SLA_DATA'] == 'T+2' else 3 if data['SLA_DATA'] == 'Monthly' else 4

        create_datetime = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
        day_id_data_list.append(CURRENT_DATE)
        round_data_list.append(TIMES_OF_DAY)
        schema_name_data_list.append(data['SCHEMA_NAME'])
        table_name_data_list.append(data['TABLE_NAME'])
        table_desc_data_list.append(data['TABLE_DESCRIPTION'])
        sla_data_list.append(data['SLA_DATA'])
        latest_date_list.append(latest_update['latest_date'])
        data_amount_data_list.append(latest_update['amount'])
        status_data_list.append(latest_update['status'])
        create_dttm_data_list.append(create_datetime)
        sort_by_type.append(sort)

        data_to_insert = {
            'CURRENT_DATE': CURRENT_DATE,
            'SCHEMA_NAME': data['SCHEMA_NAME'],
            'TABLE_NAME': data['TABLE_NAME'],
            'LATEST_DATE': latest_update['latest_date'],
            'AMOUNT': latest_update['amount'],
            'STATUS': latest_update['status'],
            'CREATE_DT': create_datetime
        }

        if data['SLA_DATA'] == 'Monthly':
            data_to_insert['LATEST_DATE'] = latest_update['latest_date']+'01'

        insert_to_master_table_log(data_to_insert)

    except (ValueError, SyntaxError, Exception) as err:
        logger.error(f" ERROR IN FUNCTION STORE_DATA_TO_REPORT {str(err)} ")

def check_latest_update(data):

    try:

        sla_type = data['SLA_DATA']
        sql_find_max = ""

        sql_find_max = f"SELECT MAX({data['CHECK_FIELD_NAME_1']}) AS LOADDATE FROM {data['TABLE_NAME']} "

        get_latest_update = cursor.execute(sql_find_max)
        latest_update_date = get_latest_update.fetchall()[0][0]
        amount = count_amount(data, latest_update_date)

        latest_update_date = check_date_format_type(str(latest_update_date))
        logger.info(f" CHECK_LATEST_UPDATE : {sla_type} : {sql_find_max}, RESULT : {latest_update_date}")
        # latest_date = datetime.strftime(latest_update_date,FULLDATE_FORMAT)
        status = check_status(data, amount, latest_update_date)

        resp = {
            'latest_date': latest_update_date if sla_type != 'Monthly' else latest_update_date[:6] ,
            'status': status,
            'amount': amount
        }
        logger.info(f" CHECK_LATEST_UPDATE: STATUS: {resp['status']}, AMT: {resp['amount']}, MAX: {data['MAX_DATA_THRESHOLD']}, MIN: {data['MIN_DATA_THRESHOLD']}")
        return resp
    except Exception as err:
        logger.error(f" ERROR OCCURS IN CHECK_LATEST_UPDATE TABLE : {data['TABLE_NAME']} => {str(err)}")
        raise(f" ERROR OCCURS IN CHECK_LATEST_UPDATE TABLE : {data['TABLE_NAME']} => {str(err)}")
    return None


def check_status(data, amount, latest_update_date):
    logger.info(f" CHECK_STATUS SLA DATA : {data['SLA_DATA']}, LATEST UPDATE : {latest_update_date}")
    try:
        status = 'Delay'
        max_data = data['MAX_DATA_THRESHOLD']
        min_data = data['MIN_DATA_THRESHOLD']

        if data['SLA_DATA'] == 'Monthly':

            current_year = int(CURRENT_DATE[:4])
            current_month = int(CURRENT_DATE[4:6])
            latest_year = int(latest_update_date[:4])
            latest_month = int(latest_update_date[4:6])

            if (current_year == latest_year and current_month-1 == latest_month) or (current_year-1 == latest_year and latest_month == 12):
                status = check_threshold(amount, max_data, min_data)

        elif data['SLA_DATA'] == 'Weekly' or data['SLA_DATA'] == 'Biweekly' :
                status = check_threshold(amount, max_data, min_data)
        elif data['SLA_DATA'] == 'Adhoc':
            status = 'Normal'

        else:
            
            check_field_name = data['CHECK_FIELD_NAME_1']
            sla_time = int(data['SLA_DATA'][2])-1  # get date
            lastest_date = datetime.strptime( latest_update_date ,FULLDATE_FORMAT)
            lastest_date = lastest_date + timedelta(days=sla_time)

            if CURRENT_DATE == check_date_format_type(datetime.strftime(lastest_date,FULLDATE_FORMAT)):
                status = check_threshold(amount, max_data, min_data)

        return status
    except Exception as err:
        logger.error(f" ERROR OCCURS IN A CHECK_STATUS => {str(err)} : TABLE :{data['TABLE_NAME']}")
    return None


def check_threshold(amount, max_data, min_data):
    status = "Above Threshold" if int(amount) > int(max_data) else "Below Threshold" if int(amount) < int(min_data) else "Normal"
    return status


def count_amount(data, latest_update_date):

    try:

        table_category = data['TABLE_CATEGORY']
        table_name = data['TABLE_NAME']
        check_field = data['CHECK_FIELD_NAME_1']
        sla = data['SLA_DATA']
        sql_count_amount = f"SELECT COUNT(1) FROM {table_name} "
        
        if table_category == "TRANSACTION" and sla != "Adhoc":
            # format_date = "%Y%m%d" if len(latest_update_date) == 8 else "%Y-%m-%d %H:%M:%S.%f" if len(latest_update_date)> 8  else "%Y%m"
            # to_datetime = datetime.strptime(latest_update_date,format_date)
            # last_date = datetime.strftime(to_datetime,format_date)
            sql_count_amount += f"WHERE {check_field} = '{latest_update_date}' "

        resp = int(cursor.execute(sql_count_amount).fetchall()[0][0])
        logger.info(f" SQL : {sql_count_amount}, TYPE : {data['TABLE_CATEGORY']}")
        logger.info(f" COUNT_AMOUNT {data['TABLE_NAME']} : {resp}")
        return resp
    except ValueError as err:
        logger.error(f" IN A COUNT_AMOUNT FUNCTION => {(str(err))} : Table {data['TABLE_NAME']}")
    return None


def init_data():
    logger.info(f" STARTED JOB CVM_CMPGN_MASTER_PROCESS_LOG STARTED ROUND {TIMES_OF_DAY} ")

    with cursor:
        cursor.execute(SQL_CMPGN_MASTER_TABLE)
        cvm_campaign_from_sql = cursor.fetchall()
        new_dataset = pd.DataFrame(cvm_campaign_from_sql, columns=LIST_SELECTED_COLUMN)
        
        for idx, data_table in new_dataset.iterrows():

                try:
                    
                    logger.info(f" START PROCESSING TABLE: {data_table['TABLE_NAME']}")
                    data = {
                        'SCHEMA_NAME': data_table['SCHEMA_NAME'],
                        'TABLE_NAME': data_table['TABLE_NAME'],
                        'TABLE_DESCRIPTION': data_table['TABLE_DESCRIPTION'],
                        'TABLE_SHORT_DESCRIPTION': data_table['TABLE_SHORT_DESCRIPTION'],
                        'TABLE_CATEGORY': data_table['TABLE_CATEGORY'],
                        'SLA_DATA': data_table['SLA_DATA'],
                        'SLA_TIME': data_table['SLA_TIME'],
                        'MAX_DATA_THRESHOLD': data_table['MAX_DATA_THRESHOLD'],
                        'MIN_DATA_THRESHOLD': data_table['MIN_DATA_THRESHOLD'],
                        'CHECK_FIELD_NAME_1': data_table['CHECK_FIELD_NAME_1'],
                        'DATA_TYPE': data_table['DATA_TYPE'],
                        'CHECK_FIELD_NAME_2': data_table['CHECK_FIELD_NAME_2'],
                        'CHECK_FIELD_NAME_3': data_table['CHECK_FIELD_NAME_3'],
                        'CHECK_FIELD_NAME_4': data_table['CHECK_FIELD_NAME_4'],
                        'UPDATE_DTTM': data_table['UPDATE_DTTM'],
                        'UPDATE_BY': data_table['UPDATE_BY']
                    }
                    
                    resp = check_latest_update(data)
                    store_data_to_report(data, resp)

                except Exception as err:
                    logger.error(f' OCCUR ERROR IN INIT_DATA FUNCTION => {str(err)}')
                    continue

def check_date_format_type(date):
    try:
      
        formats = ['%Y%m%d', '%Y%m', '%Y-%m-%d %H:%M:%S.%f']

        for fmt in formats:
            try:
                date_obj = datetime.strptime(date, fmt)
                return date_obj.strftime('%Y%m%d')
            except ValueError as err:
                pass
    
    except ValueError as err:
        logger.error(f" ERROR IN CHECK_DATE_FORMAT_TYPE FUNCTION : {str(err)} , PARAM : {date}")

def config_log():
    file_handler = logging.FileHandler(f'{LOG_PATH}{LOG_NAME}')
    file_handler.setFormatter(logging.Formatter(LOG_FORMATTER))
    logger.addHandler(file_handler)


if __name__ == '__main__':
    config_log()
    check_dir(LOG_PATH)
    check_dir(CSV_PATH)
    init_data()
    create_csv()
    cursor.close()
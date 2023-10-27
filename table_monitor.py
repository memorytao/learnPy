

import os
import sys
import re
import pandas as pd
from datetime import datetime
import cvm_log
import cvm_table_driver
from datetime import timedelta


logger = cvm_log.get_cvm_master_table_log()
cursor = cvm_table_driver.get_nzdatamart()

TIMES_OF_DAY = sys.argv[1]

CSV_PATH = "/data/CVM/table_monitoring/scripts/report/"
FULLDATE_FORMAT = "%Y%m%d"
DATETIME_FORMAT = "%Y/%m/%d %H:%M:%S"
CURRENT_DATE = datetime.strftime(datetime.now(), FULLDATE_FORMAT)
CVM_CMPGN_MASTER_PROCESS_LOG = "CVM_CMPGN_MASTER_PROCESS_LOG"

day_id_data = []
round_data = []
schema_name_data = []
table_name_data = []
table_desc_data = []
sla_data_t = []
latest_date_data = []
data_amount_data = []
status_data = []
create_dttm_data = []


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
            'DAY_ID': day_id_data,
            'ROUND': round_data,
            'SCHEMA_NAME': schema_name_data,
            'TABLE_NAME': table_name_data,
            'TABLE_DESCRIPTION': table_desc_data,
            'SLA_DATA': sla_data_t,
            'LATEST_DATE': latest_date_data,
            'DATA_AMOUNT': data_amount_data,
            'STATUS': status_data,
            'CREATE_DTTM': create_dttm_data,
        })

        data_to_process_log_table = data_to_process_log_table.sort_values(
            by="TABLE_NAME", ascending=True)
        
        os.chdir(CSV_PATH)
        create_date = datetime.now().strftime(FULLDATE_FORMAT)
        file_name = f"{CVM_CMPGN_MASTER_PROCESS_LOG}_{create_date}_{TIMES_OF_DAY}.csv"
        data_to_process_log_table.to_csv(file_name, index=False, sep="|", header=None)

        logger.info(f" The file has created {file_name} success")
        
    except (EOFError | Exception) as err:
        logger.error(f" error when create csv file {str(err)}")


def insert_to_master_table_log(data_to_insert):
    try:
        cursor.execute(f"insert into {CVM_CMPGN_MASTER_PROCESS_LOG} values (?,?,?,?,?,?,?,?)",
                       (data_to_insert['CURRENT_DATE'], TIMES_OF_DAY, data_to_insert['SCHEMA_NAME'], data_to_insert['TABLE_NAME'],
                        data_to_insert['LATEST_DATE'], data_to_insert['AMOUNT'], data_to_insert['STATUS'], data_to_insert['CREATE_DT']))

        logger.info(
            f"inserted {data_to_insert['TABLE_NAME']} to cvm_cmpgn_master_process_log succes")
    except (Exception) as err:
        logger.error(f"  error while insert to table log => {str(err)}")


def store_data_to_report(data, latest_update):

    try:
        create_datetime = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
        day_id_data.append(CURRENT_DATE)
        round_data.append(TIMES_OF_DAY)
        schema_name_data.append(data['SCHEMA_NAME'])
        table_name_data.append(data['TABLE_NAME'])
        table_desc_data.append(data['TABLE_DESCRIPTION'])
        sla_data_t.append(data['SLA_DATA'])
        latest_date_data.append(data['latest_date'])
        data_amount_data.append(data['amount'])
        status_data.append(data['status'])
        create_dttm_data.append(create_datetime)

        data_to_insert = {
            'CURRENT_DATE': CURRENT_DATE,
            'SCHEMA_NAME': data['SCHEMA_NAME'],
            'TABLE_NAME': data['TABLE_NAME'],
            'LATEST_DATE': data['latest_date'],
            'AMOUNT': data['amount'],
            'STATUS': data['status'],
            'CREATE_DT': create_datetime
        }

        insert_to_master_table_log(data_to_insert)

    except (ValueError, SyntaxError, Exception) as err:
        logger.error(f" error in function store_data_to_report {str(err)} ")


def check_latest_update(data):

    try:

        sla_type = data['SLA_DATA']
        sql_find_max = ""

        if sla_type == 'Monthly':
            sql_find_max = f"SELECT to_char(MAX(to_date({ data['CHECK_FIELD_NAME_1']})),'YYYYMM') AS LOADDATE FROM {data['TABLE_NAME']} "

        # elif sla_type == 'Weekly':
        #     sql_find_max = f"SELECT to_char(MAX(DATE({ data['CHECK_FIELD_NAME_1']})),'YYYYMMDD') AS LOADDATE FROM {data['TABLE_NAME']} "
        #     latest_update_date = cursor.execute(sql_find_max).fetchall()[0][0]
        # elif sla_type == 'Biweekly':
        #     sql_find_max = f"SELECT to_char(MAX(DATE({ data['CHECK_FIELD_NAME_1']})),'YYYYMMDD') AS LOADDATE FROM {data['TABLE_NAME']} "
        #     latest_update_date = cursor.execute(sql_find_max).fetchall()[0][0]

        # elif sla_type == 'Adhoc':
        #     sql_find_max = f"SELECT to_char(MAX(DATE({ data['CHECK_FIELD_NAME_1']})),'YYYYMMDD') AS LOADDATE FROM {data['TABLE_NAME']} "
        #     latest_update_date = cursor.execute(sql_find_max).fetchall()[0][0]

        else:
            sql_find_max = f"SELECT to_char(MAX(DATE({ data['CHECK_FIELD_NAME_1']})),'YYYYMMDD') AS LOADDATE FROM {data['TABLE_NAME']} "

        latest_update_date = cursor.execute(sql_find_max).fetchall()[0][0]
        amount = count_amount(data, latest_update_date)
        status = check_status(data, amount, latest_update_date)

        resp = {
            'latest_date': latest_update_date,
            'status': status,
            'amount': amount
        }
        logger.info(f" sla_type: {sla_type} , max: {data['']}")
        return resp
    except Exception as err:
        raise (err)
    return None


def check_status(data, amount, latest_update_date):

    try:
        status = 'Delay'

        max_data = data['MAX_DATA_THRESHOLD']
        min_data = data['MIN_DATA_THRESHOLD']

        if data['SLA_DATA'] == 'Monthly':
            latest_update_year_month = datetime.strftime(
                latest_update_date, '%Y-%m').split('-')
            current_year_month = datetime.strftime(
                CURRENT_DATE, '%Y-%m').split('-')

            current_year = int(current_year_month[0])
            current_month = int(current_year_month[1])
            latest_year = int(latest_update_year_month[0])
            latest_month = int(latest_update_year_month[1])

            if (current_year == latest_year and (current_month-1) == latest_month) or (current_year-1 == latest_year and latest_month == 12):
                status = check_threshold(amount, max_data, min_data)

        # elif data['SLA_DATA'] == 'Weekly':
        #     print()

        else:
            sla_time = int(data['SLA_DATA'][2])-1  # get date
            latest_update_date += timedelta(days=sla_time)

            if CURRENT_DATE == latest_update_date:
                status = check_threshold(amount, max_data, min_data)

        return status
    except Exception as err:
        logger.error(f" in a check check_status => {str(err)}")
    return None


def check_threshold(amount, max_data, min_data):
    status = "Above Threshold" if int(amount) > int(
        max_data) else "Below Threshold" if int(amount) < int(min_data) else "Normal"
    return status


def count_amount(data, latest_update_date):
    try:
        table_category = data['TABLE_CATEGORY']
        table_name = data['TABLE_NAME']
        check_field = data['CHECK_FIELD_NAME_1']
        sql_count_amount = f"SELECT COUNT(1) FROM {table_name} "
        if table_category == "TRANSACTION":
            last_date = latest_update_date.strftime(FULLDATE_FORMAT)
            sql_count_amount += f"WHERE to_char(DATE({check_field}),'YYYYMMDD') = '{last_date}' "

        return int(cursor.execute(sql_count_amount).fetchall()[0][0])
    except (ValueError | SyntaxError | Exception) as err:
        logger.error(f" in a count_amount function => {(str(err))}")
    return None


def init_data():
    #  moack as a DB query
    df = pd.read_csv('./CVM_CMPGN_MASTER_TABLE_202310261441.csv', header=None)
    make_header = df.iloc[0].tolist()

    new_dataset = pd.DataFrame(df.values.tolist(), columns=make_header)

    try:
        for idx, data_table in new_dataset.iterrows():

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
        logger.error(f' occur error in init_data function => {str(err)}')


check_dir(cvm_log.LOG_PATH)
check_dir(CSV_PATH)
init_data()
cursor.close()

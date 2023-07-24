import sqlalchemy as db
import pandas as pd
from datetime import datetime
from datetime import timedelta
import sys


# Create a connection to the database
DRIVER_URL = "sqlite:///C:/Users/Tao/AppData/Roaming/DBeaverData/workspace6/.metadata/sample-database-sqlite-1/Chinook.db"
# DRIVER_URL = "sqlite:////Users/memorytao/Library/DBeaverData/workspace6/.metadata/sample-database-sqlite-1/Chinook.db"
engine = db.create_engine(DRIVER_URL)




TABLES = {
    # "CVM_HOUSE_HOLD_IDCARD_ADDR": -1,
    "DIM_ACCT": -1,
    "DIM_CELLSITE_LOCATION": -1,
    "DIM_CUST": -1,
    "DIM_HANDSETS": -1,
    "DIM_ORDR_ACTVTN": -1,
    "DIM_PRICE_PLAN_MV": -1,
    "DIM_PROD": -1,
    "DIM_PROD_5G": -1,
    "DIM_PROD_EXT": -1,
    "DIM_RC_RATE": -1,
    "DIM_TOL_PROMOTION": -1,
    "DIM_TRUEID": -2,
    "FCT_CHRG": -1,
    # "FCT_INVC": -1,
    # "FCT_INVC_PROD": -1,
    "FCT_POST_CHARGE": -1,
    "FCT_PREP_SUBS_ACTIVATE": -1,
    "FCT_PYMT": -1,
    # "FCT_SUBS_SERVICE_NMODEL": -1,
    "FCT_TOPUP": -2,
    "FCT_TRUEID_UNLCK": -2,
    # "FCT_VAS_CONTENT_NMODEL": -2
}

DAY_ID = []
ROUND = []
SCHEMA_NAME = []
TABLE_NAME = []
LATEST_DATE = []
DATA_AMOUNT = []
STATUS = []
CREATE_DTTM = []

currentTime = datetime.now()
currTimeStr = currentTime.strftime("%Y-%m-%d %H:%M:%S")

INSERT_TABLE = "CVM_CMPGN_MASTER_PROCESS_LOG"
COUNT_SQL = " SELECT COUNT(1) FROM {} WHERE LOADDATE = '{}'"
# SQL_CVM_CMPGN_MASTER_TABLE = 'SELECT * FROM CVM_CMPGN_MASTER_TABLE ccmt'
SQL_STATUS = " SELECT * FROM CVM_CMPGN_MASTER_TABLE ccmt WHERE {} >= MIN_DATA_THRESHOLD  AND {} <= MAX_DATA_THRESHOLD ; "

for table in TABLES:
    timeBefore = currentTime + timedelta(days=TABLES[table])
    # print(COUNT_SQL.format(table,timeBefore.strftime("%Y-%m-%d")))

    with engine.connect() as conn:
        # query = db.text(COUNT_SQL.format(table,currTimeStr))
        query = db.text(COUNT_SQL.format(table,'20230702'))
        print(COUNT_SQL.format(table,'20230702'))
        count = conn.execute(query)
        countAllTables = pd.DataFrame(count)

        numbers = countAllTables.values[0][0]
        # print(" number of xxxx ",numbers)
        for data_count in countAllTables:
            print(SQL_STATUS.format(numbers,numbers))


# Query the data
    # with engine.connect() as conn:

    #     query = db.text(SQL)
    #     result = conn.execute(query)
    #     # Create a Pandas DataFrame from the results of the SQL query
    #     df = pd.DataFrame(result)

    #     # Replace None with "Unknown" in all columns
    #     df.replace(to_replace='None', value='', inplace=True)

    #     for i, row in df.iterrows():
    #         schema_name = df['SCHEMA_NAME']
    #         table_name = df['TABLE_NAME']
    #         table_description = df['TABLE_DESCRIPTION']
    #         table_short_description = df['TABLE_SHORT_DESCRIPTION']
    #         sla_data = df['SLA_DATA']
    #         sla_time = df['SLA_TIME']
    #         min_data_threshold = df['MIN_DATA_THRESHOLD']
    #         max_data_threshold = df['MAX_DATA_THRESHOLD']
    #         check_field_name_1 = df['CHECK_FIELD_NAME_1']
    #         check_field_name_2 = df['CHECK_FIELD_NAME_2']
    #         check_field_name_3 = df['CHECK_FIELD_NAME_3']
    #         check_field_name_4 = df['CHECK_FIELD_NAME_4']
    #         update_dttm = df['UPDATE_DTTM']
    #         update_by = df['UPDATE_BY']

    # SCHEMA_NAME.append(row['SCHEMA_NAME'])
    # TABLE_NAME.append(row['TABLE_NAME'])
    # DAY_ID.append(df['CHECK_FIELD_NAME_1'])
    # ROUND.append(ROUND_AT_DAY)
    # LATEST_DATE.append(ROUND_AT_DAY)
    # DATA_AMOUNT.append('')
    # STATUS.append('')
    # CREATE_DTTM.append(currTimeStr)


# INSERT ZONE

# data_to_process_log_table = pd.DataFrame(
#     {
#         # 'DAY_ID': DAY_ID,
#         # 'ROUND': ROUND,
#         'SCHEMA_NAME': SCHEMA_NAME,
#         'TABLE_NAME': TABLE_NAME,
#         # 'LATEST_DATE': LATEST_DATE,
#         # 'DATA_AMOUNT': DATA_AMOUNT,
#         # 'STATUS': STATUS,
#         # 'CREATE_DTTM': CREATE_DTTM,
#     }
# )

# data_to_process_log_table.to_sql(name=INSERT_TABLE, con=engine, index=False, if_exists='append')

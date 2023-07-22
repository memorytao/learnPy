import sqlalchemy as db
import pandas as pd
from datetime import datetime
from datetime import timedelta

TABLES = {
    "CVM_HOUSE_HOLD_IDCARD_ADDR": -1,
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
    "Employee": -1,
    "FCT_CHRG": -1,
    "FCT_INVC": -1,
    "FCT_INVC_PROD": -1,
    "FCT_POST_CHARGE": -1,
    "FCT_PREP_SUBS_ACTIVATE": -1,
    "FCT_PYMT": -1,
    "FCT_SUBS_SERVICE_NMODEL": -1,
    "FCT_TOPUP": -2,
    "FCT_TRUEID_UNLCK": -2,
    "FCT_VAS_CONTENT_NMODEL": -2
}

CMPGN_MASTER_COLUMNS = ['Schema_Name', 'Table_Name', 'Table_Description', 'Table_Short_Description ', 'SLA_Data', 'SLA_Time',
                        'Min_Data_Threshold', 'Max_Data_Threshold', 'Check_Field_Name_1', 'Check_Field_Name_2', 'Update_DTTM', 'Update_By']

# Create a connection to the database
# DRIVER_URL = "sqlite:///C:/Users/Tao/AppData/Roaming/DBeaverData/workspace6/.metadata/sample-database-sqlite-1/Chinook.db"
DRIVER_URL = "sqlite:///C:/Users/Tao/AppData/Roaming/DBeaverData/workspace6/.metadata/sample-database-sqlite-1/Chinook.db"

engine = db.create_engine(DRIVER_URL)
currentTime = datetime.now()

SQL = 'SELECT "NZDATAMART" as SCHEMA ,* FROM {} LIMIT 10'

# Query the data
with engine.connect() as conn:
    for table in TABLES:
        timeBefore = currentTime + timedelta(days=TABLES[table])

        query = db.text(SQL.format(table))
        result = conn.execute(query)
        # Create a Pandas DataFrame from the results of the SQL query
        df = pd.DataFrame(result)
        # Replace None with "Unknown" in all columns
        df.replace(to_replace='None', value='Unknown', inplace=True)
        # Print the first names of all the customers
        df.to_excel('./{}.xlsx'.format(table), index=False)

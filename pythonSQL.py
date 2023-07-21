import sqlalchemy as db
import pandas as pd
from datetime import datetime
from datetime import timedelta

tables = {
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
    "DIM_TRUEID": -1,
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

# Create a connection to the database
driver_url = "sqlite:///C:/Users/Tao/AppData/Roaming/DBeaverData/workspace6/.metadata/sample-database-sqlite-1/Chinook.db"
engine = db.create_engine(driver_url)
sql = 'SELECT * FROM {}'

df = []
# Query the data
with engine.connect() as conn:
    for table in tables:
        query = db.text(sql.format(table))
        result = conn.execute(query)

        # Create a Pandas DataFrame from the results of the SQL query
        # df = pd.DataFrame(result)
        df.append(pd.DataFrame(result))
        # Replace None with "Unknown" in all columns
        # df.replace(to_replace=None, value='Unknown', inplace=True)

        # Print the first names of all the customers
        # print(df.to_string(index=False))


# print(df)
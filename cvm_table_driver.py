
import nzpy as nz

SQL_CMPGN_MASTER_TABLE = """SELECT SCHEMA_NAME,TABLE_NAME,TABLE_DESCRIPTION,TABLE_SHORT_DESCRIPTION,
            TABLE_CATEGORY,SLA_DATA, SLA_TIME,MIN_DATA_THRESHOLD,
            MAX_DATA_THRESHOLD,CHECK_FIELD_NAME_1,DATA_TYPE,CHECK_FIELD_NAME_2,
            CHECK_FIELD_NAME_3,CHECK_FIELD_NAME_4,UPDATE_DTTM,UPDATE_BY
            FROM CVM_CMPGN_MASTER_TABLE 
        """

SQL_CMPGN_MASTER_TABLE_TEST = """SELECT SCHEMA_NAME,TABLE_NAME,TABLE_DESCRIPTION,TABLE_SHORT_DESCRIPTION,
            TABLE_CATEGORY,SLA_DATA, SLA_TIME,MIN_DATA_THRESHOLD,
            MAX_DATA_THRESHOLD,CHECK_FIELD_NAME_1,DATA_TYPE,CHECK_FIELD_NAME_2,
            CHECK_FIELD_NAME_3,CHECK_FIELD_NAME_4,UPDATE_DTTM,UPDATE_BY
            FROM CVM_CMPGN_MASTER_TABLE WHERE TABLE_NAME = 'DIM_TMN'
        """

LIST_SELECTED_COLUMN = ["SCHEMA_NAME", "TABLE_NAME", "TABLE_DESCRIPTION",
                        "TABLE_SHORT_DESCRIPTION", "TABLE_CATEGORY", "SLA_DATA",
                        "SLA_TIME", "MIN_DATA_THRESHOLD", "MAX_DATA_THRESHOLD",
                        "CHECK_FIELD_NAME_1", "DATA_TYPE", "CHECK_FIELD_NAME_2",
                        "CHECK_FIELD_NAME_3", "CHECK_FIELD_NAME_4", "UPDATE_DTTM", "UPDATE_BY"]

def get_nzdatamart():
    """ Start connection """
    try:
        user_name = "T968672"
        password = "sa4+VdllVLi$UkV"
        host = "10.50.78.21"
        port = 5480
        database = "NZDATAMART"

        conn = nz.connect(user=user_name, password=password,
                          host=host, port=port, database=database)

        return conn.cursor()
    except ConnectionError as err:
        raise('ERROR TO CONNECT THE DATABASE %s', str(err))
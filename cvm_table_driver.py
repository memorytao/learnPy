
import nzpy as nz
import cvm_log as logger

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

        logger.info('CONNECTED TO DATABASE')
        return conn.cursor()
    except ConnectionError as err:
        logger.error('ERROR TO CONNECT THE DATABASE %s', str(err))

import os
import logging


logger = logging.getLogger()
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler('CVM_CMPGN_MASTER_PROCESS_LOG.log')
file_handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(message)s'))
logger.addHandler(file_handler)
# logger.info('This is a log info message.')
# logger.error('This is a log error message.')

import logging

LOG_PATH = "/data/CVM/table_monitoring/scripts/log/"
LOG_NAME = 'cvm_cmpgn_master_process_log.log'
LOG_FORMATTER = '%(asctime)s:%(levelname)s:%(message)s'


def get_cvm_master_table_log():
    logging.disable(logging.DEBUG)
    logger = logging.getLogger()
    file_handler = logging.FileHandler(f'{LOG_PATH}{LOG_NAME}')
    file_handler.setFormatter(logging.Formatter(LOG_FORMATTER))
    logger.addHandler(file_handler)
    logger.setLevel(logging.INFO)
    return logger

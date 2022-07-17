import petl as etl
# import pymysql
import dbconn
from datetime import date, time, timedelta, datetime
import os
import logging

logging.basicConfig(level=logging.DEBUG) 
logging = logging.getLogger('holiday_load')

def event_data_clnup(filename):
	"""
	Event data load
	"""
	logging.info('Processing file: %s', filename)
	date = etl.dateparser('%Y-%m-%d', strict=True)
	event_table = etl.fromxlsx(filename)\
        .rename({'Mapping': 'Name', 'Holiday':'Description'})\
        .addfield('valid_from_date', '2013-01-01 00:00:00')\
        .addfield('valid_to_date', '2150-01-01 00:00:00')\
        .convert('Event1', lambda row: 0 if not row else row)\
        .convert('Event2', lambda row: 0 if not row else row)
	return event_table

def dir_file_walk(path):
    return (os.path.join(root, name)
         for root, dirs, files in os.walk(path)
         for name in files)

# event_table = event_data_clnup('/Users/biswadippaul/Downloads/Upcl_Holiday_SAS_input.xlsx')
# print event_table

def event_load(filename):
    # connection = pymysql.connect(user='root', password='power@2012', database='power')
    # # tell MySQL to use standard quote character
    # connection.cursor().execute('SET SQL_MODE=ANSI_QUOTES')
    connection = dbconn.connect('/Users/biswadippaul/Projects/batch/config/sqldb_connection_config.txt')
    cursor = connection.cursor().execute('SET SQL_MODE=ANSI_QUOTES')
    logging.info('Processing file: %s', filename)
    try:
        # table3 = data_prep_w_clnup(filename)
        event_table = event_data_clnup(filename)
        logging.debug('Data**** {}'.format(event_table))
        if event_table:
            etl.appenddb(event_table, connection, 'holiday_event_master')
    except Exception, err:
        logging.error('Error**** %s for %s', str(err), filename)
        logging.info('Data**** event_table {}'.format(event_table))
        cursor.close()
        connection.rollback()
        raise

event_load('/Users/biswadippaul/Downloads/Upcl_Holiday_SAS_input.xlsx')
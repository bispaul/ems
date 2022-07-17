import petl as etl
# import sql_load_lib
# import pymysql
import dbconn
from datetime import date, time, timedelta, datetime
import calendar
import os
import logging


logging.basicConfig(level=logging.INFO) 
logging = logging.getLogger('powercut_load')

def data_prep_w_clnup(filename):
    logging.info('Processing file: %s', filename)
    columns = ['date', 'description', 'block_no', 'powercut']


    # if filename.endswith("16.CSV") or filename.endswith("CLEANED.CSV") or filename.endswith("R.CSV") or filename.endswith("20160331.csv"):
    #   date_parser = etl.dateparser('%d-%m-%y', strict=True)
    # elif filename.split('/')[-1].startswith("POWERCUT"):
    #   date_parser = etl.dateparser('%d/%m/%Y', strict=True)
    # else:
    #   date_parser = etl.dateparser("%d.%m.%Y", strict=True)
    date_parser = etl.dateparser('%d/%m/%Y', strict=True)

    table1 = etl\
        .fromcsv(filename)\
        .cutout('StationID')\
        .setheader(columns)\
        .convert('date', date_parser)\
        .convert('block_no', int)\
        .convert(('powercut'), float)\
        .addfield('discom', 'UPCL')\
        .addfield('state', 'UTTARAKHAND')

    # constraints = [
    #     dict(name='date_date', field='date', test=etl.dateparser('%d/%m/%Y', strict=True)),
    #     # dict(name='date_str', field='date', test=str),
    #     dict(name='description_str', field='description', test=str),
    #     dict(name='block_no_int', field='block_no', test=int),
    #     dict(name='powercut_float', field='powercut', test=float),
    #     dict(name='discom_str', field='discom', test=str),
    #     dict(name='state_str', field='state', test=str)]
    # problems = etl.validate(table1, constraints=constraints)  
    # print problems.lookall()
    # parsers={'date': etl.dateparser('%d/%m/%Y', strict=True)}
    # print etl.look(etl.parsecounts(table1, 'date', parsers))
    # table2 = table1.convert('date', date)
    # print table1
    logging.info("Rowcount: %s", etl.nrows(table1))
    table2 = etl.select(table1, "{block_no} is not None and {powercut} is not None")
    logging.info("Rowcount after filter for None: %s", etl.nrows(table2))
    return table2

def powercut_data_clnup(filename):
    logging.info('Processing file: %s', filename)
    # columns = ['date', 'description', 'block_no', 'powercut']
    # columns = ['Date', 'StationID', 'Description', 'BlockNo', 'PowerCut(MW)', 'Discom']
    ren_col = ['date', 'station', 'description', 'block_no', 'powercut']


    # if filename.endswith("16.CSV") or filename.endswith("CLEANED.CSV") or filename.endswith("R.CSV") or filename.endswith("20160331.csv"):
    #   date_parser = etl.dateparser('%d-%m-%y', strict=True)
    # elif filename.split('/')[-1].startswith("POWERCUT"):
    #   date_parser = etl.dateparser('%d/%m/%Y', strict=True)
    # else:
    #   date_parser = etl.dateparser("%d.%m.%Y", strict=True)
    date_parser = etl.dateparser('%d/%m/%Y', strict=True)

    table1 = etl\
        .fromcsv(filename)\
        .setheader(ren_col)\
        .convert('date', date_parser)\
        .convert('block_no', int)\
        .convert(('powercut'), float)\
        .addfield('discom', 'UPCL')\
        .addfield('state', 'UTTARAKHAND')

    # constraints = [
    #     dict(name='date_date', field='date', test=etl.dateparser('%d/%m/%Y', strict=True)),
    #     # dict(name='date_str', field='date', test=str),
    #     dict(name='description_str', field='description', test=str),
    #     dict(name='block_no_int', field='block_no', test=int),
    #     dict(name='powercut_float', field='powercut', test=float),
    #     dict(name='discom_str', field='discom', test=str),
    #     dict(name='state_str', field='state', test=str)]
    # problems = etl.validate(table1, constraints=constraints)  
    # print problems.lookall()
    # parsers={'date': etl.dateparser('%d/%m/%Y', strict=True)}
    # print etl.look(etl.parsecounts(table1, 'date', parsers))
    # table2 = table1.convert('date', date)
    # print table1
    logging.info("Rowcount: %s", etl.nrows(table1))
    table2 = etl.select(table1, "{block_no} is not None and {powercut} is not None")
    logging.info("Rowcount after filter for None: %s", etl.nrows(table2))
    return table2    

def dir_file_walk(path):
    return (os.path.join(root, name)
         for root, dirs, files in os.walk(path)
         for name in files
         if name.endswith(".csv") and name.startswith("UPCL_POWERCUT"))

def test():
    # connection = pymysql.connect(user='root', password='power@2012', database='power')
    # # tell MySQL to use standard quote character
    # connection.cursor().execute('SET SQL_MODE=ANSI_QUOTES')
    connection = dbconn.connect('/Users/biswadippaul/Projects/batch/config/sqldb_connection_config.txt')
    cursor = connection.cursor().execute('SET SQL_MODE=ANSI_QUOTES')
    for filename in dir_file_walk('/Users/biswadippaul/Documents/UPCL/raw data/Production Data Used/HOURLY DATA/Additional hourly data/'):
        try:
            # table3 = data_prep_w_clnup(filename)
            powercut_data = powercut_data_clnup(filename)
            # logging.info('Data**** {}'.format(table3))
            if etl.nrows(powercut_data):
                # etl.tocsv(powercut_data, '/Users/biswadippaul/Documents/UPCL/debug.csv')
                # etl.appenddb(table3, connection, 'upcl_powercut_staging')
                etl.appenddb(powercut_data, connection, 'powercut_staging')
        except Exception, err:
            logging.error('Error**** %s', str(err))
            logging.info('Data**** {}'.format(powercut_data))
            cursor.close()
            connection.rollback()
            raise

test()
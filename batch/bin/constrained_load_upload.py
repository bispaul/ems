"""Loads Contrained Load and powercut file into staging Table"""
import pandas as pd
import sql_load_lib
import glob
import shutil
import sys
import argparse
import os
import logging
import csv
import urllib2

logging.basicConfig(level=logging.INFO) 
logging = logging.getLogger('contrained_load_upload')

def contrained_load(loadfile, state):
    """
    Transform the daily demand load file
    """
    df = pd.read_csv(loadfile,
                     sep=',', parse_dates=[0]
                     #, index_col=['Date', 'Block_No']
                     )
    #print df
    df.set_index(['Date', 'Block_No', 'Frequency'],
                 inplace=True, drop=True, append=False)
    newdf = df.stack(dropna=True).reset_index()
    drawl = newdf[newdf['level_3'].isin(['NBPDCL', 'SBPDCL'])]
    drawl.rename(columns={'level_3': 'Discom', 0: 'Constrained_Load'},
                 inplace=True)
    #print drawl
    schedule = newdf[newdf['level_3'].isin(['NBPDCL_Schedule',
                                            'SBPDCL_Schedule'])]
    schedule.rename(columns={'level_3': 'Discom', 0: 'Schedule'},
                    inplace=True)
    schedule.Discom[schedule.Discom == 'NBPDCL_Schedule'] = 'NBPDCL'
    schedule.Discom[schedule.Discom == 'SBPDCL_Schedule'] = 'SBPDCL'
    #print schedule
    merge = pd.merge(drawl, schedule, how='inner')
    # Generate BPDCL data by aggreagating the 2 discoms
    groupbysum = merge.groupby(['Date', 'Block_No'], as_index=False).agg({'Frequency': 'mean', 'Constrained_Load': 'sum', 'Schedule': 'sum'})
    # bpdcl_df = groupbysum.reset_index()
    groupbysum.insert(3, 'Discom', 'BPDCL')
    # Resetting the column names for appending
    # bpdcl_df.columns = ['Date', 'Block_No', 'Discom', 'Frequency', 'Constrained_Load', 'Schedule']
    complete_df = merge.append(groupbysum)
    complete_df['state'] = state
    complete_df['Date'] = complete_df['Date'].map(lambda x: x.strftime('%Y-%m-%d'))
    return [tuple(x) for x in complete_df.to_records(index=False)]


def powercut_load(loadfile, state):
    logging.info('Inside powercut_load function: {} {}'.format(loadfile, state))
    df = pd.read_csv(
            loadfile,
            sep=',', 
            parse_dates=[0]
        )
    df['state'] = state
    df['Date'] = df['Date'].map(lambda x: x.strftime('%Y-%m-%d'))    
    data = [tuple(x) for x in df.to_records(index=False)]
    logging.debug("{} File Data: {}".format(loadfile, data))
    return data

# print powercut_load('/Users/biswadippaul/Downloads/testbihar/Powercut_data_12302020.csv', 'BIHAR')

def main(args):
    """Revursively loads all the file matching a pattern
       into database and moves the file to archive"""
    logging.info("Inside Main  :{}".format(args.filenm))
    # print glob.glob(args.filenm)
    powercut_sp_execute_flg = False;
    for filex in glob.iglob(args.filenm):
        logging.info("Processing File  :{}".format(filex))
        filenm = os.path.abspath(filex).replace("\\", "/")
        basepath, filename = os.path.split(filenm)
        logging.debug("Processing File  :{} Path: {}".format(filename, basepath))
        try:
            if filename[:4] == 'Load':
                logging.debug("In side Load Processing File  :{}".format(filename))
                data = contrained_load(filenm, args.state.upper())
                # print data
                sql_load_lib.sql_table_insert_exec(args.dsn,
                                                   'drawl_staging',
                                                   data)
            else:
                logging.debug("In side Powercut Processing File  :{}".format(filename))
                powercut_sp_execute_flg = True;
                # sql_load_lib.sql_table_load_exec(args.dsn,
                #                                  'powercut_staging',
                #                                  (filenm, args.state.upper()))
                data = powercut_load(filenm, args.state.upper())
                sql_load_lib.sql_table_insert_exec(args.dsn,
                                                   'powercut_staging',
                                                   data)                
            newpath = os.path.join(args.dir, filename)
            if os.path.exists(newpath):
                os.remove(newpath)
            shutil.move(filenm, args.dir)
        except Exception, err:
            logging.error("processing file failed: {} {}".format(filenm, err))
            sys.exit(1)
    if powercut_sp_execute_flg:
        #Execute stored procedure
        sql_load_lib.sql_powercut_station_discom_upd(args.dsn)
    sys.exit(0)


if __name__ == '__main__':
    ARG = argparse.ArgumentParser(description="Inserts constrained load\
                                  into staging table")
    ARG.add_argument('-a', '--adir', dest='dir',
                     help='Directory to archive data to after processing',
                     required=True)
    ARG.add_argument('-f', '--file', dest='filenm',
                     help='file pattern along with path.',
                     required=True)
    ARG.add_argument('-m', '--dbdsnconfig', dest='dsn',
                     help='Full path and the file name of the db config',
                     required=True)
    ARG.add_argument('-s', '--state', dest='state',
                     help='State name for which data is being loaded',
                     required=True)
    # ARG.add_argument('-t', '--tabnm', dest='tabnm',
    #                  help='Name of the table to be loaded',
    #                  required=True)
    main(ARG.parse_args())

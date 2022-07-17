"""Loads weather actual and forecast file into staging Table"""
import sql_load_lib
import glob
import shutil
# import sys
import argparse
import os
import datetime as dt
import re
import logging
import csv
import time


logging.basicConfig(level=logging.INFO)
logging = logging.getLogger('weather_data_upload')


def weather_actual_load(filenm, filename, state, location=None):
    """
    Weather actual file data to list of tuples for bulk load
    """
    data = []
    with open(filenm, 'r') as csvfile:
        reader = csv.reader(csvfile)
        headers = next(reader)
        logging.debug("{} File Data header: {}".format(filename, headers))
        location_flag = True if ("Location" in headers) else False
        time_idx = headers.index('Time')
        for line in reader:
            row = list(line)
            mod_row = []
            for idx, col in enumerate(row):
                col = [None if col.strip(' ') == '' else col]
                if location_flag and idx == len(headers) - 2:
                    col = [col[0], ''.join(state).upper()]
                elif location_flag and idx == len(headers) - 1:
                    col = [col[0].split('_')[-1].upper()]
                elif not location_flag and state and location\
                        and idx == len(headers) - 1:
                    col = [col[0], ''.join(state).upper(),
                           ''.join(location).upper()]
                elif not location_flag and not location and \
                        idx == len(headers) - 1:
                    col = [col[0], ''.join(state).upper(), None]
                elif idx == time_idx:
                    col = [time.strftime('%H:%M',
                                         time.strptime(col[0], "%I:%M %p"))]
                mod_row.extend(col)
            data.append(tuple(mod_row))
    logging.debug("{} File Data: {}".format(filename, data))
    return data


def weather_forecast_load(filenm, filename, state, location=None):
    """
    Weather forecast file data to list of tuples for bulk load
    """
    data = []
    loadtimestamp = re.findall(r"\d{2}-\d{2}-\d{8}", filename)[0]
    loadtimestamp = dt.datetime\
        .strptime(loadtimestamp, '%d-%m-%Y%H%M')\
        .strftime('%Y-%m-%d %H:%M:%S')
    with open(filenm, 'r') as csvfile:
        reader = csv.reader(csvfile)
        headers = next(reader)
        logging.debug("{} File Data header: {}".format(filename, headers))
        location_flag = True if ("Location" in headers) else False
        time_idx = headers.index('Hour')
        for line in reader:
            row = list(line)
            mod_row = []
            for idx, col in enumerate(row):
                col = [None if col.strip(' ') == '' else col]
                if location_flag and idx == len(headers) - 2:
                    col = [col[0], ''.join(state).upper()]
                elif location_flag and idx == len(headers) - 1:
                    col = [col[0].split('_')[-1].upper(), loadtimestamp]
                elif not location_flag and state and location \
                        and idx == len(headers) - 1:
                    col = [col[0], ''.join(state).upper(),
                           ''.join(location).upper(), loadtimestamp]
                elif not location_flag and not location and \
                        idx == len(headers) - 1:
                    col = [col[0], ''.join(state).upper(), None, loadtimestamp]
                elif idx == time_idx:
                    col = [time.strftime('%H:%M',
                                         time.strptime(col[0], "%I:%M %p"))]
                mod_row.extend(col)
            data.append(tuple(mod_row))
    logging.debug("{} File Data: {}".format(filename, data))
    return data


def main(args):
    """Revursively loads all the file matching a pattern
       into database and moves the file to archive"""
    # print "INside Main  :" + args.filenm
    # print glob.glob(args.filenm)
    sp_run_flg = False
    logging.debug("Total file: {}".format(args.filenm))
    for filenm in args.filenm:
        logging.debug("Processing file pattern: {}".format(filenm))
        for filex in glob.iglob(filenm):
            sp_run_flg = True
            logging.debug("Processing file: {}".format(filex))
            filenm = os.path.abspath(filex).replace("\\", "/")
            logging.info("Processing file: {}".format(filenm))
            basepath, filename = os.path.split(filenm)
            # data = []
            try:
                if filename[:7] == 'Actuals':
                    data = weather_actual_load(filenm, filename,
                                               args.state, args.location)
                    sql_load_lib.sql_table_insert_exec(
                        args.dsn,
                        'actual_weather_staging',
                        data)
                    sql_load_lib.sql_wtr_actual_blk_ins_upd(args.dsn)
                elif filename[:5] == 'fh10d':
                    data = weather_forecast_load(filenm, filename,
                                                 args.state, args.location)
                    sql_load_lib.sql_table_insert_exec(
                        args.dsn,
                        'forecast_weather_staging',
                        data)
                    logging.info("Executing  sql_wtr_forecast_hrblk_ins_upd")
                    # sql_load_lib.sql_wtr_forecast_blk_ins_upd(args.dsn)
                    sql_load_lib.sql_wtr_forecast_hrblk_ins_upd(args.dsn)
                newpath = os.path.join(args.dir, filename)
                if os.path.exists(newpath):
                    os.remove(newpath)
                shutil.move(filenm, args.dir)
                # Manually updating the unified_weather data for UPCL
                # should be derived from data
            except Exception, err:
                logging.error("{} error: {}".format(filenm, err))
                raise
                # sys.exit(1)
    if sp_run_flg:
        discom = 'UPCL'
        sql_load_lib.sql_wtr_unified_ins_upd(args.dsn, discom)
    # sys.exit(0)


if __name__ == '__main__':
    ARG = argparse.ArgumentParser(description="Inserts constrained load\
                                  into staging table")
    ARG.add_argument('-a', '--adir', dest='dir',
                     help='Directory to archive data to after processing',
                     required=True)
    ARG.add_argument('-f', '--file', nargs='*', dest='filenm',
                     help='file pattern along with path.',
                     required=True)
    ARG.add_argument('-m', '--dbdsnconfig', dest='dsn',
                     help='Full path and the file name of the db config',
                     required=True)
    ARG.add_argument('-s', '--state', dest='state',
                     help='State name for ehich data is being loaded',
                     required=True)
    ARG.add_argument('-l', '--location', dest='location',
                     help='Location name for which data is being loaded',
                     required=False)
    # ARG.add_argument('-t', '--tabnm', dest='tabnm',
    #                  help='Name of the table to be loaded',
    #                  required=True)
    logging.debug("Parsing {}".format(ARG.parse_args()))

    main(ARG.parse_args())

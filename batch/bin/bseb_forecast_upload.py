"""
Extracts and uploads Demand/Wind/Solar forecast, Bilaterals and Internal
Generation
"""
import xlrd
import sql_load_lib
import argparse
import datetime as dt
import glob
import os
import sys
import shutil


def dir_diff_in_files(pdir, odir):
    """
    Return a python list with the difference between two directory files
    """
    ponlyfiles = set([os.path.split(f)[1] for f in glob.glob(pdir)])
    oonlyfiles = set([os.path.split(f)[1] for f in glob.glob(odir)])
    # print oonlyfiles
    return list(ponlyfiles - oonlyfiles)


def excel_to_tuple(filenm, sheet=None):
    """
    Reads excel row and retuns a list of tuples [(_, _), (_, _)]
    """
    print filenm
    workbook = xlrd.open_workbook(filenm)
    if sheet:
        sheet = workbook.sheet_by_name(sheet)
    else:
        sheet = workbook.sheet_by_index(0)
    data = []
    #read only 96 rows
    for i in xrange(4, 100):
        temp = sheet.row_values(i, start_colx=0, end_colx=7)
        temp[1] = dt.datetime(*xlrd.xldate_as_tuple(temp[1],
                              workbook.datemode))
        temp[1] = temp[1].date().isoformat()
        data.append(tuple(temp[:3] + ['NBPDCL', temp[5]]))
        data.append(tuple(temp[:3] + ['SBPDCL', temp[6]]))
    return data


def main(args):
    """
    Call to the main
    """
    filenm_dir = os.path.split(args.filenm)[0]
    filenm_pattern = os.path.split(args.filenm)[1]
    #print filenm_dir, filenm_pattern
    archive = os.path.join(args.archive, filenm_pattern)
    # print archive
    for filenm in dir_diff_in_files(args.filenm, archive):
        full_path_filenm = os.path.join(filenm_dir, filenm)
        afull_path_filenm = os.path.join(args.archive, filenm)
        # print full_path_filenm, afull_path_filenm
        data_list_out = excel_to_tuple(full_path_filenm)
        # print data_list_out
        sql_load_lib.sql_table_insert_exec(args.dsn,
                                           args.tabname,
                                           data_list_out)
        shutil.copyfile(full_path_filenm, afull_path_filenm)


if __name__ == '__main__':
    ARG = argparse.ArgumentParser(description="Fetches Forecast\
                                   and Schedule Prep Data")
    ARG.add_argument('-f', '--file', dest='filenm',
                     help='File Name path from which data is to be read',
                     required=True)
    ARG.add_argument('-a', '--archive', dest='archive',
                     help='Path to archive file after loading',
                     required=True)
    ARG.add_argument('-m', '--dbdsnconfig', dest='dsn',
                     help='Full path and the file name of the db config',
                     required=True)
    ARG.add_argument('-t', '--tabnm', dest='tabname',
                     help='Name of the table to be loaded',
                     required=True)
    sys.exit(main(ARG.parse_args()))


# print dir_diff_in_files('F:/bihar/schedule/report/Annexure A*.xlsx', 'F:/nrldc/data/bihar/Annexure A*.xlsx')
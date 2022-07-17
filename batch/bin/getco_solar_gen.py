# import modules
import pandas as pd
import petl
import os
import dbconn
from datetime import datetime


file = "/Users/biswadippaul/Projects/GETCO/117.247.83.101/3. Actual RE Gen/Solar actual 2016/4. APR'16.xlsx"
dsn = '/Users/biswadippaul/Projects/batch/config/sqldb_connection_config.txt'


def dir_file_walk(path):
    return (os.path.join(root, name)
            for root, dirs, files in os.walk(path)
            for name in files
            if name.endswith((".xlsx")))


def load_getco_solar_data(file, tab):
    xls_file = petl.io.xlsx.fromxlsx(file, tab)
    for i in xrange(5):
        data_list = list(xls_file.head(5))
        if data_list[i][1]:
            if 'TIME' in data_list[i][1]:
                hdr_list1 = data_list[i]
                hdr_list2 = data_list[i + 1]
                hdrindx = i + 1
                break

    hdr = [''.join([i[0], str(i[1]) if i[1] is not None else ''])
           for i in zip(hdr_list1, hdr_list2)
           if i != (None, None)]

    hdr.insert(0, 'DATE')
    # print hdr

    table1 = xls_file.skip(hdrindx)
    curhdr = table1.header()
    # print curhdr
    # print len(curhdr), len(hdr), tuple(range(len(hdr), len(curhdr)))
    if any("TOTA" in s for s in hdr):
        dis_column_indx = tuple(range(len(hdr) - 1, len(curhdr)))
        table2 = petl.cutout(table1, *dis_column_indx)
        table3 = petl.setheader(table2, hdr[:-1])
        table4 = petl.convert(table3, tuple(hdr[2:-1]), float)
    else:
        dis_column_indx = tuple(range(len(hdr), len(curhdr)))
        table2 = petl.cutout(table1, *dis_column_indx)
        table3 = petl.setheader(table2, hdr)
        table4 = petl.convert(table3, tuple(hdr[2:]), float)
    table5 = petl.convert(table4, hdr[1], int)
    table6 = petl.select(table5, 'TIMEBLOCK',
                         lambda v: v is not None)
    table7 = petl.filldown(table6, 'DATE')
    table8 = petl.melt(table7,
                       key=['DATE', 'TIMEBLOCK'])
    table9 = petl.rename(table8,
                         {'TIMEBLOCK': 'BLOCK_NO',
                          'variable': 'GENERATOR_NAME',
                          'value': 'GENERATION'})
    table10 = petl.addfield(table9, 'DISCOM', 'GUVNL')
    table11 = petl.addfield(table10, 'STATE', 'GUJARAT')
    table12 = petl.addfield(table11, 'POOL_TYPE', 'SOLAR', 2)
    table13 = petl.addfield(table12, 'POOL_NAME', 'RE', 2)
    table14 = petl.select(table13, 'GENERATION', lambda v: v is not None)
    # table10 = petl.sort(table9, key=['DATE', 'STATION_NAME', 'TIMEBLOCK'])
    # print list(table13)[:97]
    # print petl.select(table13, 'BLOCK_NO',
    #                   lambda v: v is None)
    # print table14

    if table14:
        sql = """INSERT INTO `power`.`generation_staging`
                (
                `date`,
                `block_no`,
                `pool_name`,
                `pool_type`,
                `generator_name`,
                `generation`,
                `discom`,
                `state`
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s)
                on duplicate key update
                generation = values(generation),
                processed_ind = 0,
                load_date = NULL
                """
        # colname = petl.header(table14)
        data = list(table14)[1:]
        conn = dbconn.connect(dsn)
        datacursor = conn.cursor()
        try:
            rv = datacursor.executemany(sql, data)
            print("Return Value %s" % str(rv))
            print("Load Status %s" % str(conn.info()))
            conn.commit()
            datacursor.close()
        except Exception as error:
            conn.rollback()
            datacursor.close()
            print("Error %s" % str(error))



def run(filename):
    # dirname = '/Users/biswadippaul/Projects/GETCO/117.247.83.101/3. Actual RE Gen/Solar actual 2016/'
    # for filename in dir_file_walk(dirname):
    xls_file = pd.ExcelFile(filename)
    xls_tabs = xls_file.sheet_names
    for tabs in xls_tabs:
        print filename, tabs
        if 'Sheet' not in tabs:
            load_getco_solar_data(filename, tabs)

file = "/Users/biswadippaul/Projects/GETCO/117.247.83.101/3. Actual RE Gen/Solar actual 2016/4. APR'16.xlsx"
run(file)

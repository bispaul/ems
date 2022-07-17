# import modules
import pandas as pd
import petl
import os
import dbconn
from datetime import datetime



file = "/Users/biswadippaul/Projects/GETCO/117.247.83.101/3. Actual RE Gen/Wind actual 2016/5. MAY-16-WIND(ABT) data to GEDA.xls"
dsn = '/Users/biswadippaul/Projects/batch/config/sqldb_connection_config.txt'

def dir_file_walk(path):
    return [os.path.join(root, name)
            for root, dirs, files in os.walk(path)
            for name in files
            if name.endswith((".xls"))]


def load_getco_wind_data(file):
    xls_file = petl.io.xls.fromxls(file)
    for i in xrange(5):
        data_list = list(xls_file.head(5))
        # print data_list
        if data_list[i][1]:
            if ('DATE' in data_list[i][0] or 'Date' in data_list[i][0]):
                hdrindx = i
                break
    table1 = xls_file.skip(hdrindx)
    hdr = table1.header()
    cutcol = (i for i, x in enumerate(hdr) if x is None or x == '')
    table2 = petl.cutout(table1, *cutcol)
    table3 = petl.convert(table2, hdr[1], int)
    table4 = petl.select(table3, hdr[1],
                         lambda v: v is not None)
    table5 = petl.convert(table4, hdr[0], lambda v: v.date())
    table6 = petl.convert(table5, table5.header()[2:], float)
    table7 = petl.filldown(table6, hdr[0])
    table8 = petl.melt(table7,
                       key=hdr[0:2])
    # print table7
    table9 = petl.rename(table8,
                         {hdr[1]: 'BLOCK_NO',
                          'variable': 'GENERATOR_NAME',
                          'value': 'GENERATION'})
    table10 = petl.addfield(table9, 'DISCOM', 'GUVNL')
    table11 = petl.addfield(table10, 'STATE', 'GUJARAT')
    table12 = petl.addfield(table11, 'POOL_TYPE', 'WIND', 2)
    table13 = petl.addfield(table12, 'POOL_NAME', 'RE', 2)
    # table10 = petl.select(table9, 'GENERATION', lambda v: v is not None)
    # print petl.util.vis.lookall(table12)
    if table13:
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
        data = list(table13)[1:]
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
            print(data[40:45])


def run():
    dirname = '/Users/biswadippaul/Projects/GETCO/117.247.83.101/3. Actual RE Gen/Wind actual 2017/'
    for filename in sorted(dir_file_walk(dirname)):
        print filename
        # if "10 OCTOBER-16-WIND(ABT) data to GEDA.xls" in filename:
        load_getco_wind_data(filename)


run()
import petl
import dbconn
from datetime import datetime


filename = '/Users/biswadippaul/Projects/GETCO/117.247.83.101/3. Actual RE Gen/gujarat_solar.csv'
dsn = '../config/sqldb_connection_config.txt'
date_time = petl.datetimeparser('%m/%d/%Y %I:%M:%S %p')
date_time2 = petl.datetimeparser('%m-%d-%Y %H:%M')
date_time3 = petl.datetimeparser('%m/%d/%y %H:%M')
date_time4 = petl.datetimeparser('%d/%m/%y %H:%M')
date_time5 = petl.datetimeparser('%m-%d-%Y %H:%M')
date_time6 = petl.datetimeparser('%m/%d/%Y %H:%M')

table1 = petl.fromcsv(filename)
# table2 = petl.skip(table1, 2)
hdr = list(petl.rowslice(table1, 0)[0])
hdr.insert(0, 'Time')
hdr.remove('Station Name -  ')
table2 = petl.skip(table1, 2)
table2 = petl.setheader(table2, hdr)
table3_1 = petl.convert(table2, 'Time', date_time)
table3_2 = petl.convert(table2, 'Time', date_time2)
table3_3 = petl.convert(table2, 'Time', date_time3)
table3_4 = petl.convert(table2, 'Time', date_time4)
table3_5 = petl.convert(table2, 'Time', date_time5)
table3_6 = petl.convert(table2, 'Time', date_time6)
table4_1 = petl.select(table3_1, 'Time',
                       lambda v: v is not None)
table4_2 = petl.select(table3_2, 'Time',
                       lambda v: v is not None)
table4_3 = petl.select(table3_3, 'Time',
                       lambda v: v is not None)
table4_4 = petl.select(table3_4, 'Time',
                       lambda v: v is not None)
table4_5 = petl.select(table3_5, 'Time',
                       lambda v: v is not None)
table4_6 = petl.select(table3_6, 'Time',
                       lambda v: v is not None)
table5 = petl.cat(table4_1, table4_2, table4_3, table4_4, table4_5, table4_6)
table6 = petl.convert(table5, hdr[1:], float)
table8 = petl.melt(table6,
                   key=hdr[0])
table9 = petl.rename(table8,
                     {'variable': 'GENERATOR_NAME',
                      'value': 'GENERATION'})
table10 = petl.addfield(table9, 'DISCOM', 'GUVNL')
table11 = petl.addfield(table10, 'STATE', 'GUJARAT')
table12 = petl.addfield(table11, 'POOL_TYPE', 'SOLAR', 1)
table13 = petl.addfield(table12, 'POOL_NAME', 'RE', 1)
# print table13
if table13:
    sql = """INSERT INTO `power`.`generation_staging3`
            (
            `datetime`,
            `pool_name`,
            `pool_type`,
            `generator_name`,
            `generation`,
            `discom`,
            `state`
            )
            values (%s, %s, %s, %s, %s, %s, %s)
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


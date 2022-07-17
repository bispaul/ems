import petl
import dbconn
from datetime import datetime


def try_parsing_date(text):
    for fmt in ('%m/%d/%Y %I:%M:%S %p', '%m-%d-%Y %I:%M:%S %p'):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    raise ValueError('no valid date format found')


# filename = '../data/demandpowercut/May 2016_April 2017.csv'
filename = '../data/demandpowercut/qumex_05Jun.csv'
# filename = '/Users/biswadippaul/Downloads/GUVNL_LOAD_03.07.2017.csv'
dsn = '../config/sqldb_connection_config.txt'
# dsn = '../config/sqldb_dev_gcloud.txt'
date_time = petl.datetimeparser('%m/%d/%Y %I:%M:%S %p')
date_time2 = petl.datetimeparser('%m-%d-%Y %H:%M')
date_time3 = petl.datetimeparser('%m/%d/%y %H:%M')
date_time4 = petl.datetimeparser('%d/%m/%y %H:%M')
table1 = petl.fromcsv(filename)
# print table1
table2 = petl.skip(table1, 1)
table3_1 = petl.convert(table2, 'Time', date_time)
table3_2 = petl.convert(table2, 'Time', date_time2)
table3_3 = petl.convert(table2, 'Time', date_time3)
table3_4 = petl.convert(table2, 'Time', date_time4)
table4_1 = petl.select(table3_1, 'Time',
                       lambda v: v is not None)
table4_2 = petl.select(table3_2, 'Time',
                       lambda v: v is not None)
table4_3 = petl.select(table3_3, 'Time',
                       lambda v: v is not None)
table4_4 = petl.select(table3_4, 'Time',
                       lambda v: v is not None)
# print table4_3
table5 = petl.cat(table4_1, table4_2, table4_3, table4_4)
table6 = petl.sort(table5, 'Time')

table7 = petl.addfield(table6, 'Entity_Name',
                       petl.header(table6)[1], 1)
table8 = petl.rename(table7,
                     {petl.header(table6)[1]: 'Quantum'})
table9 = petl.addfield(table8, 'Unit',
                       petl.header(table6)[1][-2:])
table10 = petl.addfield(table9, 'Discom', 'GUVNL')
table11 = petl.addfield(table10, 'State', 'GUJARAT')
table12 = petl.convert(table11, 'Quantum', float)
print len(list(table12)[1:])
print table12.head(10)

if table12:
    sql = """INSERT INTO `power`.`scada_staging`
            (
            `datetime`,
            `entity_name`,
            `quantum`,
            `unit`,
            `discom`,
            `state`,
            `source_name`
            )
            values (%s, %s, %s, %s, %s, %s, 'SCADA')
            on duplicate key update
            quantum = values(quantum),
            processed_ind = 0,
            load_date = NULL"""
    colname = petl.header(table12)
    data = list(table12)[1:]
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

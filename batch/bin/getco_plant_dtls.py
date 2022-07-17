import dbconn
import petl


file = "/Users/biswadippaul/Projects/GETCO/117.247.83.101/3. Actual RE Gen/WIND NAME NEW2.xlsx"
dsn = '/Users/biswadippaul/Projects/batch/config/sqldb_connection_config.txt'
xls_file = petl.io.xlsx.fromxlsx(file)
xls_file = xls_file.skip(2)
hdr = list(petl.header(xls_file))
cutcol = (i for i, x in enumerate(hdr) if x is None)
table1 = petl.cutout(xls_file, *cutcol)
table2 = petl.setheader(table1, ['organisation_name', 'substation_sending_end',
                                 'substation_receiving_end', 'capacity',
                                 'old_name', 'new_name'])
table3 = petl.addfield(table2, 'discom', 'GUVNL')
table4 = petl.select(table3, 'new_name',
                     lambda v: v is not None)
connection = dbconn.connect(dsn)
cursor = connection.cursor().execute('SET SQL_MODE=ANSI_QUOTES')
petl.appenddb(table4, connection, 'generator_mapping')

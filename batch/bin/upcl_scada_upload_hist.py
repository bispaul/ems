import petl as etl
# import sql_load_lib
# import pymysql
import dbconn
from datetime import date, time, timedelta, datetime
import calendar
import os

# def change_date_00hrs(date2, time2):
# 	print date2, time2, time(0,0)
# 	if time2 == time(0, 0):
# 		return date2 - timedelta(days=1)
# 	else:
# 		return date2

def row_str_detection(str):
	str_dict = {'HZ': 'frequency',
	 	'DEMAND': 'demand',
	 	'SCHEDULE': 'schedule',
	 	'OD_UD': 'od_ud',
	 	'EXTRN_NIC': 'net_drawl',
	 	'STTN': 'internal_generation'}
	for key in str_dict:
		if key in str:
			return str_dict[key]
	return 'tie_line'

#Scada the point of time 00:00:00 means data has been aggregated for 00:00:00 to 00:15:00
def time_block_ref():
	block_no_ref = [[i] for i in xrange(1, 97)]
	time_ref = [[(datetime(2000, 1, 1, 0, 0, 0) + timedelta(minutes=x*15)).time()] for x in range(0, 96)]

	table_block_no = etl.pushheader(block_no_ref, ['Block_no'])
	# print table_block_no.head()
	table_time = etl.pushheader(time_ref, ['Time'])
	# print table_time.head()
	table_bno_time = etl.annex(table_block_no, table_time)
	# print table_bno_time.head()
	return table_bno_time

def data_prep_w_clnup(filename, table_bno_time):
	date_time = etl.datetimeparser('%d/%m/%Y %H:%M:%S s')
	table1 = etl\
			.fromcsv(filename)\
			.rename('', 'Scada_datetime') \
			.skipcomments('Time') \
			.convert('Scada_datetime', date_time) \
			.addfield('Date', lambda rec: rec['Scada_datetime'].date()  
				if rec['Scada_datetime'] else rec['Scada_datetime']) \
			.addfield('Time', lambda rec: rec['Scada_datetime'].time() 
				if rec['Scada_datetime'] else rec['Scada_datetime']) \
			.addfield('Block_no', lambda rec: 
				((rec['Time'].hour * 60 * 60 + rec['Time'].minute * 60)/(15 * 60))+1)

	# print table1.head()
	# print etl.see(table1)
	c1 = etl.clock(table1)

	#Min and Max Date value
	data_min_max_date = etl.limits(table1, 'Date')
	# print data_min_max_date

	#Generate all dates for the month of the min and max dates
	date_ref = []
	data_month_val_old = -1
	data_year_val_old = -1
	for val in data_min_max_date:
		data_month_val, data_year_val = val.month, val.year
		if data_month_val != data_month_val_old or data_year_val != data_year_val_old:
			_, end_dt = calendar.monthrange(data_year_val, data_month_val)
			month_start_date = date(data_year_val, data_month_val, 1)
			month_end_date = date(data_year_val, data_month_val, end_dt)
			dd = [[month_start_date + timedelta(days=x)] \
				for x in range((month_end_date - month_start_date).days + 1)]
			date_ref.append(dd)
			data_month_val_old, data_year_val_old = data_month_val, data_year_val


	# print date_ref[0]
	table_date = etl.pushheader(date_ref[0], ['Date'])
	# print table_date.head()

	table_date_bno_time = etl.crossjoin(table_date, table_bno_time)
	# print table_date_bno_time.head()

	table2 = etl \
			.rightjoin(table1, table_date_bno_time, key=['Date', 'Block_no', 'Time']) \
			.convertnumbers() \
			.replaceall('', None)\
			.addfield('State', 'UTTARAKHAND') \
			.addfield('Discom', 'UPCL')		
	# print table2.head()
	c2 = etl.clock(table2)

	table3 = etl.melt(table2, key = ['Scada_datetime', 'Date', 'Time', 'State', 'Discom', 'Block_no']) \
			.rename('variable', 'Scada_variable') \
			.addfield('Variable', lambda rec: row_str_detection(rec['Scada_variable']))
	# print table3.head()
	c3 = etl.clock(table3)
	# p = etl.progress(c3, 100)
	# etl.tocsv(table3, '/Users/biswadippaul/Downloads/example.csv')
	return table3

# connection = pymysql.connect(user='root', password='power@2012', database='power')
# # # connection = pymysql.connect('/Users/biswadippaul/Projects/batch/config//Users/biswadippaul/Projects/batch/config.txt')
# # # tell MySQL to use standard quote character
# connection.cursor().execute('SET SQL_MODE=ANSI_QUOTES')
# etl.todb(table3, connection, 'upcl_scada_stg')
def dir_file_walk(path):
	return (os.path.join(root, name)
         for root, dirs, files in os.walk(path)
         for name in files
         if name.endswith((".csv")))

def main(args):
	"""
	Main function
	"""
	# connection = pymysql.connect(user='root', password='power@2012', database='power')
	# # connection = pymysql.connect('/Users/biswadippaul/Projects/batch/config//Users/biswadippaul/Projects/batch/config.txt')
	# # tell MySQL to use standard quote character
	# connection.cursor().execute('SET SQL_MODE=ANSI_QUOTES')
	connection = dbconn.connect('/Users/biswadippaul/Projects/batch/config/sqldb_connection_config.txt')
	cursor = connection.cursor().execute('SET SQL_MODE=ANSI_QUOTES')	
	table_bno_time = time_block_ref()
	for filename in dir_file_walk(args.dirroot):
		table3 = data_prep_w_clnup(filename, table_bno_time)
		#etl.todb(table3, connection, 'upcl_scada_stg')
		etl.appenddb(table3, connection, 'upcl_scada_stg')	


def test():
	# connection = pymysql.connect(user='root', password='power@2012', database='power')
	# # tell MySQL to use standard quote character
	# connection.cursor().execute('SET SQL_MODE=ANSI_QUOTES')
	connection = dbconn.connect('/Users/biswadippaul/Projects/batch/config/sqldb_connection_config.txt')
	cursor = connection.cursor().execute('SET SQL_MODE=ANSI_QUOTES')		
	table_bno_time = time_block_ref()
	for filename in dir_file_walk('/Users/biswadippaul/Downloads/load data/'):
		print filename
		table3 = data_prep_w_clnup(filename, table_bno_time)
		etl.appenddb(table3, connection, 'upcl_scada_stg')	

test()
# import modules
import pandas as pd
import petl
import dbconn
import os

file = '/Users/biswadippaul/Projects/GETCO/117.247.83.101/5. Load in 24 hrs (Ag - Non Ag)/AG - Non AG 2016/8. Gujarat Catered with and without Ag load(Aug-16).xlsx'
dsn = '/Users/biswadippaul/Projects/batch/config/sqldb_connection_config.txt'
# xls_file = pd.ExcelFile(file)
# xls_tabs = xls_file.sheet_names
# # df = xls_file.parse(xls_tabs[0])
# print xls_tabs

def dir_file_walk(path):
    return (os.path.join(root, name)
            for root, dirs, files in os.walk(path)
            for name in files
            if name.endswith((".xlsx")))


def load_getco_hr_dem_data(file, tab):
    # print file
    xls_file = petl.io.xlsx.fromxlsx(file, tab, range_string='A1:H35')
    # print xls_file
    date = petl.header(xls_file)[0]
    hdr_desc = petl.skip(xls_file, 2).header()
    table1 = petl.skip(xls_file, 2)
    # print 'date : ', date
    if not date:
        date = petl.skip(xls_file, 2).header()[0]
        hdr_desc = petl.skip(xls_file, 4).header()
        table1 = petl.skip(xls_file, 4)
        if not date:
            # print 'Here'
            date = petl.skip(xls_file, 1).header()[0]
            hdr_desc = petl.skip(xls_file, 3).header()
            table1 = petl.skip(xls_file, 3)
    # print 'date2 :', date
    # print hdr_desc
    clean_hdr = [x.strip() for x in list(hdr_desc)]
    # print clean_hdr
    ref_hdr = [u'TIME BLOCK',
               u'Guj Catered in MW',
               u'PGVCL                   AG HOURLY in  MW',
               u'UGVCL                   AG HOURLY in  MW',
               u'DGVCL                   AG HOURLY in  MW',
               u'MGVCL              AG HOURLY in  MW',
               u'Total Ag in MW',
               u'Gujarat Catered (without AG) in MW']
    ref_hdr2 = [u'TIME BLOCK', u'Guj Catered', u'PGVCL', u'UGVCL',
                u'DGVCL', u'MGVCL', u'Total Ag',
                u'Gujarat Catered (without AG)']
    if ref_hdr == clean_hdr or clean_hdr == ref_hdr2:
        new_hdr = ['TIME_BLOCK', 'TOTAL_LOAD', 'PVGCL_AG', 'UGVCL_AG',
                   'DGVCL_AG', 'MGVCL_AG', 'TOTAL_AG', 'TOTAL_NON_AG']
        table2 = petl.setheader(table1, new_hdr)
        table3 = petl.addrownumbers(table2)
        table4 = petl.select(table3, 'row',
                             lambda v: v <= 24)
        table5 = petl.cutout(table4, 'TIME_BLOCK')
        table6 = petl.rename(table5, {'row': 'BLOCK_HOUR_NO'})
        table7 = petl.addfield(table6, 'DATE', date, 0)
        dateparser = petl.dateparser('%d.%m.%y')
        table8 = petl.convert(table7, 'DATE', dateparser)
        table9 = petl.convert(table8, tuple(new_hdr[1:]), float)
        table10 = petl.addfield(table9, 'DISCOM', 'GUVNL', 2)
        table11 = petl.addfield(table10, 'STATE', 'GUJARAT', 2)
        print table11.head(5)
        connection = dbconn.connect(dsn)
        cursor = connection.cursor().execute('SET SQL_MODE=ANSI_QUOTES')
        # print 'loading table' + file + ' tab: ' + tab
        petl.appenddb(table11, connection, 'guvnl_drawl_hourly_stg')


def run():
    dirname = '/Users/biswadippaul/Projects/GETCO/117.247.83.101/5. Load in 24 hrs (Ag - Non Ag)/'
    for filename in dir_file_walk(dirname):
        xls_file = pd.ExcelFile(filename)
        xls_tabs = xls_file.sheet_names
        for tabs in xls_tabs:
            print filename, tabs
            if 'Sheet' not in tabs:
                load_getco_hr_dem_data(filename, tabs)

run()
# file = '/Users/biswadippaul/Projects/GETCO/117.247.83.101/5. Load in 24 hrs (Ag - Non Ag)/Gujarat Catered with and without Ag load.xlsx'
# xls_file = pd.ExcelFile(file)
# xls_tabs = xls_file.sheet_names
# for tabs in xls_tabs:
#     print xls_file, tabs
#     if 'Sheet' not in tabs:
#         load_getco_hr_dem_data(file, tabs)
#         # break
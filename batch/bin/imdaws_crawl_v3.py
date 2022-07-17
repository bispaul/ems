"""
Crawls the IMDAWS site and gets the actual data
"""
import html_get
import daterange
import html_tab_parse
import csv
import argparse
import filecmp
import difflib
import os
import sql_load_lib
import shutil
import glob
import logging
# import logging.config
# import urllib2
import requests
import dbconn
import petl as etl
import datetime as dt
import pytz
import time


# logging.config.fileConfig('logging.ini')


def imdaws_open_url_parse_data(browser, geturl, file_dir_nm):
    """using mechanize to simulate the broswer open and finally
    saving to a delimited file"""
    logger = logging.getLogger('imdaws_crawl_v3.imdaws_parse')
    for url, state, date in geturl:
        response = browser.open(url, timeout=60.0)
        html = response.read()
        tablelist = html_tab_parse.parse(html, "//*[@id='DeviceData']")
        file_dir_fnm = ''.join([file_dir_nm, "_", state, "_", date, ".dsf"])
        logger.debug("Processed for Date: %s for State: %s", date, state)
        yield file_dir_fnm, state, tablelist


def imdwas_file_wr(getfilenm_state_table):
    """Writing to a delimited file and copying the previous file"""
    logger = logging.getLogger('imdaws_crawl_v3.imdwas_file_wr')
    for file_dir_fnm, state, tablelist in getfilenm_state_table:
        logger.debug("file Name: %s", file_dir_fnm)
        rowgen = (row for table in tablelist for row in table)
        if os.path.isfile(file_dir_fnm):
            copy_file_nm = ''.join([file_dir_fnm, ".old"])
            shutil.copyfile(file_dir_fnm, copy_file_nm)
            logger.debug("Old File Written : %s", copy_file_nm)
        else:
            copy_file_nm = None
        with open(file_dir_fnm, "wb") as file_write:
            csvwriter = csv.writer(file_write, delimiter=';', quotechar='|',
                                   quoting=csv.QUOTE_MINIMAL)
            firstrow = True
            for row in rowgen:
                if firstrow:
                    row.append("State")
                    firstrow = False
                else:
                    row.append(state)
                csvwriter.writerow(row)
        file_write.close()
        logger.debug("File Written : %s", file_dir_fnm)
        yield file_dir_fnm, copy_file_nm


def imdwas_file_save(getfilenm_state_table):
    logger = logging.getLogger('imdaws_crawl_v3.imdwas_file_save')
    for file_dir_fnm, state, tablelist in getfilenm_state_table:
        print '************', file_dir_fnm, state, tablelist
        rowgen = (row for table in tablelist for row in table)
        with open(file_dir_fnm, "wb") as file_write:
            csvwriter = csv.writer(file_write, delimiter=';', quotechar='|',
                                   quoting=csv.QUOTE_MINIMAL)
            firstrow = True
            for row in rowgen:
                if firstrow:
                    row.append("State")
                    firstrow = False
                else:
                    row.append(state)
                csvwriter.writerow(row)
        file_write.close()
        logger.debug("File Written : %s", file_dir_fnm)
        yield file_dir_fnm


def db_delta_load(getfilenm):
    """Uploading delta changes to database"""
    logger = logging.getLogger('imdaws_crawl_v3.db_delta_load')
    diff = difflib.Differ()
    for newfilenm, oldfilenm in getfilenm:
        logger.debug("File Names: %s %s", newfilenm, oldfilenm)
        if oldfilenm and filecmp.cmp(newfilenm, oldfilenm):
            logger.debug("Filenames %s and %s are identical", newfilenm,
                         oldfilenm)
            logger.info("No changes to the data. Nothing to load in DB")
        elif oldfilenm and not filecmp.cmp(newfilenm, oldfilenm):
            with open(newfilenm) as new, open(oldfilenm) as old:
                add_in_new = [difference[2:] for difference in
                              diff.compare(old.readlines(), new.readlines())
                              if difference.startswith('-')]
            yield add_in_new
        else:
            return


def imdaws_date_state_url_build(statelist, start_date=None, end_date=None,
                                target_state=None):
    """
    Crawls the IMDAWS site for each state
    """
    logger = logging.getLogger('imdaws_crawl_v3.imdaws_crawl')
    viewurl = "http://imdaws.com/WeatherAWSData.aspx?&FromDate="
    logger.info('Target State: %s', target_state)
    for date in daterange.daterange(start_date, end_date):
        for key, state in statelist.iteritems():
            logger.debug('State Key: %s and State name: %s', key, state)
            if not key:
                continue
            elif target_state and state == target_state:
                url = ''.join([viewurl, date.strftime('%d/%m/%Y'), "&ToDate=",
                               date.strftime('%d/%m/%Y'), "&State=", str(key),
                               "&District=0&Loc=0&Time="])
                logger.debug(url)
                logger.debug("elif*****Date: %s State: %s",
                             date.strftime('%d-%m-%Y'),
                             state)
                yield url, state, date.strftime('%d-%m-%Y')
            elif not target_state:
                url = ''.join([viewurl, date.strftime('%d/%m/%Y'), "&ToDate=",
                               date.strftime('%d/%m/%Y'), "&State=", str(key),
                               "&District=0&Loc=0&Time="])
                logger.debug(url)
                logger.debug("else***Date: %s State: %s",
                             date.strftime('%d-%m-%Y'),
                             state)
                yield url, state, date.strftime('%d-%m-%Y')


def imdaws_file_read(filename):
    """
    Read the flast file and change it into list of tuples
    for bulk insert
    """
    logger = logging.getLogger('imdaws_crawl_v3.imdaws_file_read')
    data = []
    with open(filename, 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter=';')
        headers = next(reader)
        t_utc_indx = headers.index('TIME[UTC]')
        date_indx = headers.index('DATE')
        logger.debug("{} File Data header: {}".format(filename, headers))
        for line in reader:
            if any(field.strip() for field in line):
                row = list(line)
                newrow = [None if ele in ('NA', '----')
                          else ele for ele in row]
                date = dt.datetime.strptime(newrow[date_indx], '%d-%b-%Y')\
                    .date()
                time = dt.datetime.strptime(newrow[t_utc_indx], '%H:%M:%S')\
                    .time()
                datetime_utc = dt.datetime.combine(date, time)\
                    .replace(tzinfo=pytz.UTC)
                tz_india = pytz.timezone('Asia/Kolkata')
                # datetime_ist = datetime_utc.astimezone(tz_india).isoformat()
                datetime_ist = datetime_utc.astimezone(tz_india)\
                    .strftime('%Y-%m-%d %H:%M:%S')
                newrow.append(datetime_ist)
            data.append(tuple(newrow))
    logger.debug("{} File Data: {}".format(filename, data))
    return data


def imdaws_file_db_load(dsnfile, tabname, load_file_path_name):
    """
    Function to load files recursively
    """
    logger = logging.getLogger('imdaws_crawl_v3.imdaws_file_db_load')
    for files in glob.glob(load_file_path_name):
        # param = files.replace('\\', '//')
        # regex = re.compile("\d\d[-]\d\d[-]\d\d\d\d")
        # param.append(regex.findall(files)[0])
        # print dsnfile, tabname, param
        # logger.info("%s %s %s", dsnfile, tabname, param)
        # sql_load_lib.sql_table_load_exec(dsnfile, tabname, param)
        logger.info("%s %s %s", dsnfile, tabname, files)
        data = imdaws_file_read(files)
        sql_load_lib.sql_table_insert_exec(dsnfile, tabname, data)


def imdaws_control_list(url, controlname):
    "Get the list of the controlnames in the imdaws website"
    browser = html_get.getbrowser()
    return html_get.controlitem_fetch(browser, url, controlname)


def imdaws_gen_url(stateid=None, distid=None):
    url = "http://imdaws.com"
    if stateid and distid:
        return ''.join([url, "/LocData.aspx?DevType=AWS", "&StateId=",
                        str(stateid), "&DistId=", str(distid)])
    elif stateid and not distid:
        return ''.join([url, "/DistrictData.aspx?DevType=AWS", "&StateId=",
                        str(stateid)])
    else:
        return ''.join([url, "/ViewAwsData.aspx"])


def get_dis_loc(url):
    """Convert the string to respective list"""
    logger = logging.getLogger('imdaws_crawl_v3.main')
    logger.debug('url: %s', url)
    retries = 5
    for i in range(retries):
        try:
            # dis_loc_str = urllib2.urlopen(url).read()
            dis_loc_str = requests.get(url).text
            # logger.debug('url data: %s', dis_loc_str)
            return [dis_loc.split(':')[0]
                    for dis_loc in dis_loc_str.split('|')
                    if len(dis_loc) > 0]
        except Exception, err:
            logger.error("Error %s for url %s", str(err), url)
            if i < retries:
                time.sleep(10)
                continue
            else:
                raise
        break


def imdaws_state_district_loc_map(state_list):
    """Get the imdaws website state,district and location mapping"""
    # state_list = imdaws_control_list(imdaws_gen_url(), "CmbState")
    return [(state, dis, loc) for key, state in state_list.iteritems()
            for dis in get_dis_loc(imdaws_gen_url(key))
            for loc in get_dis_loc(imdaws_gen_url(key, dis))
            if key > 0]


def imdaws_state_dis_loc_map_db(dsnfile, state_list):
    """Upload the imdaws website state,district and location mapping into db"""
    # logger = logging.getLogger('imdaws_crawl_v3.imdaws_state_dis_loc_map_db')
    connection = dbconn.connect(dsnfile)
    connection.cursor().execute('SET SQL_MODE=ANSI_QUOTES')
    tablename = 'imdaws_state_dis_loc_map'
    ini_data = etl.fromdb(connection, """SELECT state, district,
        station_name, state_master_map FROM {} where
        delete_ind = 0""".format(tablename))
    base_data = etl.cutout(ini_data, 3)
    state_map = etl.cutout(ini_data, 1, 2)
    state_map_distinct = etl.distinct(state_map)
    # etl.tocsv(ini_data, '/Users/biswadippaul/Documents/UPCL/ini_data.csv')
    fresh_data = etl.setheader(imdaws_state_district_loc_map(state_list),
                               ['state', 'district', 'station_name'])
    # etl.tocsv(fresh_data, '/Users/biswadippaul/Documents/UPCL/fresh_data.csv')
    # logger.debug('fresh_data table: %s', etl.look(fresh_data))
    # diff_data = etl.recordcomplement(fresh_data, ini_data)
    # etl.tocsv(diff_data, '/Users/biswadippaul/Documents/UPCL/diff_data.csv')
    # logger.debug('diff_data table: %s', etl.look(diff_data))
    # added, subtracted = etl.recorddiff(fresh_data, ini_data)
    # logger.debug('diff_data table: %s', etl.look(subtracted))
    # logger.debug('add_data table: %s', etl.look(added))
    # etl.tocsv(added, '/Users/biswadippaul/Documents/UPCL/added.csv')
    # etl.tocsv(subtracted, '/Users/biswadippaul/Documents/UPCL/subtracted.csv')
    # data_to_append = etl.addfield(subtracted, 'state_master_map',
    #     lambda row: row['state'])
    # etl.appenddb(data_to_append, connection, tablename)
    fresh_data2 = etl.convert(fresh_data,
                              ('state', 'district', 'station_name'), str)
    data_tobe_added = etl.antijoin(fresh_data2, base_data,
                                   key=('state', 'district', 'station_name'))
    data_to_append = etl.lookupjoin(data_tobe_added, state_map_distinct,
                                    key='state')
    if etl.nrows(data_to_append):
        etl.appenddb(data_to_append, connection, tablename)


def main(args):
    """
    Main calls to the methods
    """
    logger = logging.getLogger('imdaws_crawl_v3.main')
    url = "http://imdaws.com/ViewAwsData.aspx"
    browser = html_get.getbrowser()
    file_dir_nm = ''.join([args.dir, args.filenm.upper()])
    # dbpaam = (args.dsn, args.table)
    # statelist = html_get.controlitem_fetch(browser, args.url, "CmbState")
    statelist = html_get.controlitem_fetch(browser, url, "CmbState")
    # statelist = {0: 'All States', 1: 'ANDAMAN & NICOBAR', 2: 'ANDHRA PRADESH', 3: 'ARUNACHAL PRADESH', 4: 'ASSAM'}
    # print [(key, state) for key, state in statelist.iteritems()]
    if args.state:
        statelist = {key: state for key, state in statelist.items()
                     if key > 0 and state == args.state}

    imdaws_state_dis_loc_map_db(args.dsn, statelist)
    geturl = imdaws_date_state_url_build(statelist, args.start_date,
                                         args.end_date, args.state)
    getfilenm_state_table = imdaws_open_url_parse_data(browser, geturl,
                                                       file_dir_nm)
    # getfilenm = imdwas_file_wr(getfilenm_state_table)
    getfilenm = imdwas_file_save(getfilenm_state_table)

    for filenm in getfilenm:
        logger.info('Filename**** %s', filenm)
        abs_file_path_name = os.path.abspath(filenm)
        logger.info('Absolute path with file**** %s', abs_file_path_name)
        imdaws_file_db_load(args.dsn, args.table, abs_file_path_name)

    if args.state:
        sql_load_lib.sql_weather_actual_load(args.dsn, args.state)
    # if args.discom:
    #     sql_load_lib.sql_wtr_unified_ins_upd(args.dsn, args.discom)
    elif args.allstate:
        sql_load_lib.sql_weather_actual_load(args.dsn)

    # for j in db_delta_load(getfilenm):
    #     print j

# imdaws_state_dis_loc_map_db('/Users/biswadippaul/Projects/batch/config/sqldb_connection_config.txt')
# imdaws_file_db_load('/Users/biswadippaul/Projects/batch/config/sqldb_connection_config.txt', 'imdaws_weather_stg', '/Users/biswadippaul/Projects/batch/data/IMDAWS_DELHI_01-04-2016.dsf')
# imdaws_file_db_load('/Users/biswadippaul/Projects/batch/config/sqldb_connection_config.txt', 'imdaws_weather_stg', '/Users/biswadippaul/Projects/batch/data/IMDAWS_NCR_01-04-2016.dsf')

if __name__ == '__main__':
    logger = logging.getLogger('imdaws_crawl_v3.main')
    ARG = argparse.ArgumentParser(description=("Fetches IMDAWS data and"
                                               "Uploads Staging table data"))
    # ARG.add_argument('-u', '--url', dest='url',
    #                  help='URL to fetch data from', required=True)
    ARG.add_argument('-d', '--dir', dest='dir',
                     help='Directory to save data to.', required=True)
    ARG.add_argument('-f', '--file', dest='filenm', help=('Name of the file.'
                     'The name will be appended with date of the data'),
                     required=True)
    ARG.add_argument('-b', '--strdt', dest='start_date', help=('Start Date'
                     'to fetch the data.Default is todays date'),
                     required=True)
    ARG.add_argument('-e', '--enddt', dest='end_date',
                     help='End Date to fetch the data.Default is todays date',
                     required=True)
    ARG.add_argument('-t', '--tablename', dest='table',
                     help='Table in which data is to be inserted',
                     required=True)
    ARG.add_argument('-m', '--dbdsnconfig', dest='dsn',
                     help='Full path and the file name of the db config',
                     required=True)
    ARG.add_argument('-s', '--state', dest='state',
                     help='State name as per the dropdown of imdaws',
                     required=False)
    ARG.add_argument('-a', '--allstate', dest='allstate', default=False,
                     type=bool,
                     help=('All State load to actual_weather table.'
                           'Value can be True or False'),
                     required=False)
    # ARG.add_argument('-di', '--discom', dest='discom', default=False,
    #                  type=bool,
    #                  help=('Discom Name.'),
    #                  required=False)
    logger.info("Parsing {}".format(ARG.parse_args()))
    main(ARG.parse_args())

# imdaws_crawl("http://imdaws.com/ViewAwsData.aspx",
#              "C:/Users/Public/Documents/ZEERONE/NRLDC_HP/",
#              "imdaws", "07-10-2012", "09-10-2012")
# imdaws_crawl("http://imdaws.com/ViewAwsData.aspx",
#              "C:/Users/Public/Documents/ZEERONE/NRLDC_HP/",
#              "imdaws", "06-08-2012", "06-08-2012")
# State={1:"HImachal Praddesh",2:"Meghalaya"}
# for date in daterange( "02-07-2012", "05-10-2012"):
#    print date.strftime('%d-%m-%Y')
#    for key in State:
#        print State[key]

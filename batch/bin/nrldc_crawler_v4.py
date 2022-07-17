"""
Crawls the nrldc site and downloads the files.
Uploads the file in the database and keeps track of the status.
"""
import mechanize
from bs4 import BeautifulSoup
import daterange
import argparse
# import time
import datetime
import sql_load_lib
import csv
import html_tab_parse
import logging
import logging.config
import os
import re
from retrying import retry


path = os.path.dirname(os.path.realpath(__file__))
logging.config.fileConfig(path + '/logging.ini')

BROWSER_HEADER = [('User-agent', 'Mozilla/5.0 '
                   '(Macintosh; Intel Mac OS X 10_12_2) '
                   'AppleWebKit/537.36 (KHTML, like Gecko) '
                   'Chrome/55.0.2883.95 Safari/537.36')]


def db_rev_check(dsnfile, date, state, file_nm):
    """
     Get the revision and the status of the job from db table
    """
    dbrev = -1
    (dbrev, dbstatus, mins_elapsed) =\
        sql_load_lib.sql_sp_load_exec(dsnfile,
                                      date.strftime('%Y-%m-%d'),
                                      dbrev, state,
                                      file_nm, '',
                                      'CHKPNT')
    if dbstatus == 'RUNNING' and mins_elapsed < 10:
        raise ValueError('Job is in RUNNING state')
    elif dbrev >= -1 and dbstatus == 'SUCCESS':
        startkey = dbrev + 1
    elif dbrev >= -1 and dbstatus != 'SUCCESS' and dbstatus != '':
        startkey = dbrev
    else:
        startkey = -1
    return startkey


# @retry(stop_max_attempt_number=6,
#        wait_exponential_multiplier=10000,
#        wait_exponential_max=20000)
def get_url(url, eventtarget):
    """
    Get all the revision number from the nrldc url passed
    """
    print('***** 0')
    browser = mechanize.Browser()
    browser.set_handle_robots(False)
    browser.set_handle_refresh(mechanize._http.HTTPRefreshProcessor(),
                               max_time=1)
    # Want debugging messages?
    # browser.set_debug_http(True)
    # browser.set_debug_redirects(True)
    # browser.set_debug_responses(True)

    browser.addheaders = BROWSER_HEADER
    # try:
    response = browser.open(url)
    htmlbody = BeautifulSoup(response.get_data())
    text = 'Sorry !! Data for this date does not exists.'
    searchlist = htmlbody.findAll(text=re.compile(text))
    print('***** 1')
    if len(searchlist) == 0:
        for events in eventtarget:
            # Sorry !! Data for this date does not exists.
            response.set_data(BeautifulSoup(response.get_data()).prettify())
            browser.set_response(response)
            browser.select_form(nr=0)
            browser.set_all_readonly(False)
            browser["__EVENTTARGET"] = events[0]
            browser["__EVENTARGUMENT"] = ''
            if events[1] != '':
                if events[0] == "txtStartDate":
                    browser[events[0]] = events[1]
                else:
                    browser[events[0]] = [str(events[1])]
            if events[0] == 'download':
                for control in browser.form.controls:
                    if control.type == "submit":
                        control.disabled = True
            response = browser.submit()
        return (browser, response)
    else:
        return (browser, None)
    # except Exception, err:
        # print "Failed:", str(err)
        # return (None, None)

# get_url('http://wbs.nrldc.in/WBS/finalentt.aspx?dt=13-07-2017&st=UTTARANCHAL', [("txtStartDate", '13-07-2017')])

def nrldc_rev_parse(url, date):
    """
    Get all the revision number from the nrldc url passed
    """
    print(url, date)
    rev = {}
    try:
        browser = get_url(url, [("txtStartDate", date)])[0]
        browser.form = list(browser.forms())[0]
        control = browser.form.find_control("RevPickerID")
        if control.type == "select":
            # means it's class ClientForm.SelectControl
            for item in control.items:
                rev[int(item.name)] = \
                    [label.text for label in item.get_labels()][0]
        return sorted(rev)
    except Exception:
        return None


def get_file(url, file_nm, date, state, revision):
    """
    Get the csv download file from nrldc url passed
    """
    url_dict = {'NRLDC_Entitlement': [("txtStartDate", date),
                                      ("ToStatePickerID", state),
                                      ("RevPickerID", revision),
                                      ("download", '')],
                'NRLDC_Declared_Capability': [("txtStartDate", date),
                                              ("RevPickerID", revision),
                                              ("download", '')],
                'ISGS_Injection_Schedule': [("txtStartDate", date),
                                            ("RevPickerID", revision),
                                            ("download", '')],
                'NRLDC_ISGS': [("download", '')],
                'NRLDC_LTA': [("download", '')],
                'NRLDC_IEX_PXIL': [("download", '')],
                'NRLDC_MTOA': [("download", '')],
                'NRLDC_Shared': [("download", '')],
                'NRLDC_Bilateral': [("download", '')]
                }
    response = get_url(url, url_dict[file_nm])[1]
    # print response.read()
    if response:
        return response.read()
    else:
        return None


def fileobj_read(fileobj, file_nm):
    """
    Read the csv file from file name passed
    """
    fdata = []
    if file_nm == "NRLDC_IEX_PXIL":
        skiprowlist = ('ForDate', 'Periphery')
    else:
        skiprowlist = ('ForDate', 'State', 'Periphery', 'StationName')
    with open(fileobj, 'rb') as csvfile:
        csvreader = csv.reader(csvfile, delimiter=',', quotechar='"')
        next(csvreader)
        first_row_data = True
        for row in csvreader:
            rdata = []
            if row[0] in skiprowlist:
                continue
            elif first_row_data:
                fordate, revision, issuedatetime = tuple(row)
                first_row_data = False
            else:
                data = [datetime.datetime.strptime(fordate, '%d/%m/%Y')
                        .strftime("%Y-%m-%d"), row[0], row[1], row[2],
                        str(issuedatetime), int(revision),
                        file_nm[len('NRLDC_'):]]
                data_map = {'NRLDC_ISGS': [data[0], data[1], data[4],
                                           data[5], data[6], data[2],
                                           None, None, None, None,
                                           None, None, None, None] +
                            row[:1:-1],
                            'NRLDC_IEX_PXIL': [data[0], data[2], data[4],
                                               data[5], data[6], None,
                                               data[1], data[3], None,
                                               None, None, None, None, None] +
                            row[:2:-1],
                            'NRLDC_Entitlement': [data[0], data[1], data[4],
                                                  data[5], data[2]] +
                            row[:1:-1],
                            'NRLDC_Declared_Capability': [data[0], data[4],
                                                          data[5], data[1]] +
                            row[:0:-1],
                            'ISGS_Injection_Schedule': [data[0], data[4],
                                                        data[5]] +
                            row[0:],
                            'Other': [data[0], data[1], data[4], data[5],
                                      data[6], None, None] +
                            row[1:8] + row[:7:-1]
                            }
                rdata = data_map.get(file_nm, data_map['Other'])
            if rdata:
                fdata.append(tuple(rdata))
    return fdata


def arg_to_str(*args):
    """
    Concatenates all the args and returns a String.
    The last paramter is for eliminating the elements passed in the string
    in case the secons last boolean arg is True.
    """
    if args[-2]:
        return ''.join([str(args[elm]) for elm in range(0, len(args) - 2)
                        if elm not in args[-1]])
    else:
        return ''.join([str(args[elm]) for elm in range(0, len(args) - 2)])


@retry(stop_max_attempt_number=6,
       wait_exponential_multiplier=10000,
       wait_exponential_max=20000)
def est_loss_parse(url, file_dir, filename, tabname, dsnfile):
    """
    Crawling the url and fetching and inserting the HTML table
    """
    browser = mechanize.Browser()
    browser.set_handle_robots(False)
    browser.addheaders = BROWSER_HEADER
    file_full_nm = arg_to_str(file_dir, filename, "_",
                              datetime.datetime.now().strftime('%d-%m-%Y'),
                              ".csv", False, ())
    try:
        response = browser.open(url)
        html = response.read()
        #print(html)
        tablelist = html_tab_parse.parse(html, "//*[@id='NRSchloss']")
        print('****tablelist: ', tablelist)
        csvwriter = csv.writer(open(file_full_nm, "wb"),
                               delimiter=',',
                               quotechar='"',
                               quoting=csv.QUOTE_MINIMAL)
        data = []
        for rowval in (row for table in tablelist for row in table):
            csvwriter.writerow(rowval)
            if rowval[0] == "Week":
                data.append((datetime.datetime.strptime(rowval[1], '%d-%b-%Y')
                             .strftime("%Y-%m-%d"),
                             datetime.datetime.strptime(rowval[2], '%d-%b-%Y')
                             .strftime("%Y-%m-%d"), rowval[3]))
        sql_load_lib.sql_table_insert_exec(dsnfile, tabname, data)
        sql_load_lib.sql_sp_load_exec(dsnfile, datetime.datetime.today()
                                      .strftime('%Y-%m-%d'), None,
                                      '', filename, 'SUCCESS',
                                      'UPINSPNT')
    except Exception:
        sql_load_lib.sql_sp_load_exec(dsnfile,
                                      datetime.datetime.today()
                                      .strftime('%Y-%m-%d'),
                                      None,
                                      '', filename, 'FAILED',
                                      'UPINSPNT')
        raise
    return


def db_panel_sp_call(dsnfile, indate, inrevision, param_list):
    """
    Call Stored Procedures to load the respective panel tables.
    """
    print(dsnfile, indate, inrevision, param_list)
    state_drawl_list = ['ISGS', 'LTA', 'MTOA', 'Shared', 'IEX_PXIL',
                        'Bilateral']
    if param_list[0] in state_drawl_list:
        sql_load_lib.sql_nrldc_state_sch(dsnfile, indate,
                                         param_list[1], param_list[0],
                                         inrevision)
        if inrevision == 0:
            sql_load_lib.sql_nrldc_state_sch_revz(dsnfile, indate,
                                                  param_list[1],
                                                  param_list[0],
                                                  inrevision)
    elif param_list[0] in ('Entitlement'):
        sql_load_lib.sql_nrldc_entitlements(dsnfile, indate,
                                            param_list[1], inrevision)
    elif param_list[0] in ('Declared_Capability'):
        sql_load_lib.sql_nrldc_declared_capability(dsnfile,
                                                   indate, inrevision)
    return


def main(args):
    """
    Main Function. Based on filename call to different functions
    """
    print(args)
    if args.filenm == "NRLDC_Schedule_Est_Loss":
        print('NRLDC_Schedule_Est_Loss')
        est_loss_parse(args.url, args.dir, args.filenm,
                       args.tabname, args.dsn)
    else:
        file_tup = ('NRLDC_Entitlement', 'NRLDC_Declared_Capability',
                    'ISGS_Injection_Schedule')
        type_dict = {'NRLDC_MTOA': "&ty=5", 'NRLDC_Shared': "&ty=1",
                     'NRLDC_Bilateral': "&ty=2"}
        for date in daterange.daterange(args.start_date, args.end_date):
            dbrev = db_rev_check(args.dsn, date, args.state, args.filenm)
            rev = nrldc_rev_parse(arg_to_str(args.url, "?dt=",
                                             date.strftime('%d-%m-%Y'),
                                             "&st=", args.state,
                                             args.filenm not in file_tup,
                                             (1, 2, 3, 4)),
                                  date.strftime('%d-%m-%Y'))
            # print '*******'
            # print rev
            # print dbrev
            # rev.index(dbrev)
            if rev and dbrev in rev and dbrev <= rev[-1]:
                if not args.revflag and dbrev < 0 and rev[-1] != 0:
                    rev_arr = [0, rev[-1]]
                elif not args.revflag:
                    rev_arr = [rev[-1]]
                elif args.revflag:
                    rev_arr = rev[rev.index(dbrev):]

                # for key in rev[rev.index(dbrev):]:
                # for key in [rev[-1]]:
                for key in rev_arr:
                    # if not key % 10:
                    #    time.sleep(5)
                    sql_load_lib.sql_sp_load_exec(args.dsn,
                                                  date.strftime('%Y-%m-%d'),
                                                  key, args.state, args.filenm,
                                                  'RUNNING', 'UPINSPNT')

                    url2 = arg_to_str(args.url, "?dt=",
                                      date.strftime('%d-%m-%Y'),
                                      type_dict.get(args.filenm, ''), "&st=",
                                      args.state, "&rev=", key,
                                      args.filenm in file_tup,
                                      (1, 2, 3, 4, 5, 6, 7))
                    logger = logging.getLogger('nrldc_crawler_v4')
                    logger.info('url2:' + url2)
                    filep_dir = arg_to_str(args.dir, args.filenm, "_",
                                           args.state, "_",
                                           date.strftime('%d-%m-%Y'),
                                           "_", "Rev", key, ".csv",
                                           args.filenm in file_tup[1:],
                                           (3, 4))
                    logger.info('filep_dir:' + filep_dir)
                    try:
                        filecontent = get_file(url2, args.filenm,
                                               date.strftime('%d-%m-%Y'),
                                               args.state, key)
                        if filecontent:
                            file_handler = open(filep_dir, 'wb')
                            file_handler.write(filecontent)
                            file_handler.close()
                            file_reader = fileobj_read(filep_dir, args.filenm)
                            # print 'FILE READ', file_reader
                            sql_load_lib.sql_table_insert_exec(args.dsn,
                                                               args.tabname,
                                                               file_reader)
                            sql_load_lib\
                                .sql_sp_load_exec(args.dsn,
                                                  date.strftime('%Y-%m-%d'),
                                                  key, args.state,
                                                  args.filenm,
                                                  'SUCCESS', 'UPINSPNT')
                            db_panel_sp_call(args.dsn,
                                             date.strftime('%Y-%m-%d'),
                                             key,
                                             [args.filenm[6:], args.state])
                        else:
                            sql_load_lib\
                                .sql_sp_load_exec(args.dsn,
                                                  date.strftime('%Y-%m-%d'),
                                                  key, args.state,
                                                  args.filenm,
                                                  'SUCCESS', 'UPINSPNT')
                    except Exception:
                        file_handler.close()
                        sql_load_lib\
                            .sql_sp_load_exec(args.dsn,
                                              date.strftime('%Y-%m-%d'),
                                              key,
                                              args.state, args.filenm,
                                              'FAILED', 'UPINSPNT')
                        raise
    return


if __name__ == '__main__':
    ARG = argparse.ArgumentParser(description=("Fetches NRLDC data and"
                                  "Uploads Staging table data"))
    ARG.add_argument('-u', '--url', dest='url',
                     help='URL to fetch data from',
                     required=True)
    ARG.add_argument('-s', '--state', dest='state',
                     help='State to fetch data for'
                     )
    ARG.add_argument('-d', '--dir', dest='dir',
                     help='Directory to save data to',
                     required=True)
    ARG.add_argument('-f', '--file', dest='filenm',
                     help=('Local file name to save data to.'
                           'The name will be appended with date for '
                           'which crawling is done'), required=True)
    ARG.add_argument('-b', '--strdt', dest='start_date',
                     help=('Start of Date to crawl the data.'
                           'Optional default is todays date'))
    ARG.add_argument('-e', '--enddt', dest='end_date',
                     help=('Start of Date to crawl the data.'
                           'Optional default is todays date'))
    ARG.add_argument('-m', '--dbdsnconfig', dest='dsn',
                     help='Full path and the file name of the db config',
                     required=True)
    ARG.add_argument('-t', '--tabnm', dest='tabname',
                     help='Name of the table to be loaded',
                     required=True)
    ARG.add_argument('-a', '--allrev', dest='revflag', type=bool,
                     help='Download all revision flag. Default is True.',
                     default=True)
    main(ARG.parse_args())

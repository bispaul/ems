"""
Crawls the ERLDC site and uploads the data to a database.
"""
import datetime
import sql_load_lib
import mechanize
from BeautifulSoup import BeautifulSoup
import nrldc_crawler_v4
import xlrd
import re
import pandas as pd
import argparse
import daterange
import cookielib
from dateutil.parser import parse
import time
import pytz


class Crawler(object):
    """
    Crawls the SLDC site and downloads the files.
    """
    def __init__(self, url, browser_header=None):
        if browser_header is None:
            self.browser_header = [('User-agent',
                                    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36')]
        else:
            self.browser_header = browser_header

        class NoHistory(object):
            def add(self, *a, **k): pass
            def clear(self): pass

        self.browser = mechanize.Browser(history=NoHistory())
        self.url = url

    def make_url(self):
        """
        In case of ERLDC there is a single file for all the states.
        Which is available via various links and date selections.
        But it just gets the files with a specific format from
        http//www.erldc.org/current_schedule/states/.
        So implementing a hack to directly fetch the files
        """
        # self.url = mechanize.urljoin(self.url, "/current_schedule/states/")
        self.url = mechanize.urljoin(self.url, "/schedule-er.aspx")

    def get_url(self):
        """
        Gets the page for the url
        """
        cj = cookielib.LWPCookieJar()
        self.browser.set_cookiejar(cj)
        self.browser.set_handle_equiv(True)
        self.browser.set_handle_gzip(True)
        self.browser.set_handle_redirect(True)
        self.browser.set_handle_referer(True)
        self.browser.set_handle_robots(False)
        self.browser.set_handle_refresh(mechanize._http.HTTPRefreshProcessor(),
                                        max_time=1)
        # Want debugging messages?
        # self.browser.set_debug_http(True)
        # self.browser.set_debug_redirects(True)
        # self.browser.set_debug_responses(True)
        self.browser.addheaders = self.browser_header
        return self.browser.open(self.url)

    def prettyfy_html(self, response, refresh=True):
        """
        Standardize and prettify the html object
        """
        if refresh:
            response = self.get_url(self.url)
        webpage_pretty = BeautifulSoup(response.get_data()).prettify()
        response.set_data(webpage_pretty)
        self.browser.set_response(response)
        self.browser.select_form(nr=0)  # select the default form
        self.browser.set_all_readonly(False)
        # print self.browser

    def get_selection_list(self, response, controlname):
        """
        Gets the dropdown list values of the dropdown controls for WRLDC
        Dropdown Controls: CONS and Revision
        """
        self.prettyfy_html(response, False)
        control = self.browser.form.find_control(controlname)
        if control.type == "select":
            return [label.text for item in control.items
                    for label in item.get_labels()]


class ErldcSchedule(object):
    """
    Crawls the SLDC site and downloads the files.
    """
    def __init__(self, date, url, dirname):
        self.url = url
        self.dirname = dirname
        # self.date = datetime.datetime.strptime(date, '%d-%b-%Y')
        self.date = date
        self.htmldateobj = "ctl00$ContentPlaceHolder1$DropDownList2"
        self.htmlrevobj = "ctl00$ContentPlaceHolder1$DropDownList1"
        self.datelist_sor, \
            self.revison = self.get_date_revision(self.url +
                                                  "/schedule-er.aspx")
        self.add_date()
        self.dict_dir = {  # 'isgs': ("/current_schedule/", 'isgs'),
                         'stat': ("/current_schedule/", 'states',
                                  'erldc_state_drawl_schedule_stg')
                         # 'reg': ("/current_schedule/", 'regions'),
                         # 'bilt': ("/current_schedule/", 'bilateral'),
                         # 'pxi': ("/current_schedule/", 'pxi'),
                         # 'iex': ("/current_schedule/", 'iex'),
                         # 'state-entl-': ("/entitlement/", 'states')
                         }

    def get_date_revision(self, url):
        """
        Pass the url for ERLDC along with desired date also the html/javscript
        name of the date selection object i.e.
        "ctl00$ContentPlaceHolder1$DropDownList2"
        as well as revison selection object i.e.
        "ctl00$ContentPlaceHolder1$DropDownList1"
        """
        print url
        erldc_crawler = Crawler(url)
        res = erldc_crawler.get_url()
        print res
        datelist = erldc_crawler.get_selection_list(res, self.htmldateobj)
        print datelist
        datelist_sor = sorted([datetime.datetime.strptime(date1,
                                                          '%d-%b-%Y').date()
                               for date1 in datelist])
        revison = erldc_crawler.get_selection_list(res, self.htmlrevobj)
        # erldc_crawler.verify_date('29-08-2014')
        # print revison[0], revison[-1]
        # print datelist_sor, revison
        return datelist_sor, revison

    def add_date(self):
        """
        Add dates in the list
        """
        if self.date.date() not in self.datelist_sor:
            self.datelist_sor.append(self.date.date())
            self.datelist_sor.sort()
            # if '0' not in self.revison:
            #     self.revison.insert(0, '0')

    def crawl_get_file(self, url, (key, filen, date, rev)):
        """
        Pass the url for ERLDC along with sorted date and revision
        """
        # print date, rev, filen
        filenm = "{0}/{1}{2}-Rev.No{3}.xls".format(filen,
                                                   key,
                                                   date.strftime('%d%m%y'),
                                                   rev
                                                   )
        # print filenm, url + filenm, self.dirname
        erldc_crawler = Crawler(url + filenm)
        # try:
        data = erldc_crawler.get_url().read()
        # save = open('F:/nrldc/Data/' + filenm.split("/")[1], 'wb')
        filedirname = self.dirname + filenm.split("/")[1]
        save = open(filedirname, 'wb')
        save.write(data)
        save.close()
        return filedirname
        # except Exception, error:
        #     print error

    def get_file(self, dns):
        """
        Generate the filename to fetch
        """
        # url = self.url + "/current_schedule/"
        # for date in self.datelist_sor:
        for date in [self.datelist_sor[0]]:        
            for rev in [int(self.revison[-1])]:
                for key, filen in self.dict_dir.iteritems():
                    dbrev = nrldc_crawler_v4.db_rev_check(dns,
                                                          date,
                                                          'ERLDC',
                                                          key + filen[1])
                    print dbrev, rev, '*******', date, key + filen[1]
                    if (dbrev == -1 or dbrev == 0) and rev >= 0:
                        newrevision = [0, rev]
                    # elif dbrev <= rev:
                    #    break
                    elif rev >= dbrev:
                        newrevision = [rev]
                    # elif dbrev - rev > 10:
                    #     newrevision = [0, rev]
                    else:
                        newrevision = []
                    print newrevision
                    for revision in newrevision:
                        try:
                            sql_load_lib\
                                .sql_sp_load_exec(dns,
                                                  date.strftime('%Y-%m-%d'),
                                                  revision,
                                                  'ERLDC',
                                                  key + filen[1],
                                                  'RUNNING',
                                                  'UPINSPNT')
                            url = self.url + filen[0]
                            filedname = self.crawl_get_file(url,
                                                            (key, filen[1],
                                                             date, revision)
                                                            )
                            print filedname, date
                            FileRead(filedname, date, dns, filen[2])
                            # ToDO upload in db
                            sql_load_lib\
                                .sql_sp_load_exec(dns,
                                                  date.strftime('%Y-%m-%d'),
                                                  revision,
                                                  'ERLDC',
                                                  key + filen[1],
                                                  'SUCCESS',
                                                  'UPINSPNT')
                        except Exception, e:
                            print e
                            # if e.code == 404:
                            #     print e, "No such file present"
                            #     sql_load_lib.sql_sp_load_exec(dns,
                            #                                   date.strftime('%Y-%m-%d'),
                            #                                   revision,
                            #                                   'ERLDC',
                            #                                   key + filen[1],
                            #                                   'FAILED',
                            #                                   'UPINSPNT')
                            # else:
                            sql_load_lib\
                                .sql_sp_load_exec(dns,
                                                  date.strftime('%Y-%m-%d'),
                                                  revision,
                                                  'ERLDC',
                                                  key + filen[1],
                                                  'FAILED',
                                                  'UPINSPNT')

                            raise


class FileRead(object):
    """
    load data to the data base
    """
    def __init__(self, filenm, filedt, dns, tablenm):
        self.filenm = filenm
        self.workbook = xlrd.open_workbook(filenm)
        self.sheet = self.workbook.sheet_by_index(0)
        self.filedt = filedt
        self.discom = ''
        self.rev = ''
        self.fordate = ''
        self.date = ''
        self.mtime = ''
        self.time = ''
        self.hdr_flg = False
        self.data_flg = False
        self.hdr_data = []
        self.data = []
        self.k = 99999999
        self.j = 99999999
        self.regex = re.compile("Rev", re.IGNORECASE)
        self.dns = dns
        self.tablenm = tablenm
        # self.csvfilename = "F:/nrldc/Data/stat310814-Rev.No25.csv"
        self.get_discom_data()

    def check_data(self):
        """
        Check data list and sent it for setting and claering
        """
        # def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
        #     """
        #     Random Name
        #     """
        #     return ''.join(random.choice(chars) for _ in range(size))
        # print "Here", len(self.data), self.data
        if len(self.data) == 96:
            # print self.hdr_data
            # print self.data
            # print self.process_header()
            dbdata = self.process_data(self.process_header())
            # dbdata.to_csv("F:/nrldc/Data/stat170914-Rev.No0_" + ".csv")
            tup = [tuple(x) for x in dbdata.to_records(index=False)]
            # print tup[:10]
            # print self.tablenm
            sql_load_lib.sql_table_insert_exec(self.dns,
                                               self.tablenm,
                                               tup)
            self.hdr_data = []
            self.data = []

    def get_discom_data(self):
        """
        Process the sheet and segregate the data discom wise
        """
        for i in xrange(self.sheet.nrows):
            # for i in xrange(0, 127):
            for j in xrange(self.sheet.ncols):
                # print self.sheet.cell(i, j).value
                if self.sheet.cell(i, j).value == "FOR DATE :":
                    self.check_data()
                    self.hdr_flg = False
                    self.data_flg = False
                    try:
                        fordate = self.sheet.cell(i, j + 3).value
                        fordate_dt = datetime.datetime.strptime(fordate,
                                                                '%d/%m/%y')
                    except:
                        regex = re.compile("(\d{2})(\d{2})(\d{2})")
                        print self.filenm
                        fordate = ''.join(regex.findall(self.filenm)[0])
                        fordate_dt = datetime.datetime.strptime(fordate,
                                                                '%d%m%y')

                    if fordate_dt.date() != self.filedt:
                        self.fordate = self.filedt.strftime('%Y-%m-%d')
                    else:
                        self.fordate = fordate_dt.strftime('%Y-%m-%d')
                if self.sheet.cell(i, j).value == "Date :":
                    try:
                        date = self.sheet.cell(i, j + 1).value
                        try:
                            year, month, day, hour, minute, second = \
                                xlrd.xldate_as_tuple(date,
                                                     self.workbook.datemode)
                        except:
                            year = parse(date).year
                            month = parse(date).month
                            day = parse(date).day
                            hour = parse(date).hour
                            minute = parse(date).minute
                    except:
                        date = self.sheet.cell(i, j + 2).value
                        year, month, day, hour, minute, second = \
                            xlrd.xldate_as_tuple(date,
                                                 self.workbook.datemode)
                    date_dt = datetime.date(year, month, day)
                    self.date = date_dt.strftime('%Y-%m-%d')
                if self.sheet.cell(i, j).value == "TIME":
                    mtime = self.sheet.cell(i, j + 2).value
                    print mtime
                    try:
                        year, month, day, hour, minute, second = \
                            xlrd.xldate_as_tuple(mtime,
                                                 self.workbook.datemode)
                    except:
                        try:
                            hour = int(mtime[:2])
                            minute = int(mtime[3:])
                            second = 0
                        except:
                            hour = 0
                            minute = 0
                            second = 0
                        print hour, minute, second
                    mtime_dt = datetime.time(hour, minute, second)
                    self.mtime = mtime_dt.strftime('%H:%M:%S')
                    print self.mtime
                if self.regex.search(str(self.sheet.cell(i, j).value)):
                    if self.sheet.cell_type(i, j + 1)\
                            not in (xlrd.XL_CELL_EMPTY,
                                    xlrd.XL_CELL_BLANK, u''):
                        self.rev = self.sheet.cell(i, j + 1).value
                    # else:
                        # self.rev = self.sheet.cell(i, j + 2).value
                    self.discom = self.sheet.row_values(i)[1]
                    self.hdr_flg = True
                    try:
                        time = self.sheet.cell(i, j + 14).value
                        year, month, day, hour, minute, second = \
                            xlrd.xldate_as_tuple(time, self.workbook.datemode)
                        time_dt = datetime.time(hour, minute, second)
                        self.time = time_dt.strftime('%H:%M:%S')
                    except:
                        self.time = self.mtime
                    if self.mtime and self.time == '00:00:00':
                        self.time = self.mtime
                    self.k = i + 2
                    self.j = i + 9
                # print self.fordate, self.date, self.mtime, self.rev, self.time
                if self.hdr_flg and i >= self.j:
                    # print "HDR", i
                    self.hdr_flg = False
                    self.data_flg = True
                    cnt = 0
                if self.hdr_flg and i >= self.k:
                    hdr = ["Date", "Discom", "Revison", "Issue_Dt"]
                    hdr.extend(self.sheet.row_values(i))
                    self.hdr_data.append(hdr)
                    break
                    # print i, self.hdr_data
                if self.data_flg and cnt < 48:
                    # print "DATA", i, cnt, self.sheet.row_values(i)[1]
                    data_temp = [self.fordate, self.discom, self.rev,
                                 self.date + ' ' + self.time]
                    data_temp.extend(self.sheet.row_values(i))
                    self.data.append(data_temp)
                    cnt = cnt + 1
                    break
        self.check_data()

    def process_header(self):
        """
        Reprocess the header data
        """
        new_hdr = []
        mark_hdr_lst = [" "]
        regulated_flg = False
        reg_str = ''
        hdr_col_mrk = 9999
        buy_sell_flg = False
        null_count = 0
        print self.hdr_data
        self.hdr_data = self.hdr_data[(len(self.hdr_data) / 2):]
        for hdr_col in xrange(0, len(self.hdr_data[0])):
            print hdr_col
            hdr_tmp = ""
            hdr = []
            count = 0
            for hdr_row in xrange(0, len(self.hdr_data)):
                # print hdr_row, hdr_col, self.hdr_data[hdr_row][hdr_col]
                ele = str(self.hdr_data[hdr_row][hdr_col]).strip()
                # """Allow data without NULL or ''"""
                # if ele == "0.0":
                #     ele = ""
                if hdr_tmp != ele and ele != "" and ele.find('BSEB BOUNDARY'):
                    hdr_tmp = ele
                    # print mark_hdr_lst, hdr_col_mrk, ele, hdr_col, buy_sell_flg
                    # """Subsitute in BUy and Sell 0.0 with NULL ('')"""
                    if hdr_tmp == "0.0":
                        hdr_tmp = ""
                    # """Store and mark the header with SELLER"""
                    if ele.find('SELLER') > -1:
                        buy_sell_flg = True
                        hdr_col_mrk = hdr_col
                        # print mark_hdr_lst, hdr_col_mrk, ele, ele.find('SELLER')
                    # """Reset the other flags and clean REGULATION
                    # QUANTUM string"""
                    elif ele.find('REGULATED QUANTUM') > -1 or\
                            ele.find('POWER REGULATION') > -1:
                        buy_sell_flg = False
                        hdr_tmp = hdr_tmp.replace('(MW)', '').strip()
                        regulated_flg = True
                        reg_str = hdr_tmp
                    # """Add indicator for Buy and Sell"""
                    elif hdr_col_mrk < hdr_col and buy_sell_flg:
                        # print "Here", m, len(mark_hdr_lst), mark_hdr_lst[m], mark_hdr_lst
                        if len(mark_hdr_lst) > 0 \
                                and mark_hdr_lst[count] != 'BLOCK':
                            if mark_hdr_lst[count] != '':
                                hdr_tmp = mark_hdr_lst[count] + ":" + ele
                            elif ele != '' and ele != '0.0':
                                hdr_tmp = ele
                            else:
                                hdr_tmp = ''
                            count = count + 1
                    hdr.append(hdr_tmp)
            if regulated_flg and len(hdr) == 2:
                hdr.insert(0, reg_str)
            elif buy_sell_flg and hdr_col_mrk == hdr_col:
                if hdr[0] != 'SELLER':
                    hdr = hdr[1:]
                if hdr[-2] != 'BUYER' and hdr[-2] != '':
                    hdr[-2] = ''
                mark_hdr_lst = hdr
            if buy_sell_flg and len(hdr) == 0:
                # print null_count
                null_count = null_count + 1
            if null_count >= 7:
                buy_sell_flg = False
                regulated_flg = True
                reg_str = 'REGULATED QUANTUM'
            new_hdr.append(hdr)
        return new_hdr

    def process_data(self, new_hdr):
        """Flatten the headers with | as a seperator"""
        new_hdr2 = []
        for hdr in new_hdr:
            new_hdr2.append('|'.join(hdr))

        datfm = pd.DataFrame(self.data, columns=new_hdr2)
        del datfm['Tm Pt.']
        del datfm['']
        datfm.set_index(['Date', 'Discom', 'Revison', 'Issue_Dt', 'Blk'],
                        inplace=True, drop=True, append=False)
        newdf = datfm.stack(dropna=False).reset_index()
        newdf.rename(columns={'level_5': 'Station_Name', 0: 'Schedule'},
                     inplace=True)
        newdf.sort(['Date', 'Discom', 'Issue_Dt', 'Revison',
                   'Station_Name', 'Blk'], inplace=True)
        newdf.insert(1, 'State', None)
        newdf.insert(5, 'Drawl_Type', 'ISGS')
        newdf['Drawl_Type'][newdf.Station_Name.str.contains('BILAT')] = \
            'BILATERAL'
        newdf['Drawl_Type'][newdf.Station_Name.str.contains('PXI')] = 'PXIL'
        newdf['Drawl_Type'][newdf.Station_Name.str.contains('IEX')] = 'IEX'
        newdf['Drawl_Type'][newdf.Station_Name.str.contains('MTOA')] = \
            'LTOA_MTOA'
        newdf['Drawl_Type'][newdf.Station_Name.str.contains('REGULATED')] = \
            'REGULATION'
        newdf = newdf[~newdf.Station_Name.str.startswith('SELLER|BUYER|',
                                                         na=False)]
        # print newdf[1:10]
        # newdf.to_csv('F:/nrldc/Data/stat121114-Rev.No25__test.csv')
        return newdf


# def get_schedule():
#     erldc_sch = ErldcSchedule("02-APR-2015", "http://www.erldc.org", "F:/nrldc/Data/")
#     erldc_sch.get_file('F:/nrldc/config/sqldb_connection_config.txt')

# get_schedule()

def main(args):
    """
    Main Function. Based on filename call to different functions
    """
    if args.start_date is not None or args.end_date is not None:
        for date in daterange.daterange(args.start_date, args.end_date):
            erldc_sch = ErldcSchedule(date, args.url, args.dir)
            print "Here"
            erldc_sch.get_file(args.dsn)
    else:
        print "Here*****else"
        # date = datetime.datetime.now()
        if time.tzname[0] == 'IST':
            local_now = datetime.datetime.today()
        else:
            dest_tz = pytz.timezone('Asia/Kolkata')
            ts = time.time()
            utc_now = datetime.utcfromtimestamp(ts)
            local_now = utc_now.replace(tzinfo=pytz.utc).astimezone(dest_tz)
        date = local_now
        print date
        erldc_sch = ErldcSchedule(date, args.url, args.dir)
        print "Here"
        erldc_sch.get_file(args.dsn)


if __name__ == '__main__':
    ARG = argparse.ArgumentParser(description=("Fetches ERLDC data and"
                                  "Uploads Staging table data"))
    ARG.add_argument('-u', '--url', dest='url',
                     help='URL to fetch data from',
                     required=True)
    # ARG.add_argument('-D', '--discom', dest='discom',
    #                  help='State to fetch data for'
    #                  )
    ARG.add_argument('-d', '--dir', dest='dir',
                     help='Directory to save data to',
                     required=True)
    # ARG.add_argument('-f', '--file', dest='filenm',
    #                  help='Local file name to save data to.\
    #                  The name will be appended with date for which crawling\
    #                  is done', required=True)
    ARG.add_argument('-b', '--strdt', dest='start_date',
                     help=('Start of Date to crawl the data.'
                           'Optional default is todays date'))
    ARG.add_argument('-e', '--enddt', dest='end_date',
                     help=('Start of Date to crawl the data.'
                           'Optional default is todays date'))
    ARG.add_argument('-m', '--dbdsnconfig', dest='dsn',
                     help='Full path and the file name of the db config',
                     required=True)
    # ARG.add_argument('-t', '--tabnm', dest='tabname',
    #                  help='Name of the table to be loaded',
    #                  required=True)
    main(ARG.parse_args())

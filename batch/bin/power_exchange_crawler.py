import mechanize
import logging
import time
import pytz
from datetime import datetime, timedelta
import argparse
try:
    from . import sql_load_lib
except:
    import sql_load_lib
import os
import re
import xlrd
import glob
from bs4 import BeautifulSoup
import urllib


BROWSER_HEADER = [('User-agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36')]

                   
logging.basicConfig(level=logging.INFO)
logging = logging.getLogger('power_exchnage_crawler')

def energy_crawler(exchange, url, FileDir, FileNm, StrDt):
    """
    simulating the browser values for the Excel download link
    """
    logging.info("Starting %s", StrDt)
    # Browser
    br = mechanize.Browser()
    cj = mechanize.CookieJar()
    br.set_cookiejar(cj)
    # Browser options
    br.set_handle_equiv(True)
    br.set_handle_gzip(True)
    br.set_handle_redirect(True)
    br.set_handle_referer(True)
    br.set_handle_robots(False)

    # Follows refresh 0 but not hangs on refresh > 0
    br.set_handle_refresh(mechanize._http.HTTPRefreshProcessor(), max_time=1)

    # Want debugging messages?
    # br.set_debug_http(True)
    # br.set_debug_redirects(True)
    # br.set_debug_responses(True)

    # User-Agent (this is cheating, ok?)
    br.addheaders = BROWSER_HEADER
    # response = br.open("http://www.iexindia.com/Reports/AreaPrice.aspx")
    response = br.open(url)
    logging.info("Opened site %s", url)
    if exchange == 'IEX':
        br.select_form("aspnetForm")
        br.set_all_readonly(False)
        # br.find_control("btnUpdateReport").disabled = True
        if datetime.strptime(StrDt, '%d/%m/%Y').date() < \
           datetime.strptime('01-04-2012', '%d-%m-%Y').date():
            logging.info("Setting Exception Params COmpleted")
            br["ctl00$ContentPlaceHolder1$ddlInterval"] = ["15-Minute Block"]
            br["__EVENTTARGET"] = 'ctl00$ContentPlaceHolder1$ddlInterval'
            br["__EVENTARGUMENT"] = ''
            br.submit()
            br.select_form("aspnetForm")
            br.set_all_readonly(False)
        # br["ctl00$ContentPlaceHolder1$ddlPeriod"] = ["-1"]
        # br["ctl00$InnerContent$ddlInterval"] = ["1"]
        # # br["ctl00$ContentPlaceHolder1$tbStartDate"] = StrDt
        # br["ctl00$InnerContent$ddlPeriod"] = ["SR"]
        # br["ctl00$InnerContent$calFromDate$txt_Date"] = StrDt
        # # br["ctl00$ContentPlaceHolder1$tbEndDate"] = StrDt
        # br["ctl00$InnerContent$calToDate$txt_Date"] = StrDt
        # # br['ctl00$InnerContent$btnUpdateReport'] = 'Update Report'
        # br['ctl00$InnerContent$reportViewer$ctl10'] = 'ltr'
        # br['ctl00$InnerContent$reportViewer$ctl11'] = 'standards'
        # br['ctl00$InnerContent$reportViewer$ctl05$ctl00$CurrentPage'] = 1
        # br['ctl00$InnerContent$reportViewer$ctl09$ScrollPosition'] = '0 0'
        # # del br['ctl00$InnerContent$calFromDate$clickme.x']
        # # del br['ctl00$InnerContent$calFromDate$clickme.y']
        # #response = br.click("btnUpdateReport")
        # response = br.submit()
        parameters = {
            "__EVENTTARGET" : br["__EVENTTARGET"],
            "__EVENTARGUMENT" : br["__EVENTARGUMENT"],
            "__LASTFOCUS" : br["__LASTFOCUS"],
            "__VIEWSTATE" : br["__VIEWSTATE"],
            "__VIEWSTATEGENERATOR" : br["__VIEWSTATEGENERATOR"],
            "__VIEWSTATEENCRYPTED" : br["__VIEWSTATEENCRYPTED"],
            "__EVENTVALIDATION" : br["__EVENTVALIDATION"],
            "ctl00$InnerContent$ddlInterval" : "1",
            "ctl00$InnerContent$ddlPeriod" : "SR",
            "ctl00$InnerContent$calFromDate$txt_Date" : StrDt,
            "ctl00$InnerContent$calToDate$txt_Date" : StrDt,
            "ctl00$InnerContent$cbArea" : br["ctl00$InnerContent$cbArea"][0],
            "ctl00$InnerContent$cblArealist$0" : br["ctl00$InnerContent$cblArealist$0"][0],
            "ctl00$InnerContent$cblArealist$2" : br["ctl00$InnerContent$cblArealist$2"][0],
            "ctl00$InnerContent$cblArealist$4" : br["ctl00$InnerContent$cblArealist$4"][0],
            "ctl00$InnerContent$cblArealist$6" : br["ctl00$InnerContent$cblArealist$6"][0],
            "ctl00$InnerContent$cblArealist$8" : br["ctl00$InnerContent$cblArealist$8"][0],
            "ctl00$InnerContent$cblArealist$10" : br["ctl00$InnerContent$cblArealist$10"][0],
            "ctl00$InnerContent$cblArealist$12" : br["ctl00$InnerContent$cblArealist$12"][0],
            "ctl00$InnerContent$cblArealist$1" : br["ctl00$InnerContent$cblArealist$1"][0],
            "ctl00$InnerContent$cblArealist$3" : br["ctl00$InnerContent$cblArealist$3"][0],
            "ctl00$InnerContent$cblArealist$5" : br["ctl00$InnerContent$cblArealist$5"][0],
            "ctl00$InnerContent$cblArealist$7" : br["ctl00$InnerContent$cblArealist$7"][0],
            "ctl00$InnerContent$cblArealist$9" : br["ctl00$InnerContent$cblArealist$9"][0],
            "ctl00$InnerContent$cblArealist$11" : br["ctl00$InnerContent$cblArealist$11"][0],
            "ctl00$InnerContent$cblArealist$13" : br["ctl00$InnerContent$cblArealist$13"][0],
            "ctl00$InnerContent$btnUpdateReport" : br["ctl00$InnerContent$btnUpdateReport"],
            "ctl00$InnerContent$reportViewer$ctl03$ctl00" : br["ctl00$InnerContent$reportViewer$ctl03$ctl00"],
            "ctl00$InnerContent$reportViewer$ctl03$ctl01" : br["ctl00$InnerContent$reportViewer$ctl03$ctl01"],
            "ctl00$InnerContent$reportViewer$ctl10" : br["ctl00$InnerContent$reportViewer$ctl10"],
            "ctl00$InnerContent$reportViewer$ctl11" : br["ctl00$InnerContent$reportViewer$ctl11"],
            "ctl00$InnerContent$reportViewer$AsyncWait$HiddenCancelField" : br["ctl00$InnerContent$reportViewer$AsyncWait$HiddenCancelField"],
            "ctl00$InnerContent$reportViewer$ToggleParam$store" : br["ctl00$InnerContent$reportViewer$ToggleParam$store"],
            "ctl00$InnerContent$reportViewer$ToggleParam$collapse" : br["ctl00$InnerContent$reportViewer$ToggleParam$collapse"],
            "ctl00$InnerContent$reportViewer$ctl05$ctl00$CurrentPage" : br["ctl00$InnerContent$reportViewer$ctl05$ctl00$CurrentPage"],
            "ctl00$InnerContent$reportViewer$ctl05$ctl03$ctl00" : br["ctl00$InnerContent$reportViewer$ctl05$ctl03$ctl00"],
            "ctl00$InnerContent$reportViewer$ctl08$ClientClickedId" : br["ctl00$InnerContent$reportViewer$ctl08$ClientClickedId"],
            "ctl00$InnerContent$reportViewer$ctl07$store" : br["ctl00$InnerContent$reportViewer$ctl07$store"],
            "ctl00$InnerContent$reportViewer$ctl07$collapse" : br["ctl00$InnerContent$reportViewer$ctl07$collapse"],
            "ctl00$InnerContent$reportViewer$ctl09$VisibilityState$ctl00" : br["ctl00$InnerContent$reportViewer$ctl09$VisibilityState$ctl00"],
            "ctl00$InnerContent$reportViewer$ctl09$ScrollPosition" : br["ctl00$InnerContent$reportViewer$ctl09$ScrollPosition"],
            "ctl00$InnerContent$reportViewer$ctl09$ReportControl$ctl02" : br["ctl00$InnerContent$reportViewer$ctl09$ReportControl$ctl02"],
            "ctl00$InnerContent$reportViewer$ctl09$ReportControl$ctl03" : br["ctl00$InnerContent$reportViewer$ctl09$ReportControl$ctl03"],
            "ctl00$InnerContent$reportViewer$ctl09$ReportControl$ctl04" : br["ctl00$InnerContent$reportViewer$ctl09$ReportControl$ctl04"]}
        # logging.info("Url Encoding", parameters)
        data = urllib.parse.urlencode(parameters)
        response = br.open(url, data)
        html = response.read().decode('utf-8')    
        logging.info("HTML Read")
        # match = re.search(r'/Reserved\.ReportViewerWebControl\.axd\?Mode=true\&ReportID=[A-Za-z0-9]*\&ControlID=[A-Za-z0-9-]*\&Culture=[0-9]*\&UICulture=[0-9]*\&ReportStack=1\&OpType=Export\&FileName=[Area|Market]+[Price|Volume]+_[A-Za-z]+\&ContentDisposition=AlwaysInline\&Format=', html)
        # match = re.search(r'/Reserved\.ReportViewerWebControl\.axd\?Culture=[0-9]*\&CultureOverrides=True\&UICulture=[0-9]*\&UICultureOverrides=True\&ReportStack=1\&ControlID=[A-Za-z0-9]*\&Mode=true\&OpType=ReportImage\&IterationId=[A-Za-z0-9]*\&StreamID=[A-Za-z0-9-]*', html)
        # match2 = re.search(r'/Reserved\.ReportViewerWebControl\.axd\?OpType=SessionKeepAlive\\u0026ControlID=[A-Za-z0-9]*\\u0026Mode=true', html)
        # response = br.open(match.group())
        # print match2.group(), re.sub('\\\\u0026', '&', match2.group())
        # response = br.open(re.sub('\\\\u0026', '&', match2.group()))
        # html = response.read()
        match3 = re.search(r'/Reserved\.ReportViewerWebControl\.axd\?Culture=[0-9]*\\u0026CultureOverrides=True\\u0026UICulture=[0-9-]*\\u0026UICultureOverrides=True\\u0026ReportStack=1\\u0026ControlID=[A-Za-z0-9-]*\\u0026Mode=[A-Za-z]*\\u0026OpType=Export\\u0026FileName=[A-Za-z]*\\u0026ContentDisposition=[A-Za-z]*\\u0026Format=', html)
        url2 = re.sub('\\\\u0026', '&', match3.group()) + 'EXCELOPENXML'
    elif exchange == 'PXIL':
        response.set_data(BeautifulSoup(response.get_data()).prettify())
        br.set_response(response)
        br.select_form(nr=0)
        br.set_all_readonly(False)
        # br["ctl00$ContentPlaceHolder4$DrpPeriods"] = ["Select Range"]
        # br["__EVENTTARGET"] = 'ctl00$ContentPlaceHolder4$DrpPeriods'
        # br["__EVENTARGUMENT"] = ''
        # Market Volume Profile Report
        br["ctl00$ContentPlaceHolder4$txtFromDate"] = StrDt
        try:
            br["ctl00$ContentPlaceHolder4$txtToDate"] = StrDt
        except Exception as e:
            logging.error("Error: %s", str(e))
        # when using Date Range
        # br["ctl00$ContentPlaceHolder4$txtstartDate"] = StrDt
        # br["ctl00$ContentPlaceHolder4$txtenddate"] = StrDt
        # response = br.click("btnUpdateReport")
        response = br.submit("ctl00$ContentPlaceHolder4$btnReport")
        html = response.read()
        # with open('output.txt', 'w') as f:
        #     f.write(html)
        match = re.search(r'/PXILReport/Reserved\.ReportViewerWebControl\.axd\?Mode=true\&ReportID=[A-Za-z0-9]*\&ControlID=[A-Za-z0-9-]*\&Culture=[0-9]*\&UICulture=[0-9]*\&ReportStack=1\&OpType=Export\&FileName=[rptMCV|dasmvpreport|rptMCP]+\&ContentDisposition=AlwaysAttachment\&Format=', html)
        url2 = match.group() + 'Excel'
    else:
        raise Exception('Missing or Invalid arguments being passed.')
        return
    logging.info('Url: %s', url2)
    response1 = br.open(url2)
    # f = open('C:/Users/Admin/Downloads/IEX.xls', 'wb')
    filename = FileDir + FileNm + "_" + StrDt.replace('/', '-') + ".xls"
    if os.path.exists(filename):
        os.remove(filename)
    file_handle = open(filename, 'wb')
    file_handle.write(response1.read())
    file_handle.close()
    logging.info('Processed for Date: %s', StrDt)
    return filename


def daterange(start_date=None, end_date=None):
    """
    itertor over a date range
    """
    if time.tzname[0] == 'IST':
        local_now = datetime.today()
    else:
        dest_tz = pytz.timezone('Asia/Kolkata')
        ts = time.time()
        utc_now = datetime.utcfromtimestamp(ts)
        local_now = utc_now.replace(tzinfo=pytz.utc).astimezone(dest_tz)

    if start_date is None:
        start_date = local_now + timedelta(0)
    else:
        start_date = datetime.strptime(start_date, '%d-%m-%Y')
    if end_date is None:
        end_date = local_now + timedelta(1)
    else:
        end_date = datetime.strptime(end_date, '%d-%m-%Y')
    if start_date <= end_date:
        for n in range((end_date - start_date).days + 1):
            yield start_date + timedelta(n)
    else:
        for n in range((start_date - end_date).days + 1):
            yield start_date - timedelta(n)
    return


def exchange_xls_to_list(filename, exchange):
    """
    Parses the pxil xls file and returns a list
    """
    data = []
    for files in glob.glob(filename):
        logging.info("Processing file to list started: %s", files)
        wb2 = xlrd.open_workbook(files)
        sheet = wb2.sheet_by_index(0)
        if exchange == 'PXIL':
            date = datetime.strptime(sheet.cell(rowx=5, colx=1).value,
                                     '%d %b %Y').strftime("%Y-%m-%d")
            i = 0
            for counter in range(5, 101):
                singlerow = []
                singlerow.append(date)
                singlerow.append(i + 1)
                for cell in sheet.row_values(counter, start_colx=2,
                                             end_colx=16):
                    if cell == '--' or cell == ' - ' or cell == '-':
                        cell = None
                    singlerow.append(cell)
                if exchange == 'PXIL':
                    singlerow.append(None)
                singlerow.append(exchange)
                i = i + 1
                data.append(singlerow)
        elif exchange == 'IEX':
            date = datetime.strptime(sheet.cell(rowx=4, colx=0).value,
                                     '%d-%m-%Y').strftime("%Y-%m-%d")
            i = 0
            for counter in range(4, 100):
                singlerow = []
                singlerow.append(date)
                singlerow.append(i + 1)
                row = sheet.row_values(counter, start_colx=2, end_colx=18)
                # del row[2]
                for cell in row:
                    if cell == '--' or cell == ' - ' or cell == '-':
                        cell = None
                    singlerow.append(cell)
                # print len(singlerow)
                if (len(singlerow) == 15):
                    singlerow.append(None)
                singlerow.append(exchange)
                i = i + 1
                data.append(singlerow)
        else:
            return None
        # i = 0
        # for counter in range(5, 101):
        #     singlerow = []
        #     singlerow.append(date)
        #     singlerow.append(i + 1)
        #     for cell in sheet.row_values(counter, start_colx=2, end_colx=16):
        #         if cell == '--' or cell == ' - ':
        #             cell = None
        #         singlerow.append(cell)
        #     if exchange == 'PXIL':
        #         singlerow.append(None)
        #     singlerow.append(exchange)
        #     i = i + 1
        #     data.append(singlerow)
    return data


def main(args):
    for date in daterange(args.start_date, args.end_date):
        try:
            filename = energy_crawler(args.exchange, args.url, args.dir,
                                      args.filenm, date.strftime('%d/%m/%Y'))
            exdata = exchange_xls_to_list(filename, args.exchange)
            print('exdata', exdata)
            sql_load_lib.sql_table_insert_exec(args.dsn, args.tabname, exdata)
        except Exception as e:
            logging.error("Error: %s", str(e))


if __name__ == '__main__':
    ARG = argparse.ArgumentParser(description=("Fetches IEX/PXIL data and"
                                  "Uploads Staging table data"))
    ARG.add_argument('-u', '--url', dest='url',
                     help='URL to fetch data from',
                     required=True)
    ARG.add_argument('-x', '--ecx', dest='exchange',
                     help='Exchange to fetch data for',
                     required=True)
    ARG.add_argument('-d', '--dir', dest='dir',
                     help='Directory to save data to',
                     required=True)
    ARG.add_argument('-f', '--file', dest='filenm',
                     help=('Local file name to save data to.'
                           'The name will be appended with date for which'
                           'crawling is done'), required=True)
    ARG.add_argument('-b', '--strdt', dest='start_date',
                     help=('Start of Date to crawl the data.'
                           'Optional default is yesterdays date')
                     # ,required=True
                     )
    ARG.add_argument('-e', '--enddt', dest='end_date',
                     help=('Start of Date to crawl the data.'
                           'Optional default is todays date'))
    ARG.add_argument('-m', '--dbdsnconfig', dest='dsn',
                     help='Full path and the file name of the db config',
                     required=True)
    ARG.add_argument('-t', '--tabnm', dest='tabname',
                     help='Name of the table to be loaded',
                     required=True)
    main(ARG.parse_args())
#python power_exchange_crawlerv2.py -u https://www.iexindia.com/marketdata/areaprice.aspx -x IEX -d ../data/exchange/ -f IEX_AreaPrice -b 01-01-2020 -e 01-01-2020 -m ../config/sqldb_connection_config.txt -t exchange_areaprice_stg
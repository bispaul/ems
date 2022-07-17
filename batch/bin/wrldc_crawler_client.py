"""
Crawls the WRLDC site and uploads the data to a database.
"""
import mechanize
from BeautifulSoup import BeautifulSoup
import csv
import re
import os
from datetime import datetime
import argparse
import pandas
import locale
import html_tab_parse
import sql_load_lib
import nrldc_crawler_v4
import daterange


class Crawler(object):
    """
    Crawls the SLDC site and downloads the files.
    """
    def __init__(self, url, browser_header=None):
        if browser_header is None:
            self.browser_header = [('User-agent',
                                    'Mozilla/5.0 (Windows NT 6.1; WOW64) \
                                    AppleWebKit/537.4 (KHTML, like Gecko) \
                                    Chrome/22.0.1229.79 Safari/537.4')]
        else:
            self.browser_header = browser_header
        self.browser = mechanize.Browser()
        self.url = url

    def get_url(self, url):
        """
        Gets the page for the url
        """
        #self.browser.set_handle_robots(False)
        #self.browser.set_handle_refresh(mechanize._http.HTTPRefreshProcessor(),
        #                                max_time=1)
        # Want debugging messages?
        #self.browser.set_debug_http(True)
        #self.browser.set_debug_redirects(True)
        #self.browser.set_debug_responses(True)
        self.browser.addheaders = self.browser_header
        return self.browser.open(url)

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

    def set_date(self, datestr):
        """
        Changes the date and refreshes the web page
        """
        response = self.get_url(self.url)
        self.prettyfy_html(response, False)
        self.browser["date_txt"] = datestr
        return self.browser.submit()

    def get_selection_list(self, response, controlname):
        """
        Gets the dropdown list values of the dropdown controls for WRLDC
        Dropdown Controls: CONS and Revision
        """
        self.prettyfy_html(response, False)
        print self.browser["date_txt"]
        control = self.browser.form.find_control(controlname)
        if control.type == "select":
            return [label.text for item in control.items
                    for label in item.get_labels()]

    def set_selection_list(self, response, controltuplelist):
        """
        Set the dropdowns for WRLDC
        """
        self.prettyfy_html(response, False)
        #print '***', controltuplelist
        for controlname, value in controltuplelist:
            #print controlname, value
            self.browser[controlname] = value
        return self.browser.submit()


class HtmlData(object):
    """
    Html Table Data Object
    """
    def __init__(self, htmlobj, crawltype, conrevdict, dirname):
        self.htmlpage = htmlobj.read()
        self.revison = conrevdict.get("rev")
        self.cons = conrevdict.get("cons")
        print self.revison, self.cons
        self.type = crawltype
        self.data = []
        self.dirname = dirname
        self.xpathdict = {"WRLDC_ISGS": {"DATE": "//*[@id='table3']",
                                         "DATA": "//*[@id='table4']",
                                         "TIME": "//*[@id='table2']"},
                          "WRLDC_Declared_Capability":
                                        {"DATE": "//*[@id='nload']/table[1]",
                                         "DATA": "//*[@id='nload']/table[2]",
                                         "TIME": "//*[@id='nload']"},
                          "WRLDC_Entitlement":
                                        {"DATE": "//*[@id='nload']/table[2]",
                                         "DATA": "//*[@id='nload']/table[3]",
                                         "TIME": "//*[@id='nload']"},
                          "WRLDC_Injection":
                                        {"DATE": "//*[@id='table3']",
                                         "DATA": "//*[@id='table4']",
                                         "TIME": "//*[@id='table2']"},
                          "STOA_Link": {"DATE": "//*[@id='nload']/table[2]",
                                        "DATA": "//*[@id='nload']/table[3]"}}
        self.datatable = html_tab_parse.parse(self.htmlpage,
                                              self.xpathdict[self.type]['DATA']
                                              )
        self.datestr = self.parse_html_date(self.xpathdict[self.type]['DATE'])
        self.timestr = self.parse_html_time(self.xpathdict[self.type]['TIME'])
        self.dirfilename = self.get_filename()

    def parse_html_date(self, xpath):
        """
        Parse the date field
        """
        rawrowdata = html_tab_parse.parse(self.htmlpage, xpath)
        #print rawrowdata, xpath
        datepattern = re.compile(r'\d{2}-\w{3}-\d{4}')
        dategenerator = (datepattern.findall(ele)
                         for iterlist in rawrowdata[0]
                         for ele in iterlist)
        for datelist in dategenerator:
            #print datelist
            if datelist:
                #print datelist[0]
                return datelist[0]

    def parse_html_time(self, xpath):
        """
        Parse the time field
        """
        rawrowdata = html_tab_parse.parse(self.htmlpage, xpath)
        #print rawrowdata, xpath
        timepattern = re.compile(r'\d{2}:\d{2}')
        datepattern = re.compile(r'\d{1,2}\/\d{1,2}\/\d{4}')
        generator = (ele for iterlist in rawrowdata[0]
                     for ele in iterlist)
        for ele in generator:
            if timepattern.findall(ele):
                time = timepattern.findall(ele)
            if datepattern.findall(ele):
                date = datepattern.findall(ele)
        #print date[0] + ' ' + time[0]
        return date[0] + ' ' + time[0]

    def add_datenrev_col_datatable(self):
        """
        Add the Date and Revison Column and Data
        """
        #print self.datatable
        firstrow = True
        locale.setlocale(locale.LC_ALL, '')
        #print "TEST", self.datatable
        for rowval in (row for table in self.datatable for row in table):
            #print type(rowval[0]), rowval[0][:4], firstrow, rowval
            #print "TEST2", rowval
            if rowval and (rowval[0][:4] == 'Note' or
                           rowval[0].find('NO DATA') == 1) and firstrow:
                print "No Data to save"
                raise Exception("No Data to save")
                #break
            elif rowval:
                if rowval[0].strip() == 'BLOCK':
                    if self.cons is not None:
                        self.data.append(['DATE'] + ['DISCOM'] + ['ISSUE_DT']
                                         + ['REVISON']
                                         + [row.strip() for row in rowval])
                    else:
                        self.data.append(['DATE'] + ['ISSUE_DT'] + ['REVISON']
                                         + [row.strip() for row in rowval])
                else:
                    if self.cons is not None:
                        self.data.append([self.datestr] + [self.cons]
                                         + [self.timestr] + [self.revison]
                                         + [locale.atof(row) for row in rowval]
                                         )
                    else:
                        self.data.append([self.datestr] + [self.timestr]
                                         + [self.revison]
                                         + [locale.atof(row) for row in rowval]
                                         )
                    #After 96 blocks no need to append any data
                    if int(rowval[0]) == 96:
                        break
            firstrow = False

    def save_datatable_as_csv(self):
        """
        Save the datatable as a CSV file
        """
        csvwriter = csv.writer(open(self.dirfilename, "wb"),
                               delimiter=',',
                               quotechar='"',
                               quoting=csv.QUOTE_MINIMAL)
        for rowval in (row for row in self.data):
            csvwriter.writerow(rowval)

    def file_to_tuple(self):
        """
        Creates tuple from a CSV file
        """
        if self.type == 'WRLDC_ISGS' or self.type == 'WRLDC_Entitlement':
            dataframe = pandas.read_csv(self.dirfilename)
            dataframe.set_index(['DATE', 'DISCOM', 'ISSUE_DT', 'REVISON',
                                 'BLOCK'],
                                inplace=True, drop=True, append=False)
            newdf = dataframe.stack(dropna=False).reset_index()
            newdf.rename(columns={'level_5': 'Station_Name', 0: 'Schedule'},
                         inplace=True)
            newdf.sort(['DATE', 'DISCOM', 'ISSUE_DT', 'REVISON',
                       'Station_Name', 'BLOCK'],
                       inplace=True)
            newdf.insert(1, 'State', None)
            if self.type == 'WRLDC_ISGS':
                newdf.insert(5, 'Drawl_Type', 'ISGS')
            newdf['Station_Name'] = newdf['Station_Name'].map(str.strip)
            return [tuple(x) for x in newdf.to_records(index=False)]
        if self.type == 'WRLDC_Declared_Capability' \
                or self.type == 'WRLDC_Injection':
            dataframe = pandas.read_csv(self.dirfilename)
            dataframe.set_index(['DATE', 'ISSUE_DT', 'REVISON',
                                 'BLOCK'],
                                inplace=True, drop=True, append=False)
            newdf = dataframe.stack(dropna=False).reset_index()
            newdf.rename(columns={'level_4': 'Station_Name', 0: 'Schedule'},
                         inplace=True)
            newdf.sort(['DATE', 'ISSUE_DT', 'REVISON',
                       'Station_Name', 'BLOCK'],
                       inplace=True)
            newdf['Station_Name'] = newdf['Station_Name'].map(str.strip)
            return [tuple(x) for x in newdf.to_records(index=False)]

    def get_filename(self):
        """
        Generate File Name
        """
        if self.cons is not None:
            filename = "{0}_{1}_{2}_{3}_Rev{4}.csv".format('WRLDC',
                                                           self.type,
                                                           self.cons,
                                                           self.datestr,
                                                           str(self.revison))
        else:
            filename = "{0}_{1}_{2}_Rev{3}.csv".format('WRLDC',
                                                       self.type,
                                                       self.datestr,
                                                       str(self.revison))
        return os.path.join(self.dirname, filename)


def wrldc_schedule(url=None, date=None, directory=None, ftype=None, dns=None):
    """
    Get the WRLDC schedule for all the discoms
    """
    #date = datetime.strptime(date, '%d-%b-%Y')
    wrldc_crawler = Crawler(url)
    res = wrldc_crawler.set_date(date.strftime('%d-%b-%Y'))
    revlist = wrldc_crawler.get_selection_list(res, "scl1")[-1]
    conslist = wrldc_crawler.get_selection_list(res, "scl2")
    for attribval in ([("scl2", cons), ("scl1", revlist)]
                      for cons in conslist[1:]):
        #print attribval
        dbrev = nrldc_crawler_v4.db_rev_check(dns,
                                              date,
                                              attribval[0][1],
                                              ftype)
        print dbrev, attribval[1][1]
        if (dbrev == -1 or dbrev == 0) and int(attribval[1][1]) >= 0:
            newrevision = [0, int(attribval[1][1])]
        elif dbrev <= int(attribval[1][1]):
            break
        elif int(attribval[1][1]) >= dbrev:
            newrevision = [int(attribval[1][1])]
        else:
            newrevision = []
        print newrevision
        for revision in newrevision:
            try:
                sql_load_lib.sql_sp_load_exec(dns,
                                              date.strftime('%Y-%m-%d'),
                                              revision,
                                              attribval[0][1],
                                              ftype,
                                              'RUNNING',
                                              'UPINSPNT')
                newattribval = [(attribval[0][0], [attribval[0][1]]),
                                (attribval[1][0], [str(revision)]),
                                ("text2", "2")]
                res = wrldc_crawler.set_selection_list(res, newattribval)
                conrevdict = {"cons": attribval[0][1], "rev": revision}
                htmldata = HtmlData(res, ftype, conrevdict, directory)
                htmldata.add_datenrev_col_datatable()
                htmldata.save_datatable_as_csv()
                tablenm = 'wrldc_state_drawl_schedule_stg'
                sql_load_lib.sql_table_insert_exec(dns,
                                                   tablenm,
                                                   htmldata.file_to_tuple())
                sql_load_lib.sql_sp_load_exec(dns,
                                              date.strftime('%Y-%m-%d'),
                                              revision,
                                              attribval[0][1],
                                              ftype,
                                              'SUCCESS',
                                              'UPINSPNT')
            except Exception, e:
                if e.args[0] == 'No Data to save':
                    sql_load_lib.sql_sp_load_exec(dns,
                                                  date.strftime('%Y-%m-%d'),
                                                  revision,
                                                  attribval[0][1],
                                                  ftype,
                                                  'SUCCESS',
                                                  'UPINSPNT')
                else:
                    sql_load_lib.sql_sp_load_exec(dns,
                                                  date.strftime('%Y-%m-%d'),
                                                  revision,
                                                  attribval[0][1],
                                                  ftype,
                                                  'FAILED',
                                                  'UPINSPNT')
                    raise
        #break


def wrldc_dc(url=None, date=None, directory=None, ftype=None, dns=None):
    """
    Gets the declared capability
    """
    wrldc_crawler = Crawler(url)
    res = wrldc_crawler.set_date(date.strftime('%d-%b-%Y'))
    revlist = wrldc_crawler.get_selection_list(res, "scl1")[-1]
    for attribval in ([("scl1", revlist)]):
        dbrev = nrldc_crawler_v4.db_rev_check(dns,
                                              date,
                                              'WRLDC',
                                              ftype)
        print dbrev, attribval
        if (dbrev == -1 or dbrev == 0) and int(attribval[1]) >= 0:
            newrevision = [0, int(attribval[1])]
        elif dbrev <= int(attribval[1]):
            break
        elif int(attribval[1]) >= dbrev:
            newrevision = [int(attribval[1])]
        else:
            newrevision = []
        print newrevision
        for revision in newrevision:
            try:
                sql_load_lib.sql_sp_load_exec(dns,
                                              date.strftime('%Y-%m-%d'),
                                              revision,
                                              'WRLDC',
                                              ftype,
                                              'RUNNING',
                                              'UPINSPNT')
                newattribval = [(attribval[0], [str(revision)]),
                                ("text2", "2")]
                print newattribval
                res = wrldc_crawler.set_selection_list(res, newattribval)
                conrevdict = {"rev": revision}
                htmldata = HtmlData(res, ftype, conrevdict, directory)
                htmldata.add_datenrev_col_datatable()
                htmldata.save_datatable_as_csv()
                tablenm = 'wrldc_declared_capability_stg'
                sql_load_lib.sql_table_insert_exec(dns,
                                                   tablenm,
                                                   htmldata.file_to_tuple())
                sql_load_lib.sql_sp_load_exec(dns,
                                              date.strftime('%Y-%m-%d'),
                                              revision,
                                              'WRLDC',
                                              ftype,
                                              'SUCCESS',
                                              'UPINSPNT')
            except Exception, e:
                if e.args[0] == 'No Data to save':
                    sql_load_lib.sql_sp_load_exec(dns,
                                                  date.strftime('%Y-%m-%d'),
                                                  revision,
                                                  'WRLDC',
                                                  ftype,
                                                  'FAILED',
                                                  'UPINSPNT')
                else:
                    sql_load_lib.sql_sp_load_exec(dns,
                                                  date.strftime('%Y-%m-%d'),
                                                  revision,
                                                  'WRLDC',
                                                  ftype,
                                                  'SUCCESS',
                                                  'UPINSPNT')
                    raise


def wrldc_entitlement(url=None, date=None, directory=None, ftype=None, dns=None):
    """
    Get the Entitlement.
    """
    wrldc_crawler = Crawler(url)
    res = wrldc_crawler.set_date(date.strftime('%d-%b-%Y'))
    revlist = wrldc_crawler.get_selection_list(res, "scl1")[-1]
    conslist = wrldc_crawler.get_selection_list(res, "scl2")
    for attribval in ([("scl2", cons), ("scl1", revlist)]
                      for cons in conslist[1:]):
        dbrev = nrldc_crawler_v4.db_rev_check(dns,
                                              date,
                                              attribval[0][1],
                                              ftype)
        print dbrev, attribval[1][1]
        if (dbrev == -1 or dbrev == 0) and int(attribval[1][1]) >= 0:
            newrevision = [0, int(attribval[1][1])]
        elif dbrev <= int(attribval[1][1]):
            break
        elif int(attribval[1][1]) >= dbrev:
            newrevision = [int(attribval[1][1])]
        else:
            newrevision = []
        print newrevision
        for revision in newrevision:
            try:
                sql_load_lib.sql_sp_load_exec(dns,
                                              date.strftime('%Y-%m-%d'),
                                              revision,
                                              attribval[0][1],
                                              ftype,
                                              'RUNNING',
                                              'UPINSPNT')
                newattribval = [(attribval[0][0], [attribval[0][1]]),
                                (attribval[1][0], [str(revision)]),
                                ("text2", "2")]
                res = wrldc_crawler.set_selection_list(res, newattribval)
                conrevdict = {"cons": attribval[0][1], "rev": revision}
                htmldata = HtmlData(res, ftype, conrevdict, directory)
                htmldata.add_datenrev_col_datatable()
                htmldata.save_datatable_as_csv()
                tablenm = 'wrldc_entitlements_stg'
                sql_load_lib.sql_table_insert_exec(dns,
                                                   tablenm,
                                                   htmldata.file_to_tuple())
                sql_load_lib.sql_sp_load_exec(dns,
                                              date.strftime('%Y-%m-%d'),
                                              revision,
                                              attribval[0][1],
                                              ftype,
                                              'SUCCESS',
                                              'UPINSPNT')
            except Exception, e:
                if e.args[0] == 'No Data to save':
                    sql_load_lib.sql_sp_load_exec(dns,
                                                  date.strftime('%Y-%m-%d'),
                                                  revision,
                                                  attribval[0][1],
                                                  ftype,
                                                  'SUCCESS',
                                                  'UPINSPNT')
                else:
                    sql_load_lib.sql_sp_load_exec(dns,
                                                  date.strftime('%Y-%m-%d'),
                                                  revision,
                                                  attribval[0][1],
                                                  ftype,
                                                  'FAILED',
                                                  'UPINSPNT')
                    raise
        #break


def wrldc_injection(url=None, date=None, directory=None, ftype=None, dns=None):
    """
    Gets the Injection schedule for WRLDC
    """
    wrldc_crawler = Crawler(url)
    res = wrldc_crawler.set_date(date.strftime('%d-%b-%Y'))
    revlist = wrldc_crawler.get_selection_list(res, "scl1")[-1]
    for attribval in ([("scl1", revlist)]):
        dbrev = nrldc_crawler_v4.db_rev_check(dns,
                                              date,
                                              'WRLDC',
                                              ftype)
        print dbrev, attribval
        if (dbrev == -1 or dbrev == 0) and int(attribval[1]) >= 0:
            newrevision = [0, int(attribval[1])]
        elif dbrev <= int(attribval[1]):
            break
        elif int(attribval[1]) >= dbrev:
            newrevision = [int(attribval[1])]
        else:
            newrevision = []
        print newrevision
        for revision in newrevision:
            try:
                sql_load_lib.sql_sp_load_exec(dns,
                                              date.strftime('%Y-%m-%d'),
                                              revision,
                                              'WRLDC',
                                              ftype,
                                              'RUNNING',
                                              'UPINSPNT')
                newattribval = [(attribval[0], [str(revision)]),
                                ("text2", "2")]
                print newattribval
                res = wrldc_crawler.set_selection_list(res, newattribval)
                conrevdict = {"rev": revision}
                htmldata = HtmlData(res, ftype, conrevdict, directory)
                htmldata.add_datenrev_col_datatable()
                htmldata.save_datatable_as_csv()
                tablenm = 'wrldc_isgs_injsch_schedule_stg'
                print htmldata.file_to_tuple()
                sql_load_lib.sql_table_insert_exec(dns,
                                                   tablenm,
                                                   htmldata.file_to_tuple())
                sql_load_lib.sql_sp_load_exec(dns,
                                              date.strftime('%Y-%m-%d'),
                                              revision,
                                              'WRLDC',
                                              ftype,
                                              'SUCCESS',
                                              'UPINSPNT')
            except Exception, e:
                if e.args[0] == 'No Data to save':
                    sql_load_lib.sql_sp_load_exec(dns,
                                                  date.strftime('%Y-%m-%d'),
                                                  revision,
                                                  'WRLDC',
                                                  ftype,
                                                  'SUCCESS',
                                                  'UPINSPNT')
                else:
                    sql_load_lib.sql_sp_load_exec(dns,
                                                  date.strftime('%Y-%m-%d'),
                                                  revision,
                                                  'WRLDC',
                                                  ftype,
                                                  'FAILED',
                                                  'UPINSPNT')
                    raise


def wrldc_stoa_link_schedule():
    """
    Gets the Entitlement
    """
    print "here"
    wrldc_crawler = Crawler("http://www.wrldc.in/test/link.asp")
    print "here2"
    res = wrldc_crawler.set_date("01-MAR-2014")
    print res.read()
    revlist = wrldc_crawler.get_selection_list(res, "scl1")[-1]
    linklist = wrldc_crawler.get_selection_list(res, "scl2")
    print "Here", revlist, linklist
    for attribval in ([("scl2", link), ("scl1", revlist)]
                      for link in linklist[1:]):
        res = wrldc_crawler.set_selection_list(res, attribval)
        conrevdict = {"cons": attribval[0][1], "rev": attribval[1][1]}
        htmldata = HtmlData(res, 'STOA_Link', conrevdict, "f:/nrldc/Data/")
        print htmldata.datatable


def wrldc_schedulex(url=None, date=None, directory=None):
    """
    Get the WRLDC schedule for all the discoms
    """
    date = datetime.strptime("01-MAR-2014", '%d-%b-%Y')
    wrldc_crawler = Crawler("http://www.wrldc.in/test/sch.asp")
    res = wrldc_crawler.set_date("01-MAR-2014")
    revlist = 1
    conslist = wrldc_crawler.get_selection_list(res, "scl2")
    for attribval in ([("scl2", cons), ("scl1", revlist)]
                      for cons in conslist[1:]):
        #print attribval
        res = wrldc_crawler.set_selection_list(res, attribval)
        conrevdict = {"cons": attribval[0][1], "rev": revlist}
        htmldata = HtmlData(res, 'WRLDC_ISGS', conrevdict, "f:/nrldc/Data/")
        htmldata.add_datenrev_col_datatable()
        htmldata.save_datatable_as_csv()
        break
#wrldc_schedule()
#wrldc_dc('http://www.wrldc.com/test/dc.asp', datetime.strptime("01-MAR-2014", '%d-%b-%Y'), "f:/nrldc/Data/", "WRLDC_Declared_Capability", "F:/nrldc/config/sqldb_connection_config.txt")
#wrldc_entitlement('http://www.wrldc.com/test/ENT.asp', datetime.strptime("01-MAR-2014", '%d-%b-%Y'), "f:/nrldc/Data/", "WRLDC_Entitlement", "F:/nrldc/config/sqldb_connection_config.txt")
#wrldc_injection('http://www.wrldc.com/test/isgs.asp', datetime.strptime("01-MAR-2014", '%d-%b-%Y'), "f:/nrldc/Data/", "WRLDC_Injection", "F:/nrldc/config/sqldb_connection_config.txt")


def main(args):
    """
    Main Function. Based on filename call to different functions
    """
    for date in daterange.daterange(args.start_date, args.end_date):
        if args.filenm == "WRLDC_ISGS":
            wrldc_schedule(args.url, date, args.dir, args.filenm,
                           args.dsn)
        elif args.filenm == "WRLDC_Declared_Capability":
            wrldc_dc(args.url, date, args.dir, args.filenm,
                     args.dsn)
        elif args.filenm == "WRLDC_Entitlement":
            wrldc_entitlement(args.url, date, args.dir, args.filenm,
                              args.dsn)
        elif args.filenm == "WRLDC_Injection":
            wrldc_injection(args.url, date, args.dir, args.filenm,
                            args.dsn)
        else:
            print "Paramter values not known."

if __name__ == '__main__':
    ARG = argparse.ArgumentParser(description="Fetches WRLDC data and\
                                     Uploads Staging table data")
    ARG.add_argument('-u', '--url', dest='url',
                     help='URL to fetch data from',
                     required=True)
    """ARG.add_argument('-D', '--discom', dest='discom',
                     help='State to fetch data for'
                     )"""
    ARG.add_argument('-d', '--dir', dest='dir',
                     help='Directory to save data to',
                     required=True)
    ARG.add_argument('-f', '--file', dest='filenm',
                     help='Local file name to save data to.\
                     The name will be appended with date for which crawling\
                     is done', required=True)
    ARG.add_argument('-b', '--strdt', dest='start_date',
                     help='Start of Date to crawl the data. \
                     Optional default is todays date'
                     )
    ARG.add_argument('-e', '--enddt', dest='end_date',
                     help='Start of Date to crawl the data. \
                     Optional default is todays date'
                     )
    ARG.add_argument('-m', '--dbdsnconfig', dest='dsn',
                     help='Full path and the file name of the db config',
                     required=True)
    """ARG.add_argument('-t', '--tabnm', dest='tabname',
                     help='Name of the table to be loaded',
                     required=True)"""
    main(ARG.parse_args())


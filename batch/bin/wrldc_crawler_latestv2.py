"""
Crawls WRLDC Data after 31/07/2016.

Quenext 2017
"""

import requests
import datetime
import time
import petl
import sql_load_lib
import nrldc_crawler_v4
import logging
import daterange
import argparse
import csv
from retrying import retry


class WrldcCrawler(object):
    """WRLDC Crawler."""

    def __init__(self):
        """Constructor."""
        self.user_agent = ('Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36'
                           ' (KHTML, like Gecko) Chrome/41.0.2228.0'
                           ' Safari/537.36')
        self.root_url = "http://103.7.130.121/WBES/"
        self.headers = {'user-agent': self.user_agent}

    def get_region(self, zone):
        """WRLDC Region Mapping."""
        region_dict = {
            "EAST": 1,
            "WEST": 2,
            "NORTH": 3,
            "SOUTH": 4,
            "NORTH EAST": 5
        }
        return region_dict.get(zone, None)

    def get_schedule_type(self, schedule):
        """WRLDC Schdule Type Mappping."""
        schdule_dict = {
            'ISGS': 1,
            'MTOA': 2,
            'STOA': 3,
            'LTA': 4,
            'IEX': 5,
            'PXIL': 6,
            'URS': 8,
            'RRAS': 9
        }
        return schdule_dict.get(schedule, None)

    def get_revison(self, regionid, scheduledate, schtype):
        """
        Get Revision.

        Without the useragent page asks for authentication.
        """
        url = {'NetSchedule': (self.root_url + 'ReportNetSchedule/'
                               'GetNetScheduleRevisionNo'
                               '?regionid={}&ScheduleDate={}'),
               'Entitlement': (self.root_url + 'Report/'
                               'GetNetScheduleRevisionNoForSpecificRegion'
                               '?regionid={}&ScheduleDate={}')}
        r = requests.get(url.get(schtype).format(regionid, scheduledate),
                         headers=self.headers)
        return r.json()

    def search_list_of_dict(self, data, regionid,
                            wrldc_buyer, getkey, state=None):
        """Search json data of the type list of dictionaries."""
        for dct in data:
            # if dct.get('RegionId') == 2
            # and dct.get('UtilName') == 'GUJARAT'
            # and dct.get('Acronym') == 'GEB_Beneficiary' and
            # dct.get('IsActive') == 1
            if (dct.get('RegionId') == regionid and
                    dct.get('IsActive') == 1 and
                    dct.get('Acronym') == wrldc_buyer and not state):
                print dct.get(getkey)
                return dct.get(getkey), dct.get('ParentStateUtilId')
            elif (dct.get('RegionId') == regionid and
                    dct.get('IsActive') == 1 and
                    not wrldc_buyer and
                    dct.get('UtilName') == state):
                return dct.get(getkey), dct.get('ParentStateUtilId')

    def map_utlityname_to_wrldcutilid(self, regionid, wrldc_buyer, schtype):
        """Get the utilityId from the name of the buyer."""
        url = {'NetSchedule': (self.root_url + "ReportNetSchedule"
                               "/GetUtils?regionId={}"),
               'Entitlement': (self.root_url + "Report/GetUtils?regionId={}")}
        r = requests.get(url.get(schtype).format(regionid),
                         headers=self.headers)
        data = r.json()
        return self.search_list_of_dict(data.get('buyers', None),
                                        regionid, wrldc_buyer, 'UtilId')

    @retry(stop_max_attempt_number=6,
           wait_exponential_multiplier=10000,
           wait_exponential_max=20000)
    def get_entitlement(self, regionid, entdate, revision, utilid):
        """Fetch the entitlement."""
        url = (self.root_url + 'Report/GetReportData'
               '?regionId={}&date={}&revision={}&utilId={}'
               '&isBuyer=1&byOnBar=0')
        r = requests.get(url.format(regionid, entdate, revision, utilid),
                         headers=self.headers)
        data = r.json()
        if data != 'nodata':
            return data.get('jaggedarray', None)
        else:
            return None

    @retry(stop_max_attempt_number=6,
           wait_exponential_multiplier=10000,
           wait_exponential_max=20000)
    def get_schedule(self, regionid, schdate, revision, utilid, schtype):
        """
        Fetch the ISGS.

        http://103.7.130.121//WBES/ReportNetSchedule/ExportNetScheduleDetailToPDF
        """
        url = (self.root_url + 'ReportNetSchedule/'
               '/ExportNetScheduleDetailToPDF?scheduleDate={}'
               '&sellerId={}&revisionNumber={}&getTokenValue={}'
               '&fileType={}&schType={}')
        now = datetime.datetime.now()
        timestamp = int(time.mktime(now.timetuple()))
        url2 = url.format(schdate, utilid,
                          revision, timestamp, 'csv', schtype)
        with requests.Session() as s:
            try:
                download = s.get(url2, stream=True, headers=self.headers)
                decoded_content = download.content.decode('utf-8')
                # Getting rid of null values '\0' from the data
                data = csv.reader((line.replace('\0', '')
                                   for line in decoded_content.splitlines()),
                                  delimiter=',')
                my_list = list(data)
            except Exception:
                return None
        # Getting rid of empty list row
        return [x for x in my_list if x]


def wrldc_entitlement(date, wrldc_buyer, state, ftype, dns):
    """Get the Entitlement."""
    logger = logging.getLogger("wrldc_entitlement")
    wrldc = WrldcCrawler()
    region = 'WEST'
    tablenm = 'wrldc_entitlements_stg'
    regionid = wrldc.get_region(region)
    schtype = 'Entitlement'
    rev_list = wrldc.get_revison(regionid, date.strftime('%d-%m-%Y'), schtype)
    utilid, parentutilid = wrldc.map_utlityname_to_wrldcutilid(regionid,
                                                               wrldc_buyer,
                                                               schtype)
    for rev in rev_list[::-1]:
        dbrev = nrldc_crawler_v4.db_rev_check(dns, date, state, ftype)
        if rev >= dbrev:
            try:
                sql_load_lib.sql_sp_load_exec(dns,
                                              date.strftime('%Y-%m-%d'),
                                              rev,
                                              state,
                                              ftype,
                                              'RUNNING',
                                              'UPINSPNT')
                ent = wrldc.get_entitlement(regionid,
                                            date.strftime('%d-%m-%Y'),
                                            rev,
                                            utilid)
                if ent:
                    ent_table = petl.cutout(ent, 'Time Desc', 'Grand Total')
                    ent_table2 = petl.select(ent_table, 'Time Block',
                                             lambda v: v is not None)
                    ent_table3 = petl.melt(ent_table2, key=['Time Block'])
                    ent_table4 = petl.addfield(ent_table3, 'Revision',
                                               rev, 0)
                    ent_table5 = petl.addfield(ent_table4, 'Issue_Date_Time',
                                               None, 0)
                    ent_table6 = petl.addfield(ent_table5, 'Discom',
                                               wrldc_buyer, 0)
                    ent_table7 = petl.addfield(ent_table6, 'State', state, 0)
                    entdate = date.strftime('%d-%b-%Y')
                    ent_table8 = petl.addfield(ent_table7, 'Date', entdate, 0)
                    ent_table9 = petl.rename(ent_table8,
                                             {'Time Block': 'Block_No',
                                              'variable': 'Station_Name',
                                              'value': 'Schedule'})
                    logger.info('Entilement data processed.')
                    logger.debug(ent_table9)
                    sql_load_lib.sql_table_insert_exec(dns,
                                                       tablenm,
                                                       list(ent_table9)[1:])
                sql_load_lib.sql_sp_load_exec(dns,
                                              date.strftime('%Y-%m-%d'),
                                              rev,
                                              state,
                                              ftype,
                                              'SUCCESS',
                                              'UPINSPNT')
            except Exception:
                sql_load_lib.sql_sp_load_exec(dns,
                                              date.strftime('%Y-%m-%d'),
                                              rev,
                                              state,
                                              ftype,
                                              'FAILED',
                                              'UPINSPNT')
                raise
    return 0


class WrldcStateSchedule(object):
    """State Schedule Object."""

    def __init__(self, date, wrldc_buyer, state, dns):
        """Constructor."""
        self.wrldc = WrldcCrawler()
        # self.date = datetime.datetime.strptime(date, '%d-%m-%Y')
        self.date = date
        self.wrldc_buyer = wrldc_buyer
        self.state = state
        self.tablenm = 'wrldc_state_drawl_schedule_stg'
        self.dns = dns
        region = 'WEST'
        schtype = 'NetSchedule'
        self.logger = logging.getLogger("WrldcStateSchedule")
        self.regionid = self.wrldc.get_region(region)
        self.rev_list = self.wrldc.get_revison(self.regionid,
                                               self.date.strftime('%d-%m-%Y'),
                                               schtype)
        self.utilid, self.parentutilid = \
            self.wrldc.map_utlityname_to_wrldcutilid(self.regionid,
                                                     self.wrldc_buyer,
                                                     schtype)

    def run(self):
        """Run for all the functions."""
        runseq = ['ISGS', 'MTOA', 'STOA', 'LTA', 'IEX', 'PXIL', 'URS', 'RRAS']
        for rev in self.rev_list[::-1]:
            for ft in runseq:
                self.wrldc_netschedule(rev, ft)

    def wrldc_netschedule(self, revision, ftype):
        """Get ISGS/URS."""
        dbrev = nrldc_crawler_v4.db_rev_check(self.dns, self.date,
                                              self.state, ftype)
        self.logger.info('dbrev : ' + str(dbrev) + ftype)
        if revision >= dbrev:
            try:
                sql_load_lib.sql_sp_load_exec(self.dns,
                                              self.date.strftime('%Y-%m-%d'),
                                              revision,
                                              self.state,
                                              ftype,
                                              'RUNNING',
                                              'UPINSPNT')
                schdate = self.date.strftime('%d-%m-%Y')
                # Connection gets stale and hangs after a long run.
                # Hence deleting and reinitializing the conn.
                del self.wrldc
                self.wrldc = WrldcCrawler()
                schedule_type = self.wrldc.get_schedule_type(ftype)
                isgs = self.wrldc.get_schedule(self.regionid, schdate,
                                               revision, self.utilid,
                                               schedule_type)
                # print isgs
                if isgs:
                    # Check the 1st 8 rows to determine header values
                    for i, row in enumerate(isgs[:8]):
                        if row[0] == 'From Utility':
                            hdr = row
                        elif row[0] == 'To Utility':
                            filt = row
                        elif row[0] == 'Appr. No':
                            apprno = row
                        elif row[0] == 'Time Block':
                            hdr_end_idx = i
                            break
                    isgs_table = petl.skip(isgs, hdr_end_idx)
                    if ftype == 'ISGS':
                        hdr2 = [hdr[i] if not isgs[hdr_end_idx][i]
                                else isgs[hdr_end_idx][i]
                                for i in xrange(len(hdr))]
                        isgs_table1 = petl.setheader(isgs_table, hdr2)
                        # cut_indx = [i for i, buyer in enumerate(filt)
                        #             if buyer[:4] != self.wrldc_buyer[:4]][1:]
                        cut_indx = [i for i, buyer in enumerate(filt)
                                    if not buyer]
                    elif ftype in ('MTOA', 'STOA', 'LTA', 'IEX',
                                   'PXIL', 'URS', 'RRAS'):
                        hdr2 = [hdr[i] + '^|^' + filt[i] + '^|^' + apprno[i]
                                if not isgs[hdr_end_idx][i]
                                else isgs[hdr_end_idx][i]
                                for i in xrange(len(hdr))]
                        isgs_table1 = petl.setheader(isgs_table, hdr2)
                        cut_indx = [i for i, buyer in enumerate(hdr)
                                    if not buyer]
                    isgs_table2 = petl.cutout(isgs_table1, *tuple(cut_indx))
                    isgs_table3 = petl.convert(isgs_table2, hdr2[0], int)
                    isgs_table4 = petl.select(isgs_table3, 'Time Block',
                                              lambda v: v is not None)
                    isgs_table5 = petl.melt(isgs_table4, key=['Time Block'])
                    isgs_table6 = petl.addfield(isgs_table5, 'Drawl_Type',
                                                ftype, 0)
                    isgs_table7 = petl.addfield(isgs_table6, 'Revision',
                                                revision, 0)
                    isgs_table8 = petl.addfield(isgs_table7, 'Issue_Date_Time',
                                                None, 0)
                    isgs_table9 = petl.addfield(isgs_table8, 'Discom',
                                                self.wrldc_buyer, 0)
                    isgs_table10 = petl.addfield(isgs_table9, 'State',
                                                 self.state, 0)
                    schdate = self.date.strftime('%d-%b-%Y')
                    isgs_table11 = petl.addfield(isgs_table10, 'Date',
                                                 schdate, 0)
                    isgs_table12 = petl.rename(isgs_table11,
                                               {'Time Block': 'Block_No',
                                                'variable': 'Station_Name',
                                                'value': 'Schedule'})
                    isgs_table13 = petl.addfield(isgs_table12, 'Appr_No',
                                                 lambda row: row.Station_Name.
                                                 split('^|^')[-1])
                    isgs_table14 = petl.convert(isgs_table13, 'Station_Name',
                                                lambda v: '|'.join(
                                                    v.split('^|^')[:2]))
                    self.logger.info(ftype + 'data processed.')
                    # print isgs_table14
                    sql_load_lib.sql_table_insert_exec(self.dns,
                                                       self.tablenm,
                                                       list(isgs_table14)[1:])
                elif ftype == 'RRAS' and not isgs:
                    blocks = [[i] for i in xrange(1, 97)]
                    blocks.insert(0, ['Block_No'])
                    isgs_table5 = petl.addfield(blocks, 'Drawl_Type',
                                                ftype, 0)
                    isgs_table6 = petl.addfield(isgs_table5, 'Revision',
                                                revision, 0)
                    isgs_table7 = petl.addfield(isgs_table6, 'Issue_Date_Time',
                                                None, 0)
                    isgs_table8 = petl.addfield(isgs_table7, 'Discom',
                                                self.wrldc_buyer, 0)
                    isgs_table9 = petl.addfield(isgs_table8, 'State',
                                                self.state, 0)
                    schdate = self.date.strftime('%d-%b-%Y')
                    isgs_table10 = petl.addfield(isgs_table9, 'Date',
                                                 schdate, 0)
                    isgs_table11 = petl.addfield(isgs_table10, 'Station_Name',
                                                 None)
                    isgs_table12 = petl.addfield(isgs_table11, 'Schedule',
                                                 0)
                    isgs_table13 = petl.addfield(isgs_table12, 'Appr_No',
                                                 '')
                    self.logger.info(ftype + 'data processed.')
                    # print isgs_table12
                    sql_load_lib.sql_table_insert_exec(self.dns,
                                                       self.tablenm,
                                                       list(isgs_table13)[1:])
                sql_load_lib.sql_sp_load_exec(self.dns,
                                              self.date.strftime('%Y-%m-%d'),
                                              revision,
                                              self.state,
                                              ftype,
                                              'SUCCESS',
                                              'UPINSPNT')
            except Exception:
                sql_load_lib.sql_sp_load_exec(self.dns,
                                              self.date.strftime('%Y-%m-%d'),
                                              revision,
                                              self.state,
                                              ftype,
                                              'FAILED',
                                              'UPINSPNT')
                raise
        return 0


def main(args):
    """Main Function."""
    for date in daterange.daterange(args.start_date, args.end_date):
        if args.ftype == 'Entitlement':
            wrldc_entitlement(date, args.wrldcbuyer,
                              args.state, args.ftype, args.dns)
        elif args.ftype == 'StateSchedule':
            wss = WrldcStateSchedule(date, args.wrldcbuyer,
                                     args.state, args.dns)
            wss.run()


if __name__ == '__main__':
    ARG = argparse.ArgumentParser(description=("Fetches WRLDC data and"
                                  "uploads into staging table."))
    ARG.add_argument('-s', '--state', dest='state',
                     help='State to fetch data for')
    ARG.add_argument('-wb', '--wrldcbuyer', dest='wrldcbuyer',
                     help='State to fetch data for')
    ARG.add_argument('-sd', '--strdt', dest='start_date',
                     help=('Start of Date to crawl the data.'
                           'Optional default is todays date'))
    ARG.add_argument('-ed', '--enddt', dest='end_date',
                     help=('Start of Date to crawl the data.'
                           'Optional default is todays date'))
    ARG.add_argument('-m', '--dbdsnconfig', dest='dns',
                     help='Full path and the file name of the db config',
                     required=True)
    ARG.add_argument('-t', '--type', dest='ftype',
                     help=('Entitlement or State Schedule'
                           '["Entitlement", "StateSchedule"]'), required=True)
    main(ARG.parse_args())

# wss = WrldcStateSchedule('02-03-2017', 'GEB_State', 'GUJARAT', '../config/sqldb_connection_config.txt')
# wss.run()
# wss.wrldc_netschedule(29, 'URS')
# wss.wrldc_oa(0, 'STOA')
# date = datetime.datetime.strptime('26-04-2017', '%d-%m-%Y')
# wrldc_entitlement(date, 'GEB_Beneficiary', 'GUJARAT', 'WRLDC_Entitlement', dns='../config/sqldb_connection_config.txt')
# wrldc = WrldcCrawler()
# date = '09-05-2017'
# date = datetime.datetime.strptime(date, '%d-%m-%Y')
# region = 'WEST'
# tablenm = 'wrldc_entitlements_stg'
# wrldc_buyer = 'GEB_State'
# regionid = wrldc.get_region(region)
# rev_list = wrldc.get_revison(regionid, date.strftime('%d-%m-%Y'))
# utilid, parentutilid = wrldc.map_utlityname_to_wrldcutilid(regionid,
#                                                            wrldc_buyer)
# schedule_type = wrldc.get_schedule_type('RRAS')
# print wrldc.get_schedule(regionid, date.strftime('%d-%m-%Y'),
#                          rev_list[0], utilid, schedule_type)
# def mycsv_reader(csv_reader):
#     while True:
#         try:
#             yield next(csv_reader)
#         except csv.Error:
#             # error handling what you want.
#             pass
#         continue
#     return

# # with open('test.csv', 'rb') as csvfile:
# #     reader = csv.reader(csvfile, delimiter=',')
# #     for row in reader:
# #         if '\0' in row:
# #             continue
# #         else:
# #             print row

# reader = mycsv_reader(csv.reader(open('test.csv', 'rb'), delimiter=','))
# for line in reader:
#     print line
# data_initial = open("test.csv", "rb")
# data = csv.reader((line.replace('\0', '') for line in data_initial), delimiter=",")
# for i in data:
#     print i

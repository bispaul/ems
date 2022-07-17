"""
Crawls WRLDC Data after 31/07/2016.

Quenext 2017
"""

import requests
import re
import json
import datetime
import time
import petl
import sql_load_lib
import nrldc_crawler_v4
import logging
import daterange
import argparse


class WrldcCrawler(object):
    """WRLDC Crawler."""

    def __init__(self):
        """Constructor."""
        self.user_agent = ('Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36'
                           ' (KHTML, like Gecko) Chrome/41.0.2228.0'
                           ' Safari/537.36')
        self.root_url = "http://103.7.130.121/WBES/"
        self.headers = {'user-agent': self.user_agent}

    def region_dict(self, zone):
        """WRLDC Region Mapping."""
        region_dict = {
            "EAST": 1,
            "WEST": 2,
            "NORTH": 3,
            "SOUTH": 4,
            "NORTH EAST": 5
        }
        return region_dict.get(zone, None)

    def get_revison(self, regionid, scheduledate):
        """
        Get Revision.

        Without the useragent page asks for authentication.
        """
        url = (self.root_url + 'Report/'
               'GetNetScheduleRevisionNoForSpecificRegion'
               '?regionid={}&ScheduleDate={}')
        r = requests.get(url.format(regionid, scheduledate),
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
                return dct.get(getkey), dct.get('ParentStateUtilId')
            elif (dct.get('RegionId') == regionid and
                    dct.get('IsActive') == 1 and
                    not wrldc_buyer and
                    dct.get('UtilName') == state):
                return dct.get(getkey), dct.get('ParentStateUtilId')

    def map_utlityname_to_wrldcutilid(self, regionid, wrldc_buyer):
        """Get the utilityId from the name of the buyer."""
        url = (self.root_url + "Report/GetUtils?regionId={}")
        r = requests.get(url.format(regionid), headers=self.headers)
        data = r.json()
        return self.search_list_of_dict(data.get('buyers', None),
                                        regionid, wrldc_buyer, 'UtilId')

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

    def get_isgs(self, regionid, schdate, revision, utilid):
        """Fetch the ISGS."""
        url = (self.root_url + 'Report/GetRldcData'
               '?isBuyer=true&utilId={}&regionId={}&scheduleDate={}'
               '&revisionNumber={}&byOnBar=0')
        r = requests.get(url.format(utilid, regionid, schdate, revision),
                         headers=self.headers)
        data = r.json()
        if data != 'nodata':
            return data.get('jaggedarray', None)
        else:
            return None

    def get_stoa(self, regionid, schdate, revision, parentutilid):
        """
        Fetch STOA for Utility.

        example api: http://103.7.130.121/WBES/Report/GetStoaNewDeatil?
        regionId=2&scheduleDate=27-04-2017&sellerId=ALL
        &buyerId=2b2428f1-1992-40ef-ac36-d0dfbe843d04&traderId=ALL
        &revisionNumber=53&scheduleType=3
        """
        url = (self.root_url + 'Report/GetStoaNewDeatil?'
               'regionId={}&scheduleDate={}&sellerId=ALL'
               '&buyerId={}&traderId=ALL'
               '&revisionNumber={}&scheduleType=3')
        r = requests.get(url.format(regionid, schdate, parentutilid, revision),
                         headers=self.headers)
        data = r.json()
        if data != 'nodata':
            return data.get('jaggedarray', None)
        else:
            return None

    def get_mtoa(self, regionid, schdate, revision, parentutilid):
        """
        Fetch MTOA for Utility.

        example api: http://103.7.130.121/WBES/Report/
        GetMtoaLtaNewDeatil?regionId=2
        &scheduleDate=27-04-2017&sellerId=ALL
        &buyerId=2b2428f1-1992-40ef-ac36-d0dfbe843d04
        &traderId=ALL&revisionNumber=54
        &scheduleType=2
        &isEXPP=1&isDetails=0
        """
        url = (self.root_url + 'Report/GetMtoaLtaNewDeatil?'
               'regionId={}&scheduleDate={}&sellerId=ALL'
               '&buyerId={}&traderId=ALL'
               '&revisionNumber={}&scheduleType=2'
               '&isEXPP=1&isDetails=0')
        r = requests.get(url.format(regionid, schdate, parentutilid, revision),
                         headers=self.headers)
        data = r.json()
        if data != 'nodata':
            return data.get('jaggedarray', None)
        else:
            return None

    def get_ltoa(self, regionid, schdate, revision, parentutilid):
        """
        Fetch MTOA for Utility.

        example api: http://103.7.130.121/WBES/Report/
        GetMtoaLtaNewDeatil?regionId=2
        &scheduleDate=27-04-2017&sellerId=ALL
        &buyerId=2b2428f1-1992-40ef-ac36-d0dfbe843d04
        &traderId=ALL&revisionNumber=54
        &scheduleType=4&isEXPP=1&isDetails=0
        """
        url = (self.root_url + 'Report/GetMtoaLtaNewDeatil?'
               'regionId={}&scheduleDate={}&sellerId=ALL'
               '&buyerId={}&traderId=ALL'
               '&revisionNumber={}&scheduleType=4'
               '&isEXPP=1&isDetails=0')
        r = requests.get(url.format(regionid, schdate, parentutilid, revision),
                         headers=self.headers)
        data = r.json()
        if data != 'nodata':
            return data.get('jaggedarray', None)
        else:
            return None

    def get_exrevision_typeid(self, schdate, exchange):
        """
        Get Exchange Revision.

        http://103.7.130.121/WBES/Report/GetRevisionNoByDate?date=27-04-2017&typeId=5
        """
        exchange_dict = {'IEX': 5, 'PXIL': 6}
        url = (self.root_url + 'Report/GetRevisionNoByDate?'
               'date={}&typeId={}')
        typeid = exchange_dict.get(exchange)
        r = requests.get(url.format(schdate, typeid),
                         headers=self.headers)
        data = r.json()
        if data != 'nodata':
            return data[0], typeid
        else:
            return None, typeid

    def get_px(self, schdate, revision, typeid):
        """
        Fetch IEX.

        http://103.7.130.121/WBES/Report/GetPxByDateAndRevision?date=27-04-2017&revId=null&typeId=5
        """
        url = (self.root_url + 'Report/GetPxByDateAndRevision?'
               'date={}&revId={}&typeId={}')
        r = requests.get(url.format(schdate, revision, typeid),
                         headers=self.headers)
        data = r.text
        # Parse the json string out of a javascript variable.
        p = re.findall(r'var dataHandsOn.*?=\s*(.*?);',
                       data, re.DOTALL | re.MULTILINE)
        return json.loads(eval(p[0].strip("JSON.parse( )")))

    def get_urs(self, regionid, schdate, revision, parentutilid):
        """
        Fetch URS.

        http://103.7.130.121/WBES/Report
        /GetUrsReport?regionId=2&date=27-04-2017
        &revision=62&utilId=37dea0e4-a8d3-4132-a822-3f4450c1aa66
        &isBuyer=1&byOnReg=0
        """
        url = (self.root_url + 'Report/GetUrsReport?'
               'regionId={}&date={}'
               '&revision={}&utilId={}'
               '&isBuyer=1&byOnReg=0')
        r = requests.get(url.format(regionid, schdate, revision, parentutilid),
                         headers=self.headers)
        data = r.json()
        if data != 'nodata':
            return data.get('jaggedarray', None)
        else:
            return None

    def get_netschedule(self, regionid, schdate, revision, utilid):
        """
        Fetch Net Schedule.

        http://103.7.130.121/WBES/ReportNetSchedule
        /GetNetScheduleSummary?regionId=2
        &scheduleDate=27-04-2017&sellerId=ALL
        &revisionNumber=65&byDetails=0&isBuyer=0
        """
        # url = ('http://103.7.130.121/WBES/ReportNetSchedule'
        #        '/GetNetSchDetailsIndex?regionId={}'
        #        '&scheduleDate={}&sellerId={}'
        #        '&revisionNumber={}&scheduleType={}'
        #        '&isJson=true')
        # works partially
        # url = ('http://103.7.130.121/WBES/ReportNetSchedule'
        #        '/GetNetScheduleDetails?regionId={}'
        #        '&scheduleDate={}&sellerId={}'
        #        '&revisionNumber={}&scheduleType={}')
        now = datetime.datetime.now()
        timestamp = int(time.mktime(now.timetuple()))
        url = (self.root_url + 'ReportNetSchedule'
               '/ExportNetScheduleDetailToPDF?'
               'scheduleDate={}&sellerId={}'
               '&revisionNumber={}&getTokenValue={}'
               '&fileType={}&schType={}')
        # with requests.Session() as s:
        #     download = s.get(url.format(schdate, utilid,
        #                                 revision, timestamp, 'csv', 1),
        #                      headers=self.headers)
        #     decoded_content = download.content.decode('utf-8')
        #     cr = csv.reader(decoded_content.splitlines(), delimiter=',')
        #     my_list = list(cr)
        #     for row in my_list:
        #         print(row)
        # with closing(requests.get(url.format(schdate, utilid,
        #                                      revision, timestamp, 'csv', 1),
        #                           stream=True, headers=self.headers)) as r:
        #     reader = csv.reader(r.iter_lines(), delimiter=',')
        #     for row in reader:
        #         print row
        # 1 and 4, 5, 6, 8 works for schType
        r = requests.get(url.format(schdate, utilid,
                                    revision, timestamp, 'csv', 1),
                         stream=True, headers=self.headers)
        f = open('test.csv', 'w')
        for chunk in r.iter_content(chunk_size=512 * 1024):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)
        f.close()


def wrldc_entitlement(date, wrldc_buyer, state, ftype, dns):
    """Get the Entitlement."""
    logger = logging.getLogger("wrldc_entitlement")
    wrldc = WrldcCrawler()
    region = 'WEST'
    tablenm = 'wrldc_entitlements_stg'
    regionid = wrldc.region_dict(region)
    rev_list = wrldc.get_revison(regionid, date.strftime('%d-%m-%Y'))
    utilid, parentutilid = wrldc.map_utlityname_to_wrldcutilid(regionid,
                                                               wrldc_buyer)
    for rev in rev_list[::-1]:
        dbrev = nrldc_crawler_v4.db_rev_check(dns, date, rev, ftype)
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
        self.date = datetime.datetime.strptime(date, '%d-%m-%Y')
        self.wrldc_buyer = wrldc_buyer
        self.state = state
        self.tablenm = 'wrldc_state_drawl_schedule_stg'
        self.dns = dns
        region = 'WEST'
        self.logger = logging.getLogger("WrldcStateSchedule")
        self.regionid = self.wrldc.region_dict(region)
        self.rev_list = self.wrldc.get_revison(self.regionid,
                                               self.date.strftime('%d-%m-%Y'))
        self.utilid, self.parentutilid = \
            self.wrldc.map_utlityname_to_wrldcutilid(self.regionid,
                                                     self.wrldc_buyer)

    def run(self):
        """Run for all the functions."""
        func_map_dict = {'ISGS': self.wrldc_isgs_urs,
                         'URS': self.wrldc_isgs_urs,
                         'STOA': self.wrldc_oa,
                         'MTOA': self.wrldc_oa,
                         'LTA': self.wrldc_oa,
                         'PXIL': self.wrldc_iex_pxil,
                         'IEX': self.wrldc_iex_pxil}
        runseq = ['ISGS', 'STOA', 'MTOA', 'LTA', 'URS']
        exrunseq = ['IEX', 'PXIL']
        for rev in self.rev_list[::-1]:
            for ft in runseq:
                func_map_dict.get(ft)(rev, ft)
        for exname in exrunseq:
            func_map_dict.get(exname)(exname)

    def wrldc_isgs_urs(self, revision, ftype):
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
                if ftype == 'ISGS':
                    isgs = self.wrldc.get_isgs(self.regionid, schdate,
                                               revision, self.utilid)
                elif ftype == 'URS':
                    isgs = self.wrldc.get_urs(self.regionid, schdate,
                                              revision, self.utilid)
                if isgs:
                    isgs_table = petl.skip(isgs, 1)
                    # print petl.header(isgs)
                    isgs_table1 = petl.cutout(isgs_table, 'Time Desc',
                                              'Grand Total')
                    isgs_table2 = petl.select(isgs_table1, 'Time Block',
                                              lambda v: v is not None)
                    isgs_table3 = petl.melt(isgs_table2, key=['Time Block'])
                    isgs_table4 = petl.addfield(isgs_table3, 'Drawl_Type',
                                                ftype, 0)
                    isgs_table5 = petl.addfield(isgs_table4, 'Revision',
                                                revision, 0)
                    isgs_table6 = petl.addfield(isgs_table5, 'Issue_Date_Time',
                                                None, 0)
                    isgs_table7 = petl.addfield(isgs_table6, 'Discom',
                                                self.wrldc_buyer, 0)
                    isgs_table8 = petl.addfield(isgs_table7, 'State',
                                                self.state, 0)
                    schdate = self.date.strftime('%d-%b-%Y')
                    isgs_table9 = petl.addfield(isgs_table8, 'Date',
                                                schdate, 0)
                    isgs_table10 = petl.rename(isgs_table9,
                                               {'Time Block': 'Block_No',
                                                'variable': 'Station_Name',
                                                'value': 'Schedule'})
                    self.logger.info('ISGS data processed.')
                    # self.logger.debug(isgs_table10)
                    sql_load_lib.sql_table_insert_exec(self.dns,
                                                       self.tablenm,
                                                       list(isgs_table10)[1:])
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

    def wrldc_oa(self, revision, ftype):
        """Get LTOA/MTOA/STOA."""
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
                if ftype == 'STOA':
                    oadata = self.wrldc.get_stoa(self.regionid, schdate,
                                                 revision, self.parentutilid)
                elif ftype == 'MTOA':
                    oadata = self.wrldc.get_mtoa(self.regionid, schdate,
                                                 revision, self.parentutilid)
                elif ftype == 'LTA':
                    oadata = self.wrldc.get_ltoa(self.regionid, schdate,
                                                 revision, self.parentutilid)
                # self.logger.info(oadata)
                if oadata:
                    from_utility = oadata[2][2:-1]
                    hdr = ['Time Block']
                    hdr.extend(from_utility)
                    oadata_table = petl.skip(oadata, 7)
                    oahdr = petl.header(oadata_table)
                    oadata_table1 = petl.cutout(oadata_table, 'Time Desc',
                                                oahdr[-1])
                    oadata_table2 = petl.select(oadata_table1, 'Time Block',
                                                lambda v: v is not None and
                                                v != '')
                    # self.logger.info(oadata_table2)
                    oadata_table3 = petl.setheader(oadata_table2, hdr)
                    oadata_table4 = petl.melt(oadata_table3,
                                              key=['Time Block'])
                    oadata_table5 = petl.addfield(oadata_table4, 'Drawl_Type',
                                                  ftype, 0)
                    oadata_table6 = petl.addfield(oadata_table5, 'Revision',
                                                  revision, 0)
                    oadata_table7 = petl.addfield(oadata_table6,
                                                  'Issue_Date_Time',
                                                  None, 0)
                    oadata_table8 = petl.addfield(oadata_table7, 'Discom',
                                                  self.wrldc_buyer, 0)
                    oadata_table9 = petl.addfield(oadata_table8, 'State',
                                                  self.state, 0)
                    schdate = self.date.strftime('%d-%b-%Y')
                    oadata_table10 = petl.addfield(oadata_table9, 'Date',
                                                   schdate, 0)
                    oadata_table11 = petl.rename(oadata_table10,
                                                 {'Time Block': 'Block_No',
                                                  'variable': 'Station_Name',
                                                  'value': 'Schedule'})
                    self.logger.info(ftype + ' data processed.')
                    # self.logger.debug(list(oadata_table11))
                    sql_load_lib.sql_table_insert_exec(self.dns,
                                                       self.tablenm,
                                                       list(oadata_table11)[1:]
                                                       )
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

    def wrldc_iex_pxil(self, ftype):
        """Get IEX/PXIL."""
        dbrev = nrldc_crawler_v4.db_rev_check(self.dns, self.date,
                                              self.state, ftype)
        schdate = self.date.strftime('%d-%m-%Y')
        exrev_dict, extypeid = self.wrldc.get_exrevision_typeid(schdate, ftype)
        revision = exrev_dict.get('revisionNo')
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
                ex = self.wrldc.get_px(schdate, revision, extypeid)
                if ex:
                    hdr = [el.replace("\n", "").replace('-', '')
                           for el in ex[0]]
                    ex1 = petl.setheader(ex, hdr)
                    cuthdr = [el for el in hdr if 'GEB_Beneficiary' not in el]
                    ex2 = petl.cutout(ex1, *tuple(cuthdr[1:]))
                    keephdr = [ftype + '_Injection'
                               if 'Injection' in el else ftype + '_Drawal'
                               for el in hdr if 'GEB_Beneficiary' in el]
                    keephdr.insert(0, 'Block_No')
                    ex3 = petl.setheader(ex2, keephdr)
                    ex4 = petl.melt(ex3, key=['Block_No'])
                    ex5 = petl.addfield(ex4, 'Drawl_Type', ftype, 0)
                    ex6 = petl.addfield(ex5, 'Revision', revision, 0)
                    ex7 = petl.addfield(ex6, 'Issue_Date_Time', None, 0)
                    ex8 = petl.addfield(ex7, 'Discom', self.wrldc_buyer, 0)
                    ex9 = petl.addfield(ex8, 'State', self.state, 0)
                    schdate = self.date.strftime('%d-%b-%Y')
                    ex10 = petl.addfield(ex9, 'Date', schdate, 0)
                    ex11 = petl.rename(ex10,
                                       {'variable': 'Station_Name',
                                        'value': 'Schedule'})
                    self.logger.info(ftype + ' data processed.')
                    # self.logger.debug(list(oadata_table11))
                    sql_load_lib.sql_table_insert_exec(self.dns,
                                                       self.tablenm,
                                                       list(ex11)[1:]
                                                       )
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


# def main(args):
#     """Main Function."""
#     for date in daterange.daterange(args.start_date, args.end_date):
#         if args.ftype == 'Entitlement':
#             wrldc_entitlement(date, args.wrldcbuyer,
#                               args.state, args.ftype, args.dns)
#         elif args.ftype == 'StateSchedule':
#             wss = WrldcStateSchedule(date, args.wrldcbuyer,
#                                      args.state, args.dns)
#             wss.run()


# if __name__ == '__main__':
#     ARG = argparse.ArgumentParser(description=("Fetches WRLDC data and"
#                                   "uploads into staging table."))
#     ARG.add_argument('-s', '--state', dest='state',
#                      help='State to fetch data for')
#     ARG.add_argument('-wb', '--wrldcbuyer', dest='wrldcbuyer',
#                      help='State to fetch data for')
#     ARG.add_argument('-sd', '--strdt', dest='start_date',
#                      help=('Start of Date to crawl the data.'
#                            'Optional default is todays date'))
#     ARG.add_argument('-ed', '--enddt', dest='end_date',
#                      help=('Start of Date to crawl the data.'
#                            'Optional default is todays date'))
#     ARG.add_argument('-m', '--dbdsnconfig', dest='dsn',
#                      help='Full path and the file name of the db config',
#                      required=True)
#     ARG.add_argument('-t', '--type', dest='ftype',
#                      help=('Entitlement or State Schedule'
#                            '["Entitlement", "StateSchedule"]'), required=True)
#     main(ARG.parse_args())


wss = WrldcStateSchedule('26-04-2017', 'GEB_Beneficiary', 'GUJARAT', '../config/sqldb_connection_config.txt')
wss.run()
# wss.wrldc_isgs_urs(0, 'ISGS')
# wss.wrldc_oa(0, 'STOA')
# wss.wrldc_oa(0, 'MTOA')
# wss.wrldc_oa(0, 'LTA')
# wss.wrldc_isgs_urs(0, 'URS')
# wss.wrldc_iex_pxil('IEX')
# wss.wrldc_iex_pxil('PXIL')

# date = datetime.datetime.strptime('26-04-2017', '%d-%m-%Y')
# wrldc_entitlement(date, 'GEB_Beneficiary', 'GUJARAT', 'WRLDC_Entitlement', dns='../config/sqldb_connection_config.txt')
# wrldc_isgs(date, 'GEB_Beneficiary', 'GUJARAT', 0, 'WRLDC_ISGS', , dns='../config/sqldb_connection_config.txt')
# region = 'WEST'
# schdate = '26-04-2017'
# wrldc_buyer = 'GEB_Beneficiary'
# wrldc = WrldcCrawler()
# regionid = wrldc.region_dict(region)
# rev_list = wrldc.get_revison(regionid, schdate)
# utilid, parentutilid = wrldc.map_utlityname_to_wrldcutilid(regionid,
#                                                            wrldc_buyer)
# ent = wrldc.get_entitlement(regionid, entdate, rev_list[-1], utilid)
# # delete Time Desc
# ent_table = petl.cutout(ent, 'Time Desc', 'Grand Total')
# ent_table2 = petl.select(ent_table, 'Time Block', lambda v: v is not None)
# ent_table3 = petl.melt(ent_table2, key=['Time Block'])
# ent_table4 = petl.addfield(ent_table3, 'Revision', rev_list[-1], 0)
# ent_table5 = petl.addfield(ent_table4, 'Issue_Date_Time', None, 0)
# ent_table6 = petl.addfield(ent_table5, 'Discom', wrldc_buyer, 0)
# ent_table7 = petl.addfield(ent_table6, 'State', 'GUJARAT', 0)
# entdate = datetime.datetime.strptime(entdate, '%d-%m-%Y').strftime('%d-%b-%Y')
# ent_table8 = petl.addfield(ent_table7, 'Date', entdate, 0)
# ent_table9 = petl.rename(ent_table8, {'Time Block': 'Block_No',
#                                       'variable': 'Station_Name',
#                                       'value': 'Schedule'})
# tablenm = 'wrldc_entitlements_stg'
# dns = '../config/sqldb_connection_config.txt'

# sql_load_lib.sql_table_insert_exec(dns,
#                                    tablenm,
#                                    list(ent_table9)[1:])



# isgs = wrldc.get_isgs(regionid, schdate, rev_list[-1], utilid)
# isgs_table = petl.skip(isgs, 1)
# # print petl.header(isgs)
# isgs_table1 = petl.cutout(isgs_table, 'Time Desc', 'Grand Total')
# isgs_table2 = petl.select(isgs_table1, 'Time Block', lambda v: v is not None)
# isgs_table3 = petl.melt(isgs_table2, key=['Time Block'])
# isgs_table4 = petl.addfield(isgs_table3, 'Revision', rev_list[-1], 0)
# isgs_table5 = petl.addfield(isgs_table4, 'Issue_Date_Time', None, 0)
# isgs_table6 = petl.addfield(isgs_table5, 'Discom', wrldc_buyer, 0)
# isgs_table7 = petl.addfield(isgs_table6, 'State', 'GUJARAT', 0)
# schdate = datetime.datetime.strptime(schdate, '%d-%m-%Y').strftime('%d-%b-%Y')
# isgs_table8 = petl.addfield(isgs_table7, 'Date', schdate, 0)
# isgs_table9 = petl.rename(isgs_table8, {'Time Block': 'Block_No',
#                                         'variable': 'Station_Name',
#                                         'value': 'Schedule'})
# print isgs_table9

# print utilid, parentutilid
# stoa = wrldc.get_stoa(regionid, schdate, rev_list[-1], parentutilid)
# stoa_table = petl.skip(stoa, 7)
# # print petl.header(stoa_table)
# stoa_table1 = petl.cutout(stoa_table, 'Time Desc', 'Total')
# isgs_table2 = petl.select(stoa_table1, 'Time Block', lambda v: v != '' and v is not None)
# ltoa = wrldc.get_ltoa(regionid, schdate, rev_list[-1], parentutilid)
# ltoa_table = petl.skip(ltoa, 7)
# print petl.header(ltoa_table)[-1]
# stoa_table1 = petl.cutout(stoa_table, 'Time Desc', 'Total')
# isgs_table2 = petl.select(stoa_table1, 'Time Block', lambda v: v != '' and v is not None)
# print list(isgs_table2)
# isgs_table3 = petl.melt(isgs_table2, key=['Time Block'])
# isgs_table4 = petl.addfield(isgs_table3, 'Revision', rev_list[-1], 0)
# isgs_table5 = petl.addfield(isgs_table4, 'Issue_Date_Time', None, 0)
# isgs_table6 = petl.addfield(isgs_table5, 'Discom', wrldc_buyer, 0)
# isgs_table7 = petl.addfield(isgs_table6, 'State', 'GUJARAT', 0)
# schdate = datetime.datetime.strptime(schdate, '%d-%m-%Y').strftime('%d-%b-%Y')
# isgs_table8 = petl.addfield(isgs_table7, 'Date', schdate, 0)
# isgs_table9 = petl.rename(isgs_table8, {'Time Block': 'Block_No',
#                                         'variable': 'Station_Name',
#                                         'value': 'Schedule'})
# exrev_dict, extypeid = get_exrevision_typeid(entdate, 'PXIL')
# print get_px(entdate, exrev_dict.get('revisionNo'), extypeid)
# print wrldc.get_urs(regionid, schdate, rev_list[-1], parentutilid)
# wrldc.get_netschedule(regionid, entdate, rev_list[-1], utilid)

# exrev_dict, extypeid = wrldc.get_exrevision_typeid(schdate, 'PXIL')
# rev = exrev_dict.get('revisionNo')
# ex = wrldc.get_px(schdate, rev, extypeid)
# # print ex[0]
# # print petl.head(ex, 10) #ex #, petl.header(ex)
# hdr = [el.replace("\n", "").replace('-', '') for el in ex[0]]
# ex1 = petl.setheader(ex, hdr)
# cuthdr = [el for el in hdr if 'GEB_Beneficiary' not in el]
# ex2 = petl.cutout(ex1, *tuple(cuthdr[1:]))
# keephdr = ['PXIL_Injection' if 'Injection' in el else 'PXIL_Drawal' for el in hdr if 'GEB_Beneficiary' in el]
# keephdr.insert(0, 'Block_No')
# ex3 = petl.setheader(ex2, keephdr)
# ex4 = petl.melt(ex3, key=['Block_No'])
# ex5 = petl.addfield(ex4, 'Drawl_Type', 'PXIL', 0)
# ex6 = petl.addfield(ex5, 'Revision', rev, 0)
# ex7 = petl.addfield(ex6, 'Issue_Date_Time', None, 0)
# ex8 = petl.addfield(ex7, 'Discom', wrldc_buyer, 0)
# ex9 = petl.addfield(ex8, 'State', 'GUJARAT', 0)
# schdate = datetime.datetime.strptime(schdate, '%d-%m-%Y').strftime('%d-%b-%Y')
# ex10 = petl.addfield(ex9, 'Date', schdate, 0)
# ex11 = petl.rename(ex10,
#                    {'variable': 'Station_Name',
#                     'value': 'Schedule'})
# print ex11
# etl.convert(table1, ('bar'), float)
# test = [["Time Block","Time Desc","West \n- WR - \n- Injection -","West \n- WR - \n- Drawal -","North-West \n- NR to WR - ","West-North \n- WR to NR - ","South-West \n- SR to WR - ","West-South \n- WR to SR - ","East-West \n- ER to WR - ","West-East \n- WR to ER - ","ACBIL \n- W0RAB0 - \n- Injection -","ACBIL \n- W0RAB0 - \n- Drawal -","BALCO \n- W0RBL2 - \n- Injection -","BALCO \n- W0RBL2 - \n- Drawal -","CSEB_Beneficiary \n- W0RCS0 - \n- Injection -","CSEB_Beneficiary \n- W0RCS0 - \n- Drawal -","DB Power \n- W0RDB0 - \n- Injection -","DB Power \n- W0RDB0 - \n- Drawal -","DD_Beneficiary \n- W0RDD0 - \n- Injection -","DD_Beneficiary \n- W0RDD0 - \n- Drawal -","DGEN \n- W0RDM0 - \n- Injection -","DGEN \n- W0RDM0 - \n- Drawal -","DNH_Beneficiary \n- W0RDN0 - \n- Injection -","DNH_Beneficiary \n- W0RDN0 - \n- Drawal -","Dhariwal_Infra \n- W0RDW0 - \n- Injection -","Dhariwal_Infra \n- W0RDW0 - \n- Drawal -","GMR WARORA \n- W0REM0 - \n- Injection -","GMR WARORA \n- W0REM0 - \n- Drawal -","ESSAR_MAHAN \n- W0REP0 - \n- Injection -","ESSAR_MAHAN \n- W0REP0 - \n- Drawal -","ESIL_WR_Beneficiary \n- W0RES0 - \n- Injection -","ESIL_WR_Beneficiary \n- W0RES0 - \n- Drawal -","GANDHAR-APM \n- W0RGA0 - \n- Injection -","GANDHAR-APM \n- W0RGA0 - \n- Drawal -","GCEL \n- W0RGC0 - \n- Injection -","GCEL \n- W0RGC0 - \n- Drawal -","GOA_Beneficiary \n- W0RGO0 - \n- Injection -","GOA_Beneficiary \n- W0RGO0 - \n- Drawal -","GEB_Beneficiary \n- W0RGU0 - \n- Injection -","GEB_Beneficiary \n- W0RGU0 - \n- Drawal -","APL Stage-1 \n- W0RGU1 - \n- Injection -","APL Stage-1 \n- W0RGU1 - \n- Drawal -","APL Stage-2 \n- W0RGU2 - \n- Injection -","APL Stage-2 \n- W0RGU2 - \n- Drawal -","APL Stage-3 \n- W0RGU3 - \n- Injection -","APL Stage-3 \n- W0RGU3 - \n- Drawal -","TORRENT_GUVNL \n- W0RGU4 - \n- Injection -","TORRENT_GUVNL \n- W0RGU4 - \n- Drawal -","JHABUA_IPP \n- W0RJB0 - \n- Injection -","JHABUA_IPP \n- W0RJB0 - \n- Drawal -","JPL-Stg-I \n- W0RJD0 - \n- Injection -","JPL-Stg-I \n- W0RJD0 - \n- Drawal -","JPL-Stg-II \n- W0RJD2 - \n- Injection -","JPL-Stg-II \n- W0RJD2 - \n- Drawal -","JSPL_DCPP \n- W0RJS0 - \n- Injection -","JSPL_DCPP \n- W0RJS0 - \n- Drawal -","KAPS \n- W0RKA0 - \n- Injection -","KAPS \n- W0RKA0 - \n- Drawal -","KSK_MAHANADI \n- W0RKM0 - \n- Injection -","KSK_MAHANADI \n- W0RKM0 - \n- Drawal -","KSTPS I&II \n- W0RKO0 - \n- Injection -","KSTPS I&II \n- W0RKO0 - \n- Drawal -","KSTPS7 \n- W0RKO3 - \n- Injection -","KSTPS7 \n- W0RKO3 - \n- Drawal -","KWPCL \n- W0RKR0 - \n- Injection -","KWPCL \n- W0RKR0 - \n- Drawal -","KAWAS-APM \n- W0RKW0 - \n- Injection -","KAWAS-APM \n- W0RKW0 - \n- Drawal -","LAMKPL \n- W0RLA0 - \n- Injection -","LAMKPL \n- W0RLA0 - \n- Drawal -","MB_POWER \n- W0RMB0 - \n- Injection -","MB_POWER \n- W0RMB0 - \n- Drawal -","MCCPL \n- W0RMC0 - \n- Injection -","MCCPL \n- W0RMC0 - \n- Drawal -","MOUDA \n- W0RMD1 - \n- Injection -","MOUDA \n- W0RMD1 - \n- Drawal -","MOUDA_II \n- W0RMD2 - \n- Injection -","MOUDA_II \n- W0RMD2 - \n- Drawal -","MPSEB_Beneficiary \n- W0RMP0 - \n- Injection -","MPSEB_Beneficiary \n- W0RMP0 - \n- Drawal -","MSEB_Beneficiary \n- W0RMS0 - \n- Injection -","MSEB_Beneficiary \n- W0RMS0 - \n- Drawal -","JPNIGRIE_JNSTPP \n- W0RNI0 - \n- Injection -","JPNIGRIE_JNSTPP \n- W0RNI0 - \n- Drawal -","NSPCL \n- W0RNS0 - \n- Injection -","NSPCL \n- W0RNS0 - \n- Drawal -","RKM_POWER \n- W0RRK0 - \n- Injection -","RKM_POWER \n- W0RRK0 - \n- Drawal -","SASAN \n- W0RSA0 - \n- Injection -","SASAN \n- W0RSA0 - \n- Drawal -","SIPAT I \n- W0RSI1 - \n- Injection -","SIPAT I \n- W0RSI1 - \n- Drawal -","SIPAT II \n- W0RSI2 - \n- Injection -","SIPAT II \n- W0RSI2 - \n- Drawal -","SPECTRUM_ACBIL \n- W0RSP0 - \n- Injection -","SPECTRUM_ACBIL \n- W0RSP0 - \n- Drawal -","TAPS-I \n- W0RTA0 - \n- Injection -","TAPS-I \n- W0RTA0 - \n- Drawal -","TRN_ENERGY \n- W0RTR0 - \n- Injection -","TRN_ENERGY \n- W0RTR0 - \n- Drawal -","VSTPS I \n- W0RVI1 - \n- Injection -","VSTPS I \n- W0RVI1 - \n- Drawal -","VSTPS II \n- W0RVI2 - \n- Injection -","VSTPS II \n- W0RVI2 - \n- Drawal -","VSTPS III \n- W0RVI3 - \n- Injection -","VSTPS III \n- W0RVI3 - \n- Drawal -","VSTPS IV \n- W0RVI4 - \n- Injection -","VSTPS IV \n- W0RVI4 - \n- Drawal -","VSTPS V \n- W0RVI5 - \n- Injection -","VSTPS V \n- W0RVI5 - \n- Drawal -","VANDANA \n- W0RVV0 - \n- Injection -","VANDANA \n- W0RVV0 - \n- Drawal -","STERLITE_WR_PX \n- W3RSE0 - \n- Injection -","STERLITE_WR_PX \n- W3RSE0 - \n- Drawal -"],["1","00:00-00:15","51.50","1.50","0","0","0","0","0","50.00","0","0","0","0","0","0","51.50","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","1.50","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0"]]
# print petl.header(test)

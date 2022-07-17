"""
Crawls NRLDC Data

Quenext 2020
"""
import requests
import datetime
import time
import petl
try:
    from . import sql_load_lib
except:
    import sql_load_lib
import logging
try:
    from . import daterange
except:
    import daterange
import argparse
import csv
from retrying import retry
import pandas as pd
from bs4 import BeautifulSoup
import re 


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
    #print('db_rev_check',dbrev, dbstatus, mins_elapsed)
    if dbstatus == 'RUNNING' and mins_elapsed < 10:
        raise ValueError('Job is in RUNNING state')
    elif dbrev >= -1 and dbstatus == 'SUCCESS':
        startkey = dbrev + 1
    elif dbrev >= -1 and dbstatus != 'SUCCESS' and dbstatus != '':
        startkey = dbrev
    else:
        startkey = -1
    return startkey

class NrldcCrawler(object):
    """Nrldc Crawler."""

    def __init__(self):
        """Constructor."""
        self.user_agent = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                           '(KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36')
        self.url_dict = {
                'estimated-tx-loss': 'https://nrldc.in/Websitedata/Commercial/SemData/NRSchloss.htm'
            }
        self.headers = {'user-agent': self.user_agent}
        self.root_url = 'http://wbes.nrldc.in/'

    def get_region(self, zone):
        """NRLDC Region Mapping."""
        region_dict = {
            "EAST": 1,
            "WEST": 2,
            "NORTH": 3,
            "SOUTH": 4,
            "NORTH EAST": 5,
            "NLDC": 6
        }
        return region_dict.get(zone, None)

    def get_schedule_type(self, schedule):
        """NRLDC Schdule Type Mappping."""
        schdule_dict = {
            'ISGS': 1,
            'MTOA': 2,
            'STOA': 3,
            'LTA': 4,
            'IEX': 5,
            'PXI': 6,
            'URS': 8,
            'RRAS': 9,
            'SCED': 10,
            'REMC': 11,
            'RTM_PXI': 12,
            'RTM_IEX': 13
        }
        return schdule_dict.get(schedule, None)

    def get_revison(self, regionid, scheduledate, schtype):
        """
        Get Revision.
        date format: dd-mm-yyyy
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
                            buyer, getkey, state=None):
        """Search json data of the type list of dictionaries."""
        for dct in data:
            # if dct.get('RegionId') == 2
            # and dct.get('UtilName') == 'GUJARAT'
            # and dct.get('Acronym') == 'GEB_Beneficiary' and
            # dct.get('IsActive') == 1
            if (dct.get('RegionId') == regionid and
                    dct.get('IsActive') == 1 and
                    (dct.get('Acronym') == buyer or dct.get('Acronym') in buyer)
                    and not state):
                #print(dct.get(getkey))
                return dct.get(getkey), dct.get('ParentStateUtilId')
            elif (dct.get('RegionId') == regionid and
                    dct.get('IsActive') == 1 and
                    not buyer and
                    dct.get('UtilName') == state):
                return dct.get(getkey), dct.get('ParentStateUtilId')
            elif (dct.get('RegionId') == regionid and
                    dct.get('IsActive') == 1 and
                    not buyer and not state and
                    dct.get('OwnerName') == discom + '_State'):
                return dct.get(getkey), dct.get('ParentStateUtilId')
        else:
            return None, None  

    def map_utlityname_to_nrldcutilid(self, regionid, nrldc_buyer, schtype):
        """Get the utilityId from the name of the buyer."""
        url = {'NetSchedule': (self.root_url + "ReportNetSchedule"
                               "/GetUtils?regionId={}"),
               'Entitlement': (self.root_url + "Report/GetUtils?regionId={}")}
        r = requests.get(url.get(schtype).format(regionid),
                         headers=self.headers)
        data = r.json()
        return self.search_list_of_dict(data.get('buyers', None),
                                        regionid, nrldc_buyer, 'UtilId')

    @retry(stop_max_attempt_number=6,
           wait_exponential_multiplier=10000,
           wait_exponential_max=20000)
    def get_entitlement(self, regionid, entdate, revision, utilid):
        """Fetch the entitlement."""
        url = (self.root_url + 'Report/GetReportData'
               '?regionId={}&date={}&revision={}&utilId={}'
               '&isBuyer=1&byOnBar=0')
        #print('get_entitlement',regionid, entdate, revision, utilid)
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
    def get_schedule_summary(self, regionid, schdate, revision, utilid):
        """
        Fetch the ISGS.
        http://wbes.nrldc.in/ReportNetSchedule/GetNetSchDetailsIndex?regionId=3
        &scheduleDate=26-06-2020&sellerId=fc7bcec9-f464-4cb8-b4f4-2d1b418f1222
        &revisionNumber=0&scheduleType=2&isJson=false
        http://wbes.nrldc.in/ReportNetSchedule/ExportNetScheduleDetailToPDF
        """
        url = (self.root_url + 'ReportNetSchedule/'
            'ExportNetScheduleSummaryToPDF?scheduleDate={}'
            '&sellerId={}&revisionNumber={}&getTokenValue={}'
            '&fileType={}&regionId={}&byDetails=1'
            '&isBuyer=1&isBuyer=1')
        now = datetime.datetime.now()
        timestamp = int(time.mktime(now.timetuple()))
        url2 = url.format(schdate, utilid,
                          revision, timestamp, 'csv', regionid)
        # print(url2)
        with requests.Session() as s:
            download = s.get(url2, stream=True, headers=self.headers)
            if download.status_code >= 400:
                raise Exception('Error in getting schedule')
            decoded_content = download.content.decode('utf-8')
            # Getting rid of null values '\0' from the data
            data = csv.reader((line.replace('\0', '')
                                for line in decoded_content.splitlines()),
                                delimiter=',')
            my_list = list(data)
            # Getting rid of empty list row
            data =  [x for x in my_list if x]
            # dataframe                  
            # pd.DataFrame(data[1:],columns=data[0])
            res = {data[0][i]: float(data[97][i]) for i in range(len(data[0])) 
                if data[97][i].replace('.','').replace('-','').isdigit()} 
            return res

    @retry(stop_max_attempt_number=6,
           wait_exponential_multiplier=10000,
           wait_exponential_max=20000)
    def get_schedule(self, regionid, schdate, revision, utilid, schtype):
        """
        Fetch the ISGS.
        http://wbes.nrldc.in/ReportNetSchedule/GetNetSchDetailsIndex?regionId=3
        &scheduleDate=26-06-2020&sellerId=fc7bcec9-f464-4cb8-b4f4-2d1b418f1222
        &revisionNumber=0&scheduleType=2&isJson=false
        http://wbes.nrldc.in/ReportNetSchedule/ExportNetScheduleDetailToPDF
        """
        url = (self.root_url + 'ReportNetSchedule/'
               'ExportNetScheduleDetailToPDF?scheduleDate={}'
               '&sellerId={}&revisionNumber={}&getTokenValue={}'
               '&fileType={}&schType={}')
        now = datetime.datetime.now()
        timestamp = int(time.mktime(now.timetuple()))
        url2 = url.format(schdate, utilid,
                          revision, timestamp, 'csv', schtype)
        #print(url2)
        with requests.Session() as s:
            try:
                download = s.get(url2, stream=True, headers=self.headers)
                if download.status_code >= 400:
                    raise Exception('Error in getting schedule')
                decoded_content = download.content.decode('utf-8')
                # Getting rid of null values '\0' from the data
                data = csv.reader((line.replace('\0', '')
                                   for line in decoded_content.splitlines()),
                                  delimiter=',')
                my_list = list(data)
                # Getting rid of empty list row
                return [x for x in my_list if x]                  
            except Exception:
                return None
                                         

    @retry(stop_max_attempt_number=6,
           wait_exponential_multiplier=10000,
           wait_exponential_max=20000)
    def get_estimated_losses(self):
        """Fetch the entitlement."""
        url = self.url_dict['estimated-tx-loss'] 
        page_content = requests.get(url, headers=self.headers).content
        # soup = BeautifulSoup(page_content, "lxml")
        df_list = pd.read_html(page_content)
        df = df_list[0].drop(columns=[0]).drop([0,1,4,5,6])
        df.columns = ['From', 'To', 'Loss']
        df['From'] =  pd.to_datetime(df['From'], format='%d-%b-%Y')
        df['To'] =  pd.to_datetime(df['To'], format='%d-%b-%Y')        
        return df


def nrldc_entitlement(date, nrldc_buyer, state, ftype, dns, discom):
    """Get the Entitlement."""
    logger = logging.getLogger("nrldc_entitlement")
    nrldc = NrldcCrawler()
    region = 'NORTH'
    tablenm = 'nrldc_entitlements_stg'
    regionid = nrldc.get_region(region)
    schtype = 'Entitlement'
    rev_list = nrldc.get_revison(regionid, date.strftime('%d-%m-%Y'), schtype)
    #print('rev_list', rev_list[-1], date.strftime('%d-%m-%Y'), regionid, nrldc_buyer, schtype)
    utilid, parentutilid = nrldc.map_utlityname_to_nrldcutilid(regionid,
                                                               nrldc_buyer,
                                                               schtype)
    #print('utilid, parentutilid', utilid, parentutilid)
    for rev in rev_list[::-1]:
        # print('rev', rev)
        dbrev = db_rev_check(dns, date, state, ftype)
        # print('dbrev', dbrev)
        if rev >= dbrev:
            try:
                sql_load_lib.sql_sp_load_exec(dns,
                                              date.strftime('%Y-%m-%d'),
                                              rev,
                                              state,
                                              ftype,
                                              'RUNNING',
                                              'UPINSPNT')
                ent = nrldc.get_entitlement(regionid,
                                            date.strftime('%d-%m-%Y'),
                                            rev,
                                            utilid)
                # print('Entitlement data started.', ent)
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
                                               discom, 0)
                    ent_table7 = petl.addfield(ent_table6, 'State', state, 0)
                    entdate = date.strftime('%d-%b-%Y')
                    ent_table8 = petl.addfield(ent_table7, 'Date', entdate, 0)
                    ent_table9 = petl.rename(ent_table8,
                                             {'Time Block': 'Block_No',
                                              'variable': 'Station_Name',
                                              'value': 'Schedule'})                   
                    ent_table10 = petl.convert(ent_table9, 'Station_Name', lambda rec: re.sub("[\(\[].*?[\)\]]", "", rec))
                    logger.info('Entitlement data processed.')
                    logger.debug(ent_table10)
                    # print('Entitlement data processed.')
                    # print(ent_table10)
                    sql_load_lib.sql_table_insert_exec(dns,
                                                       tablenm,
                                                       list(ent_table10)[1:])
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

class NrldcStateSchedule(object):
    """State Schedule Object."""

    def __init__(self, date, nrldc_buyer, state, dns, discom):
        """Constructor."""
        self.nrldc = NrldcCrawler()
        # self.date = datetime.datetime.strptime(date, '%d-%m-%Y')
        self.date = date
        self.nrldc_buyer = nrldc_buyer
        self.discom = discom
        self.state = state
        self.tablenm = 'nrldc_state_drawl_schedule_stg'
        self.dns = dns
        region = 'NORTH'
        schtype = 'NetSchedule'
        self.logger = logging.getLogger("NrldcStateSchedule")
        self.regionid = self.nrldc.get_region(region)
        self.rev_list = self.nrldc.get_revison(self.regionid,
                                               self.date.strftime('%d-%m-%Y'),
                                               schtype)
        self.utilid, self.parentutilid = \
            self.nrldc.map_utlityname_to_nrldcutilid(self.regionid,
                                                     self.nrldc_buyer,
                                                     schtype)

    def run(self):
        """Run for all the functions."""
        runseq = ['ISGS', 'MTOA', 'STOA', 'LTA', 'IEX', 'PXI', 'URS', 
            'RRAS', 'SCED', 'REMC', 'RTM_PXI', 'RTM_IEX']
        for rev in self.rev_list[::-1]:
            for ft in runseq:
                self.nrldc_netschedule(rev, ft)

    def nrldc_netschedule(self, revision, ftype):
        """Get ISGS/URS."""
        dbrev = db_rev_check(self.dns, self.date,
                                              self.state, ftype)
        self.logger.info('dbrev : ' + str(dbrev) + ftype)
        #print('dbrev : ' + str(dbrev) + ' ' + ftype, revision)
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
                del self.nrldc
                self.nrldc = NrldcCrawler()
                schedule_type = self.nrldc.get_schedule_type(ftype)
                #fetch the urls that have not qual to 0.0
                sched_dict = self.nrldc.get_schedule_summary(self.regionid, schdate, revision, self.utilid)
                if sched_dict[ftype] != 0.0:
                    isgs = self.nrldc.get_schedule(self.regionid, schdate,
                                                revision, self.utilid,
                                                schedule_type)
                    #print('isgs schedule_type', isgs,schedule_type)
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
                                    for i in range(len(hdr))]
                            isgs_table1 = petl.setheader(isgs_table, hdr2)
                            # cut_indx = [i for i, buyer in enumerate(filt)
                            #             if buyer[:4] != self.nrldc_buyer[:4]][1:]
                            cut_indx = [i for i, buyer in enumerate(filt)
                                        if not buyer]
                        elif ftype in ('MTOA', 'STOA', 'LTA', 'IEX',
                                    'PXI', 'URS', 'SCED',
                                    'REMC', 'RTM_PXI', 'RTM_IEX'):
                            hdr2 = [hdr[i] + '^|^' + filt[i] + '^|^' + apprno[i]
                                    if not isgs[hdr_end_idx][i]
                                    else isgs[hdr_end_idx][i]
                                    for i in range(len(hdr))]
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
                                                    self.discom, 0)
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
                        print('isgs_table14:',isgs_table14)
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
                                                    self.discom, 0)
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

def tx_losses(dsnfile, ftype):
    tabname = 'nrldc_schedule_est_trans_loss_stg'
    try:
        nrldc = NrldcCrawler()
        est_loss_df = nrldc.get_estimated_losses()
        data = est_loss_df.values.tolist()
        sql_load_lib.sql_table_insert_exec(dsnfile, tabname, data)
        sql_load_lib.sql_sp_load_exec(dsnfile, datetime.datetime.today()
                                    .strftime('%Y-%m-%d'), None,
                                    'NRLDC', ftype, 'SUCCESS',
                                    'UPINSPNT')
        # print('FINISHED')
    except Exception:
        sql_load_lib.sql_sp_load_exec(dsnfile,
                                    datetime.datetime.today()
                                    .strftime('%Y-%m-%d'),
                                    None,
                                    'NRLDC', ftype, 'FAILED',
                                    'UPINSPNT')
        raise
    return        

def main(args):
    """Main Function."""
    for date in daterange.daterange(args.start_date, args.end_date):
        if args.ftype == 'Entitlement':
            nrldc_entitlement(date, args.nrldcbuyer,
                              args.state, args.ftype, args.dns, args.discom)
        elif args.ftype == 'StateSchedule':
            wss = NrldcStateSchedule(date, args.nrldcbuyer,
                                     args.state, args.dns, args.discom)
            wss.run()
        elif args.ftype == 'TxLosses':
            tx_losses(args.dns, args.ftype)       

if __name__ == '__main__':
    ARG = argparse.ArgumentParser(description=("Fetches NRLDC data and"
                                  "uploads into staging table."))
    ARG.add_argument('-s', '--state', dest='state',
                     help='State to fetch data for')
    ARG.add_argument('-wb', '--nrldcbuyer', dest='nrldcbuyer',
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
                     help=('Entitlement or State Schedule or TxLosses'
                           '["Entitlement", "StateSchedule", "TxLosses"]'), required=True)
    ARG.add_argument('-d', '--discom', dest='discom',
                     help=('Discom Name'), required=True)                           
    main(ARG.parse_args())

#python nrldc_crawler.py -s UTTARAKHAND -wb UTTARAKHAND_STATE -sd 27-06-2020 -ed 27-06-2020 -m ../config/sqldb_connection_config.txt -t TxLosses -d UPCL
# python nrldc_crawler.py -s UTTARAKHAND -wb UTTARAKHAND_STATE -sd 27-06-2020 -ed 27-06-2020 -m ../config/sqldb_connection_config.txt -t Entitlement -d UPCL
# python nrldc_crawler.py -s UTTARAKHAND -wb UTTARAKHAND_STATE -sd 27-06-2020 -ed 27-06-2020 -m ../config/sqldb_connection_config.txt -t StateSchedule -d UPCL
"""
Crawls https://uksldc.com/ViewReportSchedule/Index/GetNetSchedule.

Gets the Genrator Scheduel.
"""

import requests
from bs4 import BeautifulSoup
from retrying import retry
import pandas as pd
import logging
try:
    from . import daterange
except:
    import daterange
import argparse
try:
    from . import sql_load_lib
except:
    import sql_load_lib
import datetime
# import re
# import dateutil.parser
# import petl
# import dbconn


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

class InternalCrawler(object):
    """Internal Gen/Schedule Crawler."""

    def __init__(self, date):
        """Constructor."""
        self.user_agent = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                           '(KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36')
        self.date = date

    def get_revision(self, entitytype, entityid):
        """returns a list of revision"""
        with requests.Session() as s:
            dataload = {'scheduleDate': self.date,
                'formate': 'M/d/yyyy',
                'entityType': entitytype,
                'entityid': entityid}
            headers={
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                'accept-encoding': 'gzip, deflate, br',
                'accept-language': 'en-US,en;q=0.9,id;q=0.8,ms;q=0.7,mr;q=0.6',
                'Host': 'uksldc.com',
                'Origin': 'https://uksldc.com',
                'Referer': 'https://uksldc.com/ViewReportSchedule/Index/GetNetSchedule',
                'upgrade-insecure-requests': '1',
                'user-agent': self.user_agent
            }
            r=s.post('https://uksldc.com/ViewReportSchedule/GetRevisionlist',headers=headers,data=dataload)
            if r.status_code != 200:
                print(dataload)
                raise Exception('Error in getting revision  response code {}'.format(r.status_code))
            else:
                return r.json().get('revisionList')

    @retry(stop_max_attempt_number=6,
           wait_exponential_multiplier=10000,
           wait_exponential_max=20000)
    def get_declared_capacity(self, revision):
        """returns a pandas df"""
        #revlst = get_revision(entitytype=1, entityid=-1)
        url = ('https://uksldc.com/ViewReportSchedule/ExportExcel?'
            'fromDate={}&SLDCrevision={}&formate={}&entityid={}'
            '&customertype={}&customername={}&entityType={}')
        r = requests.get(url.format(self.date, revision, 'M/d/yyyy', -1, 1, '---Select---', 1),
                        headers={'user-agent': self.user_agent})
        if r.status_code != 200:
            print(url.format(self.date, revision, 'M/d/yyyy', -1, 1, '---Select---', 1))
            raise Exception('Error in getting internal declared capacity response code {}'.format(r.status_code))
        else:
            return pd.read_html(r.content)[0]

    @retry(stop_max_attempt_number=6,
           wait_exponential_multiplier=10000,
           wait_exponential_max=20000)
    def get_drawl_schedule(self, revision):
        """returns a pandas df"""
        #revlst = get_revision(entitytype=5, entityid=999)
        url = ('https://uksldc.com/ViewReportSchedule/ExportExcel?'
               'fromDate={}&SLDCrevision={}&formate={}&entityid={}'
               '&customertype={}&customername={}&entityType={}')
        r = requests.get(url.format(self.date, revision, 'M/d/yyyy', 999, 3, 'Genco', 5),
                        headers={'user-agent': self.user_agent})
        if r.status_code != 200:
            raise Exception('Error in getting internal drawl schdeule')
        else:
            return pd.read_html(r.content)[0]


def upcl_declared_capacity(date, state, dns, discom):
    """Get the Internal Generation Declared Capacity."""
    logger = logging.getLogger("upcl_declared_capacity")
    dc = InternalCrawler(date.strftime('%m/%d/%Y'))
    tablenm = 'internal_declared_capacity_stg'
    rev_list = dc.get_revision(entitytype=1, entityid=-1)
    rev_list = list(map(int, rev_list)) 
    # print('rev_list', rev_list)
    ftype = 'upcl_declared_capacity'
    for rev in rev_list[::-1]:
        #print('rev', rev)
        dbrev = db_rev_check(dns, date, state, ftype)
        #print('dbrev', dbrev)
        if rev >= dbrev:
            try:
                sql_load_lib.sql_sp_load_exec(dns,
                                              date.strftime('%Y-%m-%d'),
                                              rev,
                                              state,
                                              ftype,
                                              'RUNNING',
                                              'UPINSPNT')
                entdf = dc.get_declared_capacity(rev)
                # print('Entitlement data started.', ent)
                if not entdf.empty and len(entdf):
                    df = entdf.drop(columns=['TimeDesc', 'Total'])
                    df = df[df['TimeBlock'].str.isdigit()]
                    df_unpivoted = df.melt(id_vars=['TimeBlock'], var_name='entity_name', value_name='schedule')
                    df_unpivoted['date'] = date.strftime('%Y-%m-%d')
                    df_unpivoted['state'] = state
                    df_unpivoted['discom'] = discom
                    df_unpivoted['revision'] = rev
                    df_unpivoted['entity_type'] = 'UNKNOWN'
                    df_unpivoted = df_unpivoted.rename(columns={'TimeBlock': 'block_no'})
                    df_final = df_unpivoted[['date', 'state', 'discom', 'revision', 
                        'block_no', 'entity_name', 'entity_type', 'schedule']]
                    logger.info('Internal declared capacity data processed.')
                    logger.debug(df_final)
                    # print('Entitlement data processed.')
                    #print(df_final.values.tolist())
                    sql_load_lib.sql_table_insert_exec(dns,
                                                       tablenm,
                                                       df_final.values.tolist())
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

def upcl_drawl_schedule(date, state, dns, discom):
    """Get the internal drawl schedule."""
    logger = logging.getLogger("upcl_drawl_schedule")
    dc = InternalCrawler(date.strftime('%m/%d/%Y'))
    tablenm = 'internal_drawl_schedule_stg'
    rev_list = dc.get_revision(entitytype=5, entityid=999)
    rev_list = list(map(int, rev_list)) 
    print('rev_list', rev_list)
    ftype = 'upcl_drawl_schedule'
    for rev in rev_list[::-1]:
        #print('rev', rev)
        dbrev = db_rev_check(dns, date, state, ftype)
        #print('dbrev', dbrev)
        if rev >= dbrev:
            try:
                sql_load_lib.sql_sp_load_exec(dns,
                                              date.strftime('%Y-%m-%d'),
                                              rev,
                                              state,
                                              ftype,
                                              'RUNNING',
                                              'UPINSPNT')
                entdf = dc.get_drawl_schedule(rev)
                #print('Entitlement data started.', entdf.columns, rev, date)
                if not entdf.empty and len(entdf):
                    if 'ISGS' in entdf.columns:
                        df = entdf.drop(columns=['TimeDesc', 'Total', 'ISGS'])
                    else:
                        df = entdf.drop(columns=['TimeDesc', 'Total'])
                    df = df[df['TimeBlock'].str.isdigit()]
                    df_unpivoted = df.melt(id_vars=['TimeBlock'], var_name='entity_name', value_name='schedule')
                    df_unpivoted['date'] = date.strftime('%Y-%m-%d')
                    df_unpivoted['state'] = state
                    df_unpivoted['discom'] = discom
                    df_unpivoted['revision'] = rev
                    df_unpivoted['entity_type'] = 'UNKNOWN'
                    df_unpivoted = df_unpivoted.rename(columns={'TimeBlock': 'block_no'})
                    df_final = df_unpivoted[['date', 'state', 'discom', 'revision', 
                        'block_no', 'entity_name', 'entity_type', 'schedule']]
                    logger.info('Internal declared capacity data processed.')
                    logger.debug(df_final)
                    # print('Entitlement data processed.')
                    #print(df_final.values.tolist())
                    sql_load_lib.sql_table_insert_exec(dns,
                                                       tablenm,
                                                       df_final.values.tolist())
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

def main(args):
    """Main Function."""
    for date in daterange.daterange(args.start_date, args.end_date):
        if args.ftype == 'DeclaredCapacity':
            upcl_declared_capacity(date, args.state, args.dns, args.discom)
        elif args.ftype == 'StateSchedule':
            upcl_drawl_schedule(date, args.state, args.dns, args.discom)
  

if __name__ == '__main__':
    ARG = argparse.ArgumentParser(description=("Fetches UPCL data and"
                                  "uploads into staging table."))
    ARG.add_argument('-s', '--state', dest='state',
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
                     help=('Declared Capacity or State Schedule'
                           '["DeclaredCapacity", "StateSchedule"]'), required=True)
    ARG.add_argument('-d', '--discom', dest='discom',
                     help=('Discom Name'), required=True)                           
    main(ARG.parse_args())

    # python upcl_genschedule_crawler.py -s UTTARAKHAND -sd 27-06-2020 -ed 27-06-2020 -m ../config/sqldb_connection_config.txt -t DeclaredCapacity -d UPCL
    # python upcl_genschedule_crawler.py -s UTTARAKHAND -sd 27-06-2020 -ed 27-06-2020 -m ../config/sqldb_connection_config.txt -t StateSchedule -d UPCL
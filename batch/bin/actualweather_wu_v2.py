###
#
# Copyright (c) 2017, Quenext
# Author Biswadip Paul
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.
# 3. Neither the name of the organization nor the names of its
#    contributors may be used to endorse or promote products derived from
#    this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
#
###

# import argparse
# from argparse import RawTextHelpFormatter
# import sys
# import json
import os
import time
from datetime import datetime
from datetime import timedelta
# import pytz
import logging
import requests
import dbconn
import csv
# import shutil
import geopy
from geopy.distance import VincentyDistance
# from urlparse import urlparse
from threading import Thread
# import httplib
# import sys
from Queue import Queue
# from retrying import retry


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
SETTINGS = {
    "api_key": "61e47cd17f994e2ba47cd17f990e2bcb",
    # "api_key": "918ee4218551be93ba4b2d79cf933b14"
}


class ActualData:
    """Fetch Actual Data from Wunderground."""

    # data = None
    data_type = 'csv'
    # args = None
    concurrent = 100
    q = Queue(concurrent * 2)

    def __init__(self):
        """Initilization constructor."""
        self.api_key = SETTINGS.get('api_key')
        self.metric_flag = SETTINGS.get('metric')
        self.base_url = ("http://cleanedobservations.wsi.com/CleanedObs.svc/"
                         "GetObs?version=2&time={tm}&lat={lat}&long={long}"
                         "&startDate={sd}&endDate={ed}&interval={interval}"
                         "&units={units}&format={fmt}"
                         "&userKey={uk}&delivery={dlvy}")

    def make_url(self, lat, lon, stdate, endate,
                 path, datesuffix,
                 interval='hourly', units='metric',
                 fmt='csv', delivery='file', tz='gmt'):
        """Genearte the uls."""
        url = self.base_url.format(tm=tz, lat=lat, long=lon, sd=stdate,
                                   ed=endate, interval=interval, units=units,
                                   fmt=fmt, uk=self.api_key, dlvy=delivery)
        filename = 'Actual_' + str(lat) + '_' + str(lon) + '_' + datesuffix
        return url, path, filename

    # @retry(wait_fixed=5000, stop_max_attempt_number=3)
    def geturl(self, ourl):
        """Fetch the url."""
        req = requests.get(ourl, timeout=60)
        if req.status_code != 200:
            logger.error('Error : %s for %s', str(req.status_code), ourl)
        req.raise_for_status()
        return req.content

    def flow(self):
        """Process Flow."""
        while True:
            url, path, filename = self.q.get()
            # print url, filename
            try:
                data = self.geturl(url)
                self.save_file(data, path, filename)
            except Exception as e:
                logger.error('Error : %s for %s', str(e), url)
            self.q.task_done()
            # time.sleep(10)

    def run(self, latlonglist, start_date, end_date, path):
        """Fetch all the urls concurrently."""
        urllist = []
        for datex in daterange(start_date, end_date):
            startdt = datex.strftime('%m/%d/%Y')
            enddt = (datex + timedelta(1)).strftime('%m/%d/%Y')
            for lat, lon in latlonglist:
                urllist.append(self.make_url(lat, lon, startdt, enddt,
                                             path, datex.strftime('%d_%m_%Y')))
        # print urllist, path
        for i in range(self.concurrent):
            t = Thread(target=self.flow)
            t.daemon = True
            t.start()

        try:
            for indx, url in enumerate(urllist, 1):
                self.q.put(url)
                if indx % 100 == 0:
                    time.sleep(10)
            self.q.join()
        except Exception as err:
            logger.error('Error : %s', str(err))
            # raise

    # def fetch_data(self, lat, lon, stdate, endate,
    #                interval='hourly', units='metric',
    #                fmt='csv', delivery='file', tz='gmt'):
    #     """Grab data from Weather Underground API."""
    #     url = self.base_url.format(tm=tz, lat=lat, long=lon, sd=stdate,
    #                                ed=endate, interval=interval, units=units,
    #                                fmt=fmt, uk=self.api_key, dlvy=delivery)
    #     self.data_type = fmt
    #     try:
    #         logger.info('Fetching url: %s', url)
    #         req = requests.get(url)
    #         logger.info('Status code: %s', req.status_code)
    #         if self.data_type == 'json':
    #             self.data = json.loads(req.content)
    #         else:
    #             self.data = req.content
    #         logger.info('Req Data: %s', self.data)
    #     except Exception as err:
    #         logger.error("Error: %s", err)
    #         raise

    def save_file(self, data, path, filenm):
        """Save the data to a file in the OS."""
        filename_suffix = self.data_type
        file_dir_fnm = os.path.join(path, filenm + "." + filename_suffix)
        if os.path.exists(file_dir_fnm):
            os.remove(file_dir_fnm)
        with open(file_dir_fnm, "wb") as file_write:
            file_write.write(data)
        file_write.close()
        logger.info('File saved : %s', file_dir_fnm)
        return file_dir_fnm


class DbFetchLocations():
    """Update Fetch the database with data."""

    def __init__(self, dsnfile, discom):
        """Database connection init."""
        self.connection = dbconn.connect(dsnfile)
        self.cursor = self.connection.cursor()
        self.discom = discom

    def fetch_locations(self):
        """Get all the location attributes from mdaws_wunderground_map."""
        logger.info('Fetching location for : %s', self.discom)
        try:
            self.cursor.execute("""SELECT distinct e.latitude, e.longitude
                from power.contract_trade_master a,
                     power.counter_party_master c,
                     power.counter_party_master d,
                     power.unit_master e,
                     power.unit_type f
                where a.counter_party_1_fk = c.counter_party_master_pk
                and   a.counter_party_2_fk = d.counter_party_master_pk
                and c.counter_party_name = %s
                and d.counter_party_name = e.unit_name
                and f.unit_type_pk = e.unit_type_fk
                and e.latitude is not null
                and e.longitude is not null
                and f.unit_type_name in ('WIND', 'SOLAR')
                and a.delete_ind = 0
                and c.delete_ind = 0
                and d.delete_ind = 0
                and e.delete_ind = 0
                and f.delete_ind = 0""", (self.discom,))
            results = self.cursor.fetchall()
            self.cursor.close()
            return results
        except Exception as err:
            logger.error("Error fetching location: %s : for discom : %s",
                         str(err), self.discom)
            if self.connection.open:
                self.cursor.close()
            raise


def daterange(start_date=None, end_date=None):
    """Iterate over a Date range else uses todays date as default."""
    if isinstance(start_date, str) and isinstance(end_date, str):
        start_date = datetime.strptime(start_date, '%d-%m-%Y')
        end_date = datetime.strptime(end_date, '%d-%m-%Y')
    if start_date < end_date:
        for now in xrange(0, (end_date - start_date).days + 1):
            logger.debug(now)
            yield start_date + timedelta(now)
    else:
        raise Exception('Invalid Start and End Date')
    return


def generate_lat_lon(seedlat, seedlon, distance, bearing):
    """
    Generate point at an bearning angle and distance from
    the seed lat and long.
    """
    origin = geopy.Point(seedlat, seedlon)
    destination = \
        VincentyDistance(kilometers=distance).destination(origin, bearing)
    return (destination.latitude, destination.longitude)


def get_grid_lat_lon(latlonpts, maxdistance, steps):
    """Get the grid pts at different bearing(angle)."""
    outputpts = []
    for pts in latlonpts:
        for dist in xrange(0, maxdistance + 1, steps):
            if dist > 0:
                for bear in xrange(0, 360, 90):
                    outputpts.append(generate_lat_lon(pts[0], pts[1],
                                                      dist, bear))
            else:
                outputpts.append(generate_lat_lon(pts[0], pts[1], dist, 0))
    return list(set(outputpts))


# Test code
# forecast = ActualData(args)
# forecast.output_data(parser)
# dsn = '../config/sqldb_connection_config.txt'
dsn = '../config/sqldb_dev_gcloud.txt'
loc = DbFetchLocations(dsn, 'GUVNL')
ldir = '/Users/biswadippaul/Projects/quenext-dev-latest/default/ems/batch/data/weather/'
# adir = '/Users/biswadippaul/Projects/quenext-dev-latest/default/ems/batch/data_archive/weather/'
all_loc = loc.fetch_locations()
imputed_loc = get_grid_lat_lon(all_loc, 50, 50)
start_date = '21-01-2016'
end_date = '31-01-2016'

# print len(imputed_loc)
# print imputed_loc[0]
actuals = ActualData()
actuals.run(imputed_loc, start_date, end_date,
            ldir)
# for datex in daterange(start_date, end_date):
#     startdt = datex.strftime('%m/%d/%Y')
#     enddt = (datex + timedelta(1)).strftime('%m/%d/%Y')
#     actuals.run(imputed_loc, startdt, enddt,
#                 ldir, datex.strftime('%d-%m-%Y'))
    # time.sleep(60)
    # actuals.run([imputed_loc[0]], startdt, enddt,
    #             ldir, datex.strftime('%d-%m-%Y'))
    # break

    # for lat, lon in imputed_loc:
        # loc_str = str(lat) + "_" + str(lon)
#         # print loc, lat, lon
#         # print 'datex', datex, datex.strftime('%d-%m-%Y')
        # actuals = ActualData()
        # actuals.fetch_data(lat, lon, startdt, enddt)
#         act_data = actuals.data
# #         print act_data, type(act_data)
# #         # actuals.append_data(['Location'], loc)
#         filename = actuals.save_file(ldir, loc_str + '_' +
#                                      datex.strftime('%d-%m-%Y'))
#         # print filename
#         dbupdate = DbUploadData(dsn, filename)
#         dbupdate.csv_to_data(['Datasource', 'Datatype'],
#                              ['IBMWEATHERCHANNEL', 'ACTUAL'])
#         dbupdate.db_upload_data()
#         dst_file = os.path.join(adir,
#                                 os.path.basename(filename))
#         # print filename, dst_file, os.path.basename(filename), os.path.exists(dst_file)
#         if os.path.exists(dst_file):
#             os.remove(dst_file)
#         shutil.move(filename, adir)




# if __name__ == '__main__':
#     main()
#     sys.exit()

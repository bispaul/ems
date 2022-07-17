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

import logging
import json
import requests
import dbconn
import csv
from dateutil.parser import parse
import datetime
import os
# import shutil

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
SETTINGS = {"current": {
    "api_key": "3551e2206e4c4c1fbb7872bce39e8ac1"}
}


class CurrentConditionsData:
    """Fetch Forecast Data from Wunderground."""

    data = None
    data_type = 'json'
    args = None

    def __init__(self):
        """Initilization constructor."""
        self.api_key = SETTINGS.get('current').get('api_key')
        self.base_url = ("https://api.weather.com/v1/geocode/{lat}/{long}"
                         "/observations/current.json?language=en-US"
                         "&units={units}&apiKey={apikey}")

    def fetch_data(self, lat, lon, units):
        """Grab data from Weather Underground API."""
        url = self.base_url.format(lat=lat, long=lon,
                                   units=units, apikey=self.api_key)
        try:
            logger.info('Fetching url: %s', url)
            req = requests.get(url)
            logger.info('Status code: %s', req.status_code)
            if self.data_type == 'json':
                self.data = json.loads(req.content)
            else:
                self.data = req.content
            logger.debug('Req Data: %s', self.data)
        except Exception as err:
            logger.error("Error: %s", err)
            raise

    def parse_current(self, location=None):
        """Parse the current conditions json file.

        {u'metric': {u'snow_7day': None, u'pchange': 2.03, u'vis': 16.09,
         u'snow_3day': None, u'snow_season': None, u'snow_2day': None,
         u'temp_change_24hour': -17, u'snow_1hour': 0.0, u'precip_1hour': 0.0,
         u'wspd': 2, u'precip_mtd': None, u'rh': 64,
         u'obs_qualifier_50char': None, u'temp_max_24hour': 26,
         u'ceiling': None, u'wc': 21, u'temp_min_24hour': 12,
         u'snow_mtd': None, u'precip_6hour': 0.0, u'snow_ytd': None,
         u'precip_24hour': 0.0, u'hi': 21, u'precip_7day': None,
         u'precip_2day': None, u'obs_qualifier_100char': None,
         u'altimeter': 1017.95, u'snow_6hour': 0.0, u'mslp': 1017.9,
         u'temp': 21, u'gust': None, u'dewpt': 13, u'precip_3day': None,
         u'precip_ytd': None, u'snow_24hour': 0.0,
         u'obs_qualifier_32char': None, u'feels_like': 21},
         u'expire_time_gmt': 1487424475, u'vocal_key': u'OT69:OX2900',
         u'sunrise': u'2017-02-18T06:54:33+0530', u'uv_index': 0,
         u'uv_desc': u'Low', u'wdir': 260, u'obs_qualifier_severity': None,
         u'clds': u'SCT', u'icon_code': 29, u'wxman': u'wx1600',
         u'obs_time_local': u'2017-02-18T18:47:55+0530', u'day_ind': u'N',
         u'icon_extd': 2900, u'wdir_cardinal': u'W', u'ptend_desc': u'Rising',
         u'uv_warning': 0, u'class': u'observation',
         u'sky_cover': u'Partly Cloudy', u'obs_time': 1487423875,
         u'ptend_code': 1, u'phrase_12char': u'P Cloudy',
         u'obs_qualifier_code': None, u'sunset': u'2017-02-18T18:06:17+0530',
         u'phrase_22char': u'Partly Cloudy',
         u'phrase_32char': u'Partly Cloudy', u'dow': u'Saturday'}
        """
        logger.info('Parsing json data.')
        if self.data.get('metadata').get('errors'):
            for err in self.data.get('metadata').get('errors'):
                logger.error(err.get('error').get('code') + ':' +
                             err.get('error').get('message'))
                logger.error('Status Code: %s',
                             self.data.get('metadata').get('status_code'))
            return False
        elif (self.data.get('metadata').get('status_code') == 200):
            val = []
            val.append(["Date", "Hour", "TemperatureCelsius",
                        "FeelsLikeCelsius", "WindSpeedKph",
                        "WindDirirectionCardinal", "WindDirectionDegrees",
                        "WindGustKph", "WindChillCelsius",
                        "DewpointTemperatureCelsius",
                        "HeatIndexCelsius", "RelativeHumidityPercent",
                        "Conditions", "CloudCoveragePercent",
                        "MslPressureMillibars", "Pressuretend_Desc",
                        "PressureChange3hrs", "TempChange24hour",
                        "TempMax24hour", "TempMin24hour",
                        "Snow1hrCentimeter", "Snow6hrCentimeter",
                        "Snow24hrCentimeter", "Precip1hrMillimeter",
                        "Precip6hrMillimeter", "Precip24hrMillimeter",
                        "Latitude", "Longitude", "DateTimeGMT",
                        "DateTimeLwt", "ExpireTimeLocal", "ExpireTimeGMT",
                        "Location", "Forecast_Type"])
            longitude = self.data['metadata']['longitude']
            latitude = self.data['metadata']['latitude']
            expire_time_gmt = self.data['metadata']['expire_time_gmt']
            gexpire_time_gmt = datetime.datetime.utcfromtimestamp(
                int(expire_time_gmt)).strftime('%Y-%m-%d %H:%M:%S')
            # logger.info(self.data['observation'])
            item = self.data['observation']
            expire_time_gmt = item['expire_time_gmt']
            lexpire_time_gmt = datetime.datetime.fromtimestamp(
                int(expire_time_gmt)).strftime('%Y-%m-%d %H:%M:%S')
            gmt_datetime = datetime.datetime.utcfromtimestamp(
                int(item['obs_time'])).strftime('%Y-%m-%d %H:%M:%S')
            local_datetime = parse(item["obs_time_local"])
            date = local_datetime.strftime('%d-%m-%Y')
            time = local_datetime.strftime('%H:%M:%S')
            cloudcoverage = {'Clear': 0.09375 * 100,
                             'Partly Cloudy': .59375 * 100,
                             'Mostly Cloudy': .75 * 100}
            val.append([date, time, item["metric"]["temp"],
                        item["metric"]["feels_like"],
                        item["metric"]["wspd"], item['wdir_cardinal'],
                        item['wdir'], item["metric"]['gust'],
                        item["metric"]['wc'], item["metric"]['dewpt'],
                        item["metric"]['hi'], item["metric"]['rh'],
                        item['phrase_22char'],
                        cloudcoverage.get(item['clds'], None),
                        item["metric"]['mslp'],
                        item["ptend_desc"], item["metric"]["pchange"],
                        item["metric"]['temp_change_24hour'],
                        item["metric"]['temp_max_24hour'],
                        item["metric"]['temp_min_24hour'],
                        item["metric"]['snow_1hour'],
                        item["metric"]['snow_6hour'],
                        item["metric"]['snow_24hour'],
                        item["metric"]['precip_1hour'],
                        item["metric"]['precip_6hour'],
                        item["metric"]['precip_24hour'],
                        latitude, longitude, gmt_datetime,
                        local_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                        lexpire_time_gmt, gexpire_time_gmt,
                        location])
            return val
        else:
            logger.error('Status Code: %s',
                         self.data.get('metadata').get('status_code'))
            return False

    def save_csv(self, path, filenm, data):
        """Save the data to a file in the OS."""
        file_dir_fnm = os.path.join(path, filenm + ".csv")
        logger.info('File save started : %s', file_dir_fnm)
        if os.path.exists(file_dir_fnm):
            os.remove(file_dir_fnm)
        with open(file_dir_fnm, "wb") as file_write:
            csvwriter = csv.writer(file_write, delimiter=',', quotechar='|',
                                   quoting=csv.QUOTE_MINIMAL)
            csvwriter.writerows((row for row in data))
        file_write.close()
        logger.info('File saved : %s', file_dir_fnm)
        return file_dir_fnm


class DbFetchLocations():
    """Update Fetch the database with data."""

    def __init__(self, dsnfile, state):
        """Database connection init."""
        self.connection = dbconn.connect(dsnfile)
        self.cursor = self.connection.cursor()
        self.state = state

    def fetch_locations(self):
        """Get all the location attributes from mdaws_wunderground_map."""
        logger.info('Fetching location for : %s', self.state)
        try:
            self.cursor.execute("""SELECT
                        mapped_location_name, latitude, longitude
                        from power.imdaws_wunderground_map
                        where latitude is not null
                        and longitude is not null
                        and state = %s""", (self.state,))
            results = self.cursor.fetchall()
            self.cursor.close()
            return results
        except Exception as err:
            logger.error("Error fetching location: %s : for state : %s",
                         str(err), self.state)
            if self.connection.open:
                self.cursor.close()
            raise


class DbUploadData():
    """Update Fetch the database with data."""

    def __init__(self, dsnfile, filenm=None, data=None):
        """Database connection init."""
        self.connection = dbconn.connect(dsnfile)
        self.cursor = self.connection.cursor()
        self.filenm = filenm
        self.data = data

    def csv_to_data(self, hdr_col, body_dat):
        """Csv to list of lists."""
        self.data = []
        with open(self.filenm, 'r') as csvfile:
            reader = csv.reader(csvfile)
            headers = next(reader)[1:]
            headers.extend(hdr_col)
            logging.debug("{} File Data header: {}"
                          .format(self.filenm, headers))
            for line in reader:
                row = list(line[1:])
                row.extend(body_dat)
                self.data.append(tuple(row))
        logger.debug("{} File Data: {}"
                     .format(self.filenm, self.data))

    def db_upload_data(self):
        """Inserting data into table wu_actual_weather_stg."""
        logger.info('Inserting data into table wu_currentobs_weather_stg.')
        try:
            self.cursor.executemany("""INSERT INTO
                `power`.`wu_currentobs_weather_stg`
                (
                `Local_Date`,
                `Local_Hour`,
                `TemperatureCelsius`,
                `ApparentTemperatureCelsius`,
                `WindSpeedKph`,
                `WindDirirectionCardinal`,
                `WindDirectionDegrees`,
                `WindGustsKph`,
                `WindChillCelsius`,
                `DewpointTemperatureCelsius`,
                `HeatIndexCelsius`,
                `RelativeHumidityPercent`,
                `Conditions`,
                `CloudCoveragePercent`,
                `MslPressureMillibars`,
                `Pressuretend_Desc`,
                `PressureChange3hrs`,
                `TempChange24hour`,
                `TempMax24hour`,
                `TempMin24hour`,
                `Snow1hrCentimeter`,
                `Snow6hrCentimeter`,
                `Snow24hrCentimeter`,
                `Precip1hrMillimeter`,
                `Precip6hrMillimeter`,
                `Precip24hrMillimeter`,
                `Latitude`,
                `Longitude`,
                `DateTimeGmt`,
                `DateTimeLwt`,
                `ExpireTimeLocal`,
                `ExpireTimeGMT`,
                `Mapped_Location_Name`
                )
                VALUES
                (
                STR_TO_DATE(%s,'%%d-%%m-%%Y'),
                TIME_FORMAT(%s, '%%H:%%i:%%s'),
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s)""", tuple(self.data))
            logger.info("Load Status %s", str(self.connection.info()))
            self.connection.commit()
            self.cursor.close()
        except Exception as err:
            logger.error("Error Inserting Data: %s", str(err))
            if self.connection.open:
                self.connection.rollback()
                self.cursor.close()
            raise


# # Test code
# # dsn = '../config/sqldb_connection_config.txt'
# dsn = '../config/sqldb_dev_gcloud.txt'
# loc = DbFetchLocations(dsn, 'UTTARAKHAND')
# all_loc = loc.fetch_locations()
# for loc, lat, lon in all_loc:
#     print loc, lat, lon
#     current_conditions = CurrentConditionsData()
#     current_conditions.fetch_data(lat, lon, 'm')
#     data = current_conditions.parse_current(loc)
#     # print data[1:]
#     dbupdate = DbUploadData(dsn, data=data[1:])
#     # print dbupdate.data
#     dbupdate.db_upload_data()
#     # break

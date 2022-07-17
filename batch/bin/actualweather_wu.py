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

import argparse
from argparse import RawTextHelpFormatter

import sys
import json
import os
import time
from datetime import datetime
from datetime import timedelta
import pytz
import logging
import requests
try:
    from . import dbconn
except:
    import dbconn
# import csv
# import shutil
import pandas as pd


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
SETTINGS = {
    # "api_key": "62ee277e1a8df98fc5a1ddaaa1a51980",
    "api_key": "61e47cd17f994e2ba47cd17f990e2bcb"
}

class OptionParser:
    """Command Line Parser Options."""

    parser = argparse.ArgumentParser(
        description=("actualweather_wu pulls actual"
                     "weather data from Wunderground's API."),
        formatter_class=RawTextHelpFormatter)
    if time.tzname[0] == 'IST':
        local_now = datetime.today()
    else:
        dest_tz = pytz.timezone('Asia/Kolkata')
        ts = time.time()
        utc_now = datetime.utcfromtimestamp(ts)
        local_now = utc_now.replace(tzinfo=pytz.utc).astimezone(dest_tz)

    def __init__(self):
        """Argparse Parser Options.current."""
        # We are going to use sub commands to make user input look cleaner.
        # sub_help = ("Pass the -h parameter to positional arguments to"
        #             " learn more about them. Example: `actualweather_wu -h`")
        # subparser = self.parser.add_subparsers(dest="sub", help=sub_help)
        subparser = self.parser

        # subparser.add_argument('-d', '--dir', dest='dir',
        #                        help=('Directory were the file will be saved.'))
        # subparser.add_argument('-f', '--file', dest='file',
        #                        help=('Filename to be saved.'))
        subparser.add_argument('-b', '--strdt', dest='start_date',
                               help=('Start of Date to get the data.'
                                     'Optional default is today - 1 date'
                                     ' in IST.'),
                               default=self.local_now + timedelta(-1))
        subparser.add_argument('-e', '--enddt', dest='end_date',
                               help=('End of Date to get the data.'
                                     'Optional default is todays date'
                                     ' in IST.'),
                               default=self.local_now)
        # subparser.add_argument('-u', '--units', dest='units',
        #                        help=('metric/imperial. Defaults to metric.'),
        #                        default='metric')
        subparser.add_argument('-m', '--dbdsnconfig', dest='dsn',
                               help=('Full path and the file'
                                     ' name of the db config.'
                                     'This option makes the fetching'
                                     ' of latitude and longitude from db.'
                                     'It also uploads the data back to db.'),
                               required=False)
        subparser.add_argument('-s', '--state', dest='state',
                               help=('State name for which'
                                     ' data is being loaded'),
                               required=False)
        subparser.add_argument('-lt', '--lat', dest='lat',
                               help=('Longitude for which'
                                     ' data is being loaded'),
                               required=False)
        subparser.add_argument('-ln', '--lng', dest='lng',
                               help=('Latitude for which'
                                     ' data is being loaded'),
                               required=False)
        # subparser.add_argument('-a', '--arc', dest='arc',
        #                        help=('Archive file dir.'
        #                              'File will be moved here'
        #                              ' after db update.'),
        #                        required=False)
        # subparser.add_argument('-tz', '--time', dest='tz',
        #                        help=('gmt/lwt'
        #                              'GMT or local time.'
        #                              'Defalt is GMT.'),
        #                        required=False)

    def parse_args(self):
        """Parse Args."""
        args = self.parser.parse_args()
        return args

    def print_usage(self):
        """Print Usage."""
        self.parser.print_usage()


class ActualData:
    """Fetch Actual Data from Wunderground."""

    data = None
    data_type = None
    args = None

    def __init__(self):
        """Initilization constructor."""
        self.api_key = SETTINGS.get('api_key')
        # self.metric_flag = SETTINGS.get('metric')
        # self.base_url = ("http://cleanedobservations.wsi.com/CleanedObs.svc/"
        #                  "GetObs?version=2&time={tm}&lat={lat}&long={long}"
        #                  "&startDate={sd}&endDate={ed}&interval={interval}"
        #                  "&units={units}&format={fmt}"
        #                  "&userKey={uk}&delivery={dlvy}")
        self.base_url = ("https://api.weather.com/v3/wx/hod/conditions/historical/point?pointType=nearest&geocode="
                         "{lat},{lng}&startDateTime={sd}&endDateTime={ed}"
                         "&units={units}&format={fmt}&apiKey={apikey}")
        # https://api.weather.com/v3/wx/hod/conditions/historical/point?pointType=nearest&geocode=78.2676100,30.0869300&startDateTime=201801010000&endDateTime=201801020000&units=m&format=json&apiKey=61e47cd17f994e2ba47cd17f990e2bcb

    def fetch_data(self, lat, lng, stdate, endate,
                   units='m', fmt='json'):
        """Grab data from Weather Underground API."""
        url = self.base_url.format(lat=lat, lng=lng, sd=stdate,
                                   ed=endate, units=units,
                                   fmt=fmt, apikey=self.api_key)
        self.data_type = fmt
        try:
            logger.info('Fetching url: %s', url)
            req = requests.get(url)
            logger.info('Status code: %s', req.status_code)
            if self.data_type == 'json':
                self.data = json.loads(req.content)
            else:
                self.data = req.content
            #logger.debug('Req Data: %s', self.data)
        except Exception as err:
            logger.error("Error: %s", err)
            raise

    def parse_actual(self, location=None):
        logger.info('Parsing json data.')
        df =  pd.DataFrame(self.data)
        columns = ['date', 'time', 'hour', 'location'] + df.columns.to_list()
        df['date'] = pd.to_datetime(df['observationTimeUtcIso'], format='%Y-%m-%dT%H:%M:%S%z').dt.tz_convert('Asia/Calcutta').dt.tz_localize(None).dt.strftime('%Y-%m-%d')
        df['time'] = pd.to_datetime(df['observationTimeUtcIso'], format='%Y-%m-%dT%H:%M:%S%z').dt.tz_convert('Asia/Calcutta').dt.tz_localize(None).dt.strftime('%H:%M:%S')
        df['hour'] = pd.to_datetime(df['observationTimeUtcIso'], format='%Y-%m-%dT%H:%M:%S%z').dt.tz_convert('Asia/Calcutta').dt.tz_localize(None).dt.strftime('%H').astype(int)
        df['location'] = location
        df = df.where(pd.notnull(df), None)
        return df[columns].values.tolist()                 


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

    def __init__(self, dsnfile, data=None):
        """Database connection init."""
        self.connection = dbconn.connect(dsnfile)
        self.cursor = self.connection.cursor()
        self.data = data

    def db_upload_data2(self):
        """Inserting data into table wu_actual_weather_stg."""
        logger.info('Inserting data into table wu_actual_weather_stg. %s ', len(self.data))
        try:
            self.cursor.executemany("""INSERT INTO `wu_actual_weather_stg`
                (`date`,
                `time`,
                `hour`,
                `location`,
                `latitude`,
                `longitude`,
                `drivingdifficultyindex`,
                `iconcode`,
                `observationtimeutciso`,
                `precip1hour`,
                `precip24hour`,
                `precip6hour`,
                `pressurechange`,
                `relativehumidity`,
                `snow1hour`,
                `snow6hour`,
                `snow24hour`,
                `temperature`,
                `temperaturechange24hour`,
                `temperaturedewpoint`,
                `temperaturefeelslike`,
                `temperaturemax24hour`,
                `temperaturemin24hour`,
                `uvindex`,
                `visibility`,
                `windgust`,
                `windspeed`,
                `wxphrasecode`)
                VALUES
                (%s,
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
                %s)
                ON DUPLICATE KEY UPDATE
                `drivingdifficultyindex` = VALUES(`drivingdifficultyindex`),
                `iconcode` = VALUES(`iconcode`),
                `observationtimeutciso` = VALUES(`observationtimeutciso`),
                `precip1hour` = VALUES(`precip1hour`),
                `precip24hour` = VALUES(`precip24hour`),
                `precip6hour` = VALUES(`precip6hour`),
                `pressurechange` = VALUES(`pressurechange`),
                `relativehumidity` = VALUES(`relativehumidity`),
                `snow1hour` = VALUES(`snow1hour`),
                `snow6hour` = VALUES(`snow6hour`),
                `snow24hour` = VALUES(`snow24hour`),
                `temperature` = VALUES(`temperature`),
                `temperaturechange24hour` = VALUES(`temperaturechange24hour`),
                `temperaturedewpoint` = VALUES(`temperaturedewpoint`),
                `temperaturefeelslike` = VALUES(`temperaturefeelslike`),
                `temperaturemax24hour` = VALUES(`temperaturemax24hour`),
                `temperaturemin24hour` = VALUES(`temperaturemin24hour`),
                `uvindex` = VALUES(`uvindex`),
                `visibility` = VALUES(`visibility`),
                `windgust` = VALUES(`windgust`),
                `windspeed` = VALUES(`windspeed`),
                `wxPhraseCode` = VALUES(`wxPhraseCode`)""", tuple(self.data))
            logger.info("Load Status %s", str(self.connection.info()))
            self.connection.commit()
            self.cursor.close()            
            logger.info("Load Status %s", str(self.connection.info()))
            self.connection.commit()
            self.cursor.close()
        except Exception as err:
            logger.error("Error Inserting Data: %s", str(err))
            logger.error("Error Inserting Data: %s", self.data)
            if self.connection.open:
                self.connection.rollback()
                self.cursor.close()
            raise

    def db_upload_data(self):
        """Inserting data into table wu_actual_weather_stg."""
        logger.info('Inserting data into table wu_actual_weather_stg.')
        try:
            self.cursor.executemany("""INSERT INTO
                `power`.`wu_actual_weather_stg`
                (
                `Latitude`,
                `Longitude`,
                `DateTimeGmt`,
                `DateTimeLwt`,
                `SurfaceTemperatureCelsius`,
                `SurfaceDewpointTemperatureCelsius`,
                `SurfaceWetBulbTemperatureCelsius`,
                `RelativeHumidityPercent`,
                `SurfaceAirPressureKilopascals`,
                `CloudCoveragePercent`,
                `WindChillTemperatureCelsius`,
                `ApparentTemperatureCelsius`,
                `WindSpeedKph`,
                `WindDirectionDegrees`,
                `PrecipitationPreviousHourCentimeters`,
                `DownwardSolarRadiationWsqm`,
                `DiffuseHorizontalRadiationWsqm`,
                `DirectNormalIrradianceWsqm`,
                `MslPressureKilopascals`,
                `HeatIndexCelsius`,
                `SnowfallCentimeters`,
                `SurfaceWindGustsKph`,
                `Mapped_Location_Name`,
                `Local_date`,
                `Local_time`
                )
                VALUES
                (
                %s,
                %s,
                STR_TO_DATE(%s,'%%m/%%d/%%Y %%H:%%i:%%s'),
                STR_TO_DATE(%s,'%%m/%%d/%%Y %%H:%%i:%%s'),
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
                DATE(DateTimeLwt),
                TIME(DateTimeLwt))
                ON DUPLICATE KEY UPDATE
                `SurfaceTemperatureCelsius`= VALUES(SurfaceTemperatureCelsius),
                `SurfaceDewpointTemperatureCelsius`=
                    VALUES(SurfaceDewpointTemperatureCelsius),
                `SurfaceWetBulbTemperatureCelsius`=VALUES(SurfaceWetBulbTemperatureCelsius),
                `RelativeHumidityPercent`=VALUES(RelativeHumidityPercent),
                `SurfaceAirPressureKilopascals`=VALUES(SurfaceAirPressureKilopascals),
                `CloudCoveragePercent`=VALUES(CloudCoveragePercent),
                `WindChillTemperatureCelsius`=VALUES(WindChillTemperatureCelsius),
                `ApparentTemperatureCelsius`=VALUES(ApparentTemperatureCelsius),
                `WindSpeedKph`=VALUES(WindSpeedKph),
                `WindDirectionDegrees`=VALUES(WindDirectionDegrees),
                `PrecipitationPreviousHourCentimeters`=VALUES(PrecipitationPreviousHourCentimeters),
                `DownwardSolarRadiationWsqm`=VALUES(DownwardSolarRadiationWsqm),
                `DiffuseHorizontalRadiationWsqm`=VALUES(DiffuseHorizontalRadiationWsqm),
                `DirectNormalIrradianceWsqm`=VALUES(DirectNormalIrradianceWsqm),
                `MslPressureKilopascals`=VALUES(MslPressureKilopascals),
                `HeatIndexCelsius`=VALUES(HeatIndexCelsius),
                `SnowfallCentimeters`=VALUES(SnowfallCentimeters),
                `SurfaceWindGustsKph`=VALUES(SurfaceWindGustsKph),
                `Local_date`=DATE(DateTimeLwt),
                `Local_time`=TIME(DateTimeLwt),
                `Mapped_Location_Name`=VALUES(Mapped_Location_Name),
                 Modified_Date = null,
                 Load_Ind = 0""", tuple(self.data))
            logger.info("Load Status %s", str(self.connection.info()))
            self.connection.commit()
            self.cursor.close()
        except Exception as err:
            logger.error("Error Inserting Data: %s", str(err))
            if self.connection.open:
                self.connection.rollback()
                self.cursor.close()
            raise


def daterange(start_date=None, end_date=None):
    """Iterate over a Date range else uses todays date as default."""
    if isinstance(start_date, str) and isinstance(end_date, str):
        start_date = datetime.strptime(start_date, '%d-%m-%Y')
        end_date = datetime.strptime(end_date, '%d-%m-%Y')
    if start_date < end_date:
        for now in range(0, (end_date - start_date).days + 1):
            logger.debug(now)
            yield start_date + timedelta(now)
    else:
        raise Exception('Invalid Start and End Date')
    return


def main():
    """Main module."""
    parser = OptionParser()
    args = parser.parse_args()
    # print args
    if (args.dsn  and args.state ):
        for date in daterange(args.start_date, args.end_date):
            startdt = date.strftime('%Y%m%d%H%M')
            if  date + timedelta(1) < datetime.utcnow():
                enddt = (date + timedelta(1)).strftime('%Y%m%d%H%M')
            else:
                enddt = datetime.utcnow().strftime('%Y%m%d%H%M')            
            logger.info('Retrieving for date : %s', startdt)
            loc = DbFetchLocations(args.dsn, args.state)
            for loc, lat, lng in loc.fetch_locations():
                print((loc, lat, lng))
                actuals = ActualData()
                actuals.fetch_data(lat, lng, startdt, enddt)
                data = actuals.parse_actual(loc)
                dbupdate = DbUploadData(args.dsn, data=data)
                dbupdate.db_upload_data2()
    else:
        logger.info('Not enough parametes passed : %s', args)
        raise Exception('Not enough parametes passed')

    # Test code
    # forecast = ActualData(args)
    # forecast.output_data(parser)
    # dsn = '../config/sqldb_connection_config.txt'
    # loc = DbFetchLocations(dsn, 'GUJARAT')
    # all_loc = loc.fetch_locations()
    # for loc, lat, lon in all_loc:
    #     print( loc, lat, lon)
    #     actuals = ActualData(None)
    #     actuals.fetch_data(lat, lon, '01/01/2018', '01/02/2018')
    #     act_data = actuals.data
    #     print( act_data, type(act_data))
    #     actuals.append_data(['Location'], loc)
    #     actuals.save_file('/Users/biswadippaul/Downloads/', 'loc')
    #     filename = '/Users/biswadippaul/Downloads/test.csv'
    #     newpath = '/Users/biswadippaul/Projects/quenext-dev-latest/default/ems/batch/data_archive/weather'
    #     dbupdate = DbUploadData(dsn, filename)
    #     dbupdate.csv_to_data(['Location'], [loc])
    #     dbupdate.db_upload_data()
    #     shutil.move(filename, newpath)
    #     break


if __name__ == '__main__':
    main()
    sys.exit()

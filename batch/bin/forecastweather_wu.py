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
import sys
import logging
import json
import requests
try:
    from . import dbconn
except:
    import dbconn
import csv
from dateutil.parser import parse
import datetime
import os
import pandas as pd
# import shutil

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# SETTINGS = {"forecast": {
#     "api_key": "d056785095a54e8da282cc12320c42b6",
#     "hourly": 'hourly/360hour.json',
#     "15min": 'fifteenminute.json'}
# }
SETTINGS = {"forecast": {
    "api_key": "61e47cd17f994e2ba47cd17f990e2bcb"
    }
}


class ForecastData:
    """Fetch Forecast Data from Wunderground."""

    data = None
    data_type = 'json'
    args = None

    def __init__(self):
        """Initilization constructor."""
        self.api_key = SETTINGS.get('forecast').get('api_key')
        # self.base_url = ("https://api.weather.com/v1/geocode/{lat}/{long}"
        #                  "/forecast/{interval}?language=en-US"
        #                  "&units={units}&apiKey={apikey}")

        self.base_url = ("https://api.weather.com/v3/wx/forecast/hourly/15day?geocode={lat},{lng}"
                         "&format=json&units={units}&language=en-US"
                         "&apiKey={apikey}")
        #https://api.weather.com/v3/wx/forecast/hourly/15day?geocode=78.2676100,30.0869300&format=json&units=m&language=en-US&apiKey=61e47cd17f994e2ba47cd17f990e2bcb 


    def fetch_data(self, lat, lng, interval, units):
        """Grab data from Weather Underground API."""
        linterval = SETTINGS.get('forecast').get(interval)
        url = self.base_url.format(lat=lat, lng=lng,
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

    def parse_forecast2(self, location=None, forecast_type=None):
        logger.info('Parsing json data.')
        df =  pd.DataFrame(self.data)
        columns = ['date', 'time', 'hour', 'expiretimelocal', 'forecasttype', 'location'] + df.columns.to_list()
        # df['date'] = pd.to_datetime(df['validTimeLocal'], format='%Y-%m-%dT%H:%M:%S%z').dt.date
        # df['time'] = pd.to_datetime(df['validTimeLocal'], format='%Y-%m-%dT%H:%M:%S%z').dt.time
        df['date'] = pd.to_datetime(df['validTimeLocal'], format='%Y-%m-%dT%H:%M:%S%z').dt.strftime('%Y-%m-%d')
        df['time'] = pd.to_datetime(df['validTimeLocal'], format='%Y-%m-%dT%H:%M:%S%z').dt.strftime('%H:%M:%S')
        df['hour'] = pd.to_datetime(df['validTimeLocal'], format='%Y-%m-%dT%H:%M:%S%z').dt.strftime('%H').astype(int)        
        df['expiretimelocal'] = pd.to_datetime(df['expirationTimeUtc'], unit='s')\
            .dt.tz_localize('UTC').dt.tz_convert('Asia/Calcutta').dt.tz_localize(None).dt.strftime('%Y-%m-%d %H:%M:%S')
        df['forecasttype'] = forecast_type
        df['location'] = location
        df = df.where(pd.notnull(df), None)
        return df[columns].values.tolist()               

    def parse_forecast(self, location=None, forecast_type=None,
                       hdr_col=None, body_dat=None):
        """
        Parse the forecast json file.

        { "class": "fod_long_range_hourly", "expire_time_gmt": 1487247545,
         "fcst_valid": 1487246400,
         "fcst_valid_local": "2017-02-16T17:30:00+0530",
         "num": 1, "day_ind": "D", "temp": 17, "dewpt": 8, "hi": 17,
         "wc": 17,
         "feels_like": 17, "icon_extd": 3200, "wxman": "wx1000",
         "icon_code": 32,
         "dow": "Thursday", "phrase_12char": "Sunny",
         "phrase_22char": "Sunny",
         "phrase_32char": "Sunny", "subphrase_pt1": "Sunny",
         "subphrase_pt2": "",
         "subphrase_pt3": "", "pop": 0, "precip_type": "rain", "qpf": 0.0,
         "snow_qpf": 0.0, "rh": 57, "wspd": 7, "wdir": 229,
         "wdir_cardinal": "SW",
         "gust": null, "clds": 15, "vis": 16.0, "mslp": 1016.5,
         "uv_index_raw": 0.11,
         "uv_index": 0, "uv_warning": 0, "uv_desc": "Low",
         "golf_index": 8,
         "golf_category": "Very Good", "severity": 1 }
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
                        "Conditions", "QpfMillimeters", "PopPercent",
                        "SnowQpfCentimeters", "CloudCoveragePercent",
                        "MslPressureMillibars",
                        "Latitude", "Longitude", "DateTimeGMT",
                        "DateTimeLwt", "ExpireTimeLocal", "ExpireTimeGMT",
                        "Location", "Forecast_Type"])
            longitude = self.data['metadata']['longitude']
            latitude = self.data['metadata']['latitude']
            expire_time_gmt = self.data['metadata']['expire_time_gmt']
            gexpire_time_gmt = datetime.datetime.utcfromtimestamp(
                int(expire_time_gmt)).strftime('%Y-%m-%d %H:%M:%S')
            for item in self.data['forecasts']:
                expire_time_gmt = item['expire_time_gmt']
                lexpire_time_gmt = datetime.datetime.fromtimestamp(
                    int(expire_time_gmt)).strftime('%Y-%m-%d %H:%M:%S')
                gmt_datetime = datetime.datetime.utcfromtimestamp(
                    int(item['fcst_valid'])).strftime('%Y-%m-%d %H:%M:%S')
                local_datetime = parse(item["fcst_valid_local"])
                date = local_datetime.strftime('%d-%m-%Y')
                time = local_datetime.strftime('%H:%M:%S')
                val.append([date, time, item["temp"], item["feels_like"],
                            item["wspd"], item['wdir_cardinal'],
                            item['wdir'], item['gust'], item['wc'],
                            item['dewpt'], item['hi'], item['rh'],
                            item['phrase_22char'], item.get('qpf', None),
                            item['pop'], item.get('snow_qpf', None),
                            item['clds'], item['mslp'],
                            latitude, longitude, gmt_datetime,
                            local_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                            lexpire_time_gmt, gexpire_time_gmt,
                            location, forecast_type])
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
                        and display_city_name is not null
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

    def db_upload_data2(self):
        """Inserting data into table wu_actual_weather_stg."""
        logger.info('Inserting data into table wu_forecast_weather_stg. %s ', len(self.data))
        try:
            self.cursor.executemany("""INSERT INTO `wu_forecast_weather_stg`
                (`date`,
                `time`,
                `hour`,
                `expiretimelocal`,
                `forecasttype`,
                `location`,
                `cloudcover`,
                `dayofweek`,
                `dayornight`,
                `expirationtimeutc`,
                `iconcode`,
                `iconcodeextend`,
                `precipchance`,
                `preciptype`,
                `pressuremeansealevel`,
                `qpf`,
                `qpfsnow`,
                `relativehumidity`,
                `temperature`,
                `temperaturedewpoint`,
                `temperaturefeelslike`,
                `temperatureheatindex`,
                `temperaturewindchill`,
                `uvdescription`,
                `uvindex`,
                `validtimelocal`,
                `validtimeutc`,
                `visibility`,
                `winddirection`,
                `winddirectioncardinal`,
                `windgust`,
                `windspeed`,
                `wxphraselong`,
                `wxphraseshort`,
                `wxseverity`)
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
                 %s,
                 %s,
                 %s,
                 %s,
                 %s,
                 %s,
                 %s,
                 %s)
                ON DUPLICATE KEY UPDATE
                `cloudcover` = VALUES(cloudcover),
                `dayofweek` = VALUES(dayofweek),
                `dayornight` = VALUES(dayornight),
                `expirationtimeutc` = VALUES(expirationtimeutc),
                `iconcode` = VALUES(iconcode),
                `iconcodeextend` = VALUES(iconcodeextend),
                `precipchance` = VALUES(precipchance),
                `preciptype` = VALUES(preciptype),
                `pressuremeansealevel` = VALUES(pressuremeansealevel),
                `qpf` = VALUES(qpf),
                `qpfsnow` = VALUES(qpfsnow),
                `relativehumidity` = VALUES(relativehumidity),
                `temperature` = VALUES(temperature),
                `temperaturedewpoint` = VALUES(temperaturedewpoint),
                `temperaturefeelslike` = VALUES(temperaturefeelslike),
                `temperatureheatindex` = VALUES(temperatureheatindex),
                `temperaturewindchill` = VALUES(temperaturewindchill),
                `uvdescription` = VALUES(uvdescription),
                `uvindex` = VALUES(uvindex),
                `validtimelocal` = VALUES(validtimelocal),
                `validtimeutc` = VALUES(validtimeutc),
                `visibility` = VALUES(visibility),
                `winddirection` = VALUES(winddirection),
                `winddirectioncardinal` = VALUES(winddirectioncardinal),
                `windgust` = VALUES(windgust),
                `windspeed` = VALUES(windspeed),
                `wxphraselong` = VALUES(wxphraselong),
                `wxphraseshort` = VALUES(wxphraseshort),
                `wxseverity` = VALUES(wxseverity),
                 processed_ind = 0""", tuple(self.data))
            logger.info("Load Status %s", str(self.connection.info()))
            self.connection.commit()
            self.cursor.close()
        except Exception as err:
            logger.error("Error Inserting Data: %s", str(err))
            # logger.error("Error Inserting Data: %s", self.data)
            if self.connection.open:
                self.connection.rollback()
                self.cursor.close()
            raise

    def db_upload_data(self):
        """Inserting data into table wu_actual_weather_stg."""
        logger.info('Inserting data into table wu_forecast_weather_stg.')
        logger.info(self.data)
        try:
            self.cursor.executemany("""INSERT INTO
                `power`.`wu_forecast_weather_stg`
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
                `QpfMillimeters`,
                `PopPercent`,
                `SnowQpfCentimeters`,
                `CloudCoveragePercent`,
                `MslPressureMillibars`,
                `Latitude`,
                `Longitude`,
                `DateTimeGmt`,
                `DateTimeLwt`,
                `ExpireTimeLocal`,
                `ExpireTimeGMT`,
                `Mapped_Location_Name`,
                `Forecast_Type`
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
                %s)
                ON DUPLICATE KEY UPDATE
                `TemperatureCelsius` = VALUES(TemperatureCelsius),
                `ApparentTemperatureCelsius` =
                    VALUES(ApparentTemperatureCelsius),
                `WindSpeedKph` = VALUES(WindSpeedKph),
                `WindDirirectionCardinal` = VALUES(WindDirirectionCardinal),
                `WindDirectionDegrees` = VALUES(WindDirectionDegrees),
                `WindGustsKph` = VALUES(WindGustsKph),
                `WindChillCelsius` = VALUES(WindChillCelsius),
                `DewpointTemperatureCelsius` =
                    VALUES(DewpointTemperatureCelsius),
                `HeatIndexCelsius` = VALUES(HeatIndexCelsius),
                `RelativeHumidityPercent` = VALUES(RelativeHumidityPercent),
                `Conditions` = VALUES(Conditions),
                `QpfMillimeters` = VALUES(QpfMillimeters),
                `PopPercent` = VALUES(PopPercent),
                `SnowQpfCentimeters` = VALUES(SnowQpfCentimeters),
                `CloudCoveragePercent` = VALUES(CloudCoveragePercent),
                `MslPressureMillibars` = VALUES(MslPressureMillibars),
                `ExpireTimeLocal` = VALUES(ExpireTimeLocal),
                `ExpireTimeGMT` = VALUES(ExpireTimeGMT),
                 Modified_Date = null,
                 Processed_Ind = 0""", tuple(self.data))
            logger.info("Load Status %s", str(self.connection.info()))
            self.connection.commit()
            self.cursor.close()
        except Exception as err:
            logger.error("Error Inserting Data: %s", str(err))
            if self.connection.open:
                self.connection.rollback()
                self.cursor.close()
            raise

def main():
    # # # # Test code
    dsn = '/Users/biswadippaul/Projects/code/power-codes/quenext-dev-dev/default/ems/batch/config/sqldb_connection_config.txt'
    # dsn = '../config/sqldb_connection_config.txt'
    # dsn = '../config/sqldb_dev_gcloud.txt'
    loc = DbFetchLocations(dsn, 'UTTARAKHAND')
    # loc = DbFetchLocations(dsn, 'GUJARAT')
    # loc = DbFetchLocations(dsn, 'TAMIL NADU')
    all_loc = loc.fetch_locations()
    # forecast_type = ['hourly', '15min']
    forecast_type = ['hourly']
    for loc, lat, lng in all_loc:
        print((loc, lat, lng))
        # forecast_type = 'hourly'
        for f_type in forecast_type:
            forecast = ForecastData()
            forecast.fetch_data(lat, lng, f_type, 'm')
            data = forecast.parse_forecast2(loc, f_type)
            # print(len(data[:1]))
            dbupdate = DbUploadData(dsn, data=data)
            # print dbupdate.data
            dbupdate.db_upload_data2()

if __name__ == '__main__':
    main()
    sys.exit()


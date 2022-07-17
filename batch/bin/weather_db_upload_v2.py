# Uploads weather data file.
import logging
import dbconn
import csv
import os
import shutil


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DbUploadForecastData():
    """Update Fetch the database with data."""

    def __init__(self, dsnfile, filenm=None, data=None):
        """Database connection init."""
        self.connection = dbconn.connect(dsnfile)
        self.cursor = self.connection.cursor()
        self.filenm = filenm
        self.data = data

    def csv_to_data(self, hdr_col=None, body_dat=None):
        """Csv to list of lists."""
        self.data = []
        with open(self.filenm, 'r') as csvfile:
            reader = csv.reader(csvfile)
            headers = next(reader)[1:]
            if hdr_col:
                headers.extend(hdr_col)
            logging.debug("{} File Data header: {}"
                          .format(self.filenm, headers))
            for line in reader:
                row = list(line[1:])
                if hdr_col:
                    row.extend(body_dat)
                self.data.append(tuple(row))
        logger.debug("{} File Data: {}"
                     .format(self.filenm, self.data))

    def db_upload_data(self):
        """Inserting data into table wu_actual_weather_stg."""
        logger.info('Inserting data into table wu_forecast_weather_stg.')
        logger.info(self.data)
        try:
            self.cursor.executemany("""INSERT INTO
                `power`.`unified_weather3`
                (`temperature`,
                 `apparenttemperature`,
                 `windspeed`,
                 `winddir_deg`,
                 `windgusts`,
                 `windchill`,
                 `dewpoint`,
                 `heatindex`,
                 `relativehumidity`,
                 `conditions`,
                 `qpf`,
                 `pop`,
                 `snowfall_qpf_cm`,
                 `cloudcoverage`,
                 `mslpressure`,
                 `latitude`,
                 `longitude`,
                 `datetime_gmt`,
                 `datetime`,
                 `data_source`,
                 `data_type`
                )
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
                %s)
                ON DUPLICATE KEY UPDATE
                `temperature` = VALUES(`temperature`),
                `dewpoint` = VALUES(`dewpoint`),
                `relativehumidity` = VALUES(`relativehumidity`),
                `cloudcoverage` = VALUES(`cloudcoverage`),
                `windchill` = VALUES(`windchill`),
                `apparenttemperature` = VALUES(`apparenttemperature`),
                `windspeed` = VALUES(`windspeed`),
                `winddir_deg` = VALUES(`winddir_deg`),
                `qpf` = VALUES(`qpf`),
                `pop` = VALUES(`pop`),
                `mslpressure` = VALUES(`mslpressure`),
                `conditions` = VALUES(`conditions`),
                `heatindex` = VALUES(`heatindex`),
                `snowfall_qpf_cm` = VALUES(`snowfall_qpf_cm`),
                `windgusts` = VALUES(`windgusts`)""", tuple(self.data))

            logger.info("Load Status %s", str(self.connection.info()))
            self.connection.commit()
            self.cursor.close()
        except Exception as err:
            logger.error("Error Inserting Data: %s", str(err))
            if self.connection.open:
                self.connection.rollback()
                self.cursor.close()
            raise


class DbUploadActualData():
    """Update Fetch the database with data."""

    def __init__(self, dsnfile, filenm):
        """Database connection init."""
        self.connection = dbconn.connect(dsnfile)
        self.cursor = self.connection.cursor()
        self.filenm = filenm
        self.data = []

    def csv_to_data(self, hdr_col=None, body_dat=None):
        """Csv to list of lists."""
        with open(self.filenm, 'r') as csvfile:
            reader = csv.reader(csvfile)
            headers = next(reader)[1:]
            if hdr_col:
                headers.extend(hdr_col)
            logger.debug("{} File Data header: {}"
                         .format(self.filenm, headers))
            for line in reader:
                row = list(line[1:])
                if hdr_col:
                    row.extend(body_dat)
                self.data.append(tuple(row))
        logger.debug("{} File Data: {}"
                     .format(self.filenm, self.data))

    def db_upload_data(self):
        """Inserting data into table wu_actual_weather_stg."""
        logger.info('Inserting data into table power.unified_weather3.')
        logger.info("data %s", str(self.data))
        try:
            self.cursor.executemany("""INSERT INTO
                `power`.`unified_weather3`
                (`latitude`,
                `longitude`,
                `datetime_gmt`,
                `datetime`,
                `temperature`,
                `dewpoint`,
                `wetbulbtemperature`,
                `relativehumidity`,
                `surfaceairpressure`,
                `cloudcoverage`,
                `windchill`,
                `apparenttemperature`,
                `windspeed`,
                `winddir_deg`,
                `rainfall_mm`,
                `downwardsolarradiation`,
                `diffusehorizontalradiation`,
                `directnormalirradiance`,
                `mslpressure`,
                `heatindex`,
                `snowfall_cm`,
                `windgusts`,
                `data_source`,
                `data_type`
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
                ROUND(%s * 10, 2),
                %s,
                %s,
                %s,
                %s,
                %s,
                ROUND(%s * 10, 4),
                %s,
                %s,
                %s,
                ROUND(%s * 10, 2),
                %s,
                %s,
                %s,
                %s,
                %s)
                ON DUPLICATE KEY UPDATE
                `temperature` = VALUES(`temperature`),
                `dewpoint` = VALUES(`dewpoint`),
                `wetbulbtemperature` = VALUES(`wetbulbtemperature`),
                `relativehumidity` = VALUES(`relativehumidity`),
                `surfaceairpressure` = VALUES(`surfaceairpressure`),
                `cloudcoverage` = VALUES(`cloudcoverage`),
                `windchill` = VALUES(`windchill`),
                `apparenttemperature` = VALUES(`apparenttemperature`),
                `windspeed` = VALUES(`windspeed`),
                `winddir_deg` = VALUES(`winddir_deg`),
                `rainfall_mm` = VALUES(`rainfall_mm`),
                `downwardsolarradiation` = VALUES(`downwardsolarradiation`),
                `diffusehorizontalradiation`
                    = VALUES(`diffusehorizontalradiation`),
                `directnormalirradiance` = VALUES(`directnormalirradiance`),
                `mslpressure` = VALUES(`mslpressure`),
                `heatindex` = VALUES(`heatindex`),
                `snowfall_cm` = VALUES(`snowfall_cm`),
                `windgusts` = VALUES(`windgusts`)""", tuple(self.data))
            logger.info("Load Status %s", str(self.connection.info()))
            self.connection.commit()
            self.cursor.close()
        except Exception as err:
            logger.error("Error Inserting Data: %s", str(err))
            if self.connection.open:
                self.connection.rollback()
                self.cursor.close()
            raise


dsn = '../config/sqldb_dev_gcloud.txt'
ldir = '/Users/biswadippaul/Projects/quenext-dev-latest/default/ems/batch/data/weather/'
adir = '/Users/biswadippaul/Projects/quenext-dev-latest/default/ems/batch/data_archive/weather/'

# for filename in os.listdir(ldir):
#     if filename.endswith(".csv"):
#         if filename.startswith('Forecast'):
#             path_filname = os.path.join(ldir, filename)
#             dbforupdate = DbUploadForecastData(dsn, path_filname)
#             dbforupdate.csv_to_data()
#             dst_file = os.path.join(adir, filename)
#             if os.path.exists(dst_file):
#                 os.remove(dst_file)
#             shutil.move(path_filname, adir)
#         elif filename.startswith('Actual'):
#             path_filname = os.path.join(ldir, filename)
#             dbactupdate = DbUploadActualData(dsn, path_filname)
#             dbactupdate.csv_to_data(['Datasource', 'Datatype'],
#                                     ['IBMWEATHERCHANNEL', 'ACTUAL'])
#             dbactupdate.db_upload_data()
#             dst_file = os.path.join(adir, filename)
#             if os.path.exists(dst_file):
#                 os.remove(dst_file)
#             shutil.move(path_filname, adir)
#         else:
#             continue
#     else:
#         continue

with open('Actualoutput.txt','wb') as fout:
    wout = csv.writer(fout, delimiter=',')
    
    interesting_files = glob.glob("*.csv") 
    for filename in interesting_files: 
        print 'Processing',filename 
        # Open and process file
        h = True
        with open(filename,'rb') as fin:
            if h:
                h = False
            else:
                fin.next()#skip header
            for line in csv.reader(fin,delimiter=','):
                wout.writerow(line)


"""DayAhead Forecast Report Generation."""
import pandas as pd
from sqlalchemy import create_engine
import os
import time
from datetime import datetime
import pytz


def get_time():
    """Get the latest time in IST."""
    if time.tzname[0] == 'IST':
        localtm = datetime.today().time()
    else:
        dest_tz = pytz.timezone('Asia/Kolkata')
        ts = time.time()
        utc_now = datetime.utcfromtimestamp(ts)
        localtm = utc_now.replace(tzinfo=pytz.utc).astimezone(dest_tz).time()
    return localtm.strftime('%H%M')


def get_dbdemforecast_data(date, model, discom, config):
    """Day Before Demand Forecast."""
    engine = create_engine(config, echo=False)
    db_dem_forecast = pd.read_sql_query("""SELECT
        a.Date, a.Discom Org_Name
        , a.Model_Name, a.Block_No
        , a.Demand_Forecast, a.Demand_Bias
        FROM power.forecast_stg a,
             (select date, discom, model_name,
              max(revision) max_revision
              from power.forecast_stg
              where date = '{}'
              and model_name = '{}'
              and discom = '{}'
              group by date, discom, model_name) b
        where a.date = b.date
        and a.discom = b.discom
        and a.model_name = b.model_name
        and a.revision = b.max_revision
        order by a.Date, a.Discom,
        a.Model_Name, a.Block_No
        """.format(date, model, discom),
        engine, index_col=None)
    return db_dem_forecast


def get_dbgenforecast_data(date, model, discom, config):
    """Day Before Generation Forecast."""
    engine = create_engine(config, echo=False)
    db_gen_forecast = pd.read_sql_query("""SELECT
        a.Date, a.Org_Name,
        a.Pool_Name, a.Pool_Type, a.Entity_Name
        , a.Model_Name, a.Block_No
        , a.Gen_Forecast
        FROM power.gen_forecast_stg a,
             (select date, org_name,
              pool_name, pool_type,
              entity_name, model_name,
              max(revision) max_revision
              from power.gen_forecast_stg
              where date = '{}'
              and model_name = '{}'
              and org_name = '{}'
              group by date, org_name, pool_name,
              pool_type, entity_name, model_name) b
        where a.date = b.date
        and a.org_name = b.org_name
        and a.model_name = b.model_name
        and a.revision = b.max_revision
        and a.pool_name = b.pool_name
        and a.pool_type = b.pool_type
        and a.entity_name = b.entity_name
        order by a.Date, a.Block_No, a.Org_Name,
        a.Pool_Name,  a.Pool_Type, a.Entity_Name,
        a.Model_Name
        """.format(date, model, discom),
        engine, index_col=None)
    return db_gen_forecast


def daybefore_forecast_report(config, discom, date, demand_model,
                              generation_model, revision, dirpath):
    """Day Before Forecast Report."""
    curtime = get_time()
    try:
        dbdemand_forecast_df = \
            get_dbdemforecast_data(date, demand_model, discom, config)
        dbdemand_forecast_df['Total'] = \
            dbdemand_forecast_df['Demand_Forecast'] + \
            dbdemand_forecast_df['Demand_Bias']
        dbdemand_forecast_df.insert(1, 'Revision', revision)
        dbdemandcolsrename = {'Model_Name': 'Model',
                              'Demand_Forecast': 'Forecast',
                              'Demand_Bias': 'Bias'}
        dbdemand_forecast_df.rename(columns=dbdemandcolsrename, inplace=True)
        dbdemand_forecast_df['Date'] = \
            pd.to_datetime(dbdemand_forecast_df['Date'], errors='coerce')
        dbdemand_forecast_df['Date'] = \
            dbdemand_forecast_df['Date'].dt.strftime('%d-%m-%Y')
        datestr = dbdemand_forecast_df['Date'][0]
        dbdemfile_name = ['Quenext_Forecast_', datestr,
                          '_REV_', str(revision), '_', curtime, '.xlsx']
        dbdempath_file_name = os.path.join(dirpath, ''.join(dbdemfile_name))
        dbdemand_forecast_df.to_excel(dbdempath_file_name,
                                      sheet_name='Demand Forecast',
                                      index=False)
    except Exception as err:
        print "Error DBDemand Forecast: " + str(err)
    try:
        dbgen_forecast_df = \
            get_dbgenforecast_data(date, generation_model, discom, config)
        dbgen_forecast_df.insert(1, 'Revision', revision)
        dbgen_forecast_df['Date'] = \
            pd.to_datetime(dbgen_forecast_df['Date'], errors='coerce')
        dbgen_forecast_df['Date'] = \
            dbgen_forecast_df['Date'].dt.strftime('%d-%m-%Y')
        datestr = dbgen_forecast_df['Date'][0]
        dbgen_windforecast_df = \
            dbgen_forecast_df[dbgen_forecast_df['Pool_Type'] == 'WIND']
        dbgen_solarforecast_df = \
            dbgen_forecast_df[dbgen_forecast_df['Pool_Type'] == 'SOLAR']
        dbgenfile_name = ['Quenext_REGenForecast_', datestr,
                          '_REV_', str(revision), '_', curtime, '.xlsx']
        dbgenpath_file_name = os.path.join(dirpath, ''.join(dbgenfile_name))
        if len(dbgen_solarforecast_df) or len(dbgen_windforecast_df):
            writer = pd.ExcelWriter(dbgenpath_file_name, engine='xlsxwriter')
            if len(dbgen_solarforecast_df):
                dbgen_solarforecast_df.to_excel(writer,
                                                'Solar Forecast', index=False)
            if len(dbgen_windforecast_df):
                dbgen_windforecast_df.to_excel(writer,
                                               'Wind Forecast', index=False)
            writer.save()
    except Exception as err:
        print "Error DBGeneration Forecast: " + str(err)

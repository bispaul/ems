"""Intraday Forecast Report Generation."""
import pandas as pd
from sqlalchemy import create_engine
import os
import simplejson as json


def get_liveposmap_data(discom, date, dns):
    """Get Live Position Map Data."""
    import sql_load_lib as sql_load_lib
    level = 'POOL_TYPE'
    results = \
        sql_load_lib.sql_sp_realtime_data_fetch(dns, date, discom, level)
    return json.dumps(results, use_decimal=True)


# def get_liveforecast_rev2(date, discom, config):
#     """Get Live Forecast Rev."""
#     engine = create_engine(config, echo=False)
#     rev_data = pd.read_sql_query("""select
#         a.date,
#         d.model_type, max(revision) revision
#         from position_map a,
#              organisation_master b,
#              (select *
#               from
#               pool_master
#               where pool_name in ('INT_GENERATION_FOR', 'REALTIME_DEMAND_FOR')
#               and pool_type in ('WIND', 'SOLAR', 'UNKNOWN')) c,
#               power.model_master d
#         where a.organisation_master_fk = b.organisation_master_pk
#         and a.model_master_fk = d.id
#         and a.pool_master_fk = c.pool_master_pk
#         and a.date = '{}'
#         and b.organisation_code = '{}'
#         group by a.date, d.model_type
#         """.format(date, discom),
#         engine, index_col=None)
#     return rev_data


def get_liveforecast_allrev(date, discom, config):
    """Get Live Forecast All Rev."""
    engine = create_engine(config, echo=False)
    rev_data = pd.read_sql_query("""select
        a.Date,
        d.Model_Type, e.Block_No, max(revision) Revision
        from position_map a,
             organisation_master b,
             (select *
              from
              pool_master
              where pool_name in ('INT_GENERATION_FOR', 'REALTIME_DEMAND_FOR')
              and pool_type in ('WIND', 'SOLAR', 'UNKNOWN')) c,
              power.model_master d,
              power.block_master e
        where a.organisation_master_fk = b.organisation_master_pk
        and a.model_master_fk = d.id
        and a.pool_master_fk = c.pool_master_pk
        and a.block_no_fk = e.block_no_pk
        and a.date = '{}'
        and b.organisation_code = '{}'
        group by a.date, d.model_type, e.block_no
        """.format(date, discom),
        engine, index_col=None)
    return rev_data


def intraday_forecast_report(config, discom, date, dns, dirpath):
    """Intraday Forecast Report."""
    # realtimerevdf = get_liveforecast_rev2(date, discom, config)
    realtimeallrevdf = get_liveforecast_allrev(date, discom, config)
    positionmap = get_liveposmap_data(discom, date, dns)
    positionmapdf = pd.read_json(positionmap, convert_dates=False)
    # print 'dtype', positionmapdf.dtypes
    keepcols = ['Date', 'Block_No', '1_DEMAND_ACT_UNKNOWN',
                '10_REALTIME_DEMAND_FOR_UNKNOWN',
                '18_INT_GENERATION_ACT_WIND', '19_INT_GENERATION_ACT_SOLAR',
                '27_INT_GENERATION_FOR_WIND', '28_INT_GENERATION_FOR_SOLAR']
    colstokeep = [col for col in positionmapdf.columns if col in keepcols]
    positionmapsubdf = positionmapdf[colstokeep]
    # print 'positionmapsubdf', positionmapsubdf
    positionmapsubdf['Date'] = \
        pd.to_datetime(positionmapsubdf['Date'],
                       format='%d-%m-%Y', errors='coerce')
    # print 'positionmapsubdf', positionmapsubdf
    positionmapsubdf['Date'] = positionmapsubdf['Date'].dt.strftime('%d-%m-%Y')
    datestr = positionmapsubdf['Date'][0]
    print 'Datestr', datestr
    demand_cols = ['Date', 'Block_No', '1_DEMAND_ACT_UNKNOWN',
                   '10_REALTIME_DEMAND_FOR_UNKNOWN']
    demandren = {'1_DEMAND_ACT_UNKNOWN': 'Demand_Actual',
                 '10_REALTIME_DEMAND_FOR_UNKNOWN': 'Demand_Forecast'}
    wind_cols = ['Date', 'Block_No', '18_INT_GENERATION_ACT_WIND',
                 '27_INT_GENERATION_FOR_WIND']
    wind_ren = {'18_INT_GENERATION_ACT_WIND': 'Wind_Actual',
                '27_INT_GENERATION_FOR_WIND': 'Wind_Forecast'}
    solar_cols = ['Date', 'Block_No', '19_INT_GENERATION_ACT_SOLAR',
                  '28_INT_GENERATION_FOR_SOLAR']
    solar_ren = {'19_INT_GENERATION_ACT_SOLAR': 'Solar_Actual',
                 '28_INT_GENERATION_FOR_SOLAR': 'Solar_Forecast'}

    realtimeallrevdf['Date'] = \
        pd.to_datetime(realtimeallrevdf['Date'],
                       format='%Y-%m-%d', errors='coerce')
    realtimeallrevdf['Date'] = realtimeallrevdf['Date'].dt.strftime('%d-%m-%Y')

    injectionallrev = \
        realtimeallrevdf[
            realtimeallrevdf['Model_Type'] == 'INJECTION']

    sinkallrev = \
        realtimeallrevdf[
            realtimeallrevdf['Model_Type'] == 'SINK']

    injectionrev = \
        injectionallrev['Revision'].max()
    sinkrev = \
        sinkallrev['Revision'].max()

    solarflag = True
    windflag = True
    try:
        demanddf = positionmapsubdf.ix[:, demand_cols]
        demanddf.rename(columns=demandren, inplace=True)
        demandrevdf = pd.merge(demanddf,
                               sinkallrev,
                               how='inner',
                               on=['Date', 'Block_No'])
        demandrevdf = \
            demandrevdf[['Date', 'Revision', 'Block_No',
                         'Demand_Actual', 'Demand_Forecast']]
        # print demandrevdf
        demfile_name = ['Quenext_RTDemand_', datestr,
                        '_REV_', str(sinkrev), '.xlsx']
        dempath_file_name = os.path.join(dirpath, ''.join(demfile_name))
        demandrevdf.to_excel(dempath_file_name,
                             sheet_name='Demand Forecast',
                             index=False)
    except Exception as err:
        print "Error Demand: " + str(err)
    try:
        winddf = positionmapsubdf.ix[:, wind_cols]
        winddf.rename(columns=wind_ren, inplace=True)
        windrevdf = pd.merge(winddf,
                             injectionallrev,
                             how='inner',
                             on=['Date', 'Block_No'])
        windrevdf = \
            windrevdf[['Date', 'Revision', 'Block_No',
                       'Wind_Actual', 'Wind_Forecast']]
    except Exception as err:
        print "Error Wind: " + str(err)
        windflag = False
    try:
        solardf = positionmapsubdf[solar_cols]
        solardf.rename(columns=solar_ren, inplace=True)
        solarrevdf = pd.merge(solardf,
                              injectionallrev,
                              how='inner',
                              on=['Date', 'Block_No'])
        solarrevdf = \
            solarrevdf[['Date', 'Revision', 'Block_No',
                        'Solar_Actual', 'Solar_Forecast']]
    except Exception as err:
        print "Error Solar: " + str(err)
        solarflag = False

    if solarflag and windflag:
        refile_name = ['Quenext_RTREGen_', datestr,
                       '_WINDREV_', str(injectionrev),
                       '_SOLARREV_', str(injectionrev), '.xlsx']
    elif windflag and not solarflag:
        refile_name = ['Quenext_RTREGen_', datestr,
                       '_WINDREV_', str(injectionrev), '.xlsx']
    elif solarflag and not windflag:
        refile_name = ['Quenext_RTREGen_', datestr,
                       '_SOLARREV_', str(injectionrev), '.xlsx']
    repath_file_name = os.path.join(dirpath, ''.join(refile_name))
    if windflag or solarflag:
        writer = pd.ExcelWriter(repath_file_name, engine='xlsxwriter')
        if windflag:
            windrevdf.to_excel(writer, 'Wind Forecast', index=False)
        if solarflag:
            solarrevdf.to_excel(writer, 'Solar Forecast', index=False)
        writer.save()



# dirpath = '/Users/biswadippaul/Projects/quenext-dev-latest/default/ems/batch/data/forecast/email'
# # # dirpath = '/opt/quenext_dev/ems/batch/data/forecast/email'
# config = 'mysql+mysqldb://root:quenext@2016@104.155.225.29/power'
# dns = '../config/sqldb_dev_gcloud.txt'
# discom = 'GUVNL'
# date = '2017-08-21'
# intraday_forecast_report(config, discom, date, dns, dirpath)
"""Intraday Forecast Report Generation."""
import pandas as pd
from sqlalchemy import create_engine
import os


def demand_forecast_intraday_report(date, discom, config, dirpath):
    """Intra Day Demand Forecast Report."""
    engine = create_engine(config, echo=False)
    dem_forecast = pd.read_sql_query("""select b.Date,
        b.Block_No, b.Revision,
        b.Aggregation_Type, b.Aggregation_Sub_Type,
        b.Entity_Name, b.Forecast_Quantum
        from power.realtime_forecast_metadata a,
             power.realtime_forecast b,
             power.model_master g
        where a.realtime_forecast_metadata_pk = b.realtime_forecast_metadata_fk
        and a.model_master_fk = g.id
        and a.date = '{}'
        and a.discom = '{}'
        and g.model_type = 'SINK'
        """.format(date, discom), engine, index_col=None)

    running_revision = dem_forecast['Revision'].max()

    dem_forecast_agg = \
        dem_forecast.groupby(
            ['Date', 'Block_No', 'Aggregation_Type',
             'Aggregation_Sub_Type']).sum()
    dem_forecast_agg = dem_forecast_agg.reset_index()

    dem_forecast_agg = \
        dem_forecast_agg[['Date', 'Block_No', 'Forecast_Quantum']]

    act_demand = pd.read_sql_query("""select a.Date,
        d.Block_No, round(a.Quantum,2) Quantum
        from power.position_map a,
        power.pool_master b,
        power.organisation_master c,
        power.block_master d
        where a.pool_master_fk = b.pool_master_pk
        and a.block_no_fk = d.block_no_pk
        and c.organisation_master_pk = a.organisation_master_fk
        and a.date = '{}'
        and c.organisation_code = '{}'
        and b.pool_name = 'DEMAND_ACT'
        """.format(date, discom), engine, index_col=None)

    rt_dem_for = \
        act_demand.merge(dem_forecast_agg,
                         on=['Date', 'Block_No'], how='left')

    rt_dem_for.insert(2, 'Revision', running_revision)
    renamedict = {'Quantum': 'Demand_Actual',
                  'Forecast_Quantum': 'Demand_Forecast'}
    rt_dem_for.rename(columns=renamedict, inplace=True)
    rt_dem_for['Date'] = pd.to_datetime(rt_dem_for['Date'], errors='coerce')
    rt_dem_for['Date'] = rt_dem_for['Date'].dt.strftime('%d-%m-%Y')
    datestr = rt_dem_for['Date'][0]
    file_name = ['Quenext_RTDemand_', datestr,
                 '_REV_', str(running_revision), '.xlsx']
    path_file_name = os.path.join(dirpath, ''.join(file_name))
    rt_dem_for.to_excel(''.join(path_file_name),
                        sheet_name='Demand Forecast',
                        index=False)
    return


def generation_forecast_intraday_report(date, discom, config, dirpath):
    """Intra Day Demand Forecast Report."""
    engine = create_engine(config, echo=False)
    gen_forecast = pd.read_sql_query("""select b.Date,
        b.Block_No, b.Revision,
        b.Aggregation_Type Pool_Name,
        b.Aggregation_Sub_Type Pool_Type,
        b.Entity_Name, b.Forecast_Quantum
        from power.realtime_forecast_metadata a,
             power.realtime_forecast b,
             power.model_master g
        where a.realtime_forecast_metadata_pk =
            b.realtime_forecast_metadata_fk
        and a.model_master_fk = g.id
        and a.date = '{}'
        and a.discom = '{}'
        and g.model_type = 'INJECTION'
        """.format(date, discom), engine, index_col=None)

    running_revision_wind = \
        gen_forecast[gen_forecast['Pool_Type'] == 'WIND']['Revision'].max()
    running_revision_solar = \
        gen_forecast[gen_forecast['Pool_Type'] == 'SOLAR']['Revision'].max()
    gen_forecast_agg = gen_forecast.groupby(['Date', 'Block_No',
                                             'Pool_Name', 'Pool_Type']).sum()
    gen_forecast_agg = gen_forecast_agg.reset_index()
    gen_forecast_agg = \
        gen_forecast_agg[['Date', 'Block_No', 'Pool_Type', 'Forecast_Quantum']]

    act_gen = pd.read_sql_query("""select a.Date,
        d.Block_No, b.Pool_Name, b.Pool_Type,
        a.Entity_Name, round(a.Quantum,2) Quantum
        from power.position_map a,
        power.pool_master b,
        power.organisation_master c,
        power.block_master d
        where a.pool_master_fk = b.pool_master_pk
        and a.block_no_fk = d.block_no_pk
        and c.organisation_master_pk = a.organisation_master_fk
        and a.date = '{}'
        and c.organisation_code = '{}'
        and b.pool_name = 'INT_GENERATION_ACT'
        and b.pool_type in ('WIND', 'SOLAR')
        """.format(date, discom), engine, index_col=None)
    act_gen_agg = act_gen.groupby(['Date', 'Block_No',
                                   'Pool_Name', 'Pool_Type']).sum()
    act_gen_agg = act_gen_agg.reset_index()
    rt_gen_for = act_gen_agg.merge(gen_forecast_agg,
                                   on=['Date', 'Block_No', 'Pool_Type'],
                                   how='left')
    datestr = rt_gen_for['Date'][0].strftime('%d-%m-%Y')
    rt_gen_for_wind = rt_gen_for[rt_gen_for['Pool_Type'] == 'WIND']
    rt_gen_for_solar = rt_gen_for[rt_gen_for['Pool_Type'] == 'SOLAR']
    rt_gen_for_wind.insert(2, 'Revision', running_revision_wind)
    rt_gen_for_solar.insert(2, 'Revision', running_revision_solar)
    solarrenamedict = {'Quantum': 'Solar_Actual',
                       'Forecast_Quantum': 'Solar_Forecast'}
    windrenamedict = {'Quantum': 'Wind_Actual',
                      'Forecast_Quantum': 'Wind_Forecast'}
    rt_gen_for_wind.rename(columns=windrenamedict, inplace=True)
    rt_gen_for_solar.rename(columns=solarrenamedict, inplace=True)
    rt_gen_for_wind['Date'] = \
        pd.to_datetime(rt_gen_for_wind['Date'], errors='coerce')
    rt_gen_for_wind['Date'] = rt_gen_for_wind['Date'].dt.strftime('%d-%m-%Y')
    rt_gen_for_solar['Date'] = \
        pd.to_datetime(rt_gen_for_solar['Date'], errors='coerce')
    rt_gen_for_solar['Date'] = rt_gen_for_solar['Date'].dt.strftime('%d-%m-%Y')
    dropcols = ['Pool_Name', 'Pool_Type']
    rt_gen_for_wind.drop(dropcols, inplace=True, axis=1, errors='ignore')
    rt_gen_for_solar.drop(dropcols, inplace=True, axis=1, errors='ignore')
    file_name = ['Quenext_RTREGen_', datestr,
                 '_WINDREV_', str(running_revision_wind),
                 '_SOLARREV_', str(running_revision_wind), '.xlsx']
    path_file_name = os.path.join(dirpath, ''.join(file_name))
    writer = pd.ExcelWriter(path_file_name, engine='xlsxwriter')
    rt_gen_for_wind.to_excel(writer, 'Wind Forecast', index=False)
    rt_gen_for_solar.to_excel(writer, 'Solar Forecast', index=False)
    writer.save()
    return


date = '2017-07-17'
discom = 'GUVNL'
config = 'mysql+mysqldb://root:quenext@2016@104.155.225.29/power'
dirpath = '/Users/biswadippaul/Projects/quenext-dev-latest/default/ems/batch/data/forecast'
demand_forecast_intraday_report(date, discom, config, dirpath)
generation_forecast_intraday_report(date, discom, config, dirpath)

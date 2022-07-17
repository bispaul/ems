"""Realtime Gen Schedule."""
from sqlalchemy import create_engine
import pandas as pd
# import numpy as np


def realtime_gen_schedule(config, date, discom, state):
    """Realtime Demand Schdule."""
    engine = create_engine(config, echo=False)
    valid_date = pd.to_datetime(date, format='%d-%m-%Y') + pd.DateOffset(-1)
    realtime_gen = pd.read_sql_query("""SELECT a.date, a.block_no,a.pool_name, a.pool_type,
                        case when a.quantum is not null
                        then round(a.quantum + coalesce(a.bias, 0), 2)
                        when  a.bias is not null and a.bias <> 0
                        then round(coalesce(a.quantum, 0) + a.bias, 2)
                        else null end quantum
                        from realtime_position_map_staging a,
                        (select date, pool_name, pool_type,
                         discom, state, max(revision) max_revision
                         from realtime_position_map_staging
                         where pool_name = 'INT_GENERATION_ACT'
                         and discom = '{}'
                         group by date, pool_name,
                         pool_type, discom, state) b
                        where a.date = b.date
                        and a.pool_name = b.pool_name
                        and a.pool_type = b.pool_type
                        and a.revision = b.max_revision
                        and a.discom = b.discom
                        and a.state = b.state""".format(discom),
                                     engine, index_col=None)
    realtime_gen['date'] = pd.to_datetime(realtime_gen['date'])
    realtime_gen = realtime_gen[(realtime_gen['date'] >= valid_date) &
                                (realtime_gen['date'] <=
                                pd.to_datetime(date, format='%d-%m-%Y'))]
    realtime_gen.sort_values(by=['date', 'pool_name',
                                 'pool_type', 'block_no'],
                             ascending=[True, True, True, True],
                             inplace=True)
    realtime_gen['quantum'] = \
        realtime_gen.groupby(['date', 'pool_name',
                              'pool_type'])['quantum'].ffill()
    realtime_gen[(realtime_gen['date'] ==
                  pd.to_datetime(date, format='%d-%m-%Y'))]
    realtime_forecast = realtime_gen.copy()
    realtime_forecast['revision'] = 0
    realtime_forecast['discom'] = discom
    realtime_forecast['state'] = state
    realtime_forecast['unit'] = 'MW'
    tablename = 'sch_realtime_tmp_{}'.format(discom)
    # realtime_forecast.to_sql(name=tablename, con=engine,
    #                          if_exists='replace', flavor='mysql')
    realtime_forecast.to_sql(name=tablename, con=engine,
                             if_exists='replace')

    ins_str = """insert into realtime_position_map_staging
        (date, block_no, revision, pool_name, pool_type,
         quantum, unit, discom, state)
         select date, block_no, revision, 'INT_GENERATION_SCH',
         pool_type, quantum, unit, discom, state
         from {}
         where date = str_to_date('{}', '%%d-%%m-%%Y')
         on duplicate key update
         revision = values(revision),
         quantum = values(quantum),
         unit = values(unit)""".format(tablename, date)
    connection = engine.connect()
    connection.execute(ins_str)
    connection.close()
    engine.dispose()
    return

# realtime_gen_schedule('mysql+mysqldb://root:quenext@2016@104.155.225.29/power',
#                       '17-06-2017', 'GUVNL', 'GUJARAT')

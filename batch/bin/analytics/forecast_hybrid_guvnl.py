"""Hybrid Model  GUVNL."""
from sqlalchemy import create_engine
import pandas as pd
# import numpy as np


def forecast_hybrid_guvnl(config, discom, state):
    """Forecast HYBRID GUVNL."""
    # engine = create_engine('mysql://root:power@2012@localhost/power',
    #                         echo=False)
    engine = create_engine(config, echo=False)
    data_forecast_nn = pd.read_sql("""select *
                                         from data_forecast_nn_{}""".
                                         format(discom),
                                         engine, index_col=None)
    data_forecast_MLP = pd.read_sql("""select *
                                          from data_forecast_MLP_{}""".
                                          format(discom),
                                          engine, index_col=None)
    data_forecast_SVR = pd.read_sql("""select *
                                          from data_forecast_SVR_{}""".
                                          format(discom),
                                          engine, index_col=None)
    data_forecast_DLN = pd.read_sql("""select *
                                          from data_forecast_DLN_{}""".
                                          format(discom),
                                          engine, index_col=None)
    data_forecast_hybknn = pd.read_sql_query("""select date, block_no,
        NN_PRED_WEEKDAY_EVENT_CORRECTED as HYBKNN_PRED
        from data_forecast_hybknn_{}""".format(discom), engine, index_col=None)

    pred_table = pd.merge(data_forecast_nn,
                          data_forecast_MLP,
                          how='left',
                          on=['date', 'block_no'])
    pred_table = pd.merge(pred_table,
                          data_forecast_SVR,
                          how='left',
                          on=['date', 'block_no'])
    pred_table = pd.merge(pred_table,
                          data_forecast_DLN,
                          how='left',
                          on=['date', 'block_no'])
    pred_table = pd.merge(pred_table,
                          data_forecast_hybknn,
                          how='left',
                          on=['date', 'block_no'])
    pred_table['date'] = pd.to_datetime(pred_table['date'])
    # pred_table.endo_demand.fillna(
    #     pred_table.NN_PRED_DEMAND, inplace=True)

    pred_table['HYBRID_PRED_DEMAND'] = \
        pred_table[['MLP_PRED_DEMAND',
                    'DLN_PRED',
                    'HYBKNN_PRED']].median(axis=1)

    pred_table['mape'] = abs(pred_table['endo_demand'] -
                             pred_table['HYBRID_PRED_DEMAND']) / \
        pred_table['endo_demand']

    pred_table_HYBRID = pred_table[['date', 'block_no', 'HYBRID_PRED_DEMAND']]
    pred_table_HYBRID.rename(
        columns={'HYBRID_PRED_DEMAND': 'demand_forecast'}, inplace=True)
    pred_table_HYBRID['discom'] = discom
    pred_table_HYBRID['state'] = state
    pred_table_HYBRID['revision'] = 0
    pred_table_HYBRID['model_name'] = 'HYBRID'
    tablename = 'pred_table_HYBRID_{}'.format(discom)
    pred_table_HYBRID.to_sql(con=engine, name=tablename,
                             if_exists='replace', flavor='mysql', index=False)
    sql_str = """insert into power.forecast_stg
          (date, state, revision, discom, block_no,
           model_name, demand_forecast)
          select a.date, a.state, a.revision, a.discom, a.block_no,
           a.model_name, round(a.demand_forecast,3) demand_forecast from
           {} a
          on duplicate key
          update demand_forecast = round(values(demand_forecast),3),
                 load_date = NULL""".format(tablename, discom)
    connection = engine.connect()
    connection.execute(sql_str)
    connection.close()
    engine.dispose()
    return

# config = 'mysql+mysqldb://root:quenext@2016@35.194.231.245/power'
# forecast_hybrid_guvnl(config, 'GUVNL', 'GUJARAT')

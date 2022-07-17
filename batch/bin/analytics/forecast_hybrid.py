# coding: utf-8
from sqlalchemy import create_engine
import pandas as pd


def forecast_hybrid(config):
    # engine = create_engine('mysql://root:power@2012@localhost/power',
    #                         echo=False)
    engine = create_engine(config, echo=False)
    data_forecast_nn = pd.read_sql_query('select * from data_forecast_nn',
                                         engine, index_col=None)
    data_forecast_MLP = pd.read_sql_query('select * from data_forecast_MLP',
                                          engine, index_col=None)
    data_forecast_SVR = pd.read_sql_query('select * from data_forecast_SVR',
                                          engine, index_col=None)

    pred_table = pd.merge(data_forecast_nn,
                          data_forecast_MLP,
                          how='left',
                          on=['date', 'block_no'])
    pred_table = pd.merge(pred_table,
                          data_forecast_SVR,
                          how='left',
                          on=['date', 'block_no'])
    pred_table['date'] = pd.to_datetime(pred_table['date'])
    pred_table.endo_demand.fillna(pred_table.pred_KNN_smooth, inplace=True)

    pred_table['HYBRID_PRED_DEMAND'] = pred_table[
        ['MLP_PRED_DEMAND', 'SVR_PRED_DEMAND']].median(axis=1)

    pred_table['mape'] = abs(pred_table['endo_demand'] -
                             pred_table['HYBRID_PRED_DEMAND']) / \
        pred_table['endo_demand']
    pred_table['mape1'] = abs(pred_table['demand'] -
                              pred_table['HYBRID_PRED_DEMAND']) / \
        pred_table['demand']

    pred_table_HYBRID = pred_table[['date', 'block_no', 'HYBRID_PRED_DEMAND']]
    pred_table_HYBRID.rename(
        columns={
            'HYBRID_PRED_DEMAND': 'demand_forecast'},
        inplace=True)
    pred_table_HYBRID['discom'] = 'UPCL'
    pred_table_HYBRID['state'] = 'UTTARAKHAND'
    pred_table_HYBRID['revision'] = 0
    pred_table_HYBRID['model_name'] = 'HYBRID'

    pred_table_HYBRID.to_sql(
        con=engine,
        name='pred_table_HYBRID_UPCL',
        if_exists='replace',
        flavor='mysql',
        index=False)

    sql_str = """insert into power.forecast_stg
          (date, state, revision, discom, block_no,
           model_name, demand_forecast)
          (select a.date, a.state, a.revision, a.discom, a.block_no,
           a.model_name, round(a.demand_forecast,3) demand_forecast from
           {} a,
           (select max(date) date from power.drawl_staging
            where discom = 'UPCL') b
            where a.date > b.date)
          on duplicate key
          update demand_forecast = round(values(demand_forecast),3),
                 load_date = NULL""".format('pred_table_HYBRID_UPCL')
    connection = engine.connect()
    connection.execute(sql_str)
    connection.close()
    engine.dispose()
    return

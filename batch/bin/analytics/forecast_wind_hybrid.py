"""Wind HYBRID."""
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from math import factorial


def savitzky_golay(y, window_size, order, deriv=0, rate=1):
    """Savitzky function."""
    try:
        window_size = np.abs(np.int(window_size))
        order = np.abs(np.int(order))
    except ValueError as msg:
        raise ValueError("window_size and order have to be of type int" +
                         str(msg))
    if window_size % 2 != 1 or window_size < 1:
        raise TypeError("window_size size must be a positive odd number")
    if window_size < order + 2:
        raise TypeError("window_size is too small for the polynomials order")
    order_range = range(order + 1)
    half_window = (window_size - 1) // 2
    # precompute coefficients
    b = np.mat([[k**i for i in order_range]
                for k in range(-half_window, half_window + 1)])
    m = np.linalg.pinv(b).A[deriv] * rate**deriv * factorial(deriv)
    # pad the signal at the extremes with
    # values taken from the signal itself
    firstvals = y[0] - np.abs(y[1:half_window + 1][::-1] - y[0])
    lastvals = y[-1] + np.abs(y[-half_window - 1:-1][::-1] - y[-1])
    y = np.concatenate((firstvals, y, lastvals))
    return np.convolve(m[::-1], y, mode='valid')


def forecast_wind_hybrid(config, discom, state):
    """Forecast Wind HYBRID."""
    # engine = create_engine('mysql://root:power@2012@localhost/power',
    #                         echo=False)
    pool_type = 'WIND'
    engine = create_engine(config, echo=False)
    data_forecast_hybrid_nn = pd.read_sql_query("""select date, block_no,
                                gen_forecast hybnn_gen_forecast
                                from
                                Pred_table_wind_HYBRID_NN_{}""".
                                                format(discom),
                                                engine, index_col=None)
    data_forecast_nn = pd.read_sql_query("""select date, block_no, gen_forecast nn_gen_forecast
                                          from Pred_table_wind_NN_{}""".
                                         format(discom),
                                         engine, index_col=None)
    data_forecast_nn1 = pd.read_sql_query("""select date, block_no, gen_forecast nn1_gen_forecast
                                          from Pred_table_wind_NN1_{}""".
                                          format(discom),
                                          engine, index_col=None)
    wind_generation_table = pd.read_sql_query("""select date, block_no,
                                                 sum(generation) * 4 quantum
                                                 from generation_staging
                                                 where pool_type = '{}'
                                                 and discom = '{}'
                                                 group by date, block_no""".
                                              format(pool_type, discom),
                                              engine, index_col=None)

    wind_generation_table['date'] = \
        pd.to_datetime(wind_generation_table['date'])
    wind_generation_table = \
        wind_generation_table[np.isfinite(wind_generation_table['quantum'])]
    wind_gen_smooth = pd.DataFrame([])
    unique_date = wind_generation_table['date'].unique()
    for j in range(0, len(unique_date)):
        test = wind_generation_table[wind_generation_table['date'] ==
                                     unique_date[j]]
        s = np.array(test['quantum'])
        yhat = savitzky_golay(s, window_size=11, order=1)
        envelop = pd.DataFrame(yhat)
        envelop = envelop.iloc[0:]
        test['quantum_smooth'] = envelop.values
        wind_gen_smooth = wind_gen_smooth.append(test)

    pred_table = pd.merge(data_forecast_nn1,
                          data_forecast_nn,
                          how='left',
                          on=['date', 'block_no'])
    pred_table = pd.merge(pred_table,
                          data_forecast_hybrid_nn,
                          how='left',
                          on=['date', 'block_no'])
    pred_table['date'] = pd.to_datetime(pred_table['date'])

    pred_table['gen_forecast'] = pred_table[['hybnn_gen_forecast',
                                             'nn_gen_forecast',
                                             'nn1_gen_forecast']].max(axis=1)
    pred_table = pd.merge(pred_table, wind_gen_smooth,
                          how='left', on=['date', 'block_no'])

    pred_table['smooth_forecast'] = \
        pred_table['gen_forecast'].rolling(window=5,
                                           center=True,
                                           axis=0).mean()
    mape = []
    for j in range(0, 100):
        pred_table['smooth_forecast'] = \
            pred_table['smooth_forecast'].rolling(window=5,
                                                  center=True,
                                                  axis=0).mean()
        test = pred_table.copy()
        test['mape'] = abs(test['quantum_smooth'] /
                           test['smooth_forecast'] - 1)
        test['mape1'] = abs(test['quantum_smooth'] / test['gen_forecast'] - 1)
        t = test['mape'].describe()
        mape.append(t[1])
        if j > 0 and abs(mape[j - 1] - mape[j]) <= 0.001:
            break

    pred_table_wind_hybrid = \
        pred_table[['date', 'block_no', 'smooth_forecast']].copy()
    pred_table_wind_hybrid.rename(columns={'smooth_forecast': 'gen_forecast'},
                                  inplace=True)
    pred_table_wind_hybrid['org_name'] = discom
    pred_table_wind_hybrid['pool_name'] = 'INT_GENERATION_FOR'
    pred_table_wind_hybrid['pool_type'] = pool_type
    pred_table_wind_hybrid['entity_name'] = discom
    pred_table_wind_hybrid['state'] = state
    pred_table_wind_hybrid['revision'] = 0
    pred_table_wind_hybrid['model_name'] = 'HYBRID'

    tablename = 'Pred_table_wind_HYBRID_{}'.format(discom)
    pred_table_wind_hybrid.to_sql(con=engine, name=tablename,
                                  if_exists='replace', index=False)

    sql_str = """insert into power.gen_forecast_stg
                (date, state, revision, org_name,
                pool_name, pool_type, entity_name,
                block_no, gen_forecast, Model_name)
                select a.date, a.state, a.revision, a.org_name, a.pool_name,
                 a.pool_type, a.entity_name, a.block_no,
                 round(a.gen_forecast,3),a.model_name
                 from {} a
                on duplicate key
                update gen_forecast = round(values(gen_forecast),3),
                load_date = NULL""".format(tablename)

    connection = engine.connect()
    connection.execute(sql_str)
    connection.close()
    return

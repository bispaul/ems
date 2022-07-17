# coding: utf-8

# import sklearn.metrics as metrics
# from numpy import log
import statsmodels.api as sm
from sqlalchemy import create_engine
import pandas as pd
# import datetime as dt
import numpy as np
from sklearn import preprocessing as pp
# from sklearn.metrics import mean_squared_error
# from numpy import array
# from numpy import sign
# from numpy import zeros
# from scipy.interpolate import interp1d
from datetime import timedelta
from scipy.stats import norm
from sknn.mlp import Regressor
#update the mlp.py file import sklearn.model_selection inplace of import sklearn.cross_validation
#update lasagne pool.py from theano.tensor.signal import downsample
#from theano.tensor.signal.pool import pool_2d
from sknn.mlp import Layer
from math import factorial
import datetime


def price_forecast(config, area, state):
    print(config, area)
    # engine = create_engine('mysql://root:power@2012@localhost/power',
    #                        echo=False)
    engine = create_engine(config, echo=False)
    iex_N2_Price = pd.read_sql("""select Delivery_Date as date,
                                     Block as block_no, {}_Price/1000
                                     as endo_price
                                     from exchange_areaprice_stg
                                     where Exchange_Name = 'IEX'""".
                                     format(area),
                                     engine, index_col=None)

    iex_N2_Price['date'] = pd.to_datetime(iex_N2_Price['date'])
    iex_id = [col for col in iex_N2_Price.columns if
              'date' in col or
              'block_no' in col or
              'price' in col]

    last_date_block = iex_N2_Price[
        iex_N2_Price['date'] == max(iex_N2_Price['date'])]
    max_block = max(last_date_block['block_no'])
    # columns = [iex_id]
    forecast_period0 = pd.DataFrame(columns=iex_id)

    forecast_period0['block_no'] = range(1, 97)
    forecast_period0['date'] = max(iex_N2_Price['date']) + pd.DateOffset(1)
   
    iex_N2_Price = pd.concat([iex_N2_Price, forecast_period0], axis=0)

    def savitzky_golay(y, window_size, order, deriv=0, rate=1):
        try:
            window_size = np.abs(np.int(window_size))
            order = np.abs(np.int(order))
        except ValueError as msg:
            raise ValueError("window_size and order have to be of type int" +
                             msg)
        if window_size % 2 != 1 or window_size < 1:
            raise TypeError("window_size size must be a positive odd number" +
                            msg)
        if window_size < order + 2:
            raise TypeError("""window_size is too small
                             for the polynomials order""" + msg)
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
    unique_date = iex_N2_Price['date'].unique()
    iex_N2_Price_smooth = pd.DataFrame([])
    for j in range(0, len(unique_date)):
        test = iex_N2_Price[iex_N2_Price['date'] == unique_date[j]]
        endo_price = np.array(test['endo_price'])
        smooth_price = savitzky_golay(endo_price, 19, 3)
        smooth_price = pd.DataFrame(smooth_price)
        smooth_price.rename(columns={0: 'endo_smooth_price'}, inplace=True)
        block_no = range(1, 97)
        block_no = pd.DataFrame(block_no)
        block_no.rename(columns={0: 'block_no'}, inplace=True)
        smooth_price = pd.concat([smooth_price, block_no], axis=1)
        smooth_price['date'] = unique_date[j]
        iex_N2_Price_smooth = iex_N2_Price_smooth.append(smooth_price)
    iex_N2_Price_smooth = pd.DataFrame(iex_N2_Price_smooth)
    #print('iex_N2_Price_smooth :', len(iex_N2_Price_smooth))
    iex_N2_Price = pd.merge(iex_N2_Price,
                            iex_N2_Price_smooth,
                            how='left',
                            on=['date', 'block_no'])

    iex_N2_Price['lag_endo_smooth_price'] = iex_N2_Price.\
        groupby(['block_no'])['endo_smooth_price'].\
        transform(lambda x: x.shift(1))
    iex_N2_Price['return_per_smooth'] = (iex_N2_Price['endo_smooth_price'] /
                                         iex_N2_Price['lag_endo_smooth_price']
                                         ) - 1
    iex_N2_Price['dayofweek'] = pd.DatetimeIndex(
        iex_N2_Price['date']).dayofweek  # Monday=0, Sunday=6
    iex_N2_Price['exog_cat_monday'] = np.where(
        (iex_N2_Price['dayofweek'] == 0), 1, 0)
    iex_N2_Price['exog_cat_saturday'] = np.where(
        (iex_N2_Price['dayofweek'] == 5), 1, 0)
    iex_N2_Price['exog_cat_Sunday'] = np.where(
        (iex_N2_Price['dayofweek'] == 6), 1, 0)
    iex_N2_Price['exog_lag1_return'] = iex_N2_Price.\
        groupby(['block_no'])['return_per_smooth'].\
        transform(lambda x: x.shift(1))
    iex_N2_Price['exog_lag2_return'] = iex_N2_Price.\
        groupby(['block_no'])['return_per_smooth'].\
        transform(lambda x: x.shift(2))
    iex_N2_Price['exog_lag3_return'] = iex_N2_Price.\
        groupby(['block_no'])['return_per_smooth'].\
        transform(lambda x: x.shift(3))
    iex_N2_Price['exog_lag4_return'] = iex_N2_Price.\
        groupby(['block_no'])['return_per_smooth'].\
        transform(lambda x: x.shift(4))
    iex_N2_Price.sort_values(by=['date', 'block_no'], ascending=[
                      True, True], inplace=True)
    filter_return_smooth = iex_N2_Price[iex_N2_Price['date'] > min(iex_N2_Price['date']) + pd.DateOffset(3)]
    exog_id = [col for col in filter_return_smooth.columns
               if 'exog' in col and 'cat' not in col]
    exog_cat_id = [col for col in filter_return_smooth.columns
                   if 'exog_cat' in col]
    endo_id = [col for col in filter_return_smooth.columns
               if 'return_per_smooth' in col]
    exog_var = filter_return_smooth[exog_id]
    exog_cat_var = filter_return_smooth[exog_cat_id]
    endo_var = filter_return_smooth[endo_id]

    # Generate exogenour vaiable all observation
    x = exog_var.iloc[:, :exog_var.shape[1]]
    x_cat = exog_var.iloc[:, :exog_cat_var.shape[1]]
    scaler = pp.StandardScaler().fit(x)
    st_x = scaler.transform(x)
    st_x = sm.add_constant(st_x, has_constant='add')
    st_x = np.concatenate((st_x, x_cat), axis=1)
    y = endo_var.iloc[:, :endo_var.shape[1]]
    y = y.dropna()
    ylabel = list(y.columns)
    scaler_y = pp.StandardScaler().fit(y)
    st_y = scaler_y.transform(y)
    st_y = st_y

    data_forecast_train = filter_return_smooth[
        filter_return_smooth['date'] < max(filter_return_smooth['date']) -
        pd.DateOffset(60)]

    #print('data_forecast_train', data_forecast_train,  min(data_forecast_train['date']), 
    #max(data_forecast_train['date']))
    exog_var_train = data_forecast_train[exog_id]
    exog_cat_var_train = data_forecast_train[exog_cat_id]
    endo_var_train = data_forecast_train[endo_id]

    # Generate training sample for exogenous and endogeneous variable
    x_train = exog_var_train.iloc[:, :exog_var_train.shape[1]]
    x_train_cat = exog_var_train.iloc[:, :exog_cat_var_train.shape[1]]

    train_x_scaler = pp.StandardScaler().fit(x_train)

    st_x_train = train_x_scaler.transform(x_train)
    st_x_train = sm.add_constant(st_x_train, has_constant='add')
    st_x_train = np.concatenate((st_x_train, x_train_cat), axis=1)
    y_train = endo_var_train.iloc[:, :endo_var_train.shape[1]]
    y_train = y_train.dropna()
    train_y_scaler = pp.StandardScaler().fit(y_train)
    st_y_train = train_y_scaler.transform(y_train)
    st_y_train = st_y_train
    nn_mlp = Regressor(
        layers=[
            Layer("Rectifier", units=5),
            Layer("Linear")],
        learning_momentum=0.7,
        weight_decay=0.2,
        learning_rate=0.0001,
        learning_rule="sgd",
        n_iter=1)
    #print(st_x_train.shape, st_y_train.shape)
    #pd.DataFrame(st_x_train).to_csv('st_x_train.csv')
    #pd.DataFrame(st_y_train).to_csv('st_y_train.csv')
    nn_mlp.fit(st_x_train, st_y_train)
    y_pred = nn_mlp.predict(st_x)
    # print(y_pred, len(y_pred))
    #pd.DataFrame(y_pred).to_csv('y_pred.csv')
    MuY = np.array(y_train.mean(axis=0))
    stdY = np.array(y_train.std(axis=0))
    y_pred_MLP = y_pred * stdY + MuY
    #print(y_pred_MLP, len(y_pred_MLP))
    endo_id = [col for col in filter_return_smooth.columns
               if 'date' in col or
               'block_no' in col or
               'endo'in col]
    IEX_pred_table = filter_return_smooth[endo_id]
    IEX_pred_table['pred_table_key'] = range(0, len(IEX_pred_table))
    y_pred_MLP = pd.DataFrame(y_pred_MLP)
    y_pred_MLP = y_pred_MLP.rename(columns={0: 'y_pred_MLP'})
    y_pred_MLP['y_pred_key'] = range(0, len(y_pred_MLP))
    y_pred_MLP.to_csv('y_pred_MLP.csv')

    IEX_pred_table = IEX_pred_table.merge(y_pred_MLP,
                                          left_on='pred_table_key',
                                          right_on='y_pred_key',
                                          how='outer')
    # IEX_pred_table['SVR_PRED_IEX'] = np.exp(IEX_pred_table['y_pred_SVR'])
    IEX_pred_table['MLP_PRED_IEX'] = IEX_pred_table['y_pred_MLP'] * \
        IEX_pred_table['lag_endo_smooth_price'] + \
        IEX_pred_table['lag_endo_smooth_price']
    unique_date = IEX_pred_table['date'].unique()
    MLP_PRED_IEX_SMOOTH = pd.DataFrame([])
    for j in range(0, len(unique_date)):
        test = IEX_pred_table[IEX_pred_table['date'] == unique_date[j]]
        MLP_PRED_IEX = np.array(test['MLP_PRED_IEX'])
        smooth_pred = savitzky_golay(MLP_PRED_IEX, 19, 3)
        smooth_pred = pd.DataFrame(smooth_pred)
        smooth_pred.rename(columns={0: 'smooth_pred'}, inplace=True)
        block_no = range(1, 97)
        block_no = pd.DataFrame(block_no)
        block_no.rename(columns={0: 'block_no'}, inplace=True)
        smooth_pred = pd.concat([smooth_pred, block_no], axis=1)
        smooth_pred['date'] = unique_date[j]
        MLP_PRED_IEX_SMOOTH = MLP_PRED_IEX_SMOOTH.append(smooth_pred)
    MLP_PRED_IEX_SMOOTH = pd.DataFrame(MLP_PRED_IEX_SMOOTH)
    IEX_pred_table = pd.merge(IEX_pred_table,
                              MLP_PRED_IEX_SMOOTH,
                              how='left',
                              on=['date', 'block_no'])
    IEX_pred_table['MLP_residual'] = IEX_pred_table[
        'endo_smooth_price'] - IEX_pred_table['MLP_PRED_IEX']

    IEX_pred_table['MLP_residual_percentage'] = IEX_pred_table[
        'MLP_residual'] / IEX_pred_table['MLP_PRED_IEX']

    #print('IEX_pred_table', IEX_pred_table)
    #IEX_pred_table.to_csv('IEX_pred_table.csv')

    IEX_pred_table['dayofweek'] = pd.DatetimeIndex(
        IEX_pred_table['date']).dayofweek  # Monday=0, Sunday=6
    error_pivot_dayofweek = pd.pivot_table(IEX_pred_table,
                                           values=['MLP_residual_percentage'],
                                           index=['block_no', 'dayofweek'],
                                           aggfunc=np.median).reset_index()
    weekend = np.array([0, 5, 6])
    weekend_Bias = pd.DataFrame([])
    for j in range(0, len(weekend)):
        test = error_pivot_dayofweek[
            error_pivot_dayofweek['dayofweek'] == weekend[j]]
    #     test = test.dropna()
        SVR_residual = np.array(test['MLP_residual_percentage'])
        bias = savitzky_golay(SVR_residual, 19, 3)
        bias = pd.DataFrame(bias)
        bias.rename(columns={0: 'weekend_correction'}, inplace=True)
        block_no = range(1, 97)
        block_no = pd.DataFrame(block_no)
        block_no.rename(columns={0: 'block_no'}, inplace=True)
        bias_correction = pd.concat([bias, block_no], axis=1)
        bias_correction['dayofweek'] = weekend[j]
        weekend_Bias = weekend_Bias.append(bias_correction)
    weekend_Bias = pd.DataFrame(weekend_Bias)
    IEX_pred_table = pd.merge(IEX_pred_table,
                              weekend_Bias,
                              how='left',
                              on=['dayofweek', 'block_no'])
    IEX_pred_table['weekend_correction'].fillna(0, inplace=True)
    IEX_pred_table['MLP_PRED_IEX_CORRECTED_WEEKEND'] = \
        IEX_pred_table['MLP_PRED_IEX'] + IEX_pred_table['MLP_PRED_IEX'] * \
        IEX_pred_table['weekend_correction']

    IEX_pred_table['residual_WEEKEND'] = \
        IEX_pred_table['endo_smooth_price'] - \
        IEX_pred_table['MLP_PRED_IEX_CORRECTED_WEEKEND']
    holiday_event_master = pd.read_sql("""select date, event1 as name
                                             from vw_holiday_event_master
                                             where state = '{}'""".format(state),
                                             engine, index_col=None)
    holiday_event_master['date'] = pd.to_datetime(holiday_event_master['date'])

    pre_event_master = holiday_event_master[['date', 'name']]

    pre_event_master['date'] = pre_event_master['date'] - timedelta(days=1)
    pre_event_master['name'] = 'pre_' + pre_event_master['name'].astype(str)

    post_event_master = holiday_event_master[['date', 'name']]

    post_event_master['date'] = pd.DatetimeIndex(
        post_event_master['date']) + timedelta(days=1)
    post_event_master['name'] = 'post_' + post_event_master['name'].astype(str)

    event_master = pd.DataFrame([])
    event_master = event_master.append(holiday_event_master)
    event_master = event_master.append(pre_event_master)
    event_master = event_master.append(post_event_master)
    # Added o=to only have one event per day
    event_master.drop_duplicates(['date'], inplace=True)
    col_id_bias = [col for col in IEX_pred_table.columns
                   if 'date' in col or
                   'block_no' in col or
                   'CORRECTED_WEEKEND'in col or
                   'smooth_price' in col and
                   'log' not in col and
                   'lag' not in col]

    data_event_bias = IEX_pred_table[col_id_bias]
    data_event_bias['residual_event'] = data_event_bias['endo_smooth_price'] - data_event_bias['MLP_PRED_IEX_CORRECTED_WEEKEND']

    data_event_bias['residual_percentage_event'] = data_event_bias['residual_event'] / data_event_bias['MLP_PRED_IEX_CORRECTED_WEEKEND']
    event_date = event_master[['date', 'name']]
    data_event_bias = pd.merge(
        event_date, data_event_bias, how='left', on='date')
    data_event_bias = data_event_bias.dropna()
    error_pivot_event = pd.pivot_table(data_event_bias,
                                       values=['residual_percentage_event'],
                                       index=['block_no', 'name'],
                                       aggfunc=np.median).reset_index()
    error_pivot_event = error_pivot_event.dropna()
    unique_event = error_pivot_event['name'].unique()
    event_Bias = pd.DataFrame([])
    for j in range(0, len(unique_event)):
        test = error_pivot_event[error_pivot_event['name'] == unique_event[j]]
        test = test.dropna()
        poly_coef = np.polyfit(test['block_no'],
                               test['residual_percentage_event'], 3)
        poly = np.poly1d(poly_coef)
        bias = poly(test['block_no'])
        bias = pd.DataFrame(bias)
        bias.rename(columns={0: 'event_correction'}, inplace=True)
        block_no = range(1, 97)
        block_no = pd.DataFrame(block_no)
        block_no.rename(columns={0: 'block_no'}, inplace=True)
        bias = pd.concat([bias, block_no], axis=1)
        bias['name'] = unique_event[j]
        event_Bias = event_Bias.append(bias)
    event_Bias = pd.DataFrame(event_Bias)
    IEX_pred_table = pd.merge(
        IEX_pred_table, event_master, how='left', on=['date'])
    IEX_pred_table = pd.merge(
        IEX_pred_table, event_Bias, how='left', on=['name', 'block_no'])
    IEX_pred_table['event_correction'].fillna(0, inplace=True)
    IEX_pred_table['IEX_PRED_FINAL'] = \
        IEX_pred_table['MLP_PRED_IEX_CORRECTED_WEEKEND'] + \
        IEX_pred_table['MLP_PRED_IEX_CORRECTED_WEEKEND'] * \
        IEX_pred_table['event_correction']
    IEX_pred_table['IEX_FINAL_RESID'] = IEX_pred_table[
        'endo_smooth_price'] - IEX_pred_table['IEX_PRED_FINAL']
    IEX_pred_table.sort_values(by=['date', 'block_no'], ascending=[
                        True, True], inplace=True)
    unique_block = IEX_pred_table['block_no'].unique()
    rolling_median_resid = pd.DataFrame([])
    for j in range(0, len(unique_block)):
        signal = IEX_pred_table[IEX_pred_table['block_no'] == unique_block[j]]
        #depreciated
        # signal['mean_resid'] = pd.rolling_mean(
        #     signal['IEX_FINAL_RESID'], window=11)
        signal['mean_resid'] = signal['IEX_FINAL_RESID'].rolling(window=11).mean()            
        rolling_median_resid = rolling_median_resid.append(signal)
    rolling_median_resid['mean_abs_dev_resid'] = \
        abs(rolling_median_resid['IEX_FINAL_RESID'] -
            rolling_median_resid['mean_resid'])

    rolling_median_resid_dev = pd.DataFrame([])
    for j in range(0, len(unique_block)):
        signal = rolling_median_resid[
            rolling_median_resid['block_no'] == unique_block[j]]
        #depreciated
        # signal['mad_resid_dev'] = pd.rolling_median(
        #     signal['mean_abs_dev_resid'], window=11)
        signal['mad_resid_dev'] = signal['mean_abs_dev_resid'].rolling(window=11).median()        
        rolling_median_resid_dev = rolling_median_resid_dev.append(signal)
    rolling_median_resid_dev['lag_mad_resid_dev'] = rolling_median_resid_dev.\
        groupby(['block_no'])['mad_resid_dev'].\
        transform(lambda x: x.shift(1))

    IEX_pred_table['mape_final'] = abs(IEX_pred_table['IEX_PRED_FINAL'] -
                                       IEX_pred_table['endo_smooth_price']) / \
        IEX_pred_table['endo_smooth_price']
    IEX_pred_table['mape_final'].describe()
    rolling_median_resid_dev.mad_resid_dev.fillna(
        rolling_median_resid_dev.lag_mad_resid_dev, inplace=True)
    rolling_median_resid_dev.to_sql(con=engine,
                                    name='rolling_median_resid_{}'.
                                    format(area),
                                    if_exists='replace', index=False)


def price_simulator(config, alpha, date, area):
    engine = create_engine(config, echo=False)
    rolling_median_resid_dev = pd.read_sql("""select *
                                        from
                                        rolling_median_resid_{}
                                        where date =
                                        str_to_date('{}', '%%d-%%m-%%Y')""".
                                                 format(area, date),
                                                 engine, index_col=None)
    rolling_median_resid_dev['UL_IEX_FINAL'] = \
        rolling_median_resid_dev['IEX_PRED_FINAL'] + \
        norm.ppf(alpha) * rolling_median_resid_dev['mad_resid_dev']
    rolling_median_resid_dev['LL_IEX_FINAL'] = \
        rolling_median_resid_dev['IEX_PRED_FINAL'] - \
        norm.ppf(alpha) * rolling_median_resid_dev['mad_resid_dev']
    pred_id = [col for col in rolling_median_resid_dev.columns
               if 'date' in col or
               'block_no' in col or
               'endo_price' in col or
               'UL'in col or
               'LL' in col]
    pred_price = rolling_median_resid_dev[pred_id]
    pred_price['alpha'] = alpha
    pred_price['area'] = area
    pred_price['exchange'] = "IEX"
    # maxrev = pd.read_sql_query("""select
    #           case when max(revision) is null
    #           then 0 else max(revision) + 1 end as maxrev
    #           from
    #           power.pred_price_final
    #           where date = str_to_date('{}', '%%d-%%m-%%Y')
    #           and area = '{}'
    #           and alpha = {}""".format(date, 'N2', alpha),
    #                            engine, index_col=None)
    # rev = maxrev['maxrev'].iloc[0]
    # pred_price['revision'] = rev
    pred_price['model_name'] = 'PRICE_{}'.format(area)
    usedt = datetime.datetime.strptime(date, '%d-%m-%Y')
    temp = pred_price[(pred_price['date'] == usedt)]
    # test = [tuple(x) for x in temp.to_records(index=False)]
    temp.to_sql(con=engine, name='pred_price_tmp',
                if_exists='replace',index=False)
    # temp.to_sql(con=engine, name='pred_price_tmp',
    #             if_exists='replace',
    #             index=False)
    sql_str = """insert into pred_price_final
              (date, area, alpha, block_no,
               exchange, model_name, ul_price, ll_price)
               select a.date, a.area, a.alpha, a.block_no, a.exchange,
               a.model_name, round(a.ul_iex_final,3),
               round(a.ll_iex_final,3)
               from
               pred_price_tmp a,
               (select date, area, alpha, block_no, exchange,
               model_name, max(ul_iex_final) max_ul_iex_final,
               max(ll_iex_final) max_ll_iex_final
               from pred_price_tmp
               group by date, area, alpha, block_no, exchange,
               model_name) b
               where a.date = b.date
               and a.area = b.area
               and a.alpha = b.alpha
               and a.block_no = b.block_no
               and a.exchange = b.exchange
               and a.ul_iex_final = b.max_ul_iex_final
               and a.ll_iex_final = b.max_ll_iex_final
               ON DUPLICATE KEY UPDATE
               ul_price = round(a.ul_iex_final,3),
               ll_price = round(a.ll_iex_final,3)"""
    connection = engine.connect()
    connection.execute(sql_str)
    drop_str = """drop table pred_price_tmp"""
    connection.execute(drop_str)
    connection.close()
    engine.dispose()
    return

# pred_price_final = price_simulator(config, alpha)
# config = 'mysql+mysqldb://root:quenext@2016@104.155.225.29/power'
# alpha = 0.95
# area = 'W2'
# datelist = [d.strftime('%d-%m-%Y') for d in pd.date_range('20170902', '20170905')]
# for x in datelist:
#     price_simulator(config, alpha, x, area)

# config = 'mysql+pymysql://root:power@2020@localhost/power'
# state = 'UTTARAKHAND'
# area = 'N2'
# price_forecast(config, area, state)
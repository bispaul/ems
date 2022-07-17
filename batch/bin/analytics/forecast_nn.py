# coding: utf-8
# import statsmodels.datasets as datasets
# import sklearn.metrics as metrics
# from numpy import log
# from pyearth import Earth as earth
# from matplotlib import pyplot
# from matplotlib import pyplot as plt
# import statsmodels.api as sm
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from math import factorial
from datetime import timedelta


def forecast_nn(config):
    """Nearesr Neighbour Scoring function."""
    # engine = create_engine('mysql://root:power@2012@localhost/power',
    #                         echo=False)
    engine = create_engine(config, echo=False)
    weather_dist_lag = pd.read_sql_query(
        'select * from weather_dist_lag_UPCL',
        engine,
        index_col=None)
    weather_dist_lag['date'] = pd.to_datetime(weather_dist_lag['date'])
    date_key = pd.read_sql_query(
        'select * from date_key_UPCL',
        engine,
        index_col=None)
    date_key['date'] = pd.to_datetime(date_key['date'])
    lag_operator_UPCL = pd.read_sql_query(
        'select * from lag_operator_UPCL', engine, index_col=None)
    daily_weather_mat = pd.read_sql_query(
        'select * from daily_weather_mat_UPCL',
        engine,
        index_col=None)
    daily_weather_mat['date'] = pd.to_datetime(daily_weather_mat['date'])
    holiday_event_master = pd.read_sql_query(
        ("select date, event1 as name from vw_holiday_event_master "
         "where state = 'UTTARAKHAND'"),
        engine,
        index_col=None)
    holiday_event_master['date'] = pd.to_datetime(holiday_event_master['date'])

    def savitzky_golay(y, window_size, order, deriv=0, rate=1):
        try:
            window_size = np.abs(np.int(window_size))
            order = np.abs(np.int(order))
        except ValueError as msg:
            raise ValueError(("window_size and order have"
                              " to be of type int: %s"), msg)
        if window_size % 2 != 1 or window_size < 1:
            raise TypeError("window_size size must be a positive odd number")
        if window_size < order + 2:
            raise TypeError("window_size is too small"
                            " for the polynomials order")
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

    daily_weather_mat_logit = daily_weather_mat

    def logit_transform(x):
        pivot = 24
        multipler = 1.3
        beta = 0.4
        delta = 30
        T = np.where((x < pivot), x + (pivot - x) * multipler, x)
        logit_x = (np.exp(beta * (T - delta)) /
                   (1 + np.exp(beta * (T - delta)))) * 100
        return logit_x

    daily_weather_mat_logit['temp_median_location_01_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_median_location_01'])
    daily_weather_mat_logit['temp_median_location_02_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_median_location_02'])
    daily_weather_mat_logit['temp_median_location_03_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_median_location_03'])
    daily_weather_mat_logit['temp_median_location_04_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_median_location_04'])
    daily_weather_mat_logit['temp_median_location_05_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_median_location_05'])
    daily_weather_mat_logit['temp_median_location_06_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_median_location_06'])
    daily_weather_mat_logit['temp_median_location_07_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_median_location_07'])
    daily_weather_mat_logit['temp_median_location_09_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_median_location_09'])
    daily_weather_mat_logit['temp_median_location_11_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_median_location_11'])
    daily_weather_mat_logit['temp_median_location_13_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_median_location_13'])
    daily_weather_mat_logit['temp_median_location_14_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_median_location_14'])
    daily_weather_mat_logit['temp_median_location_17_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_median_location_17'])
    daily_weather_mat_logit['temp_median_location_18_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_median_location_18'])
    daily_weather_mat_logit['temp_median_location_19_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_median_location_19'])
    daily_weather_mat_logit['temp_median_location_20_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_median_location_20'])
    daily_weather_mat_logit['temp_median_location_21_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_median_location_21'])
    daily_weather_mat_logit['temp_median_location_22_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_median_location_22'])
    daily_weather_mat_logit['temp_median_location_23_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_median_location_23'])

    daily_weather_mat_logit['temp_mean_location_01_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_mean_location_01'])
    daily_weather_mat_logit['temp_mean_location_02_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_mean_location_02'])
    daily_weather_mat_logit['temp_mean_location_03_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_mean_location_03'])
    daily_weather_mat_logit['temp_mean_location_04_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_mean_location_04'])
    daily_weather_mat_logit['temp_mean_location_05_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_mean_location_05'])
    daily_weather_mat_logit['temp_mean_location_06_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_mean_location_06'])
    daily_weather_mat_logit['temp_mean_location_07_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_mean_location_07'])
    daily_weather_mat_logit['temp_mean_location_09_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_mean_location_09'])
    daily_weather_mat_logit['temp_mean_location_11_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_mean_location_11'])
    daily_weather_mat_logit['temp_mean_location_13_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_mean_location_13'])
    daily_weather_mat_logit['temp_mean_location_14_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_mean_location_14'])
    daily_weather_mat_logit['temp_mean_location_17_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_mean_location_17'])
    daily_weather_mat_logit['temp_mean_location_18_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_mean_location_18'])
    daily_weather_mat_logit['temp_mean_location_19_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_mean_location_19'])
    daily_weather_mat_logit['temp_mean_location_20_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_mean_location_20'])
    daily_weather_mat_logit['temp_mean_location_21_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_mean_location_21'])
    daily_weather_mat_logit['temp_mean_location_22_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_mean_location_22'])
    daily_weather_mat_logit['temp_mean_location_23_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_mean_location_23'])

    daily_weather_mat_logit['temp_max_location_01_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_max_location_01'])
    daily_weather_mat_logit['temp_max_location_02_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_max_location_02'])
    daily_weather_mat_logit['temp_max_location_03_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_max_location_03'])
    daily_weather_mat_logit['temp_max_location_04_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_max_location_04'])
    daily_weather_mat_logit['temp_max_location_05_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_max_location_05'])
    daily_weather_mat_logit['temp_max_location_06_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_max_location_06'])
    daily_weather_mat_logit['temp_max_location_07_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_max_location_07'])
    daily_weather_mat_logit['temp_max_location_09_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_max_location_09'])
    daily_weather_mat_logit['temp_max_location_11_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_max_location_11'])
    daily_weather_mat_logit['temp_max_location_13_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_max_location_13'])
    daily_weather_mat_logit['temp_max_location_14_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_max_location_14'])
    daily_weather_mat_logit['temp_max_location_17_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_max_location_17'])
    daily_weather_mat_logit['temp_max_location_18_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_max_location_18'])
    daily_weather_mat_logit['temp_max_location_19_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_max_location_19'])
    daily_weather_mat_logit['temp_max_location_20_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_max_location_20'])
    daily_weather_mat_logit['temp_max_location_21_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_max_location_21'])
    daily_weather_mat_logit['temp_max_location_22_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_max_location_22'])
    daily_weather_mat_logit['temp_max_location_23_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_max_location_23'])

    daily_weather_mat_logit['temp_min_location_01_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_min_location_01'])
    daily_weather_mat_logit['temp_min_location_02_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_min_location_02'])
    daily_weather_mat_logit['temp_min_location_03_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_min_location_03'])
    daily_weather_mat_logit['temp_min_location_04_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_min_location_04'])
    daily_weather_mat_logit['temp_min_location_05_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_min_location_05'])
    daily_weather_mat_logit['temp_min_location_06_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_min_location_06'])
    daily_weather_mat_logit['temp_min_location_07_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_min_location_07'])
    daily_weather_mat_logit['temp_min_location_09_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_min_location_09'])
    daily_weather_mat_logit['temp_min_location_11_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_min_location_11'])
    daily_weather_mat_logit['temp_min_location_13_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_min_location_13'])
    daily_weather_mat_logit['temp_min_location_14_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_min_location_14'])
    daily_weather_mat_logit['temp_min_location_17_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_min_location_17'])
    daily_weather_mat_logit['temp_min_location_18_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_min_location_18'])
    daily_weather_mat_logit['temp_min_location_19_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_min_location_19'])
    daily_weather_mat_logit['temp_min_location_20_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_min_location_20'])
    daily_weather_mat_logit['temp_min_location_21_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_min_location_21'])
    daily_weather_mat_logit['temp_min_location_22_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_min_location_22'])
    daily_weather_mat_logit['temp_min_location_23_logit'] =\
        logit_transform(daily_weather_mat_logit['temp_min_location_23'])
    daily_weather_mat_logit.to_sql(name='daily_weather_mat_logit_upcl',
                                   con=engine,
                                   if_exists='replace',
                                   flavor='mysql')
    # In[3]:
    load_weather_lag = pd.merge(daily_weather_mat_logit,
                                lag_operator_UPCL,
                                how='outer',
                                on=['date_key'])
    load_weather_lag = load_weather_lag.merge(daily_weather_mat_logit,
                                              left_on=['lag1'],
                                              right_on=['date_key'],
                                              suffixes=('_left', '_right'))
    # list(load_weather_lag)
    date = load_weather_lag[['date_left', 'date_key_left']]
    date = date.rename(columns={col: col.split(
        '_left')[0] for col in date.columns})

    load_weather_lag_Left = load_weather_lag[load_weather_lag. columns[
        load_weather_lag.columns.to_series(). str.contains('left')]]

    load_weather_lag_Left = load_weather_lag_Left.rename(
        columns={col: col.split('_left')[0]
                 for col in load_weather_lag_Left.columns})

    load_weather_lag_Left = load_weather_lag_Left.drop('date', axis=1)
    load_weather_lag_Left = load_weather_lag_Left.drop('date_key', axis=1)

    load_weather_lag_Right = load_weather_lag[load_weather_lag. columns[
        load_weather_lag.columns.to_series(). str.contains('right')]]
    load_weather_lag_Right = load_weather_lag_Right.rename(
        columns={col: col.split('_right')[0]
                 for col in load_weather_lag_Right.columns})
    load_weather_lag_Right = load_weather_lag_Right.drop('date', axis=1)
    load_weather_lag_Right = load_weather_lag_Right.drop('date_key', axis=1)

    load_weather_lag_diff = load_weather_lag_Left - load_weather_lag_Right
    load_weather_lag_diff.columns = [
        'exog_cont_' +
        str(col) +
        '_diff' for col in load_weather_lag_diff.columns]

    load_weather_lag_diff = pd.concat([date, load_weather_lag_diff], axis=1)

    # In[5]:
    data_forecast = pd.merge(weather_dist_lag,
                             load_weather_lag_diff,
                             how='left',
                             on=['date_key'])
    data_forecast.rename(columns={'date_x': 'date'},
                         inplace=True)
    data_forecast['endo_residual'] = data_forecast['endo_demand'] - \
        data_forecast['endo_pred_sim_day_load']
    data_forecast['endo_residual_percentage'] =\
        data_forecast['endo_residual'] /\
        data_forecast['endo_pred_sim_day_load']
    data_forecast = data_forecast.drop('date_y', axis=1)
    data_forecast = data_forecast[data_forecast['year'] >= 2016]
    data_forecast['lag7_endo_residual_percentage'] = \
        data_forecast.groupby(['block_no'])['endo_residual_percentage']\
        .shift(7)
    data_forecast['lag14_endo_residual_percentage'] = \
        data_forecast.groupby(['block_no'])['endo_residual_percentage']\
        .shift(14)
    data_forecast['lag21_endo_residual_percentage'] = \
        data_forecast.groupby(['block_no'])['endo_residual_percentage']\
        .shift(21)
    data_forecast['lag28_endo_residual_percentage'] = \
        data_forecast.groupby(['block_no'])['endo_residual_percentage']\
        .shift(28)
    data_forecast['lag35_endo_residual_percentage'] = \
        data_forecast.groupby(['block_no'])['endo_residual_percentage']\
        .shift(35)

    t = 0.9
    w1 = 1
    w2 = w1 * t
    w3 = w1 * t**2
    w4 = w1 * t**3
    w5 = w1 * t**4

    data_forecast['weekend_correction'] = (
        (data_forecast['lag7_endo_residual_percentage'] * w1 +
         data_forecast['lag14_endo_residual_percentage'] * w2 +
         data_forecast['lag21_endo_residual_percentage'] * w3 +
         data_forecast['lag28_endo_residual_percentage'] * w4 +
         data_forecast['lag35_endo_residual_percentage'] * w5) /
        (w1 + w2 + w3 + w4 + w5))

    data_forecast['weekend_correction'] =\
        np.where((data_forecast['week_day'] == 0) |
                 (data_forecast['week_day'] == 5) |
                 (data_forecast['week_day'] == 6),
                 data_forecast['weekend_correction'], 0)

    unique_date = data_forecast['date'].unique()
    weekend_correction_smooth = pd.DataFrame([])
    for j in xrange(0, len(unique_date)):
        test = data_forecast[data_forecast['date'] == unique_date[j]]
        poly_coef = np.polyfit(test['block_no'],
                               test['weekend_correction'], 15)
        poly = np.poly1d(poly_coef)
        week_end_bias = poly(test['block_no'])
        week_end_bias = pd.DataFrame(week_end_bias)
        week_end_bias.rename(columns={0: 'weekend_correction_smooth'},
                             inplace=True)
        block_no = range(1, 97)
        block_no = pd.DataFrame(block_no)
        block_no.rename(columns={0: 'block_no'}, inplace=True)
        week_end_bias = pd.concat([week_end_bias, block_no], axis=1)
        week_end_bias['date'] = unique_date[j]
        weekend_correction_smooth =\
            weekend_correction_smooth.append(week_end_bias)
    weekend_correction_smooth = pd.DataFrame(weekend_correction_smooth)

    # In[8]:

    data_forecast = pd.merge(
        data_forecast, weekend_correction_smooth,
        how='left', on=['date', 'block_no'])

    # In[9]:

    data_forecast['weekend_correction_smooth'].fillna(0, inplace=True)

    # In[10]:

    data_forecast['endo_pred_sim_day_load_final'] = data_forecast[
        'endo_pred_sim_day_load'] + data_forecast[
        'endo_pred_sim_day_load'] * data_forecast['weekend_correction_smooth']

    # In[11]:

    data_forecast['endo_residual_weekend_correction'] = data_forecast[
        'endo_demand'] - data_forecast['endo_pred_sim_day_load_final']

    # In[12]:

    data_bias_fest = data_forecast[['date',
                                    'year',
                                    'month',
                                    'block_no',
                                    'hour',
                                    'endo_demand',
                                    'endo_pred_sim_day_load',
                                    'endo_pred_sim_day_load_final',
                                    'endo_residual_weekend_correction']]

    pre_event_master = holiday_event_master[['date', 'name']]
    pre_event_master['date'] = pre_event_master['date'] - timedelta(days=1)
    pre_event_master['name'] = 'pre_' + pre_event_master['name'].astype(str)
    post_event_master = holiday_event_master[['date', 'name']]
    post_event_master['date'] = pd.DatetimeIndex(post_event_master['date']) + \
        timedelta(days=1)
    post_event_master['name'] = 'post_' + post_event_master['name'].astype(str)
    event_master = pd.DataFrame([])
    event_master = event_master.append(holiday_event_master)
    event_master = event_master.append(pre_event_master)
    event_master = event_master.append(post_event_master)

    # In[13]:

    # event_date = holiday_event_master[['date', 'name']]

    # In[14]:

    data_bias_fest = pd.merge(
        data_bias_fest,
        event_master,
        on=['date'],
        how='left')

    # In[15]:

    data_bias_fest.to_sql(
        name='upcl_data_bias_fest',
        con=engine,
        if_exists='replace',
        flavor='mysql')

    data_bias_fest['endo_residual_weekend_correction_percentage'] = \
        data_bias_fest['endo_residual_weekend_correction'] /\
        data_bias_fest['endo_pred_sim_day_load_final']

    error_pivot_fest = pd.pivot_table(
        data_bias_fest,
        values=['endo_residual_weekend_correction_percentage'],
        index=['block_no', 'name'],
        aggfunc=np.median).reset_index()
    error_pivot_fest = error_pivot_fest.dropna()

    # In[17]:

    unique_event = error_pivot_fest['name'].unique()
    event_Bias = pd.DataFrame([])
    for j in xrange(0, len(unique_event)):
        test = error_pivot_fest[error_pivot_fest['name'] == unique_event[j]]
        test = test.dropna()
        poly_coef = np.polyfit(test['block_no'], test[
            'endo_residual_weekend_correction_percentage'], 15)
        poly = np.poly1d(poly_coef)
        bias = poly(test['block_no'])
        bias = pd.DataFrame(bias)
        bias.rename(columns={0: 'fest_correction'}, inplace=True)
        block_no = range(1, 97)
        block_no = pd.DataFrame(block_no)
        block_no.rename(columns={0: 'block_no'}, inplace=True)
        bias = pd.concat([bias, block_no], axis=1)
        bias['event_name'] = unique_event[j]
        event_Bias = event_Bias.append(bias)
    event_Bias = pd.DataFrame(event_Bias)

    # In[18]:

    data_forecast = pd.merge(
        data_forecast,
        event_master,
        how='left',
        on=['date'])

    # In[19]:

    data_forecast = pd.merge(
        data_forecast, event_Bias, how='left', left_on=[
            'name', 'block_no'], right_on=[
            'event_name', 'block_no'])

    # In[20]:

    data_forecast['fest_correction'].fillna(0, inplace=True)

    # In[21]:

    data_forecast['endo_sim_day_pred_demand_final_fest'] = data_forecast[
        'endo_pred_sim_day_load_final'] + data_forecast[
        'endo_pred_sim_day_load_final'] * data_forecast['fest_correction']

    # In[22]:

    data_forecast['residual_post_weekend_event'] = data_forecast[
        'endo_demand'] - data_forecast['endo_sim_day_pred_demand_final_fest']

    # In[23]:

    peak_off_peak_bias = pd.pivot_table(
        data_forecast, values=['residual_post_weekend_event'], index=[
            'block_no', 'month'], aggfunc=np.mean).reset_index()
    peak_off_peak_bias = peak_off_peak_bias.dropna()

    # In[24]:

    months = peak_off_peak_bias['month'].unique()
    month_shape_Bias = pd.DataFrame([])
    for j in xrange(0, len(months)):
        test = peak_off_peak_bias[peak_off_peak_bias['month'] == months[j]]
        test = test.dropna()
        month_poly_coef = np.polyfit(test['block_no'],
                                     test['residual_post_weekend_event'], 15)
        month_poly = np.poly1d(month_poly_coef)
        month_bias = month_poly(test['block_no'])
        month_bias = pd.DataFrame(month_bias)
        month_bias.rename(columns={0: 'month_shape_Bias'}, inplace=True)
        block_no = range(1, 97)
        block_no = pd.DataFrame(block_no)
        block_no.rename(columns={0: 'block_no'}, inplace=True)
        month_bias_correction = pd.concat([month_bias, block_no], axis=1)
        month_bias_correction['month'] = months[j]
        month_shape_Bias = month_shape_Bias.append(month_bias_correction)
    month_shape_Bias = pd.DataFrame(month_shape_Bias)

    # In[25]:

    data_forecast = pd.merge(
        data_forecast, month_shape_Bias, how='left', on=[
            'month', 'block_no'])
    data_forecast['month_shape_Bias'].fillna(0, inplace=True)

    data_forecast['endo_deterministic_demand_pred_shape'] = \
        data_forecast['endo_sim_day_pred_demand_final_fest'] + \
        data_forecast['month_shape_Bias']

    data_forecast.sort(['date', 'block_no'], ascending=[
                       True, True], inplace=True)
   # data_forecast['pred_KNN_smooth'] = data_forecast['endo_deterministic_demand_pred_shape']
   # data_forecast.sort(['date','block_no'], ascending=[True, True], inplace=True)

    unique_date = data_forecast['date'].unique()
    pred_table_test = pd.DataFrame([])
    for j in xrange(0, len(unique_date)):
        test = data_forecast[data_forecast['date'] == unique_date[j]]
        endo_deterministic_demand_pred_shape = \
            np.array(test['endo_deterministic_demand_pred_shape'])
        pred_KNN_smooth = \
            savitzky_golay(endo_deterministic_demand_pred_shape, 13, 3)
        test = test[['date', 'block_no']].reset_index()
        pred_KNN_smooth = pd.DataFrame(pred_KNN_smooth)
        pred_KNN_smooth = \
            pred_KNN_smooth.rename(columns={0: 'pred_KNN_smooth'})
        pred_KNN_smooth = pd.concat([test, pred_KNN_smooth], axis=1)
        pred_table_test = pred_table_test.append(pred_KNN_smooth)

    data_forecast = pd.merge(data_forecast, pred_table_test,
                             how='left', on=['date', 'block_no'])

    data_forecast['endo_residual_deterministic'] = data_forecast[
        'endo_demand'] - data_forecast['pred_KNN_smooth']
    data_forecast_nn = data_forecast[['date', 'block_no', 'demand',
                                      'endo_demand', 'pred_KNN_smooth']]
    data_forecast_nn.to_sql(name='data_forecast_nn',
                            con=engine,
                            if_exists='replace',
                            flavor='mysql')
    # In[31]:

    data_forecast.to_sql(
        name='data_forecast_UPCL',
        con=engine,
        if_exists='replace',
        flavor='mysql')

    # In[32]:

    pred_table_similarday = data_forecast[
        ['date', 'block_no', 'pred_KNN_smooth']]
    pred_table_similarday.rename(
        columns={
            'pred_KNN_smooth': 'demand_forecast'},
        inplace=True)
    pred_table_similarday['discom'] = 'UPCL'
    pred_table_similarday['state'] = 'UTTARAKHAND'
    pred_table_similarday['revision'] = 0
    pred_table_similarday['model_name'] = 'NEAREST_NEIGHBOUR'

    # In[33]:

    pred_table_similarday.to_sql(name='pred_table_similarday', con=engine,
                                 if_exists='replace', flavor='mysql')

    sql_str = """insert into power.forecast_stg
          (date, state, revision, discom, block_no,
           model_name, demand_forecast)
          (select a.date, a.state, a.revision, a.discom, a.block_no,
           a.model_name, round(a.demand_forecast,3) demand_forecast from
           {} a,
           (select max(date) date from power.drawl_staging
            where discom = 'UPCL') b
            where a.date >= b.date)
          on duplicate key
          update demand_forecast = round(values(demand_forecast),3),
                 load_date = NULL""".format('pred_table_similarday')
    connection = engine.connect()
    connection.execute(sql_str)
    connection.close()
    engine.dispose()
    return

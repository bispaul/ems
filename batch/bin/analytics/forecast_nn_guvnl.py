"""Forecast Nearest Neighbour."""
# import statsmodels.datasets as datasets
# import sklearn.metrics as metrics
# from numpy import log
# import statsmodels.api as sm
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
# import datetime as dt
from datetime import timedelta


def forecast_nn_guvnl(config, discom, state):
    """Forecast Nearest Neighbour function."""
    # engine = \
    #     create_engine('mysql://root:power@2012@localhost/power', echo=False)
    # state = 'UTTARAKHAND'
    engine = create_engine(config, echo=False)
    weather_dist_lag = pd.read_sql_query(
        'select * from weather_dist_lag_{}'.format(discom),
        engine,
        index_col=None)
    weather_data_pivot = pd.read_sql_query(
        'select * from weather_data_pivot_{}'.format(discom),
        engine,
        index_col=None)
    weather_dist_lag['date'] = pd.to_datetime(weather_dist_lag['date'])
    date_key = pd.read_sql_query(
        'select * from date_key_{}'.format(discom),
        engine,
        index_col=None)
    date_key['date'] = pd.to_datetime(date_key['date'])
    lag_operator_GETCO = pd.read_sql_query(
        'select * from lag_operator_{}'.format(discom),
        engine,
        index_col=None)
    daily_weather_mat = pd.read_sql_query(
        'select * from daily_weather_mat_{}'.format(discom),
        engine,
        index_col=None)
    daily_weather_mat['date'] = pd.to_datetime(daily_weather_mat['date'])
    # Holiday Event Master to be changed
    holiday_event_master = pd.read_sql_query("""select date, event1 as name
                                                from vw_holiday_event_master
                                                where state = '{}'""".
                                             format(state),
                                             engine, index_col=None)
    holiday_event_master['date'] = pd.to_datetime(holiday_event_master['date'])

    def logit_transform(x):
        pivot = 25
        multipler = 1.5
        beta = 0.4
        delta = 38
        T = np.where((x < pivot), x + (pivot - x) * multipler, x)
        logit_x = (np.exp(beta * (T - delta)) /
                   (1 + np.exp(beta * (T - delta)))) * 100
        return logit_x

    daily_weather_mat_logit = daily_weather_mat.copy()

    temp_var = [col for col in daily_weather_mat_logit.columns
                if 'temp_' in col and '_dev_' not in col]

    for i in xrange(0, len(temp_var)):
        daily_weather_mat_logit[str(
            temp_var[i]) + '_logit'] = \
            logit_transform(daily_weather_mat_logit[temp_var[i]])

    weather_data_hourly_logit = weather_data_pivot.copy()
    weather_data_hourly_logit.sort_values(
        by=['date', 'hour'], ascending=[True, True], inplace=True)

    temp_var = [col for col in weather_data_hourly_logit.columns
                if 'temp_imp' in col]

    for i in xrange(0, len(temp_var)):
        weather_data_hourly_logit[str('exog_cont_' + temp_var[i]) +
                                  '_hourly_logit'] = \
            logit_transform(weather_data_hourly_logit[temp_var[i]])

    del weather_data_hourly_logit['index']

    daily_weather_mat_logit.to_sql(
        name='daily_weather_mat_logit_lag_{}'.format(discom),
        con=engine,
        if_exists='replace',
        flavor='mysql')
    weather_data_hourly_logit.to_sql(
        name='weather_data_hourly_logit_lag_{}'.format(discom),
        con=engine,
        if_exists='replace',
        flavor='mysql')

    load_weather_lag = pd.merge(daily_weather_mat_logit,
                                lag_operator_GETCO,
                                how='outer', on=['date_key'])

    load_weather_lag = load_weather_lag.merge(daily_weather_mat_logit,
                                              left_on=['lag1'],
                                              right_on=['date_key'],
                                              suffixes=('_left', '_right'))

    date = load_weather_lag[['date_left', 'date_key_left']]
    date = date.rename(columns={col: col.split('_left')[0]
                                for col in date.columns})

    load_weather_lag_Left = \
        load_weather_lag[load_weather_lag.columns[load_weather_lag.columns.
                                                  to_series().str.
                                                  contains('left')]]

    load_weather_lag_Left = \
        load_weather_lag_Left.rename(columns={col: col.split('_left')[0]
                                     for col in load_weather_lag_Left.columns})

    load_weather_lag_Left = load_weather_lag_Left.drop('date', axis=1)
    load_weather_lag_Left = load_weather_lag_Left.drop('date_key', axis=1)

    load_weather_lag_Right = \
        load_weather_lag[load_weather_lag.columns[load_weather_lag.columns.
                                                  to_series().str.
                                                  contains('right')]]
    load_weather_lag_Right = \
        load_weather_lag_Right.rename(columns={col: col.split('_right')[0]
                                               for col in
                                               load_weather_lag_Right.columns})
    load_weather_lag_Right = load_weather_lag_Right.drop('date', axis=1)
    load_weather_lag_Right = load_weather_lag_Right.drop('date_key', axis=1)

    load_weather_lag_diff = load_weather_lag_Left - load_weather_lag_Right
    load_weather_lag_diff.columns = ['exog_cont_' + str(col) + '_diff'
                                     for col in load_weather_lag_diff.columns]

    load_weather_lag_diff = pd.concat([date, load_weather_lag_diff], axis=1)

    data_forecast = pd.merge(weather_dist_lag, load_weather_lag_diff,
                             how='left', on=['date'])

    data_forecast['endo_residual'] = \
        data_forecast['endo_demand'] - data_forecast['endo_pred_sim_day_load']
    data_forecast['endo_residual_percentage'] = \
        data_forecast['endo_residual'] / \
        data_forecast['endo_pred_sim_day_load']
    data_forecast['endo_residual_percentage'] = \
        np.where((data_forecast['endo_residual_percentage'] <= 0),
                 data_forecast['endo_residual_percentage'],
                 0)

    load_summary = \
        data_forecast.groupby(['month', 'block_no'], as_index=False).\
        agg({'endo_demand': {'median': 'median'}})
    load_summary.columns = ['_'.join(col).strip()
                            for col in load_summary.columns.values]
    load_summary.rename(columns={'block_no_': 'block_no',
                                 'month_': 'month'}, inplace=True)

    load_summary_weekday = \
        data_forecast.groupby(['month', 'block_no',
                               'week_day'], as_index=False).\
        agg({'endo_demand': {'weekday_median': 'median'}})
    load_summary_weekday.columns = \
        ['_'.join(col).strip()
         for col in load_summary_weekday.columns.values]
    load_summary_weekday.rename(columns={'month_': 'month',
                                         'block_no_': 'block_no',
                                         'week_day_': 'week_day'},
                                inplace=True)
    load_summary.rename(columns={'block_no_': 'block_no'}, inplace=True)

    weekday_correction = pd.merge(load_summary_weekday,
                                  load_summary,
                                  how='left',
                                  on=['month', 'block_no']
                                  )
    weekday_correction['week_day_correction_factor'] = \
        (weekday_correction['endo_demand_weekday_median'] -
         weekday_correction['endo_demand_median']) / \
        weekday_correction['endo_demand_median']
    weekday_correction = \
        weekday_correction[['month', 'week_day', 'block_no',
                            'week_day_correction_factor']]
    weekday_correction.sort_values(by=['month', 'week_day', 'block_no'],
                                   ascending=[True, True, True],
                                   inplace=True)

    data_forecast = pd.merge(data_forecast, weekday_correction,
                             how='left',
                             on=['month', 'week_day', 'block_no'])

    data_forecast['weekend_correction_smooth'] = \
        data_forecast['week_day_correction_factor'].\
        rolling(window=7, center=True).mean()

    data_forecast['endo_pred_sim_day_load_final'] = \
        data_forecast['endo_pred_sim_day_load'] + \
        data_forecast['endo_pred_sim_day_load'] * \
        data_forecast['weekend_correction_smooth']

    data_forecast['endo_residual_weekend_correction'] = \
        data_forecast['endo_demand'] - \
        data_forecast['endo_pred_sim_day_load_final']

    data_bias_fest = data_forecast[['date',
                                    'year',
                                    'month',
                                    'block_no',
                                    'endo_demand',
                                    'endo_pred_sim_day_load',
                                    'endo_pred_sim_day_load_final',
                                    'endo_residual_weekend_correction']]

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

    event_date = event_master[['date', 'name']]

    data_bias_fest = pd.merge(data_bias_fest, event_master,
                              on=['date'], how='left')

    data_bias_fest.to_sql(name='{}_data_bias_fest'.format(discom),
                          con=engine,
                          if_exists='replace',
                          flavor='mysql')

    data_bias_fest['endo_residual_weekend_correction_percentage'] = \
        data_bias_fest['endo_residual_weekend_correction'] / \
        data_bias_fest['endo_pred_sim_day_load_final']

    error_pivot_fest = \
        pd.pivot_table(data_bias_fest,
                       values=['endo_residual_weekend_correction_percentage'],
                       index=['block_no', 'name'],
                       aggfunc=np.median).reset_index()
    error_pivot_fest = error_pivot_fest.dropna()

    unique_event = error_pivot_fest['name'].unique()
    event_Bias = pd.DataFrame([])
    for j in xrange(0, len(unique_event)):
        test = error_pivot_fest[error_pivot_fest['name'] == unique_event[j]]
        test = test.dropna()
        poly_coef = np.polyfit(
            test['block_no'],
            test['endo_residual_weekend_correction_percentage'],
            15)
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

    data_forecast = pd.merge(data_forecast, event_master,
                             how='left', on=['date'])

    data_forecast = pd.merge(data_forecast, event_Bias,
                             how='left', left_on=['name', 'block_no'],
                             right_on=['event_name', 'block_no'])

    data_forecast['fest_correction'].fillna(0, inplace=True)

    data_forecast['endo_sim_day_pred_demand_final_fest'] = \
        data_forecast['endo_pred_sim_day_load_final'] + \
        data_forecast['endo_pred_sim_day_load_final'] * \
        data_forecast['fest_correction']

    data_forecast['residual_post_weekend_event'] = \
        data_forecast['endo_demand'] - \
        data_forecast['endo_sim_day_pred_demand_final_fest']

    data_forecast['endo_deterministic_demand_pred_shape'] = \
        data_forecast['endo_sim_day_pred_demand_final_fest']

    data_forecast.sort_values(by=['date', 'block_no'],
                              ascending=[True, True],
                              inplace=True)

    var_list_hourly = [col for col in data_forecast.columns
                       if 'hour' not in col]
    data_forecast = data_forecast[var_list_hourly]
    data_forecast['endo_residual_deterministic'] = \
        data_forecast['endo_demand'] - \
        data_forecast['endo_deterministic_demand_pred_shape']

    data_forecast.to_sql(
        name='data_forecast_{}'.format(discom),
        con=engine,
        if_exists='replace',
        flavor='mysql')

    data_forecast_nn = data_forecast[['date',
                                      'block_no',
                                      'reported_load',
                                      'endo_demand',
                                      'endo_deterministic_demand_pred_shape']]

    data_forecast_nn['NN_PRED_DEMAND'] = \
        data_forecast_nn['endo_deterministic_demand_pred_shape']
    data_forecast_nn.to_sql(name='data_forecast_nn_{}'.format(discom),
                            con=engine,
                            if_exists='replace')

    pred_table_similarday = \
        data_forecast[['date',
                       'block_no',
                       'endo_deterministic_demand_pred_shape']]
    pred_table_similarday.rename(
        columns={
            'endo_deterministic_demand_pred_shape': 'demand_forecast'},
        inplace=True)

    pred_table_similarday['discom'] = discom
    pred_table_similarday['state'] = state
    # pred_table_similarday['state'] = 'GUJARAT'
    pred_table_similarday['revision'] = 0
    pred_table_similarday['model_name'] = 'NEAREST_NEIGHBOUR'
    table_name = 'pred_table_similarday_{}'.format(discom)

    pred_table_similarday.to_sql(name=table_name,
                                 con=engine,
                                 if_exists='replace', flavor='mysql')
    sql_str = """insert into power.forecast_stg
          (date, state, revision, discom, block_no,
           model_name, demand_forecast)
          (select a.date, a.state, a.revision, a.discom, a.block_no,
           a.model_name, round(a.demand_forecast,3) demand_forecast from
           {} a,
           (select max(date) date from power.drawl_staging
            where discom = '{}') b
            where a.date >= b.date)
          on duplicate key
          update demand_forecast = round(values(demand_forecast),3),
                 load_date = NULL""".format(table_name, discom)
    connection = engine.connect()
    connection.execute(sql_str)
    connection.close()
    engine.dispose()
    return


# config = 'mysql+mysqldb://root:power@2012@localhost/power'
# forecast_nn_guvnl(config, 'GUVNL')

"""Forecast Data Preperation."""
from __future__ import division
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
# from numpy import array
# from numpy import sign
# from numpy import zeros
from scipy.interpolate import interp1d
# from scipy import interpolate
from scipy.interpolate import UnivariateSpline
# from pandas import rolling_median
from datetime import datetime, timedelta
import datetime as dt
import pytz
import time
from scipy.signal import medfilt
from scipy import *
from scipy.signal import *
# import numpy as np
from scipy.interpolate import splev, splrep
from scipy.optimize import minimize

pd.options.mode.chained_assignment = None


def data_prep_forecast_nn(config, discom, state):
    """Forecat Nearest Neighbour."""
    # engine = \
    #     create_engine('mysql://root:power@2012@localhost/power', echo=False)
    # state = 'UTTARAKHAND'
    if time.tzname[0] == 'IST':
        max_hour = dt.datetime.today().hour
    else:
        dest_tz = pytz.timezone('Asia/Kolkata')
        ts = time.time()
        utc_now = datetime.utcfromtimestamp(ts)
        local_now = utc_now.replace(tzinfo=pytz.utc).astimezone(dest_tz)
        max_hour = local_now.hour

    max_hour = max(max_hour, 8)

    engine = create_engine(config, echo=False)
    holiday_event_master = pd.read_sql("""select date, event1 as name
                                               from vw_holiday_event_master
                                                where state = '{}'""".
                                             format(state), engine,
                                             index_col=None)
    holiday_event_master['date'] = pd.to_datetime(holiday_event_master['date'])
    weather_data = pd.read_sql("""SELECT b.*, a.latitude, a.longitude
                                        FROM
                                        power.imdaws_wunderground_map a,
                                        power.unified_weather b
                                        where b.location =
                                            a.mapped_location_name
                                        and a.discom = '{}'""".format(discom),
                                     engine, index_col=None)
    weather_data = \
        weather_data.loc[weather_data['data_source'] == 'IBMWEATHERCHANNEL']
    weather_data = \
        weather_data.loc[weather_data['data_type'].isin(['FORECAST',
                                                         'ACTUAL'])]
    weather_data['date'] = pd.to_datetime(weather_data['date'])
    weather_data.rename(columns={'block_hour_no': 'hour',
                                 'temperature': 'temp',
                                 'rainfall_mm': 'RainMM'}, inplace=True)
    weather_data['year'] = pd.DatetimeIndex(weather_data['date']).year
    # jan = 1, dec = 12
    weather_data['month'] = pd.DatetimeIndex(weather_data['date']).month

    powercut_table = \
        pd.read_sql("""select date, block_no,
                          sum(powercut) as powercut
                          from powercut_staging where
                          discom = '{}'
                          group by date, block_no""".format(discom),
                          engine, index_col=None)
    powercut_table['date'] = pd.to_datetime(powercut_table['date'])
    powercut_table.sort_values(by=['date', 'block_no'], ascending=[True, True],
                               inplace=True)
    load_table_initial = \
        pd.read_sql("""select date, block_no, constrained_load
                             from drawl_staging where discom = '{}'""".
                          format(discom), engine, index_col=None)

    load_table_initial['date'] = pd.to_datetime(load_table_initial['date'])

    load_table_initial = pd.merge(
        load_table_initial, powercut_table, how='left', on=[
            'date', 'block_no'])
    load_table_initial['powercut'].fillna(0, inplace=True)
    load_table_initial['reported_load'] = load_table_initial[
        'constrained_load'] + load_table_initial['powercut']
    load_table_initial.sort_values(by=['date', 'block_no'], ascending=[
                                   True, True], inplace=True)

    nn_days = 90
    lag_d = 0
    max_date = np.max(load_table_initial['date'])
    compare_load_table = \
        load_table_initial[(load_table_initial['date'] <= max_date) &
                           (load_table_initial['date'] >= max_date -
                            timedelta(days=nn_days))]
    current_day_load = \
        load_table_initial[load_table_initial['date'] == max_date]
    current_day_load = current_day_load[
        np.isfinite(current_day_load['reported_load'])]
    block_unique = current_day_load['block_no'].unique()
    compare_load_table = compare_load_table[
        compare_load_table['block_no'].isin(block_unique)]

    #obsolete way
    # current_load_summary = \
    #     compare_load_table.groupby(['date'], as_index=False).\
    #     agg({'reported_load': {'median': 'median',
    #                            'max': 'max',
    #                            'min': 'min',
    #                            'max': 'max'}})

    current_load_summary = pd.DataFrame(compare_load_table.groupby(['date'])['reported_load'].\
        agg({'min', 'max', 'median'}).reset_index())
    current_load_summary = current_load_summary.rename(columns={"min": "reported_load_min", 
                                                                "max": "reported_load_max", 
                                                                "median": "reported_load_median"})
    # current_load_summary.columns = [
    #     '_'.join(col).strip() for col in current_load_summary.columns.values]
    # current_load_summary.rename(columns={'date_': 'date'}, inplace=True)

    current_load_summary['reported_load_max_rank'] = current_load_summary[
        'reported_load_max'].rank(ascending=1)
    current_load_summary['reported_load_min_rank'] = current_load_summary[
        'reported_load_min'].rank(ascending=1)
    current_load_summary['reported_load_median_rank'] = current_load_summary[
        'reported_load_median'].rank(ascending=1)

    current_load_all = current_load_summary.copy()
    current_load_relative = current_load_summary.copy()
    current_load_all.sort_values(by=['date'], ascending=[True], inplace=True)
    current_load_relative.sort_values(by=['date'], ascending=[True],
                                      inplace=True)

    unique_date_all = current_load_all['date'].unique()
    dist_matrix_current = pd.DataFrame([])
    for i in range(0, len(unique_date_all)):
        test_all = \
            current_load_all[current_load_all['date'] == unique_date_all[i]]
        var_temp = [col for col in current_load_all.columns
                    if '_rank'in col]
        coordinate_all = np.array(test_all[var_temp])
        test_relative = \
            current_load_relative[(current_load_relative['date'] <
                                   pd.to_datetime(unique_date_all[i]) -
                                   timedelta(days=lag_d)) &
                                  (current_load_relative['date'] >=
                                   pd.to_datetime(unique_date_all[i]) -
                                   timedelta(days=nn_days + lag_d))]
        unique_date_relative = test_relative['date'].unique()
        dist_all = pd.DataFrame([])
        for j in range(0, len(unique_date_relative)):
            test_relative_j = test_relative[
                test_relative['date'] == pd.to_datetime(
                    unique_date_relative[j])]
            coordinate_relative = np.array(test_relative_j[var_temp])
            dist = np.sum((coordinate_all - coordinate_relative)**2, axis=1)
            dist = pd.DataFrame(dist)
            dist.rename(columns={0: 'eucledean_dist'}, inplace=True)
            dist['date'] = unique_date_all[i]
            dist['lag_date'] = unique_date_relative[j]
            dist_all = dist_all.append(dist)
        dist_matrix_current = dist_matrix_current.append(dist_all)
    dist_matrix_current.sort_values(by=['date', 'eucledean_dist'], ascending=[
                                    True, True], inplace=True)

    def ranker(dist_matrix_current):
        dist_matrix_current['rank'] = np.arange(len(dist_matrix_current)) + 1
        return dist_matrix_current

    dist_matrix_current = dist_matrix_current.groupby(
        dist_matrix_current['date']).apply(ranker)
    dist_matrix_current = dist_matrix_current[dist_matrix_current['rank'] <= 7]

    dist_matrix_current = dist_matrix_current[
        dist_matrix_current['date'] == max_date]

    lag_load = pd.merge(dist_matrix_current, load_table_initial,
                        left_on='lag_date', right_on='date')
    lag_load = lag_load.rename(columns={'date_x': 'date',
                                        'reported_load': 'reported_load_lag'})
    del lag_load['date_y']

    lag_load = lag_load[['date', 'lag_date',
                         'rank', 'block_no', 'reported_load_lag']]
    lag_load['rank_no'] = lag_load['rank'].astype(str) + 'rank'

    lag_load_pivot = pd.pivot_table(lag_load, values=['reported_load_lag'],
                                    index=['date', 'block_no'],
                                    columns=['rank_no']).reset_index()
    lag_load_pivot.columns = ['_'.join(col).strip()
                              for col in lag_load_pivot.columns.values]

    lag_load_pivot.rename(columns={'date_': 'date',
                                   'block_no_': 'block_no'}, inplace=True)

    t = 0.45
    w1 = 1
    w2 = t
    w3 = t**2
    w4 = t**3
    w5 = t**4
    w6 = t**5
    w7 = t**6

    lag_load_pivot['imp_load'] = \
        ((lag_load_pivot['reported_load_lag_1rank'] * w1 +
          lag_load_pivot['reported_load_lag_2rank'] * w2 +
          lag_load_pivot['reported_load_lag_3rank'] * w3 +
          lag_load_pivot['reported_load_lag_4rank'] * w4 +
          lag_load_pivot['reported_load_lag_5rank'] * w5 +
          lag_load_pivot['reported_load_lag_6rank'] * w6 +
          lag_load_pivot['reported_load_lag_7rank'] * w7) /
         (w1 + w2 + w3 + w4 + w5 + w6 + w7))

    load_imp = pd.merge(lag_load_pivot, load_table_initial,
                        how='left',
                        on=['date', 'block_no'])

    load_imp = load_imp[['date', 'block_no', 'imp_load']]

    load_table_initial_imp = pd.merge(load_table_initial, load_imp,
                                      how='left',
                                      on=['date', 'block_no'])

    load_table_initial_imp.reported_load.fillna(
        load_table_initial_imp['imp_load'], inplace=True)

    def logit_transform(x):
        pivot = 25
        multipler = 1.5
        beta = 0.4
        delta = 38
        T = np.where((x < pivot), x + (pivot - x) * multipler, x)
        logit_x = (np.exp(beta * (T - delta)) /
                   (1 + np.exp(beta * (T - delta)))) * 100
        return logit_x

    def haversine(lon1, lat1, lon2, lat2):
        from math import radians, cos, sin, asin, sqrt
        """
        Calculate the great circle distance between two points
        on the earth (specified in decimal degrees)
        """
        # convert decimal degrees to radians
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
        # haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
        c = 2 * asin(sqrt(a))
        km = 6367 * c
        return km

    def guess(x, y, k, s, w=None):
        """Do an ordinary spline fit to provide knots."""
        return splrep(x, y, w, k=k, s=s)

    def err(c, x, y, t, k, w=None):
        """The error function to minimize."""
        diff = y - splev(x, (t, c, k))
        if w is None:
            diff = np.einsum('...i,...i', diff, diff)
        else:
            diff = np.dot(diff * diff, w)
        return np.abs(diff)

    def spline_neumann(x, y, k=3, s=0, w=None):
        t, c0, k = guess(x, y, k, s, w=w)
        x0 = x[0]  # point at which zero slope is required
        con = {'type': 'eq',
               'fun': lambda c: splev(x0, (t, c, k), der=1),
               # doesn't help, dunno why
               #  'jac': lambda c: splev(x0, (t, c, k), der=2)
               }
        opt = minimize(err, c0, (x, y, t, k, w), constraints=con)
        copt = opt.x
        return UnivariateSpline._from_tck((t, copt, k))

    def getEnvelopeModels(aTimeSeries, delta, rejectCloserThan=0):
        # Prepend the first value of (s) to the interpolating values.
        # This forces
        # the model to use the same starting point for both the upper and lower
        # envelope models.
        u_x = [0, ]
        u_y = [aTimeSeries[0], ]
        lastPeak = 0

        l_x = [0, ]
        l_y = [aTimeSeries[0], ]
        lastTrough = 0

        # Detect peaks and troughs and mark their location in u_x,u_y,l_x,l_y
        # respectively.
        for k in range(1, len(aTimeSeries) - delta):
            # Mark peaks
            if (np.sign(aTimeSeries[k] - aTimeSeries[k - delta]) in (0, 1)) and\
               (np.sign(aTimeSeries[k] - aTimeSeries[k + delta]) in (0, 1)) and\
                    ((k - lastPeak) > rejectCloserThan):
                u_x.append(k)
                u_y.append(aTimeSeries[k])
                lastPeak = k
            # Mark troughs
            if (np.sign(aTimeSeries[k] - aTimeSeries[k - delta]) in (0, -1)) and\
               ((np.sign(aTimeSeries[k] - aTimeSeries[k + delta])) in
                    (0, -1)) and ((k - lastTrough) > rejectCloserThan):
                l_x.append(k)
                l_y.append(aTimeSeries[k])
                lastTrough = k

        # Append the last value of (s) to the interpolating values. This forces
        # the model to use the same ending point for both the upper and lower
        # envelope models.
        u_x.append(len(aTimeSeries) - 1)
        u_y.append(aTimeSeries[-1])

        l_x.append(len(aTimeSeries) - 1)
        l_y.append(aTimeSeries[-1])

        # Fit suitable models to the data. Here cubic splines.
        u_p = interp1d(u_x, u_y, kind='cubic',
                       bounds_error=False, fill_value=0.0)
        l_p = interp1d(l_x, l_y, kind='cubic',
                       bounds_error=False, fill_value=0.0)
        return (u_p, l_p)

    uniquen_location = weather_data['location'].unique()
    missing_data = pd.DataFrame([])
    for i in range(0, len(uniquen_location)):
        test = weather_data[weather_data['location'] == uniquen_location[i]]
        test1 = pd.DataFrame(test.isnull().sum())
        test1.rename(columns={0: 'missing_count'}, inplace=True)
        test1['location'] = uniquen_location[i]
        test1['count'] = len(test)
        missing_data = missing_data.append(test1)

    missing_count = \
        weather_data[['temp', 'windspeed']].\
        groupby(weather_data['location'], as_index=False).\
        agg(['count', 'size']).reset_index()
    missing_count.columns = [''.join(col).strip()
                             for col in missing_count.columns.values]
    missing_count['max_count'] = np.max(missing_count['tempsize'])
    missing_count['%ge_missing_temp'] = missing_count[
        'tempcount'] / missing_count['max_count']
    missing_count['%ge_windspeedcount'] = missing_count[
        'windspeedcount'] / missing_count['max_count']
    missing_count = missing_count[missing_count['%ge_missing_temp'] > 0.5]
    missing_count.sort_values(
        by=['%ge_missing_temp'],
        ascending=[True],
        inplace=True)
    missing_temp_order = missing_count.location.unique()
    missing_count.sort_values(
        by=['%ge_windspeedcount'],
        ascending=[True],
        inplace=True)
    missing_windspeed_order = missing_count.location.unique()
    unique_location = np.asarray(missing_count['location'].unique())
    # list(unique_location)
    lat_long = weather_data[['location', 'latitude', 'longitude']]
    lat_long = lat_long.drop_duplicates()
    lat_long = lat_long[lat_long['location'].isin(list(unique_location))]

    weather_data = weather_data[
        weather_data['location'].isin(
            list(unique_location))]

    def impute(weather_data):
        """Impute function."""
        weather_data.sort_values(by=['location', 'date', 'hour'],
                                 ascending=[True, True, True],
                                 inplace=True)
        weather_data.sort_values(by=['location', 'date', 'hour'], ascending=[
                                 True, True, True], inplace=True)
        weather_data['lag1_temp'] = weather_data.groupby(['location'])[
            'temp'].shift(1)
        weather_data['lead1_temp'] = weather_data.groupby(['location'])[
            'temp'].shift(-1)

        weather_data.temp.fillna((weather_data['lag1_temp'] +
                                  weather_data['lead1_temp']) / 2,
                                 inplace=True)
        lat_long.sort_values(by=['location'], ascending=[True], inplace=True)
        # haversine(lon1, lat1, lon2, lat2)
        unique_location = lat_long['location'].unique()
        lat_long_dist = pd.DataFrame([])
        for i in range(0, len(unique_location)):
            for j in range(0, len(unique_location)):
                if j != i:
                    test = lat_long[lat_long['location'] == unique_location[i]]
                    test1 = \
                        lat_long[lat_long['location'] == unique_location[j]]
                    lon1 = test['longitude']
                    lat1 = test['latitude']
                    lon2 = test1['longitude']
                    lat2 = test1['latitude']
                    dist_km = haversine(lon1, lat1, lon2, lat2)
                    location_1 = unique_location[i]
                    location_2 = unique_location[j]
                    dist_mat_latlong = [[location_1, location_2, dist_km]]
                    lat_long_dist = lat_long_dist.append(dist_mat_latlong)
        lat_long_dist = lat_long_dist[np.isfinite(lat_long_dist[2])]
        lat_long_dist.rename(columns={0: 'location', 1: 'imp_location',
                                      2: 'distance'},
                             inplace=True)
        Imputed_temp = pd.DataFrame([])
        for i in range(0, len(missing_temp_order)):
            weather_data_temp = weather_data[
                weather_data['location'] == missing_temp_order[i]]
            test = lat_long_dist[
                lat_long_dist['location'] == missing_temp_order[i]]
            test.sort_values(by=['distance'], ascending=[True], inplace=True)
        #     test_imp = pd.DataFrame([])
            impute_temp_order = test['imp_location'].unique()
            for j in range(0, len(impute_temp_order)):
                weather_data_temp_imp = weather_data[
                    weather_data['location'] == impute_temp_order[j]]
                weather_data_temp_imp = weather_data_temp_imp[
                    ['date', 'hour', 'temp']]
                weather_data_temp_imp.rename(
                    columns={'temp': 'NN_temp'}, inplace=True)
                weather_data_temp_test = pd.merge(weather_data_temp,
                                                  weather_data_temp_imp,
                                                  how='left',
                                                  on=['date', 'hour'])
            Imputed_temp = Imputed_temp.append(weather_data_temp_test)

        Imputed_temp['imp_temp_NN'] = np.where(
            (np.isfinite(
                Imputed_temp['temp'])),
            Imputed_temp['temp'],
            Imputed_temp['NN_temp'])
        unique_location = Imputed_temp['location'].unique()
        Imputed_temp_rolling = pd.DataFrame([])
        for i in range(0, len(unique_location)):
            test = Imputed_temp[Imputed_temp['location'] == unique_location[i]]
            test.sort_values(by=['hour', 'date'], ascending=[
                             True, True], inplace=True)
            test['rolling_imp_temp_NN'] = \
                test['imp_temp_NN'].rolling(window=7, center=True).median()
            Imputed_temp_rolling = Imputed_temp_rolling.append(test)

        Imputed_temp_rolling.sort_values(by=['location', 'date', 'hour'],
                                         ascending=[True, True, True],
                                         inplace=True)
        Imputed_temp_rolling['imp_temp_NN'] = \
            np.where((np.isfinite(Imputed_temp_rolling['imp_temp_NN'])),
                     Imputed_temp_rolling['imp_temp_NN'],
                     Imputed_temp_rolling['rolling_imp_temp_NN'])
        temp_data = Imputed_temp_rolling[
            ['location', 'date', 'hour', 'temp', 'imp_temp_NN']]
        temp_data.sort_values(by=['location', 'date', 'hour'],
                              ascending=[True, True, True],
                              inplace=True)
        temp_data.sort_values(by=['location', 'date', 'hour'], ascending=[
                              True, True, True], inplace=True)
        temp_data['lag1_temp'] = temp_data.groupby(
            ['location'])['imp_temp_NN'].shift(1)
        temp_data['lead1_temp'] = temp_data.groupby(
            ['location'])['imp_temp_NN'].shift(-1)

        temp_data.imp_temp_NN.fillna((temp_data['lag1_temp'] +
                                      temp_data['lead1_temp']) / 2,
                                     inplace=True)
        temp_data['lag1_temp'] = temp_data.groupby(
            ['location'])['imp_temp_NN'].shift(1)
        temp_data['temp_curve'] = \
            temp_data['lag1_temp'] / temp_data['imp_temp_NN']
        temp_data['lag1_temp_curve'] = temp_data.groupby(['location'])[
            'temp_curve'].shift(1)
        temp_data['lead1_temp_curve'] = temp_data.groupby(
            ['location'])['temp_curve'].shift(-1)
        temp_data['temp_curve']

        unique_location = temp_data['location'].unique()

        temp_outlier = pd.DataFrame([])
        for i in range(0, len(unique_location)):
            test = temp_data[temp_data['location'] == unique_location[i]]
            unique_date = temp_data['date'].unique()
            test_outlier = pd.DataFrame([])
            for j in range(0, len(unique_date)):
                test1 = test[test['date'] == unique_date[j]]
                threshold = 0.05
                # test1['rolling'] =
                # rolling_median(test1['temp_curve'], window=3, center=True).
                # fillna(method='bfill').fillna(method='ffill')
                test1['rolling'] = test1['temp_curve'].rolling(
                    window=3, center=True).median()
                test1['difference'] = np.abs(
                    test1['temp_curve'] - test1['rolling'])
                test1['outlier_idx'] = \
                    np.where(((test1['difference'] > threshold) &
                             (np.isfinite(test1['temp']))), 1, 0)
                test_outlier = test_outlier.append(test1)
            temp_outlier = temp_outlier.append(test_outlier)

        temp_outlier['imp_temp_curve'] = np.where(
            (temp_outlier['outlier_idx'] == 1),
            temp_outlier['rolling'],
            temp_outlier['temp_curve'])
        while True:
            count1 = temp_outlier['imp_temp_curve'].count()
            temp_outlier['1d_lag_imp_temp_curve'] = \
                temp_outlier.groupby(['location'])['imp_temp_curve'].shift(96)
            temp_outlier['imp_temp_curve'].fillna(
                temp_outlier['1d_lag_imp_temp_curve'], inplace=True)
            if temp_outlier['imp_temp_curve'].count() - count1 == 0:
                break
        unique_location = temp_outlier['location'].unique()
        temp_data_test = pd.DataFrame([])
        for i in range(0, len(unique_location)):
            test = temp_outlier[temp_outlier['location'] == unique_location[i]]
            test['panel_row_id'] = range(1, len(test) + 1, 1)
            temp_data_test = temp_data_test.append(test)
        unique_location = temp_data_test['location'].unique()
        temp_data_final = pd.DataFrame([])
        for i in range(0, len(unique_location)):
            test = \
                temp_data_test[temp_data_test['location'] ==
                               unique_location[i]]
            unique_row = temp_data_test['panel_row_id'].unique()
            c = np.array(test['imp_temp_curve'])
            l = np.array(test['lag1_temp'])
            temp_imp_final = np.zeros(len(unique_row))
            for j in range(1, len(unique_row)):
                if np.isfinite(l[j]):
                    temp_imp_final[j] = min(l[j] / c[j], 44)
                else:
                    temp_imp_final[j] = min(
                        (temp_imp_final[j - 1] / c[j - 1]) / c[j], 44)
            temp_imp_final = pd.DataFrame(temp_imp_final)
            temp_imp_final.rename(columns={0: 'temp_imp_final'}, inplace=True)
            temp_imp_final['panel_row_id'] = range(1, len(unique_row) + 1, 1)
            test = pd.merge(test, temp_imp_final,
                            how='left', on=['panel_row_id'])

            temp_data_final = temp_data_final.append(test)
            temp_data_final['imputed_temp'] = \
                np.where(((np.isnan(temp_data_final['temp'])) |
                          (temp_data_final['outlier_idx'] == 1)),
                         temp_data_final['temp_imp_final'],
                         temp_data_final['temp'])
            temp_data_final['temp_imp'] = \
                np.where((np.isnan(temp_data_final['temp'])),
                         temp_data_final['temp_imp_final'],
                         temp_data_final['temp'])

        weather_data_temp = temp_data_final[
            ['location', 'date', 'hour', 'temp', 'temp_imp']]
        weather_data_temp.sort_values(by=['location', 'date', 'hour'],
                                      ascending=[True, True, True],
                                      inplace=True)
        return weather_data_temp

    if np.max(missing_count['%ge_missing_temp']) < 0.9:
        weather_data_temp = impute(weather_data)
    else:
        weather_data_temp = weather_data.copy()
        weather_data_temp['temp_imp'] = weather_data_temp['temp']
        weather_data_temp = weather_data_temp[
            ['location', 'date', 'hour', 'temp', 'temp_imp']]
    

    weather_data_temp_block = weather_data_temp[weather_data_temp['hour'] <= max_hour]

    #obselute and no longer supported
    # grouped summary with multiple aggregation function        
    # weather_summary = \
    #     weather_data_temp_block.groupby(['date', 'location'],
    #                                     as_index=False).\
    #     agg({'temp_imp': {'max': 'max',
    #                       'min': 'min',
    #                       'median': 'median'}})

    # weather_summary.columns = ['_'.join(col).strip()
    #                            for col in weather_summary.columns.values]
    # weather_summary.rename(
    #     columns={
    #         'date_': 'date',
    #         'location_': 'location'},
    #     inplace=True)

    weather_summary = pd.DataFrame(weather_data_temp_block.groupby(['date', 'location'])['temp_imp'].\
        agg({'min', 'max', 'median'}).reset_index())
    weather_summary = weather_summary.rename(columns={"min": "temp_imp_min", 
                                                      "max": "temp_imp_max", 
                                                      "median": "temp_imp_median"})    

    unique_location = weather_summary['location'].unique()
    weather_summary_ranked = pd.DataFrame([])
    for j in range(0, len(unique_location)):
        test = \
            weather_summary[weather_summary['location'] == unique_location[j]]
        test['temp_imp_max_rank'] = test['temp_imp_max'].rank(ascending=1)
        test['temp_imp_min_rank'] = test['temp_imp_min'].rank(ascending=1)
        test['temp_imp_median_rank'] = \
            test['temp_imp_median'].rank(ascending=1)
        weather_summary_ranked = weather_summary_ranked.append(test)

    weather_summary_pivot = pd.pivot_table(weather_summary_ranked,
                                           values=[
                                               'temp_imp_max_rank',
                                               'temp_imp_min_rank',
                                               'temp_imp_median_rank'
                                           ],
                                           index=['date'],
                                           columns=['location']).reset_index()

    weather_summary_pivot.columns = [
        '_'.join(col).strip() for col in weather_summary_pivot.columns.values]
    weather_summary_pivot.rename(columns={'date_': 'date'}, inplace=True)

    unique_date = load_table_initial_imp['date'].unique()
    smooth_load_curve = pd.DataFrame([])
    for j in range(0, len(unique_date)):
        signal = load_table_initial_imp[
            load_table_initial_imp['date'] == unique_date[j]]
        med_filt = pd.DataFrame(medfilt(signal['reported_load'], 5))
        med_filt = med_filt.rename(columns={0: 'median_filter'})
        med_filt['date'] = unique_date[j]
        med_filt['block_no'] = range(1, len(signal) + 1)
        smooth_load_curve = smooth_load_curve.append(med_filt)
    smooth_load_curve.sort_values(by=['date', 'block_no'], ascending=[
                                  True, True], inplace=True)
    load_curve_filtered = pd.merge(load_table_initial_imp, smooth_load_curve,
                                   how='left',
                                   on=['date', 'block_no'])

    unique_date = load_curve_filtered['date'].unique()
    load_envelop = pd.DataFrame([])
    for j in range(1, len(unique_date)):
        test = \
            load_curve_filtered[load_curve_filtered['date'] == unique_date[j]]
        s = np.array(test['median_filter'])
        P = getEnvelopeModels(s, delta=0, rejectCloserThan=5)
        #  P = getEnvelopeModels(s, delta =1, rejectCloserThan = 0)
        q_u = map(P[0], range(0, len(s)))
        test = test[['date', 'block_no', 'reported_load',
                     'median_filter']].reset_index()
        U_envelop = pd.DataFrame(q_u)
        U_envelop = U_envelop.rename(columns={0: 'U_envelop'})
        envelop = pd.concat([test, U_envelop], axis=1)
        load_envelop = load_envelop.append(envelop)

    load_envelop = \
        load_envelop[['date', 'block_no', 'reported_load', 'U_envelop']]
    load_envelop['deviation'] = load_envelop[
        'reported_load'] - load_envelop['U_envelop']

    load_envelop['deviation_rolling'] = load_envelop[
        'deviation'].rolling(window=5, center=True).mean()

    load_envelop['deviation_rolling'] = \
        np.where((load_envelop['deviation_rolling'] > 0),
                 load_envelop['deviation_rolling'], 0)
    load_envelop['spline_envelop'] = load_envelop[
        'U_envelop'] + load_envelop['deviation_rolling']
    load_envelop['endo_demand'] = load_envelop['spline_envelop']
    load_table = \
        load_envelop[['date', 'block_no', 'reported_load', 'endo_demand']]

    load_table['hour'] = np.ceil(load_table['block_no'] / 4)
    load_table['year'] = pd.DatetimeIndex(load_table['date']).year
    load_table['month'] = pd.DatetimeIndex(
        load_table['date']).month   # jan = 1, dec = 12
    load_table['dayofweek'] = pd.DatetimeIndex(
        load_table['date']).dayofweek  # Monday=0, Sunday=6
    load_table.sort_values(by=['date', 'block_no'], ascending=[
                           True, True], inplace=True)

    load_only_table = \
        load_table[['date', 'block_no', 'endo_demand', 'reported_load']]
    last_date_block = load_only_table[
        load_only_table['date'] == max(
            load_only_table['date'])]
    max_block = max(last_date_block['block_no'])
    columns = ['block_no', 'endo_demand']

    if max_block < 96:
        forecast_period0 = pd.DataFrame(columns=columns)
        forecast_period0['block_no'] = range(max_block + 1, 97)
        forecast_period0['date'] = max(load_only_table['date'])
        forecast_period0 = \
            forecast_period0[['date', 'block_no', 'endo_demand']]
    else:
        forecast_period0 = pd.DataFrame(columns=columns)
        forecast_period0['block_no'] = range(1, 97)
        forecast_period0['date'] = \
            max(load_only_table['date']) + pd.DateOffset(1)
        forecast_period0 = \
            forecast_period0[['date', 'block_no', 'endo_demand']]

    forecast_period = pd.DataFrame([])
    for j in range(1, 8):
        period = pd.DataFrame(columns=columns)
        period['block_no'] = range(1, 97)
        period['date'] = max(forecast_period0['date']) + pd.DateOffset(j)
        period = period[['date', 'block_no', 'endo_demand']]
        forecast_period = forecast_period.append(period)

    forecast_period_date = \
        pd.concat([forecast_period0, forecast_period], axis=0)
    load_only_table = \
        pd.concat([load_only_table, forecast_period_date], axis=0)
    non_missing_Load_date = pd.DataFrame(load_only_table.date.unique())
    non_missing_Load_date.rename(columns={0: 'date'}, inplace=True)
    non_missing_Load_date['date'] = \
        pd.to_datetime(non_missing_Load_date['date'])
    non_missing_Load_date.sort_values(by=['date'], ascending=[False],
                                      inplace=True)
    non_missing_Load_date['date_key'] = range(0, len(non_missing_Load_date))
    date_key = non_missing_Load_date[['date', 'date_key']]
    weather_summary_pivot['date'] = \
        pd.to_datetime(weather_summary_pivot['date'])
    weather_summary_pivot.sort_values(by=['date'],
                                      ascending=[True], inplace=True)
    weather_summary_nonmissing_load = pd.merge(
        non_missing_Load_date,
        weather_summary_pivot,
        how='left',
        on=['date'])

    weather_summary_nonmissing_load = \
        weather_summary_nonmissing_load.sort_values(by=['date'],
                                                    ascending=[False])

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
    event_master = event_master.loc[~event_master['date'].duplicated()]
    event_date = event_master[['date', 'name']]
    event_date['holiday_event'] = 1
    event_calendar = event_date[['date', 'holiday_event']]
    date_key_event = pd.merge(date_key, event_calendar, how='left', on='date')
    date_key_event['dayofweek'] = pd.DatetimeIndex(
        date_key_event['date']).dayofweek  # Monday=0, Sunday=6
    date_key_event['holiday_event'].fillna(0, inplace=True)
    date_key_event['weekend_flag'] = \
        np.where((date_key_event['dayofweek'] == 6) |
                 (date_key_event['dayofweek'] == 5) |
                 (date_key_event['dayofweek'] == 0), 1, 0)
    date_key_event['holiday_flag'] = date_key_event[
        'weekend_flag'] + date_key_event['holiday_event']
    date_key_event = date_key_event[
        ['date', 'date_key', 'holiday_flag', 'weekend_flag']]
    date_key_event['holiday_flag'] = np.where(
        (date_key_event['holiday_flag'] >= 1), 1, 0)
    date_key_event = date_key_event[['date', 'holiday_flag', 'weekend_flag']]
    date_key_event = date_key_event.drop_duplicates()
    event_master = event_master.drop_duplicates(
        subset='date', keep='first', inplace=False)

    weather_all = weather_summary_pivot.copy()
    weather_relative = weather_summary_pivot.copy()
    weather_all.sort_values(by=['date'], ascending=[True], inplace=True)
    weather_relative.sort_values(by=['date'], ascending=[True], inplace=True)

    nn_days = 45
    lag_nn = 12
    lag_d = 0
    unique_date_all = weather_all['date'].unique()
    dist_matrix = pd.DataFrame([])
    for i in range(0, len(unique_date_all)):
        test_all = weather_all[weather_all['date'] == unique_date_all[i]]
        var_temp = [col for col in weather_all.columns
                    if '_rank'in col]
        coordinate_all = np.array(test_all[var_temp])
        test_relative = \
            weather_relative[(weather_relative['date'] <
                              pd.to_datetime(unique_date_all[i]) -
                             timedelta(days=lag_d)) &
                             (weather_relative['date'] >=
                              pd.to_datetime(unique_date_all[i]) -
                              timedelta(days=nn_days + lag_d))]
        unique_date_relative = test_relative['date'].unique()
        dist_all = pd.DataFrame([])
        for j in range(0, len(unique_date_relative)):
            test_relative_j = test_relative[
                test_relative['date'] == pd.to_datetime(
                    unique_date_relative[j])]
            coordinate_relative = np.array(test_relative_j[var_temp])
            dist = np.sum((coordinate_all - coordinate_relative)**2, axis=1)
            dist = pd.DataFrame(dist)
            dist.rename(columns={0: 'eucledean_dist'}, inplace=True)
            dist['date'] = unique_date_all[i]
            dist['lag_date'] = unique_date_relative[j]
            dist_all = dist_all.append(dist)
        dist_matrix = dist_matrix.append(dist_all)
    dist_matrix.sort_values(by=['date', 'eucledean_dist'], ascending=[
                            True, True], inplace=True)

    def ranker(dist_matrix):
        """Ranker function."""
        dist_matrix['rank'] = np.arange(len(dist_matrix)) + 1
        return dist_matrix

    dist_matrix = dist_matrix.groupby(dist_matrix['date']).apply(ranker)
    dist_matrix = dist_matrix[dist_matrix['rank'] <= lag_nn]

    weather_dist_lag_initial = pd.merge(dist_matrix, load_table,
                                        left_on='lag_date', right_on='date')
    weather_dist_lag_initial.rename(columns={'date_x': 'date'}, inplace=True)
    weather_dist_lag_initial = weather_dist_lag_initial.drop('date_y', axis=1)

    # load_summary = \
    #     load_table.groupby(['month', 'block_no'], as_index=False).\
    #     agg({'endo_demand': {'median': 'median'}})
    # load_summary.columns = ['_'.join(col).strip()
    #                         for col in load_summary.columns.values]
    # load_summary.rename(columns={'block_no_': 'block_no',
    #                              'month_': 'month'}, inplace=True)

    load_summary = pd.DataFrame(load_table.groupby(['month', 'block_no'])['endo_demand']\
        .agg({'median'}).reset_index())
    load_summary = load_summary.rename(columns={"median": "endo_demand_median"})      

    # load_summary_weekday = \
    #     load_table.groupby(['month', 'block_no', 'dayofweek'],
    #                        as_index=False).\
    #     agg({'endo_demand': {'weekday_median': 'median'}})
    # load_summary_weekday.columns = [
    #     '_'.join(col).strip() for col in load_summary_weekday.columns.values]
    # load_summary_weekday.rename(columns={'month_': 'month',
    #                                      'block_no_': 'block_no',
    #                                      'dayofweek_': 'dayofweek'},
    #                             inplace=True)

    load_summary_weekday = pd.DataFrame(load_table.groupby(['month', 'block_no', 'dayofweek'])['endo_demand']\
        .agg({'median'}).reset_index())
    load_summary_weekday = load_summary_weekday.rename(columns={"median": "endo_demand_weekday_median"})                                  

    weekday_correction = pd.merge(load_summary_weekday,
                                  load_summary,
                                  how='left',
                                  )

    weekday_correction['week_day_correction_factor'] = \
        (weekday_correction['endo_demand_weekday_median'] -
         weekday_correction['endo_demand_median']) /\
        weekday_correction['endo_demand_median']
    weekday_correction.sort_values(by = ['month','dayofweek','block_no'], 
                              ascending  = [True,True,True],
                              inplace = True)
    unique_month = weekday_correction['month'].unique()

    weekday_correction_envelop = pd.DataFrame([])
    for j in range(0, len(unique_month)):
        test = weekday_correction[weekday_correction['month']==unique_month[j]]
        unique_day = test['dayofweek'].unique()
        for i in range(1,len(unique_day)):
            s = np.array(test['week_day_correction_factor'])
            P = getEnvelopeModels(s, delta = 1, rejectCloserThan = 3)
            q_u = map(P[0],range(0,len(s)))
            q_l = map(P[1],range(0,len(s))) 
            test = test[['month','dayofweek','block_no','week_day_correction_factor']].reset_index()
            U_envelop = pd.DataFrame(q_u)
            U_envelop = U_envelop.rename(columns={0: 'U_envelop'})
            L_envelop = pd.DataFrame(q_l)
            L_envelop = L_envelop.rename(columns={0: 'L_envelop'})
            envelop = pd.concat([test, U_envelop, L_envelop], axis = 1)
            envelop = pd.concat([test, U_envelop, L_envelop], axis = 1)
        weekday_correction_envelop = weekday_correction_envelop.append(envelop)
    weekday_correction_envelop['weekday_envelop_pre'] = weekday_correction_envelop[['U_envelop','L_envelop']].mean(axis=1)    
    
    weekday_correction = weekday_correction_envelop[
        ['month', 'dayofweek', 'block_no', 'weekday_envelop_pre']]
    weekday_correction.sort_values(by=['month', 'dayofweek', 'block_no'],
                                   ascending=[True, True, True],
                                   inplace=True)

    weekday_correction['week_day_correction_factor_pre'] = \
                 weekday_correction['weekday_envelop_pre']
    weather_dist_lag_initial['NN_dayofweek'] = pd.DatetimeIndex(weather_dist_lag_initial['lag_date']).dayofweek

    weather_dist_lag_initial['NN_month'] = pd.DatetimeIndex(
        weather_dist_lag_initial['lag_date']).month

    weather_dist_lag_initial_NNDOW = \
        pd.merge(weather_dist_lag_initial, weekday_correction,
                 left_on=['NN_month', 'NN_dayofweek', 'block_no'],
                 right_on=['month', 'dayofweek', 'block_no'])
    weather_dist_lag_initial_NNDOW['endo_demand_NN_DOW'] = \
        weather_dist_lag_initial_NNDOW['endo_demand'] * \
        (1 - weather_dist_lag_initial_NNDOW['week_day_correction_factor_pre'])

    event_day_load = pd.merge(event_master,
                              load_table,
                              how='left',
                              on=['date']
                              )

    event_day_load = event_day_load[np.isfinite(event_day_load['endo_demand'])]

    # event_day_load_summary = \
    #     event_day_load.groupby(['name', 'block_no', 'month'],
    #                            as_index=False).\
    #     agg({'endo_demand': {'event_day_median': 'median'}})
    # event_day_load_summary.columns = [
    #     '_'.join(col).strip() for col in event_day_load_summary.columns.values]
    # event_day_load_summary.rename(columns={'block_no_': 'block_no',
    #                                        'name_': 'name',
    #                                        'month_': 'month'}, inplace=True)

    event_day_load_summary = pd.DataFrame(event_day_load.groupby(['name', 'block_no', 'month'])['endo_demand'].\
        agg({'median'}).reset_index())
    event_day_load_summary = event_day_load_summary.rename(columns={"median": "endo_demand_event_day_median"})       

    event_correction = pd.merge(event_day_load_summary, load_summary,
                                how='left',
                                on=['month', 'block_no'])
    event_correction['event_day_correction_factor'] = \
        (event_correction['endo_demand_event_day_median'] /
         event_correction['endo_demand_median']) - 1

    # event_correction_summary = \
    #     event_correction.groupby(['name', 'block_no'], as_index=False).\
    #     agg({'event_day_correction_factor': {'median': 'median'}})
    # event_correction_summary.columns = \
    #     ['_'.join(col).strip()
    #      for col in event_correction_summary.columns.values]
    # event_correction_summary.rename(columns={'block_no_': 'block_no',
    #                                          'name_': 'name'}, inplace=True)

    event_correction_summary = pd.DataFrame(event_correction.groupby(['name', 'block_no'])['event_day_correction_factor'].\
        agg({'median'}).reset_index())
    event_correction_summary = event_correction_summary.rename(columns={"median": "event_day_correction_factor_median"})  

    weather_dist_lag_initial_NNDOW_EVENT = \
        pd.merge(weather_dist_lag_initial_NNDOW,
                 event_master,
                 how='left',
                 left_on=['lag_date'],
                 right_on=['date'])
    weather_dist_lag_initial_NNDOW_EVENT.\
        rename(columns={'block_no_': 'block_no',
                        'name_': 'name',
                        'date_x': 'date'}, inplace=True)
    event_correction_summary.sort_values(by = ['name','block_no'], 
                              ascending  = [True,True],
                              inplace = True)

    unique_event = event_correction_summary['name'].unique()

    event_correction_envelop = pd.DataFrame([])
    for j in range(0, len(unique_event)):
        test = event_correction_summary[event_correction_summary['name']==unique_event[j]]
        s = np.array(test['event_day_correction_factor_median'])
        P = getEnvelopeModels(s, delta = 0, rejectCloserThan = 10)
        q_u = map(P[0],range(0,len(s)))
        q_l = map(P[1],range(0,len(s))) 
        test = test[['name','block_no','event_day_correction_factor_median']].reset_index()
        U_envelop = pd.DataFrame(q_u)
        U_envelop = U_envelop.rename(columns={0: 'U_envelop'})
        L_envelop = pd.DataFrame(q_l)
        L_envelop = L_envelop.rename(columns={0: 'L_envelop'})
        envelop = pd.concat([test, U_envelop, L_envelop], axis = 1)
        envelop = pd.concat([test, U_envelop, L_envelop], axis = 1)
        event_correction_envelop = event_correction_envelop.append(envelop)
    event_correction_envelop['event_envelop_pre'] = event_correction_envelop[['U_envelop','L_envelop']].mean(axis=1)

    weather_dist_lag_initial_NNDOW_EVENT = pd.merge(weather_dist_lag_initial_NNDOW,
                                                event_master,
                                               how = 'left',
                                               left_on = ['lag_date'],
                                               right_on=['date'])
    weather_dist_lag_initial_NNDOW_EVENT.rename(columns={'block_no_': 'block_no',
                            'name_':'name',
                             'date_x':'date'}, inplace=True)

    del weather_dist_lag_initial_NNDOW_EVENT['date_y']
    weather_dist_lag_initial_NNDOWEVENT = pd.merge(weather_dist_lag_initial_NNDOW_EVENT,event_correction_envelop,
                                               how = 'left',
                                   on =['name','block_no'])

    weather_dist_lag_initial_NNDOWEVENT['event_envelop_pre'].fillna(0, inplace=True)
    weather_dist_lag_initial_NNDOWEVENT['endo_demand_NN_event'] = weather_dist_lag_initial_NNDOWEVENT['endo_demand']*(1-
                                                   weather_dist_lag_initial_NNDOWEVENT['event_envelop_pre'])


    weather_dist_lag_initial_NNDOWEVENT['endo_demand_NN'] = \
        weather_dist_lag_initial_NNDOWEVENT[
            ['endo_demand_NN_DOW', 'endo_demand_NN_event']].min(axis=1)
    weather_dist_lag_initial_NNDOWEVENT.sort_values(
        by=['date', 'block_no'], ascending=[True, True], inplace=True)

    weather_dist_lag_initial = weather_dist_lag_initial_NNDOWEVENT.copy()
    weather_dist_lag_initial['rank_no'] = weather_dist_lag_initial[
        'rank'].astype(str) + 'lag'
    weather_dist_lag = \
        pd.pivot_table(weather_dist_lag_initial, values=['endo_demand_NN'],
                       index=['date', 'block_no'],
                       columns=['rank_no']).reset_index()
    weather_dist_lag.columns = ['_'.join(col).strip()
                                for col in weather_dist_lag.columns.values]
    weather_dist_lag.rename(
        columns={
            'date_': 'date',
            'block_no_': 'block_no'},
        inplace=True)

    weather_dist_lag = pd.merge(load_only_table, weather_dist_lag,
                                how='left',
                                on=['date', 'block_no'])
    weather_dist_lag['year'] = pd.DatetimeIndex(weather_dist_lag['date']).year
    weather_dist_lag['month'] = pd.DatetimeIndex(
        weather_dist_lag['date']).month   # jan = 1, dec = 12
    weather_dist_lag['dayofweek'] = pd.DatetimeIndex(
        weather_dist_lag['date']).dayofweek  # Monday=0, Sunday=6
    
    weather_dist_lag.rename(columns={'endo_demand_NN_1lag': 'Load_NN1',
                                     'endo_demand_NN_2lag': 'Load_NN2',
                                     'endo_demand_NN_3lag': 'Load_NN3',
                                     'endo_demand_NN_4lag': 'Load_NN4',
                                     'endo_demand_NN_5lag': 'Load_NN5',
                                     'endo_demand_NN_6lag': 'Load_NN6',
                                     'endo_demand_NN_7lag': 'Load_NN7',
                                     'endo_demand_NN_8lag': 'Load_NN8',
                                     'endo_demand_NN_9lag': 'Load_NN9',
                                     'endo_demand_NN_10lag': 'Load_NN10',
                                     'endo_demand_NN_11lag': 'Load_NN11',
                                     'endo_demand_NN_12lag': 'Load_NN12'},
                            inplace=True)
    
    weather_dist_lag.Load_NN7.fillna(weather_dist_lag.Load_NN8, inplace=True)
    weather_dist_lag.Load_NN6.fillna(weather_dist_lag.Load_NN7, inplace=True)
    weather_dist_lag.Load_NN5.fillna(weather_dist_lag.Load_NN6, inplace=True)
    weather_dist_lag.Load_NN4.fillna(weather_dist_lag.Load_NN5, inplace=True)
    weather_dist_lag.Load_NN3.fillna(weather_dist_lag.Load_NN4, inplace=True)
    weather_dist_lag.Load_NN2.fillna(weather_dist_lag.Load_NN3, inplace=True)
    weather_dist_lag.Load_NN1.fillna(weather_dist_lag.Load_NN2, inplace=True)
    t = 0.45
    w1 = 1
    w2 = t
    w3 = t**2
    w4 = t**3
    w5 = t**4
    w6 = t**5
    w7 = t**6

    weather_dist_lag['endo_pred_sim_day_load'] = \
        (weather_dist_lag['Load_NN1'] * w1 +
         weather_dist_lag['Load_NN2'] * w2 +
         weather_dist_lag['Load_NN3'] * w3 +
         weather_dist_lag['Load_NN4'] * w4 +
         weather_dist_lag['Load_NN5'] * w5 +
         weather_dist_lag['Load_NN6'] * w6 +
         weather_dist_lag['Load_NN7'] * w7) /\
        (w1 + w2 + w3 + w4 + w5 + w6 + w7)

    weather_dist_lag['similar_day_load'] = weather_dist_lag["Load_NN1"]

    # NN_PRED_summary = \
    #     weather_dist_lag.groupby(['month', 'block_no'],
    #                              as_index=False).\
    #     agg({'endo_pred_sim_day_load': {'median': 'median'}})
    # NN_PRED_summary.columns = ['_'.join(col).strip()
    #                            for col in NN_PRED_summary.columns.values]
    # NN_PRED_summary.rename(columns={'block_no_': 'block_no',
    #                                 'month_': 'month'}, inplace=True)

    NN_PRED_summary = pd.DataFrame(weather_dist_lag.groupby(['month', 'block_no'])['endo_pred_sim_day_load'].\
        agg({'median'}).reset_index())
    NN_PRED_summary = NN_PRED_summary.rename(columns={"median": "endo_pred_sim_day_load_median"})     

    weather_dist_lag['resdidual_NN'] = weather_dist_lag[
        'endo_demand'] - weather_dist_lag['endo_pred_sim_day_load']

    # residual_summary_weekday = \
    #     weather_dist_lag.groupby(['month', 'block_no', 'dayofweek'],
    #                              as_index=False).\
    #     agg({'resdidual_NN': {'weekday_median': 'median'}})
    # residual_summary_weekday.columns = \
    #     ['_'.join(col).strip()
    #      for col in residual_summary_weekday.columns.values]
    # residual_summary_weekday.rename(columns={'month_': 'month',
    #                                          'block_no_': 'block_no',
    #                                          'dayofweek_': 'dayofweek'},
    #                                 inplace=True)
    # residual_summary_weekday.rename(
    #     columns={'block_no_': 'block_no'},
    #     inplace=True)

    residual_summary_weekday = pd.DataFrame(weather_dist_lag.groupby(['month', 'block_no', 'dayofweek'])['resdidual_NN'].\
        agg({'median'}).reset_index())
    residual_summary_weekday = residual_summary_weekday.rename(columns={"median": "resdidual_NN_weekday_median"})    

    weekday_correction = pd.merge(NN_PRED_summary,
                                  residual_summary_weekday,
                                  how='left',
                                  on=['month', 'block_no']
                                  )

    weekday_correction['week_day_correction_factor'] = \
        (weekday_correction['resdidual_NN_weekday_median'] /
         weekday_correction['endo_pred_sim_day_load_median'])
    weekday_correction = weekday_correction[
        ['month', 'dayofweek', 'block_no', 'week_day_correction_factor']]
    weekday_correction.sort_values(by=['month', 'dayofweek', 'block_no'],
                                   ascending=[True, True, True],
                                   inplace=True)
       
    weekday_correction.sort_values(by = ['month','dayofweek','block_no'], 
                              ascending  = [True,True,True],
                              inplace = True)
    unique_month = weekday_correction['month'].unique()

    weekday_correction_envelop = pd.DataFrame([])
    for j in range(0, len(unique_month)):
        test = weekday_correction[weekday_correction['month']==unique_month[j]]
        unique_day = test['dayofweek'].unique()
        for i in range(1,len(unique_day)):
            s = np.array(test['week_day_correction_factor'])
            P = getEnvelopeModels(s, delta = 1, rejectCloserThan = 3)
            q_u = map(P[0],range(0,len(s)))
            q_l = map(P[1],range(0,len(s))) 
            test = test[['month','dayofweek','block_no','week_day_correction_factor']].reset_index()
            U_envelop = pd.DataFrame(q_u)
            U_envelop = U_envelop.rename(columns={0: 'U_envelop'})
            L_envelop = pd.DataFrame(q_l)
            L_envelop = L_envelop.rename(columns={0: 'L_envelop'})
            envelop = pd.concat([test, U_envelop, L_envelop], axis = 1)
            envelop = pd.concat([test, U_envelop, L_envelop], axis = 1)
        weekday_correction_envelop = weekday_correction_envelop.append(envelop)
    weekday_correction_envelop['weekday_envelop_post'] = weekday_correction_envelop[['U_envelop','L_envelop']].mean(axis=1)

    data_forecast_weekday = pd.merge(weather_dist_lag, weekday_correction_envelop,
                                     how='left',
                                     on=['month', 'dayofweek', 'block_no'])
    data_forecast_weekday['weekend_correction_smooth'] = \
        data_forecast_weekday['weekday_envelop_post']
    data_forecast_weekday['NN_PRED_WEEKDAY_CORRECTED'] = \
        data_forecast_weekday['endo_pred_sim_day_load'] * \
        (1 + data_forecast_weekday['weekend_correction_smooth'])

    event_day_pred = pd.merge(data_forecast_weekday,
                              event_master,
                              how='left',
                              on=['date']
                              )

    event_day_pred['event_residual'] = \
        event_day_pred['endo_demand'] - \
        event_day_pred['NN_PRED_WEEKDAY_CORRECTED']
    event_day_pred['event_residual_factor'] = \
        event_day_pred['event_residual'] /\
        event_day_pred['NN_PRED_WEEKDAY_CORRECTED']

    # event_day_residual_summary = \
    #     event_day_pred.groupby(['name', 'block_no'],
    #                            as_index=False).\
    #     agg({'event_residual_factor': {'median': 'median'}})
    # event_day_residual_summary.columns = \
    #     ['_'.join(col).strip()
    #      for col in event_day_residual_summary.columns.values]
    # event_day_residual_summary.rename(columns={'block_no_': 'block_no',
    #                                            'name_': 'name'}, inplace=True)

    event_day_residual_summary = pd.DataFrame(event_day_pred.groupby(['name', 'block_no'])['event_residual_factor'].\
        agg({'median'}).reset_index())
    event_day_residual_summary = event_day_residual_summary.rename(columns={"median": "event_residual_factor_median"}) 
    
    event_day_residual_summary = event_day_residual_summary[
        np.isfinite(event_day_residual_summary['event_residual_factor_median'])
    ]
    event_day_residual_summary.sort_values(by = ['name','block_no'], 
                              ascending  = [True,True],
                              inplace = True)
    unique_event = event_day_residual_summary['name'].unique()

    event_correction_smooth = pd.DataFrame([])
    for j in range(0, len(unique_event)):
        test = event_day_residual_summary[event_day_residual_summary['name']==unique_event[j]]
        test.sort_values(by = ['block_no'],
                          ascending = [True],
                          inplace = True)
        s = np.array(test['event_residual_factor_median'])
        P = getEnvelopeModels(s, delta = 0, rejectCloserThan = 10)
        q_u = map(P[0],range(0,len(s)))
        q_l = map(P[1],range(0,len(s))) 
        test = test[['name','block_no','event_residual_factor_median']].reset_index()
        U_envelop = pd.DataFrame(q_u)
        U_envelop = U_envelop.rename(columns={0: 'U_envelop'})
        L_envelop = pd.DataFrame(q_l)
        L_envelop = L_envelop.rename(columns={0: 'L_envelop'})
        envelop = pd.concat([test, U_envelop, L_envelop], axis = 1)
        envelop = pd.concat([test, U_envelop, L_envelop], axis = 1)
        event_correction_smooth = event_correction_smooth.append(envelop)
    event_correction_smooth['event_envelop_post'] = event_correction_smooth[['U_envelop','L_envelop']].mean(axis=1)




    event_correction_smooth = event_correction_smooth[
        ['name', 'block_no', 'event_envelop_post']]

    data_forecast_weekday_event = pd.merge(event_day_pred,
                                           event_correction_smooth,
                                           how='left',
                                           on=['name', 'block_no']
                                           )

    data_forecast_weekday_event['event_envelop_post'].\
        fillna(0, inplace=True)

    data_forecast_weekday_event['NN_PRED_WEEKDAY_EVENT_CORRECTED'] = \
        data_forecast_weekday_event['NN_PRED_WEEKDAY_CORRECTED'] * \
        (1 + data_forecast_weekday_event['event_envelop_post'])

    data_forecast_nn = \
        data_forecast_weekday_event[['date',
                                     'block_no',
                                     'reported_load',
                                     'endo_demand',
                                     'NN_PRED_WEEKDAY_EVENT_CORRECTED']]
    data_forecast_nn['NN_PRED_DEMAND'] = \
        data_forecast_nn['NN_PRED_WEEKDAY_EVENT_CORRECTED']
    data_forecast_nn.to_sql(
        name='data_forecast_nn_{}'.format(discom),
        con=engine,
        if_exists='replace')

    pred_table_similarday = \
        data_forecast_weekday_event[['date',
                                     'block_no',
                                     'NN_PRED_WEEKDAY_EVENT_CORRECTED']]
    pred_table_similarday.rename(
        columns={
            'NN_PRED_WEEKDAY_EVENT_CORRECTED': 'demand_forecast'},
        inplace=True)
    pred_table_similarday['discom'] = discom
    pred_table_similarday['state'] = state
    pred_table_similarday['revision'] = 0
    pred_table_similarday['model_name'] = 'NEAREST_NEIGHBOUR'
    table_name = 'pred_table_similarday_{}'.format(discom)
    pred_table_similarday.to_sql(name=table_name, con=engine,
                                 if_exists='replace')
    sql_str = """insert into power.forecast_stg
          (date, state, revision, discom, block_no,
           model_name, demand_forecast)
          (select a.date, a.state, a.revision, a.discom, a.block_no,
           a.model_name, round(a.demand_forecast,3) demand_forecast from
           {} a)
          on duplicate key
          update demand_forecast = round(values(demand_forecast),3)""".format(table_name, discom)
    connection = engine.connect()
    connection.execute(sql_str)
    connection.close()

    data_forecast = data_forecast_weekday_event.copy()

    data_forecast = \
        data_forecast[data_forecast['date'] >=
                      pd.to_datetime(dt.datetime.today().
                      strftime("%m/%d/%Y")) - pd.DateOffset(400)]

    data_forecast['MLP_residual'] = data_forecast['endo_demand'] - \
        data_forecast['NN_PRED_WEEKDAY_EVENT_CORRECTED']
    dist_matrix = dist_matrix[dist_matrix['rank'] <= 7]
    weather_data_temp_lag = pd.merge(dist_matrix, weather_data_temp,
                                     left_on='lag_date', right_on='date')
    weather_data_temp_lag['rank_no'] = weather_data_temp_lag[
        'rank'].astype(str) + 'rank'
    weather_data_temp_lag.rename(columns={'date_x': 'date'}, inplace=True)
    weather_data_temp_lag = weather_data_temp_lag[
        ['date', 'hour', 'location', 'temp_imp', 'rank_no']]

    weather_data_temp_lag_pivot = \
        pd.pivot_table(weather_data_temp_lag, values=['temp_imp'],
                       index=['date', 'hour', 'location'],
                       columns=['rank_no']).reset_index()
    weather_data_temp_lag_pivot.columns = \
        ['_'.join(col).strip()
         for col in weather_data_temp_lag_pivot.columns.values]

    weather_data_temp_lag_pivot.rename(columns={'date_': 'date',
                                                'hour_': 'hour',
                                                'location_': 'location'},
                                       inplace=True)
    t = 0.4
    w1 = 1
    w2 = t
    w3 = t**2
    w4 = t**3
    w5 = t**4
    w6 = t**5
    w7 = t**6

    weather_data_temp_lag_pivot['sim_day_temp_imp'] = \
        (weather_data_temp_lag_pivot['temp_imp_1rank'] * w1 +
         weather_data_temp_lag_pivot['temp_imp_2rank'] * w2 +
         weather_data_temp_lag_pivot['temp_imp_3rank'] * w3 +
         weather_data_temp_lag_pivot['temp_imp_4rank'] * w4 +
         weather_data_temp_lag_pivot['temp_imp_5rank'] * w5 +
         weather_data_temp_lag_pivot['temp_imp_6rank'] * w6 +
         weather_data_temp_lag_pivot['temp_imp_7rank'] * w7) /\
        (w1 + w2 + w3 + w4 + w5 + w6 + w7)

    weather_data_temp_lag_pivot = \
        weather_data_temp_lag_pivot[['date',
                                     'hour',
                                     'location',
                                     'sim_day_temp_imp']]
    weather_data_temp_diff = \
        pd.merge(weather_data_temp, weather_data_temp_lag_pivot,
                 on=['date', 'hour', 'location'])
    weather_data_temp_diff['temp_diff'] = \
        (weather_data_temp_diff['temp_imp'] -
         weather_data_temp_diff['sim_day_temp_imp'])

    temp_diff_location = \
        pd.pivot_table(weather_data_temp_diff, values=['temp_diff'],
                       index=['date', 'hour'],
                       columns=['location']).reset_index()
    temp_diff_location.columns = \
        ['_'.join(col).strip()
         for col in temp_diff_location.columns.values]

    temp_diff_location.rename(columns={'date_': 'date',
                                       'hour_': 'hour'},
                              inplace=True)
    hourly_temp = pd.pivot_table(weather_data_temp, values=['temp_imp'],
                                 index=['date', 'hour'],
                                 columns=['location']).reset_index()
    hourly_temp.columns = ['_'.join(col).strip()
                           for col in hourly_temp.columns.values]

    hourly_temp.rename(columns={'date_': 'date',
                                'hour_': 'hour'}, inplace=True)
    hourly_temp_logit = hourly_temp.copy()

    temp_var = [col for col in hourly_temp_logit.columns
                if 'temp_' in col and '_dev_' not in col]
    for i in range(0, len(temp_var)):
        hourly_temp_logit[str(temp_var[i]) + '_logit'] = \
            logit_transform(hourly_temp_logit[temp_var[i]])
    temp_diff_logit = \
        pd.merge(temp_diff_location, hourly_temp_logit, how='left',
                 on=['date', 'hour'])
    data_forecast['hour'] = np.ceil(data_forecast['block_no'] / 4)
    data_train_test = pd.merge(data_forecast, temp_diff_logit,
                               on=['date', 'hour'])

    data_train_test.to_sql(
        name='data_train_test_{}'.format(discom),
        con=engine,
        if_exists='replace')
    engine.dispose()
    return

# config = 'mysql+mysqldb://root:quenext@2016@104.155.225.29/power'
# data_prep_forecast_nn(config, 'GUVNL', 'GUJARAT')
# config = 'mysql+pymysql://root:power@2020@localhost/power'
# data_prep_forecast_nn(config, 'UPCL', 'UTTARAKHAND')
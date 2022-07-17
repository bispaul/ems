# coding: utf-8

import datetime as dt
import time

from keras.layers import Dense
from keras.layers.core import Activation
from keras.models import Sequential

import numpy as np
# from numpy import array
# from numpy import sign
# from numpy import zeros
import pandas as pd
# from scipy.interpolate import interp1d
# from datetime import timedelta
# from keras.wrappers.scikit_learn import KerasRegressor
# from keras.layers.core import TimeDistributedDense, Activation, Dropout
# from keras.layers.recurrent import GRU
from sklearn import preprocessing as pp
# from sklearn.model_selection import cross_val_score
# from sklearn.model_selection import KFold
# from sklearn.preprocessing import StandardScaler
# from sklearn.preprocessing import MinMaxScaler
# from sklearn.pipeline import Pipeline
from sklearn.preprocessing import Imputer
# import statsmodels.api as sm
# from keras.models import load_model
# from math import factorial
from sqlalchemy import create_engine

pd.options.mode.chained_assignment = None

# def savitzky_golay(y, window_size, order, deriv=0, rate=1):
#     try:
#         window_size = np.abs(np.int(window_size))
#         order = np.abs(np.int(order))
#     except ValueError, msg:
#         raise ValueError("window_size and order have to be of type int")
#     if window_size % 2 != 1 or window_size < 1:
#         raise TypeError("window_size size must be a positive odd number")
#     if window_size < order + 2:
#         raise TypeError("window_size is too small for the polynomials order")
#     order_range = range(order+1)
#     half_window = (window_size -1) // 2
#     # precompute coefficients
#     b = np.mat([[k**i for i in order_range] for k in range(-half_window, half_window+1)])
#     m = np.linalg.pinv(b).A[deriv] * rate**deriv * factorial(deriv)
#     # pad the signal at the extremes with
#     # values taken from the signal itself
#     firstvals = y[0] - np.abs( y[1:half_window+1][::-1] - y[0] )
#     lastvals = y[-1] + np.abs(y[-half_window-1:-1][::-1] - y[-1])
#     y = np.concatenate((firstvals, y, lastvals))
#     return np.convolve( m[::-1], y, mode='valid')


def forecast_dln_training(config):
    # engine = create_engine('mysql://root:power@2012@localhost/power',
    # echo=False)
    engine = create_engine(config, echo=False)
    data_forecast = pd.read_sql_query('select * from data_forecast_UPCL',
                                      engine, index_col=None)
    data_forecast['date'] = pd.to_datetime(data_forecast['date'])
    data_forecast.endo_demand.fillna(data_forecast.pred_KNN_smooth,
                                     inplace=True)
    data_forecast = data_forecast[
        data_forecast['date'] >= pd.to_datetime(
            dt.datetime.today().strftime("%m/%d/%Y")) -
        pd.DateOffset(400)]
    daily_weather_mat_logit = pd.read_sql_query(
        'select * from daily_weather_mat_logit_upcl',
        engine, index_col=None)
    daily_weather_mat_logit['date'] = \
        pd.to_datetime(daily_weather_mat_logit['date'])

    # data_forecast['MLP_residual'] = \
    #    data_forecast['endo_demand'] - data_forecast['Load_NN1']
    data_forecast['MLP_residual'] = \
        data_forecast['endo_demand'] - data_forecast['pred_KNN_smooth']

    # data_forecast.dropna()
    # list(daily_weather_mat_logit)

    data_forecast = pd.merge(data_forecast,
                             daily_weather_mat_logit,
                             how='left',
                             on=['date'])

    hour_dummies = pd.get_dummies(data_forecast['hour'])
    hour_dummies.columns = ['hour_' + str(col) for col in hour_dummies.columns]
    month_dummies = pd.get_dummies(data_forecast['month'])
    month_dummies.columns = ['month_' + str(col) for col
                             in month_dummies.columns]
    week_day_dummies = pd.get_dummies(data_forecast['week_day'])
    week_day_dummies.columns = ['week_day_' + str(col) for col
                                in week_day_dummies.columns]

    event_day_dummies = pd.get_dummies(data_forecast['event_name'])
    event_day_dummies.columns = ['event_name_' + str(col) for col
                                 in event_day_dummies.columns]

    data_forecast = pd.concat([data_forecast,
                               hour_dummies,
                               month_dummies,
                               week_day_dummies,
                               event_day_dummies], axis=1)

    data_forecast_train = data_forecast[
        data_forecast['date'] < pd.to_datetime(
            dt.datetime.today().strftime("%m/%d/%Y")) -
        pd.DateOffset(2)]

    exog_var = [col for col in data_forecast_train
                if '_logit' in col and '_diff' not in col or 'Load_NN'in col]

    exog_cat = [col for col in data_forecast_train
                if 'hour_' in col or 'month_' in col or 'week_day_' in col or
                'event_name_'in col]

    endo_var = [col for col in data_forecast_train if 'endo_demand' in col]

    Analytics_exog_cont_train = data_forecast_train[exog_var]
    Analytics_exog_cat_train = data_forecast_train[exog_cat]
    Analytics_endo_train = data_forecast_train[endo_var]

    # # Generate training sample for exogenous and endogeneous variable
    x_train = \
        Analytics_exog_cont_train.ix[:, :Analytics_exog_cont_train.shape[1]]
    imp = Imputer(missing_values='NaN',
                  strategy='median',
                  axis=0)
    imp.fit(x_train)
    x_imp = imp.transform(x_train)

    scaler = pp.StandardScaler().fit(x_imp)
    st_x_train = scaler.transform(x_imp)

    x_cat_train = \
        Analytics_exog_cat_train.ix[:, :Analytics_exog_cat_train.shape[1]]

    stx_x_train = np.concatenate((st_x_train, x_cat_train), axis=1)
    y_train = Analytics_endo_train.ix[:, :Analytics_endo_train.shape[1]]
    # y_imp = imp.transform(x_train)
    train_y_scaler = pp.StandardScaler().fit(y_train)
    st_y_train = train_y_scaler.transform(y_train)

    X = stx_x_train
    Y = st_y_train
    nrows, ncols = X.shape
    seed = 7
    np.random.seed(seed)
    # Finalized layers, 231, 100, 50, 45, 40,35,25,15,10)

    model = Sequential()
    model.add(Dense(ncols, input_dim=ncols, init='normal', activation='tanh'))
    model.add(Dense(100))
    model.add(Activation('relu'))
    model.add(Dense(50))
    model.add(Activation('relu'))
    model.add(Dense(45))
    model.add(Activation('relu'))
    model.add(Dense(40))
    model.add(Activation('relu'))
    model.add(Dense(35))
    model.add(Activation('relu'))
    model.add(Dense(25))
    model.add(Activation('relu'))
    model.add(Dense(15))
    model.add(Activation('relu'))
    model.add(Dense(10))
    model.add(Activation('relu'))
    model.add(Dense(1))
    model.compile(loss='mean_absolute_error', optimizer='adam')

    model.fit(X, Y, nb_epoch=5, batch_size=10, verbose=0)
    time.sleep(0.1)
    # score = model.evaluate(X, Y, batch_size=10)
    # print score
    model.save('keras_model.h5')  # creates a HDF5 file 'my_model.h5'
    del model  # deletes the existing model
    engine.dispose()
    return

# forecast_dln_training('mysql://root:power@2012@localhost/power')

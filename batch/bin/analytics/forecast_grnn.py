# import statsmodels.datasets as datasets
# import sklearn.metrics as metrics
# from numpy import log
# from pyearth import Earth as earth
# from matplotlib import pyplot
# from matplotlib import pyplot as plt
import statsmodels.api as sm
from sqlalchemy import create_engine
import pandas as pd
# from sklearn import datasets
# from sklearn.cross_validation import train_test_split
# import multiprocessing
# from multiprocessing import pool
# from neupy import algorithms, estimators, environment
from neupy import environment
# from sklearn import preprocessing
from sklearn import preprocessing as pp
# from sklearn import cross_validation as cv
from neupy.algorithms import GRNN as grnn
import datetime as dt
import numpy as np
import time
import pytz
# from neupy.functions import mse
# from neupy.estimators import rmsle
# from neupy.estimators import mse
# from sklearn import preprocessing as pp


def forecast_grnn(config):
    # engine = create_engine('mysql://root:power@2012@localhost/power',
    #                         echo=False)
    environment.reproducible()
    engine = create_engine(config, echo=False)
    data_forecast = pd.read_sql_query(
        'select * from data_forecast_UPCL',
        engine,
        index_col=None)
    data_forecast['date'] = pd.to_datetime(data_forecast['date'])
    data_forecast = data_forecast[data_forecast['date'] >= '2014-12-31']

    # In[4]:

    # data_forecast = data_forecast.dropna()
    Analytics_exog_cont = data_forecast[data_forecast.columns[
        data_forecast.columns.to_series().str.contains('exog_cont')]]
    Analytics_endo = data_forecast[data_forecast.columns[
        data_forecast.columns.to_series().str.contains('endo')]]

    # Generate exogenour vaiable all observation
    x = Analytics_exog_cont.ix[:, :Analytics_exog_cont.shape[1]]
    scaler = pp.StandardScaler().fit(x)
    st_x = scaler.transform(x)
    xlabel = list(x.columns)
    st_x.mean(axis=0)
    st_x.std(axis=0)
    st_x = sm.add_constant(st_x)

    y = Analytics_endo.ix[:, 8:Analytics_endo.shape[1] + 1]
    y = y.dropna()
    ylabel = list(y.columns)
    scaler_y = pp.StandardScaler().fit(y)
    st_y = scaler_y.transform(y)
    st_y.mean(axis=0)
    st_y.std(axis=0)

    # In[5]:
    if time.tzname[0] == 'IST':
        local_now = dt.datetime.today().strftime("%m/%d/%Y")
    else:
        dest_tz = pytz.timezone('Asia/Kolkata')
        ts = time.time()
        utc_now = dt.datetime.utcfromtimestamp(ts)
        local_now = utc_now.replace(tzinfo=pytz.utc).astimezone(dest_tz)\
            .strftime("%m/%d/%Y")
    data_forecast_train = data_forecast[data_forecast['date'] <
                                        pd.to_datetime(local_now) -
                                        pd.DateOffset(22)]
    data_forecast_test = data_forecast[data_forecast['date'] >=
                                       pd.to_datetime(local_now) -
                                       pd.DateOffset(22)]

    data_forecast_test = data_forecast_test.dropna()

    Analytics_exog_cont_train = data_forecast_train[
        data_forecast_train.columns[
            data_forecast_train.columns.to_series().str.contains('exog_cont')]]
    Analytics_endo_train = data_forecast_train[data_forecast_train.columns[
        data_forecast_train.columns.to_series().str.contains('endo')]]

    Analytics_exog_cont_test = data_forecast_test[data_forecast_test.columns[
        data_forecast_test.columns.to_series().str.contains('exog_cont')]]
    Analytics_endo_test = data_forecast_test[data_forecast_test.columns[
        data_forecast_test.columns.to_series().str.contains('endo')]]

    # Generate training sample for exogenous and endogeneous variable
    x_train = Analytics_exog_cont_train.ix[:,
                                           :Analytics_exog_cont_train.shape[1]]
    x_trainlabel = list(x_train.columns)

    train_x_scaler = pp.StandardScaler().fit(x_train)

    st_x_train = train_x_scaler.transform(x_train)
    st_x_train.mean(axis=0)
    st_x_train.std(axis=0)
    st_x_train = sm.add_constant(st_x_train)

    y_train = Analytics_endo_train.ix[:, 8:Analytics_endo_train.shape[1] + 1]
    train_y_scaler = pp.StandardScaler().fit(y_train)
    # y_trainlabel = list(y_train.columns)
    st_y_train = train_y_scaler.transform(y_train)
    st_y_train.mean(axis=0)
    st_y_train.std(axis=0)

    # Generate test sample for exogenous and endogeneous variable
    x_test = Analytics_exog_cont_test.ix[:, :Analytics_exog_cont_test.shape[1]]
    # x_testlabel = list(x_test.columns)
    st_x_test = train_x_scaler.transform(x_test)
    st_x_test.mean(axis=0)
    st_x_test.std(axis=0)
    st_x_test = sm.add_constant(st_x_test)

    y_test = Analytics_endo_test.ix[:, 8:Analytics_endo_test.shape[1] + 1]
    # y_testlabel = list(y_test.columns)
    st_y_test = train_y_scaler.transform(y_test)
    st_y_test.mean(axis=0)
    st_y_test.std(axis=0)

    # In[6]:

    my_grnn_UPCL = grnn(std=5.5, verbose=False)
    my_grnn_UPCL.train(st_x_train, y_train)
    y_pred = my_grnn_UPCL.predict(st_x)

    # In[7]:

    pred_table = data_forecast[['date',
                                'block_no',
                                'year',
                                'month',
                                'demand',
                                'endo_demand',
                                'endo_residual_deterministic',
                                'pred_KNN_smooth',
                                'endo_residual']]
    pred_table['pred_table_key'] = range(0, len(pred_table))

    y_pred = pd.DataFrame(y_pred)
    y_pred['y_pred_key'] = range(0, len(y_pred))

    pred_table = pred_table.merge(y_pred, left_on='pred_table_key',
                                  right_on='y_pred_key', how='outer')

    pred_table.rename(columns={0: 'resid_hat_grnn'}, inplace=True)
    pred_table['demand_pred_grnn'] = \
        pred_table['pred_KNN_smooth'] + pred_table['resid_hat_grnn']
    pred_table.sort(['date', 'block_no'], ascending=[True, True], inplace=True)

    unique_date = pred_table['date'].unique()

    pred_table_test = pd.DataFrame([])
    for j in xrange(0, len(unique_date)):
        test = pred_table[pred_table['date'] == unique_date[j]]
        pred_grnn_poly_coef = np.polyfit(test['block_no'],
                                         test['demand_pred_grnn'], 15)
        pred_grnn_poly_coef_poly = np.poly1d(pred_grnn_poly_coef)
        pred_grnn_smooth = pred_grnn_poly_coef_poly(test['block_no'])

        test = test[['date', 'block_no', 'year', 'month', 'demand',
                     'endo_demand', 'demand_pred_grnn']].reset_index()
        pred_grnn_smooth = pd.DataFrame(pred_grnn_smooth)
        pred_grnn_smooth = pred_grnn_smooth\
            .rename(columns={0: 'pred_grnn_smooth'})
        pred_grnn_smooth = pd.concat([test, pred_grnn_smooth], axis=1)
        pred_table_test = pred_table_test.append(pred_grnn_smooth)

    pred_table_final = pred_table_test

    # In[18]:

    pred_table_GRNN = pred_table_final[['date', 'block_no',
                                        'pred_grnn_smooth']]
    pred_table_GRNN.rename(columns={'pred_grnn_smooth': 'demand_forecast'},
                           inplace=True)
    pred_table_GRNN['discom'] = 'UPCL'
    pred_table_GRNN['state'] = 'UTTARAKHAND'
    pred_table_GRNN['revision'] = 0
    pred_table_GRNN['model_name'] = 'GRNN'

    pred_table_GRNN.to_sql(con=engine, name='pred_table_GRNN_UPCL',
                           if_exists='replace', flavor='mysql', index=False)

    sql_str = """insert into power.forecast_stg
            (date, state, revision, discom, block_no,
             model_name, demand_forecast)
            (select a.date, state, revision, discom, block_no,
             model_name, demand_forecast from
             %s a,
            (select max(date) date from power.drawl_staging
              where discom = 'UPCL') b
            where a.date > b.date)
            on duplicate key
            update demand_forecast = values(demand_forecast),
                   load_date = NULL""" % 'power.pred_table_GRNN_UPCL'
    connection = engine.connect()
    connection.execute(sql_str)
    connection.close()
    return

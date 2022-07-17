# coding: utf-8
# import statsmodels.datasets as datasets
# import sklearn.metrics as metrics
# from numpy import log
# import statsmodels.api as sm
from sqlalchemy import create_engine
import pandas as pd
import datetime as dt
import numpy as np
# import multiprocessing
# from multiprocessing import pool
# from neupy import algorithms
# from neupy import estimators
# from neupy import environment
from sklearn import preprocessing as pp
# from sklearn import cross_validation as cv
# import numpy as np
# from sklearn.metrics import mean_squared_error
# import optunity
# import optunity.metrics
# import sklearn.svm
# from __future__ import division
# import time
from sklearn.svm import SVR
# from sklearn.grid_search import GridSearchCV
# from sklearn.learning_curve import learning_curve
# from sklearn.kernel_ridge import KernelRidge
# import matplotlib.pyplot as plt
# import statsmodels.api as sm


def forecast_svr(config):
    # engine = create_engine('mysql://root:power@2012@localhost/power',
    #                         echo=False)
    engine = create_engine(config, echo=False)
    data_forecast = pd.read_sql_query(
        'select * from data_forecast_UPCL',
        engine,
        index_col=None)
    data_forecast['date'] = pd.to_datetime(data_forecast['date'])
    data_forecast = data_forecast[data_forecast['date'] >= '2014-12-31']
    data_forecast['constant'] = 1
    data_forecast.endo_demand.fillna(data_forecast.pred_KNN_smooth,
                                     inplace=True)

    Analytics_exog_cont = data_forecast[data_forecast.columns[
        data_forecast.columns. to_series()
        .str.contains('exog_cont_temp|constant')]]
    Analytics_endo = data_forecast[data_forecast.columns
                                   [data_forecast.columns.
                                    to_series().str.contains('endo')]]
    # Generate exogenour vaiable all observation
    x = Analytics_exog_cont.ix[:, :Analytics_exog_cont.shape[1]]
    scaler = pp.StandardScaler().fit(x)
    st_x = scaler.transform(x)
    # xlabel = list(x.columns)

    # st_x = sm.add_constant(st_x)
    # x_cat = Analytics_exog_cat.ix[:, :Analytics_exog_cat.shape[1]]
    # st_x = np.concatenate((st_xcont, x_cat), axis=1)
    y = Analytics_endo.ix[:, 8:Analytics_endo.shape[1] + 1]
    y = y.dropna()
    # ylabel = list(y.columns)
    scaler_y = pp.StandardScaler().fit(y)
    st_y = scaler_y.transform(y)
    st_y = st_y

    data_forecast_train = data_forecast[
        data_forecast['date'] < pd.to_datetime(
            dt.datetime.today(). strftime("%m/%d/%Y")) -
        pd.DateOffset(2)]
    data_forecast_test = data_forecast[
        data_forecast['date'] >= pd.to_datetime(
            dt.datetime.today().strftime("%m/%d/%Y")) -
        pd.DateOffset(2)]

    data_forecast_test = data_forecast_test.dropna()

    Analytics_exog_cont_train = \
        data_forecast_train[data_forecast_train
                            .columns[data_forecast_train
                                     .columns
                                     .to_series()
                                     .str
                                     .contains('exog_cont_temp|constant')]]
    Analytics_endo_train = \
        data_forecast_train[data_forecast_train
                            .columns[data_forecast_train
                                     .columns
                                     .to_series()
                                     .str
                                     .contains('endo')]]
    # Analytics_exog_cont_test = \
    #     data_forecast_test[data_forecast_test
    #                        .columns[data_forecast_test
    #                                 .columns
    #                                 .to_series()
    #                                 .str
    #                                 .contains('exog_cont_temp|constant')]]
    # Analytics_endo_test = \
    #     data_forecast_test[data_forecast_test
    #                        .columns[data_forecast_test
    #                                 .columns
    #                                 .to_series()
    #                                 .str
    #                                 .contains('endo')]]

    # Generate training sample for exogenous and endogeneous variable
    x_train = Analytics_exog_cont_train.ix[:,
                                           :Analytics_exog_cont_train.shape[1]]
    # x_trainlabel = list(x_train.columns)

    train_x_scaler = pp.StandardScaler().fit(x_train)

    st_x_train = train_x_scaler.transform(x_train)
    # st_x_train = sm.add_constant(st_x_train)

    y_train = Analytics_endo_train.ix[:, 8:Analytics_endo_train.shape[1] + 1]
    train_y_scaler = pp.StandardScaler().fit(y_train)
    # y_trainlabel = list(y_train.columns)
    st_y_train = train_y_scaler.transform(y_train)

    st_y_train = st_y_train
    # # Generate test sample for exogenous and endogeneous variable
    # x_test = Analytics_exog_cont_test.ix[:,
    #                                      :Analytics_exog_cont_test.shape[1]]
    # x_testlabel = list(x_test.columns)
    # st_x_test = train_x_scaler.transform(x_test)
    # st_x_test = sm.add_constant(st_x_test)

    # y_test = Analytics_endo_test.ix[:, 8:Analytics_endo_test.shape[1] + 1]
    # y_testlabel = list(y_test.columns)
    # st_y_test = train_y_scaler.transform(y_test)
    # st_y_test = st_y_test

    # In[36]:

    # # Fit SV Rgression

    # # train_size = 6880
    # svr = GridSearchCV(SVR(kernel='rbf', gamma=0.1), cv=3,refit = True,
    #                        n_jobs=-1,
    #                    param_grid={"C": [1e0, 1e1],
    #                                "kernel":['linear','rbf','poly'],
    #                                "degree":[2,3],
    #                                "gamma": np.logspace(-2, 2, 5)})
    # t0 = time.time()
    # svr.fit(st_x_train, st_y_train.ravel())
    # svr_fit = time.time() - t0
    # print("SVR complexity and bandwidth selected and model fitted in %.3f s"
    #       % svr_fit)
    # svr.get_params([x])

    # In[37]:

    # train_size = len(x_train)
    # sv_ratio = svr.best_estimator_.support_.shape[0] / train_size
    # print("Support vector ratio: %.3f" % sv_ratio)
    # t0 = time.time()
    # """
    # decision_function(X)
    # get_params([deep])
    # inverse_transform(Xt)
    # predict(X)
    # predict_log_proba(X)
    # predict_proba(X)
    # score(X[, y])
    # set_params(**params)
    # transform(X)
    # """
    # svr.get_params([x])

    # In[38]:

    # sample_weight
    svr = SVR(
        C=1.0,
        cache_size=200,
        coef0=0.0,
        degree=3,
        epsilon=0.1,
        gamma=0.1,
        kernel='rbf',
        max_iter=-1,
        shrinking=True,
        tol=0.001,
        verbose=False)

    # In[39]:

    # t0 = time.time()
    yhat = svr.fit(st_x_train, st_y_train.ravel()).predict(st_x)
    # SVR_fit = time.time() - t0
    # print SVR_fit

    # In[40]:

    MuY = np.array(y_train.mean(axis=0))
    stdY = np.array(y_train.std(axis=0))
    y_pred_SVR = yhat * stdY + MuY

    # In[41]:

    pred_table = data_forecast[['date',
                                'block_no',
                                'year',
                                'month',
                                'demand',
                                'endo_demand',
                                'endo_mod_demand',
                                'pred_KNN_smooth',
                                'endo_residual_deterministic']]
    pred_table['pred_table_key'] = range(0, len(pred_table))

    y_pred_SVR = pd.DataFrame(y_pred_SVR)

    y_pred_SVR['y_pred_key'] = range(0, len(y_pred_SVR))

    y_pred_SVR.rename(columns={0: 'svr_residual_pred'}, inplace=True)
    pred_table = pred_table.merge(
        y_pred_SVR,
        left_on='pred_table_key',
        right_on='y_pred_key',
        how='outer')
    pred_table['SVR_PRED_DEMAND'] = pred_table[
        'pred_KNN_smooth'] + pred_table['svr_residual_pred']
    pred_table_final = pred_table
    pred_table_final.to_sql(
        name='pred_table_final_SVR_UPCL',
        con=engine,
        if_exists='replace',
        flavor='mysql')
    pred_table['mape'] = abs(pred_table['endo_demand'] -
                             pred_table['SVR_PRED_DEMAND']) / \
        pred_table['endo_demand']
    pred_table['mape1'] = abs(pred_table['demand'] -
                              pred_table['SVR_PRED_DEMAND']) / \
        pred_table['demand']

    data_forecast_SVR = pred_table[['date', 'block_no', 'SVR_PRED_DEMAND']]
    data_forecast_SVR.to_sql(name='data_forecast_SVR',
                             con=engine,
                             if_exists='replace',
                             flavor='mysql')

    pred_table_SVR = pred_table[['date', 'block_no', 'SVR_PRED_DEMAND']]
    pred_table_SVR.rename(
        columns={
            'SVR_PRED_DEMAND': 'demand_forecast'},
        inplace=True)
    pred_table_SVR['discom'] = 'UPCL'
    pred_table_SVR['state'] = 'UTTARAKHAND'
    pred_table_SVR['revision'] = 0
    pred_table_SVR['model_name'] = 'SVR'

    pred_table_SVR.to_sql(
        con=engine,
        name='pred_table_SVR_UPCL',
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
                 load_date = NULL""".format('pred_table_SVR_UPCL')

    connection = engine.connect()
    connection.execute(sql_str)
    connection.close()
    engine.dispose()
    return

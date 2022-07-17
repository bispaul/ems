# coding: utf-8
# import sklearn.metrics as metrics
import statsmodels.api as sm
from sqlalchemy import create_engine
import pandas as pd
import datetime as dt
# import numpy as np
from sklearn import preprocessing as pp
from sklearn.svm import SVR
from sklearn.externals import joblib
from sklearn.preprocessing import Imputer


def forecast_svr_training(config):
    # engine = create_engine('mysql://root:power@2012@localhost/power',
    #                        echo=False)
    engine = create_engine(config, echo=False)
    data_forecast = pd.read_sql_query(
        'select * from data_forecast_UPCL', engine, index_col=None)
    data_forecast['date'] = pd.to_datetime(data_forecast['date'])
    data_forecast = data_forecast[
        data_forecast['date'] >= pd.to_datetime(
            dt.datetime.today().strftime("%m/%d/%Y")
        ) - pd.DateOffset(365)]
    data_forecast.endo_demand.fillna(
        data_forecast.pred_KNN_smooth, inplace=True)

    data_forecast['MLP_residual'] = data_forecast['endo_demand'] - data_forecast['pred_KNN_smooth']
    # In[3]:

    data_forecast_train = data_forecast[
        data_forecast['date'] < pd.to_datetime(
            dt.datetime.today().strftime("%m/%d/%Y")
        ) - pd.DateOffset(2)]

    exog_var = [col for col in data_forecast if '_logit_diff' in col 
            or 'exog_cont_windspeed' in col]
    endo_var = [col for col in data_forecast if 'MLP_residual' in col]

    Analytics_exog_cont_train = data_forecast_train[exog_var]
    Analytics_endo_train = data_forecast_train[endo_var]
    # Generate training sample for exogenous and endogeneous variable
    x_train = Analytics_exog_cont_train.ix[:,
                                           :Analytics_exog_cont_train.shape[1]]
    imp = Imputer(missing_values='NaN', strategy='median', axis=0)
    imp.fit(x_train)
    x_train_imp = imp.transform(x_train)

    scaler = pp.StandardScaler().fit(x_train_imp)
    st_x_train = scaler.transform(x_train_imp)
#    st_x_train = sm.add_constant(st_x_train, has_constant='add')
    y_train = Analytics_endo_train.ix[:, :Analytics_endo_train.shape[1]]
    train_y_scaler = pp.StandardScaler().fit(y_train)
    st_y_train = train_y_scaler.transform(y_train)
    # In[4]:

    SVR_UPCL = SVR(C=1.0, cache_size=200, coef0=0.0, degree=3, epsilon=0.1,
                   gamma=0.1, kernel='rbf', max_iter=-1, shrinking=True,
                   tol=0.001, verbose=False)

    SVR_UPCL = SVR_UPCL.fit(st_x_train, st_y_train.ravel())
    SVR_UPCL = joblib.dump(SVR_UPCL, 'SVR_UPCL.pkl')
    engine.dispose()
    return

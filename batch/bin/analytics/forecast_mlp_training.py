# coding: utf-8
from sqlalchemy import create_engine
import pandas as pd
# import numpy as np
# import statsmodels.api as sm
import datetime as dt
from sklearn import preprocessing as pp
from sknn.mlp import Regressor
from sknn.mlp import Layer
from sklearn.externals import joblib
from sklearn.preprocessing import Imputer


def forecast_mlp_training(config):
    # engine = create_engine('mysql://root:power@2012@localhost/power',
    #                         echo=False)
    engine = create_engine(config, echo=False)
    data_forecast = pd.read_sql_query(
        'select * from data_forecast_UPCL', engine, index_col=None)
    data_forecast['date'] = pd.to_datetime(data_forecast['date'])
    data_forecast.endo_demand.fillna(
        data_forecast.pred_KNN_smooth, inplace=True)
    data_forecast = data_forecast[
        data_forecast['date'] >= pd.to_datetime(
            dt.datetime.today().strftime("%m/%d/%Y")
        ) - pd.DateOffset(365)]
    data_forecast['MLP_residual'] =\
        data_forecast['endo_demand'] - data_forecast['pred_KNN_smooth']

    # In[4]:

    data_forecast_train = data_forecast[
        data_forecast['date'] < pd.to_datetime(
            dt.datetime.today().strftime("%m/%d/%Y")
        ) - pd.DateOffset(2)]
    exog_var = [col for col in data_forecast if '_logit_diff' in col or
                'exog_cont_windspeed' in col]
    endo_var = [col for col in data_forecast if 'MLP_residual' in col]

    Analytics_exog_cont_train = data_forecast_train[exog_var]
    Analytics_endo_train = data_forecast_train[endo_var]

    # Generate training sample for exogenous and endogeneous variable
    x_train = Analytics_exog_cont_train.ix[:,
                                           :Analytics_exog_cont_train.shape[1]]
    imp = Imputer(missing_values='NaN', strategy='median', axis=0)
    imp.fit(x_train)
    x_imp = imp.transform(x_train)

    scaler = pp.StandardScaler().fit(x_imp)
    st_x_train = scaler.transform(x_imp)
#    st_x_train = sm.add_constant(st_x_train, has_constant='add')

    y_train = Analytics_endo_train.ix[:, :Analytics_endo_train.shape[1]]
    train_y_scaler = pp.StandardScaler().fit(y_train)
    st_y_train = train_y_scaler.transform(y_train)

    # In[5]:

    nn_mlp = Regressor(
        layers=[
            Layer("Rectifier", units=150),
            Layer("Linear")],
        learning_momentum=0.7,
        weight_decay=0.1,
        learning_rate=0.0001,
        learning_rule="sgd",
        n_iter=5)

    nn_mlp.fit(st_x_train, st_y_train)

    # In[6]:

    nn_mlp = joblib.dump(nn_mlp, 'nn_mlp_UPCL.pkl')
    engine.dispose()
    return

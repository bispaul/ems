"""Forecast DLN Training GUVNL."""
import numpy as np
from sqlalchemy import create_engine
import pandas as pd
# from numpy import array
# from numpy import sign
# from numpy import zeros
# from scipy.interpolate import interp1d
# from datetime import timedelta
from keras.models import Sequential
from keras.layers import Dense
# from keras.wrappers.scikit_learn import KerasRegressor
# from keras.layers.core import TimeDistributedDense, Activation, Dropout
from keras.layers.core import Activation
# from keras.layers.recurrent import GRU
from sklearn import preprocessing as pp
# from sklearn.model_selection import cross_val_score
# from sklearn.model_selection import KFold
# from sklearn.preprocessing import StandardScaler
# from sklearn.preprocessing import MinMaxScaler
# from sklearn.pipeline import Pipeline
# from sklearn.preprocessing import Imputer
from sklearn.impute import SimpleImputer
# import statsmodels.api as sm
import datetime as dt
# import numpy as np
import time
# from keras.models import load_model
pd.options.mode.chained_assignment = None


def forecast_dln_training_guvnl(config, discom):
    """Forecast SVR Scoring GUVNL."""
    # engine = \
    #     create_engine('mysql://root:power@2012@localhost/power', echo=False)
    engine = create_engine(config, echo=False)
    data_train_test = pd.read_sql("""select *
                                        from data_train_test_{}""".
                                        format(discom),
                                        engine,
                                        index_col=None)
    data_forecast = data_train_test.copy()
    hour_dummies = pd.get_dummies(data_forecast['hour'])
    hour_dummies.columns = ['hour_' + str(col) for col in hour_dummies.columns]
    month_dummies = pd.get_dummies(data_forecast['month'])
    month_dummies.columns = ['month_' + str(col)
                             for col in month_dummies.columns]
    week_day_dummies = pd.get_dummies(data_forecast['dayofweek'])
    week_day_dummies.columns = ['dayofweek_' +
                                str(col) for col in week_day_dummies.columns]

    data_forecast = pd.concat([data_forecast,
                               hour_dummies,
                               month_dummies,
                               week_day_dummies], axis=1)
    lag = 2
    data_forecast_train = data_forecast[
        data_forecast['date'] < pd.to_datetime(
            dt.datetime.today().strftime("%m/%d/%Y")
        ) - pd.DateOffset(lag)]

    data_forecast_train = data_forecast_train[
        np.isfinite(data_forecast_train['MLP_residual'])]

    exog_var = [col for col in data_forecast_train.columns
                if 'diff' in col]

    exog_cat = [col for col in data_forecast_train.columns
                if 'hour_' in col and
                'month_shape_Bias' not in col or
                'week_day_' in col]

    endo_var = [col for col in data_forecast_train
                if 'MLP_residual' in col]

    Analytics_exog_cont_train = data_forecast_train[exog_var]
    Analytics_exog_cat_train = data_forecast_train[exog_cat]
    Analytics_endo_train = data_forecast_train[endo_var]
    # print('exog_var', exog_var)
    # print('exog_cat', exog_cat)
    # # Generate training sample for exogenous and endogeneous variable
    # x_train = \
    #     Analytics_exog_cont_train.ix[:, :Analytics_exog_cont_train.shape[1]]
    x_train = \
        Analytics_exog_cont_train.iloc[:, :Analytics_exog_cont_train.shape[1]]        
    # imp = Imputer(missing_values='NaN',
    #               strategy='median',
    #               axis=0)
    imp = SimpleImputer(missing_values=np.nan, strategy='median')
    imp.fit(x_train)
    x_imp = imp.transform(x_train)

    scaler = pp.StandardScaler().fit(x_imp)
    st_x_train = scaler.transform(x_imp)
    # print('st_x_train shape', st_x_train.shape)
    # x_cat_train = Analytics_exog_cat_train.ix[
    #     :, :Analytics_exog_cat_train.shape[1]]
    x_cat_train = Analytics_exog_cat_train.iloc[:, :Analytics_exog_cat_train.shape[1]]
    stx_x_train = np.concatenate((st_x_train, x_cat_train), axis=1)
    # print('stx_x_train shape', stx_x_train.shape)
    # y_train = Analytics_endo_train.ix[:, :Analytics_endo_train.shape[1]]
    y_train = Analytics_endo_train.iloc[:, :Analytics_endo_train.shape[1]]
    imp.fit(y_train)
    y_imp = imp.transform(y_train)
    train_y_scaler = pp.StandardScaler().fit(y_imp)
    st_y_train = train_y_scaler.transform(y_train)

    X = stx_x_train.copy()
    Y = st_y_train
    # print('X shape', X.shape)
    # print('Y shape', Y.shape)
    seed = 7    
    np.random.seed(seed)
    N = X.shape[1]
    model = Sequential()
    # model.add(
    #     Dense(
    #         X.shape[1],
    #         input_dim=X.shape[1],
    #         init='normal',
    #         activation='tanh'))
    model.add(
        Dense(
            X.shape[1],
            input_dim=X.shape[1],
            kernel_initializer='normal', #'glorot_uniform',
            activation='tanh'))            
    model.add(Dense(N / 2))
    model.add(Activation('relu'))
    model.add(Dense(N / 4))
    model.add(Activation('relu'))
    model.add(Dense(N / 8))
    model.add(Activation('relu'))
    model.add(Dense(1))
    model.compile(loss='mean_absolute_error', optimizer='adam')
    # model.fit(X, Y, nb_epoch=5, batch_size=150)
    model.fit(X, Y, epochs=5, batch_size=150)
    time.sleep(0.1)
    score = model.evaluate(X, Y, batch_size=150)
    model.save('keras_model_{}.h5'.format(discom))
    del model  # deletes the existing model
    engine.dispose()
    # print('X Y',X.shape, Y.shape)
    return

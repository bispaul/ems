"""Forecast SVR Training GUVNL."""
# import sklearn.metrics as metrics
import statsmodels.api as sm
from sqlalchemy import create_engine
import pandas as pd
import datetime as dt
import numpy as np
from sklearn import preprocessing as pp
from sklearn.svm import SVR
# from sklearn.externals import joblib
import joblib
# from sklearn.preprocessing import Imputer
from sklearn.impute import SimpleImputer


def forecast_svr_training_guvnl(config, discom):
    """Forecast MLP Scoring GUVNL."""
    # engine = \
    #     create_engine('mysql://root:power@2012@localhost/power', echo=False)
    engine = create_engine(config, echo=False)
    data_train_test = pd.read_sql("""select *
                                        from data_train_test_{}""".
                                        format(discom),
                                        engine,
                                        index_col=None)
    lag = 2
    data_forecast_train = data_train_test[data_train_test['date'] <
                                          pd.to_datetime(dt.datetime.today().
                                          strftime("%m/%d/%Y")) -
                                          pd.DateOffset(lag)]

    exog_var = [col for col in data_forecast_train.columns
                if 'diff' in col]

    Analytics_exog_cont_train = data_forecast_train[exog_var]

    endo_var = [col for col in data_forecast_train.columns
                if 'MLP_residual' in col]

    Analytics_endo_train = data_forecast_train[endo_var]

    # Generate training sample for exogenous and endogeneous variable
    # x_train = \
    #     Analytics_exog_cont_train.ix[:, :Analytics_exog_cont_train.shape[1]]
    x_train = \
        Analytics_exog_cont_train.iloc[:, :Analytics_exog_cont_train.shape[1]]        
    # imp = SimpleImputer(missing_values='NaN', strategy='median', axis=0)
    imp = SimpleImputer(missing_values=np.nan, strategy='median')
    imp.fit(x_train)
    x_imp = imp.transform(x_train)

    scaler = pp.StandardScaler().fit(x_imp)
    st_x_train = scaler.transform(x_imp)
    st_x_train = sm.add_constant(st_x_train, has_constant='add')

    # y_train = Analytics_endo_train.ix[:, :Analytics_endo_train.shape[1]]
    y_train = Analytics_endo_train.iloc[:, :Analytics_endo_train.shape[1]]
    # imp = Imputer(missing_values='NaN', strategy='median', axis=0)
    imp = SimpleImputer(missing_values=np.nan, strategy='median')
    imp.fit(y_train)
    y_imp = imp.transform(y_train)
    train_y_scaler = pp.StandardScaler().fit(y_imp)
    st_y_train = train_y_scaler.transform(y_imp)

    SVR_GETCO = SVR(C=30, cache_size=200, coef0=0.0,
                    degree=3, epsilon=0.1, gamma=3,
                    kernel='rbf', max_iter=-1,
                    shrinking=True, tol=0.0001, verbose=False)

    SVR_GETCO = SVR_GETCO.fit(st_x_train, st_y_train.ravel())
    SVR_GETCO = joblib.dump(SVR_GETCO, 'SVR_{}.pkl'.format(discom))
    engine.dispose()
    return

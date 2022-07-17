"""Forecast SVR Scoring GUVNL."""
# import sklearn.metrics as metrics
from sqlalchemy import create_engine
import pandas as pd
import datetime as dt
import numpy as np
from sklearn import preprocessing as pp
# from sklearn.svm import SVR
import statsmodels.api as sm
# from sklearn.externals import joblib
import joblib
# from sklearn.preprocessing import Imputer
from sklearn.impute import SimpleImputer

def forecast_svr_scoring_guvnl(config, discom, state):
    """Forecast SVR Scoring GUVNL."""
    # engine = \
    #     create_engine('mysql://root:power@2012@localhost/power', echo=False)
    engine = create_engine(config, echo=False)
    data_train_test = pd.read_sql_query("""select *
                                        from data_train_test_{}""".
                                        format(discom),
                                        engine,
                                        index_col=None)
    lag = 2

    data_forecast_train = data_train_test[data_train_test['date'] <
                                          pd.to_datetime(dt.datetime.today().
                                          strftime("%m/%d/%Y")) -
                                          pd.DateOffset(lag)]
    data_forecast_test = data_train_test[data_train_test['date'] >=
                                         pd.to_datetime(dt.datetime.today().
                                         strftime("%m/%d/%Y")) -
                                         pd.DateOffset(lag)]

    exog_var = [col for col in data_forecast_train.columns
                if 'diff' in col]

    endo_var = [col for col in data_forecast_train.columns
                if 'MLP_residual' in col]
    Analytics_exog_cont_train = data_forecast_train[exog_var]

    Analytics_exog_cont_test = data_forecast_test[exog_var]

    Analytics_endo_train = data_forecast_train[endo_var]

    # Generate training sample for exogenous and endogeneous variable
    # x_train = \
    #     Analytics_exog_cont_train.ix[:, :Analytics_exog_cont_train.shape[1]]
    x_train = \
        Analytics_exog_cont_train.iloc[:, :Analytics_exog_cont_train.shape[1]]        
    # imp = Imputer(missing_values='NaN', strategy='median', axis=0)
    imp = SimpleImputer(missing_values=np.nan, strategy='median')
    imp.fit(x_train)
    x_imp = imp.transform(x_train)

    scaler = pp.StandardScaler().fit(x_imp)
    st_x_train = scaler.transform(x_imp)
    st_x_train = sm.add_constant(st_x_train, has_constant='add')

    # Generate test sample for exogenous variable
    # x_test = Analytics_exog_cont_test.ix[:, :Analytics_exog_cont_test.shape[1]]
    x_test = Analytics_exog_cont_test.iloc[:, :Analytics_exog_cont_test.shape[1]]
    # imp_test = Imputer(missing_values='NaN', strategy='median', axis=0)
    imp_test = SimpleImputer(missing_values=np.nan, strategy='median')
    imp_test.fit(x_test)
    x_test_imp = imp.transform(x_test)
    st_x_test = scaler.transform(x_test_imp)
    st_x_test = sm.add_constant(st_x_test, has_constant='add')

    # y_train = Analytics_endo_train.ix[:, :Analytics_endo_train.shape[1]]
    y_train = Analytics_endo_train.iloc[:, :Analytics_endo_train.shape[1]]

    SVR_UPCL = joblib.load('SVR_{}.pkl'.format(discom))
    y_pred = SVR_UPCL.predict(st_x_test)
    MuY = np.array(y_train.mean(axis=0))
    stdY = np.array(y_train.std(axis=0))
    y_pred_SVR = y_pred * stdY + MuY

    pred_table = data_forecast_test[['date',
                                     'block_no',
                                     'year',
                                     'month',
                                     'endo_demand',
                                     'NN_PRED_WEEKDAY_EVENT_CORRECTED']]
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
        'NN_PRED_WEEKDAY_EVENT_CORRECTED'] + pred_table['svr_residual_pred']
    pred_table_final = pred_table
    pred_table_final.to_sql(
        name='pred_table_final_SVR_UPCL',
        con=engine,
        if_exists='replace')
    pred_table['mape'] = abs(pred_table['endo_demand'] -
                             pred_table['SVR_PRED_DEMAND']) / \
        pred_table['endo_demand']

    data_forecast_SVR = pred_table[['date', 'block_no', 'SVR_PRED_DEMAND']]

    data_forecast_SVR.to_sql(name='data_forecast_SVR_{}'.format(discom),
                             con=engine,
                             if_exists='replace')

    pred_table_SVR = pred_table[['date', 'block_no', 'SVR_PRED_DEMAND']]
    pred_table_SVR.rename(columns={'SVR_PRED_DEMAND': 'demand_forecast'},
                          inplace=True)
    pred_table_SVR['discom'] = discom
    pred_table_SVR['state'] = state
    pred_table_SVR['revision'] = 0
    pred_table_SVR['model_name'] = 'SVR'
    tablename = 'pred_table_SVR_{}'.format(discom)
    pred_table_SVR.to_sql(con=engine, name=tablename,
                          if_exists='replace',
                          index=False)
    sql_str = """insert into power.forecast_stg
          (date, state, revision, discom, block_no,
           model_name, demand_forecast)
          select a.date, a.state, a.revision, a.discom, a.block_no,
           a.model_name, round(a.demand_forecast,3) demand_forecast from
           {} a
          on duplicate key
          update demand_forecast = round(values(demand_forecast),3)""".format(tablename, discom)
    connection = engine.connect()
    connection.execute(sql_str)
    connection.close()
    engine.dispose()
    return

"""Forecast MLP Scoring GUVNL."""
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from sklearn import preprocessing as pp
# from sklearn.preprocessing import StandardScaler
import statsmodels.api as sm
import datetime as dt
# from sknn.mlp import Regressor
# from sknn.mlp import Layer
# from sklearn.externals import joblib
import joblib
# from sklearn.preprocessing import Imputer
from sklearn.impute import SimpleImputer


def forecast_mlp_scoring_guvnl(config, discom, state):
    """Forecast MLP Scoring GUVNL."""
    # engine = \
    #     create_engine('mysql://root:power@2012@localhost/power', echo=False)
    engine = create_engine(config, echo=False)
    data_train_test = pd.read_sql_query("""select *
                                        from data_train_test_{}""".
                                        format(discom), engine,
                                        index_col=None)
    lag = 2

    data_forecast_train = \
        data_train_test[data_train_test['date'] <
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

    # Generate sample for whole data and endogeneous variable
    exog_cont_x = data_train_test[exog_var]
    # x = exog_cont_x.ix[:, :exog_cont_x.shape[1]]
    x = exog_cont_x.iloc[:, :exog_cont_x.shape[1]]
    # imp = Imputer(missing_values='NaN', strategy='median', axis=0)
    imp = SimpleImputer(missing_values=np.nan, strategy='median')
    imp.fit(x)
    ximp = imp.transform(x)

    scaler = pp.StandardScaler().fit(ximp)
    st_x = scaler.transform(ximp)
    st_x = sm.add_constant(st_x, has_constant='add')

    exog_cont = data_train_test[endo_var]
    # y = exog_cont.ix[:, :exog_cont.shape[1]]
    y = exog_cont.iloc[:, :exog_cont.shape[1]]
    nn_mlp = joblib.load('nn_mlp_{}.pkl'.format(discom))
    y_pred = nn_mlp.predict(st_x)
    MuY = np.array(y.mean(axis=0))
    stdY = np.array(y.std(axis=0))
    y_pred_MLP = y_pred * stdY + MuY

    pred_table = data_train_test[['date',
                                  'block_no',
                                  'year',
                                  'month',
                                  'endo_demand',
                                  'NN_PRED_WEEKDAY_EVENT_CORRECTED']]
    pred_table['pred_table_key'] = range(0, len(pred_table))

    y_pred_MLP = pd.DataFrame(y_pred_MLP)

    y_pred_MLP['y_pred_MLP_key'] = range(0, len(y_pred_MLP))

    y_pred_MLP.rename(columns={0: 'MLP_Pred'}, inplace=True)
    pred_table = \
        pred_table.merge(y_pred_MLP,
                         left_on='pred_table_key',
                         right_on='y_pred_MLP_key',
                         how='outer')
    pred_table['MLP_PRED_DEMAND'] = \
        pred_table['MLP_Pred'] + pred_table['NN_PRED_WEEKDAY_EVENT_CORRECTED']

    pred_table['mape'] = abs(pred_table['endo_demand'] -
                             pred_table['MLP_PRED_DEMAND']) / \
        pred_table['endo_demand']
    data_forecast_MLP = pred_table[['date', 'block_no', 'MLP_PRED_DEMAND']]
    data_forecast_MLP.to_sql(name='data_forecast_MLP_{}'.format(discom),
                             con=engine, if_exists='replace')

    pred_table_MLP = pred_table[['date', 'block_no', 'MLP_PRED_DEMAND']]
    pred_table_MLP.rename(columns={'MLP_PRED_DEMAND': 'demand_forecast'},
                          inplace=True)
    pred_table_MLP['discom'] = discom
    pred_table_MLP['state'] = state
    pred_table_MLP['revision'] = 0
    pred_table_MLP['model_name'] = 'MLP'
    tablename = 'pred_table_MLP_{}'.format(discom)
    pred_table_MLP.to_sql(con=engine, name=tablename,
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

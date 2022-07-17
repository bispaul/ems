# coding: utf-8
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from sklearn import preprocessing as pp
import statsmodels.api as sm
import datetime as dt
# from sknn.mlp import Regressor
# from sknn.mlp import Layer
from sklearn.externals import joblib
from sklearn.preprocessing import Imputer


# In[63]:
def forecast_mlp_scoring(config):
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
   

    # In[64]:

    data_forecast_train = data_forecast[
        data_forecast['date'] < pd.to_datetime(
            dt.datetime.today().strftime("%m/%d/%Y")
        ) - pd.DateOffset(2)]
    data_forecast_test = data_forecast[
        data_forecast['date'] >= pd.to_datetime(
            dt.datetime.today().strftime("%m/%d/%Y")
        ) - pd.DateOffset(2)]

    exog_var = [col for col in data_forecast if '_logit_diff' in col
                or 'exog_cont_windspeed' in col]

    endo_var = [col for col in data_forecast if 'MLP_residual' in col]

    Analytics_exog_cont_train = data_forecast_train[exog_var]

    Analytics_exog_cont_test = data_forecast_test[exog_var]

    Analytics_endo_train = data_forecast_train[endo_var]

    # Generate training sample for exogenous and endogeneous variable
    x_train = Analytics_exog_cont_train.ix[:,
                                           :Analytics_exog_cont_train.shape[1]]
    imp = Imputer(missing_values='NaN', strategy='median', axis=0)
    imp.fit(x_train)
    x_imp = imp.transform(x_train)

    scaler = pp.StandardScaler().fit(x_imp)
    st_x_train = scaler.transform(x_imp)
    #st_x_train = sm.add_constant(st_x_train, has_constant='add')

    # # Generate test sample for exogenous variable
    x_test = Analytics_exog_cont_test.ix[:, :Analytics_exog_cont_test.shape[1]]
    imp_test = Imputer(missing_values='NaN', strategy='median', axis=0)
    imp_test.fit(x_test)
    x_test_imp = imp.transform(x_test)
    st_x_test = scaler.transform(x_test_imp)
#   st_x_test = sm.add_constant(st_x_test, has_constant='add')

    y_train = Analytics_endo_train.ix[:, :Analytics_endo_train.shape[1]]

    # In[65]:

    nn_mlp = joblib.load('nn_mlp_UPCL.pkl')

    # In[66]:

    y_pred = nn_mlp.predict(st_x_test)
    MuY = np.array(y_train.mean(axis=0))
    stdY = np.array(y_train.std(axis=0))
    y_pred_MLP = y_pred * stdY + MuY

    # In[67]:

    pred_table = data_forecast_test[['date',
                                     'block_no',
                                     'year',
                                     'month',
                                     'demand',
                                     'endo_demand',
                                     'pred_KNN_smooth',
                                     'endo_residual_deterministic']]
    pred_table['pred_table_key'] = range(0, len(pred_table))

    y_pred_MLP = pd.DataFrame(y_pred_MLP)

    y_pred_MLP['y_pred_MLP_key'] = range(0, len(y_pred_MLP))

    y_pred_MLP.rename(columns={0: 'mlp_residual_pred'}, inplace=True)
    pred_table = pred_table.merge(y_pred_MLP, left_on='pred_table_key',
                                  right_on='y_pred_MLP_key', how='outer')
    pred_table['MLP_PRED_DEMAND'] = pred_table[
        'pred_KNN_smooth'] + pred_table['mlp_residual_pred']

    pred_table['mape'] = abs(pred_table['endo_demand'] -
                             pred_table['MLP_PRED_DEMAND']) / \
        pred_table['endo_demand']
    pred_table['mape1'] = abs(pred_table['demand'] -
                              pred_table['MLP_PRED_DEMAND']) / \
        pred_table['demand']
    data_forecast_MLP = pred_table[['date', 'block_no', 'MLP_PRED_DEMAND']]
    data_forecast_MLP.to_sql(name='data_forecast_MLP',
                             con=engine, if_exists='replace', flavor='mysql')

    # In[68]:

    # pred_table_MLP = pred_table[['date' , 'block_no', 'MLP_PRED_DEMAND'],
    # index=idx]
    pred_table_MLP = pred_table[['date', 'block_no', 'MLP_PRED_DEMAND']]
    pred_table_MLP.rename(
        columns={'MLP_PRED_DEMAND': 'demand_forecast'}, inplace=True)
    pred_table_MLP['discom'] = 'UPCL'
    pred_table_MLP['state'] = 'UTTARAKHAND'
    pred_table_MLP['revision'] = 0
    pred_table_MLP['model_name'] = 'MLP'

    pred_table_MLP.to_sql(con=engine, name='pred_table_MLP_UPCL',
                          if_exists='replace', flavor='mysql', index=False)

    sql_str = """insert into power.forecast_stg
          (date, state, revision, discom, block_no,
           model_name, demand_forecast)
          (select a.date, a.state, a.revision, a.discom, a.block_no,
           a.model_name, round(a.demand_forecast,3) demand_forecast from
           {} a,
           (select max(date) date from power.drawl_staging
            where discom = 'UPCL') b
            where a.date >= b.date)
          on duplicate key
          update demand_forecast = round(values(demand_forecast),3),
                 load_date = NULL""".format('pred_table_MLP_UPCL')
    connection = engine.connect()
    connection.execute(sql_str)
    connection.close()
    engine.dispose()
    return

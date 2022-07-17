# coding: utf-8
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from sklearn import preprocessing as pp
# import statsmodels.api as sm
# from pandas.stats.api import ols
import datetime as dt
# from sklearn import preprocessing as pp
from sknn.mlp import Regressor
from sknn.mlp import Layer
# from sklearn.metrics import mean_squared_error
# from neupy.estimators import mse
# import operator
# from sklearn.grid_search import GridSearchCV


def forecast_mlp(config):
    # engine = create_engine('mysql://root:power@2012@localhost/power',
    #                         echo=False)
    engine = create_engine(config, echo=False)
    data_forecast = pd.read_sql_query(
        'select * from data_forecast_UPCL',
        engine,
        index_col=None)
    data_forecast['date'] = pd.to_datetime(data_forecast['date'])
    data_forecast = data_forecast[data_forecast['date'] >= '2014-12-30']
    data_forecast.endo_demand.fillna(data_forecast.pred_KNN_smooth,
                                     inplace=True)

    Analytics_exog_cont = data_forecast[data_forecast. columns[
        data_forecast.columns. to_series().str.contains('exog_cont')]]
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
    # scaler_y = pp.StandardScaler().fit(y)
    # st_y = scaler_y.transform(y)

    data_forecast_train = data_forecast[
        data_forecast['date'] < pd.to_datetime(
            dt.datetime.today(). strftime("%m/%d/%Y")) -
        pd.DateOffset(60)]
    data_forecast_test = data_forecast[
        data_forecast['date'] >= pd.to_datetime(
            dt.datetime.today(). strftime("%m/%d/%Y")) -
        pd.DateOffset(60)]

    data_forecast_test = data_forecast_test.dropna()

    Analytics_exog_cont_train = \
        data_forecast_train[data_forecast_train
                            .columns[data_forecast_train
                                     .columns
                                     .to_series()
                                     .str
                                     .contains('exog_cont')]]
    Analytics_endo_train = data_forecast_train[data_forecast_train
                                               .columns[data_forecast_train
                                                        .columns
                                                        .to_series()
                                                        .str
                                                        .contains('endo')]]
    # Analytics_exog_cont_test = data_forecast_test[data_forecast_test.columns[
    #     data_forecast_test.columns. to_series().str.contains('exog_cont')]]
    # Analytics_endo_test = data_forecast_test[data_forecast_test. columns[
    #     data_forecast_test.columns. to_series().str.contains('endo')]]

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
    # # Generate test sample for exogenous and endogeneous variable
    # x_test = Analytics_exog_cont_test.ix[:,
    #                                      :Analytics_exog_cont_test.shape[1]]
    # x_testlabel = list(x_test.columns)
    # st_x_test = train_x_scaler.transform(x_test)
    # st_x_test = sm.add_constant(st_x_test)

    # y_test = Analytics_endo_test.ix[:, 8:Analytics_endo_test.shape[1] + 1]
    # y_testlabel = list(y_test.columns)
    # st_y_test = train_y_scaler.transform(y_test)

    # In[26]:

    # st_x_train.shape, st_x_test.shape, st_y_train.shape, st_y_test.shape
    # In[27]:

    # def try_std(k):
    #     nn_mlp_test = Regressor(
    #     layers=[
    #         Layer("Rectifier", units=150),  #120 potential candidate
    #         Layer("Linear")],
    #         learning_rate=0.0001,
    #         learning_momentum = 0.7,
    #         weight_decay = 0.1,
    #         learning_rule = "sgd",
    #         n_iter= k)

    #     nn_mlp_test.fit(st_x_train, st_y_train)
    #     mse_train = mean_squared_error(st_y_train,
    #                                    nn_mlp_test.predict(st_x_train))
    #     mse_test = mean_squared_error(st_y_test,
    #                                   nn_mlp_test.predict(st_x_test))
    #     print mse_train, mse_test

    # for k in np.linspace(1,5,5):
    #     print k
    #     try_std(k)
    # get_parameters()

    # In[28]:

    # NN_MLP  = GridSearchCV(Regressor(layers=[
    #                                 Layer("Rectifier", units= 50),
    # #                                 Layer("Rectifier", units=k2),
    #                                 Layer("Linear")],
    #                                 learning_momentum = 0.7,
    #                                 weight_decay = 0.1,
    #                                 learning_rate=0.0001,
    #                                 learning_rule = "sgd",
    #                                 n_iter=1),
    #                      cv=3, n_jobs=-1,
    #                      param_grid={"learning_momentum": [0.5, 0.7, 0.9],
    #                                  "weight_decay": [0.05, 0.1, 0.2],
    # #                                             "units":[50, 100, 150],
    # #                                             "k2":[50, 100, 150],
    #                                             "n_iter":[1]
    #                                  })
    # NN_MLP.fit(st_x_train, st_y_train)

    # In[29]:

    # NN_MLP.get_params().keys()
    # NN_MLP.get_params([x])
    # In[31]:

    nn_mlp = Regressor(
        layers=[
            Layer("Rectifier", units=150),
            Layer("Linear")],
        learning_momentum=0.7,
        weight_decay=0.1,
        learning_rate=0.0001,
        learning_rule="sgd",
        n_iter=1)

    nn_mlp.fit(st_x_train, st_y_train)
    # mse_train = mean_squared_error(st_y_train, nn_mlp.predict(st_x_train))
    # mse_test = mean_squared_error(st_y_test, nn_mlp.predict(st_x_test))
    # print mse_train, mse_test

    # In[102]:

    # nn_mlp.get_params
    # get_parameters()
    # nn_mlp.decision_function
    # In[32]:

    y_pred = nn_mlp.predict(st_x)
    MuY = np.array(y_train.mean(axis=0))
    stdY = np.array(y_train.std(axis=0))
    y_pred_MLP = y_pred * stdY + MuY
    # In[34]:
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

    y_pred_MLP = pd.DataFrame(y_pred_MLP)

    y_pred_MLP['y_pred_MLP_key'] = range(0, len(y_pred_MLP))

    y_pred_MLP.rename(columns={0: 'mlp_residual_pred'}, inplace=True)
    pred_table = pred_table.merge(
        y_pred_MLP,
        left_on='pred_table_key',
        right_on='y_pred_MLP_key',
        how='outer')
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
                             con=engine,
                             if_exists='replace',
                             flavor='mysql')
    pred_table_MLP = pred_table[['date', 'block_no', 'MLP_PRED_DEMAND']]
    pred_table_MLP.rename(
        columns={
            'MLP_PRED_DEMAND': 'demand_forecast'},
        inplace=True)
    pred_table_MLP['discom'] = 'UPCL'
    pred_table_MLP['state'] = 'UTTARAKHAND'
    pred_table_MLP['revision'] = 0
    pred_table_MLP['model_name'] = 'MLP'

    pred_table_MLP.to_sql(
        con=engine,
        name='pred_table_MLP_UPCL',
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
                 load_date = NULL""".format('pred_table_MLP_UPCL')
    connection = engine.connect()
    connection.execute(sql_str)
    connection.close()
    engine.dispose()
    return

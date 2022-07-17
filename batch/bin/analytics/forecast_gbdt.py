import random
import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import LabelEncoder
from scipy.signal import medfilt
from sqlalchemy import create_engine
import gc
# import seaborn as sns
# from matplotlib import pyplot as plt
# plt.style.use("seaborn")
# sns.set(font_scale=1)

def create_lag_features(df, window,feature_cols):
    """
    Creating lag-based features looking back in time.
    """
    #df_site = df.groupby("site_id")
    
    df_rolled = df[feature_cols].rolling(window=window, min_periods=0)
    
    df_mean = df_rolled.mean().reset_index().astype(np.float16)
    df_median = df_rolled.median().reset_index().astype(np.float16)
    df_min = df_rolled.min().reset_index().astype(np.float16)
    df_max = df_rolled.max().reset_index().astype(np.float16)
    df_std = df_rolled.std().reset_index().astype(np.float16)
    df_skew = df_rolled.skew().reset_index().astype(np.float16)
    
    for feature in feature_cols:
        df[f"{feature}_mean_lag{window}"] = df_mean[feature]
        df[f"{feature}_median_lag{window}"] = df_median[feature]
        df[f"{feature}_min_lag{window}"] = df_min[feature]
        df[f"{feature}_max_lag{window}"] = df_max[feature]
        df[f"{feature}_std_lag{window}"] = df_std[feature]
        df[f"{feature}_skew_lag{window}"] = df_std[feature]

    return df

def prepare_data(X, test=False):
    
    if not test:
        X.sort_values(["date","block_no"], inplace=True)
        X.reset_index(drop=True, inplace=True)
    
    gc.collect()
    
    drop_features = ["date"]

    X.drop(drop_features, axis=1, inplace=True)

    if test:
        row_ids = X.row_id
        X.drop("row_id", axis=1, inplace=True)
        return X, row_ids
    else:
        y = np.log1p(X.load)
        X.drop("load", axis=1, inplace=True)
        return X, y

def forecast_gbdt(config, discom, state):
    """Forecast gbdt."""
    #seed
    seed = 0
    random.seed(seed)    
    engine = create_engine(config, echo=False)
    #powercut and load data from db fetch merge and sort
    powercut_table = \
        pd.read_sql("""select date, block_no,
                          sum(powercut) as powercut
                          from powercut_staging where
                          discom = '{}'
                          group by date, block_no""".format(discom),
                          engine, index_col=None)
    powercut_table['date'] = pd.to_datetime(powercut_table['date'])
    powercut_table.sort_values(by=['date', 'block_no'], ascending=[True, True],
                               inplace=True)
    load_table_initial = \
        pd.read_sql("""select date, block_no, constrained_load
                             from drawl_staging where discom = '{}'""".
                          format(discom), engine, index_col=None)

    load_table_initial['date'] = pd.to_datetime(load_table_initial['date'])

    load_table_initial = pd.merge(load_table_initial, powercut_table, 
        how='left', on=['date', 'block_no'])
    load_table_initial['powercut'].fillna(0, inplace=True)
    load_table_initial['unconstrained_load'] = load_table_initial[
        'constrained_load'] + load_table_initial['powercut']
    
    load_table_initial.sort_values(by=['date', 'block_no'], ascending=[
                                   True, True], inplace=True)
    
    block_no = pd.read_sql("""select  block_no, block_hour_no hour
                          from block_master""".format(discom),
                          engine, index_col=None)
    
    load_data = load_table_initial.merge(block_no, how='left', on=['block_no'])
    #cutoff_date
    cutoff_date = np.max(load_data['date'])   
    load_data = load_data[(load_data['date'] <= cutoff_date)]
    load_data.dropna(inplace=True)
    load_data['load'] = load_data['unconstrained_load'].rolling(4, center=True).mean()
    load_data = load_data[['date','block_no','hour','load']]
    load_data['load'] = load_data['load'].fillna(method='ffill').fillna(method='bfill')    

    
    #smooth_load_curve
    unique_date = pd.unique(load_data['date'])
    smooth_load_curve = pd.DataFrame([])
    for j in range(0, len(unique_date)):
        signal = load_data[load_data['date'] == unique_date[j]]
        med_filt = pd.DataFrame(medfilt(signal['load'], 5))
        med_filt = med_filt.rename(columns={0: 'load'})
        med_filt['date'] = unique_date[j]
        med_filt['block_no'] = range(1, len(signal) + 1)
        smooth_load_curve = smooth_load_curve.append(med_filt)
        
    smooth_load_curve['load'] = np.ceil(smooth_load_curve['load'])
    smooth_load_curve['hour'] = np.ceil(smooth_load_curve['block_no']/4)
    smooth_load_curve = smooth_load_curve[['date','block_no','hour','load']]
    smooth_load_curve.date = pd.to_datetime(smooth_load_curve.date, format='%Y-%m-%d')
    
    load_only_table = smooth_load_curve[['date','block_no','hour','load']]
    last_date_block = load_only_table[load_only_table['date'] == 
        max(load_only_table['date'])]
    max_block = max(last_date_block['block_no'])
    columns = ['block_no', 'hour', 'load']

    if max_block < 96:
        forecast_period0 = pd.DataFrame(columns=columns)
        forecast_period0['block_no'] = range(max_block + 1, 97)
        forecast_period0['date'] = max(load_only_table['date'])
        forecast_period0 = forecast_period0[['date', 'block_no','hour', 'load']]
    else:
        forecast_period0 = pd.DataFrame(columns=columns)
        forecast_period0['block_no'] = range(1, 97)
        forecast_period0['date'] = max(load_only_table['date']) + pd.DateOffset(1)
        forecast_period0 = forecast_period0[['date', 'block_no','hour','load']]

    forecast_period = pd.DataFrame([])
    for j in range(1, 8):
        period = pd.DataFrame(columns=columns)
        period['block_no'] = range(1, 97)
        period['date'] = max(forecast_period0['date']) + pd.DateOffset(j)
        period = period[['date', 'block_no','hour', 'load']]
        forecast_period = forecast_period.append(period)

    forecast_period_date = pd.concat([forecast_period0, forecast_period], axis=0)
    load_only_table = pd.concat([load_only_table, forecast_period_date], axis=0)
    
    load_only_table['hour'] = np.ceil(load_only_table['block_no']/4)
    
    #weather data from db
    weather_data = pd.read_sql("""SELECT b.*, a.latitude, a.longitude
                                        FROM
                                        power.imdaws_wunderground_map a,
                                        power.unified_weather b
                                        where b.location =
                                            a.mapped_location_name
                                        and a.discom = '{}'""".format(discom),
                                     engine, index_col=None)    
    weather_data['rainfall_mm'].fillna(0, inplace=True)
    missing_count = list(weather_data.isna().sum())

    keep_columns = ['date',
     'block_hour_no',
     'temperature',
     'rainfall_mm',
     'windspeed',
     'apparenttemperature',
     'dewpoint',
     'relativehumidity',
     'location']
    weather_data = weather_data[keep_columns]

    weather_data_pivot = pd.pivot_table(data=weather_data,index=['date','block_hour_no'], 
          columns = ['location'],aggfunc="mean").reset_index()
    weather_data_pivot.columns = weather_data_pivot.columns.map(lambda x: '_'.join([str(i) for i in x]))
    weather_data_pivot.rename(columns={"date_": "date",
                                       "block_hour_no_":"hour"}, inplace = True)
    weather_data_summary = weather_data.groupby(['date','block_hour_no']).agg(['min','max','mean']).reset_index()
    weather_data_summary.columns = weather_data_summary.columns.map(lambda x: '_'.join([str(i) for i in x]))
    weather_data_summary.rename(columns = {'date_':'date',
                                          'block_hour_no_':'hour'},inplace = True)
    weather_param = weather_data.drop(['block_hour_no', 'location'], axis=1)
    weather_summary = weather_param.groupby(['date']).agg(['min','max','mean']).reset_index()
    weather_summary.columns = weather_summary.columns.map(lambda x: '_'.join([(str(i)+str('_daily')) for i in x]))
    weather_summary.rename(columns = {'date_daily__daily':'date'},inplace = True)
    all_weather_data = weather_data_pivot.merge(weather_data_summary, on = ['date','hour'], how = 'left')
    all_weather_data = all_weather_data.merge(weather_summary, on = 'date', how = 'left') 
    
    del weather_data, weather_data_pivot, weather_data_summary, weather_param, weather_summary, powercut_table, load_table_initial, block_no
    gc.collect()
    
    feature_cols = [item for item in all_weather_data.columns if "location" in item or 'daily' in item]
    
    window = 48
    
    all_weather_data = create_lag_features(all_weather_data, window,feature_cols)
    all_weather_data.date = pd.to_datetime(all_weather_data.date, format='%Y-%m-%d')   
    
    model_df = load_only_table.merge(all_weather_data, on = ['date','hour'], how = 'left')
    
    model_df['lag1_load1'] = model_df['load'].shift(96*2)
    model_df['lag2_load1'] = model_df['load'].shift(96*3)
    model_df['lag3_load1'] = model_df['load'].shift(96*4)
    model_df['lag4_load1'] = model_df['load'].shift(96*5)
    model_df['lag5_load1'] = model_df['load'].shift(96*6)
    model_df['lag6_load1'] = model_df['load'].shift(96*7)
    model_df['lag7_load1'] = model_df['load'].shift(96*8)
    
    max_date = max(model_df['date'])
    min_date = min(model_df['date'])
    
    model_df.date = pd.to_datetime(model_df.date, format='%Y-%m-%d')
    model_df['weekday'] = model_df.date.dt.weekday
    model_df['month'] = model_df.date.dt.month    
    
    model_df_train = model_df[model_df['date'] < cutoff_date]
    X_train, y_train = prepare_data(model_df_train) 
    
    X_half_1 = X_train[:int(X_train.shape[0] / 2)]
    X_half_2 = X_train[int(X_train.shape[0] / 2):]

    y_half_1 = y_train[:int(X_train.shape[0] / 2)]
    y_half_2 = y_train[int(X_train.shape[0] / 2):]

    categorical_features = [ "hour","weekday","month"]

    d_half_1 = lgb.Dataset(X_half_1, label=y_half_1, categorical_feature=categorical_features, free_raw_data=False)
    d_half_2 = lgb.Dataset(X_half_2, label=y_half_2, categorical_feature=categorical_features, free_raw_data=False)

    watchlist_1 = [d_half_1, d_half_2]
    watchlist_2 = [d_half_2, d_half_1]

    params = {
        "objective": "regression",
        "boosting": "gbdt",
        "num_leaves": 90,
        "learning_rate": 0.01,
        "feature_fraction": 0.85,
        "reg_lambda": 2,
        "metric": "rmse"
    }

    print("Building model with first half and validating on second half:")
    model_half_1 = lgb.train(params, train_set=d_half_1, 
                             num_boost_round=2000, 
                             valid_sets=watchlist_1, 
                             verbose_eval=200, 
                             early_stopping_rounds=200)

    print("Building model with second half and validating on first half:")
    model_half_2 = lgb.train(params, train_set=d_half_2, 
                             num_boost_round=2000, 
                             valid_sets=watchlist_2, 
                             verbose_eval=200, 
                             early_stopping_rounds=200)
    
    X_test, row_ids = prepare_data(model_df)
    pred = np.expm1(model_half_1.predict(X_test, num_iteration=model_half_1.best_iteration)) / 2
    pred += np.expm1(model_half_2.predict(X_test, num_iteration=model_half_2.best_iteration)) / 2    
    load_only_table['pred_load'] = pred
    load_only_table['residual']= load_only_table['load'] - load_only_table['pred_load']
    load_only_table['weekday'] = load_only_table.date.dt.weekday
    load_only_table['month'] = load_only_table.date.dt.month
    weekday_residual =  load_only_table.groupby(['month','weekday','block_no']).agg('mean').reset_index()
    weekday_residual = weekday_residual[['month','weekday','block_no','residual']]
    weekday_residual.rename(columns = {'residual':'weekday_residual'}, inplace = True)
    load_only_table = load_only_table.merge(weekday_residual, on = ['month','weekday','block_no'], how = 'left')
    load_only_table['pred_load_weedday_bias_adjusted'] = (load_only_table['pred_load']
        +load_only_table['weekday_residual'])
    load_only_table['weekday_adjusted_pred']= load_only_table['pred_load'] + load_only_table['weekday_residual']    
    predicted_load = load_only_table[['date','block_no','weekday_adjusted_pred']]
    predicted_load.rename(columns={'weekday_adjusted_pred': 'demand_forecast'}, inplace=True)
    predicted_load['discom'] = discom
    predicted_load['state'] = state
    predicted_load['revision'] = 0
    predicted_load['model_name'] = 'GDBOOST'
    table_name = 'pred_table_gdbt_{}'.format(discom)
    predicted_load.to_sql(name=table_name, con=engine,
                                 if_exists='replace')
    sql_str = """insert into power.forecast_stg
          (date, state, revision, discom, block_no,
           model_name, demand_forecast)
          (select a.date, a.state, a.revision, a.discom, a.block_no,
           a.model_name, round(a.demand_forecast,3) demand_forecast from
           {} a)
          on duplicate key
          update demand_forecast = round(values(demand_forecast),3)""".format(table_name, discom)
    connection = engine.connect()
    connection.execute(sql_str)
    connection.close()
    return

# config = 'mysql+pymysql://root:power@2020@localhost/power'
# forecast_gbdt(config, 'UPCL', 'UTTARAKHAND')
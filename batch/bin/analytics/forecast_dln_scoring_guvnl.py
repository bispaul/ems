"""DLN Model Scoring GUVNL."""
import numpy as np
from sqlalchemy import create_engine
import pandas as pd
from sklearn import preprocessing as pp
# from sklearn.preprocessing import Imputer
from sklearn.impute import SimpleImputer
import datetime as dt
from keras.models import load_model
import statsmodels.api as sm
from scipy.signal import medfilt
from scipy import *
from scipy.signal import *
pd.options.mode.chained_assignment = None


def forecast_dln_scoring_guvnl(config, discom, state):
    """Forecast SVR Scoring GUVNL."""
    engine = create_engine(config, echo=False)
    data_train_test = pd.read_sql("""select *
                                        from data_train_test_{}""".
                                        format(discom),
                                        engine,
                                        index_col=None)
    data_forecast = data_train_test.copy()
    lag = 2
    data_forecast_train = \
        data_forecast[data_forecast['date'] <
                      pd.to_datetime(dt.datetime.today().
                      strftime("%m/%d/%Y")) -
                      pd.DateOffset(lag)]
    data_forecast_test = \
        data_forecast[data_forecast['date'] >=
                      pd.to_datetime(dt.datetime.today().
                      strftime("%m/%d/%Y")) -
                      pd.DateOffset(lag)]

    data_forecast_train = \
        data_forecast_train[np.isfinite(data_forecast_train['MLP_residual'])]

    exog_var = [col for col in data_forecast_train.columns
                if 'temp' in col or
                'NN_PRED_WEEKDAY_EVENT_CORRECTED' in col or
                'week_day_correction_factor' in col]

    endo_var = [col for col in data_forecast_train if 'MLP_residual' in col]

    Analytics_exog_cont_train = data_forecast_train[exog_var]
    Analytics_endo_train = data_forecast_train[endo_var]

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
    st_x_train = sm.add_constant(st_x_train, has_constant='add')
    # print('st_x_train shape', st_x_train.shape)
    # y_train = Analytics_endo_train.ix[:, :Analytics_endo_train.shape[1]]
    y_train = Analytics_endo_train.iloc[:, :Analytics_endo_train.shape[1]]
    imp.fit(y_train)
    y_imp = imp.transform(y_train)
    train_y_scaler = pp.StandardScaler().fit(y_imp)
    st_y_train = train_y_scaler.transform(y_train)
    # print('st_y_train shape', st_y_train.shape)
    # Generate test sample for exogenous and endogeneous variable
    Analytics_exog_cont_test = data_forecast_test[exog_var]
    Analytics_endo_test = data_forecast_test[endo_var]
    # print('exog_var', exog_var)
    # x_test = \
    #     Analytics_exog_cont_test.ix[:, :Analytics_exog_cont_test.shape[1]]
    x_test = \
        Analytics_exog_cont_test.iloc[:, :Analytics_exog_cont_test.shape[1]]        
    # imp = Imputer(missing_values='NaN',
    #               strategy='median',
    #               axis=0)
    # print('x_test shape', x_test.shape)
    imp = SimpleImputer(missing_values=np.nan, strategy='median')
    imp.fit(x_test)
    x_imp_test = imp.transform(x_test)

    scaler = pp.StandardScaler().fit(x_imp)
    st_x_test = scaler.transform(x_imp_test)
    # print('st_x_test shape', st_x_test.shape)
    st_x_test = sm.add_constant(st_x_test, has_constant='add')

    X = st_x_test
    model = load_model('keras_model_{}.h5'.format(discom))
    # print('Xshape', X.shape)
    # model.fit(X, Y, batch_size=30, nb_epoch=1)
    # predicted = model.predict(X)
    y_pred = model.predict(X)
    MuY = np.array(y_train.mean(axis=0))
    stdY = np.array(y_train.std(axis=0))
    y_pred_MLP = y_pred * stdY + MuY

    pred_table_test = data_forecast_test[['date',
                                          'block_no',
                                          'year',
                                          'month',
                                          'endo_demand',
                                          'NN_PRED_WEEKDAY_EVENT_CORRECTED']]

    pred_table_test['pred_table_key'] = range(0, len(pred_table_test))

    y_pred_MLP = pd.DataFrame(y_pred_MLP)

    y_pred_MLP['y_pred_MLP_key'] = range(0, len(y_pred_MLP))

    y_pred_MLP.rename(columns={0: 'mlp_pred'}, inplace=True)
    pred_table_test = pred_table_test.merge(y_pred_MLP,
                                            left_on='pred_table_key',
                                            right_on='y_pred_MLP_key',
                                            how='outer')

    pred_table_test['DLN_PRED']= np.where((pred_table_test['DLN_PRED']> 250),250,
                             np.where((pred_table_test['DLN_PRED'] < -250),-250, 
                                         pred_table_test['DLN_PRED']))

    def getEnvelopeModels(aTimeSeries, delta , rejectCloserThan = 0):   
        #Prepend the first value of (s) to the interpolating values. This forces the model to use the same starting point for both the upper and lower envelope models.    
        u_x = [0,]
        u_y = [aTimeSeries[0],]    
        lastPeak = 0
        
        l_x = [0,]
        l_y = [aTimeSeries[0],]
        lastTrough = 0
        
        #Detect peaks and troughs and mark their location in u_x,u_y,l_x,l_y respectively.    
        for k in xrange(1,len(aTimeSeries)- delta):
            #Mark peaks        
            if (sign(aTimeSeries[k]-aTimeSeries[k-delta]) in (0,1)) and (sign(aTimeSeries[k]-aTimeSeries[k+delta]) in (0,1)) and ((k-lastPeak)>rejectCloserThan):
                u_x.append(k)
                u_y.append(aTimeSeries[k])    
                lastPeak = k
                
            #Mark troughs
            if (sign(aTimeSeries[k]-aTimeSeries[k-delta]) in (0,-1)) and ((sign(aTimeSeries[k]-aTimeSeries[k+delta])) in (0,-1)) and ((k-lastTrough)>rejectCloserThan):
                l_x.append(k)
                l_y.append(aTimeSeries[k])
                lastTrough = k
        
        #Append the last value of (s) to the interpolating values. This forces the model to use the same ending point for both the upper and lower envelope models.    
        u_x.append(len(aTimeSeries)-1)
        u_y.append(aTimeSeries[-1])
        
        l_x.append(len(aTimeSeries)-1)
        l_y.append(aTimeSeries[-1])
        
        #Fit suitable models to the data. Here cubic splines.    
        u_p = interp1d(u_x,u_y, kind = 'cubic',bounds_error = False, fill_value=0.0)
        l_p = interp1d(l_x,l_y,kind = 'cubic',bounds_error = False, fill_value=0.0)    
        return (u_p,l_p)

    pred_table_test.sort_values(by=['date','block_no'], ascending=[True, True], inplace=True)
    pred_table_test = pred_table_test[['date',
                                       'block_no',
                                       'endo_demand',
                                       'DLN_PRED',
                                       'NN_PRED_WEEKDAY_EVENT_CORRECTED']].reset_index()
    
    unique_date = pred_table_test['date'].unique()
    smooth_pred_curve = pd.DataFrame([])
    for j in xrange(0, len(unique_date)):
        signal = pred_table_test[pred_table_test['date']==unique_date[j]]
        med_filt = pd.DataFrame(medfilt(signal['DLN_PRED'], 5))
        med_filt = med_filt.rename(columns={0:'median_filter'})
        med_filt['date'] = unique_date[j]
        med_filt['block_no'] = range(1,len(signal)+1)
        smooth_pred_curve= smooth_pred_curve.append(med_filt)     
    smooth_pred_curve.sort_values(by = ['date','block_no'], 
					ascending=[True, True], inplace=True)
    pred_curve_filtered = pd.merge(pred_table_test,
				   smooth_pred_curve, how = 'left', 
                           	  on = ['date','block_no'])
    
    unique_date = pred_curve_filtered['date'].unique()
    dln_envelop = pd.DataFrame([])
    for j in xrange(1, len(unique_date)):
        test = pred_curve_filtered[pred_curve_filtered['date']==unique_date[j]]
        test.sort_values(by=['date','block_no'], ascending=[True, True], inplace=True)
        s = np.array(test['median_filter'])
        P = getEnvelopeModels(s, delta =0, rejectCloserThan = 5)
        q_u = map(P[0],xrange(0,len(s)))     
        q_l = map(P[1],xrange(0,len(s)))
        test = test[['date','block_no',
                     'DLN_PRED','endo_demand',
                     'NN_PRED_WEEKDAY_EVENT_CORRECTED']].reset_index()
        U_envelop = pd.DataFrame(q_u)
        U_envelop = U_envelop.rename(columns={0: 'U_envelop'})
        L_envelop = pd.DataFrame(q_l)
        L_envelop = L_envelop.rename(columns={0: 'L_envelop'})
        envelop = pd.concat([test, U_envelop, L_envelop], axis = 1)
        envelop['DLN_PRED_SMOOTH'] = envelop[['U_envelop','L_envelop']].mean(axis=1)
        dln_envelop = dln_envelop.append(envelop)

    dln_envelop = dln_envelop[['date',
                           'block_no',
                           'endo_demand',
                           'NN_PRED_WEEKDAY_EVENT_CORRECTED',
                           'DLN_PRED_SMOOTH']]


    dln_envelop['DLN_PRED'] = dln_envelop['NN_PRED_WEEKDAY_EVENT_CORRECTED'] \
                         + dln_envelop['DLN_PRED_SMOOTH']
    

    pred_table_DLN = pred_table_test[['date', 'block_no', 'DLN_PRED']]
    pred_table_DLN.rename(
        columns={'DLN_PRED': 'demand_forecast'}, inplace=True)
    pred_table_DLN['discom'] = discom
    pred_table_DLN['state'] = state
    pred_table_DLN['revision'] = 0
    pred_table_DLN['model_name'] = 'DLN'
    tablename = 'pred_table_DLN_{}'.format(discom)

    pred_table_DLN.to_sql(con=engine, name=tablename,
                          if_exists='replace', index=False)
    sql_str = """insert into power.forecast_stg
          (date, state, revision, discom, block_no,
           model_name, demand_forecast)
          select a.date, a.state, a.revision, a.discom, a.block_no,
           a.model_name, round(a.demand_forecast,3) demand_forecast from
           {} a
          on duplicate key
          update demand_forecast = round(values(demand_forecast),3),
                 load_date = NULL""".format(tablename, discom)
    connection = engine.connect()
    connection.execute(sql_str)
    connection.close()
    engine.dispose()
    return

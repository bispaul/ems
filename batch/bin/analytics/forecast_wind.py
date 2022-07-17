# coding: utf-8

from __future__ import division
from sqlalchemy import create_engine
import pandas as pd

import numpy as np
from numpy import array
from numpy import sign
from numpy import zeros
from scipy.interpolate import interp1d
from scipy import interpolate
from scipy.interpolate import UnivariateSpline
from pandas import rolling_median
from datetime import datetime, timedelta
import datetime as dt
import pytz
import time
from scipy.signal import medfilt
from scipy import *
from scipy.signal import *

pd.options.mode.chained_assignment = None


def forecast_wind_nn(config, discom, state):
    re_type = "WIND"
    engine = create_engine(config, echo=False)

    if time.tzname[0] == 'IST':
        max_hour = dt.datetime.today().hour
    else:
        dest_tz = pytz.timezone('Asia/Kolkata')
        ts = time.time()
        utc_now = datetime.utcfromtimestamp(ts)
        local_now = utc_now.replace(tzinfo=pytz.utc).astimezone(dest_tz)
        max_hour = local_now.hour

    max_hour = max(max_hour, 8)

    from math import radians, cos, sin, asin, sqrt
    def haversine(lon1, lat1, lon2, lat2):
        """
        Calculate the great circle distance between two points 
        on the earth (specified in decimal degrees)
        """
        # convert decimal degrees to radians 
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
        # haversine formula 
        dlon = lon2 - lon1 
        dlat = lat2 - lat1 
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * asin(sqrt(a)) 
        km = 6367 * c
        return km

    import numpy as np
    from scipy.interpolate import UnivariateSpline, splev, splrep
    from scipy.optimize import minimize

    def guess(x, y, k, s, w=None):
        """Do an ordinary spline fit to provide knots"""
        return splrep(x, y, w, k=k, s=s)

    def err(c, x, y, t, k, w=None):
        """The error function to minimize"""
        diff = y - splev(x, (t, c, k))
        if w is None:
            diff = np.einsum('...i,...i', diff, diff)
        else:
            diff = np.dot(diff*diff, w)
        return np.abs(diff)

    def spline_neumann(x, y, k=3, s=0, w=None):
        t, c0, k = guess(x, y, k, s, w=w)
        x0 = x[0] # point at which zero slope is required
        con = {'type': 'eq',
               'fun': lambda c: splev(x0, (t, c, k), der=1),
               #'jac': lambda c: splev(x0, (t, c, k), der=2) # doesn't help, dunno why
               }
        opt = minimize(err, c0, (x, y, t, k, w), constraints=con)
        copt = opt.x
        return UnivariateSpline._from_tck((t, copt, k))



    def getEnvelopeModels(aTimeSeries, delta , rejectCloserThan = 0):   
        #Prepend the first value of (s) to the interpolating values. This forces the model to use the same starting point for both the upper and lower envelope models.    
        u_x = [0,]
        u_y = [aTimeSeries[0],# In[106]:
    ]    
        lastPeak = 0;
        
        l_x = [0,]
        l_y = [aTimeSeries[0],]
        lastTrough = 0;
        
        #Detect peaks and troughs and mark their location in u_x,u_y,l_x,l_y respectively.    
        for k in xrange(1,len(aTimeSeries)- delta):
            #Mark peaks        
            if (sign(aTimeSeries[k]-aTimeSeries[k-delta]) in (0,1)) and (sign(aTimeSeries[k]-aTimeSeries[k+delta]) in (0,1)) and ((k-lastPeak)>rejectCloserThan):
                u_x.append(k)
                u_y.append(aTimeSeries[k])    
                lastPeak = k;
                
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


    # In[107]:

    weather_data = pd.read_sql_query("""SELECT b.* , a.latitude, a.longitude
    FROM
    power.imdaws_wunderground_map a,
    power.unified_weather2 b
    where b.location = a.mapped_location_name 
    and a.discom = '{}'""".format(discom), engine, index_col = None)
    weather_data = weather_data.loc[weather_data['data_source'] =='IBMWEATHERCHANNEL']
    weather_data = weather_data.loc[weather_data['data_type'].isin(['FORECAST','ACTUAL'])]
    weather_data['date'] = pd.to_datetime(weather_data['date'])
    weather_data.rename(columns={'block_hour_no': 'hour', 'temperature': 'temp',
                       'rainfall_mm': 'RainMM'}, inplace=True)
    weather_data['year'] = pd.DatetimeIndex(weather_data['date']).year
    weather_data['month'] = pd.DatetimeIndex(weather_data['date']).month   # jan = 1, dec = 12
    WIND_generation_table = pd.read_sql_query("""select generator_name, date, block_no, generation  
                                 from generation_staging where pool_type = '{}' and discom = '{}'""".format(re_type,discom), engine, index_col = None)
    WIND_generation_table['date'] = pd.to_datetime(WIND_generation_table['date'])

    WIND_generation_table.sort_values(by=[ 'generator_name', 'date','block_no'], 
                                      ascending=[True, True, True], inplace=True)

    weather_actual = weather_data[weather_data['data_type']=='ACTUAL']
    weather_forecast = weather_data[weather_data['data_type']=='FORECAST']

    weather_actual = weather_actual[[ 'location', 'date', 'hour','temp','RainMM','windspeed']]

    weather_forecast = weather_forecast[[ 'location', 'date', 'hour','temp','pop','windspeed']]

    weather_actual_nonmissing = weather_actual[np.isfinite(weather_actual['temp'])]
    weather_actual_nonmissing = weather_actual_nonmissing[np.isfinite(weather_actual_nonmissing['RainMM'])]
    weather_actual_nonmissing = weather_actual_nonmissing[np.isfinite(weather_actual_nonmissing['windspeed'])]


    weather_forecast_nonmissing = weather_forecast[np.isfinite(weather_forecast['temp'])]
    weather_forecast_nonmissing = weather_forecast_nonmissing[ np.isfinite(weather_forecast_nonmissing['pop'])]
    weather_forecast_nonmissing = weather_forecast_nonmissing[ np.isfinite(weather_forecast_nonmissing['windspeed'])]

    # binning of weather variables  actual 


    min_wind = min(weather_actual_nonmissing['windspeed'])
    max_wind = max(weather_actual_nonmissing['windspeed'])

    windspeed_bins = list(linspace(min_wind,max_wind,10))
    series = list(weather_actual_nonmissing['windspeed'])
    bin_limit = windspeed_bins
    cat_var = []
    for j in range(0,len(series)):
        for i in range(0,len(bin_limit)):
            if i == 0 and series[j]<bin_limit[i]: 
                cat_var.append(i)
            elif i > 0 and series[j]>= bin_limit[i-1] and series[j] < bin_limit[i]:
                cat_var.append(i)
            elif i == (len(bin_limit)-1) and series[j]>=bin_limit[i]:
                cat_var.append(i+1) 
    binned_windspeed = pd.Series(cat_var)
    weather_actual_nonmissing['windspeed_bin'] = binned_windspeed.values


    min_temp = min(weather_actual_nonmissing['temp'])
    max_temp = max(weather_actual_nonmissing['temp'])

    temp_bins = list(linspace(min_temp,max_temp,50))
    series = list(weather_actual_nonmissing['temp'])
    bin_limit = temp_bins
    cat_var = []
    for j in range(0,len(series)):
        for i in range(0,len(bin_limit)):
            if i == 0 and series[j]<bin_limit[i]: 
                cat_var.append(i)
            elif i > 0 and series[j]>= bin_limit[i-1] and series[j] < bin_limit[i]:
                cat_var.append(i)
            elif i == (len(bin_limit)-1) and series[j]>=bin_limit[i]:
                cat_var.append(i+1) 
    binned_temp = pd.Series(cat_var)
    weather_actual_nonmissing['temp_bin'] = binned_temp.values

    rain_bins = [0,1,5,10,20]
    series = list(weather_actual_nonmissing['RainMM'])
    bin_limit = rain_bins
    cat_var = []
    for j in range(0,len(series)):
        for i in range(0,len(bin_limit)):
            if i == 0 and series[j]<bin_limit[i]: 
                cat_var.append(i)
            elif i > 0 and series[j]>= bin_limit[i-1] and series[j] < bin_limit[i]:
                cat_var.append(i)
            elif i == (len(bin_limit)-1) and series[j]>=bin_limit[i]:
                cat_var.append(i+1) 
    binned_RainMM = pd.Series(cat_var)
    weather_actual_nonmissing['rain_bin'] = binned_RainMM.values
    weather_actual_nonmissing['rain_bin'] = np.where((weather_actual_nonmissing['RainMM']>0),
                                                  weather_actual_nonmissing['rain_bin'],0)
    # binning of weather variables  forecast 

    min_wind = min(weather_forecast_nonmissing['windspeed'])
    max_wind = max(weather_forecast_nonmissing['windspeed'])

    windspeed_bins = list(linspace(min_wind,max_wind,10))
    series = list(weather_forecast_nonmissing['windspeed'])
    bin_limit = windspeed_bins
    cat_var = []
    for j in range(0,len(series)):
        for i in range(0,len(bin_limit)):
            if i == 0 and series[j]<bin_limit[i]: 
                cat_var.append(i)
            elif i > 0 and series[j]>= bin_limit[i-1] and series[j] < bin_limit[i]:
                cat_var.append(i)
            elif i == (len(bin_limit)-1) and series[j]>=bin_limit[i]:
                cat_var.append(i+1) 
    binned_windspeed = pd.Series(cat_var)
    weather_forecast_nonmissing['windspeed_bin'] = binned_windspeed.values


    min_temp = min(weather_forecast_nonmissing['temp'])
    max_temp = max(weather_forecast_nonmissing['temp'])

    temp_bins = list(linspace(min_temp,max_temp,50))
    series = list(weather_forecast_nonmissing['temp'])
    bin_limit = temp_bins
    cat_var = []
    for j in range(0,len(series)):
        for i in range(0,len(bin_limit)):
            if i == 0 and series[j]<bin_limit[i]: 
                cat_var.append(i)
            elif i > 0 and series[j]>= bin_limit[i-1] and series[j] < bin_limit[i]:
                cat_var.append(i)
            elif i == (len(bin_limit)-1) and series[j]>=bin_limit[i]:
                cat_var.append(i+1) 
    binned_temp = pd.Series(cat_var)
    weather_forecast_nonmissing['temp_bin'] = binned_temp.values



    pop_bins = [0,10,20,30,40,50]
    series = list(weather_forecast_nonmissing['pop'])
    bin_limit = pop_bins
    cat_var = []
    for j in range(0,len(series)):
        for i in range(0,len(bin_limit)):
            if i == 0 and series[j]<bin_limit[i]: 
                cat_var.append(i)
            elif i > 0 and series[j]>= bin_limit[i-1] and series[j] < bin_limit[i]:
                cat_var.append(i)
            elif i == (len(bin_limit)-1) and series[j]>=bin_limit[i]:
                cat_var.append(i+1) 
    binned_pop = pd.Series(cat_var)
    weather_forecast_nonmissing['pop_bin'] = binned_pop.values
    weather_forecast_nonmissing['pop_bin'] = np.where((weather_forecast_nonmissing['pop']>0),
                                                  weather_forecast_nonmissing['pop_bin'],0)

    weather_forecast_nonmissing = weather_forecast_nonmissing[['location','date','hour',
                                                               'temp_bin','windspeed_bin' 
                                                               ,'pop_bin']]
    weather_forecast_nonmissing.rename(columns={'temp_bin': 'temp_bin_forecast', 
                                                'windspeed_bin': 'windspeed_bin_forecast',
                                                'pop_bin': 'pop_bin_forecast',  }, inplace=True)

    weather_initial = pd.merge(weather_actual_nonmissing,
                              weather_forecast_nonmissing,
                              how = 'outer',
                              on = ['location','date','hour',])

    weather_initial['temp_bin'].fillna(weather_initial['temp_bin_forecast'])
    weather_initial['windspeed_bin'].fillna(weather_initial['windspeed_bin_forecast'])
    weather_initial['rain_bin'].fillna(weather_initial['pop_bin_forecast'])

    weather_initial = weather_initial[['location', 'date', 'hour','temp_bin','rain_bin','windspeed_bin']]


    # In[110]:

    WIND_gen_table = WIND_generation_table.copy()
    WIND_gen = WIND_gen_table.groupby(['date', 'block_no'],as_index=False).agg({'generation':{'sum':'sum'
                                                                                }})

    WIND_gen.columns = ['_'.join(col).strip() for col in WIND_gen.columns.values]
    WIND_gen.rename(columns={'date_': 'date', 'block_no_' : 'block_no'}, inplace=True)
    WIND_gen['gen_MW'] = WIND_gen['generation_sum']*4

    WIND_gen.sort_values(by=['date','block_no'], ascending=[True, True], inplace=True)


    # In[111]:

    # len(WIND_table_initial['date'].unique()), len(WIND_table_initial)/96


    # In[112]:

    # Missing gen Data Imputation 
    days_missing_block = WIND_gen.groupby(['date'],as_index=False).agg({'gen_MW':{'count':'count'}})  
    days_missing_block.columns = ['_'.join(col).strip() for col in days_missing_block.columns.values]
    days_missing_block.rename(columns={'date_': 'date'}, inplace=True)
    days_to_imp = days_missing_block[days_missing_block['gen_MW_count']<96]
    unique_days_imp = days_to_imp['date'].unique()

    nn_days = 90
    dist_matrix = pd.DataFrame([])
    for j in xrange(0,len(unique_days_imp)):    
        WIND_gen_na = WIND_gen[WIND_gen['date']==unique_days_imp[j]]
        WIND_gen_na = WIND_gen_na[np.isfinite(WIND_gen_na['gen_MW'])]
        block_unique = WIND_gen_na['block_no'].unique()
        
        WIND_gen_window = WIND_gen[(WIND_gen['date']<= unique_days_imp[j]) 
                                    & (WIND_gen['date']>= pd.to_datetime(unique_days_imp[j])
                                       - timedelta(days=nn_days))]
        WIND_gen_window = WIND_gen_window[WIND_gen_window['block_no'].isin(block_unique)]
        
        WIND_gen_window_sum =  WIND_gen_window.groupby(['date'],as_index=False).agg({'gen_MW':{'mean':'mean',                                                                                        'max':'max',
                                                                                             'min':'min',
                                                                                            'max':'max'}}) 
        WIND_gen_window_sum.columns = ['_'.join(col).strip() for col in WIND_gen_window_sum.columns.values]
        WIND_gen_window_sum.rename(columns={'date_': 'date'}, inplace=True)
        WIND_gen_window_sum['gen_MW_max_rank']=WIND_gen_window_sum['gen_MW_max'].rank(ascending=1)
        WIND_gen_window_sum['gen_MW_min_rank']=WIND_gen_window_sum['gen_MW_min'].rank(ascending=1)
        WIND_gen_window_sum['gen_MW_mean_rank']=WIND_gen_window_sum['gen_MW_mean'].rank(ascending=1)
        imp_day_WIND_sum = WIND_gen_window_sum[WIND_gen_window_sum['date']==unique_days_imp[j]]
        nn_day_WIND_sum = WIND_gen_window_sum[WIND_gen_window_sum['date']< unique_days_imp[j]]
        unique_nn_days = nn_day_WIND_sum['date'].unique()
        for i in xrange(0,len(unique_nn_days)):
            test_nn = nn_day_WIND_sum[nn_day_WIND_sum['date']==unique_nn_days[i]]
            var_temp =[col for col in nn_day_WIND_sum.columns if '_rank'in col ]
            coordinate_NN = np.array(test_nn[var_temp])
            coordimate_self = np.array(imp_day_WIND_sum[var_temp])
            dist = np.sum((coordimate_self - coordinate_NN)**2,axis=1)
            dist = pd.DataFrame(dist)
            dist.rename(columns={0: 'eucl_dist'}, inplace=True)
            dist['date']=unique_days_imp[j]
            dist['lag_date']=unique_nn_days[i]
            dist_matrix = dist_matrix.append(dist)
        
    dist_matrix.sort_values(by =['date','eucl_dist'], ascending = [True,True], inplace = True)    
    def ranker(dist_matrix):
        dist_matrix['rank'] = np.arange(len(dist_matrix)) + 1
        return dist_matrix 
    dist_matrix = dist_matrix.groupby(dist_matrix['date']).apply(ranker)
    dist_matrix = dist_matrix[dist_matrix['rank']<=8]
    lag_WIND_gen = pd.merge(dist_matrix ,WIND_gen,
                        left_on = 'lag_date', right_on = 'date')
    lag_WIND_gen = lag_WIND_gen.rename(columns={'date_x':'date',
                                        'gen_MW':'gen_MW_lag'})
    del lag_WIND_gen['date_y']



    lag_WIND_gen = lag_WIND_gen[['date', 'lag_date','rank','block_no','gen_MW_lag']]
    lag_WIND_gen['rank_no']=lag_WIND_gen['rank'].astype(str)+'rank'
    lag_WIND_gen_pivot = pd.pivot_table(lag_WIND_gen, values=['gen_MW_lag'], 
                                                  index=['date','block_no'], columns=['rank_no']).reset_index()
    lag_WIND_gen_pivot.columns = ['_'.join(col).strip() for col in lag_WIND_gen_pivot.columns.values]

    lag_WIND_gen_pivot.rename(columns={'date_': 'date',
                                      'block_no_':'block_no'}, inplace=True)
    t=0.45
    w1=1
    w2=t
    w3=t**2
    w4=t**3
    w5=t**4
    w6=t**5
    w7=t**6

    lag_WIND_gen_pivot['imp_WIND_gen']= ((lag_WIND_gen_pivot['gen_MW_lag_1rank']*w1+
                                 lag_WIND_gen_pivot['gen_MW_lag_2rank']*w2+
                                 lag_WIND_gen_pivot['gen_MW_lag_3rank']*w3+
                                  lag_WIND_gen_pivot['gen_MW_lag_4rank']*w4+
                                  lag_WIND_gen_pivot['gen_MW_lag_5rank']*w5+
                                 lag_WIND_gen_pivot['gen_MW_lag_6rank']*w6+
                                 lag_WIND_gen_pivot['gen_MW_lag_7rank']*w7)
                                /(w1+w2+w3+w4+w5+w6+w7))
        

    lag_WIND_NN = lag_WIND_gen_pivot[['date','block_no','imp_WIND_gen' ]]
    unique_date = lag_WIND_NN['date'].unique()
    envelop_test = pd.DataFrame([])
    for j in xrange(0, len(unique_date)):
        test = lag_WIND_NN[lag_WIND_NN['date']==unique_date[j]]
        s = np.array(test['imp_WIND_gen'])
        P = getEnvelopeModels(s, delta =0, rejectCloserThan = 5)
    #     P = getEnvelopeModels(s, delta =1, rejectCloserThan = 0)

        q_u = map(P[0],xrange(0,len(s)))
        q_l = map(P[1],xrange(0,len(s)))
        test = test[['date','block_no','imp_WIND_gen']].reset_index()
        U_envelop = pd.DataFrame(q_u)
        U_envelop = U_envelop.rename(columns={0: 'U_envelop'})
        L_envelop = pd.DataFrame(q_l)
        L_envelop = L_envelop.rename(columns={0: 'L_envelop'})
        envelop = pd.concat([test, U_envelop,L_envelop], axis = 1)
        envelop_test = envelop_test.append(envelop)
    envelop_test['envelop'] = envelop_test[['U_envelop','L_envelop']].mean(axis =1)
    envelop_test.sort_values(by = ['date','block_no'], 
                                           ascending=[True, True], 
                                           inplace=True)
    envelop_test['envelop_lag'] = envelop_test['envelop'].shift(1)
    envelop_test['fcurve'] = envelop_test['envelop']/envelop_test['envelop_lag'] 
    WIND_gen_impdate = pd.merge(envelop_test,WIND_gen,
                        how = 'left',
                        on = ['date','block_no'])


    fcurve = list(np.array(WIND_gen_impdate['fcurve']))
    gen_MW = list(np.array(WIND_gen_impdate['gen_MW']))
    imp_gen_imp = []
    for j in range(0,len(WIND_gen_impdate)):
        if j == 0:
            imp_gen = gen_MW[j]
            imp_gen_imp.append(imp_gen)
        else:
            imp_gen1 = min(imp_gen_imp[j-1]*fcurve[j],max(WIND_gen['gen_MW']))
            imp_gen_imp.append(imp_gen1)
    imp_gen_imp = pd.Series(imp_gen_imp)
    WIND_gen_impdate['smooth_imp'] = imp_gen_imp.values

    WIND_gen_imp = WIND_gen_impdate[['date','block_no', 'smooth_imp']]

    WIND_table_initial = pd.merge(WIND_gen,WIND_gen_imp,
                        how = 'left',
                        on = ['date','block_no'])

         

    WIND_table_initial = WIND_table_initial[['date','block_no','gen_MW','smooth_imp']]

    WIND_table_initial['gen_MW'].fillna(WIND_table_initial['smooth_imp'], inplace = True)


    # In[113]:

    weather_initial['hour_no']=weather_initial['hour'].astype(str)+'hour'


    weather_summary = weather_initial.groupby(['date', 'location'],as_index=False).agg({'temp_bin':{'max':'max',
                                                                                                  'min':'min',
                                                                                                  'mean':'mean'},
                                                                                                'windspeed_bin':{'max':'max',
                                                                                                  'min':'min',
                                                                                                  'mean':'mean'}, 
                                                                                                'rain_bin':{'max':'max',
                                                                                                  'min':'min',
                                                                                                  'mean':'mean'} 
                                                                                                  })

    weather_summary.columns = ['_'.join(col).strip() for col in weather_summary.columns.values]
    weather_summary.rename(columns={'date_': 'date', 'location_' : 'location'}, inplace=True)

        
    weather_hourly_pivot = pd.pivot_table(weather_initial, 
                                values=['temp_bin',
                                        'windspeed_bin',
                                        'rain_bin'
                                       ], 
                                index=['date'], 
                                columns=['location','hour_no']).reset_index()

    weather_hourly_pivot.columns = ['_'.join(col).strip() for col in weather_hourly_pivot.columns.values]
    weather_hourly_pivot.rename(columns={'date__': 'date'}, inplace=True)

    weather_summary_pivot = pd.pivot_table(weather_summary, 
                                values=[
                                        'temp_bin_max',
                                        'temp_bin_mean',
                                        'temp_bin_min'
                                        'windspeed_bin_max',
                                        'windspeed_bin_mean',
                                        'windspeed_bin_min',
                                         'rain_bin_max',
                                         'rain_bin_mean',
                                         'rain_bin_min'
                                        ], 
                                index=['date'], 
                                columns=['location']).reset_index()

    weather_summary_pivot.columns = ['_'.join(col).strip() for col in weather_summary_pivot.columns.values]
    weather_summary_pivot.rename(columns={'date_': 'date'}, inplace=True)


    # In[114]:

    unique_date = WIND_table_initial['date'].unique()
    smooth_WIND_curve = pd.DataFrame([])
    for j in xrange(0, len(unique_date)):
        signal = WIND_table_initial[WIND_table_initial['date']==unique_date[j]]
        med_filt = pd.DataFrame(medfilt(signal['gen_MW'], 5))
        med_filt = med_filt.rename(columns={0:'med_filter'})
        med_filt['date'] = unique_date[j]
        med_filt['block_no'] = range(1,len(signal)+1)
        smooth_WIND_curve= smooth_WIND_curve.append(med_filt)     
    smooth_WIND_curve.sort_values(by = ['date','block_no'], ascending=[True, True], inplace=True)
    WIND_curve_filtered = pd.merge(WIND_table_initial,smooth_WIND_curve, how = 'left', 
                               on = ['date','block_no'])


    # In[115]:

    unique_date = WIND_curve_filtered['date'].unique()
    WIND_envelop = pd.DataFrame([])
    for j in xrange(0, len(unique_date)):
        test = WIND_curve_filtered[WIND_curve_filtered['date']==unique_date[j]]
        s = np.array(test['med_filter'])
        P = getEnvelopeModels(s, delta =0, rejectCloserThan = 5)
    #     P = getEnvelopeModels(s, delta =1, rejectCloserThan = 0)

        q_u = map(P[0],xrange(0,len(s)))
        q_l = map(P[1],xrange(0,len(s)))
        test = test[['date','block_no','gen_MW','med_filter']].reset_index()
        U_envelop = pd.DataFrame(q_u)
        U_envelop = U_envelop.rename(columns={0: 'U_envelop'})
        L_envelop = pd.DataFrame(q_l)
        L_envelop = L_envelop.rename(columns={0: 'L_envelop'})
        envelop = pd.concat([test, U_envelop,L_envelop], axis = 1)
        WIND_envelop = WIND_envelop.append(envelop)
    WIND_envelop['envelop'] = WIND_envelop[['U_envelop','L_envelop']].mean(axis =1)
    WIND_envelop.envelop.fillna(WIND_envelop.med_filter, inplace=True)


    # In[116]:


    WIND_envelop = WIND_envelop[['date','block_no','gen_MW','envelop']]
    WIND_envelop['deviation'] = WIND_envelop['gen_MW']-WIND_envelop['envelop']

    WIND_envelop['deviation_rolling']=WIND_envelop['deviation'].rolling(window = 5, center = True).mean()

    WIND_envelop['deviation_rolling'] = np.where((WIND_envelop['deviation_rolling'] > 0),
                                                       WIND_envelop['deviation_rolling'],0)
    WIND_envelop['spline_envelop'] = WIND_envelop['envelop'] + WIND_envelop['deviation_rolling']
    WIND_envelop['endo_WIND'] = WIND_envelop['spline_envelop']
    WIND_table = WIND_envelop[['date','block_no', 'gen_MW', 'endo_WIND']] 


    # In[117]:

    WIND_table['hour'] = np.ceil(WIND_table['block_no']/4)
    WIND_table['year'] = pd.DatetimeIndex(WIND_table['date']).year
    WIND_table['month'] = pd.DatetimeIndex(WIND_table['date']).month   # jan = 1, dec = 12
    WIND_table['dayofweek'] = pd.DatetimeIndex(WIND_table['date']).dayofweek # Monday=0, Sunday=6
    WIND_table.sort_values(by = ['date','block_no'], ascending=[True, True], inplace=True)


    # In[118]:


    WIND_only_table = WIND_table[['date','block_no','endo_WIND','gen_MW']]
    last_date_block = WIND_only_table[WIND_only_table['date']== max(WIND_only_table['date'])]
    max_block = max(last_date_block['block_no'])
    columns = [['block_no','endo_WIND']]

    if max_block < 96:
        forecast_period0 = pd.DataFrame(columns=columns)
        forecast_period0['block_no']=range(max_block+1, 97)
        forecast_period0['date'] =max(load_only_table['date']) 
        forecast_period0 = forecast_period0[['date','block_no','endo_WIND']]    
    else:
        forecast_period0 = pd.DataFrame(columns=columns)
        forecast_period0['block_no']=range(1, 97)
        forecast_period0['date'] =max(WIND_only_table['date']) + pd.DateOffset(1)
        forecast_period0 = forecast_period0[['date','block_no','endo_WIND']]

        
    forecast_period = pd.DataFrame([])
    for j in xrange(1, 8):
        period = pd.DataFrame(columns=columns)
        period['block_no']=range(1, 97)
        period['date'] =max(forecast_period0['date']) + pd.DateOffset(j)
        period = period[['date','block_no','endo_WIND']]
        forecast_period = forecast_period.append(period)
        
    forecast_period_date = pd.concat([forecast_period0, forecast_period] , axis =0)

    WIND_only_table = pd.concat([WIND_only_table, forecast_period_date] , axis =0)



    # In[120]:

    # var_hourly_windspeed

    weather_all = weather_hourly_pivot
    weather_relative = weather_hourly_pivot
    weather_all.sort_values(by = ['date'], ascending = [True], inplace = True)
    weather_relative.sort_values(by = ['date'], ascending = [True], inplace = True)

    nn_days = 90
    lag_nn = 12
    lag_d = 0
    var_temp =[col for col in weather_all.columns 
                 if 'windspeed_bin'in col ]
    unique_date_all = weather_all['date'].unique()
    dist_matrix_WS = pd.DataFrame([])
    for i in xrange(0,len(unique_date_all)):
        test_all = weather_all[weather_all['date']==unique_date_all[i]]
        coordinate_all = np.array(test_all[var_temp])
        test_relative =weather_relative[(weather_relative['date'] <  pd.to_datetime(unique_date_all[i]) 
                                        - timedelta(days=lag_d)) & 
                                       (weather_relative['date']>=  
                                        pd.to_datetime(unique_date_all[i]) 
                                        - timedelta(days=nn_days+lag_d))]
        unique_date_relative = test_relative['date'].unique()
        dist_all = pd.DataFrame([])
        for j in xrange(0,len(unique_date_relative)):
            test_relative_j = test_relative[test_relative['date']==pd.to_datetime(unique_date_relative[j])]
            coordinate_relative = np.array(test_relative_j[var_temp])
            dist = np.sum((coordinate_all - coordinate_relative)**2,axis=1)
            dist = pd.DataFrame(dist)
            dist.rename(columns={0: 'eucledean_dist'}, inplace=True)
            dist['date']=unique_date_all[i]
            dist['lag_date']=unique_date_relative[j]
            dist_all = dist_all.append(dist)
        dist_matrix_WS = dist_matrix_WS.append(dist_all)
    dist_matrix_WS.sort_values(by =['date','eucledean_dist'], ascending = [True,True], inplace = True)    
    def ranker(dist_matrix_WS):
        dist_matrix_WS['rank'] = np.arange(len(dist_matrix_WS)) + 1
        return dist_matrix_WS

    dist_matrix_WS = dist_matrix_WS.groupby(dist_matrix_WS['date']).apply(ranker)
    dist_matrix_WS = dist_matrix_WS[dist_matrix_WS['rank']<=lag_nn]
    weather_dist_lag_initial = pd.merge(dist_matrix_WS,WIND_table, 
                                       left_on ='lag_date', right_on = 'date' )
    weather_dist_lag_initial.rename(columns={'date_x': 'date'}, inplace=True)
    weather_dist_lag_initial = weather_dist_lag_initial.drop('date_y', axis=1)
    weather_dist_lag_initial['rank_no']=weather_dist_lag_initial['rank'].astype(str)+'lag'
    weather_dist_lag = pd.pivot_table(weather_dist_lag_initial, values=['endo_WIND'], 
                                                  index=['date','block_no'], columns=['rank_no']).reset_index()
    weather_dist_lag.columns = ['_'.join(col).strip() for col in weather_dist_lag.columns.values]
    weather_dist_lag.rename(columns={'date_':'date','block_no_':'block_no'},inplace = True)


    weather_dist_lag = pd.merge(WIND_only_table,weather_dist_lag,
                                how = 'left',
                                on = ['date','block_no'])


    weather_dist_lag.endo_WIND_8lag.fillna(weather_dist_lag.endo_WIND_9lag, inplace=True)
    weather_dist_lag.endo_WIND_7lag.fillna(weather_dist_lag.endo_WIND_8lag, inplace=True)
    weather_dist_lag.endo_WIND_6lag.fillna(weather_dist_lag.endo_WIND_7lag, inplace=True)
    weather_dist_lag.endo_WIND_5lag.fillna(weather_dist_lag.endo_WIND_6lag, inplace=True)
    weather_dist_lag.endo_WIND_4lag.fillna(weather_dist_lag.endo_WIND_5lag, inplace=True)
    weather_dist_lag.endo_WIND_3lag.fillna(weather_dist_lag.endo_WIND_4lag, inplace=True)
    weather_dist_lag.endo_WIND_2lag.fillna(weather_dist_lag.endo_WIND_3lag, inplace=True)
    weather_dist_lag.endo_WIND_1lag.fillna(weather_dist_lag.endo_WIND_2lag, inplace=True)
    t=0.8
    w1=1
    w2=t
    w3=t**2
    w4=t**3
    w5=t**4
    w6=t**5
    w7=t**6
    w8=t**7
    w9=t**8

    weather_dist_lag['endo_pred_sim_day_WIND_WS_hourly']= (weather_dist_lag['endo_WIND_1lag']*w1+
                                                 weather_dist_lag['endo_WIND_2lag']*w2+
                                                 weather_dist_lag['endo_WIND_3lag']*w3+
                                                 weather_dist_lag['endo_WIND_4lag']*w4+
                                                 weather_dist_lag['endo_WIND_5lag']*w5+
                                                 weather_dist_lag['endo_WIND_6lag']*w6+
                                                 weather_dist_lag['endo_WIND_7lag']*w7+
                                                 weather_dist_lag['endo_WIND_8lag']*w8+
                                                 weather_dist_lag['endo_WIND_9lag']*w9
                                                 )\
                                                 /(w1+w2+w3+w4+w5+w6+w7+w8+w9)
        
    WIND_forecast_nn_WS = weather_dist_lag[['date',
                                      'block_no',
                                      'gen_MW',
                                      'endo_pred_sim_day_WIND_WS_hourly']]
    # WIND_forecast_nn_WS.to_sql(name='WIND_forecast_nn_WS_{}'.format(discom), con=engine,  if_exists='replace')


    # In[121]:

    # var_summary

    weather_all = weather_summary_pivot
    weather_relative = weather_summary_pivot
    weather_all.sort_values(by = ['date'], ascending = [True], inplace = True)
    weather_relative.sort_values(by = ['date'], ascending = [True], inplace = True)

    nn_days = 90
    lag_nn = 12
    lag_d = 0

    var_temp =[col for col in weather_all.columns 
                 if 'temp_bin'in col]

    unique_date_all = weather_all['date'].unique()
    dist_matrix_sum = pd.DataFrame([])
    for i in xrange(0,len(unique_date_all)):
        test_all = weather_all[weather_all['date']==unique_date_all[i]]
        coordinate_all = np.array(test_all[var_temp])
        test_relative =weather_relative[(weather_relative['date'] <  pd.to_datetime(unique_date_all[i]) 
                                        - timedelta(days=lag_d)) & 
                                       (weather_relative['date']>=  
                                        pd.to_datetime(unique_date_all[i]) 
                                        - timedelta(days=nn_days+lag_d))]
        unique_date_relative = test_relative['date'].unique()
        dist_all = pd.DataFrame([])
        for j in xrange(0,len(unique_date_relative)):
            test_relative_j = test_relative[test_relative['date']==pd.to_datetime(unique_date_relative[j])]
            coordinate_relative = np.array(test_relative_j[var_temp])
            dist = np.sum((coordinate_all - coordinate_relative)**2,axis=1)
            dist = pd.DataFrame(dist)
            dist.rename(columns={0: 'eucledean_dist'}, inplace=True)
            dist['date']=unique_date_all[i]
            dist['lag_date']=unique_date_relative[j]
            dist_all = dist_all.append(dist)
        dist_matrix_sum = dist_matrix_sum.append(dist_all)
    dist_matrix_sum.sort_values(by =['date','eucledean_dist'], ascending = [True,True], inplace = True)    
    def ranker(dist_matrix_sum):
        dist_matrix_sum['rank'] = np.arange(len(dist_matrix_sum)) + 1
        return dist_matrix_sum

    dist_matrix_sum = dist_matrix_sum.groupby(dist_matrix_sum['date']).apply(ranker)
    dist_matrix_sum = dist_matrix_sum[dist_matrix_sum['rank']<=lag_nn]
    weather_dist_lag_initial = pd.merge(dist_matrix_sum,WIND_table, 
                                       left_on ='lag_date', right_on = 'date' )
    weather_dist_lag_initial.rename(columns={'date_x': 'date'}, inplace=True)
    weather_dist_lag_initial = weather_dist_lag_initial.drop('date_y', axis=1)
    weather_dist_lag_initial['rank_no']=weather_dist_lag_initial['rank'].astype(str)+'lag'
    weather_dist_lag = pd.pivot_table(weather_dist_lag_initial, values=['endo_WIND'], 
                                                  index=['date','block_no'], columns=['rank_no']).reset_index()
    weather_dist_lag.columns = ['_'.join(col).strip() for col in weather_dist_lag.columns.values]
    weather_dist_lag.rename(columns={'date_':'date','block_no_':'block_no'},inplace = True)


    weather_dist_lag = pd.merge(WIND_only_table,weather_dist_lag,
                                how = 'left',
                                on = ['date','block_no'])


    weather_dist_lag.endo_WIND_8lag.fillna(weather_dist_lag.endo_WIND_9lag, inplace=True)
    weather_dist_lag.endo_WIND_7lag.fillna(weather_dist_lag.endo_WIND_8lag, inplace=True)
    weather_dist_lag.endo_WIND_6lag.fillna(weather_dist_lag.endo_WIND_7lag, inplace=True)
    weather_dist_lag.endo_WIND_5lag.fillna(weather_dist_lag.endo_WIND_6lag, inplace=True)
    weather_dist_lag.endo_WIND_4lag.fillna(weather_dist_lag.endo_WIND_5lag, inplace=True)
    weather_dist_lag.endo_WIND_3lag.fillna(weather_dist_lag.endo_WIND_4lag, inplace=True)
    weather_dist_lag.endo_WIND_2lag.fillna(weather_dist_lag.endo_WIND_3lag, inplace=True)
    weather_dist_lag.endo_WIND_1lag.fillna(weather_dist_lag.endo_WIND_2lag, inplace=True)
    t=0.8
    w1=1
    w2=t
    w3=t**2
    w4=t**3
    w5=t**4
    w6=t**5
    w7=t**6
    w8=t**7
    w9=t**8

    weather_dist_lag['endo_pred_sim_day_temp_sum']= (weather_dist_lag['endo_WIND_1lag']*w1+
                                                 weather_dist_lag['endo_WIND_2lag']*w2+
                                                 weather_dist_lag['endo_WIND_3lag']*w3+
                                                 weather_dist_lag['endo_WIND_4lag']*w4+
                                                 weather_dist_lag['endo_WIND_5lag']*w5+
                                                 weather_dist_lag['endo_WIND_6lag']*w6+
                                                 weather_dist_lag['endo_WIND_7lag']*w7+
                                                 weather_dist_lag['endo_WIND_8lag']*w8+
                                                 weather_dist_lag['endo_WIND_9lag']*w9
                                                 )\
                                                 /(w1+w2+w3+w4+w5+w6+w7+w8+w9)
        
    WIND_forecast_nn_temp_sum = weather_dist_lag[['date',
                                      'block_no',
                                      'endo_pred_sim_day_temp_sum']]
    # WIND_forecast_nn_temp_sum.to_sql(name='WIND_forecast_nn_temp_sum_{}'.format(discom), con=engine,  if_exists='replace')



    # In[122]:

    # var_summary

    weather_all = weather_summary_pivot
    weather_relative = weather_summary_pivot
    weather_all.sort_values(by = ['date'], ascending = [True], inplace = True)
    weather_relative.sort_values(by = ['date'], ascending = [True], inplace = True)

    nn_days = 90
    lag_nn = 12
    lag_d = 0

    var_temp =[col for col in weather_all.columns 
                 if 'temp_bin'in col or 'windspeed_bin' in col ]

    unique_date_all = weather_all['date'].unique()
    dist_matrix_sum = pd.DataFrame([])
    for i in xrange(0,len(unique_date_all)):
        test_all = weather_all[weather_all['date']==unique_date_all[i]]
        coordinate_all = np.array(test_all[var_temp])
        test_relative =weather_relative[(weather_relative['date'] <  pd.to_datetime(unique_date_all[i]) 
                                        - timedelta(days=lag_d)) & 
                                       (weather_relative['date']>=  
                                        pd.to_datetime(unique_date_all[i]) 
                                        - timedelta(days=nn_days+lag_d))]
        unique_date_relative = test_relative['date'].unique()
        dist_all = pd.DataFrame([])
        for j in xrange(0,len(unique_date_relative)):
            test_relative_j = test_relative[test_relative['date']==pd.to_datetime(unique_date_relative[j])]
            coordinate_relative = np.array(test_relative_j[var_temp])
            dist = np.sum((coordinate_all - coordinate_relative)**2,axis=1)
            dist = pd.DataFrame(dist)
            dist.rename(columns={0: 'eucledean_dist'}, inplace=True)
            dist['date']=unique_date_all[i]
            dist['lag_date']=unique_date_relative[j]
            dist_all = dist_all.append(dist)
        dist_matrix_sum = dist_matrix_sum.append(dist_all)
    dist_matrix_sum.sort_values(by =['date','eucledean_dist'], ascending = [True,True], inplace = True)    
    def ranker(dist_matrix_sum):
        dist_matrix_sum['rank'] = np.arange(len(dist_matrix_sum)) + 1
        return dist_matrix_sum

    dist_matrix_sum = dist_matrix_sum.groupby(dist_matrix_sum['date']).apply(ranker)
    dist_matrix_sum = dist_matrix_sum[dist_matrix_sum['rank']<=lag_nn]
    weather_dist_lag_initial = pd.merge(dist_matrix_sum,WIND_table, 
                                       left_on ='lag_date', right_on = 'date' )
    weather_dist_lag_initial.rename(columns={'date_x': 'date'}, inplace=True)
    weather_dist_lag_initial = weather_dist_lag_initial.drop('date_y', axis=1)
    weather_dist_lag_initial['rank_no']=weather_dist_lag_initial['rank'].astype(str)+'lag'
    weather_dist_lag = pd.pivot_table(weather_dist_lag_initial, values=['endo_WIND'], 
                                                  index=['date','block_no'], columns=['rank_no']).reset_index()
    weather_dist_lag.columns = ['_'.join(col).strip() for col in weather_dist_lag.columns.values]
    weather_dist_lag.rename(columns={'date_':'date','block_no_':'block_no'},inplace = True)


    weather_dist_lag = pd.merge(WIND_only_table,weather_dist_lag,
                                how = 'left',
                                on = ['date','block_no'])


    weather_dist_lag.endo_WIND_8lag.fillna(weather_dist_lag.endo_WIND_9lag, inplace=True)
    weather_dist_lag.endo_WIND_7lag.fillna(weather_dist_lag.endo_WIND_8lag, inplace=True)
    weather_dist_lag.endo_WIND_6lag.fillna(weather_dist_lag.endo_WIND_7lag, inplace=True)
    weather_dist_lag.endo_WIND_5lag.fillna(weather_dist_lag.endo_WIND_6lag, inplace=True)
    weather_dist_lag.endo_WIND_4lag.fillna(weather_dist_lag.endo_WIND_5lag, inplace=True)
    weather_dist_lag.endo_WIND_3lag.fillna(weather_dist_lag.endo_WIND_4lag, inplace=True)
    weather_dist_lag.endo_WIND_2lag.fillna(weather_dist_lag.endo_WIND_3lag, inplace=True)
    weather_dist_lag.endo_WIND_1lag.fillna(weather_dist_lag.endo_WIND_2lag, inplace=True)
    t=0.8
    w1=1
    w2=t
    w3=t**2
    w4=t**3
    w5=t**4
    w6=t**5
    w7=t**6
    w8=t**7
    w9=t**8

    weather_dist_lag['endo_pred_sim_day_sum']= (weather_dist_lag['endo_WIND_1lag']*w1+
                                                 weather_dist_lag['endo_WIND_2lag']*w2+
                                                 weather_dist_lag['endo_WIND_3lag']*w3+
                                                 weather_dist_lag['endo_WIND_4lag']*w4+
                                                 weather_dist_lag['endo_WIND_5lag']*w5+
                                                 weather_dist_lag['endo_WIND_6lag']*w6+
                                                 weather_dist_lag['endo_WIND_7lag']*w7+
                                                 weather_dist_lag['endo_WIND_8lag']*w8+
                                                 weather_dist_lag['endo_WIND_9lag']*w9
                                                 )\
                                                 /(w1+w2+w3+w4+w5+w6+w7+w8+w9)
        
    WIND_forecast_nn_sum = weather_dist_lag[['date',
                                      'block_no',
                                      'endo_pred_sim_day_sum']]
    # WIND_forecast_nn_sum.to_sql(name='WIND_forecast_nn_sum_{}'.format(discom), con=engine,  if_exists='replace')



    # In[123]:

    # var_summary

    weather_all = weather_summary_pivot
    weather_relative = weather_summary_pivot
    weather_all.sort_values(by = ['date'], ascending = [True], inplace = True)
    weather_relative.sort_values(by = ['date'], ascending = [True], inplace = True)

    nn_days = 90
    lag_nn = 12
    lag_d = 0


    var_temp =[col for col in weather_all.columns 
                   if 'windspeed_bin' in col]

    unique_date_all = weather_all['date'].unique()
    dist_matrix_WS_sum = pd.DataFrame([])
    for i in xrange(0,len(unique_date_all)):
        test_all = weather_all[weather_all['date']==unique_date_all[i]]
        coordinate_all = np.array(test_all[var_temp])
        test_relative =weather_relative[(weather_relative['date'] <  pd.to_datetime(unique_date_all[i]) 
                                        - timedelta(days=lag_d)) & 
                                       (weather_relative['date']>=  
                                        pd.to_datetime(unique_date_all[i]) 
                                        - timedelta(days=nn_days+lag_d))]
        unique_date_relative = test_relative['date'].unique()
        dist_all = pd.DataFrame([])
        for j in xrange(0,len(unique_date_relative)):
            test_relative_j = test_relative[test_relative['date']==pd.to_datetime(unique_date_relative[j])]
            coordinate_relative = np.array(test_relative_j[var_temp])
            dist = np.sum((coordinate_all - coordinate_relative)**2,axis=1)
            dist = pd.DataFrame(dist)
            dist.rename(columns={0: 'eucledean_dist'}, inplace=True)
            dist['date']=unique_date_all[i]
            dist['lag_date']=unique_date_relative[j]
            dist_all = dist_all.append(dist)
        dist_matrix_WS_sum = dist_matrix_WS_sum.append(dist_all)
    dist_matrix_WS_sum.sort_values(by =['date','eucledean_dist'], ascending = [True,True], inplace = True)    
    def ranker(dist_matrix_WS_sum):
        dist_matrix_WS_sum['rank'] = np.arange(len(dist_matrix_WS_sum)) + 1
        return dist_matrix_WS_sum

    dist_matrix_WS_sum = dist_matrix_WS_sum.groupby(dist_matrix_WS_sum['date']).apply(ranker)
    dist_matrix_WS_sum = dist_matrix_WS_sum[dist_matrix_WS_sum['rank']<=lag_nn]
    weather_dist_lag_initial = pd.merge(dist_matrix_WS_sum,WIND_table, 
                                       left_on ='lag_date', right_on = 'date' )
    weather_dist_lag_initial.rename(columns={'date_x': 'date'}, inplace=True)
    weather_dist_lag_initial = weather_dist_lag_initial.drop('date_y', axis=1)
    weather_dist_lag_initial['rank_no']=weather_dist_lag_initial['rank'].astype(str)+'lag'
    weather_dist_lag = pd.pivot_table(weather_dist_lag_initial, values=['endo_WIND'], 
                                                  index=['date','block_no'], columns=['rank_no']).reset_index()
    weather_dist_lag.columns = ['_'.join(col).strip() for col in weather_dist_lag.columns.values]
    weather_dist_lag.rename(columns={'date_':'date','block_no_':'block_no'},inplace = True)


    weather_dist_lag = pd.merge(WIND_only_table,weather_dist_lag,
                                how = 'left',
                                on = ['date','block_no'])


    weather_dist_lag.endo_WIND_8lag.fillna(weather_dist_lag.endo_WIND_9lag, inplace=True)
    weather_dist_lag.endo_WIND_7lag.fillna(weather_dist_lag.endo_WIND_8lag, inplace=True)
    weather_dist_lag.endo_WIND_6lag.fillna(weather_dist_lag.endo_WIND_7lag, inplace=True)
    weather_dist_lag.endo_WIND_5lag.fillna(weather_dist_lag.endo_WIND_6lag, inplace=True)
    weather_dist_lag.endo_WIND_4lag.fillna(weather_dist_lag.endo_WIND_5lag, inplace=True)
    weather_dist_lag.endo_WIND_3lag.fillna(weather_dist_lag.endo_WIND_4lag, inplace=True)
    weather_dist_lag.endo_WIND_2lag.fillna(weather_dist_lag.endo_WIND_3lag, inplace=True)
    weather_dist_lag.endo_WIND_1lag.fillna(weather_dist_lag.endo_WIND_2lag, inplace=True)
    t=0.8
    w1=1
    w2=t
    w3=t**2
    w4=t**3
    w5=t**4
    w6=t**5
    w7=t**6
    w8=t**7
    w9=t**8

    weather_dist_lag['endo_pred_sim_day_WS_sum']= (weather_dist_lag['endo_WIND_1lag']*w1+
                                                 weather_dist_lag['endo_WIND_2lag']*w2+
                                                 weather_dist_lag['endo_WIND_3lag']*w3+
                                                 weather_dist_lag['endo_WIND_4lag']*w4+
                                                 weather_dist_lag['endo_WIND_5lag']*w5+
                                                 weather_dist_lag['endo_WIND_6lag']*w6+
                                                 weather_dist_lag['endo_WIND_7lag']*w7+
                                                 weather_dist_lag['endo_WIND_8lag']*w8+
                                                 weather_dist_lag['endo_WIND_9lag']*w9
                                                 )\
                                                 /(w1+w2+w3+w4+w5+w6+w7+w8+w9)
        
    WIND_forecast_nn_WS_sum = weather_dist_lag[['date',
                                      'block_no',
                                      'endo_pred_sim_day_WS_sum']]
    # WIND_forecast_nn_WS_sum.to_sql(name='WIND_forecast_nn_WS_sum_{}'.format(discom), con=engine,  if_exists='replace')


    # In[124]:

    WIND_forecast_composit = pd.merge(pd.merge(pd.merge(WIND_forecast_nn_WS,
                                             WIND_forecast_nn_temp_sum,
                                             how = 'left',
                                             on = ['date','block_no']),
                                             WIND_forecast_nn_sum,
                                             how = 'left',
                                             on = ['date','block_no']),
                                             WIND_forecast_nn_WS_sum,
                                             how = 'left',
                                             on = ['date','block_no'])


    WIND_forecast_composit['final_forecast'] = WIND_forecast_composit[[
            'endo_pred_sim_day_WIND_WS_hourly',
            'endo_pred_sim_day_sum',
            'endo_pred_sim_day_WS_sum'
        ]].max(axis = 1)


    # In[128]:

    # list(WIND_forecast_composit)


    # In[ ]:

    Pred_table_wind_NN = WIND_forecast_composit[['date', 'block_no', 'final_forecast']]
    Pred_table_wind_NN.rename(columns={'final_forecast': 'wind_gen_forecast'}, inplace=True)
    Pred_table_wind_NN['discom_name'] = discom
    Pred_table_wind_NN['state'] = state
    Pred_table_wind_NN['revision'] = 0
    Pred_table_wind_NN['model_name'] = 'NN'
    Pred_table_wind_NN['pool_name'] = 'INT_GENERATION_FOR'
    Pred_table_wind_NN['pool_type'] = 'WIND'

    tablename = 'Pred_table_wind_NN_{}'.format(discom)
    Pred_table_wind_NN.to_sql(con=engine, name=tablename, if_exists='replace', flavor='mysql', index = False)

    sql_str = """insert into power.gen_forecast_stg
                (date, state, revision, org_name,
                pool_name, pool_type, entity_name,
                block_no, gen_forecast, Model_name)
                select a.date, a.state, a.revision, a.org_name, a.pool_name,
                 a.pool_type, a.entity_name, a.block_no,
                 round(a.gen_forecast,3),a.model_name
                 from {} a
                on duplicate key
                update gen_forecast = round(values(gen_forecast),3),
                load_date = NULL""".format(tablename)

    connection = engine.connect()
    connection.execute(sql_str)
    connection.close()
    return

config = 'mysql+mysqldb://root:quenext@2016@104.155.225.29/power'
discom = "GUVNL"
state = "GUJARAT"
forecast_wind_nn(config, discom, state)

# coding: utf-8

# In[166]:

from __future__ import division
from sqlalchemy import create_engine
import pandas as pd
pd.options.mode.chained_assignment = None
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

# 104.155.225.29

def forecast_solar_nn(config, discom, state):
    # discom = "ADANI"discom
    # state = "TAMIL NADU"
    # plant_name
    engine = create_engine(config, echo=False)

    if time.tzname[0] == 'IST':
        max_hour = dt.datetime.today().hour
    else:
        dest_tz = pytz.timezone('Asia/Kolkata')
        ts = time.time()
        utc_now = datetime.utcfromtimestamp(ts)
        local_now = utc_now.replace(tzinfo=pytz.utc).astimezone(dest_tz)
        max_hour = local_now.hour

    max_hour = max(max_hour,8)

    # max_hour = 8


    # In[2]:

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
                       'rainfall_mm': 'RainMM',  }, inplace=True)
    weather_data['year'] = pd.DatetimeIndex(weather_data['date']).year
    weather_data['month'] = pd.DatetimeIndex(weather_data['date']).month   # jan = 1, dec = 12



    # In[3]:

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


    # In[4]:

    def getEnvelopeModels(aTimeSeries, delta , rejectCloserThan = 0):   
        #Prepend the first value of (s) to the interpolating values. This forces the model to use the same starting point for both the upper and lower envelope models.    
        u_x = [0,]
        u_y = [aTimeSeries[0],]    
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


    # In[5]:

    solar_generation_table = pd.read_sql_query("""select generator_name, date, block_no, generation  
                                 from generation_staging where pool_type = 'solar'and discom = '{}'""".format(discom), engine, index_col = None)


    # In[6]:

    solar_generation_table['date'] = pd.to_datetime(solar_generation_table['date'])

    solar_generation_table.sort_values(by=[ 'generator_name', 'date','block_no'], 
                                      ascending=[True, True, True], inplace=True)


    # In[7]:

    solar_generation = solar_generation_table.groupby(['date', 'block_no'],as_index=False).agg({'generation':{'sum':'sum'
                                                                                                           }})
    solar_generation.columns = ['_'.join(col).strip() for col in solar_generation.columns.values]
    solar_generation.rename(columns={'date_': 'date', 'block_no_' : 'block_no'}, inplace=True)
    solar_generation['generation_MW'] = solar_generation['generation_sum']*4
    solar_generation['generation_MW'] = np.where((solar_generation['generation_MW'] >0),
                                                solar_generation['generation_MW'],0)


    # In[8]:

    # weather data preparation


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


    # In[9]:

    solar_table_initial = solar_generation.copy()
    solar_table_initial.sort_values(by=['date','block_no'], ascending=[True, True], inplace=True)


    # In[10]:


    unique_days_imp = solar_table_initial['date'].unique()

    nn_days = 90
    dist_matrix = pd.DataFrame([])
    for j in xrange(0,len(unique_days_imp)):    
        solar_table_na = solar_table_initial[solar_table_initial['date']==unique_days_imp[j]]
        solar_table_na = solar_table_initial[np.isfinite(solar_table_initial['generation_MW'])]
        block_unique = solar_table_initial['block_no'].unique()

        solar_gen_window = solar_table_initial[(solar_table_initial['date']<= unique_days_imp[j]) 
                                    & (solar_table_initial['date']>= pd.to_datetime(unique_days_imp[j])
                                       - timedelta(days=nn_days))]
        solar_gen_window = solar_gen_window[solar_gen_window['block_no'].isin(block_unique)]

        solar_gen_window_sum =  solar_gen_window.groupby(['date'],as_index=False).agg({'generation_MW':{'mean':'mean',                                                                                        'max':'max',
                                                                                             'min':'min',
                                                                                            'max':'max'}}) 
        solar_gen_window_sum.columns = ['_'.join(col).strip() for col in solar_gen_window_sum.columns.values]
        solar_gen_window_sum.rename(columns={'date_': 'date'}, inplace=True)
        solar_gen_window_sum['generation_MW_max_rank']=solar_gen_window_sum['generation_MW_max'].rank(ascending=1)
        solar_gen_window_sum['generation_MW_min_rank']=solar_gen_window_sum['generation_MW_min'].rank(ascending=1)
        solar_gen_window_sum['genneration_MW_mean_rank']=solar_gen_window_sum['generation_MW_mean'].rank(ascending=1)
        imp_day_solar_sum = solar_gen_window_sum[solar_gen_window_sum['date']==unique_days_imp[j]]
        nn_day_solar_sum = solar_gen_window_sum[solar_gen_window_sum['date']< unique_days_imp[j]]
        unique_nn_days = nn_day_solar_sum['date'].unique()
        for i in xrange(0,len(unique_nn_days)):
            test_nn = nn_day_solar_sum[nn_day_solar_sum['date']==unique_nn_days[i]]
            var_temp =[col for col in nn_day_solar_sum.columns if '_rank'in col ]
            coordinate_NN = np.array(test_nn[var_temp])
            coordimate_self = np.array(imp_day_solar_sum[var_temp])
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
    lag_solar_gen = pd.merge(dist_matrix ,solar_table_initial,
                        left_on = 'lag_date', right_on = 'date')
    lag_solar_gen = lag_solar_gen.rename(columns={'date_x':'date',
                                        'generation_MW':'generation_MW_lag'})
    del lag_solar_gen['date_y']



    lag_solar_gen = lag_solar_gen[['date', 'lag_date','rank','block_no','generation_MW_lag']]
    lag_solar_gen['rank_no']=lag_solar_gen['rank'].astype(str)+'rank'
    lag_solar_gen_pivot = pd.pivot_table(lag_solar_gen, values=['generation_MW_lag'], 
                                                  index=['date','block_no'], columns=['rank_no']).reset_index()
    lag_solar_gen_pivot.columns = ['_'.join(col).strip() for col in lag_solar_gen_pivot.columns.values]

    lag_solar_gen_pivot.rename(columns={'date_': 'date',
                                      'block_no_':'block_no'}, inplace=True)

    t=0.45
    w1=1
    w2=t
    w3=t**2
    w4=t**3
    w5=t**4
    w6=t**5
    w7=t**6

    lag_solar_gen_pivot['imp_solar_gen']= ((lag_solar_gen_pivot['generation_MW_lag_1rank']*w1+
                                 lag_solar_gen_pivot['generation_MW_lag_2rank']*w2+
                                 lag_solar_gen_pivot['generation_MW_lag_3rank']*w3+
                                  lag_solar_gen_pivot['generation_MW_lag_4rank']*w4+
                                  lag_solar_gen_pivot['generation_MW_lag_5rank']*w5+
                                 lag_solar_gen_pivot['generation_MW_lag_6rank']*w6+
                                 lag_solar_gen_pivot['generation_MW_lag_7rank']*w7)
                                /(w1+w2+w3+w4+w5+w6+w7))



    solar_gen_imp_temp = lag_solar_gen_pivot[['date','block_no','imp_solar_gen']]

    solar_gen_imp = pd.merge(solar_table_initial,solar_gen_imp_temp,
                        how = 'left',
                        on = ['date','block_no'])

    solar_gen_imp['generation_MW'].fillna(solar_gen_imp['imp_solar_gen'], inplace = True)


    # In[11]:

    unique_date = solar_gen_imp['date'].unique()
    smooth_solar_curve = pd.DataFrame([])
    for j in xrange(0, len(unique_date)):
        signal = solar_gen_imp[solar_gen_imp['date']==unique_date[j]]
        med_filt = pd.DataFrame(medfilt(signal['generation_MW'], 7))
        med_filt = med_filt.rename(columns={0:'median_filter'})
        med_filt['date'] = unique_date[j]
        med_filt['block_no'] = range(1,len(signal)+1)
        smooth_solar_curve= smooth_solar_curve.append(med_filt)     
    smooth_solar_curve.sort_values(by = ['date','block_no'], ascending=[True, True], inplace=True)
    solar_curve_filtered = pd.merge(solar_gen_imp,smooth_solar_curve, how = 'left', 
                               on = ['date','block_no'])


    # In[12]:

    unique_date = solar_curve_filtered['date'].unique()
    solar_envelop = pd.DataFrame([])
    for j in xrange(1, len(unique_date)):
        test = solar_curve_filtered[solar_curve_filtered['date']==unique_date[j]]
        s = np.array(test['median_filter'])
        P = getEnvelopeModels(s, delta =0, rejectCloserThan = 3)
    #     P = getEnvelopeModels(s, delta =1, rejectCloserThan = 0)
        
        q_u = map(P[0],xrange(0,len(s)))
        q_l = map(P[1],xrange(0,len(s)))
        test = test[['date','block_no','generation_MW','median_filter']].reset_index()
        U_envelop = pd.DataFrame(q_u)
        U_envelop = U_envelop.rename(columns={0: 'U_envelop'})
        L_envelop = pd.DataFrame(q_l)
        L_envelop = L_envelop.rename(columns={0: 'L_envelop'})
        envelop = pd.concat([test, U_envelop,L_envelop], axis = 1)
        solar_envelop = solar_envelop.append(envelop)
    solar_envelop['envelop'] = solar_envelop[['L_envelop','U_envelop']].mean(axis=1)
    solar_envelop['endo_gen'] = np.where((solar_envelop['envelop'] <=0),0,
                                       solar_envelop['envelop']) 


    # In[13]:


    solar_envelop['deviation'] = solar_envelop['generation_MW']- solar_envelop['endo_gen']

    solar_envelop['deviation_rolling']=solar_envelop['deviation'].rolling(window = 5, center = True).mean()

    solar_envelop['deviation_rolling'] = np.where((solar_envelop['deviation_rolling'] > 0),
                                                       solar_envelop['deviation_rolling'],0)
    solar_envelop['endo_gen'] = solar_envelop['endo_gen'] + solar_envelop['deviation_rolling']
    solar_table = solar_envelop[['date','block_no', 'generation_MW', 'endo_gen']] 
    solar_table['endo_gen'] = np.where((solar_envelop['generation_MW'] >0),
                                       solar_table['endo_gen'],0) 


    # In[14]:

    solar_table['hour'] = np.ceil(solar_table['block_no']/4)
    solar_table['year'] = pd.DatetimeIndex(solar_table['date']).year
    solar_table['month'] = pd.DatetimeIndex(solar_table['date']).month   # jan = 1, dec = 12
    solar_table['dayofweek'] = pd.DatetimeIndex(solar_table['date']).dayofweek # Monday=0, Sunday=6
    solar_table.sort_values(by = ['date','block_no'], ascending=[True, True], inplace=True)


    # In[15]:


    solar_only_table = solar_table[['date','block_no','endo_gen','generation_MW']]
    last_date_block = solar_only_table[solar_only_table['date']== max(solar_only_table['date'])]
    max_block = max(last_date_block['block_no'])
    columns = [['block_no','endo_gen']]

    if max_block < 96:
        forecast_period0 = pd.DataFrame(columns=columns)
        forecast_period0['block_no']=range(max_block+1, 97)
        forecast_period0['date'] =max(solar_only_table['date']) 
        forecast_period0 = forecast_period0[['date','block_no','endo_gen']]    
    else:
        forecast_period0 = pd.DataFrame(columns=columns)
        forecast_period0['block_no']=range(1, 97)
        forecast_period0['date'] =max(solar_only_table['date']) + pd.DateOffset(1)
        forecast_period0 = forecast_period0[['date','block_no','endo_gen']]

        
    forecast_period = pd.DataFrame([])
    for j in xrange(1, 8):
        period = pd.DataFrame(columns=columns)
        period['block_no']=range(1, 97)
        period['date'] =max(forecast_period0['date']) + pd.DateOffset(j)
        period = period[['date','block_no','endo_gen']]
        forecast_period = forecast_period.append(period)
        
    forecast_period_date = pd.concat([forecast_period0, forecast_period] , axis =0)

    solar_only_table = pd.concat([solar_only_table, forecast_period_date] , axis =0)

    non_missing_solar_date = pd.DataFrame(solar_only_table.date.unique())
    non_missing_solar_date.rename(columns={0 : 'date'}, inplace=True)
    non_missing_solar_date['date'] = pd.to_datetime(non_missing_solar_date['date'])
    non_missing_solar_date.sort_values(by=['date'], ascending=[False], inplace=True)
    non_missing_solar_date['date_key'] = range(0, len(non_missing_solar_date))
    date_key = non_missing_solar_date[['date', 'date_key']]   
    # weather_summary_pivot['date'] = pd.to_datetime(weather_summary_pivot['date'])
    # weather_summary_pivot.sort_values(by=['date'], ascending=[True], inplace=True)
    # weather_summary_nonmissing_solar = pd.merge(non_missing_solar_date, weather_summary_pivot, how = 'left', on = ['date'])


    # In[16]:

    weather_data_temp_block = weather_initial[weather_initial['hour']<= max_hour]
    weather_data_temp_block['hour_no']=weather_initial['hour'].astype(str)+'hour'

    weather_summary = weather_data_temp_block.groupby(['date', 'location'],as_index=False).agg({'temp_bin':{'max':'max',
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



    weather_hourly_pivot = pd.pivot_table(weather_data_temp_block, 
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


    # In[17]:

    # Weather Forecast based on summary temp 
    nn_days = 30
    lag_nn = 12
    lag_d = 0
    weather_all = weather_summary_pivot
    weather_relative = weather_summary_pivot
    weather_all.sort_values(by = ['date'], ascending = [True], inplace = True)
    weather_relative.sort_values(by = ['date'], ascending = [True], inplace = True)

    var_temp =[col for col in weather_all.columns 
                 if 'temp_bin' in col ]


    unique_date_all = weather_all['date'].unique()
    dist_matrix = pd.DataFrame([])
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
        dist_matrix = dist_matrix.append(dist_all)
    dist_matrix.sort_values(by =['date','eucledean_dist'], ascending = [True,True], inplace = True)    
    def ranker(dist_matrix):
        dist_matrix['rank'] = np.arange(len(dist_matrix)) + 1
        return dist_matrix

    dist_matrix = dist_matrix.groupby(dist_matrix['date']).apply(ranker)
    dist_matrix = dist_matrix[dist_matrix['rank']<=lag_nn]
    weather_dist_lag_initial = pd.merge(dist_matrix,solar_only_table, 
                                       left_on ='lag_date', right_on = 'date' )
    weather_dist_lag_initial.rename(columns={'date_x': 'date'}, inplace=True)
    weather_dist_lag_initial = weather_dist_lag_initial.drop('date_y', axis=1)
    weather_dist_lag_initial['rank_no']=weather_dist_lag_initial['rank'].astype(str)+'lag'
    weather_dist_lag = pd.pivot_table(weather_dist_lag_initial, values=['endo_gen'], 
                                                  index=['date','block_no'], columns=['rank_no']).reset_index()
    weather_dist_lag.columns = ['_'.join(col).strip() for col in weather_dist_lag.columns.values]
    weather_dist_lag.rename(columns={'date_':'date','block_no_':'block_no'},inplace = True)


    weather_dist_lag = pd.merge(solar_only_table,weather_dist_lag,
                                how = 'left',
                                on = ['date','block_no'])

    weather_dist_lag.endo_gen_8lag.fillna(weather_dist_lag.endo_gen_9lag, inplace=True)
    weather_dist_lag.endo_gen_7lag.fillna(weather_dist_lag.endo_gen_8lag, inplace=True)
    weather_dist_lag.endo_gen_6lag.fillna(weather_dist_lag.endo_gen_7lag, inplace=True)
    weather_dist_lag.endo_gen_5lag.fillna(weather_dist_lag.endo_gen_6lag, inplace=True)
    weather_dist_lag.endo_gen_4lag.fillna(weather_dist_lag.endo_gen_5lag, inplace=True)
    weather_dist_lag.endo_gen_3lag.fillna(weather_dist_lag.endo_gen_4lag, inplace=True)
    weather_dist_lag.endo_gen_2lag.fillna(weather_dist_lag.endo_gen_3lag, inplace=True)
    weather_dist_lag.endo_gen_1lag.fillna(weather_dist_lag.endo_gen_2lag, inplace=True)
    t=0.45
    w1=1
    w2=t
    w3=t**2
    w4=t**3
    w5=t**4
    w6=t**5
    w7=t**6
    w8=t**7
    w9=t**8

    weather_dist_lag['endo_pred_sim_day_solar_tempsum']= (weather_dist_lag['endo_gen_1lag']*w1+
                                                 weather_dist_lag['endo_gen_2lag']*w2+
                                                 weather_dist_lag['endo_gen_3lag']*w3+
                                                 weather_dist_lag['endo_gen_4lag']*w4+
                                                 weather_dist_lag['endo_gen_5lag']*w5+
                                                 weather_dist_lag['endo_gen_6lag']*w6+
                                                 weather_dist_lag['endo_gen_7lag']*w7+
                                                 weather_dist_lag['endo_gen_8lag']*w8+
                                                 weather_dist_lag['endo_gen_9lag']*w9
                                                 )\
                                                 /(w1+w2+w3+w4+w5+w6+w7+w8+w9)
    solar_forecast_nntempsum = weather_dist_lag[['date',
                                      'block_no',
                                      'generation_MW',
                                      'endo_gen',
                                      'endo_pred_sim_day_solar_tempsum']]
    solar_forecast_nntempsum.to_sql(name='solar_forecast_nntempsum_{}'.format(discom), con=engine,  if_exists='replace')


    # In[18]:


    weather_all = weather_hourly_pivot
    weather_relative = weather_hourly_pivot
    weather_all.sort_values(by = ['date'], ascending = [True], inplace = True)
    weather_relative.sort_values(by = ['date'], ascending = [True], inplace = True)


    var_temp =[col for col in weather_all.columns 
                 if 'temp_bin'in col ]
    nn_days = 45
    lag_nn = 12
    lag_d = 0
    unique_date_all = weather_all['date'].unique()
    dist_matrix = pd.DataFrame([])
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
        dist_matrix = dist_matrix.append(dist_all)
    dist_matrix.sort_values(by =['date','eucledean_dist'], ascending = [True,True], inplace = True)    
    def ranker(dist_matrix):
        dist_matrix['rank'] = np.arange(len(dist_matrix)) + 1
        return dist_matrix

    dist_matrix = dist_matrix.groupby(dist_matrix['date']).apply(ranker)
    dist_matrix = dist_matrix[dist_matrix['rank']<=lag_nn]
    weather_dist_lag_initial = pd.merge(dist_matrix,solar_only_table, 
                                       left_on ='lag_date', right_on = 'date' )
    weather_dist_lag_initial.rename(columns={'date_x': 'date'}, inplace=True)
    weather_dist_lag_initial = weather_dist_lag_initial.drop('date_y', axis=1)
    weather_dist_lag_initial['rank_no']=weather_dist_lag_initial['rank'].astype(str)+'lag'
    weather_dist_lag = pd.pivot_table(weather_dist_lag_initial, values=['endo_gen'], 
                                                  index=['date','block_no'], columns=['rank_no']).reset_index()
    weather_dist_lag.columns = ['_'.join(col).strip() for col in weather_dist_lag.columns.values]
    weather_dist_lag.rename(columns={'date_':'date','block_no_':'block_no'},inplace = True)


    weather_dist_lag = pd.merge(solar_only_table,weather_dist_lag,
                                how = 'left',
                                on = ['date','block_no'])
    weather_dist_lag.endo_gen_8lag.fillna(weather_dist_lag.endo_gen_9lag, inplace=True)
    weather_dist_lag.endo_gen_7lag.fillna(weather_dist_lag.endo_gen_8lag, inplace=True)
    weather_dist_lag.endo_gen_6lag.fillna(weather_dist_lag.endo_gen_7lag, inplace=True)
    weather_dist_lag.endo_gen_5lag.fillna(weather_dist_lag.endo_gen_6lag, inplace=True)
    weather_dist_lag.endo_gen_4lag.fillna(weather_dist_lag.endo_gen_5lag, inplace=True)
    weather_dist_lag.endo_gen_3lag.fillna(weather_dist_lag.endo_gen_4lag, inplace=True)
    weather_dist_lag.endo_gen_2lag.fillna(weather_dist_lag.endo_gen_3lag, inplace=True)
    weather_dist_lag.endo_gen_1lag.fillna(weather_dist_lag.endo_gen_2lag, inplace=True)
    t=0.45
    w1=1
    w2=t
    w3=t**2
    w4=t**3
    w5=t**4
    w6=t**5
    w7=t**6
    w8=t**7
    w9=t**8

    weather_dist_lag['endo_pred_sim_day_gen_temphourly']= (weather_dist_lag['endo_gen_1lag']*w1+
                                                 weather_dist_lag['endo_gen_2lag']*w2+
                                                 weather_dist_lag['endo_gen_3lag']*w3+
                                                 weather_dist_lag['endo_gen_4lag']*w4+
                                                 weather_dist_lag['endo_gen_5lag']*w5+
                                                 weather_dist_lag['endo_gen_6lag']*w6+
                                                 weather_dist_lag['endo_gen_7lag']*w7+
                                                 weather_dist_lag['endo_gen_8lag']*w8+
                                                 weather_dist_lag['endo_gen_9lag']*w9
                                                 )\
                                                 /(w1+w2+w3+w4+w5+w6+w7+w8+w9)
        
    solar_forecast_nntemp = weather_dist_lag[['date',
                                              'block_no',
                                              'endo_pred_sim_day_gen_temphourly']]
    solar_forecast_nntemp.to_sql(name='solar_forecast_nntemp_{}'.format(discom), con=engine,  if_exists='replace')


    # In[19]:

    #  solar forecast based on rain

    var_temp =[col for col in weather_all.columns 
                 if 'rain'in col or 'temp' in col ]
    nn_days = 45
    lag_nn = 12
    lag_d = 0
    unique_date_all = weather_all['date'].unique()
    dist_matrix = pd.DataFrame([])
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
        dist_matrix = dist_matrix.append(dist_all)
    dist_matrix.sort_values(by =['date','eucledean_dist'], ascending = [True,True], inplace = True)    
    def ranker(dist_matrix):
        dist_matrix['rank'] = np.arange(len(dist_matrix)) + 1
        return dist_matrix

    dist_matrix = dist_matrix.groupby(dist_matrix['date']).apply(ranker)
    dist_matrix = dist_matrix[dist_matrix['rank']<=lag_nn]
    weather_dist_lag_initial = pd.merge(dist_matrix,solar_only_table, 
                                       left_on ='lag_date', right_on = 'date' )
    weather_dist_lag_initial.rename(columns={'date_x': 'date'}, inplace=True)
    weather_dist_lag_initial = weather_dist_lag_initial.drop('date_y', axis=1)
    weather_dist_lag_initial['rank_no']=weather_dist_lag_initial['rank'].astype(str)+'lag'
    weather_dist_lag = pd.pivot_table(weather_dist_lag_initial, values=['endo_gen'], 
                                     index=['date','block_no'], 
                                     columns=['rank_no']).reset_index()
    weather_dist_lag.columns = ['_'.join(col).strip() for col in weather_dist_lag.columns.values]
    weather_dist_lag.rename(columns={'date_':'date','block_no_':'block_no'},inplace = True)


    weather_dist_lag = pd.merge(solar_only_table,weather_dist_lag,
                                how = 'left',
                                on = ['date','block_no'])
    weather_dist_lag.endo_gen_8lag.fillna(weather_dist_lag.endo_gen_9lag, inplace=True)
    weather_dist_lag.endo_gen_7lag.fillna(weather_dist_lag.endo_gen_8lag, inplace=True)
    weather_dist_lag.endo_gen_6lag.fillna(weather_dist_lag.endo_gen_7lag, inplace=True)
    weather_dist_lag.endo_gen_5lag.fillna(weather_dist_lag.endo_gen_6lag, inplace=True)
    weather_dist_lag.endo_gen_4lag.fillna(weather_dist_lag.endo_gen_5lag, inplace=True)
    weather_dist_lag.endo_gen_3lag.fillna(weather_dist_lag.endo_gen_4lag, inplace=True)
    weather_dist_lag.endo_gen_2lag.fillna(weather_dist_lag.endo_gen_3lag, inplace=True)
    weather_dist_lag.endo_gen_1lag.fillna(weather_dist_lag.endo_gen_2lag, inplace=True)

    t=0.45
    w1=1
    w2=t
    w3=t**2
    w4=t**3
    w5=t**4
    w6=t**5
    w7=t**6
    w8=t**7
    w9=t**8

    weather_dist_lag['endo_pred_sim_day_gen_rain']= (weather_dist_lag['endo_gen_1lag']*w1+
                                                 weather_dist_lag['endo_gen_2lag']*w2+
                                                 weather_dist_lag['endo_gen_3lag']*w3+
                                                 weather_dist_lag['endo_gen_4lag']*w4+
                                                 weather_dist_lag['endo_gen_5lag']*w5+
                                                 weather_dist_lag['endo_gen_6lag']*w6+
                                                 weather_dist_lag['endo_gen_7lag']*w7+
                                                 weather_dist_lag['endo_gen_8lag']*w8+
                                                 weather_dist_lag['endo_gen_9lag']*w9
                                                 )\
                                                 /(w1+w2+w3+w4+w5+w6+w7+w8+w9)
        
    solar_forecast_nnrain = weather_dist_lag[['date',
                                              'block_no',
                                              'endo_pred_sim_day_gen_rain']]
    # solar_forecast_nnrain.to_sql(name='solar_forecast_nnrain_{}'.format(discom), con=engine,  if_exists='replace')


    # In[20]:


    solar_forecast_nncomposite = pd.merge(pd.merge(solar_forecast_nntempsum,
                                                   solar_forecast_nntemp, 
                                                   how = 'left', 
                                                   on= ['date','block_no']),
                                                   solar_forecast_nnrain,
                                                   how = 'left',
                                                   on=['date','block_no'])


    # In[21]:

    solar_forecast_nncomposite['solar_forecast_NN'] = solar_forecast_nncomposite[[
                                                        'endo_pred_sim_day_solar_tempsum',
                                                        'endo_pred_sim_day_gen_temphourly',
                                                        'endo_pred_sim_day_gen_rain'
                                                        ]].median(axis=1)

    solar_forecast_nncomposite.to_sql(con=engine, name='solar_forecast_nncomposite_{}'.format(discom), if_exists='replace', flavor='mysql', index = False)


    # In[22]:

    test = solar_forecast_nncomposite.copy()
    test['mape1'] = abs((test['generation_MW']/test['endo_pred_sim_day_solar_tempsum'])-1)
    test['mape2'] = abs((test['generation_MW']/test['endo_pred_sim_day_gen_temphourly'])-1)
    test['mape3'] = abs((test['generation_MW']/test['endo_pred_sim_day_gen_rain'])-1)
    test['mape4'] = abs((test['generation_MW']/test['solar_forecast_NN'])-1)



    test['year'] = pd.DatetimeIndex(test['date']).year
    test = test[test['endo_gen']>10]
    # test = test[test['endo_gen']<1200]
    # test = test[test['mape1'] < 0.9]
    # test = test[test['mape1']<=0.20]
    test = test[np.isfinite(test['mape1'])]
    test['mape1'].describe(),test['mape2'].describe(),test['mape3'].describe(), test['mape4'].describe()


    # In[23]:


    # gen_forecast_stg
    # date, state, revision, org_name, pool_name, pool_type, entity_name, block_no, gen_forecast, Model_name

    Pred_table_solar_NN = solar_forecast_nncomposite[['date' , 'block_no', 'solar_forecast_NN']]
    Pred_table_solar_NN.rename(columns={'solar_forecast_NN': 'gen_forecast'}, inplace=True)
    Pred_table_solar_NN['org_name']=discom
    Pred_table_solar_NN['pool_name']='INT_GENERATION_FOR'
    Pred_table_solar_NN['pool_type']='SOLAR'
    Pred_table_solar_NN['entity_name']=discom
    Pred_table_solar_NN['state']= state
    Pred_table_solar_NN['revision']=0
    Pred_table_solar_NN['model_name']='NN'


    tablename = 'Pred_table_solar_NN_{}'.format(discom)
    Pred_table_solar_NN.to_sql(con=engine, name=tablename, if_exists='replace', flavor='mysql', index = False)

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

# config = 'mysql+mysqldb://root:quenext@2016@104.155.225.29/power'
# discom = "ADANI"
# state = "TAMIL NADU"
# solar_forecast(config, discom, state)

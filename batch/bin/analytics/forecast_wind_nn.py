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
from datetime import timedelta
import datetime as dt
import pytz
import time
from scipy.signal import medfilt
from scipy import *
from scipy.signal import *
from itertools import *
import scipy


def forecast_wind_nn(config, discom, state):
    re_type = "WIND"
    engine = create_engine(config, echo=False)

    if time.tzname[0] == 'IST':
        max_hour = dt.datetime.today().hour
    else:
        dest_tz = pytz.timezone('Asia/Kolkata')
        ts = time.time()
        utc_now = dt.datetime.utcfromtimestamp(ts)
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


    # In[3]:


    def savitzky_golay(y, window_size, order, deriv=0, rate=1):

        import numpy as np
        from math import factorial

        try:
            window_size = np.abs(np.int(window_size))
            order = np.abs(np.int(order))
        except ValueError:
            raise ValueError("window_size and order have to be of type int")
        if window_size % 2 != 1 or window_size < 1:
            raise TypeError("window_size size must be a positive odd number")
        if window_size < order + 2:
            raise TypeError("window_sizfe is too small for the polynomials order")
        order_range = range(order+1)
        half_window = (window_size -1) // 2
        # precompute coefficients
        b = np.mat([[k**i for i in order_range] for k in range(-half_window, half_window+1)])
        m = np.linalg.pinv(b).A[deriv] * rate**deriv * factorial(deriv)
        # pad the signal at the extremes with
        # values taken from the signal itself
        firstvals = y[0] - np.abs( y[1:half_window+1][::-1] - y[0] )
        lastvals = y[-1] + np.abs(y[-half_window-1:-1][::-1] - y[-1])
        y = np.concatenate((firstvals, y, lastvals))
        return np.convolve( m[::-1], y, mode='valid')


    # In[4]:


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


    # In[5]:


    def getEnvelopeModels(aTimeSeries, delta , rejectCloserThan = 0):   
        #Prepend the first value of (s) to the interpolating values. This forces the model to use the same starting point for both the upper and lower envelope models.    
        u_x = [0,]
        u_y = [aTimeSeries[0],]    
        lastPeak = 0;
        
        l_x = [0,]
        l_y = [aTimeSeries[0],]
        lastTrough = 0;
        
        #Detect peaks and troughs and mark their location in u_x,u_y,l_x,l_y respectively.    
        for k in range(1,len(aTimeSeries)- delta):
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


    # In[6]:


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


    # In[7]:


    WIND_generation_table = pd.read_sql_query("""select generator_name, date, block_no, generation  
                                 from generation_staging where pool_type = '{}' and discom = '{}'""".format(re_type,discom), engine, index_col = None)
    WIND_generation_table['date'] = pd.to_datetime(WIND_generation_table['date'])

    WIND_generation_table.sort_values(by=[ 'generator_name', 'date','block_no'], 
                                      ascending=[True, True, True], inplace=True)


    # In[8]:


    wind_unit = pd.read_sql_query("""

    select a.unit_master_pk, a.unit_name,
    # a.cogeneration_ind, 
    a.rated_unit_capacity, a.latitude, a.longitude,
    a.cogeneration_ind, 
    round(a.min_generation_Level * a.rated_unit_capacity,0) min_cap,
    round(a.max_generation_Level * a.rated_unit_capacity,0) max_cap,
    round(b.ramp_up_rate * rated_unit_capacity,0) ramp_up_mw_min, 
    round(b.ramp_down_rate * rated_unit_capacity,0) ramp_down_mw_min,
    round(b.from_capacity_perc * Rated_Unit_Capacity,0) cap_low_limit,
    round(b.to_capacity_perc * Rated_Unit_Capacity,0) cap_upper_limit
    from power.unit_master a
    left join
    unit_master_lines b
    on (a.unit_master_pk = b.unit_master_fk),
    power.unit_type c
    where c.unit_type_pk = a.unit_type_fk
    and c.unit_type_name = 'WIND'""",engine, index_col = None)


    # In[9]:


    # weather data preparation



    weather_actual_IMP = weather_data[weather_data['data_type']=='ACTUAL']
    weather_actual_IMP.rename(columns={'temp': 'temp_actual', 
                                                'windspeed': 'windspeed_actual',
                                                'RainMM': 'RainMM_actual',  }, inplace=True)



    weather_forecast_IMP = weather_data[weather_data['data_type']=='FORECAST']
    weather_forecast_IMP.rename(columns={'temp': 'temp_forecast', 
                                                'windspeed': 'windspeed_forecast',
                                                'pop': 'pop_forecast',  }, inplace=True)



    weather_actual_IMP = weather_actual_IMP[[ 'location', 'date', 'hour','temp_actual','RainMM_actual','windspeed_actual']]

    weather_forecast_IMP = weather_forecast_IMP[[ 'location', 'date', 'hour','temp_forecast','pop_forecast','windspeed_forecast']]


    min_date = min(weather_actual_IMP['date'])

    weather_actual_IMP = weather_actual_IMP[weather_actual_IMP['date']>= min_date]
    weather_forecast_IMP = weather_forecast_IMP[weather_forecast_IMP['date']>= min_date]

    weather_actual_nonmissing_IMP = weather_actual_IMP[np.isfinite(weather_actual_IMP['temp_actual'])]
    weather_actual_nonmissing_IMP = weather_actual_nonmissing_IMP[np.isfinite(weather_actual_nonmissing_IMP['RainMM_actual'])]
    weather_actual_nonmissing_IMP = weather_actual_nonmissing_IMP[np.isfinite(weather_actual_nonmissing_IMP['windspeed_actual'])]


    weather_forecast_nonmissing_IMP = weather_forecast_IMP[np.isfinite(weather_forecast_IMP['temp_forecast'])]
    weather_forecast_nonmissing_IMP = weather_forecast_nonmissing_IMP[ np.isfinite(weather_forecast_nonmissing_IMP['pop_forecast'])]
    weather_forecast_nonmissing_IMP = weather_forecast_nonmissing_IMP[ np.isfinite(weather_forecast_nonmissing_IMP['windspeed_forecast'])]



    # binning of weather variables  actual 

    min_wind = min(weather_forecast_nonmissing_IMP['windspeed_forecast'])
    max_wind = max(weather_forecast_nonmissing_IMP['windspeed_forecast'])

    windspeed_bins = list(linspace(min_wind,max_wind,15))
    series = list(weather_forecast_nonmissing_IMP['windspeed_forecast'])
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
    weather_forecast_nonmissing_IMP['windspeed_bin_forecast'] = binned_windspeed.values



    windspeed_bins = list(linspace(min_wind,max_wind,15))
    series = list(weather_actual_nonmissing_IMP['windspeed_actual'])
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
    weather_actual_nonmissing_IMP['windspeed_bin_actual'] = binned_windspeed.values



    WS_table = pd.merge(weather_actual_nonmissing_IMP,
                        weather_forecast_nonmissing_IMP,
                       how = 'left',
                       on = ['location','date','hour']) 

    WS_table = WS_table[['location','date','hour','windspeed_actual','windspeed_forecast',
                         'windspeed_bin_actual',
                         'windspeed_bin_forecast']]

    WS_table = WS_table[np.isfinite(WS_table['windspeed_forecast'])]
    freq_act_fore_WS = pd.DataFrame([])
    unique_foecast_bin = WS_table['windspeed_bin_forecast'].unique()
    for j in range(0,len(unique_foecast_bin)):
        test = WS_table[WS_table['windspeed_bin_forecast']==unique_foecast_bin[j]]
        unique_actual_bin = test['windspeed_bin_actual'].unique()
        for i in range(0,len(unique_actual_bin)):
            test1 = test[test['windspeed_bin_actual']==unique_actual_bin[i]]
            ws_forecast_bin = unique_foecast_bin[j]
            ws_acual_bin = unique_actual_bin[i]
            count_actual = len(test1)
            count_forecast = len(test)
            freq_table = [[ws_forecast_bin, ws_acual_bin, count_actual, count_forecast]]
            freq_act_fore_WS = freq_act_fore_WS.append(freq_table)

    freq_act_fore_WS.rename(columns={0: 'ws_forecast_bin',
                                    1: 'ws_actual_bin',
                                    2:'count_actual',
                                    3:'count_forecast'}, inplace=True)
        
    freq_act_fore_WS['freq_wt'] = (freq_act_fore_WS['count_actual']/
                                               freq_act_fore_WS['count_forecast'])
    freq_act_fore_WS['freq_wtd_forecast'] = freq_act_fore_WS['freq_wt']*freq_act_fore_WS['ws_actual_bin']

    WS_WTD_SUM = ((freq_act_fore_WS.groupby(['ws_forecast_bin'],as_index=False)
                .agg({'freq_wtd_forecast':'sum'}))
                .rename(columns={'freq_wtd_forecast':'freq_wtd_forecast_sum'}))


    rain_bins = [0,1,5,10,20]
    series = list(weather_actual_nonmissing_IMP['RainMM_actual'])
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
    weather_actual_nonmissing_IMP['rain_bin_actual'] = binned_RainMM.values
    weather_actual_nonmissing_IMP['rain_bin_actual'] = np.where((weather_actual_nonmissing_IMP['RainMM_actual']>0),
                                                  weather_actual_nonmissing_IMP['rain_bin_actual'],0)

    pop_bins = [0,10,20,30,40,50]
    series = list(weather_forecast_nonmissing_IMP['pop_forecast'])
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
    weather_forecast_nonmissing_IMP['rain_bin_forecast'] = binned_pop.values
    weather_forecast_nonmissing_IMP['rain_bin_forecast'] = np.where((weather_forecast_nonmissing_IMP['pop_forecast']>0),
                                                  weather_forecast_nonmissing_IMP['rain_bin_forecast'],0)


    rain_table = pd.merge(weather_actual_nonmissing_IMP,
                        weather_forecast_nonmissing_IMP,
                       how = 'left',
                       on = ['location','date','hour']) 

    rain_table = rain_table[['location','date','hour','RainMM_actual','pop_forecast',
                         'rain_bin_actual',
                         'rain_bin_forecast']]

    rain_table = rain_table[np.isfinite(rain_table['pop_forecast'])]
    freq_act_fore_rain = pd.DataFrame([])
    unique_foecast_bin = rain_table['rain_bin_forecast'].unique()
    for j in range(0,len(unique_foecast_bin)):
        test = rain_table[rain_table['rain_bin_forecast']==unique_foecast_bin[j]]
        unique_actual_bin = test['rain_bin_actual'].unique()
        for i in range(0,len(unique_actual_bin)):
            test1 = test[test['rain_bin_actual']==unique_actual_bin[i]]
            rain_forecast_bin = unique_foecast_bin[j]
            rain_acual_bin = unique_actual_bin[i]
            count_actual = len(test1)
            count_forecast = len(test)
            freq_table = [[rain_forecast_bin, rain_acual_bin, count_actual, count_forecast]]
            freq_act_fore_rain = freq_act_fore_rain.append(freq_table)

    freq_act_fore_rain.rename(columns={0: 'rain_forecast_bin',
                                    1: 'rain_acual_bin',
                                    2:'count_actual',
                                    3:'count_forecast'}, inplace=True)
        
    freq_act_fore_rain['freq_wt'] = (freq_act_fore_rain['count_actual']/
                                               freq_act_fore_rain['count_forecast'])
    freq_act_fore_rain['freq_wtd_forecast'] = freq_act_fore_rain['freq_wt']*freq_act_fore_rain['rain_acual_bin']

    RAIN_WTD_SUM = ((freq_act_fore_rain.groupby(['rain_forecast_bin'],as_index=False)
        .agg({'freq_wtd_forecast':'sum'}))
        .rename(columns={'freq_wtd_forecast':'freq_wtd_forecast_sum'}))


    # In[10]:


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

    windspeed_bins = list(linspace(min_wind,max_wind,20))
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

    temp_bins = list(linspace(min_temp,max_temp,40))
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

    windspeed_bins = list(linspace(min_wind,max_wind,15))
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

    weather_forecast_nonmissing = pd.merge(weather_forecast_nonmissing,
                                          WS_WTD_SUM,
                                          how = 'left',
                                          left_on = ['windspeed_bin'],
                                          right_on = ['ws_forecast_bin'])
    weather_forecast_nonmissing['windspeed_bin'] = weather_forecast_nonmissing['freq_wtd_forecast_sum']

    weather_forecast_nonmissing = weather_forecast_nonmissing[['location','date','hour',
                                                               'temp','pop','windspeed',
                                                               'windspeed_bin']]

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

    weather_forecast_nonmissing = pd.merge(weather_forecast_nonmissing,
                                          RAIN_WTD_SUM,
                                          how = 'left',
                                          left_on = ['pop_bin'],
                                          right_on = ['rain_forecast_bin'])
    weather_forecast_nonmissing['rain_forecast_bin'] = weather_forecast_nonmissing['freq_wtd_forecast_sum']

    weather_forecast_nonmissing = weather_forecast_nonmissing[['location','date','hour',
                                                               'temp','pop','windspeed',
                                                               'windspeed_bin','rain_forecast_bin']]

    temp_bin_summary = (weather_actual_nonmissing.groupby(['temp_bin'],as_index=False)
                        .agg({'temp':['min', 'max']}))
    temp_bin_summary.columns = ['_'.join(col).strip() for col in temp_bin_summary.columns.values]
    temp_bin_summary.rename(columns ={'temp_bin_' : 'temp_bin'}, inplace=True)

    min_temp = min(temp_bin_summary['temp_min'])
    max_temp = max(temp_bin_summary['temp_max'])
    min_temp_bin = min(temp_bin_summary['temp_bin'])
    max_temp_bin = max(temp_bin_summary['temp_bin'])
    UL = list(np.array(temp_bin_summary['temp_max']))
    LL = list(np.array(temp_bin_summary['temp_min']))
    temp_bin = list(np.array(temp_bin_summary['temp_bin']))          
    series = list(np.array(weather_forecast_nonmissing['temp']))
    cat_var = []
    for j in range(0,len(series)):
        for i in range(0,len(UL)):
            if series[j] < min_temp: 
                cat_var.append(min_temp_bin -1)
            elif i > 0 and series[j]>= LL[i] and series[j] <= UL[i]:
                cat_var.append(temp_bin[i])
            elif series[j] > max_temp:
                cat_var.append(max_temp_bin +1) 
    binned_temp = pd.Series(cat_var)
    weather_forecast_nonmissing['temp_bin'] = binned_temp.values


    weather_forecast_nonmissing = weather_forecast_nonmissing[['location','date','hour',
                                                               'temp_bin','windspeed_bin' 
                                                               ,'rain_forecast_bin']]
    weather_forecast_nonmissing.rename(columns={'temp_bin': 'temp_bin_forecast', 
                                                'windspeed_bin': 'windspeed_bin_forecast'}, inplace=True)


    max_date_actual  = max(weather_actual_nonmissing['date'])
    test  = weather_actual_nonmissing[weather_actual_nonmissing['date'] ==max_date_actual]
    max_hour = max(test['hour'])

    append_forecast1 = weather_forecast_nonmissing[weather_forecast_nonmissing['date'] == max_date_actual]
    append_forecast1 = append_forecast1[append_forecast1['hour']> max_hour]

    append_forecast2 = weather_forecast_nonmissing[weather_forecast_nonmissing['date'] > max_date_actual]
    append_forecast = pd.DataFrame([])
    append_forecast = append_forecast.append(append_forecast1)
    append_forecast = append_forecast.append(append_forecast2)

    append_forecast.rename(columns={'temp_bin_forecast': 'temp_bin', 
                                                'windspeed_bin_forecast': 'windspeed_bin',
                                                'rain_forecast_bin': 'rain_bin',  }, inplace=True)

    weather_actual = weather_actual_nonmissing[['location','date','hour',
                                                 'windspeed_bin','temp_bin','rain_bin']]

    weather_initial = pd.DataFrame([])
    weather_initial = weather_initial.append(weather_actual)
    weather_initial = weather_initial.append(append_forecast)
    weather_initial = weather_initial[['location', 'date', 'hour','temp_bin','rain_bin','windspeed_bin']]


    # In[11]:


    min_date = min(weather_initial['date'])
    max_date = max(weather_initial['date'])
    min_date, max_date
    days = int((max_date - min_date).days + 1)

    date = min_date
    block_no = range(1,97)
    block_no =  pd.Series(block_no)
    window_master = pd.DataFrame([])
    window_master['block_no'] = block_no.values
    window_master['date'] = min_date
    window_master['date'] = pd.to_datetime(window_master['date'])

    window_table = pd.DataFrame([])
    for j in range(0,days):
        test = window_master
        test['date']= pd.to_datetime(min_date.strftime("%m/%d/%Y")) + timedelta(days=j)
        window_table = window_table.append(test)
    window_table['hour']= np.ceil(window_table['block_no']/4)
    window_size = 4
    window_id =[]
    for j in range (1,len(window_table)+1):
        window_id.append( np.ceil(j/4))
    window= pd.DataFrame(window_id)
    window_id = list(window[0])
    window = pd.Series(window[0].values)
    window_table['window'] = window.values
    unique_window = window_table['window'].unique()
    y = []
    for j in range(0,len(unique_window)):
        for i in range(0,window_size):
            y.append(i+1)   
    block_id = pd.DataFrame(y)
    block = pd.Series(block_id[0].values)
    window_table['window_block']=block.values
    window_table['window_hour']=np.ceil(window_table['window_block']/4)


    # In[12]:


    unique_wind_gen= wind_unit['unit_master_pk'].unique()
    lat_long_dist = pd.DataFrame([])
    for i in range(0, len(unique_wind_gen)):
        for j in range(0, len(unique_wind_gen)):
            if j!=i:
                test = wind_unit[wind_unit['unit_master_pk']==unique_wind_gen[i]]
                test1 = wind_unit[wind_unit['unit_master_pk']==unique_wind_gen[j]]
                lon1 = test['longitude']
                lat1 = test['latitude']
                lon2 = test1['longitude']
                lat2 = test1['latitude']
                dist_km = haversine(lon1, lat1, lon2, lat2)
                unique_wind_gen_1 = unique_wind_gen[i]
                unique_wind_gen_2 = unique_wind_gen[j]
                dist_mat_latlong = [[unique_wind_gen_1,unique_wind_gen_2,dist_km]]
                lat_long_dist = lat_long_dist.append(dist_mat_latlong)
    lat_long_dist= lat_long_dist[np.isfinite(lat_long_dist[2])]  
    lat_long_dist.rename(columns={0: 'self', 1:'closest',2: 'distance'}, 
                         inplace=True)

    lat_long_dist.sort_values(by =['self','distance'], ascending = [True,True], inplace = True)    
    def ranker(lat_long_dist):
        lat_long_dist['rank'] = np.arange(len(lat_long_dist)) + 1
        return lat_long_dist
    lat_long_dist_ranked = lat_long_dist.groupby(lat_long_dist['self']).apply(ranker)

    lat_long_dist_ranked = lat_long_dist_ranked[lat_long_dist_ranked['rank']<=1]

    turbine_cap = pd.merge(lat_long_dist_ranked,
                           wind_unit,
                          how = 'left' , left_on = ['self'],
                                         right_on = ['unit_master_pk'])

    turbine_cap.rename(columns={'rated_unit_capacity': 'self_capacity'}, inplace=True)

    turbine_cap = turbine_cap[['self','closest','unit_name','self_capacity']]
    turbine_cap.rename(columns={'unit_name': 'self_name'}, inplace=True)

    turbine_cap_NN = pd.merge(turbine_cap,
                           wind_unit,
                          how = 'left' , left_on = ['closest'],
                                         right_on = ['unit_master_pk'])
    turbine_cap_NN.rename(columns={'rated_unit_capacity': 'NN_capacity'}, inplace=True)

    turbine_cap_NN = turbine_cap_NN[['self','closest','self_name', 'unit_name',
                                    'self_capacity', 'NN_capacity']]
    turbine_cap_NN.rename(columns={'unit_name': 'NN_name'}, inplace=True)



    capacity_gen_NN = pd.merge(WIND_generation_table,
                               turbine_cap_NN,
                              left_on = 'generator_name',
                              right_on = 'self_name')
    capacity_gen_NN.rename(columns={'generation': 'self_generation'}, inplace=True)
    capacity_gen_NN = capacity_gen_NN[['self','closest','self_name','NN_name', 
                        'self_capacity', 'NN_capacity',
                        'date','block_no','self_generation']]

    capacity_gen_self_nn = pd.merge(capacity_gen_NN,
                                    WIND_generation_table,
                                    left_on = ['NN_name','date','block_no'],
                                    right_on = ['generator_name','date','block_no'])
    capacity_gen_self_nn.rename(columns={'generation': 'NN_generation'}, inplace=True)

    capacity_gen_self_nn = capacity_gen_self_nn[['self','closest','self_name','NN_name', 
                                                 'self_capacity', 'NN_capacity',
                                                 'date','block_no','self_generation',
                                                'NN_generation']]

    capacity_gen_self_nn['self_generation'] = np.where((capacity_gen_self_nn['self_generation']<=0),0,
                                                      capacity_gen_self_nn['self_generation'])


    capacity_gen_self_nn['NN_generation'] = np.where((capacity_gen_self_nn['NN_generation']<=0),0,
                                                      capacity_gen_self_nn['NN_generation'])

    capacity_gen_self_nn['self_ratio'] = capacity_gen_self_nn['self_generation']/capacity_gen_self_nn['self_capacity']


    capacity_gen_self_nn['NN_ratio'] = capacity_gen_self_nn['NN_generation']/capacity_gen_self_nn['NN_capacity']

    capacity_gen_self_nn['available_capacity'] = np.where((capacity_gen_self_nn['NN_ratio'] > 
                                                          capacity_gen_self_nn['self_ratio'] ),
                                                          capacity_gen_self_nn['self_generation']/
                                                          capacity_gen_self_nn['NN_ratio'],
                                                          capacity_gen_self_nn['self_capacity'])



    # capacity_gen_self_nn['available_capacity'] = capacity_gen_self_nn['self_capacity']

    capacity_summary = (capacity_gen_self_nn.groupby(['date','block_no'],as_index=False)
                        .agg({'available_capacity':'sum',
                              'self_generation':'sum'}))
    capacity_summary.rename(columns ={'available_capacity' : 'available_capacity_sum',
                                      'self_generation':'self_generation_sum'}, inplace=True)



    capacity_summary.sort_values(by = ['date','block_no'], ascending = [True,True], inplace = True)
    capacity_summary['prev_day_capacity_sum'] = capacity_summary['available_capacity_sum'].shift(96) 
    capacity_summary['gen_MW'] = capacity_summary['self_generation_sum']*4


    daily_capacity_summary = (capacity_summary.groupby(['date'],as_index=False)
                        .agg({'available_capacity_sum':'mean',
                              'prev_day_capacity_sum':'mean',
                               'gen_MW':'mean'}))
    daily_capacity_summary.rename(columns ={'available_capacity_sum' : 'available_capacity_sum_mean',
                                            'prev_day_capacity_sum':'prev_day_capacity_sum_mean',
                                            'gen_MW':'gen_MW_mean' }, inplace=True)


    capacity_summary_final = pd.merge(capacity_summary,
                                     daily_capacity_summary,
                                     how = 'left',
                                     on = ['date'])


    # In[13]:


    # capacity_summary_final


    # In[14]:


    capacity_summary_final['per_gen'] = (capacity_summary_final['gen_MW']/
                                         capacity_summary_final['available_capacity_sum'])

    per_gen = capacity_summary_final[['date','block_no','per_gen','gen_MW',
                                      'available_capacity_sum',
                                      'prev_day_capacity_sum',                                  
                                      'available_capacity_sum_mean',
                                      'prev_day_capacity_sum_mean']]


    # In[15]:


    smooth_WIND_curve = per_gen.copy()
    y = list(smooth_WIND_curve['per_gen'])
    med_filt = medfilt(y, 5)
    s_gen = list(med_filt)
    s_gen = pd.Series(s_gen)
    smooth_WIND_curve['smooth_gen'] =s_gen.values 
    smooth_WIND_curve['envelop']  = smooth_WIND_curve['smooth_gen']
    WIND_curve_filtered = smooth_WIND_curve[['date','block_no','per_gen','envelop']]
    WIND_envelop = WIND_curve_filtered.copy()


    # In[16]:


    WIND_envelop['endo_WIND'] = WIND_envelop['envelop']
    WIND_table = WIND_envelop[['date','block_no', 'per_gen', 'endo_WIND']] 


    # In[17]:


    WIND_table['hour'] = np.ceil(WIND_table['block_no']/4)
    WIND_table['year'] = pd.DatetimeIndex(WIND_table['date']).year
    WIND_table['month'] = pd.DatetimeIndex(WIND_table['date']).month   # jan = 1, dec = 12
    WIND_table['dayofweek'] = pd.DatetimeIndex(WIND_table['date']).dayofweek # Monday=0, Sunday=6
    WIND_table.sort_values(by = ['date','block_no'], ascending=[True, True], inplace=True)


    # In[18]:


    WIND_only_table = WIND_table[['date','block_no','endo_WIND','per_gen']]
    last_date_block = WIND_only_table[WIND_only_table['date']== max(WIND_only_table['date'])]
    max_block = max(last_date_block['block_no'])
    columns = [['block_no','endo_WIND']]

    if max_block < 96:
        forecast_period0 = pd.DataFrame(columns=columns)
        forecast_period0['block_no']=range(max_block+1, 97)
        forecast_period0['date'] =max(WIND_only_table['date']) 
        forecast_period0 = forecast_period0[['date','block_no','endo_WIND']]    
    else:
        forecast_period0 = pd.DataFrame(columns=columns)
        forecast_period0['block_no']=range(1, 97)
        forecast_period0['date'] =max(WIND_only_table['date']) + pd.DateOffset(1)
        forecast_period0 = forecast_period0[['date','block_no','endo_WIND']]

        
    forecast_period = pd.DataFrame([])
    for j in range(1, 8):
        period = pd.DataFrame(columns=columns)
        period['block_no']=range(1, 97)
        period['date'] =max(forecast_period0['date']) + pd.DateOffset(j)
        period = period[['date','block_no','endo_WIND']]
        forecast_period = forecast_period.append(period)
        
    forecast_period_date = pd.concat([forecast_period0, forecast_period] , axis =0)

    WIND_only_table = pd.concat([WIND_only_table, forecast_period_date] , axis =0)



    # In[19]:


    WIND_only_table_window = pd.merge(WIND_only_table,
                                     window_table,
                                     how = 'left',
                                     on = ['date', 'block_no'])
    WIND_only_table_window['hour']= np.ceil(WIND_only_table_window['block_no']/4)

    WIND_only_table_window['window_hour']= np.ceil(WIND_only_table_window['window_block']/4)


    # In[20]:


    window_summary = (WIND_only_table_window.groupby(['window'],as_index=False)
                        .agg({'endo_WIND':'count'}))
    window_summary.rename(columns={'endo_WIND': 'endo_WIND_count'}, inplace=True)                      
    window_summary = window_summary[window_summary['endo_WIND_count']==window_size]
    unique_nonmissing_window = window_summary['window'].unique()


    # In[21]:


    window_hourmap = window_table[['date','hour','window','window_hour']].drop_duplicates()
    window_weather = pd.merge(weather_initial,
                              window_hourmap,
                              how = 'left',
                              on = ['date','hour'])

    window_weather['window_hour_no']=window_weather['window_hour'].astype(str)+'window_hour'
        
    weather_hourly_pivot = pd.pivot_table(window_weather, 
                                values=['temp_bin',
                                        'windspeed_bin',
                                        'rain_bin'
                                       ], 
                                index=['window'], 
                                columns=['location','window_hour_no']).reset_index()

    weather_hourly_pivot.columns = ['_'.join(col).strip() for col in weather_hourly_pivot.columns.values]
    weather_hourly_pivot.rename(columns={'window__': 'window'}, inplace=True)


    # In[22]:


    non_missing_windgen = WIND_only_table_window[np.isfinite(WIND_only_table_window['endo_WIND'])]


    # In[23]:


    weather_all = weather_hourly_pivot
    non_missing_window_relative = [x for x in non_missing_windgen['window'].unique() if x in 
                                          weather_hourly_pivot['window'].unique()]

    non_missing_windgen_count = WIND_only_table_window.groupby(['window'],as_index=False).agg({'endo_WIND':{'count':'count'}})
    non_missing_windgen_count.columns = ['_'.join(col).strip() for col in non_missing_windgen_count.columns.values]
    non_missing_windgen_count.rename(columns={'window_': 'window'}, inplace=True)
    non_missing_windgen_count = non_missing_windgen_count[non_missing_windgen_count['endo_WIND_count']==window_size]
    non_missing_endo_wind_window = non_missing_windgen_count['window'].unique()

    non_missing_window_final =    [x for x in non_missing_window_relative if x in 
                                          non_missing_endo_wind_window]


    weather_relative = weather_all[weather_all['window'].isin(non_missing_window_final)]
    weather_all.sort_values(by = ['window'], ascending = [True], inplace = True)
    weather_relative.sort_values(by = ['window'], ascending = [True], inplace = True)

    var_temp =[col for col in weather_hourly_pivot.columns 
                 if 'windspeed_bin'in col ]

    a = weather_all[var_temp]
    b = weather_relative[var_temp]
    window_all = weather_all['window'].unique()
    widow_relative = weather_relative['window'].unique()

    dist = scipy.spatial.distance.cdist(a,b,) # pick the appropriate distance metric 
    dist_matrix = pd.DataFrame(dist)
    dist_matrix.columns = widow_relative
    window_all = pd.Series(window_all)  
    dist_matrix['window'] = window_all.values

    # Memory Error Prevention
    dist_matrix = dist_matrix.astype('float32')
    dist_matrix = pd.melt(dist_matrix, id_vars = ['window'], value_name='eucledean_dist')
    dist_matrix.rename(columns={'variable': 'lag_window'}, inplace=True)
    dist_matrix = dist_matrix[dist_matrix['window'] > dist_matrix['lag_window']]
    dist_matrix.sort_values(by = ['window','eucledean_dist'], ascending = [True,True], inplace = True)

    def ranker(dist_matrix):
        dist_matrix['rank_dist'] = np.arange(len(dist_matrix)) + 1
        return dist_matrix

    # Memory Error Prevention
    dist_matrix = dist_matrix.astype('float32')
    dist_matrix = dist_matrix.groupby(dist_matrix['window']).apply(ranker)
    dist_matrix = dist_matrix[dist_matrix['rank_dist']<=12]
    dist_matrix = dist_matrix[np.isfinite(dist_matrix['eucledean_dist'])]


    # In[24]:


    weather_dist_lag_initial = pd.merge(dist_matrix,non_missing_windgen, 
                                       left_on ='lag_window', right_on = 'window' )
    weather_dist_lag_initial.rename(columns={'window_x': 'window'}, inplace=True)
    weather_dist_lag_initial = weather_dist_lag_initial.drop('window_y', axis=1)
    weather_dist_lag_initial['rank_no']=weather_dist_lag_initial['rank_dist'].astype(str)+'lag'
    weather_dist_lag = pd.pivot_table(weather_dist_lag_initial, values=['endo_WIND'], 
                                                  index=['window','window_block'], columns=['rank_no']).reset_index()
    weather_dist_lag.columns = ['_'.join(col).strip() for col in weather_dist_lag.columns.values]
    weather_dist_lag.rename(columns={'window_':'window','window_block_':'window_block'},inplace = True)


    weather_dist_lag = pd.merge(WIND_only_table_window,weather_dist_lag,
                                how = 'left',
                                on = ['window','window_block'])


    weather_dist_lag['endo_WIND_9lag'].fillna(weather_dist_lag['endo_WIND_10lag'], inplace = True)
    weather_dist_lag['endo_WIND_8lag'].fillna(weather_dist_lag['endo_WIND_9lag'], inplace = True)
    weather_dist_lag['endo_WIND_7lag'].fillna(weather_dist_lag['endo_WIND_8lag'], inplace = True)
    weather_dist_lag['endo_WIND_6lag'].fillna(weather_dist_lag['endo_WIND_7lag'], inplace = True)
    weather_dist_lag['endo_WIND_5lag'].fillna(weather_dist_lag['endo_WIND_6lag'], inplace = True)
    weather_dist_lag['endo_WIND_4lag'].fillna(weather_dist_lag['endo_WIND_5lag'], inplace = True)
    weather_dist_lag['endo_WIND_3lag'].fillna(weather_dist_lag['endo_WIND_4lag'], inplace = True)
    weather_dist_lag['endo_WIND_2lag'].fillna(weather_dist_lag['endo_WIND_3lag'], inplace = True)
    weather_dist_lag['endo_WIND_1lag'].fillna(weather_dist_lag['endo_WIND_2lag'], inplace = True)


    t=.25
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
                                      'per_gen',
                                      'endo_pred_sim_day_WIND_WS_hourly']]
    # # WIND_forecast_nn_WS.to_sql(name='WIND_forecast_nn_WS_{}'.format(discom), con=engine,  if_exists='replace')
    previsous_day_capacity = capacity_summary_final[['date','block_no','gen_MW','prev_day_capacity_sum_mean']]


    previsous_day_capacity['date'] = pd.to_datetime(previsous_day_capacity['date'])
    WIND_forecast_nn_WS['date'] = pd.to_datetime(WIND_forecast_nn_WS['date'])

    WIND_forecast_nn_WS_final = pd.merge(WIND_forecast_nn_WS,
                                        previsous_day_capacity,
                                        how = 'left',
                                        on = ['date','block_no'])

    WIND_forecast_nn_WS_final['prev_day_capacity_sum_mean'].fillna(method='ffill',inplace=True)

    WIND_forecast_nn_WS_final['simday_WIND_WS_MW_hourly'] = (WIND_forecast_nn_WS_final['prev_day_capacity_sum_mean']*
                                                             WIND_forecast_nn_WS_final['endo_pred_sim_day_WIND_WS_hourly'])


    # In[25]:


    smooth_WIND_forecast = WIND_forecast_nn_WS_final.copy()
    y = list(smooth_WIND_forecast['simday_WIND_WS_MW_hourly'])
    med_filt = medfilt(y, 9)
    s_gen = list(med_filt)
    f_gen = savitzky_golay(s_gen, window_size=21, order=1, deriv=0, rate=1)
    f_gen = list(f_gen)
    f_gen = pd.Series(f_gen)
    smooth_WIND_forecast['smooth_forecast'] =f_gen.values 
    smooth_WIND_forecast = smooth_WIND_forecast[['date','block_no','gen_MW','prev_day_capacity_sum_mean',
                                                 'per_gen', 'endo_pred_sim_day_WIND_WS_hourly',
                                                 'smooth_forecast']]


    # In[26]:


    Pred_table_wind_NN = smooth_WIND_forecast[['date' , 'block_no', 'smooth_forecast']]
    Pred_table_wind_NN.rename(columns={'smooth_forecast': 'gen_forecast'}, inplace=True)
    Pred_table_wind_NN['org_name']=discom
    Pred_table_wind_NN['pool_name']='INT_GENERATION_FOR'
    Pred_table_wind_NN['pool_type']='WIND'
    Pred_table_wind_NN['entity_name']=discom
    Pred_table_wind_NN['state']= state
    Pred_table_wind_NN['revision']=0
    Pred_table_wind_NN['model_name']='NN'



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

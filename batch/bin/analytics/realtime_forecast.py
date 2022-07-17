"""Realtime Forecast."""
from __future__ import division
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import math
from math import factorial
from datetime import timedelta
from numpy import sign
from scipy.interpolate import interp1d
from scipy.interpolate import UnivariateSpline
from datetime import datetime, timedelta
import datetime as dt
import pytz
import time
from scipy.signal import medfilt
from scipy import *
from scipy.signal import *
# import numpy as np
from scipy.interpolate import splev, splrep
from scipy.optimize import minimize
import scipy


def get_time():
    """Get the latest time in IST."""
    if time.tzname[0] == 'IST':
        localtm = datetime.today().time()
    else:
        dest_tz = pytz.timezone('Asia/Kolkata')
        ts = time.time()
        utc_now = datetime.utcfromtimestamp(ts)
        localtm = utc_now.replace(tzinfo=pytz.utc).astimezone(dest_tz).time()
    return localtm.strftime('%H:%M')


def savitzky_golay(y, window_size, order, deriv=0, rate=1):
    import numpy as np
    from math import factorial
    try:
        window_size = np.abs(np.int(window_size))
        order = np.abs(np.int(order))
    except ValueError as msg:
        raise ValueError("window_size and order have to be of type int")
    if window_size % 2 != 1 or window_size < 1:
        raise TypeError("window_size size must be a positive odd number")
    if window_size < order + 2:
        raise TypeError("window_size is too small for the polynomials order")
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


# In[3]
def logit_transform(x):
    pivot = 25
    multipler = 1.5
    beta = 0.4
    delta = 38
    T = np.where((x < pivot), x + (pivot - x) * multipler, x)
    logit_x = (np.exp(beta * (T - delta)) /
               (1 + np.exp(beta * (T - delta)))) * 100
    return logit_x

def haversine(lon1, lat1, lon2, lat2):
    from math import radians, cos, sin, asin, sqrt
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * asin(sqrt(a))
    km = 6367 * c
    return km

def guess(x, y, k, s, w=None):
    """Do an ordinary spline fit to provide knots."""
    return splrep(x, y, w, k=k, s=s)

def err(c, x, y, t, k, w=None):
    """The error function to minimize."""
    diff = y - splev(x, (t, c, k))
    if w is None:
        diff = np.einsum('...i,...i', diff, diff)
    else:
        diff = np.dot(diff * diff, w)
    return np.abs(diff)

def spline_neumann(x, y, k=3, s=0, w=None):
    t, c0, k = guess(x, y, k, s, w=w)
    x0 = x[0]  # point at which zero slope is required
    con = {'type': 'eq',
           'fun': lambda c: splev(x0, (t, c, k), der=1),
           # doesn't help, dunno why
           #  'jac': lambda c: splev(x0, (t, c, k), der=2)
           }
    opt = minimize(err, c0, (x, y, t, k, w), constraints=con)
    copt = opt.x
    return UnivariateSpline._from_tck((t, copt, k))

def get_revision(curblock):
    """Get Revision based on time."""
    from sortedcontainers import SortedDict

    # block_dict = SortedDict({2: (0, 7), 9: (0, 13), 15: (0, 19),
    #                          21: (0, 25), 27: (0, 31), 33: (0, 37),
    #                          39: (0, 43), 45: (0, 49), 51: (0, 55),
    #                          57: (0, 61), 63: (0, 67), 69: (0, 73),
    #                          75: (0, 79), 81: (0, 85), 87: (0, 91),
    #                          93: (1, 1)})
    block_dict = SortedDict({4: (0, 1), 9: (0, 13), 15: (0, 19),
                             21: (0, 25), 27: (0, 31), 33: (0, 37),
                             39: (0, 43), 45: (0, 49), 51: (0, 55),
                             57: (0, 61), 63: (0, 67), 69: (0, 73),
                             75: (0, 79), 81: (0, 85), 87: (0, 91),
                             93: (1, 1)})    
    for key, value in block_dict.items():
        if key >= curblock:
            return value[0], value[1]
    return 1, 1

def check_torun(curblock):
    """Get Revision based on time."""
    blocklist = [2, 9, 15, 21, 27, 33, 39, 45, 51, 57, 63, 69, 75, 81, 87, 93]
    blkfilt = [b for b in blocklist if b >= curblock]
    if len(blkfilt):
        return True
    else:
        return False

def update_last_run_block(date, block_no, discom, engine):
    df = pd.DataFrame([{'date': pd.to_datetime(date, dayfirst=True), 'block_no': block_no, 'discom': discom}])
    df.to_sql(name='realtime_demand_tracker_{}'.format(discom), con=engine, if_exists='replace')
    return

def check_last_run_block(date, discom, engine):
    try:
        lst_run_blk = pd.read_sql_query("""
            select block_no
            from realtime_demand_tracker_{}
            where date = STR_TO_DATE('{}', '%%d-%%m-%%Y') 
            and discom = '{}'""".format(discom, date, discom),  engine, index_col=None)
        return lst_run_blk['block_no'].iloc[0]
    except:
        return 0


def check_curblock_data_available(date, discom, engine):
    cur_data_block = pd.read_sql_query("""select max(block_no) max_block_no
         from drawl_staging 
         where date = STR_TO_DATE('{}', '%%d-%%m-%%Y') and discom = '{}'
         and (constrained_load > 0 or constrained_load is not null)""".format(date, discom),
                                       engine, index_col=None)
    return cur_data_block['max_block_no'].iloc[0]


def realtime_demand_forecast(config, date, model_short_name,
                             mrr, discom, state):
    """Realtime forecast."""
    engine = create_engine(config, echo=False)
    today = pd.to_datetime(date, dayfirst=True)
    """Check if this is the block where it's reuired to run"""
    curblock = check_curblock_data_available(date, discom, engine)
    print curblock, check_last_run_block(date, discom, engine)

    """Return ..i.e get out of the execution if the desired block data not hit."""
    if not check_torun(curblock):
        return
    """Return ..i.e get out of the execution if the desired block data has been already executed."""
    if check_last_run_block(date, discom, engine) >= curblock:
        return

    current_date_lag = today - pd.DateOffset(1)

###################################################################
    holiday_event_master = pd.read_sql_query("""select date, event1 as name
                                                   from vw_holiday_event_master
                                                    where state = '{}'""".
                                                 format(state), engine,
                                                 index_col=None)
    holiday_event_master['date'] = pd.to_datetime(holiday_event_master['date'])


    # In[5]:


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





    # In[5]:

    # In[8]:
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



    # In[6]:





    # In[9]:


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


    weather_forecast_nonmissing = weather_forecast_nonmissing[['location','date','hour',
                                                               'temp','windspeed_bin' 
                                                               ,'rain_forecast_bin']]
    weather_forecast_nonmissing.rename(columns={'temp': 'temp_forecast', 
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

    append_forecast.rename(columns={'temp_forecast': 'temp', 
                                                'windspeed_bin_forecast': 'windspeed_bin',
                                                'rain_forecast_bin': 'rain_bin',  }, inplace=True)

    weather_actual = weather_actual_nonmissing[['location','date','hour',
                                                 'windspeed_bin','temp','rain_bin']]

    weather_initial = pd.DataFrame([])
    weather_initial = weather_initial.append(weather_actual)
    weather_initial = weather_initial.append(append_forecast)




    # In[7]:

    # In[6]:

    powercut_table =     pd.read_sql_query("""select date, block_no,
                          sum(powercut) as powercut
                          from powercut_staging where
                          discom = '{}'
                          group by date, block_no""".format(discom),
                          engine, index_col=None)
    powercut_table['date'] = pd.to_datetime(powercut_table['date'])
    powercut_table.sort_values(by=['date', 'block_no'], ascending=[True, True],
                               inplace=True)
    load_table =     pd.read_sql_query("""select date, block_no, constrained_load
                             from drawl_staging where discom = '{}'""".
                          format(discom), engine, index_col=None)

    load_table['date'] = pd.to_datetime(load_table['date'])


    load_table_initial = pd.merge(load_table, powercut_table, how='left', on=[
            'date', 'block_no'])
    load_table_initial['powercut'].fillna(0, inplace=True)
    load_table_initial['reported_load'] = load_table_initial[
        'constrained_load'] + load_table_initial['powercut']
    load_table_initial.sort_values(by=['date', 'block_no'], ascending=[
                                   True, True], inplace=True)


    load_table_hist = load_table_initial[load_table_initial['date'] <= current_date_lag]
    load_table_current = load_table_initial[(load_table_initial['date'] > current_date_lag) 
                                         & (load_table_initial['date'] <= today)
                                         & (load_table_initial['block_no']<= curblock)]

    load_table_initial = load_table_hist.append(load_table_current)

    load_table_initial['int_obs_load']=load_table_initial['reported_load']


    # In[8]:


    missing_load_count = ((load_table_initial.groupby(['date'],as_index=False)
        .agg({'reported_load':'count'}))
              .rename(columns={'reported_load':'reported_load_count'}))



    # In[9]:

    load_table_initial['lag_reported_load'] = load_table_initial['reported_load'].shift(1)


    # In[10]:

    missing_load_date = missing_load_count[missing_load_count['reported_load_count']<96][['date']]
    non_missing_load_date = missing_load_count[missing_load_count['reported_load_count']>=96][['date']]


    # In[11]:

    missing_date  = list(missing_load_date['date'].unique())
    non_missing_date  = list(non_missing_load_date['date'].unique())


    # In[12]:

    daily_load_summary = ((load_table_initial.groupby(['date'],as_index=False)
        .agg({'reported_load':{'max','min','mean'}})))

    daily_load_summary.columns = ['_'.join(col).strip() for col in daily_load_summary.columns.values]
    daily_load_summary.rename(columns={'date_': 'date'}, inplace=True)


    # In[13]:

    event_date_only = holiday_event_master[['date']]
    Preevent_date_only = holiday_event_master[['date']]
    Preevent_date_only['date'] = Preevent_date_only['date'] - pd.DateOffset(1)
    Postevent_date_only = holiday_event_master[['date']]
    Postevent_date_only['date'] = Postevent_date_only['date'] + pd.DateOffset(1)
    pre_evet_date_only =event_date_only.append(Preevent_date_only)
    pre_post_evet_date_only =pre_evet_date_only.append(Postevent_date_only)

    unique_event_date = pre_post_evet_date_only['date'].unique()


    # In[14]:

    daily_load_summary['dayofweek'] = pd.DatetimeIndex(
        daily_load_summary['date']).dayofweek  # Monday=0, Sunday=6


    # In[15]:

    daily_load_summary['event_day'] = np.where(daily_load_summary['date'].isin(unique_event_date),
                                              1,0)


    # In[16]:

    Temp_initial_summary = ((weather_initial.groupby(['date'],as_index=False)
        .agg({'temp':{'max','min','mean'}, 'rain_bin':{'max','min','mean'}
              })))
    Temp_initial_summary.columns = ['_'.join(col).strip() for col in Temp_initial_summary.columns.values]
    Temp_initial_summary.rename(columns={'date_': 'date'}, inplace=True)


    # In[17]:

    outlier_detection_table = pd.merge(daily_load_summary, Temp_initial_summary, how = 'left', 
                                      on = ['date'])


    # In[18]:

    # outlier_detection_table.to_csv('/Users/Awadhesh/Documents/LoadForecasting/GETCO_PROJECT/outlier_detection_table.csv', index=False)


    # In[19]:

    load_table_copy = load_table_initial.copy()
    load_table_copy['hour'] = np.ceil(load_table_copy['block_no']/4)
    hourly_load_summary = ((load_table_copy.groupby(['date','hour'],as_index=False)
        .agg({'reported_load':'max'}))
              .rename(columns={'reported_load':'reported_load_max'}))


    # In[20]:

    hourly_load_summary_missing_day =hourly_load_summary[hourly_load_summary['date'].isin(missing_date)] 
    hourly_load_summary_non_missing_day =hourly_load_summary[hourly_load_summary['date'].isin(non_missing_date)] 
    hourly_load_summary_non_missing_day['hour_no']=hourly_load_summary_non_missing_day['hour'].astype(str)+'hour'
    hourly_load_summary_missing_day['hour_no']=hourly_load_summary_missing_day['hour'].astype(str)+'hour'


    # In[21]:

    hourly_load_summary_non_missing_day['date'] = pd.to_datetime(hourly_load_summary_non_missing_day['date'])
    hourly_load_summary_missing_day['date'] = pd.to_datetime(hourly_load_summary_missing_day['date'])


    # In[22]:

    final_dist_matrix = pd.DataFrame([])
    for j in range(0, len(missing_date)): 
        test_current = hourly_load_summary_missing_day[hourly_load_summary_missing_day['date']==missing_date[j]]
        test_current_pivot = pd.pivot_table(test_current, 
                                    values=['reported_load_max'], 
                                    index=['date'], 
                                    columns=['hour_no']).reset_index()
        test_current_pivot.columns = ['_'.join(col).strip() for col in test_current_pivot.columns.values]
        test_current_pivot.rename(columns={'date_': 'date'}, inplace=True)
        test_compare = hourly_load_summary_non_missing_day[(hourly_load_summary_non_missing_day['date'] >= 
                                                           pd.to_datetime(missing_date[j]) - pd.DateOffset(31)) & 
                                                          (hourly_load_summary_non_missing_day['date'] < 
                                                           missing_date[j])
                                                          ]
        test_compare_pivot = pd.pivot_table(test_compare, 
                                    values=['reported_load_max'], 
                                    index=['date'], 
                                    columns=['hour_no']).reset_index()
        test_compare_pivot.columns = ['_'.join(col).strip() for col in test_compare_pivot.columns.values]
        test_compare_pivot.rename(columns={'date_': 'date'}, inplace=True)
        var_temp =[col for col in test_current_pivot.columns 
                 if 'reported_load' in col]
        a = test_current_pivot[var_temp]
        b = test_compare_pivot[var_temp]
        date_current = test_current['date'].unique()
        date_compare = test_compare['date'].unique()
        dist = scipy.spatial.distance.cdist(a,b, metric='euclidean') # pick the appropriate distance metric 
        dist_matrix = pd.DataFrame(dist)
        dist_matrix.columns = date_compare
        date_current = pd.Series(date_current)  
        dist_matrix['date'] = date_current.values
        dist_matrix = pd.melt(dist_matrix, id_vars = ['date'], value_name='eucledean_dist')
        dist_matrix.rename(columns={'variable': 'lag_date'}, inplace=True)
        dist_matrix['date'] = pd.to_datetime(dist_matrix['date'])
        dist_matrix['lag_date'] = pd.to_datetime(dist_matrix['lag_date'])
        dist_matrix.sort_values(by = ['date', 'eucledean_dist'], ascending = [True,True], inplace = True)
        def ranker(dist_matrix):
            dist_matrix['rank'] = np.arange(len(dist_matrix)) + 1
            return dist_matrix
        dist_matrix = dist_matrix.groupby(dist_matrix['date']).apply(ranker)
        final_dist_matrix = final_dist_matrix.append(dist_matrix)
        


    # In[23]:

    final_dist_matrix = final_dist_matrix[final_dist_matrix['rank']==1]


    # In[24]:

    imp_load_table = pd.merge(final_dist_matrix,load_table_copy,how = 'left',
                            left_on = ['lag_date'],
                            right_on = ['date'])


    # In[25]:

    imp_load_table.rename(columns={'date_x': 'date'}, inplace=True)
    imp_load_table = imp_load_table.drop('date_y', axis=1)
    imp_load_table = imp_load_table[['date','rank','block_no','reported_load']]


    # In[26]:

    imp_load_table.rename(columns={'reported_load': 'reported_load_imp'}, inplace=True)


    # In[27]:

    unique_date_imp = list(imp_load_table['date'].unique())
    actual_missing = load_table_copy[load_table_copy['date'].isin(unique_date_imp)]


    # In[28]:

    load_imp = pd.merge(imp_load_table,actual_missing,how = 'left',
                       on = ['date','block_no'])
    # load_imp['imp_curve'] = load_imp['spline_envelop_imp']/load_imp['spline_envelop_imp'].shift(1)
    load_imp['imp_curve_reported'] = load_imp['reported_load_imp']/load_imp['reported_load_imp'].shift(1)

    imputed_load_table = pd.DataFrame([])
    unique_date = load_imp['date'].unique()
    for j in range(0,len(unique_date)):
        test = load_imp[load_imp['date']==unique_date[j]]
        f_curve_reported =list(test['imp_curve_reported'])
        reported_envelop = list(test['reported_load'])
        imp_load =[]
        reported_imp_load=[]
        for j in range(0,len(test)):
            if reported_envelop[j]>0:
                new_reported = reported_envelop[j]
                reported_imp_load.append(new_reported)
            else: 
                new1_reported = f_curve_reported[j]*reported_imp_load[j-1] 
                reported_imp_load.append(new1_reported)
            envelop_reported = pd.DataFrame(reported_imp_load)
            envelop_reported = envelop_reported.iloc[0:]
        test['reported_imp_load']=envelop_reported.values
        imputed_load_table = imputed_load_table.append(test)
    imputed_load_table = imputed_load_table[['date','block_no','reported_imp_load','int_obs_load']]


    # In[29]:



    # test = imputed_load_table[imputed_load_table['date']>current_date_lag]
    # test


    # In[30]:

    load_table_final = pd.merge(load_table_initial,
                              imputed_load_table,
                              how = 'outer',
                              on = ['date','block_no'])

    load_table_final.rename(columns={'int_obs_load_x': 'int_obs_load'}, inplace=True)
    load_table_final = load_table_final.drop('int_obs_load_y', axis=1)
    load_table_final['imp_load'] = np.where(load_table_final['reported_load']>0
                                                    ,load_table_final['reported_load'],
                                                   load_table_final['reported_imp_load'])

    load_envelop = load_table_final
        
    load_envelop['endo_demand'] = load_envelop['imp_load']
    load_table =     load_envelop[['date', 'block_no', 'int_obs_load', 'endo_demand']]

    load_table['hour'] = np.ceil(load_table['block_no'] / 4)
    load_table['year'] = pd.DatetimeIndex(load_table['date']).year
    load_table['month'] = pd.DatetimeIndex(
        load_table['date']).month   # jan = 1, dec = 12
    load_table['dayofweek'] = pd.DatetimeIndex(
        load_table['date']).dayofweek  # Monday=0, Sunday=6
    load_table.sort_values(by=['date', 'block_no'], ascending=[
                           True, True], inplace=True)

    load_only_table =     load_table[['date', 'block_no', 'endo_demand', 'int_obs_load']]
    last_date_block = load_only_table[
        load_only_table['date'] == max(
            load_only_table['date'])]
    max_block = max(last_date_block['block_no'])
    columns = [['block_no', 'endo_demand']]

    if max_block < 96:
        forecast_period0 = pd.DataFrame(columns=columns)
        forecast_period0['block_no'] = range(max_block + 1, 97)
        forecast_period0['date'] = max(load_only_table['date'])
        forecast_period0 =         forecast_period0[['date', 'block_no', 'endo_demand']]
    else:
        forecast_period0 = pd.DataFrame(columns=columns)
        forecast_period0['block_no'] = range(1, 97)
        forecast_period0['date'] =         max(load_only_table['date']) + pd.DateOffset(1)
        forecast_period0 =         forecast_period0[['date', 'block_no', 'endo_demand']]

    forecast_period = pd.DataFrame([])
    for j in range(1, 8):
        period = pd.DataFrame(columns=columns)
        period['block_no'] = range(1, 97)
        period['date'] = max(forecast_period0['date']) + pd.DateOffset(j)
        period = period[['date', 'block_no', 'endo_demand']]
        forecast_period = forecast_period.append(period)

    forecast_period_date =     pd.concat([forecast_period0, forecast_period], axis=0)
    load_only_table =     pd.concat([load_only_table, forecast_period_date], axis=0)


    non_missing_Load_date = pd.DataFrame(load_only_table.date.unique())
    non_missing_Load_date.rename(columns={0: 'date'}, inplace=True)
    non_missing_Load_date['date'] =     pd.to_datetime(non_missing_Load_date['date'])
    non_missing_Load_date.sort_values(by=['date'], ascending=[False],
                                      inplace=True)
    non_missing_Load_date['date_key'] = range(0, len(non_missing_Load_date))
    date_key = non_missing_Load_date[['date', 'date_key']]


    pre_event_master = holiday_event_master[['date', 'name']]
    pre_event_master['date'] = pre_event_master['date'] - timedelta(days=1)
    pre_event_master['name'] = 'pre_' + pre_event_master['name'].astype(str)
    post_event_master = holiday_event_master[['date', 'name']]
    post_event_master['date'] = pd.DatetimeIndex(
        post_event_master['date']) + timedelta(days=1)
    post_event_master['name'] = 'post_' + post_event_master['name'].astype(str)
    event_master = pd.DataFrame([])
    event_master = event_master.append(holiday_event_master)
    event_master = event_master.append(pre_event_master)
    event_master = event_master.append(post_event_master)
    event_master = event_master.loc[~event_master['date'].duplicated()]
    event_date = event_master[['date', 'name']]
    event_date['holiday_event'] = 1
    event_calendar = event_date[['date', 'holiday_event']]
    date_key_event = pd.merge(date_key, event_calendar, how='left', on='date')
    date_key_event['dayofweek'] = pd.DatetimeIndex(
        date_key_event['date']).dayofweek  # Monday=0, Sunday=6
    date_key_event['holiday_event'].fillna(0, inplace=True)
    date_key_event['weekend_flag'] =     np.where((date_key_event['dayofweek'] == 6) , 1, 0)
    date_key_event['holiday_flag'] = date_key_event[
        'weekend_flag'] + date_key_event['holiday_event']
    date_key_event = date_key_event[
        ['date', 'date_key', 'holiday_flag', 'weekend_flag']]
    date_key_event['holiday_flag'] = np.where(
        (date_key_event['holiday_flag'] >= 1), 1, 0)
    date_key_event = date_key_event[['date', 'holiday_flag', 'weekend_flag']]
    date_key_event = date_key_event.drop_duplicates()
    event_master = event_master.drop_duplicates(
        subset='date', keep='first', inplace=False)


    # In[31]:

    # event day Load Impute
    event_day_load = pd.merge(event_master,
                              load_table,
                              how='left',
                              on=['date']
                              )

    event_day_load = event_day_load[np.isfinite(event_day_load['endo_demand'])]
    event_day_load_summary =     event_day_load.groupby(['name', 'block_no', 'month'],
                               as_index=False).\
        agg({'endo_demand': 'median'})
    event_day_load_summary.rename(columns={'endo_demand': 
                                           'endo_demand_event_day_median'}, 
                                  inplace=True)
    unique_event_date = event_day_load['date'].unique()
    event_load_curve = pd.DataFrame([])
    for j in range(0,len(unique_event_date)):
        test = event_day_load[event_day_load['date']==unique_event_date[j]]
        test['L_curve'] = test['int_obs_load']/test['int_obs_load'].shift(1)
        event_load_curve = event_load_curve.append(test)
    event_load_curve_summary = event_load_curve.groupby(['name', 'block_no'],
                               as_index=False).\
        agg({'L_curve': 'mean'})
    unique_event_name = event_load_curve_summary['name'].unique()
    event_load_curve_final = pd.DataFrame([])
    for j in range(0,len(unique_event_name)):
        test = event_load_curve_summary[event_load_curve_summary['name']==unique_event_name[j]]
        test['L_curve'][:1] = test['L_curve'][1:2]
        event_load_curve_final = event_load_curve_final.append(test)
        
    event_date_imp = [i for i in unique_date_imp if i in unique_event_date]
    eventname_date_imp = event_day_load[event_day_load['date'].isin(event_date_imp)]
    eventname_date_imp = eventname_date_imp[['date','name']]
    eventname_date_imp = eventname_date_imp.drop_duplicates(subset=None, keep='first', inplace=False)
    event_date_missing_load = pd.merge(eventname_date_imp,
                                       event_load_curve_final, how = 'left',
                                       on = 'name')
    event_date_missing_loadcurve = event_date_missing_load[['date','block_no','L_curve']]
    load_table_final_event = pd.merge(load_table_final,event_date_missing_loadcurve, how = 'left',
                                      on = ['date','block_no'])
    load_curve_event =  load_table_final_event[load_table_final_event['L_curve']>0]

    event_day_imp_load_table = pd.DataFrame([])
    unique_date = load_curve_event['date'].unique()
    for j in range(0,len(unique_date)):
        test = load_curve_event[load_curve_event['date']==unique_date[j]]
        L_curve =list(test['L_curve'])
        obs_load = list(test['int_obs_load'])
        imp_load =[]
        obs_imp_load =[]
        for j in range(0,len(test)):
            if obs_load[j]>0:
                new_reported = obs_load[j]
                obs_imp_load.append(new_reported)
            else: 
                new1_reported = L_curve[j]*obs_imp_load[j-1] 
                obs_imp_load.append(new1_reported)
            envet_day_imp_load = pd.DataFrame(obs_imp_load)
            envet_day_imp_load = envet_day_imp_load.iloc[0:]
        test['envet_day_imp_load']=envet_day_imp_load.values
        event_day_imp_load_table = event_day_imp_load_table.append(test)
    event_day_imp_load_table = event_day_imp_load_table[['date','block_no', 'envet_day_imp_load']]


    # In[32]:

    load_table_final_test =  pd.merge(load_table_final, event_day_imp_load_table, how = 'left',
                                      on = ['date','block_no'])

    load_table_final_test['endo_demand'] = np.where(load_table_final_test['envet_day_imp_load'] > 0,
                                                   load_table_final_test['envet_day_imp_load'],
                                                   load_table_final_test['endo_demand'])


    # In[33]:

    weather_initial_summary = ((weather_initial.groupby(['date','hour'],as_index=False)
        .agg({'temp':'median',
              'rain_bin':'median',
              'windspeed_bin':'median'}))
              .rename(columns={'temp':'temp_median',
                               'rain_bin':'rain_bin_median',
                               'windspeed_bin':'windspeed_bin_median'}))


    # In[34]:

    weather_initial_summary['temp_median_lag1']=weather_initial_summary['temp_median'].shift(24)
    weather_initial_summary['temp_median_lag2']=weather_initial_summary['temp_median'].shift(48)
    weather_initial_summary['temp_median_lag3']=weather_initial_summary['temp_median'].shift(72)
    weather_initial_summary['rain_bin_median_lag1']=weather_initial_summary['rain_bin_median'].shift(24)
    weather_initial_summary['rain_bin_median_lag2']=weather_initial_summary['rain_bin_median'].shift(48)
    weather_initial_summary['rain_bin_median_lag3']=weather_initial_summary['rain_bin_median'].shift(72)


    # In[35]:

    weather_initial_summary['temp_diff1'] = (weather_initial_summary['temp_median'] 
                                         - weather_initial_summary['temp_median_lag1'])
    weather_initial_summary['temp_diff2'] = (weather_initial_summary['temp_median'] 
                                         - weather_initial_summary['temp_median_lag2'])
    weather_initial_summary['temp_diff3'] = (weather_initial_summary['temp_median'] 
                                         - weather_initial_summary['temp_median_lag3'])


    weather_initial_summary['rain_bin_diff1'] = (weather_initial_summary['rain_bin_median'] 
                                         - weather_initial_summary['rain_bin_median_lag1'])
    weather_initial_summary['rain_bin_diff2'] = (weather_initial_summary['rain_bin_median'] 
                                         - weather_initial_summary['rain_bin_median_lag2'])
    weather_initial_summary['rain_bin_diff3'] = (weather_initial_summary['rain_bin_median'] 
                                         - weather_initial_summary['rain_bin_median_lag3'])


    # In[36]:

    load_table=     load_table_final_test[['date', 'block_no','int_obs_load', 'endo_demand']]
    load_table['block_no'] = load_table['block_no'].astype(int)
    load_table['hour'] = np.ceil(load_table['block_no'] / 4)
    load_table['year'] = pd.DatetimeIndex(load_table['date']).year
    load_table['month'] = pd.DatetimeIndex(
        load_table['date']).month   # jan = 1, dec = 12
    load_table['dayofweek'] = pd.DatetimeIndex(
        load_table['date']).dayofweek  # Monday=0, Sunday=6
    load_table.sort_values(by=['date', 'block_no'], ascending=[
                           True, True], inplace=True)

    load_only_table =     load_table[['date', 'block_no', 'endo_demand', 'int_obs_load']]
    last_date_block = load_only_table[
        load_only_table['date'] == max(
            load_only_table['date'])]
    max_block = max(last_date_block['block_no'])
    columns = [['block_no', 'endo_demand']]

    if max_block < 96:
        forecast_period0 = pd.DataFrame(columns=columns)
        forecast_period0['block_no'] = range(max_block + 1, 97)
        forecast_period0['date'] = max(load_only_table['date'])
        forecast_period0 =         forecast_period0[['date', 'block_no', 'endo_demand']]
    else:
        forecast_period0 = pd.DataFrame(columns=columns)
        forecast_period0['block_no'] = range(1, 97)
        forecast_period0['date'] =         max(load_only_table['date']) + pd.DateOffset(1)
        forecast_period0 =         forecast_period0[['date', 'block_no', 'endo_demand']]

    forecast_period = pd.DataFrame([])
    for j in range(1, 8):
        period = pd.DataFrame(columns=columns)
        period['block_no'] = range(1, 97)
        period['date'] = max(forecast_period0['date']) + pd.DateOffset(j)
        period = period[['date', 'block_no', 'endo_demand']]
        forecast_period = forecast_period.append(period)

    forecast_period_date =     pd.concat([forecast_period0, forecast_period], axis=0)
    load_only_table =     pd.concat([load_only_table, forecast_period_date], axis=0)


    non_missing_Load_date = pd.DataFrame(load_only_table.date.unique())
    non_missing_Load_date.rename(columns={0: 'date'}, inplace=True)
    non_missing_Load_date['date'] =     pd.to_datetime(non_missing_Load_date['date'])
    non_missing_Load_date.sort_values(by=['date'], ascending=[False],
                                      inplace=True)
    non_missing_Load_date['date_key'] = range(0, len(non_missing_Load_date))
    date_key = non_missing_Load_date[['date', 'date_key']]


    pre_event_master = holiday_event_master[['date', 'name']]
    pre_event_master['date'] = pre_event_master['date'] - timedelta(days=1)
    pre_event_master['name'] = 'pre_' + pre_event_master['name'].astype(str)
    post_event_master = holiday_event_master[['date', 'name']]
    post_event_master['date'] = pd.DatetimeIndex(
        post_event_master['date']) + timedelta(days=1)
    post_event_master['name'] = 'post_' + post_event_master['name'].astype(str)
    event_master = pd.DataFrame([])
    event_master = event_master.append(holiday_event_master)
    event_master = event_master.append(pre_event_master)
    event_master = event_master.append(post_event_master)
    event_master = event_master.loc[~event_master['date'].duplicated()]
    event_date = event_master[['date', 'name']]
    event_date['holiday_event'] = 1
    event_calendar = event_date[['date', 'holiday_event']]
    date_key_event = pd.merge(date_key, event_calendar, how='left', on='date')
    date_key_event['dayofweek'] = pd.DatetimeIndex(
        date_key_event['date']).dayofweek  # Monday=0, Sunday=6
    date_key_event['holiday_event'].fillna(0, inplace=True)
    date_key_event['weekend_flag'] =     np.where((date_key_event['dayofweek'] == 6) , 1, 0)
    date_key_event['holiday_flag'] = date_key_event[
        'weekend_flag'] + date_key_event['holiday_event']
    date_key_event = date_key_event[
        ['date', 'date_key', 'holiday_flag', 'weekend_flag']]
    date_key_event['holiday_flag'] = np.where(
        (date_key_event['holiday_flag'] >= 1), 1, 0)
    date_key_event = date_key_event[['date', 'holiday_flag', 'weekend_flag']]
    date_key_event = date_key_event.drop_duplicates()
    event_master = event_master.drop_duplicates(
        subset='date', keep='first', inplace=False)


    # In[37]:

    pivot_var =[col for col in weather_initial_summary.columns 
                 if 'temp' in col or 'rain_bin' in col]


    # In[38]:



    weather_initial_summary['hour_no']=weather_initial_summary['hour'].astype(str)+'hour'


    weather_hourly_pivot = pd.pivot_table(weather_initial_summary, 
                                values=['temp_median','temp_median_lag1','temp_median_lag2','temp_median_lag3',
                                       'rain_bin_median', 'rain_bin_median_lag1','rain_bin_median_lag2',
                                       'rain_bin_median_lag3'], 
                                index=['date'], 
                                columns=['hour_no']).reset_index()

    weather_hourly_pivot.columns = ['_'.join(col).strip() for col in weather_hourly_pivot.columns.values]
    weather_hourly_pivot.rename(columns={'date_': 'date'}, inplace=True)


    # In[39]:

    # In[16]:


    # get non missing dates load and weather
    non_missing_load = load_only_table[np.isfinite(load_only_table['endo_demand'])]
    load_only_table['hour']= np.ceil(load_only_table['block_no']/4)
    window_summary = (load_only_table.groupby(['date'],as_index=False)
                        .agg({'endo_demand':'count'}))
    window_summary.rename(columns={'endo_demand': 'endo_demand_count'}, inplace=True)                      
    window_summary = window_summary[window_summary['endo_demand_count']==96]
    non_missing_load_date = window_summary['date'].unique()
     
    weather_all = weather_hourly_pivot
    non_missing_date_relative = [x for x in non_missing_load['date'].unique() if x in 
                                          weather_hourly_pivot['date'].unique()]

    non_missing_date_final =    [x for x in non_missing_date_relative if x in 
                                          non_missing_load_date]




    # In[40]:

    # In[17]:

    load_table_nonmissing = load_table[load_table['date'].isin(non_missing_date_final)]

    weather_all = weather_hourly_pivot.copy()
    weather_relative = weather_hourly_pivot.copy()
    weather_relative = weather_hourly_pivot[weather_relative['date'].isin(non_missing_date_final)]
    weather_all.sort_values(by=['date'], ascending=[True], inplace=True)
    weather_relative.sort_values(by=['date'], ascending=[True], inplace=True)

    nn_days = 7
    lag_nn = 3
    lag_d = 0


    var_temp =[col for col in weather_hourly_pivot.columns 
                 if 'temp' in col or 'rain_bin' in col]


    a = weather_all[var_temp]
    b = weather_relative[var_temp]
    date_all = weather_all['date'].unique()
    date_relative = weather_relative['date'].unique()
    dist = scipy.spatial.distance.cdist(a,b, metric='cosine') # pick the appropriate distance metric 
    dist_matrix = pd.DataFrame(dist)
    dist_matrix.columns = date_relative
    date_all = pd.Series(date_all)  
    dist_matrix['date'] = date_all.values
    dist_matrix = pd.melt(dist_matrix, id_vars = ['date'], value_name='eucledean_dist')
    dist_matrix.rename(columns={'variable': 'lag_date'}, inplace=True)

    dist_matrix['date'] = pd.to_datetime(dist_matrix['date'])
    dist_matrix['lag_date'] = pd.to_datetime(dist_matrix['lag_date'])
    dist_matrix = dist_matrix[dist_matrix['lag_date'] < dist_matrix['date'] - pd.DateOffset(0)]
    dist_matrix['days'] = (dist_matrix['date'] 
                        - dist_matrix['lag_date']).dt.days
    dist_matrix = dist_matrix[dist_matrix['days']<=nn_days]


    dist_matrix.sort_values(by = ['date', 'eucledean_dist'], ascending = [True,True], inplace = True)
    def ranker(dist_matrix):
        dist_matrix['rank'] = np.arange(len(dist_matrix)) + 1
        return dist_matrix
    dist_matrix = dist_matrix.groupby(dist_matrix['date']).apply(ranker)
    dist_matrix = dist_matrix[dist_matrix['rank']<=lag_nn]


    # In[41]:


    weather_dist_lag_initial = pd.merge(dist_matrix, load_table_nonmissing,
                                        left_on='lag_date', right_on='date')
    weather_dist_lag_initial.rename(columns={'date_x': 'date'}, inplace=True)
    weather_dist_lag_initial = weather_dist_lag_initial.drop('date_y', axis=1)

    load_summary =     load_table.groupby(['month', 'block_no'], as_index=False).    agg({'endo_demand': 'median'})
    load_summary.rename(columns={'endo_demand': 'endo_demand_median'}, inplace=True)


    load_summary_weekday =     load_table.groupby(['month', 'block_no', 'dayofweek'],
                           as_index=False).\
        agg({'endo_demand': 'median'})

    load_summary_weekday.rename(columns={'endo_demand': 'endo_demand_weekday_median'},
                                inplace=True)

    weekday_correction = pd.merge(load_summary_weekday,
                                  load_summary,
                                  how='left',
                                  )
        
    weekday_correction['week_day_correction_factor'] =  (weekday_correction['endo_demand_weekday_median'] -
                                                         weekday_correction['endo_demand_median'])    
    weekday_correction.sort_values(by = ['month','dayofweek','block_no'], 
                              ascending  = [True,True,True],
                              inplace = True)

    unique_month = weekday_correction['month'].unique()
    weekday_correction_envelop = pd.DataFrame([])
    for j in range(0, len(unique_month)):
        test = weekday_correction[weekday_correction['month']==unique_month[j]]
        unique_day = test['dayofweek'].unique()
        for i in range(0,len(unique_day)):
            test1=test[test['dayofweek']==unique_day[i]]
            s = np.array(test1['week_day_correction_factor'])
            P =  savitzky_golay(s, window_size=5,order=1)
            envelop = pd.DataFrame(P)
            envelop=envelop.iloc[0:]
            test1['envelop_weekend']=envelop.values
            weekday_correction_envelop = weekday_correction_envelop.append(test1)
    weekday_correction_envelop['weekday_envelop_pre'] = weekday_correction_envelop['envelop_weekend']

    weekday_correction = weekday_correction_envelop[
        ['month', 'dayofweek', 'block_no', 'weekday_envelop_pre']]
    weekday_correction.sort_values(by=['month', 'dayofweek', 'block_no'],
                                   ascending=[True, True, True],
                                   inplace=True)

    weekday_correction['week_day_correction_factor_pre'] =  weekday_correction['weekday_envelop_pre']
    weather_dist_lag_initial['NN_dayofweek'] = pd.DatetimeIndex(weather_dist_lag_initial['lag_date']).dayofweek

    weather_dist_lag_initial['NN_month'] = pd.DatetimeIndex(
        weather_dist_lag_initial['lag_date']).month

    weather_dist_lag_initial_NNDOW =     pd.merge(weather_dist_lag_initial, weekday_correction,
                 left_on=['NN_month', 'NN_dayofweek', 'block_no'],
                 right_on=['month', 'dayofweek', 'block_no'])
    weather_dist_lag_initial_NNDOW['endo_demand_NN_DOW'] =     (weather_dist_lag_initial_NNDOW['endo_demand'] 
                                                - weather_dist_lag_initial_NNDOW['week_day_correction_factor_pre'])

    event_day_load = pd.merge(event_master,
                              load_table,
                              how='left',
                              on=['date']
                              )

    event_day_load = event_day_load[np.isfinite(event_day_load['endo_demand'])]
    event_day_load_summary =     event_day_load.groupby(['name', 'block_no', 'month'],
                               as_index=False).\
        agg({'endo_demand': 'median'})
    event_day_load_summary.rename(columns={'endo_demand': 
                                           'endo_demand_event_day_median'}, 
                                  inplace=True)


    event_correction = pd.merge(event_day_load_summary, load_summary,
                                how='left',
                                on=['month', 'block_no'])

    event_correction['event_day_correction_factor'] =     (event_correction['endo_demand_event_day_median'] 
                                                           - event_correction['endo_demand_median'])

    event_correction_summary =     event_correction.groupby(['name', 'block_no'], as_index=False). agg({'event_day_correction_factor': 'median'})
    event_correction_summary.rename(columns={'event_day_correction_factor': 
                                             'event_day_correction_factor_median'}, inplace=True)

    weather_dist_lag_initial_NNDOW_EVENT =     pd.merge(weather_dist_lag_initial_NNDOW,
                 event_master,
                 how='left',
                 left_on=['lag_date'],
                 right_on=['date'])
    weather_dist_lag_initial_NNDOW_EVENT.    rename(columns={'block_no_': 'block_no',
                        'name_': 'name',
                        'date_x': 'date'}, inplace=True)
    event_correction_summary.sort_values(by = ['name','block_no'], 
                              ascending  = [True,True],
                              inplace = True)

    unique_event = event_correction_summary['name'].unique()

    event_correction_envelop = pd.DataFrame([])
    for j in range(0, len(unique_event)):
        test = event_correction_summary[event_correction_summary['name']==unique_event[j]]
        s = np.array(test['event_day_correction_factor_median'])
        P =  savitzky_golay(s, window_size=5,order=1)
        envelop = pd.DataFrame(P)
        envelop=envelop.iloc[0:]
        test['envelop_event']=envelop.values
        event_correction_envelop = event_correction_envelop.append(test)
    event_correction_envelop['event_envelop_pre'] = event_correction_envelop['envelop_event']

    weather_dist_lag_initial_NNDOW_EVENT = pd.merge(weather_dist_lag_initial_NNDOW,
                                                event_master,
                                               how = 'left',
                                               left_on = ['lag_date'],
                                               right_on=['date'])
    weather_dist_lag_initial_NNDOW_EVENT.rename(columns={'block_no_': 'block_no',
                            'name_':'name',
                             'date_x':'date'}, inplace=True)

    del weather_dist_lag_initial_NNDOW_EVENT['date_y']




    # In[42]:

    # In[19]:


    weather_dist_lag_initial_NNDOWEVENT = pd.merge(weather_dist_lag_initial_NNDOW_EVENT,event_correction_envelop,
                                               how = 'left',
                                   on =['name','block_no'])

    weather_dist_lag_initial_NNDOWEVENT['event_envelop_pre'].fillna(0, inplace=True)

    weather_dist_lag_initial_NNDOWEVENT['endo_demand_NN_event'] = (weather_dist_lag_initial_NNDOWEVENT['endo_demand']
                                                                - weather_dist_lag_initial_NNDOWEVENT['event_envelop_pre'])



    weather_dist_lag_initial_NNDOWEVENT['endo_demand_NN'] =     weather_dist_lag_initial_NNDOWEVENT[
            ['endo_demand_NN_DOW', 'endo_demand_NN_event']].max(axis=1)
    weather_dist_lag_initial_NNDOWEVENT.sort_values(
        by=['date', 'block_no'], ascending=[True, True], inplace=True)

    weather_dist_lag_initial = weather_dist_lag_initial_NNDOWEVENT.copy()
    weather_dist_lag_initial['rank_no'] = weather_dist_lag_initial[
        'rank'].astype(str) + 'lag'
    weather_dist_lag =     pd.pivot_table(weather_dist_lag_initial, values=['endo_demand_NN'],
                       index=['date', 'block_no'],
                       columns=['rank_no']).reset_index()
    weather_dist_lag.columns = ['_'.join(col).strip()
                                for col in weather_dist_lag.columns.values]
    weather_dist_lag.rename(
        columns={
            'date_': 'date',
            'block_no_': 'block_no'},
        inplace=True)

    weather_dist_lag = pd.merge(load_only_table, weather_dist_lag,
                                how='left',
                                on=['date', 'block_no'])
    weather_dist_lag['year'] = pd.DatetimeIndex(weather_dist_lag['date']).year
    weather_dist_lag['month'] = pd.DatetimeIndex(
        weather_dist_lag['date']).month   # jan = 1, dec = 12
    weather_dist_lag['dayofweek'] = pd.DatetimeIndex(
        weather_dist_lag['date']).dayofweek  # Monday=0, Sunday=6

    weather_dist_lag.rename(columns={'endo_demand_NN_1lag': 'Load_NN1',
                                     'endo_demand_NN_2lag': 'Load_NN2',
                                     'endo_demand_NN_3lag': 'Load_NN3'

                                    },
                            inplace=True)

    weather_dist_lag.Load_NN2.fillna(weather_dist_lag.Load_NN3, inplace=True)
    weather_dist_lag.Load_NN1.fillna(weather_dist_lag.Load_NN2, inplace=True)
    t = 0.25
    w1 = 1
    w2 = t
    w3 = t**2

    weather_dist_lag['endo_pred_sim_day_load'] = (weather_dist_lag['Load_NN1'] * w1 +
         weather_dist_lag['Load_NN2'] * w2 +
         weather_dist_lag['Load_NN3'] * w3 )/(w1 + w2 + w3)




    # In[43]:

    # In[20]:


    weather_dist_lag['similar_day_load'] = weather_dist_lag["Load_NN1"]

    NN_PRED_summary =     weather_dist_lag.groupby(['month', 'block_no'],
                                 as_index=False).\
        agg({'endo_pred_sim_day_load': 'median'})
    NN_PRED_summary.rename(columns={'endo_pred_sim_day_load': 'endo_pred_sim_day_load_median',
                                    'month_': 'month'}, inplace=True)

    weather_dist_lag['resdidual_NN'] = weather_dist_lag[
        'endo_demand'] - weather_dist_lag['endo_pred_sim_day_load']

    residual_summary_weekday =     weather_dist_lag.groupby(['month', 'block_no', 'dayofweek'],
                                 as_index=False).\
        agg({'resdidual_NN': 'median'})
    residual_summary_weekday.rename(columns={'resdidual_NN': 'resdidual_NN_weekday_median'},
                                    inplace=True)


    # In[44]:

    # In[26]:


    weekday_correction = pd.merge(NN_PRED_summary,
                                  residual_summary_weekday,
                                  how='left',
                                  on=['month', 'block_no']
                                  )



    weekday_correction['week_day_correction_factor'] =  weekday_correction['resdidual_NN_weekday_median'] 
    weekday_correction = weekday_correction[
        ['month', 'dayofweek', 'block_no', 'week_day_correction_factor']]
    weekday_correction.sort_values(by=['month', 'dayofweek', 'block_no'],
                                   ascending=[True, True, True],
                                   inplace=True)

    weekday_correction.sort_values(by = ['month','dayofweek','block_no'], 
                              ascending  = [True,True,True],
                              inplace = True)
    unique_month = weekday_correction['month'].unique()

    weekday_correction_envelop = pd.DataFrame([])
    for j in range(0, len(unique_month)):
        test = weekday_correction[weekday_correction['month']==unique_month[j]]
        unique_day = test['dayofweek'].unique()
        for i in range(0,len(unique_day)):
            test1 = test[test['dayofweek']==unique_day[i]]
            s = np.array(test1['week_day_correction_factor'])
            P =  savitzky_golay(s, window_size=5,order=1)
            envelop = pd.DataFrame(P)
            envelop=envelop.iloc[0:]
            test1['envelop_weekday_post']=envelop.values
            weekday_correction_envelop = weekday_correction_envelop.append(test1)
    weekday_correction_envelop['weekday_envelop_post'] = weekday_correction_envelop['envelop_weekday_post']

    data_forecast_weekday = pd.merge(weather_dist_lag, weekday_correction_envelop,
                                     how='left',
                                     on=['month', 'dayofweek', 'block_no'])
    data_forecast_weekday['weekend_correction_smooth'] =     data_forecast_weekday['weekday_envelop_post']
    data_forecast_weekday['NN_PRED_WEEKDAY_CORRECTED'] = (data_forecast_weekday['endo_pred_sim_day_load']  
                                                          + data_forecast_weekday['weekend_correction_smooth'])

    event_day_pred = pd.merge(data_forecast_weekday,
                              event_master,
                              how='left',
                              on=['date']
                              )

    event_day_pred['event_residual'] =     (event_day_pred['endo_demand']
                                            -  event_day_pred['NN_PRED_WEEKDAY_CORRECTED'])
    event_day_pred['event_residual_factor'] =     event_day_pred['event_residual'] 
    event_day_residual_summary =     event_day_pred.groupby(['name', 'block_no'],
                               as_index=False).\
        agg({'event_residual_factor': 'median'})

    event_day_residual_summary.rename(columns={'event_residual_factor': 
                                      'event_residual_factor_median'}, 
                                      inplace=True)
    event_day_residual_summary = event_day_residual_summary[
        np.isfinite(event_day_residual_summary['event_residual_factor_median'])
    ]
    event_day_residual_summary.sort_values(by = ['name','block_no'], 
                              ascending  = [True,True],
                              inplace = True)
    unique_event = event_day_residual_summary['name'].unique()

    event_correction_smooth = pd.DataFrame([])
    for j in range(0, len(unique_event)):
        test = event_day_residual_summary[event_day_residual_summary['name']==unique_event[j]]
        test.sort_values(by = ['block_no'],
                          ascending = [True],
                          inplace = True)
        s = np.array(test['event_residual_factor_median'])
        P =  savitzky_golay(s, window_size=5,order=1)
        envelop = pd.DataFrame(P)
        envelop=envelop.iloc[0:]
        test['envelop_event_post']=envelop.values
        event_correction_smooth = event_correction_smooth.append(test)
    event_correction_smooth['event_envelop_post'] = event_correction_smooth['envelop_event_post']




    event_correction_smooth = event_correction_smooth[
        ['name', 'block_no', 'event_envelop_post']]

    data_forecast_weekday_event = pd.merge(event_day_pred,
                                           event_correction_smooth,
                                           how='left',
                                           on=['name', 'block_no']
                                           )

    data_forecast_weekday_event['event_envelop_post'].    fillna(0, inplace=True)

    data_forecast_weekday_event['NN_PRED_WEEKDAY_EVENT_CORRECTED'] = (data_forecast_weekday_event['NN_PRED_WEEKDAY_CORRECTED']  
                                                                      + data_forecast_weekday_event['event_envelop_post'])

    data_forecast_nn =     data_forecast_weekday_event[['date',
                                     'block_no',
                                     'int_obs_load',
                                     'endo_demand',
                                      'endo_pred_sim_day_load',                  
                                     'NN_PRED_WEEKDAY_EVENT_CORRECTED']]
    data_forecast_nn['NN_PRED_DEMAND'] =     data_forecast_nn['NN_PRED_WEEKDAY_EVENT_CORRECTED']

    pred_table_similarday_KNN_HYBRID =     data_forecast_weekday_event[['date',
                                     'block_no',
                                     'NN_PRED_WEEKDAY_EVENT_CORRECTED']]
    pred_table_similarday_KNN_HYBRID.rename(
        columns={
            'NN_PRED_WEEKDAY_EVENT_CORRECTED': 'demand_forecast'},
        inplace=True)


    # In[45]:

    lag1 = dist_matrix[dist_matrix['rank']==1][['date','lag_date']]
    lag2 = dist_matrix[dist_matrix['rank']==2][['date','lag_date']]
    lag3 = dist_matrix[dist_matrix['rank']==3][['date','lag_date']]


    # In[46]:

    median_weather = weather_initial_summary[['date','hour','temp_median','rain_bin_median','windspeed_bin_median']]


    # In[47]:

    lag1_weather = pd.merge(lag1,median_weather, how = 'left',
                           left_on = ['lag_date'],right_on=['date'])
    lag1_weather.columns = [str(col) + '_NN1' for col in lag1_weather.columns]
    lag1_weather = lag1_weather[['date_x_NN1','hour_NN1','temp_median_NN1','rain_bin_median_NN1','windspeed_bin_median_NN1']]
    lag1_weather.rename(columns={'date_x_NN1': 'date', 'hour_NN1': 'hour'}, inplace=True)


    # In[48]:

    lag2_weather = pd.merge(lag2,median_weather, how = 'left',
                           left_on = ['lag_date'],right_on=['date'])
    lag2_weather.columns = [str(col) + '_NN2' for col in lag2_weather.columns]
    lag2_weather = lag2_weather[['date_x_NN2','hour_NN2','temp_median_NN2','rain_bin_median_NN2',
                                'windspeed_bin_median_NN2']]
    lag2_weather.rename(columns={'date_x_NN2': 'date', 'hour_NN2': 'hour'}, inplace=True)


    # In[49]:

    lag3_weather = pd.merge(lag3,median_weather, how = 'left',
                           left_on = ['lag_date'],right_on=['date'])
    lag3_weather.columns = [str(col) + '_NN3' for col in lag3_weather.columns]
    lag3_weather = lag3_weather[['date_x_NN3','hour_NN3','temp_median_NN3','rain_bin_median_NN3','windspeed_bin_median_NN3']]
    lag3_weather.rename(columns={'date_x_NN3': 'date', 'hour_NN3': 'hour'}, inplace=True)


    # In[50]:

    lag_weather = pd.merge(pd.merge(lag1_weather,lag2_weather, how = 'left',
                          on = ['date','hour']),
                          lag3_weather, how = 'left', 
                          on = ['date','hour'])


    # In[51]:

    t = 0.25
    w1 = 1
    w2 = t
    w3 = t**2

    lag_weather['temp_median_NN'] = (lag_weather['temp_median_NN1'] * w1 +
         lag_weather['temp_median_NN2'] * w2 +
         lag_weather['temp_median_NN3'] * w3 )/(w1 + w2 + w3)

    lag_weather['rain_bin_median_NN'] = (lag_weather['rain_bin_median_NN1'] * w1 +
         lag_weather['rain_bin_median_NN2'] * w2 +
         lag_weather['rain_bin_median_NN3'] * w3 )/(w1 + w2 + w3)

    lag_weather['windspeed_bin_median_NN'] = (lag_weather['windspeed_bin_median_NN1'] * w1 +
         lag_weather['windspeed_bin_median_NN2'] * w2 +
         lag_weather['windspeed_bin_median_NN3'] * w3 )/(w1 + w2 + w3)



    # In[52]:

    lag_weather = lag_weather[['date','hour','temp_median_NN','rain_bin_median_NN','windspeed_bin_median_NN']]


    # In[53]:

    lag_weather_summary = pd.merge(weather_initial_summary,lag_weather,
                                   how = 'left', on = ['date','hour'])


    # In[54]:

    data_forecast_nn['hour'] = np.ceil(data_forecast_nn['block_no']/4)


    # In[55]:

    load_lag_weather = pd.merge(data_forecast_nn,
                                lag_weather_summary,how = 'left',
                                on = ['date','hour'])
    load_lag_weather['temp_median_diff_nn'] = (load_lag_weather['temp_median']-load_lag_weather['temp_median_NN'] )


    # In[56]:

    load_lag_weather['temp_median_diff_nn'] = (load_lag_weather['temp_median_NN'] 
                                               - load_lag_weather['temp_median'])
    load_lag_weather['rain_bin_median_diff_nn'] = (load_lag_weather['rain_bin_median_NN'] 
                                                   - load_lag_weather['rain_bin_median'])
    load_lag_weather['windspeed_bin_median_diff_nn'] = (load_lag_weather['windspeed_bin_median_NN'] 
                                                   - load_lag_weather['windspeed_bin_median'])

    load_lag_weather['temp_median_diff_nn_lag1']=(load_lag_weather['temp_median_diff_nn']).shift(96)
    load_lag_weather['temp_median_diff_nn_lag2']=(load_lag_weather['temp_median_diff_nn']).shift(192)
    load_lag_weather['rain_bin_median_diff_nn_lag1']=(load_lag_weather['rain_bin_median_diff_nn']).shift(96)
    load_lag_weather['rain_bin_median_diff_nn_lag2']=(load_lag_weather['rain_bin_median_diff_nn']).shift(192)
    load_lag_weather['rain_bin_median_diff_nn_lag3']=(load_lag_weather['rain_bin_median_diff_nn']).shift(288)
    load_lag_weather['windspeed_bin_median_diff_nn_lag1']=(load_lag_weather['windspeed_bin_median_diff_nn']).shift(96)
    load_lag_weather['windspeed_bin_median_diff_nn_lag2']=(load_lag_weather['windspeed_bin_median_diff_nn']).shift(192)
    load_lag_weather['temp_median_NN_sq'] = load_lag_weather['temp_median_NN']**2


    # In[57]:

    load_lag_weather['residual_pred_factor'] = ((load_lag_weather['endo_demand'] 
                                         - load_lag_weather['NN_PRED_DEMAND'])/ load_lag_weather['NN_PRED_DEMAND'])



    # In[58]:

    load_lag_weather['temp_median_diff_nn'].fillna(0, inplace = True)
    load_lag_weather['temp_median_diff_nn'].fillna(0, inplace = True)


    # In[59]:

    # binning of weather variables  actual 

    min_temp_diff = min(load_lag_weather['temp_median_diff_nn'])
    max_temp_diff = max(load_lag_weather['temp_median_diff_nn'])

    temp_diff_NN_bin = list(linspace(min_temp_diff,max_temp_diff,20))
    series = list(load_lag_weather['temp_median_diff_nn'])
    bin_limit = temp_diff_NN_bin
    cat_var = []
    for j in range(0,len(series)):
        for i in range(0,len(bin_limit)):
            if i == 0 and series[j]<bin_limit[i]: 
                cat_var.append(i)
            elif i > 0 and series[j]>= bin_limit[i-1] and series[j] < bin_limit[i]:
                cat_var.append(i)
            elif i == (len(bin_limit)-1) and series[j]>=bin_limit[i]:
                cat_var.append(i+1) 
    binned_temp_diff = pd.Series(cat_var)
    load_lag_weather['binned_temp_diff'] = binned_temp_diff.values



    min_temp_diff = min(load_lag_weather['temp_median_diff_nn'])
    max_temp_diff = max(load_lag_weather['temp_median_diff_nn'])

    temp_diff_NN_bin = list(linspace(min_temp_diff,max_temp_diff,5))
    series = list(load_lag_weather['temp_median_diff_nn'])
    bin_limit = temp_diff_NN_bin
    cat_var = []
    for j in range(0,len(series)):
        for i in range(0,len(bin_limit)):
            if i == 0 and series[j]<bin_limit[i]: 
                cat_var.append(i)
            elif i > 0 and series[j]>= bin_limit[i-1] and series[j] < bin_limit[i]:
                cat_var.append(i)
            elif i == (len(bin_limit)-1) and series[j]>=bin_limit[i]:
                cat_var.append(i+1) 
    binned_temp_diff = pd.Series(cat_var)
    load_lag_weather['binned_temp_diff'] = binned_temp_diff.values


    # In[60]:

    load_lag_weather['month'] = pd.DatetimeIndex(
        load_lag_weather['date']).month  


    # In[61]:

    hourly_resdidual_temp_summary = (load_lag_weather.groupby(['month','hour','binned_temp_diff'],as_index=False)
        .agg({'residual_pred_factor':'median'
              })
              .rename(columns={'residual_pred_factor':'tempdiff_residual_pred_factor'}))



    # In[62]:

    load_lag_weather['temp_temp_diff_interaction']=(load_lag_weather['temp_median']*
                                                    load_lag_weather['temp_median_diff_nn'])


    # In[63]:

    # Adjusting forecast for temperature deviation

    train = load_lag_weather[load_lag_weather['date']<= current_date_lag]
    test= load_lag_weather[load_lag_weather['date']> current_date_lag]




    min_temp_median = min(train['temp_median'])
    max_temp_median = max(train['temp_median'])

    temp_median_bin = list(linspace(min_temp_median,max_temp_median,40))
    series_train = list(train['temp_median'])
    bin_limit = temp_median_bin
    cat_var_train = []
    for j in range(0,len(series_train)):
        for i in range(0,len(bin_limit)):
            if i == 0 and series_train[j]<bin_limit[i]: 
                cat_var_train.append(i)
            elif i > 0 and series_train[j]>= bin_limit[i-1] and series_train[j] < bin_limit[i]:
                cat_var_train.append(i)
            elif i == (len(bin_limit)-1) and series_train[j]>=bin_limit[i]:
                cat_var_train.append(i+1) 
    binned_temp_median_train = pd.Series(cat_var_train)
    train['temp_median_bin'] = binned_temp_median_train.values



    series_test = list(test['temp_median'])
    cat_var_test = []
    for j in range(0,len(series_test)):
        for i in range(0,len(bin_limit)):
            if i == 0 and series_test[j]<bin_limit[i]: 
                cat_var_test.append(i)
            elif i > 0 and series_test[j]>= bin_limit[i-1] and series_test[j] < bin_limit[i]:
                cat_var_test.append(i)
            elif i == (len(bin_limit)-1) and series_test[j]>=bin_limit[i]:
                cat_var_test.append(i+1) 
    binned_temp_median_test = pd.Series(cat_var_test)
    test['temp_median_bin'] = binned_temp_median_test.values




    min_temp_diff = min(train['temp_median_diff_nn'])
    max_temp_diff = max(train['temp_median_diff_nn'])

    temp_diff_NN_bin = list(linspace(min_temp_diff,max_temp_diff,5))
    series_train = list(train['temp_median_diff_nn'])
    bin_limit = temp_diff_NN_bin
    cat_var_train = []
    for j in range(0,len(series_train)):
        for i in range(0,len(bin_limit)):
            if i == 0 and series_train[j]<bin_limit[i]: 
                cat_var_train.append(i)
            elif i > 0 and series_train[j]>= bin_limit[i-1] and series_train[j] < bin_limit[i]:
                cat_var_train.append(i)
            elif i == (len(bin_limit)-1) and series_train[j]>=bin_limit[i]:
                cat_var_train.append(i+1) 
    binned_temp_diff_train = pd.Series(cat_var_train)
    train['binned_temp_diff'] = binned_temp_diff_train.values


    series_test = list(test['temp_median_diff_nn'])
    bin_limit = temp_diff_NN_bin
    cat_var_test = []
    for j in range(0,len(series_test)):
        for i in range(0,len(bin_limit)):
            if i == 0 and series_test[j]<bin_limit[i]: 
                cat_var_test.append(i)
            elif i > 0 and series_test[j]>= bin_limit[i-1] and series_test[j] < bin_limit[i]:
                cat_var_test.append(i)
            elif i == (len(bin_limit)-1) and series_test[j]>=bin_limit[i]:
                cat_var_test.append(i+1) 
    binned_temp_diff_test = pd.Series(cat_var_test)
    test['binned_temp_diff'] = binned_temp_diff_test.values

    train_test = train.append(test)


    resdidual_temp_summary = (train.groupby(['month',
                                                        'temp_median_bin',
                                                        'hour',
                                                        'binned_temp_diff']
                                                       ,as_index=False)
                                .agg({'residual_pred_factor':'median'})
                              .rename(columns={'residual_pred_factor':
                                               'tempdiff_residual_pred_factor'}))

    tempdiff_residual_pred = pd.merge(train_test,resdidual_temp_summary,
                                how = 'left', on = ['month',
                                                    'temp_median_bin',
                                                    'hour',
                                                    'binned_temp_diff'])

    tempdiff_residual_pred['tempdiff_residual_pred_factor'].fillna(0, inplace = True)
    tempdiff_residual_pred['tempdiff_residual_pred'] = (tempdiff_residual_pred['tempdiff_residual_pred_factor']
                                                  *tempdiff_residual_pred['NN_PRED_DEMAND'])

    tempdiff_residual_pred = tempdiff_residual_pred[np.isfinite(tempdiff_residual_pred['tempdiff_residual_pred'])]
    tempdiff_residual_pred_smooth = pd.DataFrame([])
    unique_date = tempdiff_residual_pred['date'].unique()
    for j in range(0,len(unique_date)):
        test = tempdiff_residual_pred[tempdiff_residual_pred['date']== unique_date[j]]
        s =  np.array(test['tempdiff_residual_pred'])
        yhat = savitzky_golay(s, window_size=5,order=1)
        envelop = pd.DataFrame(yhat)
        envelop=envelop.iloc[0:]
        test['tempdiff_residual_pred_smooth']=envelop.values
        tempdiff_residual_pred_smooth = tempdiff_residual_pred_smooth.append(test)
        
        
    tempdiff_residual_pred_smooth['final_load_pred_tempdiff'] = (tempdiff_residual_pred_smooth['NN_PRED_DEMAND'] + 
                                          tempdiff_residual_pred_smooth['tempdiff_residual_pred_smooth'])


    # In[64]:

    # # Adjusting forecast for Rain

    # Adjusting forecast for Rain
    raindiff_residual_pred = tempdiff_residual_pred_smooth.copy()
    raindiff_residual_pred['residual_pred_rain'] = (raindiff_residual_pred['endo_demand'] - 
                          raindiff_residual_pred['final_load_pred_tempdiff'])

    raindiff_residual_pred['residual_pred_rain_factor'] = (raindiff_residual_pred['residual_pred_rain']
                                                           /raindiff_residual_pred['final_load_pred_tempdiff'])


    raindiff_residual_pred['rain_bin_median_diff_nn_lag1'].fillna(0, inplace=True)
    raindiff_residual_pred['rain_bin_median_diff_nn_lag2'].fillna(0, inplace=True)
    raindiff_residual_pred['rain_bin_median_diff_nn_lag3'].fillna(0, inplace=True)

    train =  raindiff_residual_pred[raindiff_residual_pred['date']<= current_date_lag]
    test =  raindiff_residual_pred[raindiff_residual_pred['date'] > current_date_lag]



    min_rain_bin_median_diff_nn = min(train['rain_bin_median_diff_nn'])
    max_rain_bin_median_diff_nn = max(train['rain_bin_median_diff_nn'])
    rain_median_diff_nn_bin = list(linspace(min_rain_bin_median_diff_nn,max_rain_bin_median_diff_nn,7))
    series_train = list(train['rain_bin_median_diff_nn'])
    bin_limit = rain_median_diff_nn_bin
    cat_var_train = []
    for j in range(0,len(series_train)):
        for i in range(0,len(bin_limit)):
            if i == 0 and series_train[j]<bin_limit[i]: 
                cat_var_train.append(i)
            elif i > 0 and series_train[j]>= bin_limit[i-1] and series_train[j] < bin_limit[i]:
                cat_var_train.append(i)
            elif i == (len(bin_limit)-1) and series_train[j]>=bin_limit[i]:
                cat_var_train.append(i+1) 
    binned_rain_bin_median_diff_nn_train = pd.Series(cat_var_train)
    train['binned_rain_bin_median_diff_nn'] = binned_rain_bin_median_diff_nn_train.values

    series_test = list(test['rain_bin_median_diff_nn'])
    bin_limit = rain_median_diff_nn_bin
    cat_var_test = []
    for j in range(0,len(series_test)):
        for i in range(0,len(bin_limit)):
            if i == 0 and series_test[j]<bin_limit[i]: 
                cat_var_test.append(i)
            elif i > 0 and series_test[j]>= bin_limit[i-1] and series_test[j] < bin_limit[i]:
                cat_var_test.append(i)
            elif i == (len(bin_limit)-1) and series_test[j]>=bin_limit[i]:
                cat_var_test.append(i+1) 
    binned_rain_bin_median_diff_nn_test = pd.Series(cat_var_test)
    test['binned_rain_bin_median_diff_nn'] = binned_rain_bin_median_diff_nn_test.values


    min_rain_bin_median_diff_nn_lag1 = min(train['rain_bin_median_diff_nn_lag1'])
    max_rain_bin_median_diff_nn_lag1 = max(train['rain_bin_median_diff_nn_lag1'])
    rain_median_diff_nn_bin_lag1_bin = list(linspace(min_rain_bin_median_diff_nn_lag1,max_rain_bin_median_diff_nn_lag1,7))
    series_train = list(train['rain_bin_median_diff_nn_lag1'])
    bin_limit_lag1 = rain_median_diff_nn_bin_lag1_bin
    cat_var_lag1_train = []
    for j in range(0,len(series_train)):
        for i in range(0,len(bin_limit_lag1)):
            if i == 0 and series_train[j]<bin_limit_lag1[i]: 
                cat_var_lag1_train.append(i)
            elif i > 0 and series_train[j]>= bin_limit_lag1[i-1] and series_train[j] < bin_limit_lag1[i]:
                cat_var_lag1_train.append(i)
            elif i == (len(bin_limit_lag1)-1) and series_train[j]>=bin_limit_lag1[i]:
                cat_var_lag1_train.append(i+1) 
    binned_rain_bin_median_diff_nn_lag1_train = pd.Series(cat_var_lag1_train)
    train['binned_rain_bin_median_diff_nn_lag1'] = binned_rain_bin_median_diff_nn_lag1_train.values

    series_test = list(test['rain_bin_median_diff_nn_lag1'])
    bin_limit_lag1 = rain_median_diff_nn_bin_lag1_bin
    cat_var_lag1_test = []
    for j in range(0,len(series_test)):
        for i in range(0,len(bin_limit_lag1)):
            if i == 0 and series_test[j]<bin_limit_lag1[i]: 
                cat_var_lag1_test.append(i)
            elif i > 0 and series_test[j]>= bin_limit_lag1[i-1] and series_test[j] < bin_limit_lag1[i]:
                cat_var_lag1_test.append(i)
            elif i == (len(bin_limit_lag1)-1) and series_test[j]>=bin_limit_lag1[i]:
                cat_var_lag1_test.append(i+1) 
    binned_rain_bin_median_diff_nn_lag1_test = pd.Series(cat_var_lag1_test)
    test['binned_rain_bin_median_diff_nn_lag1'] = binned_rain_bin_median_diff_nn_lag1_test.values




    min_rain_bin_median_diff_nn_lag2 = min(train['rain_bin_median_diff_nn_lag2'])
    max_rain_bin_median_diff_nn_lag2 = max(train['rain_bin_median_diff_nn_lag2'])
    rain_median_diff_nn_bin_lag2_bin = list(linspace(min_rain_bin_median_diff_nn_lag2,max_rain_bin_median_diff_nn_lag2,7))
    series_train = list(train['rain_bin_median_diff_nn_lag2'])
    bin_limit_lag2 = rain_median_diff_nn_bin_lag2_bin
    cat_var_lag2_train = []
    for j in range(0,len(series_train)):
        for i in range(0,len(bin_limit_lag2)):
            if i == 0 and series_train[j]<bin_limit_lag2[i]: 
                cat_var_lag2_train.append(i)
            elif i > 0 and series_train[j]>= bin_limit_lag2[i-1] and series_train[j] < bin_limit_lag2[i]:
                cat_var_lag2_train.append(i)
            elif i == (len(bin_limit_lag2)-1) and series_train[j]>=bin_limit_lag2[i]:
                cat_var_lag2_train.append(i+1) 
    binned_rain_bin_median_diff_nn_lag2_train = pd.Series(cat_var_lag2_train)
    train['binned_rain_bin_median_diff_nn_lag2'] = binned_rain_bin_median_diff_nn_lag2_train.values

    series_test = list(test['rain_bin_median_diff_nn_lag2'])
    bin_limit_lag2 = rain_median_diff_nn_bin_lag2_bin
    cat_var_lag2_test = []
    for j in range(0,len(series_test)):
        for i in range(0,len(bin_limit_lag2)):
            if i == 0 and series_test[j]<bin_limit_lag2[i]: 
                cat_var_lag2_test.append(i)
            elif i > 0 and series_test[j]>= bin_limit_lag2[i-1] and series_test[j] < bin_limit_lag2[i]:
                cat_var_lag2_test.append(i)
            elif i == (len(bin_limit_lag2)-1) and series_test[j]>=bin_limit_lag2[i]:
                cat_var_lag2_test.append(i+1) 
    binned_rain_bin_median_diff_nn_lag2_test = pd.Series(cat_var_lag2_test)
    test['binned_rain_bin_median_diff_nn_lag2'] = binned_rain_bin_median_diff_nn_lag2_test.values


    min_rain_bin_median_diff_nn_lag3 = min(train['rain_bin_median_diff_nn_lag3'])
    max_rain_bin_median_diff_nn_lag3 = max(train['rain_bin_median_diff_nn_lag3'])
    rain_median_diff_nn_bin_lag3_bin = list(linspace(min_rain_bin_median_diff_nn_lag3,max_rain_bin_median_diff_nn_lag3,7))
    series_train = list(train['rain_bin_median_diff_nn_lag3'])
    bin_limit_lag3 = rain_median_diff_nn_bin_lag3_bin
    cat_var_lag3_train = []
    for j in range(0,len(series_train)):
        for i in range(0,len(bin_limit_lag3)):
            if i == 0 and series_train[j]<bin_limit_lag3[i]: 
                cat_var_lag3_train.append(i)
            elif i > 0 and series_train[j]>= bin_limit_lag2[i-1] and series_train[j] < bin_limit_lag3[i]:
                cat_var_lag3_train.append(i)
            elif i == (len(bin_limit_lag3)-1) and series_train[j]>=bin_limit_lag3[i]:
                cat_var_lag3_train.append(i+1) 
    binned_rain_bin_median_diff_nn_lag3_train = pd.Series(cat_var_lag3_train)
    train['binned_rain_bin_median_diff_nn_lag3'] = binned_rain_bin_median_diff_nn_lag3_train.values

    series_test = list(test['rain_bin_median_diff_nn_lag3'])
    bin_limit_lag3 = rain_median_diff_nn_bin_lag3_bin
    cat_var_lag3_test = []
    for j in range(0,len(series_test)):
        for i in range(0,len(bin_limit_lag3)):
            if i == 0 and series_test[j]<bin_limit_lag3[i]: 
                cat_var_lag3_test.append(i)
            elif i > 0 and series_test[j]>= bin_limit_lag2[i-1] and series_test[j] < bin_limit_lag3[i]:
                cat_var_lag3_test.append(i)
            elif i == (len(bin_limit_lag3)-1) and series_test[j]>=bin_limit_lag3[i]:
                cat_var_lag3_test.append(i+1) 
    binned_rain_bin_median_diff_nn_lag3_test = pd.Series(cat_var_lag3_test)
    test['binned_rain_bin_median_diff_nn_lag3'] = binned_rain_bin_median_diff_nn_lag3_test.values

    train_test = train.append(test)

    residual_pred_rain_summary = (train.groupby(['binned_rain_bin_median_diff_nn',
                                                                  'binned_rain_bin_median_diff_nn_lag1',
                                                                  'binned_rain_bin_median_diff_nn_lag2',
                                                                  'binned_rain_bin_median_diff_nn_lag3',
                                                                  
                                                        'month','hour']
                                                       ,as_index=False)
                                .agg({'residual_pred_rain_factor':'median'})
                              .rename(columns={'residual_pred_rain_factor':
                                               'raindiff_residual_pred_rain_factor'}))

    raindiff_residual_pred_final = pd.merge(train_test,residual_pred_rain_summary,
                                how = 'left', on = [
                                                    'binned_rain_bin_median_diff_nn',
                                                    'binned_rain_bin_median_diff_nn_lag1',
                                                    'binned_rain_bin_median_diff_nn_lag2',
                                                    'binned_rain_bin_median_diff_nn_lag3',
                    
                                                    'month',
                                                    'hour'])

    raindiff_residual_pred_final['raindiff_residual_pred_rain_factor'].fillna(0, inplace = True)
    raindiff_residual_pred_final['raindiff_residual_pred_rain'] = (raindiff_residual_pred_final['raindiff_residual_pred_rain_factor']
                                                  *raindiff_residual_pred_final['final_load_pred_tempdiff'])

    raindiff_residual_pred_final = raindiff_residual_pred_final[np.isfinite(raindiff_residual_pred_final['raindiff_residual_pred_rain'])]
    raindiff_residual_predsmooth = pd.DataFrame([])
    unique_date = raindiff_residual_pred_final['date'].unique()
    for j in range(0,len(unique_date)):
        test = raindiff_residual_pred_final[raindiff_residual_pred_final['date']== unique_date[j]]
        s =  np.array(test['raindiff_residual_pred_rain'])
        yhat = savitzky_golay(s, window_size=5,order=1)
        envelop = pd.DataFrame(yhat)
        envelop=envelop.iloc[0:]
        test['raindiff_residual_predsmooth']=envelop.values
        raindiff_residual_predsmooth = raindiff_residual_predsmooth.append(test)
        
        
    raindiff_residual_predsmooth['final_load_pred_rain'] = (raindiff_residual_predsmooth['final_load_pred_tempdiff'] + 
                                          raindiff_residual_predsmooth['raindiff_residual_predsmooth'])


    # In[65]:

    KNN_Forecast_table = raindiff_residual_predsmooth[['date','block_no','int_obs_load','endo_demand','NN_PRED_DEMAND',
                                                       'final_load_pred_tempdiff','final_load_pred_rain',
                                                   ]] 

    final_forecast_table = pd.DataFrame([])
    unique_date = KNN_Forecast_table['date'].unique()
    for j in range(0,len(unique_date)):
        test = KNN_Forecast_table[KNN_Forecast_table['date']== unique_date[j]]
        s1 =  np.array(test['NN_PRED_DEMAND'])
        s2 =  np.array(test['final_load_pred_tempdiff'])
        s3 =  np.array(test['final_load_pred_rain'])
        s4 =  np.array(test['int_obs_load'])
    #     s6 =  np.array(test['endo_demand'])
        yhat1 = savitzky_golay(s1, window_size=3,order=1)
        yhat2 = savitzky_golay(s2, window_size=3,order=1)
        yhat3 = savitzky_golay(s3, window_size=3,order=1)
        yhat4 = savitzky_golay(s4, window_size=3,order=1)
        envelop1 = pd.DataFrame(yhat1)
        envelop2 = pd.DataFrame(yhat2)
        envelop3 = pd.DataFrame(yhat3)
        envelop4 = pd.DataFrame(yhat4)
        
        envelop1=envelop1.iloc[0:]
        envelop2=envelop2.iloc[0:]
        envelop3=envelop3.iloc[0:]
        envelop4=envelop4.iloc[0:]
        
        test['NN_PRED_DEMAND_smooth']=envelop1.values
        test['NN_PRED_DEMAND_TEMPDIFF_smooth']=envelop2.values
        test['NN_PRED_DEMAND_RAINDIFF_smooth']=envelop3.values
        test['int_obs_load_smooth']=envelop4.values
        
        final_forecast_table = final_forecast_table.append(test)


    # In[69]:

    demand_forecast = final_forecast_table.copy()

    demand_forecast['demand_forecast'] = demand_forecast['NN_PRED_DEMAND_RAINDIFF_smooth']
    demand_forecast['quantum'] = demand_forecast['int_obs_load_smooth']
    demand_forecast.sort_values(by = ['date','block_no'], 
                                           ascending=[True, True], 
                                           inplace=True)

    demand_forecast['demand_forecast_lag'] = demand_forecast['demand_forecast'].shift(1)
    demand_forecast['quantum_lag'] = demand_forecast['quantum'].shift(1)
    demand_forecast['fcurve'] = demand_forecast['demand_forecast']/demand_forecast['demand_forecast_lag'] 


    # In[70]:


    real_time_forecast_table = demand_forecast[demand_forecast['date']>=current_date_lag]
    quantum_lag = list(real_time_forecast_table['quantum_lag'])

    forecast = list(real_time_forecast_table['demand_forecast'])
    fcurve = list(real_time_forecast_table['fcurve'])
    if np.isnan(quantum_lag[0]):
        quantum_lag[0]=quantum_lag[1]
    real_time_forecast =[]
    for j in range(0,len(real_time_forecast_table)):
        if np.isfinite(quantum_lag[j]):
            new = fcurve[j]*quantum_lag[j] + mrr*(forecast[j] - quantum_lag[j])
            real_time_forecast.append(new)        
        else: 
            new1 = fcurve[j]*real_time_forecast[j-1] + mrr*(forecast[j] - real_time_forecast[j-1])
            real_time_forecast.append(new1)
            
            
    real_time_forecast = pd.DataFrame(real_time_forecast)
    real_time_forecast = real_time_forecast.iloc[0:]
    real_time_forecast_table['real_time_forecast']= real_time_forecast.values
###################################################################
    # In[31]:

    model_master = pd.read_sql_query("""select b.model_master_fk
        from
        power.model_master a,
        power.model_org_map b,
        power.organisation_master c
        where a.id = b.model_master_fk
        and b.organisation_master_fk = c.organisation_master_pk
        and c.organisation_code = '{}'
        and a.model_type = 'SINK'
        and a.model_short_name = '{}'""".format(discom, model_short_name),
                                     engine, index_col=None)

    model_master_fk = model_master.iloc[0]['model_master_fk']

            
    real_time_forecast_table['aggregation_point'] = 'DISCOM'
    real_time_forecast_table['mrr'] = mrr
    real_time_forecast_table['aggregation_type'] = 'REALTIME_DEMAND_FOR'
    real_time_forecast_table['entity_name'] = 'TOTAL_DEMAND'
    real_time_forecast_table['model_master_fk'] = model_master_fk
    real_time_forecast_table['discom'] = discom
    real_time_forecast_table['state'] = state
    real_time_forecast_table['unit'] = 'MW'
    # realtime_forecast['revision'] = max_revision        
    realtime_forecast = real_time_forecast_table[
        ['date', 'block_no', 'model_master_fk', 'real_time_forecast',
         'aggregation_point', 'aggregation_type', 'mrr',
         'entity_name', 'discom', 'state', 'unit']]
    tmp_table_nm = 'sink_realtime_tmp_{}'.format(discom)


    # In[46]:

    realtime_forecast.to_sql(name=tmp_table_nm,
                             con=engine,
                             if_exists='replace')
    cur_date_q = pd.read_sql_query("""select curdate() date""",engine, index_col=None)
    cur_date_obj = cur_date_q.iloc[0]['date']
    # cur_date = cur_date_obj.strftime('%Y-%m-%d')
    # curtime = get_time()
    # cur_block = pd.read_sql_query("""select block_no
    #     from block_master
    #     where '{}'
    #     between start_time
    #     and end_time
    #     """.format(curtime),
    #     engine, index_col=None)
    # cur_block_no = cur_block.iloc[0]['block_no']
    # datedelta, start_block = get_revision(cur_block_no)
    
    datedelta, start_block = get_revision(curblock)
    # print 'start_block', start_block
    end_block = 96
    date_to_update_rev_obj = cur_date_obj + timedelta(days=datedelta)
    date_to_update_rev = date_to_update_rev_obj.strftime('%Y-%m-%d')
    date_block = realtime_forecast[['date', 'block_no']]
    date_block = date_block.reset_index()[['date', 'block_no']]
    udate = list(pd.unique(date_block['date'].dt.strftime('%Y-%m-%d')))


    # In[48]:

    # datedelta, start_block, cur_block_no


    # In[41]:

    udate = list(pd.unique(date_block['date'].dt.strftime('%Y-%m-%d')))
    for xdate in udate:
        if xdate == date_to_update_rev:
            realtime_forecast_staging = pd.read_sql_query("""SELECT
                coalesce(max(revision) + 1, 0) max_revision,
                max(realtime_forecast_metadata_pk) old_id
                from `power`.`realtime_forecast_metadata` a,
                     (select b.model_master_fk
                        from
                        power.model_master a,
                        power.model_org_map b,
                        power.organisation_master c
                        where a.id = b.model_master_fk
                        and b.organisation_master_fk = c.organisation_master_pk
                        and c.organisation_code = '{}'
                        and a.model_type = 'SINK') b
                where a.date = '{}'
                and a.discom = '{}'
                and a.model_master_fk = b.model_master_fk
                """.format(discom, xdate, discom),
                engine, index_col=None)
            max_revision = realtime_forecast_staging.iloc[0]['max_revision']
            old_id = realtime_forecast_staging.iloc[0]['old_id']

            old_rev = max_revision - 1 if max_revision > 0 else 0

            # cur_block_no = cur_block.iloc[0]['block_no']
            sql_str = """update `power`.`realtime_forecast_metadata`
                set to_block_no = {}
                where date = '{}'
                and discom = '{}'
                and revision = {}
                and realtime_forecast_metadata_pk = {}"""\
                .format(start_block - 1, xdate,
                        discom, old_rev, old_id)
            connection = engine.connect()
            connection.execute(sql_str)
            sql_str = """insert into `power`.`realtime_forecast_metadata`
                (date, revision, mrr, model_master_fk,
                from_block_no, to_block_no, discom, state)
                select distinct date, {}, mrr, model_master_fk,
                {} from_block_no, {} to_block_no, discom, state
                from {} where date = '{}'
                on duplicate key
                update mrr = values(mrr)"""\
                .format(max_revision, start_block, end_block,
                        tmp_table_nm, xdate)
            connection = engine.connect()
            connection.execute(sql_str)
            get_last_insert_pk = pd.read_sql_query("""select realtime_forecast_metadata_pk
                from `power`.`realtime_forecast_metadata`
                where date = '{}'
                and mrr = {}
                and revision = {}
                and discom ='{}'
                and model_master_fk = {}
                """.format(xdate, mrr, max_revision, discom, model_master_fk),
                engine, index_col=None)
            connection = engine.connect()
            connection.execute(sql_str)
            last_ins_pk =             get_last_insert_pk.iloc[0]['realtime_forecast_metadata_pk']
            sql_str = """insert into `power`.`realtime_forecast`
            (realtime_forecast_metadata_fk, date, block_no,
             revision, aggregation_point,
             aggregation_type,
             entity_name, forecast_quantum, unit)
             select {}, b.date, b.block_no, {},
              b.aggregation_point, aggregation_type,
              b.entity_name,
              round(b.real_time_forecast, 2),
              b.unit
            from {} b
            where b.date = '{}'
            and b.block_no >= {}
            on duplicate key
            update realtime_forecast_metadata_fk = {},
            revision = {},
            forecast_quantum = round(b.real_time_forecast, 2),
            unit = values(unit)"""\
            .format(last_ins_pk, max_revision, tmp_table_nm,
                    xdate, start_block, last_ins_pk, max_revision)
            connection = engine.connect()
            connection.execute(sql_str)
        elif xdate > date_to_update_rev:
            max_revision = 0
            start_block = 1
            end_block = 96
            sql_str = """insert into `power`.`realtime_forecast_metadata`
                (date, revision, mrr, model_master_fk,
                from_block_no, to_block_no, discom, state)
                select distinct date, {}, mrr, model_master_fk,
                {} from_block_no, {} to_block_no, discom, state
                from {} where date = '{}'
                on duplicate key
                update mrr = values(mrr)
                """.format(max_revision, start_block,
                           end_block, tmp_table_nm, xdate)
            connection = engine.connect()
            connection.execute(sql_str)
            get_last_insert_pk = pd.read_sql_query("""select
                realtime_forecast_metadata_pk
                from `power`.`realtime_forecast_metadata`
                where date = '{}'
                and mrr = {}
                and revision = {}
                and discom ='{}'
                and model_master_fk = {}
                """.format(xdate, mrr, max_revision, discom, model_master_fk),
                engine, index_col=None)
            connection = engine.connect()
            connection.execute(sql_str)
            last_ins_pk =             get_last_insert_pk.iloc[0]['realtime_forecast_metadata_pk']
            sql_str = """insert into `power`.`realtime_forecast`
            (realtime_forecast_metadata_fk, date,
             block_no, revision, aggregation_point,
             aggregation_type, entity_name, forecast_quantum, unit)
             select {} , b.date, b.block_no, {},
              b.aggregation_point, aggregation_type,
              b.entity_name,
              round(b.real_time_forecast, 2),
              b.unit
            from {} b
            where b.date = '{}'
            on duplicate key
            update realtime_forecast_metadata_fk = {},
            revision = {},
            forecast_quantum = round(b.real_time_forecast, 2),
            unit = values(unit)""".format(last_ins_pk, max_revision,
                                          tmp_table_nm,
                                          xdate, last_ins_pk, max_revision)
            connection = engine.connect()
            connection.execute(sql_str)

    rpms_sql_str = """insert into realtime_position_map_staging
        (date, block_no, revision, pool_name, pool_type, quantum,
         unit, discom, state)
        select a.date, b.block_no,  b.revision, c.pool_name, c.pool_type,
                sum(b.forecast_quantum) as quantum, b.unit, a.discom, a.state
        from power.realtime_forecast_metadata a,
             power.realtime_forecast b,
             power.model_master g,
            (select * from
            power.pool_master
            where pool_name = 'REALTIME_DEMAND_FOR'
            and pool_type = 'UNKNOWN'
            and delete_ind = 0) c,
            power.organisation_master d,
            power.block_master e,
            power.uom_master f
        where a.date = b.date
        and a.realtime_forecast_metadata_pk =  b.realtime_forecast_metadata_fk
        and g.id =  a.model_master_fk
        and g.model_type = 'SINK'
        and a.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        and a.discom = '{}'
        and b.block_no = e.block_no
        and d.organisation_code = a.discom
        and f.uom_code = b.unit
        and c.pool_name = b.aggregation_type
        and c.pool_type = b.aggregation_sub_type
        group by a.date, b.block_no,  b.revision,
        c.pool_name, c.pool_type, b.unit, a.discom, a.state
        on duplicate key update
        revision = values(revision),
        quantum = values(quantum),
        unit = values(unit)""".format(discom)
    connection = engine.connect()
    connection.execute(rpms_sql_str)
    pm_sql_str = """insert into power.position_map
        ( date, block_no_fk, revision, organisation_master_fk, pool_master_fk,
          uom_master_fk, model_master_fk, entity_name, quantum)
        select  a.date, e.block_no_pk, a.revision,
        d.organisation_master_pk, c.pool_master_pk,
        f.uom_pk , a.model_master_fk, b.entity_name, b.forecast_quantum
        from power.realtime_forecast_metadata a,
             power.realtime_forecast b,
             power.model_master g,
            (select * from
            power.pool_master
            where pool_name = 'REALTIME_DEMAND_FOR'
            and pool_type = 'UNKNOWN'
            and delete_ind = 0) c,
            power.organisation_master d,
            power.block_master e,
            power.uom_master f
        where a.date = b.date
        and a.realtime_forecast_metadata_pk =  b.realtime_forecast_metadata_fk
        and g.id =  a.model_master_fk
        and g.model_type = 'SINK'
        and a.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        and a.discom = '{}'
        and b.block_no = e.block_no
        and d.organisation_code = a.discom
        and f.uom_code = b.unit
        and c.pool_name = b.aggregation_type
        and c.pool_type = b.aggregation_sub_type
        on duplicate key update
        revision = coalesce(a.revision, position_map.revision),
        quantum = coalesce(b.forecast_quantum, position_map.quantum),
        model_master_fk = coalesce(a.model_master_fk,
            position_map.model_master_fk)""".format(discom)
    connection = engine.connect()
    connection.execute(pm_sql_str)
    update_last_run_block(date, curblock, discom, engine)
    engine.dispose()
    return


def realtime_generation_wind(config, date, model_short_name,
                             mrr, discom, state):
    """Wind Forecast."""
    pool_name = 'INT_GENERATION_FOR'
    pool_type = 'WIND'
    engine = create_engine(config, echo=False)
    valid_date = pd.to_datetime(date, format='%d-%m-%Y') - pd.DateOffset(20)

    model_master = pd.read_sql_query("""SELECT b.model_master_fk,
        a.model_name
        from
        power.model_master a,
        power.model_org_map b,
        power.organisation_master c
        where a.id = b.model_master_fk
        and b.organisation_master_fk = c.organisation_master_pk
        and c.organisation_code = '{}'
        and a.model_type = 'INJECTION'
        and a.model_short_name = '{}'""".format(discom, model_short_name),
                                     engine, index_col=None)
    model_master_fk = model_master.iloc[0]['model_master_fk']
    model_name = model_master.iloc[0]['model_name']
    gen_forecast_stg = pd.read_sql_query("""SELECT date as date,
        block_no as block_no,
        sum(gen_forecast) as gen_forecast, pool_type,
        model_name as model_name
        from gen_forecast_stg where
        date >= '{}'
        and model_name = '{}'
        and state = '{}'
        and pool_name = '{}'
        and pool_type = '{}'
        and org_name = '{}'
        and revision = 0
        group by date, block_no, pool_name,
        pool_type, model_name, org_name
        """.format(valid_date, model_name,
                   state, pool_name, pool_type, discom),
        engine, index_col=None)

    # gen_forecast_stg = pd.read_sql_query("""SELECT a.date,
    #     a.block_no, a.schedule gen_forecast, a.pool_type
    #     from power.position_map_staging a,
    #     (select date, discom, pool_name, pool_type, max(revision) max_revision
    #     from power.position_map_staging
    #     where date >= '{}'
    #     and pool_name = '{}'
    #     and pool_type = '{}'
    #     and discom = '{}'
    #     group by date, discom ,pool_name, pool_type) b
    #     where a.date = b.date
    #     and a.pool_name = b.pool_name
    #     and a.pool_type = b.pool_type
    #     and a.revision = b.max_revision
    #     and a.discom = b.discom
    #     """.format(valid_date, pool_name, pool_type, discom), engine,
    #     index_col=None)
    gen_forecast_stg['date'] = pd.to_datetime(gen_forecast_stg['date'])

    # realtime_gen = pd.read_sql_query("""SELECT
    #         a.date, d.organisation_code, e.block_no,
    #         c.pool_name, c.pool_type,
    #         sum(a.quantum) quantum
    #         from position_map a,
    #         (select date, pool_name, pool_type,
    #          organisation_code, max(revision) max_revision
    #          from position_map a,
    #               pool_master b,
    #               organisation_master c
    #          where a.pool_master_fk = b.pool_master_pk
    #          and a.organisation_master_fk = c.organisation_master_pk
    #          and b.pool_name = 'INT_GENERATION_ACT'
    #          and b.pool_type = '{}'
    #          and c.organisation_code = '{}'
    #          and a.date >= '{}'
    #          group by date, pool_name,pool_type, organisation_code) b,
    #         pool_master c,
    #         organisation_master d,
    #         block_master e
    #         where a.date = b.date
    #         and a.pool_master_fk = c.pool_master_pk
    #         and a.organisation_master_fk = d.organisation_master_pk
    #         and a.block_no_fk = e.block_no_pk
    #         and c.pool_name = b.pool_name
    #         and c.pool_type = b.pool_type
    #         and a.revision = b.max_revision
    #         and d.organisation_code = b.organisation_code
    #         group by a.date, d.organisation_code,
    #         e.block_no, c.pool_name, c.pool_type
    #         """.format(pool_type, discom, valid_date),
    #     engine, index_col=None)

    # realtime_gen['date'] = pd.to_datetime(realtime_gen['date'])
    # # realtime_gen = realtime_gen[realtime_gen['date'] >= valid_date]
    # realtime_gen = realtime_gen[np.isfinite(realtime_gen['quantum'])]
    # realtime_gen_smooth = pd.DataFrame([])
    # unique_date = realtime_gen['date'].unique()
    # for j in range(0, len(unique_date)):
    #     test = realtime_gen[realtime_gen['date'] == unique_date[j]]
    #     s = np.array(test['quantum'])
    #     yhat = savitzky_golay(s, window_size=11, order=1)
    #     envelop = pd.DataFrame(yhat)
    #     envelop = envelop.iloc[0:]
    #     test['quantum_smooth'] = envelop.values
    #     realtime_gen_smooth = realtime_gen_smooth.append(test)

    # gen_forecast_stg = \
    #     gen_forecast_stg[np.isfinite(gen_forecast_stg['gen_forecast'])]
    # gen_forecast_smooth = pd.DataFrame([])
    # unique_date = gen_forecast_stg['date'].unique()
    # for j in range(0, len(unique_date)):
    #     test = gen_forecast_stg[gen_forecast_stg['date'] == unique_date[j]]
    #     s = np.array(test['gen_forecast'])
    #     yhat = savitzky_golay(s, window_size=11, order=1)
    #     envelop = pd.DataFrame(yhat)
    #     envelop = envelop.iloc[0:]
    #     test['gen_forecast_smooth'] = envelop.values
    #     gen_forecast_smooth = gen_forecast_smooth.append(test)

    # gen_forecast = pd.merge(gen_forecast_smooth, realtime_gen_smooth,
    #                         how='left', on=['date', 'block_no'])

    # gen_forecast.sort_values(by=['date', 'block_no'],
    #                          ascending=[True, True],
    #                          inplace=True)
    # gen_forecast['quantum_lag'] = gen_forecast['quantum_smooth'].shift(1)

    # gen_forecast['fcurve'] = (gen_forecast['gen_forecast_smooth'] /
    #                           gen_forecast['gen_forecast_smooth'].shift(1))

    # gen_forecast['fcurve'] = np.where((gen_forecast['block_no'] == 1),
    #                                   gen_forecast['fcurve'].shift(-1),
    #                                   gen_forecast['fcurve'])

    # gen_forecast.sort_values(by=['date', 'block_no'],
    #                          ascending=[True, True],
    #                          inplace=True)

    # date_block = gen_forecast[['date', 'block_no']]
    # date_block = date_block.reset_index()[['date', 'block_no']]
    # quantum = list(gen_forecast['quantum_smooth'])
    # quantum_lag = list(gen_forecast['quantum_lag'])
    # if math.isnan(quantum_lag[0]):
    #     quantum_lag[0] = quantum_lag[1]
    # forecast = list(gen_forecast['gen_forecast'])
    # fcurve = list(gen_forecast['fcurve'])
    # real_time_forecast = []

    # for j in range(0, len(quantum)):
    #     if quantum_lag[j] > 0:
    #         new = fcurve[j] * quantum_lag[j] + mrr * \
    #             (forecast[j] - quantum_lag[j])
    #         real_time_forecast.append(new)
    #     else:
    #         new1 = fcurve[j] * real_time_forecast[j - 1] + \
    #             mrr * (forecast[j] - real_time_forecast[j - 1])
    #         real_time_forecast.append(new1)
    # realtime_forecast = pd.DataFrame(real_time_forecast)
    # realtime_forecast.rename(columns={0: 'real_time_forecast'}, inplace=True)
    # realtime_forecast = pd.concat([date_block, realtime_forecast], axis=1)

    # realtime_forecast_smooth = pd.DataFrame([])
    # unique_date = realtime_forecast['date'].unique()
    # for j in range(0, len(unique_date)):
    #     test = realtime_forecast[realtime_forecast['date'] == unique_date[j]]
    #     s = np.array(test['real_time_forecast'])
    #     yhat = savitzky_golay(s, window_size=9, order=1)
    #     envelop = pd.DataFrame(yhat)
    #     envelop = envelop.iloc[0:]
    #     test['real_time_forecast_smooth'] = envelop.values
    #     realtime_forecast_smooth = realtime_forecast_smooth.append(test)

    realtime_forecast_smooth = gen_forecast_stg.copy()
    realtime_forecast_smooth.rename(columns={'gen_forecast': 'real_time_forecast_smooth'}, inplace=True)

    realtime_forecast_smooth['real_time_forecast_smooth'] = \
        np.where(
            (realtime_forecast_smooth['real_time_forecast_smooth'] >= 3800),
            3800,
            realtime_forecast_smooth['real_time_forecast_smooth'])
    realtime_forecast_smooth['real_time_forecast_smooth'] = \
        np.where(
            (realtime_forecast_smooth['real_time_forecast_smooth'] <= 100),
            100,
            realtime_forecast_smooth['real_time_forecast_smooth'])

    realtime_forecast_smooth['aggregation_point'] = 'UNIT'
    realtime_forecast_smooth['mrr'] = mrr
    realtime_forecast_smooth['model_master_fk'] = model_master_fk
    realtime_forecast_smooth['unit'] = 'MW'
    realtime_forecast_smooth['organisation_code'] = discom
    realtime_forecast_smooth['state'] = state
    realtime_forecast_smooth['pool_name'] = pool_name
    realtime_forecast_smooth['pool_type'] = pool_type

    realtime_forecast_smooth = realtime_forecast_smooth[
        ['date', 'block_no', 'model_master_fk', 'real_time_forecast_smooth',
         'aggregation_point', 'pool_name', 'pool_type', 'mrr',
         'organisation_code', 'state', 'unit']]

    realtime_forecast_smooth.rename(
        columns={'real_time_forecast_smooth': 'real_time_forecast'},
        inplace=True)

    # realtime_forecast_smooth.rename(
    #     columns={'smooth_forecast': 'real_time_forecast'},
    #     inplace=True)
    tmp_table_nm = 'injection_realtime_wind_tmp_{}'.format(discom)
    # realtime_forecast_wind_final.to_sql(name=tablename, con=engine,
    #                                     if_exists='replace')
    realtime_forecast_smooth.to_sql(name=tmp_table_nm, con=engine,
                                    if_exists='replace')
    cur_date_q = pd.read_sql_query("""select curdate() date""",
                                   engine, index_col=None)
    cur_date_obj = cur_date_q.iloc[0]['date']
    # cur_date = cur_date_obj.strftime('%Y-%m-%d')
    curtime = get_time()
    cur_block = pd.read_sql_query("""select block_no
        from block_master
        where '{}'
        between start_time
        and end_time
        """.format(curtime),
        engine, index_col=None)
    cur_block_no = cur_block.iloc[0]['block_no']
    datedelta, start_block = get_revision(cur_block_no)
    # print 'start_block', start_block
    end_block = 96
    date_to_update_rev_obj = cur_date_obj + timedelta(days=datedelta)
    date_to_update_rev = date_to_update_rev_obj.strftime('%Y-%m-%d')
    udate = list(
        pd.unique(realtime_forecast_smooth['date'].dt.strftime('%Y-%m-%d')))
    hdr_skip_flag = False
    for xdate in udate:
        if xdate == date_to_update_rev:
            realtime_forecast_staging = pd.read_sql_query("""SELECT
                coalesce(max(revision) + 1, 0) max_revision,
                max(realtime_forecast_metadata_pk) old_id
                from `power`.`realtime_forecast_metadata` a,
                     (select b.model_master_fk
                        from
                        power.model_master a,
                        power.model_org_map b,
                        power.organisation_master c
                        where a.id = b.model_master_fk
                        and b.organisation_master_fk = c.organisation_master_pk
                        and c.organisation_code = '{}'
                        and a.model_type = 'INJECTION') b
                where a.date = '{}'
                and a.discom = '{}'
                and a.model_master_fk = b.model_master_fk
                """.format(discom, xdate, discom),
                engine, index_col=None)

            max_revision = realtime_forecast_staging.iloc[0]['max_revision']
            old_id = realtime_forecast_staging.iloc[0]['old_id']
            if max_revision > 0:
                old_id_count = pd.read_sql_query("""SELECT count(1) count
                    from `power`.`realtime_forecast` a
                    where realtime_forecast_metadata_fk = {}
                    and aggregation_sub_type = '{}'
                    """.format(old_id, pool_type),
                    engine, index_col=None)
                old_count = old_id_count.iloc[0]['count']

            if old_count == 0:
                hdr_skip_flag = True
                old_id_block_rng = pd.read_sql_query("""SELECT
                    from_block_no, to_block_no
                    from `power`.`realtime_forecast_metadata` a
                    where realtime_forecast_metadata_pk = {}
                    """.format(old_id),
                    engine, index_col=None)
                start_block = old_id_block_rng.iloc[0]['from_block_no']
                end_block = old_id_block_rng.iloc[0]['to_block_no']
                max_revision = max_revision - 1
                last_ins_pk = old_id
            else:
                old_rev = max_revision - 1 if max_revision > 0 else 0
                sql_str = """update `power`.`realtime_forecast_metadata`
                    set to_block_no = {}
                    where date = '{}'
                    and discom = '{}'
                    and revision = {}
                    and realtime_forecast_metadata_pk = {}
                    """.format(start_block - 1, xdate,
                               discom, old_rev, old_id)
                connection = engine.connect()
                connection.execute(sql_str)

            if not hdr_skip_flag:
                sql_str = """insert into `power`.`realtime_forecast_metadata`
                    (date, revision, mrr, model_master_fk,
                    from_block_no, to_block_no, discom, state)
                    select distinct date, {}, mrr, model_master_fk,
                    {} from_block_no, {} to_block_no, organisation_code, state
                    from {} where date = '{}'
                    on duplicate key
                    update mrr = values(mrr)
                    """.format(max_revision, start_block,
                               end_block, tmp_table_nm, xdate)
                connection = engine.connect()
                connection.execute(sql_str)
                get_last_insert_pk = pd.read_sql_query("""select realtime_forecast_metadata_pk
                    from `power`.`realtime_forecast_metadata`
                    where date = '{}'
                    and mrr = {}
                    and revision = {}
                    and discom ='{}'
                    and model_master_fk = {}
                    """.format(xdate, mrr, max_revision,
                               discom, model_master_fk),
                    engine, index_col=None)
                connection = engine.connect()
                connection.execute(sql_str)
                last_ins_pk = \
                    get_last_insert_pk.iloc[0]['realtime_forecast_metadata_pk']

            sql_str = """insert into `power`.`realtime_forecast`
            (realtime_forecast_metadata_fk, date, block_no,
            forecast_quantum, revision,
            aggregation_point, aggregation_type,
            aggregation_sub_type, entity_name, unit)
            select {}, h.date, h.block_no,
            round(coalesce(a.rated_unit_capacity/tot.total_capacity *
                           real_time_forecast, 0),2) gen_forecast,
            {} revision, h.aggregation_point,
            h.pool_name, h.pool_type, a.unit_name, h.unit
            from
            (select sum(a.rated_unit_capacity) total_capacity
            from power.unit_master a,
            power.unit_type c,
            (select a.contract_trade_master_pk, a.delivery_start_date,
             a.delivery_end_date,
             c.counter_party_name cp1, d.counter_party_name cp2
            from power.contract_trade_master a,
                 power.counter_party_master c,
                 power. counter_party_master d
            where a.counter_party_1_fk = c.counter_party_master_pk
            and   a.counter_party_2_fk = d.counter_party_master_pk
            and c.counter_party_name = '{}'
            and curdate() between a.delivery_start_date and a.delivery_end_date
            and a.delete_ind = 0
            and c.delete_ind = 0
            and d.delete_ind = 0) g
            where c.unit_type_pk = a.unit_type_fk
            and a.unit_name = g.cp2
            and c.unit_type_name = '{}') tot,
            power.unit_master a,
            power.unit_type c,
            (select a.contract_trade_master_pk, a.delivery_start_date,
             a.delivery_end_date,
             c.counter_party_name cp1, d.counter_party_name cp2
            from power.contract_trade_master a,
                 power.counter_party_master c,
                 power. counter_party_master d
            where a.counter_party_1_fk = c.counter_party_master_pk
            and   a.counter_party_2_fk = d.counter_party_master_pk
            and c.counter_party_name = '{}'
            and a.delete_ind = 0
            and c.delete_ind = 0
            and d.delete_ind = 0) g,
            {} h
            where c.unit_type_pk = a.unit_type_fk
            and a.unit_name = g.cp2
            and date(h.date) between
                g.delivery_start_date and g.delivery_end_date
            and c.unit_type_name = h.pool_type
            and h.date = '{}'
            and h.block_no >= {}
            on duplicate key update
            realtime_forecast_metadata_fk = {},
            revision = {},
            forecast_quantum =
                round(coalesce(a.rated_unit_capacity/tot.total_capacity *
                               real_time_forecast, 0),2),
            unit = values(unit)
            """.format(last_ins_pk, max_revision, discom,
                       pool_type, discom, tmp_table_nm, xdate,
                       start_block, last_ins_pk, max_revision)
            connection = engine.connect()
            connection.execute(sql_str)
        elif xdate > date_to_update_rev:
            max_revision = 0
            start_block = 1
            end_block = 96

            sql_str = """insert into `power`.`realtime_forecast_metadata`
                (date, revision, mrr, model_master_fk,
                from_block_no, to_block_no, discom, state)
                select distinct date, {}, mrr, model_master_fk,
                {} from_block_no, {} to_block_no, organisation_code, state
                from {} where date = '{}'
                on duplicate key
                update mrr = values(mrr)
                """.format(max_revision, start_block,
                           end_block, tmp_table_nm, xdate)
            connection = engine.connect()
            connection.execute(sql_str)
            get_last_insert_pk = pd.read_sql_query("""select realtime_forecast_metadata_pk
                from `power`.`realtime_forecast_metadata`
                where date = '{}'
                and mrr = {}
                and revision = {}
                and discom ='{}'
                and model_master_fk = {}
                """.format(xdate, mrr, max_revision,
                           discom, model_master_fk),
                engine, index_col=None)
            connection = engine.connect()
            connection.execute(sql_str)
            last_ins_pk = \
                get_last_insert_pk.iloc[0]['realtime_forecast_metadata_pk']
            sql_str = """insert into `power`.`realtime_forecast`
            (realtime_forecast_metadata_fk, date, block_no,
            forecast_quantum, revision,
            aggregation_point, aggregation_type,
            aggregation_sub_type, entity_name, unit)
            select {}, h.date, h.block_no,
            round(coalesce(a.rated_unit_capacity/tot.total_capacity *
                           real_time_forecast, 0),2) gen_forecast,
            {} revision, h.aggregation_point,
            h.pool_name, h.pool_type, a.unit_name, h.unit
            from
            (select sum(a.rated_unit_capacity) total_capacity
            from power.unit_master a,
            power.unit_type c,
            (select a.contract_trade_master_pk, a.delivery_start_date,
             a.delivery_end_date,
             c.counter_party_name cp1, d.counter_party_name cp2
            from power.contract_trade_master a,
                 power.counter_party_master c,
                 power. counter_party_master d
            where a.counter_party_1_fk = c.counter_party_master_pk
            and   a.counter_party_2_fk = d.counter_party_master_pk
            and c.counter_party_name = '{}'
            and curdate() between a.delivery_start_date and a.delivery_end_date
            and a.delete_ind = 0
            and c.delete_ind = 0
            and d.delete_ind = 0) g
            where c.unit_type_pk = a.unit_type_fk
            and a.unit_name = g.cp2
            and c.unit_type_name = '{}') tot,
            power.unit_master a,
            power.unit_type c,
            (select a.contract_trade_master_pk, a.delivery_start_date,
             a.delivery_end_date,
             c.counter_party_name cp1, d.counter_party_name cp2
            from power.contract_trade_master a,
                 power.counter_party_master c,
                 power. counter_party_master d
            where a.counter_party_1_fk = c.counter_party_master_pk
            and   a.counter_party_2_fk = d.counter_party_master_pk
            and c.counter_party_name = '{}'
            and a.delete_ind = 0
            and c.delete_ind = 0
            and d.delete_ind = 0) g,
            {} h
            where c.unit_type_pk = a.unit_type_fk
            and a.unit_name = g.cp2
            and date(h.date) between
                g.delivery_start_date and g.delivery_end_date
            and c.unit_type_name = h.pool_type
            and h.date = '{}'
            on duplicate key update
            realtime_forecast_metadata_fk = {},
            revision = {},
            forecast_quantum =
                round(coalesce(a.rated_unit_capacity/tot.total_capacity *
                               real_time_forecast, 0),2),
            unit = values(unit)""".format(last_ins_pk, max_revision, discom,
                                          pool_type, discom, tmp_table_nm,
                                          xdate, last_ins_pk, max_revision)
            connection = engine.connect()
            connection.execute(sql_str)
    rpms_sql_str = """insert into realtime_position_map_staging
        (date, block_no, revision, pool_name,
         pool_type, quantum, unit, discom, state)
        select a.date, b.block_no,  b.revision, c.pool_name, c.pool_type,
                sum(b.forecast_quantum) as quantum, b.unit, a.discom, a.state
        from power.realtime_forecast_metadata a,
             power.realtime_forecast b,
             power.model_master g,
            (select * from
            power.pool_master
            where pool_name = 'INT_GENERATION_FOR'
            and pool_type = '{}'
            and delete_ind = 0) c,
            power.organisation_master d,
            power.block_master e,
            power.uom_master f
        where a.date = b.date
        and a.realtime_forecast_metadata_pk =  b.realtime_forecast_metadata_fk
        and g.id =  a.model_master_fk
        and g.model_type = 'INJECTION'
        and a.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        and a.discom = '{}'
        and b.block_no = e.block_no
        and d.organisation_code = a.discom
        and f.uom_code = b.unit
        and c.pool_name = b.aggregation_type
        and c.pool_type = b.aggregation_sub_type
        group by a.date, b.block_no,  b.revision,
        c.pool_name, c.pool_type, b.unit, a.discom, a.state
        on duplicate key update
        revision = values(revision),
        quantum = values(quantum),
        unit = values(unit)""".format(pool_type, discom)
    connection = engine.connect()
    connection.execute(rpms_sql_str)
    pm_sql_str = """insert into power.position_map
        ( date, block_no_fk, revision, organisation_master_fk, pool_master_fk,
          uom_master_fk, model_master_fk, entity_name, quantum)
        select  a.date, e.block_no_pk, a.revision,
        d.organisation_master_pk, c.pool_master_pk,
        f.uom_pk , a.model_master_fk, b.entity_name, b.forecast_quantum
        from power.realtime_forecast_metadata a,
             power.realtime_forecast b,
             power.model_master g,
            (select * from
            power.pool_master
            where pool_name = 'INT_GENERATION_FOR'
            and pool_type = '{}'
            and delete_ind = 0) c,
            power.organisation_master d,
            power.block_master e,
            power.uom_master f
        where a.date = b.date
        and a.realtime_forecast_metadata_pk =  b.realtime_forecast_metadata_fk
        and g.id =  a.model_master_fk
        and g.model_type = 'INJECTION'
        and a.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        and a.discom = '{}'
        and b.block_no = e.block_no
        and d.organisation_code = a.discom
        and f.uom_code = b.unit
        and c.pool_name = b.aggregation_type
        and c.pool_type = b.aggregation_sub_type
        on duplicate key update
        revision = coalesce(a.revision, position_map.revision),
        quantum = coalesce(b.forecast_quantum, position_map.quantum),
        model_master_fk = coalesce(a.model_master_fk,
            position_map.model_master_fk)""".format(pool_type, discom)
    connection = engine.connect()
    connection.execute(pm_sql_str)
    engine.dispose()
    return


def realtime_generation_solar(config, date, model_short_name,
                              mrr, discom, state):
    """Solar Forecast."""
    pool_name = 'INT_GENERATION_FOR'
    pool_type = 'SOLAR'
    engine = create_engine(config, echo=False)
    valid_date = pd.to_datetime(date, format='%d-%m-%Y') + pd.DateOffset(0)
    model_master = pd.read_sql_query("""SELECT b.model_master_fk,
        a.model_name
        from
        power.model_master a,
        power.model_org_map b,
        power.organisation_master c
        where a.id = b.model_master_fk
        and b.organisation_master_fk = c.organisation_master_pk
        and c.organisation_code = '{}'
        and a.model_type = 'INJECTION'
        and a.model_short_name = '{}'""".format(discom, model_short_name),
                                     engine, index_col=None)
    model_master_fk = model_master.iloc[0]['model_master_fk']
    # model_name = model_master.iloc[0]['model_name']
    model_name = 'NN'
    gen_forecast_stg = pd.read_sql_query("""SELECT
        date as date, block_no as block_no,
        sum(gen_forecast) as gen_forecast, pool_type,
        model_name as model_name
        from gen_forecast_stg where
        date = '{}'
        and model_name = '{}'
        and state = '{}'
        and pool_name = '{}'
        and pool_type = '{}'
        and org_name = '{}'
        and revision = 0
        group by date, block_no, pool_name,
        pool_type, model_name, org_name
        """.format(valid_date, model_name,
                   state, pool_name, pool_type, discom), engine,
        index_col=None)
    # gen_forecast_stg = pd.read_sql_query("""SELECT a.date,
    #     a.block_no, a.schedule gen_forecast, a.pool_type
    #     from power.position_map_staging a,
    #     (select date, discom, pool_name, pool_type, max(revision) max_revision
    #     from power.position_map_staging
    #     where date >= '{}'
    #     and pool_name = '{}'
    #     and pool_type = '{}'
    #     and discom = '{}'
    #     group by date, discom ,pool_name, pool_type) b
    #     where a.date = b.date
    #     and a.pool_name = b.pool_name
    #     and a.pool_type = b.pool_type
    #     and a.revision = b.max_revision
    #     and a.discom = b.discom
    #     """.format(valid_date, pool_name, pool_type, discom), engine,
    #     index_col=None)
    gen_forecast_stg['date'] = pd.to_datetime(gen_forecast_stg['date'])

    realtime_gen = pd.read_sql_query("""SELECT
            a.date, d.organisation_code, e.block_no,
            c.pool_name, c.pool_type,
            sum(a.quantum) quantum
            from position_map a,
            (select date, pool_name, pool_type,
             organisation_code, max(revision) max_revision
             from position_map a,
                  pool_master b,
                  organisation_master c
             where a.pool_master_fk = b.pool_master_pk
             and a.organisation_master_fk = c.organisation_master_pk
             and b.pool_name = 'INT_GENERATION_ACT'
             and b.pool_type = '{}'
             and c.organisation_code = '{}'
             group by date, pool_name,pool_type, organisation_code) b,
            pool_master c,
            organisation_master d,
            block_master e
            where a.date = b.date
            and a.pool_master_fk = c.pool_master_pk
            and a.organisation_master_fk = d.organisation_master_pk
            and a.block_no_fk = e.block_no_pk
            and c.pool_name = b.pool_name
            and c.pool_type = b.pool_type
            and a.revision = b.max_revision
            and d.organisation_code = b.organisation_code
            and a.date = '{}'
            group by a.date, d.organisation_code,
            e.block_no, c.pool_name, c.pool_type
            """.format(pool_type, discom, valid_date), engine, index_col=None)

    realtime_gen['date'] = pd.to_datetime(realtime_gen['date'])
    realtime_gen = realtime_gen[realtime_gen['date'] == valid_date]
    realtime_gen = realtime_gen[['date', 'block_no', 'quantum']]
    gen_forecast = \
        pd.merge(gen_forecast_stg, realtime_gen,
                 how='left',
                 on=['date', 'block_no'])
    gen_forecast['day_time'] = \
        np.where((gen_forecast['gen_forecast'] <= 0), 0, 1)
    min_max_bloc_daytime = gen_forecast[gen_forecast['day_time'] == 1]
    min_block = min(min_max_bloc_daytime['block_no'])
    max_block = max(min_max_bloc_daytime['block_no'])

    gen_forecast_daytime = \
        gen_forecast[(gen_forecast['block_no'] >= min_block) &
                     (gen_forecast['block_no'] <= max_block)]
    gen_forecast_daytime.sort_values(by=['date', 'block_no'],
                                     ascending=[True, True],
                                     inplace=True)
    gen_forecast_daytime['quantum_lag1'] = \
        gen_forecast_daytime['quantum'].shift(1)
    gen_forecast_daytime['gen_forecast_lag1'] = \
        gen_forecast_daytime['gen_forecast'].shift(1)

    gen_forecast_daytime.sort_values(by=['date', 'block_no'],
                                     ascending=[True, True],
                                     inplace=True)
    gen_forecast_daytime['fcurve'] = \
        (gen_forecast_daytime['gen_forecast'] /
         gen_forecast_daytime['gen_forecast_lag1'])
    gen_forecast_daytime['fcurve'].fillna(0, inplace=True)
    gen_forecast_daytime['fcurve'] = \
        np.where((gen_forecast_daytime['gen_forecast_lag1'] == 0),
                 1, gen_forecast_daytime['fcurve'])

    gen_forecast_daytime['INTRADAY_FORECAST'] = \
        gen_forecast_daytime['fcurve'] * gen_forecast_daytime['quantum_lag1']
    gen_forecast_daytime['INTRADAY_FORECAST'].fillna(0, inplace=True)
    gen_forecast_daytime['INTRADAY_FORECAST_LAG'] = \
        gen_forecast_daytime['INTRADAY_FORECAST'].shift(1)
    gen_forecast_daytime['INTRADAY_FORECAST_LAG'] = \
        gen_forecast_daytime['INTRADAY_FORECAST_LAG']
    gen_forecast_daytime['INTRADAY_FORECAST_LAG'].fillna(0, inplace=True)

    gen_forecast_daytime = \
        gen_forecast_daytime[gen_forecast_daytime['day_time'] == 1]
    min_block = min(gen_forecast_daytime['block_no'])
    max_block = max(gen_forecast_daytime['block_no'])
    quantum = list(gen_forecast_daytime['quantum'])
    quantum_lag = list(gen_forecast_daytime['quantum_lag1'])
    quantum_lag[0] = 1
    forecast_int = list(gen_forecast_daytime['INTRADAY_FORECAST'])
    day_time = list(gen_forecast_daytime['day_time'])
    forecast = list(gen_forecast_daytime['gen_forecast'])
    fcurve = list(gen_forecast_daytime['fcurve'])
    real_time_forecast = []

    for j in range(0, len(quantum)):
        if quantum_lag[j] > 0:
            new = fcurve[j] * quantum_lag[j] + mrr * \
                (forecast[j] - fcurve[j] * quantum_lag[j])
            block = j + 1
            real_time_forecast.append(new)
        else:
            new1 = fcurve[j] * real_time_forecast[j - 1] + mrr * \
                (forecast[j] - fcurve[j] * real_time_forecast[j - 1])
            real_time_forecast.append(new1)
    realtime_forecast = pd.DataFrame(real_time_forecast)
    realtime_forecast.rename(columns={0: 'real_time_forecast'}, inplace=True)
    block_no = []
    for k in range(min_block - 1, max_block):
        n = k + 1
        block_no.append(n)

    block_no = pd.DataFrame(block_no)
    block_no.rename(columns={0: 'block_no'}, inplace=True)
    realtime_forecast = pd.concat([realtime_forecast, block_no], axis=1)
    realtime_forecast['date'] = valid_date
    realtime_forecast_final = pd.merge(gen_forecast, realtime_forecast,
                                       how='left',
                                       on=['date', 'block_no'])
    realtime_forecast_final['real_time_forecast'].fillna(0, inplace=True)
    # Added to check for max genration in a block 
    # across history and limit forecast to that value
    act_blockwise_totmaxgen = pd.read_sql_query("""SELECT 
        block_no, max(tot_gen) max_gen
        from        
        (select date, block_no, sum(generation) * 4 tot_gen
        from power.generation_staging
        where discom = '{}'
        and state = '{}'
        and pool_type = '{}'
        and pool_name = 'RE'
        group by date, block_no) tot_gen
        group by block_no""".format(discom, state, pool_type), 
        engine, index_col=None)    

    realtime_forecast_final = pd.merge(realtime_forecast_final, act_blockwise_totmaxgen, on=['block_no'], how='left')

    realtime_forecast_final['real_time_forecast'] = \
        np.where(realtime_forecast_final['real_time_forecast'] > realtime_forecast_final['max_gen'],
            realtime_forecast_final['max_gen'], 
            realtime_forecast_final['real_time_forecast'])
    # realtime_forecast_final['real_time_forecast'] = \
    #     np.where(
    #         (realtime_forecast_final['real_time_forecast'] >= 1000),
    #         1000,
    #         realtime_forecast_final['real_time_forecast']) 

    realtime_forecast_final['aggregation_point'] = 'UNIT'
    realtime_forecast_final['mrr'] = mrr
    realtime_forecast_final['model_master_fk'] = model_master_fk
    realtime_forecast_final['unit'] = 'MW'
    realtime_forecast_final['organisation_code'] = discom
    realtime_forecast_final['state'] = state
    realtime_forecast_final['pool_name'] = pool_name
    realtime_forecast_final['pool_type'] = pool_type
    tmp_table_nm = 'injection_realtime_solar_tmp_{}'.format(discom)
    realtime_forecast_final.to_sql(name=tmp_table_nm, con=engine,
                                   if_exists='replace')
    cur_date_q = pd.read_sql_query("""select curdate() date""",
                                   engine, index_col=None)
    cur_date_obj = cur_date_q.iloc[0]['date']
    # cur_date = cur_date_obj.strftime('%Y-%m-%d')
    curtime = get_time()
    cur_block = pd.read_sql_query("""select block_no
        from block_master
        where '{}'
        between start_time
        and end_time
        """.format(curtime),
        engine, index_col=None)
    cur_block_no = cur_block.iloc[0]['block_no']
    datedelta, start_block = get_revision(cur_block_no)
    # print 'start_block', start_block
    end_block = 96
    date_to_update_rev_obj = cur_date_obj + timedelta(days=datedelta)
    date_to_update_rev = date_to_update_rev_obj.strftime('%Y-%m-%d')
    udate = list(pd.unique(realtime_forecast['date'].dt.strftime('%Y-%m-%d')))
    hdr_skip_flag = False
    for xdate in udate:
        if xdate == date_to_update_rev:
            realtime_forecast_staging = pd.read_sql_query("""SELECT
                coalesce(max(revision) + 1, 0) max_revision,
                max(realtime_forecast_metadata_pk) old_id
                from `power`.`realtime_forecast_metadata` a,
                     (select b.model_master_fk
                        from
                        power.model_master a,
                        power.model_org_map b,
                        power.organisation_master c
                        where a.id = b.model_master_fk
                        and b.organisation_master_fk = c.organisation_master_pk
                        and c.organisation_code = '{}'
                        and a.model_type = 'INJECTION') b
                where a.date = '{}'
                and a.discom = '{}'
                and a.model_master_fk = b.model_master_fk
                """.format(discom, xdate, discom),
                engine, index_col=None)

            max_revision = realtime_forecast_staging.iloc[0]['max_revision']
            old_id = realtime_forecast_staging.iloc[0]['old_id']
            # print max_revision, old_id
            if max_revision > 0:
                old_id_count = pd.read_sql_query("""SELECT count(1) count
                    from `power`.`realtime_forecast` a
                    where realtime_forecast_metadata_fk = {}
                    and aggregation_sub_type = '{}'
                    """.format(old_id, pool_type),
                    engine, index_col=None)
                old_count = old_id_count.iloc[0]['count']

            if old_count == 0:
                hdr_skip_flag = True
                old_id_block_rng = pd.read_sql_query("""SELECT
                    from_block_no, to_block_no
                    from `power`.`realtime_forecast_metadata` a
                    where realtime_forecast_metadata_pk = {}
                    """.format(old_id),
                    engine, index_col=None)
                start_block = old_id_block_rng.iloc[0]['from_block_no']
                end_block = old_id_block_rng.iloc[0]['to_block_no']
                max_revision = max_revision - 1
                last_ins_pk = old_id
            else:
                old_rev = max_revision - 1 if max_revision > 0 else 0
                sql_str = """update `power`.`realtime_forecast_metadata`
                    set to_block_no = {}
                    where date = '{}'
                    and discom = '{}'
                    and revision = {}
                    and realtime_forecast_metadata_pk = {}
                    """.format(start_block - 1, xdate,
                               discom, old_rev, old_id)
                connection = engine.connect()
                connection.execute(sql_str)
            if not hdr_skip_flag:
                sql_str = """insert into `power`.`realtime_forecast_metadata`
                    (date, revision, mrr, model_master_fk,
                    from_block_no, to_block_no, discom, state)
                    select distinct date, {}, mrr, model_master_fk,
                    {} from_block_no, {} to_block_no, organisation_code, state
                    from {} where date = '{}'
                    on duplicate key
                    update mrr = values(mrr)
                    """.format(max_revision, start_block, end_block,
                               tmp_table_nm, xdate)
                connection = engine.connect()
                connection.execute(sql_str)
                get_last_insert_pk = pd.read_sql_query("""select realtime_forecast_metadata_pk
                    from `power`.`realtime_forecast_metadata`
                    where date = '{}'
                    and mrr = {}
                    and revision = {}
                    and discom ='{}'
                    and model_master_fk = {}
                    """.format(xdate, mrr, max_revision,
                               discom, model_master_fk),
                    engine, index_col=None)
                connection = engine.connect()
                connection.execute(sql_str)
                last_ins_pk = \
                    get_last_insert_pk.iloc[0]['realtime_forecast_metadata_pk']

            sql_str = """insert into `power`.`realtime_forecast`
            (realtime_forecast_metadata_fk, date, block_no,
            forecast_quantum, revision,
            aggregation_point, aggregation_type,
            aggregation_sub_type, entity_name, unit)
            select {}, h.date, h.block_no,
            round(coalesce(a.rated_unit_capacity/tot.total_capacity *
                           real_time_forecast, 0),2) gen_forecast,
            {} revision, h.aggregation_point,
            h.pool_name, h.pool_type, a.unit_name, h.unit
            from
            (select sum(a.rated_unit_capacity) total_capacity
            from power.unit_master a,
            power.unit_type c,
            (select a.contract_trade_master_pk, a.delivery_start_date,
             a.delivery_end_date,
             c.counter_party_name cp1, d.counter_party_name cp2
            from power.contract_trade_master a,
                 power.counter_party_master c,
                 power. counter_party_master d
            where a.counter_party_1_fk = c.counter_party_master_pk
            and   a.counter_party_2_fk = d.counter_party_master_pk
            and c.counter_party_name = '{}'
            and curdate() between a.delivery_start_date and a.delivery_end_date
            and a.delete_ind = 0
            and c.delete_ind = 0
            and d.delete_ind = 0) g
            where c.unit_type_pk = a.unit_type_fk
            and a.unit_name = g.cp2
            and c.unit_type_name = '{}') tot,
            power.unit_master a,
            power.unit_type c,
            (select a.contract_trade_master_pk, a.delivery_start_date,
             a.delivery_end_date,
             c.counter_party_name cp1, d.counter_party_name cp2
            from power.contract_trade_master a,
                 power.counter_party_master c,
                 power. counter_party_master d
            where a.counter_party_1_fk = c.counter_party_master_pk
            and   a.counter_party_2_fk = d.counter_party_master_pk
            and c.counter_party_name = '{}'
            and a.delete_ind = 0
            and c.delete_ind = 0
            and d.delete_ind = 0) g,
            {} h
            where c.unit_type_pk = a.unit_type_fk
            and a.unit_name = g.cp2
            and date(h.date) between
             g.delivery_start_date and g.delivery_end_date
            and c.unit_type_name = h.pool_type
            and h.date = '{}'
            and h.block_no >= {}
            on duplicate key update
            realtime_forecast_metadata_fk = {},
            revision = {},
            forecast_quantum =
                round(coalesce(a.rated_unit_capacity/tot.total_capacity *
                               real_time_forecast, 0),2),
            unit = values(unit)
            """.format(last_ins_pk, max_revision, discom,
                       pool_type, discom, tmp_table_nm, xdate,
                       start_block, last_ins_pk, max_revision)
            connection = engine.connect()
            connection.execute(sql_str)
        elif xdate > date_to_update_rev:
            max_revision = 0
            start_block = 1
            end_block = 96

            sql_str = """insert into `power`.`realtime_forecast_metadata`
                (date, revision, mrr, model_master_fk,
                from_block_no, to_block_no, discom, state)
                select distinct date, {}, mrr, model_master_fk,
                {} from_block_no, {} to_block_no, organisation_code, state
                from {} where date = '{}'
                on duplicate key
                update mrr = values(mrr)
                """.format(max_revision, start_block,
                           end_block, tmp_table_nm, xdate)
            connection = engine.connect()
            connection.execute(sql_str)
            get_last_insert_pk = pd.read_sql_query("""select realtime_forecast_metadata_pk
                from `power`.`realtime_forecast_metadata`
                where date = '{}'
                and mrr = {}
                and revision = {}
                and discom ='{}'
                and model_master_fk = {}
                """.format(xdate, mrr, max_revision, discom, model_master_fk),
                engine, index_col=None)
            connection = engine.connect()
            connection.execute(sql_str)
            last_ins_pk = \
                get_last_insert_pk.iloc[0]['realtime_forecast_metadata_pk']
            sql_str = """insert into `power`.`realtime_forecast`
            (realtime_forecast_metadata_fk, date, block_no,
            forecast_quantum, revision,
            aggregation_point, aggregation_type,
            aggregation_sub_type, entity_name, unit)
            select {}, h.date, h.block_no,
            round(coalesce(a.rated_unit_capacity/tot.total_capacity *
                           real_time_forecast, 0),2) gen_forecast,
            {} revision, h.aggregation_point,
            h.pool_name, h.pool_type, a.unit_name, h.unit
            from
            (select sum(a.rated_unit_capacity) total_capacity
            from power.unit_master a,
            power.unit_type c,
            (select a.contract_trade_master_pk, a.delivery_start_date,
             a.delivery_end_date,
             c.counter_party_name cp1, d.counter_party_name cp2
            from power.contract_trade_master a,
                 power.counter_party_master c,
                 power. counter_party_master d
            where a.counter_party_1_fk = c.counter_party_master_pk
            and   a.counter_party_2_fk = d.counter_party_master_pk
            and c.counter_party_name = '{}'
            and curdate() between a.delivery_start_date and a.delivery_end_date
            and a.delete_ind = 0
            and c.delete_ind = 0
            and d.delete_ind = 0) g
            where c.unit_type_pk = a.unit_type_fk
            and a.unit_name = g.cp2
            and c.unit_type_name = '{}') tot,
            power.unit_master a,
            power.unit_type c,
            (select a.contract_trade_master_pk, a.delivery_start_date,
             a.delivery_end_date,
             c.counter_party_name cp1, d.counter_party_name cp2
            from power.contract_trade_master a,
                 power.counter_party_master c,
                 power. counter_party_master d
            where a.counter_party_1_fk = c.counter_party_master_pk
            and   a.counter_party_2_fk = d.counter_party_master_pk
            and c.counter_party_name = '{}'
            and a.delete_ind = 0
            and c.delete_ind = 0
            and d.delete_ind = 0) g,
            {} h
            where c.unit_type_pk = a.unit_type_fk
            and a.unit_name = g.cp2
            and date(h.date) between
             g.delivery_start_date and g.delivery_end_date
            and c.unit_type_name = h.pool_type
            and h.date = '{}'
            on duplicate key update
            realtime_forecast_metadata_fk = {},
            revision = {},
            forecast_quantum =
                round(coalesce(a.rated_unit_capacity/tot.total_capacity *
                               real_time_forecast, 0),2),
            unit = values(unit)
            """.format(last_ins_pk, max_revision, discom,
                       pool_type, discom, tmp_table_nm, xdate,
                       last_ins_pk, max_revision)
            connection = engine.connect()
            connection.execute(sql_str)
    rpms_sql_str = """insert into realtime_position_map_staging
        (date, block_no, revision, pool_name,
         pool_type, quantum, unit, discom, state)
        select a.date, b.block_no,  b.revision, c.pool_name, c.pool_type,
                sum(b.forecast_quantum) as quantum, b.unit, a.discom, a.state
        from power.realtime_forecast_metadata a,
             power.realtime_forecast b,
             power.model_master g,
            (select * from
            power.pool_master
            where pool_name = 'INT_GENERATION_FOR'
            and pool_type = '{}'
            and delete_ind = 0) c,
            power.organisation_master d,
            power.block_master e,
            power.uom_master f
        where a.date = b.date
        and a.realtime_forecast_metadata_pk =  b.realtime_forecast_metadata_fk
        and g.id =  a.model_master_fk
        and g.model_type = 'INJECTION'
        and a.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        and a.discom = '{}'
        and b.block_no = e.block_no
        and d.organisation_code = a.discom
        and f.uom_code = b.unit
        and c.pool_name = b.aggregation_type
        and c.pool_type = b.aggregation_sub_type
        group by a.date, b.block_no,  b.revision,
        c.pool_name, c.pool_type, b.unit, a.discom, a.state
        on duplicate key update
        revision = values(revision),
        quantum = values(quantum),
        unit = values(unit)""".format(pool_type, discom)
    connection = engine.connect()
    connection.execute(rpms_sql_str)
    pm_sql_str = """insert into power.position_map
        ( date, block_no_fk, revision, organisation_master_fk, pool_master_fk,
          uom_master_fk, model_master_fk, entity_name, quantum)
        select  a.date, e.block_no_pk, a.revision,
        d.organisation_master_pk, c.pool_master_pk,
        f.uom_pk , a.model_master_fk, b.entity_name, b.forecast_quantum
        from power.realtime_forecast_metadata a,
             power.realtime_forecast b,
             power.model_master g,
            (select * from
            power.pool_master
            where pool_name = 'INT_GENERATION_FOR'
            and pool_type = '{}'
            and delete_ind = 0) c,
            power.organisation_master d,
            power.block_master e,
            power.uom_master f
        where a.date = b.date
        and a.realtime_forecast_metadata_pk =  b.realtime_forecast_metadata_fk
        and g.id =  a.model_master_fk
        and g.model_type = 'INJECTION'
        and a.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        and a.discom = '{}'
        and b.block_no = e.block_no
        and d.organisation_code = a.discom
        and f.uom_code = b.unit
        and c.pool_name = b.aggregation_type
        and c.pool_type = b.aggregation_sub_type
        on duplicate key update
        revision = coalesce(a.revision, position_map.revision),
        quantum = coalesce(b.forecast_quantum, position_map.quantum),
        model_master_fk = coalesce(a.model_master_fk,
            position_map.model_master_fk)""".format(pool_type, discom)
    connection = engine.connect()
    connection.execute(pm_sql_str)
    engine.dispose()
    return


def realtime_generation_forecast(config, date, model_name, mrr, discom, state):
    """Run Solar and Wind Forecast."""
    engine = create_engine(config, echo=False)
    curtime = get_time()
    cur_block = pd.read_sql_query("""select block_no
        from block_master
        where '{}' between start_time and end_time
        """.format(curtime),
        engine, index_col=None)
    cur_block_no = cur_block.iloc[0]['block_no']
    engine.dispose()
    try:
        realtime_generation_wind(config, date, model_name, mrr, discom, state)
    except Exception as err:
        print str(err)
    try:
        if cur_block_no >= 32:
            realtime_generation_solar(config, date, model_name,
                                      mrr, discom, state)
    except Exception as err:
        print str(err)


# realtime_demand_forecast('mysql+mysqldb://root:quenext@2016@104.155.225.29/power',
#                          '18-03-2018', 'KNN', 0.02, 'GUVNL', 'GUJARAT')
# realtime_generation_forecast('mysql+mysqldb://root:quenext@2016@104.155.225.29/power',
#                              '06-09-2017', 'HYBRID', 0.0, 'GUVNL', 'GUJARAT')
# realtime_generation_wind('mysql+mysqldb://root:quenext@2016@35.194.240.56/power',
#                          '19-08-2017', 'HYBNN', 0.00, 'GUVNL', 'GUJARAT')
# realtime_generation_solar('mysql+mysqldb://root:quenext@2016@104.155.225.29/power',
#                           '20-08-2017', 'NN', 0.00, 'ADANI', 'TAMIL NADU')
# realtime_generation_solar('mysql+mysqldb://root:quenext@2016@35.194.148.56/power',
#                           '11-07-2017', 'NN', 0.05, 'GUVNL', 'GUJARAT')
# realtime_generation_wind('mysql+mysqldb://root:quenext@2016@104.155.225.29/power',
#                          '12-07-2017', 'NN', 0.00, 'GUVNL', 'GUJARAT')

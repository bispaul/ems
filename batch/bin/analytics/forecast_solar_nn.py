# coding: utf-8

from __future__ import division
from sqlalchemy import create_engine
import pandas as pd
import scipy
from numpy import sign
# from numpy import zeros
# import numpy as np
# from numpy import array
from scipy.interpolate import interp1d
# from scipy import interpolate
# from scipy.interpolate import UnivariateSpline
# from pandas import rolling_median
from datetime import timedelta
from scipy.signal import medfilt
from scipy import *
from scipy.signal import *
from geopy.geocoders import Nominatim
# from time import mktime
# from astral import Astral
from astral import Location
import datetime
import datetime as dt
from datetime import time
from time import sleep
# import pytz
# import time
pd.options.mode.chained_assignment = None


def forecast_solar_nn(config, discom, state):
    # discom = "ADANI"
    # state = "TAMIL NADU"
    engine = create_engine(config, echo=False)

    # if time.tzname[0] == 'IST':
    #     max_hour = dt.datetime.today().hour
    # else:
    #     dest_tz = pytz.timezone('Asia/Kolkata')
    #     ts = time.time()
    #     utc_now = datetime.utcfromtimestamp(ts)
    #     local_now = utc_now.replace(tzinfo=pytz.utc).astimezone(dest_tz)
    #     max_hour = local_now.hour

    # max_hour = max(max_hour, 8)

    re_type = "solar"
    if discom == 'ADANI':
        city_name = "chennai"
    elif discom == 'GUVNL':
        city_name = "ahmedabad"

    today = pd.to_datetime(dt.date.today()).strftime("%Y-%m-%d")

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


    def savitzky_golay(y, window_size, order, deriv=0, rate=1):

        import numpy as np
        from math import factorial

        try:
            window_size = np.abs(np.int(window_size))
            order = np.abs(np.int(order))
        except ValueError, msg:
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
    SOLAR_generation_table = pd.read_sql_query("""select generator_name, date, block_no, generation, solar_radiation  
                                 from generation_staging where pool_type = '{}' and discom = '{}'""".format(re_type,discom), engine, index_col = None)
    SOLAR_generation_table['date'] = pd.to_datetime(SOLAR_generation_table['date'])

    SOLAR_generation_table.sort_values(by=[ 'generator_name', 'date','block_no'], 
                                      ascending=[True, True, True], inplace=True)

    # SOLAR_table_initial.sort_values(by=['date','block_no'], ascending=[True, True], inplace=True)

    solar_unit = pd.read_sql_query("""select a.unit_master_pk, a.unit_name,
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
    power.unit_type c,
    (select a.contract_trade_master_pk, 
     c.counter_party_name cp1, d.counter_party_name cp2
    from power.contract_trade_master a,
         power.counter_party_master c,
         power. counter_party_master d
    where a.counter_party_1_fk = c.counter_party_master_pk
    and   a.counter_party_2_fk = d.counter_party_master_pk
    and c.counter_party_name = '{}'
    and a.delivery_start_date <= '{}'
    and a.delivery_end_date >= '{}'            
    and a.delete_ind = 0
    and c.delete_ind = 0
    and d.delete_ind = 0) g
    where c.unit_type_pk = a.unit_type_fk
    and a.unit_name = g.cp2
    and g.cp1 = '{}'
    and c.unit_type_name = '{}'""".format(discom,today,today,discom,re_type),engine, index_col = None)

    geolocator = Nominatim()
    location = geolocator.geocode(city_name)
    LAT = location.latitude 
    LONG = location.longitude
    sleep(1)

    # city_name = city_name
    # geolocator = Nominatim()
    # location = geolocator.geocode(city_name)
    # sleep(1)
    # LAT = location.latitude 
    # LONG = location.longitude

    name = city_name
    region = "India"
    timezone = "Asia/Kolkata"
    l = Location((name, region,
                  LAT, LONG, timezone, 0))

    dawn_time = l.sun(datetime.datetime(2008, 2, 28)).get('dawn')
    dusk_time = l.sun(datetime.datetime(2008, 2, 28)).get('dusk')
    noon_time = l.sun(datetime.datetime(2008, 2, 28)).get('noon')
    sunrise_time = l.sun(datetime.datetime(2008, 2, 28)).get('sunrise')
    sunset_time = l.sun(datetime.datetime(2008, 2, 28)).get('sunset')

    unique_date = weather_data['date'].unique()
    # date = pd.Timestamp(np.datetime64(unique_date[j])).to_pydatetime()
    sunrise_set_time = pd.DataFrame([])
    sunrise_sunset = []
    for j in xrange(0, len(unique_date)):
        date = pd.Timestamp(np.datetime64(unique_date[j])).to_pydatetime()    
        dawn_time = l.sun(datetime.datetime(date.year,date.month, date.day)).get('dawn')
        dawn_hour = dawn_time.hour
        dawn_minute = dawn_time.minute
        dawn_second = dawn_time.second
        
        dusk_time = l.sun(datetime.datetime(date.year,date.month, date.day)).get('dusk')
        dusk_hour = dusk_time.hour 
        dusk_minute =dusk_time.minute
        dusk_second =dusk_time.second
        
        noon_time = l.sun(datetime.datetime(date.year,date.month, date.day)).get('noon')
        noon_hour =noon_time.hour 
        noon_minute =noon_time.minute
        noon_second =noon_time.second
        
        sunrise_time = l.sun(datetime.datetime(date.year,date.month, date.day)).get('sunrise')
        sunrise_hour =sunrise_time.hour 
        sunrise_minute =sunrise_time.minute
        sunrise_second =sunrise_time.second
        
        sunset_time = l.sun(datetime.datetime(date.year,date.month, date.day)).get('sunset')
        sunset_hour =sunset_time.hour 
        sunset_minute =sunset_time.minute
        sunset_second =sunset_time.second
        sunrise_sunset.append([date, time(dawn_hour,dawn_minute,dawn_second), 
                               time(dusk_hour,dusk_minute,dusk_second),
                               time(noon_hour,noon_minute,noon_second),
                               time(sunrise_hour,sunrise_minute,sunrise_second),
                               time(sunset_hour,sunset_minute,sunset_second)
                              ])
    sunrise_sunset=pd.DataFrame(sunrise_sunset, columns=['date','dawn','dusk','noon','sunrise','sunset'])

    block_master = []
    for i in range(0,96):
        a = datetime.datetime(100,1,1,00,00,00)
        start_time = (a + datetime.timedelta(minutes=15*(i))).time()
        end_time = (a + datetime.timedelta(minutes=15*(i+1))).time()
        block_no = i+1
        block_master.append([block_no, start_time,end_time])
    block_master = pd.DataFrame(block_master, columns=['block_no','start_time','end_time'])

    date_block_no = SOLAR_generation_table[['date','block_no']]
    date_block_no.drop_duplicates()
    last_date = max(SOLAR_generation_table['date'])
    future_period0 = date_block_no[date_block_no['date'] >= last_date]
    future_period0['date'] = future_period0['date']+ pd.DateOffset(1)
    future_period0.reindex(columns=list(date_block_no))

    forecast_period = pd.DataFrame([])
    for j in xrange(0, 8):
        period = future_period0
        period['date'] =max(future_period0['date']) + pd.DateOffset(j)
        forecast_period = forecast_period.append(period)

    future_period = forecast_period[['date','block_no']]
    date_block = pd.concat([date_block_no, forecast_period] , axis =0)


    sunrise_sunset = pd.merge(pd.merge(date_block,
                                      block_master,
                                      how = 'left',
                                      on = 'block_no'),
                                      sunrise_sunset,
                                      how = 'left',
                                     on = ['date'])

    sunrise_sunset['sunrise'] = pd.to_datetime(sunrise_sunset['sunrise'], format='%H:%M:%S').dt.time
    sunrise_sunset['sunset'] = pd.to_datetime(sunrise_sunset['sunset'], format='%H:%M:%S').dt.time
    sunrise_sunset['start_time'] = pd.to_datetime(sunrise_sunset['start_time'], format='%H:%M:%S').dt.time
    sunrise_sunset['end_time'] = pd.to_datetime(sunrise_sunset['end_time'], format='%H:%M:%S').dt.time
    sunrise_sunset['day_time'] = np.where((sunrise_sunset['start_time'] >= sunrise_sunset['sunrise']) & 
                                           (sunrise_sunset['end_time'] <= sunrise_sunset['sunset']),1,0)

    sunrise_sunset['day_time'] = np.where((sunrise_sunset['block_no'] == 96) ,0,sunrise_sunset['day_time'])
    sunrise_sunset = sunrise_sunset[['date','block_no','day_time']]


    # In[157]:

    sunrise_sunset = sunrise_sunset.drop_duplicates()


    # In[158]:

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
    for j in xrange(0,len(unique_foecast_bin)):
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
    WS_WTD_SUM = freq_act_fore_WS.groupby(['ws_forecast_bin'],as_index=False).agg({'freq_wtd_forecast':{'sum':'sum'}}
                                                                                            
                                                                                              )
    WS_WTD_SUM.columns = ['_'.join(col).strip() for col in WS_WTD_SUM.columns.values]
    WS_WTD_SUM.rename(columns={'ws_forecast_bin_': 'ws_forecast_bin'}, inplace=True)

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
    for j in xrange(0,len(unique_foecast_bin)):
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
    RAIN_WTD_SUM = freq_act_fore_rain.groupby(['rain_forecast_bin'],as_index=False).agg({'freq_wtd_forecast':{'sum':'sum'}}
                                                                                                )
    RAIN_WTD_SUM.columns = ['_'.join(col).strip() for col in RAIN_WTD_SUM.columns.values]
    RAIN_WTD_SUM.rename(columns={'rain_forecast_bin_': 'rain_forecast_bin'}, inplace=True)

    # In[159]:

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


    temp_bin_summary = weather_actual_nonmissing.groupby(['temp_bin'],as_index=False).agg({'temp':{'min':'min',
                                                                                                   'max':'max'}}
                                                                                                )

    temp_bin_summary.columns = ['_'.join(col).strip() for col in temp_bin_summary.columns.values]
    temp_bin_summary.rename(columns={'temp_bin_': 'temp_bin'}, inplace=True)

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


    # In[160]:

    solar_gen_capacity = pd.merge(SOLAR_generation_table,
                                 solar_unit,
                                 how = 'left',
                                 left_on = ['generator_name'],
                                 right_on = ['unit_name'])


    # In[161]:

    SOLAR_generation = solar_gen_capacity.groupby(['date', 'block_no'],as_index=False).agg({'generation':{'sum':'sum'
                                                                                                           },
                                                                                           'rated_unit_capacity':{'sum':'sum'
                                                                                                           }})

    SOLAR_generation.columns = ['_'.join(col).strip() for col in SOLAR_generation.columns.values]
    SOLAR_generation.rename(columns={'date_': 'date', 'block_no_' : 'block_no'}, inplace=True)
    SOLAR_generation['gen_MW'] = SOLAR_generation['generation_sum']*4


    # In[162]:

    capacity_summary_initial = SOLAR_generation.copy()

    daily_capacity = capacity_summary_initial.groupby(['date'],as_index=False).agg({
                                                                        'rated_unit_capacity_sum':{'mean':'mean'
                                                                                    }})
    daily_capacity.columns = ['_'.join(col).strip() for col in daily_capacity.columns.values]
    daily_capacity.rename(columns={'date_': 'date'}, inplace=True)

    capacity_summary_final = pd.merge(capacity_summary_initial,
                                      daily_capacity,
                                      how = 'left',
                                      on = ['date'])

    capacity_summary_final['per_gen'] = (capacity_summary_final['gen_MW']/
                                         capacity_summary_final['rated_unit_capacity_sum_mean'])

    capacity_summary_daytime  = pd.merge(capacity_summary_final,
                                         sunrise_sunset,
                                         how = 'left', 
                                         on = ['date','block_no'])
    capacity_summary_daytime['per_gen'] = capacity_summary_daytime['per_gen']*capacity_summary_daytime['day_time']


    # In[163]:

    # len(capacity_summary_final['date'].unique())*96, len(sunrise_sunset['date'].unique())*96

    # In[164]:

    def impute():
        gen_only_table = capacity_summary_final[['date','block_no','gen_MW','per_gen']]
        solar_radiation_summary = SOLAR_generation_table.groupby(['date', 'block_no'],
                                                          as_index=False).agg({'solar_radiation':{'median':'median',
                                                                                                      'mean':'mean'}
                                                                                                      })
        solar_radiation_summary.columns = ['_'.join(col).strip() for col in solar_radiation_summary.columns.values]
        solar_radiation_summary.rename(columns={'date_': 'date', 'block_no_' : 'block_no'}, inplace=True)

        solar_radiation_summary_daytime = pd.merge(solar_radiation_summary,
                                                  sunrise_sunset,
                                                  how = 'left',
                                                  on = ['date','block_no'])
        solar_radiation_summary_daytime = solar_radiation_summary_daytime[solar_radiation_summary_daytime['day_time']==1]

        missing_radiation_summary = solar_radiation_summary_daytime.groupby(['date'],
                                            as_index = False).agg({'solar_radiation_median':{'count':'count'},
                                                                   'day_time':{'sum':'sum'}
                                                                                        })
        missing_radiation_summary.columns = ['_'.join(col).strip() for col in missing_radiation_summary.columns.values]
        missing_radiation_summary.rename(columns={'date_': 'date'}, inplace=True)
        nonmissing_radiation = missing_radiation_summary[missing_radiation_summary['solar_radiation_median_count'] == 
                                                         missing_radiation_summary['day_time_sum'] ]
        non_missing_date = nonmissing_radiation['date'].unique()


        generation = capacity_summary_daytime[['date','block_no','gen_MW','day_time']]

        generation = generation[generation['date'].isin(non_missing_date)]
        generation = generation[generation['day_time']==1]

        missing_generation_summary = generation.groupby(['date'],
                                            as_index = False).agg({'gen_MW':{'count':'count'},
                                                                   'day_time':{'sum':'sum'}  
                                                                    })
        missing_generation_summary.columns = ['_'.join(col).strip() for col in missing_generation_summary.columns.values]
        missing_generation_summary.rename(columns={'date_': 'date'}, inplace=True)
        nonmissing_gen = missing_generation_summary[missing_generation_summary['gen_MW_count'] == 
                                                         missing_generation_summary['day_time_sum'] ]

        nonmissing_date = nonmissing_gen['date'].unique()
        solar_radiation_summary = SOLAR_generation_table.groupby(['date', 'block_no'],
                                                          as_index=False).agg({'solar_radiation':{'median':'median',
                                                                                                      'mean':'mean'}
                                                                                                      })
        solar_radiation_summary.columns = ['_'.join(col).strip() for col in solar_radiation_summary.columns.values]
        solar_radiation_summary.rename(columns={'date_': 'date', 'block_no_' : 'block_no'}, inplace=True)
        solar_radiation_summary['block_rank']=solar_radiation_summary['block_no'].astype(str)+'rank'
        solar_radition_pivot = pd.pivot_table(solar_radiation_summary, values=['solar_radiation_median'], 
                                                      index=['date'], columns=['block_rank'], aggfunc='median').reset_index()
        solar_radition_pivot.columns = ['_'.join(col).strip() for col in solar_radition_pivot.columns.values]
        solar_radition_pivot.rename(columns={'date_':'date'},inplace = True)



        weather_all = solar_radition_pivot
        non_missing_window = non_missing_date
        weather_relative = weather_all[weather_all['date'].isin(nonmissing_date)]
        weather_all.sort_values(by = ['date'], ascending = [True], inplace = True)
        weather_relative.sort_values(by = ['date'], ascending = [True], inplace = True)

        var_temp =[col for col in solar_radition_pivot.columns 
                     if 'solar_radiation_median'in col ]

        a = weather_all[var_temp]
        b = weather_relative[var_temp]
        dist = scipy.spatial.distance.cdist(a,b,) # pick the appropriate distance metric 
        dist_matrix = pd.DataFrame(dist)
        date = weather_all['date']
        date = pd.Series(date)  
        dist_matrix['date'] = date.values
        dist_matrix['date'] = pd.to_datetime(dist_matrix['date'])
        dist_matrix = pd.melt(dist_matrix, id_vars = ['date'], value_name='eucledean_dist')
        dist_matrix.rename(columns={'variable': 'lag_window'}, inplace=True)
        dist_matrix['lag_window'] = dist_matrix['lag_window']+1
        dist_matrix['lag_window'] = pd.to_numeric(dist_matrix['lag_window'])
        unique_lag_window =dist_matrix['lag_window'].unique()
        unique_lag_window = pd.DataFrame(unique_lag_window)
        unique_lag_window.rename(columns={0: 'lag_window'}, inplace=True)
        lag_window = unique_lag_window['lag_window'] 
        ts = pd.Series(unique_lag_window['lag_window'].values)
        lag_date = pd.to_datetime(weather_relative['date'])
        lag_date = pd.DataFrame(lag_date)
        lag_date['lag_window']= ts.values
        lag_date.rename(columns={'date': 'lag_date'}, inplace=True)
        dist_matrix_final = pd.merge(dist_matrix, 
                                     lag_date,
                                     how = 'left',
                                     on = ['lag_window']
                                      )
        dist_matrix_final = dist_matrix_final[dist_matrix_final['date'] > dist_matrix_final['lag_date']]

        dist_matrix_final.sort_values(by = ['date','eucledean_dist'], ascending = [True,True], inplace = True)
        def ranker(dist_matrix_final):
            dist_matrix_final['rank_dist'] = np.arange(len(dist_matrix_final)) + 1
            return dist_matrix_final
        dist_matrix_final = dist_matrix_final.groupby(dist_matrix_final['date']).apply(ranker)
        dist_matrix_WSH = dist_matrix_final[dist_matrix_final['rank_dist']<=12]
        dist_matrix_WSH = dist_matrix_WSH[np.isfinite(dist_matrix_WSH['eucledean_dist'])]

        weather_dist_lag_initial = pd.merge(dist_matrix_WSH,gen_only_table, 
                                           left_on ='lag_date', right_on = 'date' )
        weather_dist_lag_initial.rename(columns={'date_x': 'date'}, inplace=True)
        weather_dist_lag_initial = weather_dist_lag_initial.drop('date_y', axis=1)
        weather_dist_lag_initial['rank_no']=weather_dist_lag_initial['rank_dist'].astype(str)+'lag'
        weather_dist_lag = pd.pivot_table(weather_dist_lag_initial, values=['per_gen'], 
                                                      index=['date','block_no'], columns=['rank_no']).reset_index()
        weather_dist_lag.columns = ['_'.join(col).strip() for col in weather_dist_lag.columns.values]
        weather_dist_lag.rename(columns={'date_':'date','block_no_':'block_no'},inplace = True)


        weather_dist_lag = pd.merge(gen_only_table,weather_dist_lag,
                                    how = 'left',
                                    on = ['date','block_no'])

        weather_dist_lag['per_gen_8lag'].fillna(weather_dist_lag['per_gen_9lag'], inplace = True)
        weather_dist_lag['per_gen_7lag'].fillna(weather_dist_lag['per_gen_8lag'], inplace = True)
        weather_dist_lag['per_gen_6lag'].fillna(weather_dist_lag['per_gen_7lag'], inplace = True)
        weather_dist_lag['per_gen_5lag'].fillna(weather_dist_lag['per_gen_6lag'], inplace = True)
        weather_dist_lag['per_gen_4lag'].fillna(weather_dist_lag['per_gen_3lag'], inplace = True)
        weather_dist_lag['per_gen_3lag'].fillna(weather_dist_lag['per_gen_4lag'], inplace = True)
        weather_dist_lag['per_gen_2lag'].fillna(weather_dist_lag['per_gen_3lag'], inplace = True)
        weather_dist_lag['per_gen_1lag'].fillna(weather_dist_lag['per_gen_2lag'], inplace = True)


        t=.45
        w1=1
        w2=t
        w3=t**2
        w4=t**3
        w5=t**4
        w6=t**5
        w7=t**6
        w8=t**7
        w9=t**8


        weather_dist_lag['endo_pred_sim_day_solar']= (weather_dist_lag['per_gen_1lag']*w1+
                                                     weather_dist_lag['per_gen_2lag']*w2+
                                                     weather_dist_lag['per_gen_3lag']*w3+
                                                     weather_dist_lag['per_gen_4lag']*w4+
                                                     weather_dist_lag['per_gen_5lag']*w5+
                                                     weather_dist_lag['per_gen_6lag']*w6+
                                                     weather_dist_lag['per_gen_7lag']*w7+
                                                     weather_dist_lag['per_gen_8lag']*w8+
                                                     weather_dist_lag['per_gen_9lag']*w9
                                                     )\
                                                     /(w1+w2+w3+w4+w5+w6+w7+w8+w9)

        solar_imp_nn_SR = weather_dist_lag[['date',
                                          'block_no',
                                          'per_gen',
                                          'endo_pred_sim_day_solar']]
        solar_imp = pd.merge(solar_imp_nn_SR,  
                             sunrise_sunset,
                             how = 'left', 
                             on = ['date','block_no']) 

        solar_imp['per_gen'] = solar_imp['per_gen']*solar_imp['day_time']
        solar_imp['endo_pred_sim_day_solar'] = solar_imp['endo_pred_sim_day_solar']*solar_imp['day_time']

        solar_imp['per_gen'] = np.where((solar_imp['day_time']==0),0,
                                       solar_imp['per_gen'])
        solar_imp['endo_pred_sim_day_solar'] = np.where((solar_imp['day_time']==0),0,
                                       solar_imp['endo_pred_sim_day_solar'])


        solar_imp['per_gen'] = np.where((solar_imp['endo_pred_sim_day_solar']>solar_imp['per_gen']),
                                        solar_imp['endo_pred_sim_day_solar'],
                                       solar_imp['per_gen'])
        per_gen = solar_imp[['date','block_no','per_gen','day_time','endo_pred_sim_day_solar']]
        return per_gen


    # In[165]:

    if discom == "ADANI":
        per_gen = impute()
    else:
        per_gen = capacity_summary_daytime[['date','block_no','per_gen','day_time']]
        per_gen['endo_pred_sim_day_solar'] = per_gen ['per_gen']


    # In[166]:

    smooth_SOLAR_curve = per_gen[per_gen['day_time']==1]
    y = list(smooth_SOLAR_curve['per_gen'])
    s_gen = savitzky_golay(y, window_size=21, order=2, deriv=0, rate=1)
    s_gen = list(s_gen)
    s_gen = pd.Series(s_gen)
    smooth_SOLAR_curve['smooth_gen'] =s_gen.values 
    smooth_SOLAR_curve['envelop']  = smooth_SOLAR_curve['smooth_gen']

    smooth_SOLAR_curve = smooth_SOLAR_curve[['date','block_no','per_gen','envelop']]


    # In[167]:

    SOLAR_envelop = pd.merge(sunrise_sunset,smooth_SOLAR_curve,
                             how = 'left',
                             on = ['date','block_no'])


    # In[168]:

    sunrise_sunset_hour = sunrise_sunset.copy()
    sunrise_sunset_hour['hour'] = np.ceil(sunrise_sunset_hour['block_no']/4)

    sunrise_sunset_hour_sum = sunrise_sunset_hour.groupby(['date','hour'],as_index=False).agg({'day_time':{'sum':'sum'}}
                                                                                                
                                                                                                  )

    sunrise_sunset_hour_sum.columns = ['_'.join(col).strip() for col in sunrise_sunset_hour_sum.columns.values]
    sunrise_sunset_hour_sum.rename(columns={'date_': 'date',
                                            'hour_':'hour'}, inplace=True)


    # In[169]:

    sunrise_sunset_hour_sum = sunrise_sunset_hour_sum[sunrise_sunset_hour_sum['day_time_sum']>0]
    surise_sunset_hour = sunrise_sunset_hour_sum.copy() 


    # In[170]:

    solar_hour = surise_sunset_hour['hour'].unique()
    weather_initial = weather_initial[weather_initial['hour'].isin(solar_hour)]
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


    # In[171]:

    # list(SOLAR_envelop)


    # In[172]:

    SOLAR_envelop['envelop'] = SOLAR_envelop['envelop']*SOLAR_envelop['day_time']


    # In[173]:

    SOLAR_table = SOLAR_envelop[['date','block_no','per_gen','envelop']]
    SOLAR_table['hour'] = np.ceil(SOLAR_table['block_no']/4)
    SOLAR_table['year'] = pd.DatetimeIndex(SOLAR_table['date']).year
    SOLAR_table['month'] = pd.DatetimeIndex(SOLAR_table['date']).month   # jan = 1, dec = 12
    SOLAR_table['dayofweek'] = pd.DatetimeIndex(SOLAR_table['date']).dayofweek # Monday=0, Sunday=6
    SOLAR_table.sort_values(by = ['date','block_no'], ascending=[True, True], inplace=True)
    SOLAR_table['endo_SOLAR']=SOLAR_table['envelop']


    # In[174]:


    SOLAR_only_table = SOLAR_table[['date','block_no','endo_SOLAR','per_gen']]

    last_date_block = SOLAR_only_table[SOLAR_only_table['date']== max(SOLAR_only_table['date'])]
    max_block = max(last_date_block['block_no'])
    columns = [['block_no','endo_SOLAR']]

    if max_block < 96:
        forecast_period0 = pd.DataFrame(columns=columns)
        forecast_period0['block_no']=range(max_block+1, 97)
        forecast_period0['date'] =max(SOLAR_only_table['date']) 
        forecast_period0 = forecast_period0[['date','block_no','endo_SOLAR']]    
    else:
        forecast_period0 = pd.DataFrame(columns=columns)
        forecast_period0['block_no']=range(1, 97)
        forecast_period0['date'] =max(SOLAR_only_table['date']) + pd.DateOffset(1)
        forecast_period0 = forecast_period0[['date','block_no','endo_SOLAR']]

        
    forecast_period = pd.DataFrame([])
    for j in xrange(1, 8):
        period = pd.DataFrame(columns=columns)
        period['block_no']=range(1, 97)
        period['date'] =max(forecast_period0['date']) + pd.DateOffset(j)
        period = period[['date','block_no','endo_SOLAR']]
        forecast_period = forecast_period.append(period)
        
    forecast_period_date = pd.concat([forecast_period0, forecast_period] , axis =0)

    SOLAR_only_table = pd.concat([SOLAR_only_table, forecast_period_date] , axis =0)


    # In[175]:

    solar_only_table_final = pd.merge(SOLAR_only_table,
                                      sunrise_sunset,
                                      how = 'left',
                                      on = ['date','block_no'])

    solar_only_table_final['per_gen']=solar_only_table_final['per_gen']*solar_only_table_final['day_time']
    solar_only_table_final['endo_SOLAR']=solar_only_table_final['endo_SOLAR']*solar_only_table_final['day_time']


    # In[176]:

    solar_day_Time = solar_only_table_final[solar_only_table_final['day_time']==1]

    nonmissing_day_summary = solar_day_Time.groupby(['date'],
                                        as_index = False).agg({'per_gen':{'count':'count'},
                                                               'day_time':{'sum':'sum'}  
                                                                })
    nonmissing_day_summary.columns = ['_'.join(col).strip() for col in nonmissing_day_summary.columns.values]
    nonmissing_day_summary.rename(columns={'date_': 'date'}, inplace=True)
    nonmissing_gen = nonmissing_day_summary[nonmissing_day_summary['per_gen_count'] == 
                                                     nonmissing_day_summary['day_time_sum'] ]

    non_missing_date = nonmissing_gen['date'].unique()


    # In[177]:

    nn_days = 60
    weather_all = weather_hourly_pivot
    non_missing_window = non_missing_date
    weather_relative = weather_all[weather_all['date'].isin (non_missing_date)]
    weather_all.sort_values(by = ['date'], ascending = [True], inplace = True)
    weather_relative.sort_values(by = ['date'], ascending = [True], inplace = True)

    var_temp =[col for col in weather_hourly_pivot.columns 
                 if 'temp_bin'in col ]

    a = weather_all[var_temp]
    b = weather_relative[var_temp]
    dist = scipy.spatial.distance.cdist(a,b,) # pick the appropriate distance metric 
    dist_matrix = pd.DataFrame(dist)
    date = weather_hourly_pivot['date']
    date = pd.Series(date)  
    dist_matrix['date'] = date.values
    dist_matrix['date'] = pd.to_datetime(dist_matrix['date'])
    dist_matrix = pd.melt(dist_matrix, id_vars = ['date'], value_name='eucledean_dist')
    dist_matrix.rename(columns={'variable': 'lag_window'}, inplace=True)
    dist_matrix['lag_window'] = dist_matrix['lag_window']+1
    dist_matrix['lag_window'] = pd.to_numeric(dist_matrix['lag_window'])
    unique_lag_window =dist_matrix['lag_window'].unique()
    unique_lag_window = pd.DataFrame(unique_lag_window)
    unique_lag_window.rename(columns={0: 'lag_window'}, inplace=True)
    lag_window = unique_lag_window['lag_window'] 
    ts = pd.Series(unique_lag_window['lag_window'].values)
    lag_date = pd.to_datetime(weather_relative['date'])
    lag_date = pd.DataFrame(lag_date)
    lag_date['lag_window']= ts.values
    lag_date.rename(columns={'date': 'lag_date'}, inplace=True)
    dist_matrix_final = pd.merge(dist_matrix, 
                                 lag_date,
                                 how = 'left',
                                 on = ['lag_window']
                                  )
    dist_matrix_final = dist_matrix_final[dist_matrix_final['date'] > dist_matrix_final['lag_date']]

    dist_matrix_final = dist_matrix_final[dist_matrix_final['lag_date'] >= dist_matrix_final['date'] 
                                                - timedelta(days=nn_days)]
    dist_matrix_final.sort_values(by = ['date','eucledean_dist'], ascending = [True,True], inplace = True)
    def ranker(dist_matrix_final):
        dist_matrix_final['rank_dist'] = np.arange(len(dist_matrix_final)) + 1
        return dist_matrix_final
    dist_matrix_final = dist_matrix_final.groupby(dist_matrix_final['date']).apply(ranker)
    dist_matrix_WSH = dist_matrix_final[dist_matrix_final['rank_dist']<=12]
    dist_matrix_WSH = dist_matrix_WSH[np.isfinite(dist_matrix_WSH['eucledean_dist'])]

    weather_dist_lag_initial = pd.merge(dist_matrix_WSH,solar_only_table_final, 
                                       left_on ='lag_date', right_on = 'date' )

    weather_dist_lag_initial.rename(columns={'date_x': 'date'}, inplace=True)
    weather_dist_lag_initial = weather_dist_lag_initial.drop('date_y', axis=1)
    weather_dist_lag_initial['rank_no']=weather_dist_lag_initial['rank_dist'].astype(str)+'lag'
    weather_dist_lag = pd.pivot_table(weather_dist_lag_initial, values=['endo_SOLAR'], 
                                                  index=['date','block_no'], columns=['rank_no']).reset_index()
    weather_dist_lag.columns = ['_'.join(col).strip() for col in weather_dist_lag.columns.values]
    weather_dist_lag.rename(columns={'date_':'date','block_no_':'block_no'},inplace = True)


    weather_dist_lag = pd.merge(solar_only_table_final,weather_dist_lag,
                                how = 'left',
                                on = ['date','block_no'])

    weather_dist_lag['endo_SOLAR_8lag'].fillna( weather_dist_lag['endo_SOLAR_9lag'],inplace = True)
    weather_dist_lag['endo_SOLAR_7lag'].fillna( weather_dist_lag['endo_SOLAR_8lag'],inplace = True)
    weather_dist_lag['endo_SOLAR_6lag'].fillna( weather_dist_lag['endo_SOLAR_7lag'],inplace = True)
    weather_dist_lag['endo_SOLAR_5lag'].fillna( weather_dist_lag['endo_SOLAR_4lag'],inplace = True)
    weather_dist_lag['endo_SOLAR_4lag'].fillna( weather_dist_lag['endo_SOLAR_3lag'],inplace = True)
    weather_dist_lag['endo_SOLAR_3lag'].fillna( weather_dist_lag['endo_SOLAR_2lag'],inplace = True)
    weather_dist_lag['endo_SOLAR_2lag'].fillna( weather_dist_lag['endo_SOLAR_1lag'],inplace = True)

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

    weather_dist_lag['sim_day_solar']= (weather_dist_lag['endo_SOLAR_1lag']*w1+
                                                 weather_dist_lag['endo_SOLAR_2lag']*w2+
                                                 weather_dist_lag['endo_SOLAR_3lag']*w3+
                                                 weather_dist_lag['endo_SOLAR_4lag']*w4+
                                                 weather_dist_lag['endo_SOLAR_5lag']*w5+
                                                 weather_dist_lag['endo_SOLAR_6lag']*w6+
                                                 weather_dist_lag['endo_SOLAR_7lag']*w7+
                                                 weather_dist_lag['endo_SOLAR_8lag']*w8+
                                                 weather_dist_lag['endo_SOLAR_9lag']*w9
                                                 )\
                                                 /(w1+w2+w3+w4+w5+w6+w7+w8+w9)

    SOLAR_forecast_nn_temp = weather_dist_lag[['date',
                                      'block_no',
                                      'per_gen',
                                      'sim_day_solar']]
    previsous_day_capacity = capacity_summary_final[['date','block_no','gen_MW','rated_unit_capacity_sum_mean']]


    previsous_day_capacity['date'] = pd.to_datetime(previsous_day_capacity['date'])
    SOLAR_forecast_nn_temp['date'] = pd.to_datetime(SOLAR_forecast_nn_temp['date'])

    SOLAR_forecast_nn_temp_final = pd.merge(SOLAR_forecast_nn_temp,
                                        previsous_day_capacity,
                                        how = 'left',
                                        on = ['date','block_no'])

    SOLAR_forecast_nn_temp_final['rated_unit_capacity_sum_mean'].fillna(method='ffill',inplace=True)

    smooth_SOLAR_forecast_NN_TEMP = SOLAR_forecast_nn_temp_final.copy()
    med_filt = medfilt(smooth_SOLAR_forecast_NN_TEMP['sim_day_solar'], 5)
    med_filt = pd.Series(med_filt)
    smooth_SOLAR_forecast_NN_TEMP['med_filt'] = med_filt.values
    smooth_SOLAR_forecast_NN_TEMP.rename(columns={'med_filt': 'SOLAR_forecast_NN_TEMP'}, inplace=True)
    smooth_SOLAR_forecast_NN_TEMP.sort_values(by = ['date','block_no'], ascending=[True, True], inplace=True)
    smooth_SOLAR_forecast_NN_TEMP = smooth_SOLAR_forecast_NN_TEMP[['date','block_no','gen_MW','per_gen','SOLAR_forecast_NN_TEMP']]


    # In[180]:

    nn_days = 30
    weather_all = weather_hourly_pivot
    non_missing_window = non_missing_date
    weather_relative = weather_all[weather_all['date'].isin (non_missing_date)]
    weather_all.sort_values(by = ['date'], ascending = [True], inplace = True)
    weather_relative.sort_values(by = ['date'], ascending = [True], inplace = True)

    var_temp =[col for col in weather_hourly_pivot.columns 
                 if 'temp_bin'in col or 'windspeed_bin' in col ]

    a = weather_all[var_temp]
    b = weather_relative[var_temp]
    dist = scipy.spatial.distance.cdist(a,b,) # pick the appropriate distance metric 
    dist_matrix = pd.DataFrame(dist)
    date = weather_hourly_pivot['date']
    date = pd.Series(date)  
    dist_matrix['date'] = date.values
    dist_matrix['date'] = pd.to_datetime(dist_matrix['date'])
    dist_matrix = pd.melt(dist_matrix, id_vars = ['date'], value_name='eucledean_dist')
    dist_matrix.rename(columns={'variable': 'lag_window'}, inplace=True)
    dist_matrix['lag_window'] = dist_matrix['lag_window']+1
    dist_matrix['lag_window'] = pd.to_numeric(dist_matrix['lag_window'])
    unique_lag_window =dist_matrix['lag_window'].unique()
    unique_lag_window = pd.DataFrame(unique_lag_window)
    unique_lag_window.rename(columns={0: 'lag_window'}, inplace=True)
    lag_window = unique_lag_window['lag_window'] 
    ts = pd.Series(unique_lag_window['lag_window'].values)
    lag_date = pd.to_datetime(weather_relative['date'])
    lag_date = pd.DataFrame(lag_date)
    lag_date['lag_window']= ts.values
    lag_date.rename(columns={'date': 'lag_date'}, inplace=True)
    dist_matrix_final = pd.merge(dist_matrix, 
                                 lag_date,
                                 how = 'left',
                                 on = ['lag_window']
                                  )
    dist_matrix_final = dist_matrix_final[dist_matrix_final['date'] > dist_matrix_final['lag_date']]
    dist_matrix_final = dist_matrix_final[dist_matrix_final['lag_date'] >= dist_matrix_final['date'] 
                                                - timedelta(days=nn_days)]

    dist_matrix_final.sort_values(by = ['date','eucledean_dist'], ascending = [True,True], inplace = True)
    def ranker(dist_matrix_final):
        dist_matrix_final['rank_dist'] = np.arange(len(dist_matrix_final)) + 1
        return dist_matrix_final
    dist_matrix_final = dist_matrix_final.groupby(dist_matrix_final['date']).apply(ranker)
    dist_matrix_WSH = dist_matrix_final[dist_matrix_final['rank_dist']<=12]
    dist_matrix_WSH = dist_matrix_WSH[np.isfinite(dist_matrix_WSH['eucledean_dist'])]

    weather_dist_lag_initial = pd.merge(dist_matrix_WSH,solar_only_table_final, 
                                       left_on ='lag_date', right_on = 'date' )
    weather_dist_lag_initial.rename(columns={'date_x': 'date'}, inplace=True)
    weather_dist_lag_initial = weather_dist_lag_initial.drop('date_y', axis=1)
    weather_dist_lag_initial['rank_no']=weather_dist_lag_initial['rank_dist'].astype(str)+'lag'
    weather_dist_lag = pd.pivot_table(weather_dist_lag_initial, values=['endo_SOLAR'], 
                                                  index=['date','block_no'], columns=['rank_no']).reset_index()
    weather_dist_lag.columns = ['_'.join(col).strip() for col in weather_dist_lag.columns.values]
    weather_dist_lag.rename(columns={'date_':'date','block_no_':'block_no'},inplace = True)


    weather_dist_lag = pd.merge(solar_only_table_final,weather_dist_lag,
                                how = 'left',
                                on = ['date','block_no'])
    weather_dist_lag['endo_SOLAR_8lag'].fillna( weather_dist_lag['endo_SOLAR_9lag'],inplace = True)
    weather_dist_lag['endo_SOLAR_7lag'].fillna( weather_dist_lag['endo_SOLAR_8lag'],inplace = True)
    weather_dist_lag['endo_SOLAR_6lag'].fillna( weather_dist_lag['endo_SOLAR_7lag'],inplace = True)
    weather_dist_lag['endo_SOLAR_5lag'].fillna( weather_dist_lag['endo_SOLAR_4lag'],inplace = True)
    weather_dist_lag['endo_SOLAR_4lag'].fillna( weather_dist_lag['endo_SOLAR_3lag'],inplace = True)
    weather_dist_lag['endo_SOLAR_3lag'].fillna( weather_dist_lag['endo_SOLAR_2lag'],inplace = True)
    weather_dist_lag['endo_SOLAR_2lag'].fillna( weather_dist_lag['endo_SOLAR_1lag'],inplace = True)

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

    weather_dist_lag['sim_day_solar']= (weather_dist_lag['endo_SOLAR_1lag']*w1+
                                                 weather_dist_lag['endo_SOLAR_2lag']*w2+
                                                 weather_dist_lag['endo_SOLAR_3lag']*w3+
                                                 weather_dist_lag['endo_SOLAR_4lag']*w4+
                                                 weather_dist_lag['endo_SOLAR_5lag']*w5+
                                                 weather_dist_lag['endo_SOLAR_6lag']*w6+
                                                 weather_dist_lag['endo_SOLAR_7lag']*w7+
                                                 weather_dist_lag['endo_SOLAR_8lag']*w8+
                                                 weather_dist_lag['endo_SOLAR_9lag']*w9
                                                 )\
                                                 /(w1+w2+w3+w4+w5+w6+w7+w8+w9)

    SOLAR_forecast_nn_temp = weather_dist_lag[['date',
                                      'block_no',
                                      'per_gen',
                                      'sim_day_solar']]
    previsous_day_capacity = capacity_summary_final[['date','block_no','gen_MW','rated_unit_capacity_sum_mean']]


    previsous_day_capacity['date'] = pd.to_datetime(previsous_day_capacity['date'])
    SOLAR_forecast_nn_temp['date'] = pd.to_datetime(SOLAR_forecast_nn_temp['date'])

    SOLAR_forecast_nn_temp_final = pd.merge(SOLAR_forecast_nn_temp,
                                        previsous_day_capacity,
                                        how = 'left',
                                        on = ['date','block_no'])

    SOLAR_forecast_nn_temp_final['rated_unit_capacity_sum_mean'].fillna(method='ffill',inplace=True)

    smooth_SOLAR_forecast_NN_TEMPWIND = SOLAR_forecast_nn_temp_final.copy()
    med_filt = medfilt(smooth_SOLAR_forecast_NN_TEMPWIND['sim_day_solar'], 5)
    med_filt = pd.Series(med_filt)
    smooth_SOLAR_forecast_NN_TEMPWIND['med_filt'] = med_filt.values
    smooth_SOLAR_forecast_NN_TEMPWIND.rename(columns={'med_filt': 'SOLAR_forecast_NN_TEMPWIND'}, inplace=True)
    smooth_SOLAR_forecast_NN_TEMPWIND.sort_values(by = ['date','block_no'], ascending=[True, True], inplace=True)
    smooth_SOLAR_forecast_NN_TEMPWIND = smooth_SOLAR_forecast_NN_TEMPWIND[['date','block_no','SOLAR_forecast_NN_TEMPWIND']]


    nn_days = 30
    weather_all = weather_summary_pivot
    non_missing_window = non_missing_date
    weather_relative = weather_all[weather_all['date'].isin (non_missing_date)]
    weather_all.sort_values(by = ['date'], ascending = [True], inplace = True)
    weather_relative.sort_values(by = ['date'], ascending = [True], inplace = True)

    var_temp =[col for col in weather_summary_pivot.columns 
                 if 'temp_bin'in col ]

    a = weather_all[var_temp]
    b = weather_relative[var_temp]
    dist = scipy.spatial.distance.cdist(a,b,) # pick the appropriate distance metric 
    dist_matrix = pd.DataFrame(dist)
    date = weather_hourly_pivot['date']
    date = pd.Series(date)  
    dist_matrix['date'] = date.values
    dist_matrix['date'] = pd.to_datetime(dist_matrix['date'])
    dist_matrix = pd.melt(dist_matrix, id_vars = ['date'], value_name='eucledean_dist')
    dist_matrix.rename(columns={'variable': 'lag_window'}, inplace=True)
    dist_matrix['lag_window'] = dist_matrix['lag_window']+1
    dist_matrix['lag_window'] = pd.to_numeric(dist_matrix['lag_window'])
    unique_lag_window =dist_matrix['lag_window'].unique()
    unique_lag_window = pd.DataFrame(unique_lag_window)
    unique_lag_window.rename(columns={0: 'lag_window'}, inplace=True)
    lag_window = unique_lag_window['lag_window'] 
    ts = pd.Series(unique_lag_window['lag_window'].values)
    lag_date = pd.to_datetime(weather_relative['date'])
    lag_date = pd.DataFrame(lag_date)
    lag_date['lag_window']= ts.values
    lag_date.rename(columns={'date': 'lag_date'}, inplace=True)
    dist_matrix_final = pd.merge(dist_matrix, 
                                 lag_date,
                                 how = 'left',
                                 on = ['lag_window']
                                  )
    dist_matrix_final = dist_matrix_final[dist_matrix_final['date'] > dist_matrix_final['lag_date']]
    dist_matrix_final = dist_matrix_final[dist_matrix_final['lag_date'] >= dist_matrix_final['date'] 
                                                - timedelta(days=nn_days)]

    dist_matrix_final.sort_values(by = ['date','eucledean_dist'], ascending = [True,True], inplace = True)
    def ranker(dist_matrix_final):
        dist_matrix_final['rank_dist'] = np.arange(len(dist_matrix_final)) + 1
        return dist_matrix_final
    dist_matrix_final = dist_matrix_final.groupby(dist_matrix_final['date']).apply(ranker)
    dist_matrix_WSH = dist_matrix_final[dist_matrix_final['rank_dist']<=12]
    dist_matrix_WSH = dist_matrix_WSH[np.isfinite(dist_matrix_WSH['eucledean_dist'])]

    weather_dist_lag_initial = pd.merge(dist_matrix_WSH,solar_only_table_final, 
                                       left_on ='lag_date', right_on = 'date' )
    weather_dist_lag_initial.rename(columns={'date_x': 'date'}, inplace=True)
    weather_dist_lag_initial = weather_dist_lag_initial.drop('date_y', axis=1)
    weather_dist_lag_initial['rank_no']=weather_dist_lag_initial['rank_dist'].astype(str)+'lag'
    weather_dist_lag = pd.pivot_table(weather_dist_lag_initial, values=['endo_SOLAR'], 
                                                  index=['date','block_no'], columns=['rank_no']).reset_index()
    weather_dist_lag.columns = ['_'.join(col).strip() for col in weather_dist_lag.columns.values]
    weather_dist_lag.rename(columns={'date_':'date','block_no_':'block_no'},inplace = True)


    weather_dist_lag = pd.merge(solar_only_table_final,weather_dist_lag,
                                how = 'left',
                                on = ['date','block_no'])
    weather_dist_lag['endo_SOLAR_8lag'].fillna( weather_dist_lag['endo_SOLAR_9lag'],inplace = True)
    weather_dist_lag['endo_SOLAR_7lag'].fillna( weather_dist_lag['endo_SOLAR_8lag'],inplace = True)
    weather_dist_lag['endo_SOLAR_6lag'].fillna( weather_dist_lag['endo_SOLAR_7lag'],inplace = True)
    weather_dist_lag['endo_SOLAR_5lag'].fillna( weather_dist_lag['endo_SOLAR_4lag'],inplace = True)
    weather_dist_lag['endo_SOLAR_4lag'].fillna( weather_dist_lag['endo_SOLAR_3lag'],inplace = True)
    weather_dist_lag['endo_SOLAR_3lag'].fillna( weather_dist_lag['endo_SOLAR_2lag'],inplace = True)
    weather_dist_lag['endo_SOLAR_2lag'].fillna( weather_dist_lag['endo_SOLAR_1lag'],inplace = True)

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

    weather_dist_lag['sim_day_solar']= (weather_dist_lag['endo_SOLAR_1lag']*w1+
                                                 weather_dist_lag['endo_SOLAR_2lag']*w2+
                                                 weather_dist_lag['endo_SOLAR_3lag']*w3+
                                                 weather_dist_lag['endo_SOLAR_4lag']*w4+
                                                 weather_dist_lag['endo_SOLAR_5lag']*w5+
                                                 weather_dist_lag['endo_SOLAR_6lag']*w6+
                                                 weather_dist_lag['endo_SOLAR_7lag']*w7+
                                                 weather_dist_lag['endo_SOLAR_8lag']*w8+
                                                 weather_dist_lag['endo_SOLAR_9lag']*w9
                                                 )\
                                                 /(w1+w2+w3+w4+w5+w6+w7+w8+w9)

    SOLAR_forecast_nn_temp = weather_dist_lag[['date',
                                      'block_no',
                                      'per_gen',
                                      'sim_day_solar']]
    previsous_day_capacity = capacity_summary_final[['date','block_no','gen_MW','rated_unit_capacity_sum_mean']]


    previsous_day_capacity['date'] = pd.to_datetime(previsous_day_capacity['date'])
    SOLAR_forecast_nn_temp['date'] = pd.to_datetime(SOLAR_forecast_nn_temp['date'])

    SOLAR_forecast_nn_temp_final = pd.merge(SOLAR_forecast_nn_temp,
                                        previsous_day_capacity,
                                        how = 'left',
                                        on = ['date','block_no'])

    SOLAR_forecast_nn_temp_final['rated_unit_capacity_sum_mean'].fillna(method='ffill',inplace=True)

    smooth_SOLAR_forecast_NN_TEMPSUM = SOLAR_forecast_nn_temp_final.copy()
    med_filt = medfilt(smooth_SOLAR_forecast_NN_TEMPSUM['sim_day_solar'], 5)
    med_filt = pd.Series(med_filt)
    smooth_SOLAR_forecast_NN_TEMPSUM['med_filt'] = med_filt.values
    smooth_SOLAR_forecast_NN_TEMPSUM.rename(columns={'med_filt': 'SOLAR_forecast_NN_TEMPSUM'}, inplace=True)
    smooth_SOLAR_forecast_NN_TEMPSUM.sort_values(by = ['date','block_no'], ascending=[True, True], inplace=True)
    smooth_SOLAR_forecast_NN_TEMPSUM = smooth_SOLAR_forecast_NN_TEMPSUM[['date','block_no','SOLAR_forecast_NN_TEMPSUM','rated_unit_capacity_sum_mean']]


    # In[182]:

    nn_days = 60
    weather_all = weather_hourly_pivot
    non_missing_window = non_missing_date
    weather_relative = weather_all[weather_all['date'].isin (non_missing_date)]
    weather_all.sort_values(by = ['date'], ascending = [True], inplace = True)
    weather_relative.sort_values(by = ['date'], ascending = [True], inplace = True)

    var_temp =[col for col in weather_hourly_pivot.columns 
                 if 'temp_bin'in col or 'rain_bin' in col ]

    a = weather_all[var_temp]
    b = weather_relative[var_temp]
    dist = scipy.spatial.distance.cdist(a,b,) # pick the appropriate distance metric 
    dist_matrix = pd.DataFrame(dist)
    date = weather_hourly_pivot['date']
    date = pd.Series(date)  
    dist_matrix['date'] = date.values
    dist_matrix['date'] = pd.to_datetime(dist_matrix['date'])
    dist_matrix = pd.melt(dist_matrix, id_vars = ['date'], value_name='eucledean_dist')
    dist_matrix.rename(columns={'variable': 'lag_window'}, inplace=True)
    dist_matrix['lag_window'] = dist_matrix['lag_window']+1
    dist_matrix['lag_window'] = pd.to_numeric(dist_matrix['lag_window'])
    unique_lag_window =dist_matrix['lag_window'].unique()
    unique_lag_window = pd.DataFrame(unique_lag_window)
    unique_lag_window.rename(columns={0: 'lag_window'}, inplace=True)
    lag_window = unique_lag_window['lag_window'] 
    ts = pd.Series(unique_lag_window['lag_window'].values)
    lag_date = pd.to_datetime(weather_relative['date'])
    lag_date = pd.DataFrame(lag_date)
    lag_date['lag_window']= ts.values
    lag_date.rename(columns={'date': 'lag_date'}, inplace=True)
    dist_matrix_final = pd.merge(dist_matrix, 
                                 lag_date,
                                 how = 'left',
                                 on = ['lag_window']
                                  )
    dist_matrix_final = dist_matrix_final[dist_matrix_final['date'] > dist_matrix_final['lag_date']]
    dist_matrix_final = dist_matrix_final[dist_matrix_final['lag_date'] >= dist_matrix_final['date'] 
                                                - timedelta(days=nn_days)]

    dist_matrix_final.sort_values(by = ['date','eucledean_dist'], ascending = [True,True], inplace = True)
    def ranker(dist_matrix_final):
        dist_matrix_final['rank_dist'] = np.arange(len(dist_matrix_final)) + 1
        return dist_matrix_final
    dist_matrix_final = dist_matrix_final.groupby(dist_matrix_final['date']).apply(ranker)
    dist_matrix_WSH = dist_matrix_final[dist_matrix_final['rank_dist']<=12]
    dist_matrix_WSH = dist_matrix_WSH[np.isfinite(dist_matrix_WSH['eucledean_dist'])]

    weather_dist_lag_initial = pd.merge(dist_matrix_WSH,solar_only_table_final, 
                                       left_on ='lag_date', right_on = 'date' )
    weather_dist_lag_initial.rename(columns={'date_x': 'date'}, inplace=True)
    weather_dist_lag_initial = weather_dist_lag_initial.drop('date_y', axis=1)
    weather_dist_lag_initial['rank_no']=weather_dist_lag_initial['rank_dist'].astype(str)+'lag'
    weather_dist_lag = pd.pivot_table(weather_dist_lag_initial, values=['endo_SOLAR'], 
                                                  index=['date','block_no'], columns=['rank_no']).reset_index()
    weather_dist_lag.columns = ['_'.join(col).strip() for col in weather_dist_lag.columns.values]
    weather_dist_lag.rename(columns={'date_':'date','block_no_':'block_no'},inplace = True)


    weather_dist_lag = pd.merge(solar_only_table_final,weather_dist_lag,
                                how = 'left',
                                on = ['date','block_no'])
    weather_dist_lag['endo_SOLAR_8lag'].fillna( weather_dist_lag['endo_SOLAR_9lag'],inplace = True)
    weather_dist_lag['endo_SOLAR_7lag'].fillna( weather_dist_lag['endo_SOLAR_8lag'],inplace = True)
    weather_dist_lag['endo_SOLAR_6lag'].fillna( weather_dist_lag['endo_SOLAR_7lag'],inplace = True)
    weather_dist_lag['endo_SOLAR_5lag'].fillna( weather_dist_lag['endo_SOLAR_4lag'],inplace = True)
    weather_dist_lag['endo_SOLAR_4lag'].fillna( weather_dist_lag['endo_SOLAR_3lag'],inplace = True)
    weather_dist_lag['endo_SOLAR_3lag'].fillna( weather_dist_lag['endo_SOLAR_2lag'],inplace = True)
    weather_dist_lag['endo_SOLAR_2lag'].fillna( weather_dist_lag['endo_SOLAR_1lag'],inplace = True)

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

    weather_dist_lag['sim_day_solar']= (weather_dist_lag['endo_SOLAR_1lag']*w1+
                                                 weather_dist_lag['endo_SOLAR_2lag']*w2+
                                                 weather_dist_lag['endo_SOLAR_3lag']*w3+
                                                 weather_dist_lag['endo_SOLAR_4lag']*w4+
                                                 weather_dist_lag['endo_SOLAR_5lag']*w5+
                                                 weather_dist_lag['endo_SOLAR_6lag']*w6+
                                                 weather_dist_lag['endo_SOLAR_7lag']*w7+
                                                 weather_dist_lag['endo_SOLAR_8lag']*w8+
                                                 weather_dist_lag['endo_SOLAR_9lag']*w9
                                                 )\
                                                 /(w1+w2+w3+w4+w5+w6+w7+w8+w9)

    SOLAR_forecast_nn_temp = weather_dist_lag[['date',
                                      'block_no',
                                      'per_gen',
                                      'sim_day_solar']]
    previsous_day_capacity = capacity_summary_final[['date','block_no','gen_MW','rated_unit_capacity_sum_mean']]


    previsous_day_capacity['date'] = pd.to_datetime(previsous_day_capacity['date'])
    SOLAR_forecast_nn_temp['date'] = pd.to_datetime(SOLAR_forecast_nn_temp['date'])

    SOLAR_forecast_nn_temp_final = pd.merge(SOLAR_forecast_nn_temp,
                                        previsous_day_capacity,
                                        how = 'left',
                                        on = ['date','block_no'])

    SOLAR_forecast_nn_temp_final['rated_unit_capacity_sum_mean'].fillna(method='ffill',inplace=True)

    smooth_SOLAR_forecast_NN_TEMPRAIN = SOLAR_forecast_nn_temp_final.copy()
    med_filt = medfilt(smooth_SOLAR_forecast_NN_TEMPRAIN['sim_day_solar'], 5)
    med_filt = pd.Series(med_filt)
    smooth_SOLAR_forecast_NN_TEMPRAIN['med_filt'] = med_filt.values
    smooth_SOLAR_forecast_NN_TEMPRAIN.rename(columns={'med_filt': 'SOLAR_forecast_NN_TEMPRAIN'}, inplace=True)
    smooth_SOLAR_forecast_NN_TEMPRAIN.sort_values(by = ['date','block_no'], ascending=[True, True], inplace=True)
    smooth_SOLAR_forecast_NN_TEMPRAIN = smooth_SOLAR_forecast_NN_TEMPRAIN[['date','block_no','SOLAR_forecast_NN_TEMPRAIN']]


    # In[183]:

    nn_days = 30
    weather_all = weather_summary_pivot
    non_missing_window = non_missing_date
    weather_relative = weather_all[weather_all['date'].isin (non_missing_date)]
    weather_all.sort_values(by = ['date'], ascending = [True], inplace = True)
    weather_relative.sort_values(by = ['date'], ascending = [True], inplace = True)

    var_temp =[col for col in weather_summary_pivot.columns 
                 if 'temp_bin'in col or 'rain_bin' in col or 'windspeed_bin' in col ]

    a = weather_all[var_temp]
    b = weather_relative[var_temp]
    dist = scipy.spatial.distance.cdist(a,b,) # pick the appropriate distance metric 
    dist_matrix = pd.DataFrame(dist)
    date = weather_hourly_pivot['date']
    date = pd.Series(date)  
    dist_matrix['date'] = date.values
    dist_matrix['date'] = pd.to_datetime(dist_matrix['date'])
    dist_matrix = pd.melt(dist_matrix, id_vars = ['date'], value_name='eucledean_dist')
    dist_matrix.rename(columns={'variable': 'lag_window'}, inplace=True)
    dist_matrix['lag_window'] = dist_matrix['lag_window']+1
    dist_matrix['lag_window'] = pd.to_numeric(dist_matrix['lag_window'])
    unique_lag_window =dist_matrix['lag_window'].unique()
    unique_lag_window = pd.DataFrame(unique_lag_window)
    unique_lag_window.rename(columns={0: 'lag_window'}, inplace=True)
    lag_window = unique_lag_window['lag_window'] 
    ts = pd.Series(unique_lag_window['lag_window'].values)
    lag_date = pd.to_datetime(weather_relative['date'])
    lag_date = pd.DataFrame(lag_date)
    lag_date['lag_window']= ts.values
    lag_date.rename(columns={'date': 'lag_date'}, inplace=True)
    dist_matrix_final = pd.merge(dist_matrix, 
                                 lag_date,
                                 how = 'left',
                                 on = ['lag_window']
                                  )
    dist_matrix_final = dist_matrix_final[dist_matrix_final['date'] > dist_matrix_final['lag_date']]
    dist_matrix_final = dist_matrix_final[dist_matrix_final['lag_date'] >= dist_matrix_final['date'] 
                                                - timedelta(days=nn_days)]

    dist_matrix_final.sort_values(by = ['date','eucledean_dist'], ascending = [True,True], inplace = True)
    def ranker(dist_matrix_final):
        dist_matrix_final['rank_dist'] = np.arange(len(dist_matrix_final)) + 1
        return dist_matrix_final
    dist_matrix_final = dist_matrix_final.groupby(dist_matrix_final['date']).apply(ranker)
    dist_matrix_WSH = dist_matrix_final[dist_matrix_final['rank_dist']<=12]
    dist_matrix_WSH = dist_matrix_WSH[np.isfinite(dist_matrix_WSH['eucledean_dist'])]

    weather_dist_lag_initial = pd.merge(dist_matrix_WSH,solar_only_table_final, 
                                       left_on ='lag_date', right_on = 'date' )
    weather_dist_lag_initial.rename(columns={'date_x': 'date'}, inplace=True)
    weather_dist_lag_initial = weather_dist_lag_initial.drop('date_y', axis=1)
    weather_dist_lag_initial['rank_no']=weather_dist_lag_initial['rank_dist'].astype(str)+'lag'
    weather_dist_lag = pd.pivot_table(weather_dist_lag_initial, values=['endo_SOLAR'], 
                                                  index=['date','block_no'], columns=['rank_no']).reset_index()
    weather_dist_lag.columns = ['_'.join(col).strip() for col in weather_dist_lag.columns.values]
    weather_dist_lag.rename(columns={'date_':'date','block_no_':'block_no'},inplace = True)


    weather_dist_lag = pd.merge(solar_only_table_final,weather_dist_lag,
                                how = 'left',
                                on = ['date','block_no'])
    
    weather_dist_lag['endo_SOLAR_8lag'].fillna( weather_dist_lag['endo_SOLAR_9lag'],inplace = True)
    weather_dist_lag['endo_SOLAR_7lag'].fillna( weather_dist_lag['endo_SOLAR_8lag'],inplace = True)
    weather_dist_lag['endo_SOLAR_6lag'].fillna( weather_dist_lag['endo_SOLAR_7lag'],inplace = True)
    weather_dist_lag['endo_SOLAR_5lag'].fillna( weather_dist_lag['endo_SOLAR_4lag'],inplace = True)
    weather_dist_lag['endo_SOLAR_4lag'].fillna( weather_dist_lag['endo_SOLAR_3lag'],inplace = True)
    weather_dist_lag['endo_SOLAR_3lag'].fillna( weather_dist_lag['endo_SOLAR_2lag'],inplace = True)
    weather_dist_lag['endo_SOLAR_2lag'].fillna( weather_dist_lag['endo_SOLAR_1lag'],inplace = True)

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

    weather_dist_lag['sim_day_solar']= (weather_dist_lag['endo_SOLAR_1lag']*w1+
                                                 weather_dist_lag['endo_SOLAR_2lag']*w2+
                                                 weather_dist_lag['endo_SOLAR_3lag']*w3+
                                                 weather_dist_lag['endo_SOLAR_4lag']*w4+
                                                 weather_dist_lag['endo_SOLAR_5lag']*w5+
                                                 weather_dist_lag['endo_SOLAR_6lag']*w6+
                                                 weather_dist_lag['endo_SOLAR_7lag']*w7+
                                                 weather_dist_lag['endo_SOLAR_8lag']*w8+
                                                 weather_dist_lag['endo_SOLAR_9lag']*w9
                                                 )\
                                                 /(w1+w2+w3+w4+w5+w6+w7+w8+w9)

    SOLAR_forecast_nn_temp = weather_dist_lag[['date',
                                      'block_no',
                                      'per_gen',
                                      'sim_day_solar']]
    previsous_day_capacity = capacity_summary_final[['date','block_no','gen_MW','rated_unit_capacity_sum_mean']]


    previsous_day_capacity['date'] = pd.to_datetime(previsous_day_capacity['date'])
    SOLAR_forecast_nn_temp['date'] = pd.to_datetime(SOLAR_forecast_nn_temp['date'])

    SOLAR_forecast_nn_temp_final = pd.merge(SOLAR_forecast_nn_temp,
                                        previsous_day_capacity,
                                        how = 'left',
                                        on = ['date','block_no'])

    SOLAR_forecast_nn_temp_final['rated_unit_capacity_sum_mean'].fillna(method='ffill',inplace=True)

    smooth_SOLAR_forecast_NN_ALL = SOLAR_forecast_nn_temp_final.copy()
    med_filt = medfilt(smooth_SOLAR_forecast_NN_ALL['sim_day_solar'], 5)
    med_filt = pd.Series(med_filt)
    smooth_SOLAR_forecast_NN_ALL['med_filt'] = med_filt.values
    smooth_SOLAR_forecast_NN_ALL.rename(columns={'med_filt': 'SOLAR_forecast_NN_ALL'}, inplace=True)
    smooth_SOLAR_forecast_NN_ALL.sort_values(by = ['date','block_no'], ascending=[True, True], inplace=True)
    smooth_SOLAR_forecast_NN_ALL = smooth_SOLAR_forecast_NN_ALL[['date','block_no','SOLAR_forecast_NN_ALL']]

    # In[185]:
    SOLAR_forecast_composit = pd.merge(pd.merge(pd.merge(pd.merge(smooth_SOLAR_forecast_NN_TEMP,
                                      smooth_SOLAR_forecast_NN_TEMPWIND,
                                      how = 'left',
                                      on = ['date','block_no']),
                                      smooth_SOLAR_forecast_NN_TEMPSUM,
                                      how = 'left',
                                      on = ['date','block_no']),
                                      smooth_SOLAR_forecast_NN_TEMPRAIN,
                                      how = 'left',
                                      on = ['date','block_no']),
                                      smooth_SOLAR_forecast_NN_ALL,
                                      how = 'left',
                                      on = ['date','block_no'])

    # In[198]:

    SOLAR_forecast_composit['final_forecast'] = SOLAR_forecast_composit[[
                                                'SOLAR_forecast_NN_TEMP',
                                                'SOLAR_forecast_NN_TEMPRAIN',
                                                'SOLAR_forecast_NN_TEMPSUM'
                                                ]].mean(axis = 1)
    SOLAR_forecast_composit['final_forecast'] = np.where((SOLAR_forecast_composit['final_forecast']<=0),0,
                                                         SOLAR_forecast_composit['final_forecast'])


    # forecast_table = SOLAR_only_table[['gen_MW', 'date', 'block_no']]
    # forecast_table = pd.merge(forecast_table,SOLAR_forecast_composit,
    #                          how = 'left',
    #                          on = ['date','block_no'])


    Pred_table_SOLAR_NN = SOLAR_forecast_composit[['date' , 'block_no', 'final_forecast']]
    Pred_table_SOLAR_NN.rename(columns={'final_forecast': 'SOLAR_gen_forecast'}, inplace=True)
    Pred_table_SOLAR_NN['org_name']=discom
    Pred_table_SOLAR_NN['pool_name']='INT_GENERATION_FOR'
    Pred_table_SOLAR_NN['pool_type']='SOLAR'
    Pred_table_SOLAR_NN['entity_name']=discom
    Pred_table_SOLAR_NN['state']= state
    Pred_table_SOLAR_NN['revision']=0
    Pred_table_SOLAR_NN['model_name']='NN'

    tablename = 'Pred_table_SOLAR_NN_{}'.format(discom)
    Pred_table_SOLAR_NN.to_sql(con=engine, name=tablename, if_exists='replace', flavor='mysql', index = False)

    sql_str = """insert into power.gen_forecast_stg
                (date, state, revision, org_name,
                pool_name, pool_type, entity_name,
                block_no, gen_forecast, Model_name)
                select h.date, h.state, h.revision, h.org_name,
                h.pool_name, h.pool_type, a.unit_name, h.block_no,
                round(coalesce(a.rated_unit_capacity * solar_gen_forecast, 0),2) gen_forecast,
                model_name
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
                and a.delete_ind = 0
                and c.delete_ind = 0
                and d.delete_ind = 0) g,
                {} h
                where c.unit_type_pk = a.unit_type_fk
                and a.unit_name = g.cp2
                and date(h.date) between g.delivery_start_date and g.delivery_end_date
                and c.unit_type_name = h.pool_type
                on duplicate key
                update gen_forecast = round(coalesce(a.rated_unit_capacity * solar_gen_forecast, 0),2),
                load_date = NULL""".format(discom, tablename)
    connection = engine.connect()
    connection.execute(sql_str)
    connection.close()
    return

# config = 'mysql+mysqldb://root:quenext@2016@104.155.225.29/power'
# # discom = "ADANI"
# # state = "TAMIL NADU"
# discom='GUVNL'
# state='GUJARAT'
# forecast_solar_nn(config, discom, state)

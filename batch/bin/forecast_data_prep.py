
# coding: utf-8

# In[221]:

# import statsmodels.datasets as datasets
# import sklearn.metrics as metrics
from numpy import log
# from pyearth import Earth as earth
# from matplotlib import pyplot
# from matplotlib import pyplot as plt
# import statsmodels.api as sm
import dbconn
import os

basedir = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       os.pardir, 'config'))
# dsnfile = basedir + '/sqldb_connection_config.txt'
dsnfile = basedir + '/sqldb_gcloud.txt'


from pandas.io import sql
# import MySQLdb
import pandas as pd
import numpy as np
pd.options.mode.chained_assignment = None
# db = MySQLdb.connect(host="localhost",    # your host, usually localhost
#                      user="root",         # your username
#                      passwd="power@2012",  # your password
#                      db="power")
db = dbconn.connect(dsnfile)
cur = db.cursor()
weather_dist_lag = pd.read_sql("""select * from weather_dist_lag_UPCL ;""",
                               con=db)
weather_dist_lag['date'] = pd.to_datetime(weather_dist_lag['date'])
cur.close()
# db.close ()


# In[223]:

from pandas.io import sql
# import MySQLdb
import pandas as pd
import numpy as np
# UPCL_DB = MySQLdb.connect(host="localhost",    # your host, usually localhost
#                      user="root",         # your username
#                      passwd="power@2012",  # your password
#                      db="power")        # name of the data base
UPCL_DB = dbconn.connect(dsnfile)
cur = UPCL_DB.cursor()
date_key = pd.read_sql("""select * from date_key_UPCL;""", con=UPCL_DB)
#drawl_data['date'] = pd.to_datetime(drawl_data['date'])
cur.close ()
# UPCL_DB.close ()


# In[224]:

from pandas.io import sql
# import MySQLdb
import pandas as pd
import numpy as np
# UPCL_DB = MySQLdb.connect(host="localhost",    # your host, usually localhost
#                      user="root",         # your username
#                      passwd="power@2012",  # your password
#                      db="power")        # name of the data base
UPCL_DB = dbconn.connect(dsnfile) 
cur = UPCL_DB.cursor()
lag_operator_UPCL = pd.read_sql("""select * from lag_operator_UPCL;""", con = UPCL_DB)
#drawl_data['date'] = pd.to_datetime(drawl_data['date'])
cur.close()
# UPCL_DB.close ()


# In[225]:

from pandas.io import sql
# import MySQLdb
import pandas as pd
import numpy as np
# UPCL_DB = MySQLdb.connect(host="localhost",    # your host, usually localhost
#                      user="root",         # your username
#                      passwd="power@2012",  # your password
#                      db="power")        # name of the data base
UPCL_DB = dbconn.connect(dsnfile) 
cur = UPCL_DB.cursor()
daily_weather_mat = pd.read_sql("""select * from daily_weather_mat_UPCL;""", con = UPCL_DB)
#drawl_data['date'] = pd.to_datetime(drawl_data['date'])
cur.close()
# UPCL_DB.close ()


# In[226]:

load_weather_lag = pd.merge(daily_weather_mat,lag_operator_UPCL, how = 'outer', on = ['date_key'])
load_weather_lag = load_weather_lag.merge(daily_weather_mat , left_on = ['lag1'] , right_on = ['date_key'] , 
                                                                              suffixes=('_left', '_right'))


# In[227]:

load_weather_lag['exog_cont_tempmean_location_01_diff'] = load_weather_lag['tempmean_location_01_left'] - load_weather_lag['tempmean_location_01_right']
load_weather_lag['exog_cont_tempmean_location_02_diff'] = load_weather_lag['tempmean_location_02_left'] - load_weather_lag['tempmean_location_02_right']
load_weather_lag['exog_cont_tempmean_location_03_diff'] = load_weather_lag['tempmean_location_03_left'] - load_weather_lag['tempmean_location_03_right']
load_weather_lag['exog_cont_tempmean_location_04_diff'] = load_weather_lag['tempmean_location_04_left'] - load_weather_lag['tempmean_location_04_right']
load_weather_lag['exog_cont_tempmean_location_05_diff'] = load_weather_lag['tempmean_location_05_left'] - load_weather_lag['tempmean_location_05_right']
load_weather_lag['exog_cont_tempmean_location_06_diff'] = load_weather_lag['tempmean_location_06_left'] - load_weather_lag['tempmean_location_06_right']
load_weather_lag['exog_cont_tempmean_location_07_diff'] = load_weather_lag['tempmean_location_07_left'] - load_weather_lag['tempmean_location_07_right']
# load_weather_lag['exog_cont_tempmean_location_08_diff'] = load_weather_lag['tempmean_location_08_left'] - load_weather_lag['tempmean_location_08_right']
load_weather_lag['exog_cont_tempmean_location_09_diff'] = load_weather_lag['tempmean_location_09_left'] - load_weather_lag['tempmean_location_09_right']
# load_weather_lag['exog_cont_tempmean_location_10_diff'] = load_weather_lag['tempmean_location_10_left'] - load_weather_lag['tempmean_location_10_right']
load_weather_lag['exog_cont_tempmean_location_11_diff'] = load_weather_lag['tempmean_location_11_left'] - load_weather_lag['tempmean_location_11_right']
# load_weather_lag['exog_cont_tempmean_location_12_diff'] = load_weather_lag['tempmean_location_12_left'] - load_weather_lag['tempmean_location_12_right']
load_weather_lag['exog_cont_tempmean_location_13_diff'] = load_weather_lag['tempmean_location_13_left'] - load_weather_lag['tempmean_location_13_right']
load_weather_lag['exog_cont_tempmean_location_14_diff'] = load_weather_lag['tempmean_location_14_left'] - load_weather_lag['tempmean_location_14_right']
# load_weather_lag['exog_cont_tempmean_location_15_diff'] = load_weather_lag['tempmean_location_15_left'] - load_weather_lag['tempmean_location_15_right']
# load_weather_lag['exog_cont_tempmean_location_16_diff'] = load_weather_lag['tempmean_location_16_left'] - load_weather_lag['tempmean_location_16_right']
load_weather_lag['exog_cont_tempmean_location_17_diff'] = load_weather_lag['tempmean_location_17_left'] - load_weather_lag['tempmean_location_17_right']
load_weather_lag['exog_cont_tempmean_location_18_diff'] = load_weather_lag['tempmean_location_18_left'] - load_weather_lag['tempmean_location_18_right']
load_weather_lag['exog_cont_tempmean_location_19_diff'] = load_weather_lag['tempmean_location_19_left'] - load_weather_lag['tempmean_location_19_right']
load_weather_lag['exog_cont_tempmean_location_20_diff'] = load_weather_lag['tempmean_location_20_left'] - load_weather_lag['tempmean_location_20_right']
load_weather_lag['exog_cont_tempmean_location_21_diff'] = load_weather_lag['tempmean_location_21_left'] - load_weather_lag['tempmean_location_21_right']
load_weather_lag['exog_cont_tempmean_location_22_diff'] = load_weather_lag['tempmean_location_22_left'] - load_weather_lag['tempmean_location_22_right']
load_weather_lag['exog_cont_tempmean_location_23_diff'] = load_weather_lag['tempmean_location_23_left'] - load_weather_lag['tempmean_location_23_right']


# load_weather_lag['exog_cont_tempmax_location_01_diff'] = load_weather_lag['tempmax_location_01_left'] - load_weather_lag['tempmax_location_01_right']
# load_weather_lag['exog_cont_tempmax_location_02_diff'] = load_weather_lag['tempmax_location_02_left'] - load_weather_lag['tempmax_location_02_right']
# load_weather_lag['exog_cont_tempmax_location_03_diff'] = load_weather_lag['tempmax_location_03_left'] - load_weather_lag['tempmax_location_03_right']
# load_weather_lag['exog_cont_tempmax_location_04_diff'] = load_weather_lag['tempmax_location_04_left'] - load_weather_lag['tempmax_location_04_right']
# load_weather_lag['exog_cont_tempmax_location_05_diff'] = load_weather_lag['tempmax_location_05_left'] - load_weather_lag['tempmax_location_05_right']
# load_weather_lag['exog_cont_tempmax_location_06_diff'] = load_weather_lag['tempmax_location_06_left'] - load_weather_lag['tempmax_location_06_right']
# load_weather_lag['exog_cont_tempmax_location_07_diff'] = load_weather_lag['tempmax_location_07_left'] - load_weather_lag['tempmax_location_07_right']
# # load_weather_lag['exog_cont_tempmax_location_08_diff'] = load_weather_lag['tempmax_location_08_left'] - load_weather_lag['tempmax_location_08_right']
# load_weather_lag['exog_cont_tempmax_location_09_diff'] = load_weather_lag['tempmax_location_09_left'] - load_weather_lag['tempmax_location_09_right']
# # load_weather_lag['exog_cont_tempmax_location_10_diff'] = load_weather_lag['tempmax_location_10_left'] - load_weather_lag['tempmax_location_10_right']
# load_weather_lag['exog_cont_tempmax_location_11_diff'] = load_weather_lag['tempmax_location_11_left'] - load_weather_lag['tempmax_location_11_right']
# # load_weather_lag['exog_cont_tempmax_location_12_diff'] = load_weather_lag['tempmax_location_12_left'] - load_weather_lag['tempmax_location_12_right']
# load_weather_lag['exog_cont_tempmax_location_13_diff'] = load_weather_lag['tempmax_location_13_left'] - load_weather_lag['tempmax_location_13_right']
# load_weather_lag['exog_cont_tempmax_location_14_diff'] = load_weather_lag['tempmax_location_14_left'] - load_weather_lag['tempmax_location_14_right']
# # load_weather_lag['exog_cont_tempmax_location_15_diff'] = load_weather_lag['tempmax_location_15_left'] - load_weather_lag['tempmax_location_15_right']
# # load_weather_lag['exog_cont_tempmax_location_16_diff'] = load_weather_lag['tempmax_location_16_left'] - load_weather_lag['tempmax_location_16_right']
# load_weather_lag['exog_cont_tempmax_location_17_diff'] = load_weather_lag['tempmax_location_17_left'] - load_weather_lag['tempmax_location_17_right']
# load_weather_lag['exog_cont_tempmax_location_18_diff'] = load_weather_lag['tempmax_location_18_left'] - load_weather_lag['tempmax_location_18_right']
# load_weather_lag['exog_cont_tempmax_location_19_diff'] = load_weather_lag['tempmax_location_19_left'] - load_weather_lag['tempmax_location_19_right']
# load_weather_lag['exog_cont_tempmax_location_20_diff'] = load_weather_lag['tempmax_location_20_left'] - load_weather_lag['tempmax_location_20_right']
# load_weather_lag['exog_cont_tempmax_location_21_diff'] = load_weather_lag['tempmax_location_21_left'] - load_weather_lag['tempmax_location_21_right']
# load_weather_lag['exog_cont_tempmax_location_22_diff'] = load_weather_lag['tempmax_location_22_left'] - load_weather_lag['tempmax_location_22_right']
# load_weather_lag['exog_cont_tempmax_location_23_diff'] = load_weather_lag['tempmax_location_23_left'] - load_weather_lag['tempmax_location_23_right']

# load_weather_lag['exog_cont_tempmin_location_01_diff'] = load_weather_lag['tempmin_location_01_left'] - load_weather_lag['tempmin_location_01_right']
# load_weather_lag['exog_cont_tempmin_location_02_diff'] = load_weather_lag['tempmin_location_02_left'] - load_weather_lag['tempmin_location_02_right']
# load_weather_lag['exog_cont_tempmin_location_03_diff'] = load_weather_lag['tempmin_location_03_left'] - load_weather_lag['tempmin_location_03_right']
# load_weather_lag['exog_cont_tempmin_location_04_diff'] = load_weather_lag['tempmin_location_04_left'] - load_weather_lag['tempmin_location_04_right']
# load_weather_lag['exog_cont_tempmin_location_05_diff'] = load_weather_lag['tempmin_location_05_left'] - load_weather_lag['tempmin_location_05_right']
# load_weather_lag['exog_cont_tempmin_location_06_diff'] = load_weather_lag['tempmin_location_06_left'] - load_weather_lag['tempmin_location_06_right']
# load_weather_lag['exog_cont_tempmin_location_07_diff'] = load_weather_lag['tempmin_location_07_left'] - load_weather_lag['tempmin_location_07_right']
# # load_weather_lag['exog_cont_tempmin_location_08_diff'] = load_weather_lag['tempmin_location_08_left'] - load_weather_lag['tempmin_location_08_right']
# load_weather_lag['exog_cont_tempmin_location_09_diff'] = load_weather_lag['tempmin_location_09_left'] - load_weather_lag['tempmin_location_09_right']
# # load_weather_lag['exog_cont_tempmin_location_10_diff'] = load_weather_lag['tempmin_location_10_left'] - load_weather_lag['tempmin_location_10_right']
# load_weather_lag['exog_cont_tempmin_location_11_diff'] = load_weather_lag['tempmin_location_11_left'] - load_weather_lag['tempmin_location_11_right']
# # load_weather_lag['exog_cont_tempmin_location_12_diff'] = load_weather_lag['tempmin_location_12_left'] - load_weather_lag['tempmin_location_12_right']
# load_weather_lag['exog_cont_tempmin_location_13_diff'] = load_weather_lag['tempmin_location_13_left'] - load_weather_lag['tempmin_location_13_right']
# load_weather_lag['exog_cont_tempmin_location_14_diff'] = load_weather_lag['tempmin_location_14_left'] - load_weather_lag['tempmin_location_14_right']
# # load_weather_lag['exog_cont_tempmin_location_15_diff'] = load_weather_lag['tempmin_location_15_left'] - load_weather_lag['tempmin_location_15_right']
# # load_weather_lag['exog_cont_tempmin_location_16_diff'] = load_weather_lag['tempmin_location_16_left'] - load_weather_lag['tempmin_location_16_right']
# load_weather_lag['exog_cont_tempmin_location_17_diff'] = load_weather_lag['tempmin_location_17_left'] - load_weather_lag['tempmin_location_17_right']
# load_weather_lag['exog_cont_tempmin_location_18_diff'] = load_weather_lag['tempmin_location_18_left'] - load_weather_lag['tempmin_location_18_right']
# load_weather_lag['exog_cont_tempmin_location_19_diff'] = load_weather_lag['tempmin_location_19_left'] - load_weather_lag['tempmin_location_19_right']
# load_weather_lag['exog_cont_tempmin_location_20_diff'] = load_weather_lag['tempmin_location_20_left'] - load_weather_lag['tempmin_location_20_right']
# load_weather_lag['exog_cont_tempmin_location_21_diff'] = load_weather_lag['tempmin_location_21_left'] - load_weather_lag['tempmin_location_21_right']
# load_weather_lag['exog_cont_tempmin_location_22_diff'] = load_weather_lag['tempmin_location_22_left'] - load_weather_lag['tempmin_location_22_right']
# load_weather_lag['exog_cont_tempmin_location_23_diff'] = load_weather_lag['tempmin_location_23_left'] - load_weather_lag['tempmin_location_23_right']

# load_weather_lag['exog_cont_temp_dev_location_01_diff'] = load_weather_lag['temp_dev_location_01_left'] - load_weather_lag['temp_dev_location_01_right']
# load_weather_lag['exog_cont_temp_dev_location_02_diff'] = load_weather_lag['temp_dev_location_02_left'] - load_weather_lag['temp_dev_location_02_right']
# load_weather_lag['exog_cont_temp_dev_location_03_diff'] = load_weather_lag['temp_dev_location_03_left'] - load_weather_lag['temp_dev_location_03_right']
# load_weather_lag['exog_cont_temp_dev_location_04_diff'] = load_weather_lag['temp_dev_location_04_left'] - load_weather_lag['temp_dev_location_04_right']
# load_weather_lag['exog_cont_temp_dev_location_05_diff'] = load_weather_lag['temp_dev_location_05_left'] - load_weather_lag['temp_dev_location_05_right']
# load_weather_lag['exog_cont_temp_dev_location_06_diff'] = load_weather_lag['temp_dev_location_06_left'] - load_weather_lag['temp_dev_location_06_right']
# load_weather_lag['exog_cont_temp_dev_location_07_diff'] = load_weather_lag['temp_dev_location_07_left'] - load_weather_lag['temp_dev_location_07_right']
# # load_weather_lag['exog_cont_temp_dev_location_08_diff'] = load_weather_lag['temp_dev_location_08_left'] - load_weather_lag['temp_dev_location_08_right']
# load_weather_lag['exog_cont_temp_dev_location_09_diff'] = load_weather_lag['temp_dev_location_09_left'] - load_weather_lag['temp_dev_location_09_right']
# # load_weather_lag['exog_cont_temp_dev_location_10_diff'] = load_weather_lag['temp_dev_location_10_left'] - load_weather_lag['temp_dev_location_10_right']
# load_weather_lag['exog_cont_temp_dev_location_11_diff'] = load_weather_lag['temp_dev_location_11_left'] - load_weather_lag['temp_dev_location_11_right']
# # load_weather_lag['exog_cont_temp_dev_location_12_diff'] = load_weather_lag['temp_dev_location_12_left'] - load_weather_lag['temp_dev_location_12_right']
# load_weather_lag['exog_cont_temp_dev_location_13_diff'] = load_weather_lag['temp_dev_location_13_left'] - load_weather_lag['temp_dev_location_13_right']
# load_weather_lag['exog_cont_temp_dev_location_14_diff'] = load_weather_lag['temp_dev_location_14_left'] - load_weather_lag['temp_dev_location_14_right']
# # load_weather_lag['exog_cont_temp_dev_location_15_diff'] = load_weather_lag['temp_dev_location_15_left'] - load_weather_lag['temp_dev_location_15_right']
# # load_weather_lag['exog_cont_temp_dev_location_16_diff'] = load_weather_lag['temp_dev_location_16_left'] - load_weather_lag['temp_dev_location_16_right']
# load_weather_lag['exog_cont_temp_dev_location_17_diff'] = load_weather_lag['temp_dev_location_17_left'] - load_weather_lag['temp_dev_location_17_right']
# load_weather_lag['exog_cont_temp_dev_location_18_diff'] = load_weather_lag['temp_dev_location_18_left'] - load_weather_lag['temp_dev_location_18_right']
# load_weather_lag['exog_cont_temp_dev_location_19_diff'] = load_weather_lag['temp_dev_location_19_left'] - load_weather_lag['temp_dev_location_19_right']
# load_weather_lag['exog_cont_temp_dev_location_20_diff'] = load_weather_lag['temp_dev_location_20_left'] - load_weather_lag['temp_dev_location_20_right']
# load_weather_lag['exog_cont_temp_dev_location_21_diff'] = load_weather_lag['temp_dev_location_21_left'] - load_weather_lag['temp_dev_location_21_right']
# load_weather_lag['exog_cont_temp_dev_location_22_diff'] = load_weather_lag['temp_dev_location_22_left'] - load_weather_lag['temp_dev_location_22_right']
# load_weather_lag['exog_cont_temp_dev_location_23_diff'] = load_weather_lag['temp_dev_location_23_left'] - load_weather_lag['temp_dev_location_23_right']


# In[229]:

diff_index = load_weather_lag.columns[load_weather_lag.columns.to_series().str.contains('_diff')]
index_date = load_weather_lag.columns[load_weather_lag.columns.to_series().str.contains('date_left')]
weather_diff = load_weather_lag[load_weather_lag.columns[load_weather_lag.columns.to_series().str.contains('_diff')]]
date = load_weather_lag[['date_left','date_key_left']]
weather_diff1 = pd.concat([date,weather_diff], axis=1) 
weather_diff1.rename(columns={'date_left': 'date', 'date_key_left': 'date_key'}, inplace=True)
data_forecast = pd.merge(weather_dist_lag, weather_diff1 , how = 'left', on = ['date_key'])
data_forecast.rename(columns={'date_x': 'date'}, inplace=True)
data_forecast['endo_residual'] = data_forecast['endo_demand'] - data_forecast['endo_pred_sim_day_load']
data_forecast = data_forecast.drop('date_y', axis=1)

#data_forecast = data_forecast.dropna()
#data_forecast.rename(columns={'date_x': 'date'}, inplace=True)


# In[231]:

data_bias = data_forecast[['date' , 'year','month','block_no' , 'hour', 'endo_demand' , 'endo_pred_sim_day_load',                            'endo_residual','week_day']]

error_pivot_dayofweek = pd.pivot_table(data_bias, values=['endo_residual'], 
                                              index=['block_no'], columns=['week_day'],
                                      aggfunc=np.mean ).reset_index()
error_pivot_dayofweek.columns =[s1 + str(s2)  for (s1,s2) in error_pivot_dayofweek.columns.tolist()]


# In[232]:

import numpy
# from matplotlib.pyplot import *
mon_poly_coef = numpy.polyfit(error_pivot_dayofweek['block_no'],
                                 error_pivot_dayofweek['endo_residual0'], 6)
mon_poly = numpy.poly1d(mon_poly_coef)
mon_bias = mon_poly(error_pivot_dayofweek['block_no'])
# print mon_poly_coef
# print mon_poly
# plot(error_pivot_dayofweek['block_no'], error_pivot_dayofweek['endo_residual0'], 'o')
# plot(error_pivot_dayofweek['block_no'], mon_bias)
# ylabel('endo_residual')
# xlabel('block_no')
# xlim(1,96)
# ylim(-200,200)
# show()

mon_bias = pd.DataFrame(mon_bias)
mon_bias.rename(columns={0: 'weekend_correction'}, inplace=True)
block_no = range(1, 97)
block_no = pd.DataFrame(block_no)
block_no.rename(columns={0: 'block_no'}, inplace=True)
mon_bias_correction = pd.concat([mon_bias,block_no], axis=1)
mon_bias_correction['week_day'] = 0


# In[233]:

import numpy
# from matplotlib.pyplot import *
sat_poly_coef = numpy.polyfit(error_pivot_dayofweek['block_no'],
                                 error_pivot_dayofweek['endo_residual5'], 6)
sat_poly = numpy.poly1d(sat_poly_coef)
sat_bias = sat_poly(error_pivot_dayofweek['block_no'])
# print sat_poly_coef
# print sat_poly
# plot(error_pivot_dayofweek['block_no'], error_pivot_dayofweek['endo_residual5'], 'o')
# plot(error_pivot_dayofweek['block_no'], sat_bias)
# ylabel('endo_residual')
# xlabel('block_no')
# xlim(1,96)
# ylim(-200,200)
# show()

sat_bias = pd.DataFrame(sat_bias)
sat_bias.rename(columns={0: 'weekend_correction'}, inplace=True)
block_no = range(1, 97)
block_no = pd.DataFrame(block_no)
block_no.rename(columns={0: 'block_no'}, inplace=True)
sat_bias_correction = pd.concat([sat_bias,block_no], axis=1)



# In[234]:

import numpy
# from matplotlib.pyplot import *
sun_poly_coef = numpy.polyfit(error_pivot_dayofweek['block_no'],
                                 error_pivot_dayofweek['endo_residual6'], 5)
sun_poly = numpy.poly1d(sun_poly_coef)
sun_bias = sun_poly(error_pivot_dayofweek['block_no'])
# print sun_poly_coef
# print sun_poly
# plot(error_pivot_dayofweek['block_no'], error_pivot_dayofweek['endo_residual6'], 'o')
# plot(error_pivot_dayofweek['block_no'], sun_bias)
# ylabel('endo_residual')
# xlabel('block_no')
# xlim(1,96)
# ylim(-200,200)
# show()

sun_bias = pd.DataFrame(sun_bias)
sun_bias.rename(columns={0: 'weekend_correction'}, inplace=True)
block_no = range(1, 97)
block_no = pd.DataFrame(block_no)
block_no.rename(columns={0: 'block_no'}, inplace=True)
sun_bias_correction = pd.concat([sun_bias,block_no], axis=1)
sun_bias_correction['week_day'] = 6



# In[235]:

weekend_Bias = pd.concat([mon_bias_correction,sat_bias_correction,sun_bias_correction], axis =0)
data_forecast = pd.merge(data_forecast, weekend_Bias , how = 'left', on = ['block_no','week_day'])


# In[236]:

data_forecast['weekend_correction'].fillna(0, inplace=True)


# In[237]:

data_forecast['endo_pred_sim_day_load_final'] = data_forecast['endo_pred_sim_day_load'] + data_forecast['weekend_correction']


# In[238]:

data_forecast['endo_residual_weekend_correction'] = data_forecast['endo_demand'] - data_forecast['endo_pred_sim_day_load_final']


# In[239]:

data_bias_fest = data_forecast[['date' , 'year','month','block_no' , 'hour', 'endo_demand','endo_pred_sim_day_load',
                                  'endo_residual_weekend_correction']]


# In[241]:

# db = MySQLdb.connect(host="localhost",    # your host, usually localhost
#                      user="root",         # your username
#                      passwd="power@2012",  # your password
#                      db="power")   
db = dbconn.connect(dsnfile)      
cur = db.cursor()
holiday_event_master  = pd.read_sql("""select date, event1 as name from vw_holiday_event_master 
                                       where state = 'UTTARAKHAND' ;""",con = db)
holiday_event_master['date'] = pd.to_datetime(holiday_event_master['date'])
cur.close()
# db.close ()


# In[242]:

event_date = holiday_event_master[['date','name']]


# In[244]:

data_bias_fest = pd.merge(data_bias_fest, holiday_event_master, on=['date'], how='left')


# In[245]:

from pandas.io import sql
# import MySQLdb
# UPCL_DB = MySQLdb.connect(host="localhost",    # your host, usually localhost
#                      user="root",         # your username
#                      passwd="power@2012",  # your password
#                      db="power")        # name of the data base
UPCL_DB = dbconn.connect(dsnfile) 
cur = UPCL_DB.cursor()
data_bias_fest.to_sql(con=UPCL_DB, name='upcl_data_bias_fest', 
                if_exists='replace', flavor='mysql')
UPCL_DB.commit()
cur.close()
# UPCL_DB.close ()


# In[247]:

error_pivot_fest = pd.pivot_table(data_bias_fest, values=['endo_residual_weekend_correction'], 
                                              index=['block_no'], columns=['name'],
                                      aggfunc=np.mean ).reset_index()
error_pivot_fest.columns =[s1 + str(s2)  for (s1,s2) in error_pivot_fest.columns.tolist()]


# In[248]:

#  Correction factor for events 

#  Correction factor for events 

BhaiDooj_poly_coef = np.polyfit(error_pivot_fest['block_no'],
                                 error_pivot_fest['endo_residual_weekend_correctionBhaiDooj'], 2)
BhaiDooj_poly = np.poly1d(BhaiDooj_poly_coef)
BhaiDooj_bias = BhaiDooj_poly(error_pivot_fest['block_no'])

BhaiDooj_bias = pd.DataFrame(BhaiDooj_bias)
BhaiDooj_bias.rename(columns={0: 'fest_correction'}, inplace=True)
block_no = range(1, 97)
block_no = pd.DataFrame(block_no)
block_no.rename(columns={0: 'block_no'}, inplace=True)
BhaiDooj_bias = pd.concat([BhaiDooj_bias,block_no], axis=1)
BhaiDooj_bias['event_name'] = 'BhaiDooj'


Diwali2_poly_coef = np.polyfit(error_pivot_fest['block_no'],
                                 error_pivot_fest['endo_residual_weekend_correctionDiwali2'], 3)
Diwali2_poly = np.poly1d(Diwali2_poly_coef)
Diwali2_bias = Diwali2_poly(error_pivot_fest['block_no'])
Diwali2_bias = pd.DataFrame(Diwali2_bias)
Diwali2_bias.rename(columns={0: 'fest_correction'}, inplace=True)
block_no = range(1, 97)
block_no = pd.DataFrame(block_no)
block_no.rename(columns={0: 'block_no'}, inplace=True)
Diwali2_bias = pd.concat([Diwali2_bias,block_no], axis=1)
Diwali2_bias['event_name'] = 'Diwali2'


Diwali1_poly_coef = np.polyfit(error_pivot_fest['block_no'],
                                 error_pivot_fest['endo_residual_weekend_correctionDiwali1'], 3)
Diwali1_poly = np.poly1d(Diwali1_poly_coef)
Diwali1_bias = Diwali1_poly(error_pivot_fest['block_no'])
Diwali1_bias = pd.DataFrame(Diwali1_bias)
Diwali1_bias.rename(columns={0: 'fest_correction'}, inplace=True)
block_no = range(1, 97)
block_no = pd.DataFrame(block_no)
block_no.rename(columns={0: 'block_no'}, inplace=True)
Diwali1_bias = pd.concat([Diwali1_bias,block_no], axis=1)
Diwali1_bias['event_name'] = 'Diwali1'


DrBRAmbedkarsBday_poly_coef = np.polyfit(error_pivot_fest['block_no'],
                                 error_pivot_fest['endo_residual_weekend_correctionDrBRAmbedkarsBday'], 2)
DrBRAmbedkarsBday_poly = np.poly1d(DrBRAmbedkarsBday_poly_coef)
DrBRAmbedkarsBday_bias = DrBRAmbedkarsBday_poly(error_pivot_fest['block_no'])

DrBRAmbedkarsBday_bias = pd.DataFrame(DrBRAmbedkarsBday_bias)
DrBRAmbedkarsBday_bias.rename(columns={0: 'fest_correction'}, inplace=True)
block_no = range(1, 97)
block_no = pd.DataFrame(block_no)
block_no.rename(columns={0: 'block_no'}, inplace=True)
DrBRAmbedkarsBday_bias = pd.concat([DrBRAmbedkarsBday_bias,block_no], axis=1)
DrBRAmbedkarsBday_bias['event_name'] = 'DrBRAmbedkarsBday'


Dusherra_poly_coef = np.polyfit(error_pivot_fest['block_no'],
                                 error_pivot_fest['endo_residual_weekend_correctionDusherra'], 2)
Dusherra_poly = np.poly1d(Dusherra_poly_coef)
Dusherra_bias = Dusherra_poly(error_pivot_fest['block_no'])
Dusherra_bias = pd.DataFrame(Dusherra_bias)
Dusherra_bias.rename(columns={0: 'fest_correction'}, inplace=True)
block_no = range(1, 97)
block_no = pd.DataFrame(block_no)
block_no.rename(columns={0: 'block_no'}, inplace=True)
Dusherra_bias = pd.concat([Dusherra_bias,block_no], axis=1)
Dusherra_bias['event_name'] = 'Dusherra'


Election_poly_coef = np.polyfit(error_pivot_fest['block_no'],
                                 error_pivot_fest['endo_residual_weekend_correctionElection'], 3)
Election_poly = np.poly1d(Election_poly_coef)
Election_bias = Election_poly(error_pivot_fest['block_no'])

Election_bias = pd.DataFrame(Election_bias)
Election_bias.rename(columns={0: 'fest_correction'}, inplace=True)
block_no = range(1, 97)
block_no = pd.DataFrame(block_no)
block_no.rename(columns={0: 'block_no'}, inplace=True)
Election_bias = pd.concat([Election_bias,block_no], axis=1)
Election_bias['event_name'] = 'Election'


GuruNanakBday_poly_coef = np.polyfit(error_pivot_fest['block_no'],
                                 error_pivot_fest['endo_residual_weekend_correctionGuruNanakBday'], 2)
GuruNanakBday_poly = np.poly1d(GuruNanakBday_poly_coef)
GuruNanakBday_bias = GuruNanakBday_poly(error_pivot_fest['block_no'])
GuruNanakBday_bias = pd.DataFrame(GuruNanakBday_bias)
GuruNanakBday_bias.rename(columns={0: 'fest_correction'}, inplace=True)
block_no = range(1, 97)
block_no = pd.DataFrame(block_no)
block_no.rename(columns={0: 'block_no'}, inplace=True)
GuruNanakBday_bias = pd.concat([GuruNanakBday_bias,block_no], axis=1)
GuruNanakBday_bias['event_name'] = 'GuruNanakBday'

GuruTegBahadurDday_poly_coef = np.polyfit(error_pivot_fest['block_no'],
                                 error_pivot_fest['endo_residual_weekend_correctionGuruTegBahadurDday'], 2)
GuruTegBahadurDday_poly = np.poly1d(GuruTegBahadurDday_poly_coef)
GuruTegBahadurDday_bias = GuruTegBahadurDday_poly(error_pivot_fest['block_no'])
GuruTegBahadurDday_bias = pd.DataFrame(GuruTegBahadurDday_bias)
GuruTegBahadurDday_bias.rename(columns={0: 'fest_correction'}, inplace=True)
block_no = range(1, 97)
block_no = pd.DataFrame(block_no)
block_no.rename(columns={0: 'block_no'}, inplace=True)
GuruTegBahadurDday_bias = pd.concat([GuruTegBahadurDday_bias,block_no], axis=1)
GuruTegBahadurDday_bias['event_name'] = 'GuruTegBahadurDday'



Holi_poly_coef = np.polyfit(error_pivot_fest['block_no'],
                                 error_pivot_fest['endo_residual_weekend_correctionHoli'], 7)
Holi_poly = np.poly1d(Holi_poly_coef)
Holi_bias = Holi_poly(error_pivot_fest['block_no'])
Holi_bias = pd.DataFrame(Holi_bias)
Holi_bias.rename(columns={0: 'fest_correction'}, inplace=True)
block_no = range(1, 97)
block_no = pd.DataFrame(block_no)
block_no.rename(columns={0: 'block_no'}, inplace=True)
Holi_bias = pd.concat([Holi_bias,block_no], axis=1)
Holi_bias['event_name'] = 'Holi'

HoliKaDahan_poly_coef = np.polyfit(error_pivot_fest['block_no'],
                                 error_pivot_fest['endo_residual_weekend_correctionHoliKaDahan'], 4)
HoliKaDahan_poly = np.poly1d(HoliKaDahan_poly_coef)
HoliKaDahan_bias = HoliKaDahan_poly(error_pivot_fest['block_no'])
HoliKaDahan_bias = pd.DataFrame(HoliKaDahan_bias)
HoliKaDahan_bias.rename(columns={0: 'fest_correction'}, inplace=True)
block_no = range(1, 97)
block_no = pd.DataFrame(block_no)
block_no.rename(columns={0: 'block_no'}, inplace=True)
HoliKaDahan_bias = pd.concat([HoliKaDahan_bias,block_no], axis=1)
HoliKaDahan_bias['event_name'] = 'HoliKaDahan'

Id_Ul_Fitr_poly_coef = np.polyfit(error_pivot_fest['block_no'],
                                 error_pivot_fest['endo_residual_weekend_correctionId_Ul_Fitr'], 4)
Id_Ul_Fitr_poly = np.poly1d(Id_Ul_Fitr_poly_coef)
Id_Ul_Fitr_bias = Id_Ul_Fitr_poly(error_pivot_fest['block_no'])
Id_Ul_Fitr_bias = pd.DataFrame(Id_Ul_Fitr_bias)
Id_Ul_Fitr_bias.rename(columns={0: 'fest_correction'}, inplace=True)
block_no = range(1, 97)
block_no = pd.DataFrame(block_no)
block_no.rename(columns={0: 'block_no'}, inplace=True)
Id_Ul_Fitr_bias = pd.concat([Id_Ul_Fitr_bias,block_no], axis=1)
Id_Ul_Fitr_bias['event_name'] = 'Id_Ul_Fitr'


Id_Ul_Juha_Bakrid_poly_coef = np.polyfit(error_pivot_fest['block_no'],
                                 error_pivot_fest['endo_residual_weekend_correctionId_Ul_Juha_Bakrid'], 4)
Id_Ul_Juha_Bakrid_poly = np.poly1d(Id_Ul_Juha_Bakrid_poly_coef)
Id_Ul_Juha_Bakrid_bias = Id_Ul_Juha_Bakrid_poly(error_pivot_fest['block_no'])
Id_Ul_Juha_Bakrid_bias = pd.DataFrame(Id_Ul_Juha_Bakrid_bias)
Id_Ul_Juha_Bakrid_bias.rename(columns={0: 'fest_correction'}, inplace=True)
block_no = range(1, 97)
block_no = pd.DataFrame(block_no)
block_no.rename(columns={0: 'block_no'}, inplace=True)
Id_Ul_Juha_Bakrid_bias = pd.concat([Id_Ul_Juha_Bakrid_bias,block_no], axis=1)
Id_Ul_Juha_Bakrid_bias['event_name'] = 'Id_Ul_Juha_Bakrid'


IndependenceDay_poly_coef = np.polyfit(error_pivot_fest['block_no'],
                                 error_pivot_fest['endo_residual_weekend_correctionIndependenceDay'], 7)
IndependenceDay_poly = np.poly1d(IndependenceDay_poly_coef)
IndependenceDay_bias = IndependenceDay_poly(error_pivot_fest['block_no'])
IndependenceDay_bias = pd.DataFrame(IndependenceDay_bias)
IndependenceDay_bias.rename(columns={0: 'fest_correction'}, inplace=True)
block_no = range(1, 97)
block_no = pd.DataFrame(block_no)
block_no.rename(columns={0: 'block_no'}, inplace=True)
IndependenceDay_bias = pd.concat([IndependenceDay_bias,block_no], axis=1)
IndependenceDay_bias['event_name'] = 'IndependenceDay'


Jamat_ul_Vida_poly_coef = np.polyfit(error_pivot_fest['block_no'],
                                 error_pivot_fest['endo_residual_weekend_correctionJamat_ul_Vida'], 4)
Jamat_ul_Vida_poly = np.poly1d(Jamat_ul_Vida_poly_coef)
Jamat_ul_Vida_bias = Jamat_ul_Vida_poly(error_pivot_fest['block_no'])
Jamat_ul_Vida_bias = pd.DataFrame(Jamat_ul_Vida_bias)
Jamat_ul_Vida_bias.rename(columns={0: 'fest_correction'}, inplace=True)
block_no = range(1, 97)
block_no = pd.DataFrame(block_no)
block_no.rename(columns={0: 'block_no'}, inplace=True)
Jamat_ul_Vida_bias = pd.concat([Jamat_ul_Vida_bias,block_no], axis=1)
Jamat_ul_Vida_bias['event_name'] = 'Jamat_ul_Vida'

Janamashtami_poly_coef = np.polyfit(error_pivot_fest['block_no'],
                                 error_pivot_fest['endo_residual_weekend_correctionJanamashtami'], 4)
Janamashtami_poly = np.poly1d(Janamashtami_poly_coef)
Janamashtami_bias = Janamashtami_poly(error_pivot_fest['block_no'])
Janamashtami_bias = pd.DataFrame(Janamashtami_bias)
Janamashtami_bias.rename(columns={0: 'fest_correction'}, inplace=True)
block_no = range(1, 97)
block_no = pd.DataFrame(block_no)
block_no.rename(columns={0: 'block_no'}, inplace=True)
Janamashtami_bias = pd.concat([Janamashtami_bias,block_no], axis=1)
Janamashtami_bias['event_name'] = 'Janamashtami'


MahaAshtami_poly_coef = np.polyfit(error_pivot_fest['block_no'],
                                 error_pivot_fest['endo_residual_weekend_correctionMahaAshtami'], 7)
MahaAshtami_poly = np.poly1d(MahaAshtami_poly_coef)
MahaAshtami_bias = MahaAshtami_poly(error_pivot_fest['block_no'])
MahaAshtami_bias = pd.DataFrame(MahaAshtami_bias)
MahaAshtami_bias.rename(columns={0: 'fest_correction'}, inplace=True)
block_no = range(1, 97)
block_no = pd.DataFrame(block_no)
block_no.rename(columns={0: 'block_no'}, inplace=True)
MahaAshtami_bias = pd.concat([MahaAshtami_bias,block_no], axis=1)
MahaAshtami_bias['event_name'] = 'MahaAshtami'


# MahaNavami_poly_coef = np.polyfit(error_pivot_fest['block_no'],
#                                  error_pivot_fest['endo_residual_weekend_correctionMahaNavami'], 8)
# MahaNavami_poly = np.poly1d(MahaNavami_poly_coef)
# MahaNavami_bias = MahaNavami_poly(error_pivot_fest['block_no'])
# MahaNavami_bias = pd.DataFrame(MahaNavami_bias)
# MahaNavami_bias.rename(columns={0: 'fest_correction'}, inplace=True)
# block_no = range(1, 97)
# block_no = pd.DataFrame(block_no)
# block_no.rename(columns={0: 'block_no'}, inplace=True)
# MahaNavami_bias = pd.concat([MahaNavami_bias,block_no], axis=1)
# MahaNavami_bias['event_name'] = 'MahaNavami'



MahaSaptami_poly_coef = np.polyfit(error_pivot_fest['block_no'],
                                 error_pivot_fest['endo_residual_weekend_correctionMahaSaptami'], 7)
MahaSaptami_poly = np.poly1d(MahaSaptami_poly_coef)
MahaSaptami_bias = MahaSaptami_poly(error_pivot_fest['block_no'])
MahaSaptami_bias = pd.DataFrame(MahaSaptami_bias)
MahaSaptami_bias.rename(columns={0: 'fest_correction'}, inplace=True)
block_no = range(1, 97)
block_no = pd.DataFrame(block_no)
block_no.rename(columns={0: 'block_no'}, inplace=True)
MahaSaptami_bias = pd.concat([MahaSaptami_bias,block_no], axis=1)
MahaSaptami_bias['event_name'] = 'MahaSaptami'



MahatmaGandhisBday_poly_coef = np.polyfit(error_pivot_fest['block_no'],
                                 error_pivot_fest['endo_residual_weekend_correctionMahatmaGandhisBday'], 4)
MahatmaGandhisBday_poly = np.poly1d(MahatmaGandhisBday_poly_coef)
MahatmaGandhisBday_bias = MahatmaGandhisBday_poly(error_pivot_fest['block_no'])
MahatmaGandhisBday_bias = pd.DataFrame(MahatmaGandhisBday_bias)
MahatmaGandhisBday_bias.rename(columns={0: 'fest_correction'}, inplace=True)
block_no = range(1, 97)
block_no = pd.DataFrame(block_no)
block_no.rename(columns={0: 'block_no'}, inplace=True)
MahatmaGandhisBday_bias = pd.concat([MahatmaGandhisBday_bias,block_no], axis=1)
MahatmaGandhisBday_bias['event_name'] = 'MahatmaGandhisBday'


# MakarSankranti_poly_coef = np.polyfit(error_pivot_fest['block_no'],
#                                  error_pivot_fest['endo_residual_weekend_correctionMakarSankranti'], 3)
# MakarSankranti_poly = np.poly1d(MakarSankranti_poly_coef)
# MakarSankranti_bias = MakarSankranti_poly(error_pivot_fest['block_no'])
# MakarSankranti_bias = pd.DataFrame(MakarSankranti_bias)
# MakarSankranti_bias.rename(columns={0: 'fest_correction'}, inplace=True)
# block_no = range(1, 97)
# block_no = pd.DataFrame(block_no)
# block_no.rename(columns={0: 'block_no'}, inplace=True)
# MakarSankranti_bias = pd.concat([MakarSankranti_bias,block_no], axis=1)
# MakarSankranti_bias['event_name'] = 'MakarSankranti'


RakshaBandhan_poly_coef = np.polyfit(error_pivot_fest['block_no'],
                                 error_pivot_fest['endo_residual_weekend_correctionRakshaBandhan'], 4)
RakshaBandhan_poly = np.poly1d(RakshaBandhan_poly_coef)
RakshaBandhan_bias = RakshaBandhan_poly(error_pivot_fest['block_no'])
RakshaBandhan_bias = pd.DataFrame(RakshaBandhan_bias)
RakshaBandhan_bias.rename(columns={0: 'fest_correction'}, inplace=True)
block_no = range(1, 97)
block_no = pd.DataFrame(block_no)
block_no.rename(columns={0: 'block_no'}, inplace=True)
RakshaBandhan_bias = pd.concat([RakshaBandhan_bias,block_no], axis=1)
RakshaBandhan_bias['event_name'] = 'RakshaBandhan'

RepublicDay_poly_coef = np.polyfit(error_pivot_fest['block_no'],
                                 error_pivot_fest['endo_residual_weekend_correctionRepublicDay'], 7)
RepublicDay_poly = np.poly1d(RepublicDay_poly_coef)
RepublicDay_bias = RepublicDay_poly(error_pivot_fest['block_no'])
RepublicDay_bias = pd.DataFrame(RepublicDay_bias)
RepublicDay_bias.rename(columns={0: 'fest_correction'}, inplace=True)
block_no = range(1, 97)
block_no = pd.DataFrame(block_no)
block_no.rename(columns={0: 'block_no'}, inplace=True)
RepublicDay_bias = pd.concat([RepublicDay_bias,block_no], axis=1)
RepublicDay_bias['event_name'] = 'RepublicDay'

# Vaishakhi_poly_coef = np.polyfit(error_pivot_fest['block_no'],
#                                  error_pivot_fest['endo_residual_weekend_correctionVaishakhi'], 2)
# Vaishakhi_poly = np.poly1d(Vaishakhi_poly_coef)
# Vaishakhi_bias = Vaishakhi_poly(error_pivot_fest['block_no'])
# Vaishakhi_bias = pd.DataFrame(Vaishakhi_bias)
# Vaishakhi_bias.rename(columns={0: 'fest_correction'}, inplace=True)
# block_no = range(1, 97)
# block_no = pd.DataFrame(block_no)
# block_no.rename(columns={0: 'block_no'}, inplace=True)
# Vaishakhi_bias = pd.concat([Vaishakhi_bias,block_no], axis=1)
# Vaishakhi_bias['event_name'] = 'Vaishakhi'



# In[249]:

event_Bias = pd.concat([
BhaiDooj_bias,
Diwali1_bias,
Diwali2_bias,
DrBRAmbedkarsBday_bias,
Dusherra_bias,
Election_bias,
# GovardhanPuja_bias,
GuruNanakBday_bias,
GuruTegBahadurDday_bias,
Holi_bias,
HoliKaDahan_bias,
Id_Ul_Fitr_bias,
Id_Ul_Juha_Bakrid_bias,
IndependenceDay_bias,
Jamat_ul_Vida_bias,
Janamashtami_bias,
MahaAshtami_bias,
# MahaNavami_bias,
MahaSaptami_bias,
MahatmaGandhisBday_bias,
# MakarSankranti_bias,
RakshaBandhan_bias,
RepublicDay_bias
# Vaishakhi_bias
    ], axis =0)


# In[251]:

data_forecast = pd.merge(data_forecast, holiday_event_master, how = 'left', on = ['date'] )


# In[252]:

data_forecast = pd.merge(data_forecast, event_Bias, how = 'left', left_on = ['name','block_no'] ,
                                                                  right_on = ['event_name','block_no'] )


# In[253]:

data_forecast['fest_correction'].fillna(0, inplace=True)


# In[27]:

data_forecast['endo_sim_day_pred_demand_final_fest'] = data_forecast['endo_pred_sim_day_load_final'] + \
    data_forecast['fest_correction']


# In[28]:

data_forecast['residual_post_weekend_event'] = data_forecast['endo_demand'] - \
    data_forecast['endo_sim_day_pred_demand_final_fest']


# In[29]:


peak_off_peak_bias = pd.pivot_table(data_forecast,
                                    values=['residual_post_weekend_event'],
                                    index=['block_no'], columns=['month'],
                                    aggfunc=np.mean).reset_index()
peak_off_peak_bias.columns = [s1 + str(s2) for (s1, s2) in \
                              peak_off_peak_bias.columns.tolist()]


# In[31]:


import numpy
# from matplotlib.pyplot import *
month1_poly_coef = numpy.polyfit(peak_off_peak_bias['block_no'],
                                 peak_off_peak_bias['residual_post_weekend_event1'], 15)
month1_poly = numpy.poly1d(month1_poly_coef)
month1_bias = month1_poly(peak_off_peak_bias['block_no'])
month1_bias = pd.DataFrame(month1_bias)
month1_bias.rename(columns={0: 'month_shape_Bias'}, inplace=True)
block_no = range(1, 97)
block_no = pd.DataFrame(block_no)
block_no.rename(columns={0: 'block_no'}, inplace=True)
month1_bias_correction = pd.concat([month1_bias,block_no], axis=1)
month1_bias_correction['month'] = 1


# In[32]:

# import numpy
# from matplotlib.pyplot import *
month2_poly_coef = numpy.polyfit(peak_off_peak_bias['block_no'],
                                 peak_off_peak_bias['residual_post_weekend_event2'], 15)
month2_poly = numpy.poly1d(month2_poly_coef)
month2_bias = month2_poly(peak_off_peak_bias['block_no'])
month2_bias = pd.DataFrame(month2_bias)
month2_bias.rename(columns={0: 'month_shape_Bias'}, inplace=True)
block_no = range(1, 97)
block_no = pd.DataFrame(block_no)
block_no.rename(columns={0: 'block_no'}, inplace=True)
month2_bias_correction = pd.concat([month2_bias,block_no], axis=1)
month2_bias_correction['month'] = 2


# In[33]:

# import numpy
# from matplotlib.pyplot import *
month3_poly_coef = numpy.polyfit(peak_off_peak_bias['block_no'],
                                 peak_off_peak_bias['residual_post_weekend_event3'], 15)
month3_poly = numpy.poly1d(month3_poly_coef)
month3_bias = month3_poly(peak_off_peak_bias['block_no'])
month3_bias = pd.DataFrame(month3_bias)
month3_bias.rename(columns={0: 'month_shape_Bias'}, inplace=True)
block_no = range(1, 97)
block_no = pd.DataFrame(block_no)
block_no.rename(columns={0: 'block_no'}, inplace=True)
month3_bias_correction = pd.concat([month3_bias,block_no], axis=1)
month3_bias_correction['month'] = 3


# In[34]:

# import numpy
# from matplotlib.pyplot import *
month4_poly_coef = numpy.polyfit(peak_off_peak_bias['block_no'],
                                 peak_off_peak_bias['residual_post_weekend_event4'], 15)
month4_poly = numpy.poly1d(month4_poly_coef)
month4_bias = month4_poly(peak_off_peak_bias['block_no'])


month4_bias = pd.DataFrame(month4_bias)
month4_bias.rename(columns={0: 'month_shape_Bias'}, inplace=True)
block_no = range(1, 97)
block_no = pd.DataFrame(block_no)
block_no.rename(columns={0: 'block_no'}, inplace=True)
month4_bias_correction = pd.concat([month4_bias,block_no], axis=1)
month4_bias_correction['month'] = 4


# In[35]:

# import numpy
# from matplotlib.pyplot import *
month5_poly_coef = numpy.polyfit(peak_off_peak_bias['block_no'],
                                 peak_off_peak_bias['residual_post_weekend_event5'], 15)
month5_poly = numpy.poly1d(month5_poly_coef)
month5_bias = month5_poly(peak_off_peak_bias['block_no'])

month5_bias = pd.DataFrame(month5_bias)
month5_bias.rename(columns={0: 'month_shape_Bias'}, inplace=True)
block_no = range(1, 97)
block_no = pd.DataFrame(block_no)
block_no.rename(columns={0: 'block_no'}, inplace=True)
month5_bias_correction = pd.concat([month5_bias,block_no], axis=1)
month5_bias_correction['month'] = 5


# In[36]:

# import numpy
# from matplotlib.pyplot import *
month6_poly_coef = numpy.polyfit(peak_off_peak_bias['block_no'],
                                 peak_off_peak_bias['residual_post_weekend_event6'], 15)
month6_poly = numpy.poly1d(month6_poly_coef)
month6_bias = month6_poly(peak_off_peak_bias['block_no'])

month6_bias = pd.DataFrame(month6_bias)
month6_bias.rename(columns={0: 'month_shape_Bias'}, inplace=True)
block_no = range(1, 97)
block_no = pd.DataFrame(block_no)
block_no.rename(columns={0: 'block_no'}, inplace=True)
month6_bias_correction = pd.concat([month6_bias,block_no], axis=1)
month6_bias_correction['month'] =6


# In[37]:

# import numpy
# from matplotlib.pyplot import *
month7_poly_coef = numpy.polyfit(peak_off_peak_bias['block_no'],
                                 peak_off_peak_bias['residual_post_weekend_event7'], 15)
month7_poly = numpy.poly1d(month7_poly_coef)
month7_bias = month7_poly(peak_off_peak_bias['block_no'])

month7_bias = pd.DataFrame(month7_bias)
month7_bias.rename(columns={0: 'month_shape_Bias'}, inplace=True)
block_no = range(1, 97)
block_no = pd.DataFrame(block_no)
block_no.rename(columns={0: 'block_no'}, inplace=True)
month7_bias_correction = pd.concat([month7_bias,block_no], axis=1)
month7_bias_correction['month'] = 7


# In[38]:

# import numpy
# from matplotlib.pyplot import *
month8_poly_coef = numpy.polyfit(peak_off_peak_bias['block_no'],
                                 peak_off_peak_bias['residual_post_weekend_event8'], 15)
month8_poly = numpy.poly1d(month8_poly_coef)
month8_bias = month8_poly(peak_off_peak_bias['block_no'])

month8_bias = pd.DataFrame(month8_bias)
month8_bias.rename(columns={0: 'month_shape_Bias'}, inplace=True)
block_no = range(1, 97)
block_no = pd.DataFrame(block_no)
block_no.rename(columns={0: 'block_no'}, inplace=True)
month8_bias_correction = pd.concat([month8_bias,block_no], axis=1)
month8_bias_correction['month'] = 8


# In[39]:

# import numpy
# from matplotlib.pyplot import *
month9_poly_coef = numpy.polyfit(peak_off_peak_bias['block_no'],
                                 peak_off_peak_bias['residual_post_weekend_event9'], 15)
month9_poly = numpy.poly1d(month9_poly_coef)
month9_bias = month9_poly(peak_off_peak_bias['block_no'])

month9_bias = pd.DataFrame(month9_bias)
month9_bias.rename(columns={0: 'month_shape_Bias'}, inplace=True)
block_no = range(1, 97)
block_no = pd.DataFrame(block_no)
block_no.rename(columns={0: 'block_no'}, inplace=True)
month9_bias_correction = pd.concat([month9_bias,block_no], axis=1)
month9_bias_correction['month'] = 9


# In[40]:

# import numpy
# from matplotlib.pyplot import *
month10_poly_coef = numpy.polyfit(peak_off_peak_bias['block_no'],
                                 peak_off_peak_bias['residual_post_weekend_event10'], 15)
month10_poly = numpy.poly1d(month10_poly_coef)
month10_bias = month10_poly(peak_off_peak_bias['block_no'])

month10_bias = pd.DataFrame(month10_bias)
month10_bias.rename(columns={0: 'month_shape_Bias'}, inplace=True)
block_no = range(1, 97)
block_no = pd.DataFrame(block_no)
block_no.rename(columns={0: 'block_no'}, inplace=True)
month10_bias_correction = pd.concat([month10_bias,block_no], axis=1)
month10_bias_correction['month'] = 10


# In[41]:

# import numpy
# from matplotlib.pyplot import *
month11_poly_coef = numpy.polyfit(peak_off_peak_bias['block_no'],
                                 peak_off_peak_bias['residual_post_weekend_event11'], 15)
month11_poly = numpy.poly1d(month11_poly_coef)
month11_bias = month11_poly(peak_off_peak_bias['block_no'])

month11_bias = pd.DataFrame(month11_bias)
month11_bias.rename(columns={0: 'month_shape_Bias'}, inplace=True)
block_no = range(1, 97)
block_no = pd.DataFrame(block_no)
block_no.rename(columns={0: 'block_no'}, inplace=True)
month11_bias_correction = pd.concat([month11_bias,block_no], axis=1)
month11_bias_correction['month'] = 11


# In[42]:

# import numpy
# from matplotlib.pyplot import *
month12_poly_coef = numpy.polyfit(peak_off_peak_bias['block_no'],
                                 peak_off_peak_bias['residual_post_weekend_event12'], 15)
month12_poly = numpy.poly1d(month12_poly_coef)
month12_bias = month12_poly(peak_off_peak_bias['block_no'])

month12_bias = pd.DataFrame(month12_bias)
month12_bias.rename(columns={0: 'month_shape_Bias'}, inplace=True)
block_no = range(1, 97)
block_no = pd.DataFrame(block_no)
block_no.rename(columns={0: 'block_no'}, inplace=True)
month12_bias_correction = pd.concat([month12_bias,block_no], axis=1)
month12_bias_correction['month'] = 12


# In[43]:

month_shape_Bias = pd.concat([month1_bias_correction, month2_bias_correction,
                              month3_bias_correction, month4_bias_correction,
                              month5_bias_correction, month6_bias_correction,
                              month7_bias_correction, month8_bias_correction,
                              month9_bias_correction, month10_bias_correction,
                              month11_bias_correction,
                              month12_bias_correction], axis=0)


# In[44]:

data_forecast = pd.merge(data_forecast, month_shape_Bias, how='left',
                         left_on=['month', 'block_no'],
                         right_on=['month', 'block_no'])
data_forecast['month_shape_Bias'].fillna(0, inplace=True)


# In[45]:

data_forecast['deterministic_demand_pred_shape'] = \
    data_forecast['endo_sim_day_pred_demand_final_fest'] + \
    data_forecast['month_shape_Bias']


# In[46]:

test = data_forecast[['date', 'block_no', 'endo_demand',
                     'deterministic_demand_pred_shape']]
test['mape'] = abs(test['endo_demand'] -
                   test['deterministic_demand_pred_shape'])/test['endo_demand']


# In[47]:

pred_table_similarday = data_forecast[['date', 'block_no', 'deterministic_demand_pred_shape']]
pred_table_similarday.rename(columns={'deterministic_demand_pred_shape': 'demand_forecast'}, inplace=True)
pred_table_similarday['discom_name'] = 'UPCL'
pred_table_similarday['state'] = 'UTTARAKHAND'
pred_table_similarday['revision'] = 0
pred_table_similarday['model_name'] = 'NEAREST_NEIGHBOUR'


# from pandas.io import sql
# # import MySQLdb
# # UPCL_DB = MySQLdb.connect(host="localhost",    # your host, usually localhost
# #                      user="root",         # your username
# #                      passwd="power@2012",  # your password
# #                      db="power")        # name of the data base

# UPCL_DB = dbconn.connect(dsnfile)
# cur = UPCL_DB.cursor()
# pred_table_similarday.to_sql(con=UPCL_DB, name='forecast_stg',
#                              if_exists='append', flavor='mysql',
#                              index=False)
# UPCL_DB.commit()
# cur.close()



# In[137]:


from pandas.io import sql
# import MySQLdb
# UPCL_DB = MySQLdb.connect(host="localhost",    # your host, usually localhost
#                      user="root",         # your username
#                      passwd="power@2012",  # your password
#                      db="power")        # name of the data base
# UPCL_DB = dbconn.connect(dsnfile)
# cur = UPCL_DB.cursor()
# data_forecast.to_sql(con=UPCL_DB, name='data_forecast_UPCL',
#                      if_exists='replace', flavor='mysql')
# UPCL_DB.commit()
# cur.close()


# In[47]:

from pandas.io import sql
# import MySQLdb
# UPCL_DB = MySQLdb.connect(host="localhost",    # your host, usually localhost
#                      user="root",         # your username
#                      passwd="power@2012",  # your password
#                      db="power")        # name of the data base
UPCL_DB = dbconn.connect(dsnfile)
cur = UPCL_DB.cursor()
pred_table_similarday.to_sql(con=UPCL_DB, name='upcl_deterministic_forecast',
                             if_exists='replace', flavor='mysql', index=False)
UPCL_DB.commit()
cur.close()
# UPCL_DB.close()

UPCL_DB = dbconn.connect(dsnfile)
cur = UPCL_DB.cursor()
sql_str = """insert into power.forecast_stg
        (date, state, revision, discom_name, block_no,
         model_name, demand_forecast)
        (select date, state, revision, discom_name, block_no,
         model_name, demand_forecast from
         power.upcl_deterministic_forecast)
        on duplicate key
        update demand_forecast = values(demand_forecast),
               load_date = NULL"""
cur.execute(sql_str)
UPCL_DB.commit()
cur.close()
# UPCL_DB.close()


# coding: utf-8

# In[1]:

import pandas as pd

from sqlalchemy import create_engine

# In[6]:

path_with_fname = '/Users/biswadippaul/Downloads/Upcl_Holiday_SAS_input.xlsx'
# path_with_fname = '/Users/biswadippaul/Downloads/Gujarat_Holiday.xlsx'
excel = pd.read_excel(path_with_fname)
engine = create_engine('mysql+pymysql://root:power@2020@localhost/power', echo=False)
excel.to_sql(con=engine, name='holiday_tmp', if_exists='replace')

##Database side

# SELECT * 
# FROM power.holiday_upcl_tmp 
# where date >= '2017-01-01'
# and mapping not in (select distinct name from power.holiday_event_master where discom = 'UPCL')

# select * from power.holiday_event_master
# where discom = 'UPCL'
# and name not in (select distinct mapping FROM power.holiday_upcl_tmp 
# where date >= '2017-01-01')

# select distinct name FROM power.holiday_event_master where discom = 'UPCL' 

# update power.holiday_upcl_tmp 
# set mapping = 'Diwali1' 
# where mapping = 'Diwali' and index = 216

# drop table power.holiday_upcl_tmp

# insert into power.holiday_event_master

# INSERT INTO `power`.`holiday_event_master`
# (
# `state`,
# `discom`,
# `date`,
# `name`,
# `description`,
# `event1`,
# `event2`,
# `valid_from_date`,
# `valid_to_date`)
# select 'UTTARAKHAND' state, 'UPCL' discom,
# date, mapping, holiday, coalesce(event1,0), coalesce(event2,0), 
# '2017-01-01' valid_from_date, '2150-01-01' valid_to_date
# from  power.holiday_upcl_tmp
# where date >= '2017-01-01'
# and mapping not in ('NewYear', 'MakarSankranti')


# SELECT * 
# FROM power.holiday_tmp 
# where date >= '2018-01-01'
# and mapping not in (select distinct name from power.holiday_event_master where discom = 'UPCL')

# select * from power.holiday_event_master
# where discom = 'UPCL'
# and name not in (select distinct mapping FROM power.holiday_tmp 
# where date >= '2018-01-01')

# select distinct name FROM power.holiday_event_master where discom = 'UPCL' 

# update power.holiday_tmp 
# set mapping = 'Diwali1' 
# where mapping = 'Diwali' and `index` = 264

# update power.holiday_tmp 
# set mapping = 'Diwali2' 
# where mapping = 'Diwali' and `index` = 265

# update power.holiday_tmp 
# set mapping = 'Election' 
# where mapping = 'Vote' and `index` = 233

# drop table power.holiday_upcl_tmp


# INSERT INTO `power`.`holiday_event_master`
# (
# `state`,
# `discom`,
# `date`,
# `name`,
# `description`,
# `event1`,
# `event2`,
# `valid_from_date`,
# `valid_to_date`)
# select 'UTTARAKHAND' state, 'UPCL' discom,
# date, mapping, holiday, coalesce(event1,0), coalesce(event2,0), 
# '2018-01-01' valid_from_date, '2150-01-01' valid_to_date
# from  power.holiday_tmp
# where date >= '2018-01-01'
# and mapping not in ('NewYear', 'MakarSankranti')


# select * from `power`.`holiday_event_master`
# where discom = 'UPCL'


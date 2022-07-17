# import pandas as pd
# import sql_load_lib
# import pymysql
# import dbconn
# from datetime import date, time, timedelta, datetime
# import calendar
import os
from sqlalchemy import create_engine
import pandas as pd
import numpy as np

def dir_file_walk(path):
	return sorted([os.path.join(root, name)
         for root, dirs, files in os.walk(path)
         for name in files
         if name.endswith(".csv")])

path = '/Users/biswadippaul/Downloads/Hourly Data from 201805 to current'

files_lst = dir_file_walk(path)

for i, file in enumerate(files_lst):
    if i == 0:
        df = pd.read_csv(file)
    else:
        df = df.append(pd.read_csv(file))

df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%Y', errors='coerce')
df['Date'] = df['Date'].dt.date

print(len(df['Date'].unique()) * 24, df['Date'].max(), df['Date'].min(), len(df), 
    len(df['Date'].unique()) * 96)

#check for missing date
pd.date_range(start = df['Date'].max().strftime('%Y-%m-%d') , end = df['Date'].max().strftime('%Y-%m-%d') ).difference(df['Date'])

df['Total'] = df['Demand'] + df['Powercut']

# checkif UnrestricatedDemand tallys
df[(df['UnrestricatedDemand']!=df['Total'])]

#check for outliars
df[['Date','Hour','Total']].groupby(['Hour']).agg(['min', 'max', 'mean'])

config = 'mysql+pymysql://root:power@2020@localhost/power'
engine = create_engine(config, echo=False)
df_block_master = pd.read_sql("select block_no, block_hour_no as Hour "
                              "from block_master where delete_ind = 0",engine)

df = df.merge(df_block_master, on='Hour')

df = df.sort_values(by=['Date', 'Hour', 'block_no'])

df['state'] = 'UTTARAKHAND'

df = df.where(pd.notnull(df), None)

df_demand = df[['Date', 'block_no', 'Discom', 'Frequency', 'UIRate', 'InternalGeneration', 'Schedule', 'Demand', 'state']]

df_powercut = df[['Date', 'block_no', 'Discom', 'Powercut', 'state']]

sql = """insert into drawl_staging
       (date, block_no, discom, frequency,
        ui_rate, internal_generation,
        schedule, constrained_load, state)
        values (%s, %s, %s, %s, %s, %s,
                %s, %s, %s)
        on duplicate key update
        frequency = values(frequency),
        ui_rate = values(ui_rate),
        internal_generation = values(internal_generation),
        schedule = values(schedule),
        constrained_load = values(constrained_load),
        processed_ind = 0"""

connection = engine.connect()
connection.execute(sql, df_demand.values.tolist())

sql = """insert into powercut_staging
               (date, 
                block_no, discom, powercut, state)
                values (%s, %s, %s,
                        %s, %s)
                on duplicate key update
                powercut = values(powercut),
                processed_ind = 0"""


connection = engine.connect()
connection.execute(sql, df_demand.values.tolist())

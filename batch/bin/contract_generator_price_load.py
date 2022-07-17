from sqlalchemy import create_engine
import pandas as pd
import numpy as np

df  = pd.read_excel('/Users/biswadippaul/Downloads/generator_dtls_upcl (1).xlsx', header=1)
df = df.where(pd.notnull(df), None)
df['added_by_fk'] = 3
df['modified_by_fk'] = 3

df_unit_master = df[['station_name', 'rated_unit_capacity', 'min_generation_level', 
    'max_generation_level', 'fuel', 'org_name', 'added_by_fk', 'modified_by_fk']]

df_commodity = df[['fuel']]

df_con_trade_master = df[['station_name','contracted_capacity', 'surrender_vol_perc', 'discom', 'added_by_fk', 'modified_by_fk']]

df_cntprty = df[['station_name', 'added_by_fk', 'modified_by_fk']]

df_cst_brkdwn = df[['station_name','fixed_cost', 'variable_cost', 'valid_from_dt', 'valid_to_dt', 'discom','added_by_fk', 'modified_by_fk']]

df_unit_master[['station_name', 'rated_unit_capacity','min_generation_level', 
                                        'max_generation_level','added_by_fk', 'modified_by_fk']]

#TODO add unit_type
sql = """insert into unit_master (unit_name, rated_unit_capacity, min_generation_level, 
        max_generation_level, added_by_fk, modified_by_fk )
        values (%s, %s, %s, %s, %s, %s)               
        on duplicate key update
        rated_unit_capacity = values(rated_unit_capacity),
        min_generation_level = values(min_generation_level),
        max_generation_level = values(min_generation_level)"""
    
connection = engine.connect()
connection.execute(sql, df_unit_master[['station_name', 'rated_unit_capacity','min_generation_level', 
                                        'max_generation_level','added_by_fk', 'modified_by_fk']].values.tolist())

df_um_db = pd.read_sql("""select unit_master_pk, unit_name as station_name from unit_master""", engine)

df_unit_lines = df_unit_lines.merge(df_um_db, how='left')

sql = """insert into unit_master_lines (unit_master_fk, ramp_up_rate,ramp_down_rate,from_capacity_perc,to_capacity_perc,added_by_fk, modified_by_fk)
        values (%s, %s, %s, %s, %s,%s,%s)               
        on duplicate key update
        ramp_up_rate = values(ramp_up_rate),
        ramp_down_rate = values(ramp_down_rate),
        from_capacity_perc = values(from_capacity_perc),
        to_capacity_perc = values(to_capacity_perc)
        """

connection = engine.connect()
connection.execute(sql, df_unit_lines[['unit_master_pk', 'ramp_up_rate','ramp_down_rate', 
                                        'from_capacity_perc', 'to_capacity_perc','added_by_fk', 'modified_by_fk']].values.tolist())

df_cntprty_typ = pd.read_sql("""select * from counter_party_type""", engine)

df_cntprty_typ[(df_cntprty_typ['Counter_Party_Type_Name'] == 'GENERATOR')]['Counter_Party_Type_PK']

df_cntprty['Counter_Party_Type_FK'] = df_cntprty_typ[(df_cntprty_typ['Counter_Party_Type_Name'] == 'GENERATOR')]['Counter_Party_Type_PK'].values.tolist()[0]

df_cntprty2= df[['discom', 'added_by_fk', 'modified_by_fk']].drop_duplicates()
df_cntprty_typ[(df_cntprty_typ['Counter_Party_Type_Name'] == 'DISCOM')]['Counter_Party_Type_PK']
df_cntprty2['Counter_Party_Type_FK'] = df_cntprty_typ[(df_cntprty_typ['Counter_Party_Type_Name'] == 'DISCOM')]['Counter_Party_Type_PK'].values.tolist()[0]
df_cntprty2 = df_cntprty2.rename(columns={"discom": "station_name"})

df_cntprty = df_cntprty.append(df_cntprty2)

sql = """insert into counter_party_master(counter_party_type_fk, counter_party_name, added_by_fk, modified_by_fk)
        values (%s, %s, %s, %s)"""

connection = engine.connect()
connection.execute(sql, df_cntprty[['Counter_Party_Type_FK', 'station_name','added_by_fk', 'modified_by_fk']].values.tolist())

df_cnprty_db = pd.read_sql("""select counter_party_master_pk, counter_party_type_fk, counter_party_name from counter_party_master""", engine)
df_con_trade_master = df_con_trade_master.merge(df_cnprty_db, how='left', right_on='counter_party_name', left_on='discom')
df_con_trade_master = df_con_trade_master.drop(columns=['counter_party_type_fk','counter_party_name','discom'])
df_con_trade_master = df_con_trade_master.rename(columns={"counter_party_master_pk": "counter_party_1_fk"})

df_con_trade_master = df_con_trade_master.merge(df_cnprty_db, how='left', right_on='counter_party_name', left_on='station_name')

df_con_trade_master = df_con_trade_master.drop(columns=['counter_party_type_fk','counter_party_name','station_name'])
df_con_trade_master = df_con_trade_master.rename(columns={"counter_party_master_pk": "counter_party_2_fk"})

df_con_trade_master['buy_ind'] = 1

df_con_trade_master.insert(0, 'contract_name_t', range(0, 0 + len(df_con_trade_master)))

df_con_trade_master['contract_name'] = 'UPCL' + df_con_trade_master['contract_name_t'].astype(str) 

df_con_trade_master['contract_code'] = 'UK' + df_con_trade_master['contract_name_t'].astype(str) 

sql = """insert into contract_trade_master(contract_code,
        contract_name, contracted_capacity,
        surrender_vol_perc, buy_ind,counter_party_1_fk, counter_party_2_fk, added_by_fk, modified_by_fk)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""

connection = engine.connect()
connection.execute(sql, df_con_trade_master[['contract_code',
        'contract_name', 'contracted_capacity',
        'surrender_vol_perc', 'buy_ind','counter_party_1_fk', 
        'counter_party_2_fk','added_by_fk', 'modified_by_fk']].values.tolist())

df_cost_type = pd.read_sql("""select contract_cost_type_pk, cost_type_name from contract_cost_type""", engine)

df_contract_db = pd.read_sql("""SELECT a.contract_trade_master_pk, a.contract_code,
        a.contract_name, a.contracted_capacity,
        a.surrender_vol_perc, a.buy_ind,
        c.counter_party_name cp1, d.counter_party_name cp2
        from power.contract_trade_master a,
             power.counter_party_master c,
             power.counter_party_master d
        where a.counter_party_1_fk = c.counter_party_master_pk
        and   a.counter_party_2_fk = d.counter_party_master_pk
        and c.counter_party_name = 'UPCL'
        and a.delete_ind = 0
        and c.delete_ind = 0
        and d.delete_ind = 0""", engine)

df_cst_brkdwn = df_cst_brkdwn.merge(df_contract_db, how='left', left_on=['station_name','discom'], right_on=['cp2','cp1'])

df_cst_brkdwn  =df_cst_brkdwn.drop(columns=['station_name', 'discom','contract_code','contract_name','contracted_capacity','surrender_vol_perc','buy_ind'])

df_cst_brkdwn = df_cst_brkdwn.rename(columns={"contract_trade_master_pk": "contract_trade_master_fk"})

df_cst_brkdwn_var = df_cst_brkdwn[['variable_cost','valid_from_dt','valid_to_dt','added_by_fk','modified_by_fk',
                                   'contract_trade_master_fk','cp1','cp2']]

df_cst_brkdwn_fixed = df_cst_brkdwn[['fixed_cost','valid_from_dt','valid_to_dt','added_by_fk','modified_by_fk',
                                   'contract_trade_master_fk','cp1','cp2']]

df_cst_brkdwn_var['contract_cost_type_fk'] = df_cost_type[(df_cost_type['cost_type_name'] == 'VARIABLE')]['contract_cost_type_pk'].values.tolist()[0]

df_cst_brkdwn_fixed['contract_cost_type_fk'] = df_cost_type[(df_cost_type['cost_type_name'] == 'FIXED')]['contract_cost_type_pk'].values.tolist()[0]

today = pd.to_datetime("today").strftime('%Y-%m-%d')


df_valid_cst_brkdwn = pd.read_sql("""select contract_trade_master_fk, contract_cost_type_fk,cost,valid_from_dt, valid_to_dt 
                                     from contract_cost_breakdown
                                     where valid_from_dt <= '{0}'
                                     and valid_to_dt >= '{0}'
                                     and delete_ind = 0""".format(today), engine)
max_date = '2030-01-01 00:00:00'

df_cst_brkdwn_var_updt = df_cst_brkdwn_var.merge(df_valid_cst_brkdwn, on=['contract_trade_master_fk', 'contract_cost_type_fk'])

df_cst_brkdwn_fixed_updt = df_cst_brkdwn_fixed.merge(df_valid_cst_brkdwn, on=['contract_trade_master_fk', 'contract_cost_type_fk'])

#TODO update the contract_cost_breakdown valid_to_dt to todays date for the contracts active

sql= """insert into contract_cost_breakdown
        (contract_trade_master_fk, contract_cost_type_fk,cost,valid_from_dt, valid_to_dt,added_by_fk,modified_by_fk)
        values(%s, %s, %s, %s,%s,%s,%s)"""

df_cst_brkdwn_var['valid_to_dt'] = max_date

df_cst_brkdwn_fixed['valid_to_dt'] = max_date
df_cst_brkdwn_fixed['valid_from_dt'] = df_cst_brkdwn_fixed['valid_from_dt'].dt.strftime('%Y-%m-%d %H:%M:%S')
connection = engine.connect()
connection.execute(sql, df_cst_brkdwn_fixed[['contract_trade_master_fk',
        'contract_cost_type_fk', 'fixed_cost',
        'valid_from_dt', 'valid_to_dt','added_by_fk', 'modified_by_fk']].values.tolist())

df_cst_brkdwn_var['valid_from_dt'] = df_cst_brkdwn_var['valid_from_dt'].dt.strftime('%Y-%m-%d %H:%M:%S')
connection = engine.connect()
connection.execute(sql, df_cst_brkdwn_var[['contract_trade_master_fk',
        'contract_cost_type_fk', 'variable_cost',
        'valid_from_dt', 'valid_to_dt','added_by_fk', 'modified_by_fk']].values.tolist())

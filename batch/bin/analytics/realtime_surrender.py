"""Realtime Allocation Optimisation."""
from __future__ import division
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
# from numpy import array
# from numpy import sign
# from numpy import zeros
# import datetime as dt
pd.options.mode.chained_assignment = None


def realtime_surrender(config, date, window,
                       discom, state):
    """Realtime Allocation Optimisation."""
    date_str = date
    valid_date = pd.to_datetime(date_str, format='%d-%m-%Y')
    engine = create_engine(config, echo=False)

    unit_data = pd.read_sql_query("""SELECT a.unit_master_pk, a.unit_name,
        a.cogeneration_ind,
        a.rated_unit_capacity,
        round(a.min_generation_Level * a.rated_unit_capacity,0) min_cap,
        round(a.max_generation_Level * a.rated_unit_capacity,0) max_cap,
        round(b.ramp_up_rate * rated_unit_capacity,0) ramp_up_mw_min,
        round(b.ramp_down_rate * rated_unit_capacity,0) ramp_down_mw_min,
        round(b.from_capacity_perc * Rated_Unit_Capacity,0) cap_low_limit,
        round(b.to_capacity_perc * Rated_Unit_Capacity,0) cap_upper_limit
        from power.unit_master a,
        unit_master_lines b
        where a.unit_master_pk = b.unit_master_fk
        """, engine, index_col=None)

    contract_master = pd.read_sql_query("""SELECT a.contract_code,
        a.contract_name, a.contracted_capacity,
        a.surrender_vol_perc, a.buy_ind,
        c.counter_party_name cp1, d.counter_party_name cp2
        from power.contract_trade_master a,
             power.counter_party_master c,
             power. counter_party_master d
        where a.counter_party_1_fk = c.counter_party_master_pk
        and   a.counter_party_2_fk = d.counter_party_master_pk
        and c.counter_party_name = '{}'
        and a.delete_ind = 0
        and c.delete_ind = 0
        and d.delete_ind = 0
        """.format(discom), engine, index_col=None)

    # /*Contract and Costing*/
    variable_cost = pd.read_sql_query("""SELECT a.contract_name,
        f.cost_type_name, e.cost, e.valid_from_dt,e.valid_to_dt
        from power.contract_trade_master a,
             power.contract_cost_breakdown e,
             power.contract_cost_type f
        where a.contract_trade_master_pk = e.contract_trade_master_fk
        and e.valid_from_dt <= '{}'
        and e.valid_to_dt >= '{}'
        and f.cost_type_name = 'VARIABLE'
        and  e.contract_cost_type_fk = f.contract_cost_type_pk
        and a.delete_ind = 0
        and e.delete_ind = 0
        and f.delete_ind = 0
        order by contract_name
        """.format(valid_date, valid_date), engine, index_col=None)

    # Current running schedule & Declared Capacity
    Running_Schedule_SCADA_temp = pd.read_sql_query("""SELECT date,
        block_no, revision, pool_name, pool_type, entity_name, quantum
        from power.scada_processed_staging
        where date = '{}'
        and pool_type = 'CONVENTIONAL'
        and discom = '{}'
        """.format(valid_date, discom), engine, index_col=None)

    Running_Schedule_SCADA_temp['date'] = \
        pd.to_datetime(Running_Schedule_SCADA_temp['date'])

    organisation_code = pd.read_sql_query("""select organisation_master_pk
        from power.organisation_master
        where organisation_code = '{}'
        """.format(discom), engine, index_col=None)
    organisation_code_fk = organisation_code['organisation_master_pk'].iloc[0]

    internal_declared_capacity = pd.read_sql_query("""SELECT a.date as date,
        a.block_no_fk as block_no,
        a.revision as revision, b.unit_name,
        a.declared_capacity as declared_capacity,
        b.unit_type_name
        from (select *
              from power.declared_capacity
              where date = '{}'
              and organisation_fk = {}
              and delete_ind = 0) a,
             (select um.unit_master_pk, um.unit_name, ut.unit_type_name
              from power.unit_master um,
                   power.unit_type ut
              where ut.unit_type_pk = um.unit_type_fk
              and ut.delete_ind = 0
              and um.delete_ind = 0) b
        where a.unit_master_fk = b.unit_master_pk
        """.format(valid_date, organisation_code_fk),
        engine, index_col=None)
    max_revsion_Schedule_SCADA = \
        max(Running_Schedule_SCADA_temp['revision'])
    Running_Schedule_SCADA_temp = \
        Running_Schedule_SCADA_temp[
            Running_Schedule_SCADA_temp['revision'] ==
            max_revsion_Schedule_SCADA]
    internal_declared_capacity['date'] = \
        pd.to_datetime(internal_declared_capacity['date'])

    unique_entity = Running_Schedule_SCADA_temp['entity_name'].unique()
    Running_Schedule_SCADA = pd.DataFrame([])
    for j in range(0, len(unique_entity)):
        test = \
            Running_Schedule_SCADA_temp[
                Running_Schedule_SCADA_temp['entity_name'] == unique_entity[j]]
        for col in ['quantum']:
            test[col] = test[col].ffill()
        Running_Schedule_SCADA = Running_Schedule_SCADA.append(test)
    Running_Schedule_SCADA['quantum'] = \
        np.where((Running_Schedule_SCADA['quantum'] >= 0),
                 Running_Schedule_SCADA['quantum'], 0)

    Running_Schedule_ISGS = pd.read_sql_query("""select a.*
        from (select *
             from power.wrldc_state_drawl_schedule_stg
             where date = '{}'
             and drawl_type = 'ISGS') a,
        (select a.ldc_org_name state
        from org_isgs_map a,
             state_master b,
             organisation_master c
        where c.state_master_fk = b.state_master_pk
        and a.organisation_master_fk = c.organisation_master_pk
        and a.delete_ind = 0
        and b.delete_ind = 0
        and c.delete_ind = 0
        and c.organisation_code = '{}'
        and b.state_name = '{}') b
        where a.state = b.state
        """.format(valid_date, discom, state),
        engine, index_col=None)
    max_revision_isgs = np.max(Running_Schedule_ISGS.Revision)
    Running_Schedule_ISGS = \
        Running_Schedule_ISGS[
            Running_Schedule_ISGS['Revision'] == max_revision_isgs]
    Running_Schedule_ISGS['Date'] =\
        pd.to_datetime(Running_Schedule_ISGS['Date'])

    Running_suplus_deficit = pd.read_sql_query("""SELECT *
        from power.realtime_position_map_staging
        where discom = '{}'
        and date = '{}'
        and pool_name = 'SURPLUS_DEFICIT'
        """.format(discom, valid_date), engine, index_col=None)

    internal_declared_capacity = \
        internal_declared_capacity[
            internal_declared_capacity['unit_type_name'] != 'SOLAR']

    internal_declared_capacity = \
        internal_declared_capacity[
            internal_declared_capacity['unit_type_name'] != 'WIND']

    # internal_declared_capacity = internal_declared_capacity[internal_declared_capacity['unit_type_name'] 
    #                                                                 !='PUMPED STORAGE HYDRO']
    max_revision_internal_dec = max(internal_declared_capacity.revision)

    internal_declared_capacity = \
        internal_declared_capacity[
            internal_declared_capacity['revision'] ==
            max_revision_internal_dec]

    internal_declared_capacity.rename(
        columns={'declared_capacity': 'allocated_schedule'}, inplace=True)
    internal_declared_capacity_final = \
        internal_declared_capacity[['date',
                                    'block_no',
                                    'unit_name',
                                    'allocated_schedule']]

    max_revision_internal_run = max(Running_Schedule_SCADA['revision'])
    internal_running_capacity = \
        Running_Schedule_SCADA[
            Running_Schedule_SCADA['revision'] ==
            max_revision_internal_run]
    internal_running_capacity.rename(
        columns={'quantum': 'current_schedule',
                 'entity_name': 'unit_name'}, inplace=True)
    internal_running_capacity_final = \
        internal_running_capacity[['date',
                                   'block_no',
                                   'unit_name',
                                   'current_schedule']]

    internal_declared_capacity_final['date'] = \
        pd.to_datetime(internal_declared_capacity_final['date'])
    internal_running_capacity_final['date'] = \
        pd.to_datetime(internal_running_capacity_final['date'])

    allocated_running_schedule_internal = \
        pd.merge(internal_declared_capacity_final,
                 internal_running_capacity_final,
                 how='left',
                 on=['date',
                     'block_no',
                     'unit_name'])

    current_schedule_isgs = \
        Running_Schedule_ISGS.loc[
            (Running_Schedule_ISGS.Revision == max_revision_isgs)]
    allocated_schedule_isgs = \
        Running_Schedule_ISGS.loc[
            (Running_Schedule_ISGS.Revision == 0)]
    allocated_schedule_isgs = \
        allocated_schedule_isgs[['Station_Name',
                                 'Date',
                                 'Block_No',
                                 'Schedule']]

    allocated_schedule_isgs.rename(
        columns={'Schedule': 'allocated_schedule'}, inplace=True)
    current_schedule_isgs = \
        current_schedule_isgs[['Station_Name',
                               'Date',
                               'Block_No',
                               'Schedule']]

    current_schedule_isgs.rename(
        columns={'Schedule': 'current_schedule'}, inplace=True)

    allocated_schedule_isgs['Date'] = \
        pd.to_datetime(allocated_schedule_isgs['Date'])
    current_schedule_isgs['Date'] = \
        pd.to_datetime(current_schedule_isgs['Date'])

    allocated_running_schedule_ISGS = \
        pd.merge(allocated_schedule_isgs,
                 current_schedule_isgs,
                 how='left',
                 on=['Station_Name',
                     'Date',
                     'Block_No'])

    allocated_running_schedule_ISGS.rename(
        columns={'Date': 'date',
                 'Block_No': 'block_no',
                 'Station_Name': 'unit_name'}, inplace=True)
    # list(allocated_running_schedule_ISGS), list(allocated_running_schedule_internal)
    allocated_running_schedule_ISGS['import'] = 1
    allocated_running_schedule_internal['import'] = 0
    allocated_running_schedule_composite = pd.DataFrame([])
    allocated_running_schedule_composite = \
        allocated_running_schedule_composite.append(
            allocated_running_schedule_ISGS)
    allocated_running_schedule_composite = \
        allocated_running_schedule_composite.append(
            allocated_running_schedule_internal)
    allocated_running_schedule_composite = \
        allocated_running_schedule_composite[['date',
                                              'block_no',
                                              'unit_name',
                                              'import',
                                              'allocated_schedule',
                                              'current_schedule']]

    test = \
        allocated_running_schedule_composite[
            allocated_running_schedule_composite['allocated_schedule'] > 0]
    test1 = allocated_running_schedule_composite[
        allocated_running_schedule_composite['current_schedule'] > 0]
    allocated_unit = test['unit_name'].unique()
    runing_unit = test1['unit_name'].unique()
    variable_cost['valid_from_dt'] = \
        pd.to_datetime(variable_cost['valid_from_dt'])
    variable_cost['valid_to_dt'] = pd.to_datetime(variable_cost['valid_to_dt'])
    valid_date = pd.to_datetime(valid_date)

    valid_variable_cost = \
        variable_cost[(variable_cost['valid_from_dt'] <= valid_date) &
                      (variable_cost['valid_to_dt'] >= valid_date)]

    valid_ppa = variable_cost['contract_name'].unique()
    valid_unit_varcost = \
        contract_master[contract_master['contract_name'].isin(valid_ppa)]

    allunits_varcost = pd.merge(valid_unit_varcost, variable_cost,
                                how='left',
                                on='contract_name')
    unit_costs = allunits_varcost[['cp2', 'cost', 'surrender_vol_perc']]
    unit_costs = unit_costs.drop_duplicates()

    allocated_running_schedule_internal['date'] = \
        pd.to_datetime(allocated_running_schedule_internal['date'])
    current_day_int_gen = \
        allocated_running_schedule_internal[
            allocated_running_schedule_internal['date'] == valid_date]

    unique_block = current_day_int_gen['block_no'].unique()
    unit_current_limit = pd.DataFrame([])
    for k in range(0, len(unique_block)):
        current_day_int_gen_block = \
            current_day_int_gen[
                current_day_int_gen['block_no'] == unique_block[k]]
        unique_entity = current_day_int_gen_block['unit_name'].unique()
        for j in range(0, len(unique_entity)):
            child_unit = unit_data[unit_data['unit_name'] == unique_entity[j]]
            current_schedule = \
                current_day_int_gen_block[
                    current_day_int_gen_block['unit_name'] == unique_entity[j]]
            for i in range(0, len(child_unit)):
                test = child_unit.iloc[i]
                test['block_no'] = unique_block[k]
                T = current_schedule['current_schedule'].values
                if ((T[0] >= test['cap_low_limit']) and
                        (T[0] <= test['cap_upper_limit'])):
                    unit_current_limit = unit_current_limit.append(test)
    unit_current_limit_internal = pd.DataFrame(unit_current_limit)
    unit_current_limit_internal['date'] = valid_date

    allocated_running_schedule_composite['date'] = \
        pd.to_datetime(allocated_running_schedule_composite['date'])
    unit_current_limit_internal['date'] = \
        pd.to_datetime(unit_current_limit_internal['date'])

    runnig_schedule_limit = \
        pd.merge(pd.merge(allocated_running_schedule_composite,
                          unit_current_limit_internal,
                          how='left',
                          on=['date', 'block_no', 'unit_name']),
                 unit_costs,
                 how='left',
                 left_on=['unit_name'],
                 right_on=['cp2'])

    runnig_schedule_limit['ramp_up_perblock'] = \
        runnig_schedule_limit['ramp_up_mw_min'] * 15
    runnig_schedule_limit['ramp_down_perblock'] = \
        runnig_schedule_limit['ramp_down_mw_min'] * 15

    runnig_schedule_limit['current_schedule'].fillna(0, inplace=True)
    runnig_schedule_limit['allocated_schedule'].fillna(0, inplace=True)
    runnig_schedule_limit['cap_low_limit'] = \
        np.where((np.isfinite(runnig_schedule_limit['cap_low_limit'])),
                 runnig_schedule_limit['cap_low_limit'],
                 (runnig_schedule_limit['current_schedule'] -
                 (runnig_schedule_limit['current_schedule'] *
                  runnig_schedule_limit['surrender_vol_perc'])))

    runnig_schedule_limit['cap_low_limit'] = \
        np.where(runnig_schedule_limit['cap_low_limit'] > 0,
                 runnig_schedule_limit['cap_low_limit'], 0)
    runnig_schedule_limit['cap_upper_limit'] = \
        np.where((np.isfinite(runnig_schedule_limit['cap_upper_limit'])),
                 runnig_schedule_limit['cap_upper_limit'],
                 runnig_schedule_limit['allocated_schedule'])
    runnig_schedule_limit['cap_upper_limit'] = \
        np.where(runnig_schedule_limit['cap_upper_limit'] > 0,
                 runnig_schedule_limit['cap_upper_limit'], 0)
    runnig_schedule_limit['ramp_up_perblock'] = \
        np.where((np.isfinite(runnig_schedule_limit['ramp_up_perblock'])),
                 runnig_schedule_limit['ramp_up_perblock'],
                 (runnig_schedule_limit['allocated_schedule'] -
                 runnig_schedule_limit['current_schedule']))
    runnig_schedule_limit['ramp_down_perblock'] = \
        np.where((np.isfinite(runnig_schedule_limit['ramp_down_perblock'])),
                 runnig_schedule_limit['ramp_down_perblock'],
                 (runnig_schedule_limit['current_schedule'] *
                  runnig_schedule_limit['surrender_vol_perc']))
    runnig_schedule_limit['ramp_up_perblock'] = \
        np.where((runnig_schedule_limit['ramp_up_perblock'] > 0),
                 runnig_schedule_limit['ramp_up_perblock'], 0)
    runnig_schedule_limit['ramp_down_perblock'] = \
        np.where((runnig_schedule_limit['ramp_down_perblock'] > 0),
                 runnig_schedule_limit['ramp_down_perblock'], 0)
    runnig_schedule_limit = \
        runnig_schedule_limit[
            runnig_schedule_limit['current_schedule'] > 0]

    max_revision = Running_suplus_deficit['revision']
    Running_suplus_deficit = \
        Running_suplus_deficit[
            Running_suplus_deficit['revision'] == max_revision]
    real_time_position = Running_suplus_deficit.copy()
    real_time_position['surplus'] = \
        np.where((real_time_position['quantum'] >= 0),
                 real_time_position['quantum'], 0)
    real_time_position['deficit'] = \
        np.where((real_time_position['quantum'] <= 0),
                 real_time_position['quantum'] * -1, 0)
    real_time_position['date'] = pd.to_datetime(real_time_position['date'])

    deficit_position = real_time_position[real_time_position['deficit'] > 0]
    deficit_block = deficit_position['block_no'].unique()
    ramp_up_table = pd.DataFrame([])
    for j in range(0, len(deficit_block)):
        block_scehedule = runnig_schedule_limit[
            runnig_schedule_limit['block_no'] == deficit_block[j]]
        block_position_map = real_time_position[
            real_time_position['block_no'] == deficit_block[j]]
        running_units = block_scehedule['unit_name'].unique()
        running_unit_cost = unit_costs[unit_costs['cp2'].isin(running_units)]
        running_unit_cost.sort_values(
            by=['cost'], ascending=[True], inplace=True)
        runnig_units_order = running_unit_cost['cp2'].unique()
        vraible_cost = list(running_unit_cost['cost'])

        deficit = balance = block_position_map['deficit'].values[0]
        cum_ramp = 0
        k = 0
        while not (balance == 0 or k == len(runnig_units_order)):
            balance = deficit  # start from scratch
            for i in range(0, len(runnig_units_order)):
                least_cost_unit = block_scehedule[
                    block_scehedule['unit_name'] == runnig_units_order[i]]
                max_possible_rampup = \
                    least_cost_unit['ramp_up_perblock'].values[0]
                current_schedule = \
                    least_cost_unit['current_schedule'].values[0]
                max_capacity = least_cost_unit['cap_upper_limit'].values[0]
                ramp_up = min(max_possible_rampup,
                              balance, max_capacity - current_schedule)
                cum_ramp = cum_ramp + ramp_up
                residual = max(0, deficit - cum_ramp)
                balance = residual
                k = k + i
                least_cost_unit['ramp_up'] = ramp_up
                least_cost_unit['cum_ramp_up'] = cum_ramp
                least_cost_unit['variable_cost'] = vraible_cost[i]
                ramp_up_table = ramp_up_table.append(least_cost_unit)
    # block_1_scehedule = block_1_scehedule[block_1_scehedule['unit_name'].isin(running_units)]

    surplus_position = real_time_position[real_time_position['surplus'] > 0]
    surplus_block = surplus_position['block_no'].unique()
    ramp_down_table = pd.DataFrame([])
    unique_block = real_time_position['block_no'].unique()
    t = 0
    for j in range(0, len(unique_block)):
        block_scehedule = runnig_schedule_limit[
            runnig_schedule_limit['block_no'] == unique_block[j]] 
        block_position_map = real_time_position[
            real_time_position['block_no'] == unique_block[j]]
        running_units = block_scehedule['unit_name'].unique()
        running_unit_cost = unit_costs[unit_costs['cp2'].isin(running_units)]
        running_unit_cost.sort_values(
            by=['cost'], ascending=[False], inplace=True)
        runnig_units_order1 = running_unit_cost['cp2'].unique()
        vraible_cost = list(running_unit_cost['cost'])
        surplus = balance = block_position_map['surplus'].values[0]
        cum_ramp = 0
        k = 0
        while not (balance == 0 or k == len(runnig_units_order)):
            rem_balance = surplus  # start from scratch
            for i in range(0, len(runnig_units_order1)):
                highest_cost_unit = block_scehedule[
                    block_scehedule['unit_name'] == runnig_units_order1[i]]
                max_possible_ramdown = \
                    highest_cost_unit['ramp_down_perblock'].values[0]
                current_schedule = \
                    highest_cost_unit['current_schedule'].values[0]
                min_capacity = highest_cost_unit['cap_low_limit'].values[0]
                ramp_down = min(max_possible_ramdown, balance,
                                current_schedule - min_capacity)
                cum_ramp = cum_ramp + ramp_down
                residual = max(0, surplus - cum_ramp)
                balance = residual
                k = k + i
                highest_cost_unit['ramp_down'] = ramp_down
                highest_cost_unit['cum_ramp_down'] = cum_ramp
                highest_cost_unit['variable_cost'] = vraible_cost[i]
                ramp_down_table = ramp_down_table.append(highest_cost_unit)
    # block_1_scehedule = block_1_scehedule[block_1_scehedule['unit_name'].isin(running_units)]

    ramp_up_table = ramp_up_table[['date', 'block_no', 'unit_name',
                                   'ramp_up']]
    ramp_down_table = ramp_down_table[['date', 'block_no', 'unit_name',
                                       'ramp_down']]

    ramp_up_table['date'] = pd.to_datetime(ramp_up_table['date'])
    ramp_down_table['date'] = pd.to_datetime(ramp_down_table['date'])
    post_rampup_schedule = pd.merge(allocated_running_schedule_composite,
                                    ramp_up_table,
                                    how='left',
                                    on=['unit_name', 'date', 'block_no'])
    post_rampup_schedule['ramp_up'].fillna(0, inplace=True)

    final_schedule_conventional = \
        pd.merge(post_rampup_schedule,
                 ramp_down_table,
                 how='left',
                 on=['unit_name', 'date', 'block_no'])
    final_schedule_conventional['ramp_down'].fillna(0, inplace=True)

    final_schedule_conventional['ramp_up_ind'] = \
        np.where((final_schedule_conventional['ramp_up'] > 0), 1, 0)
    final_schedule_conventional['ramp_down_ind'] = \
        np.where((final_schedule_conventional['ramp_down'] > 0), 1, 0)

    final_schedule_conventional['pool_name'] = \
        np.where((final_schedule_conventional['import'] == 1),
                 'ISGS', 'INT_GENERATION_ACT')
    final_schedule_conventional['pool_type'] = \
        np.where((final_schedule_conventional['import'] == 1),
                 'UNKNOWN', 'CONVENTIONAL')

    final_schedule_conventional['quantum'] = \
        (final_schedule_conventional['ramp_up'] +
         final_schedule_conventional['ramp_down'] * -1)

    max_revision = max(Running_suplus_deficit['revision'])
    final_schedule_conventional['revision'] = max_revision
    final_schedule_conventional['unit'] = 'MW'
    final_schedule_conventional['discom'] = discom
    final_schedule_conventional['state'] = state
    tablename = 'surrender_realtime_tmp_{}'.format(discom)
    final_schedule_conventional.to_sql(name=tablename,
                                       con=engine,
                                       if_exists='replace')
    surrender = pd.read_sql_query("""select count(1) count
        from realtime_surr_rev_staging
        where date = '{}'
        and discom = '{}'""".format(valid_date,
                                    discom),
                                  engine,
                                  index_col=None)
    count = surrender.iloc[0]['count']
    # print 'count:', count
    connection = engine.connect()
    if count > 0:
        upd_str = """update realtime_surr_rev_staging a,
           {} b
           set a.revision = b.revision,
           a.quantum = b.quantum,
           a.unit = b.unit
           where a.date = b.date
           and a.block_no = b.block_no
           and a.pool_name = b.pool_name
           and a.pool_type = b.pool_type
           and a.discom = b.discom
           and a.state = b.state
           and a.generator_name = b.unit_name
           and b.revision = {}
           and a.date = '{}'
           and a.discom = '{}'"""\
           .format(tablename, max_revision, valid_date, discom)
        connection.execute(upd_str)
    elif count == 0:
        ins_str = """insert into realtime_surr_rev_staging
           (date, block_no, revision, pool_name, pool_type,
            generator_name,
            quantum, unit, discom, state)
            select date, block_no, revision, pool_name,
            pool_type, unit_name,
            quantum, unit, discom, state
            from {}
            where date = '{}'
            and discom = '{}'
            and revision = {}
            on duplicate key
            update quantum = values(quantum),
            unit = values(unit),
            revision = values(revision)
            """.format(tablename, valid_date, discom,
                       max_revision)
        connection.execute(ins_str)
    connection.close()
    engine.dispose()
    return

realtime_surrender('mysql+mysqldb://root:quenext@2016@104.155.225.29/power',
                   '18-08-2017', 4, 'GUVNL', 'GUJARAT')

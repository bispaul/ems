"""Allocation Optimisation."""
from __future__ import division
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
# from numpy import array
# from numpy import sign
# from numpy import zeros
# import datetime as dt
pd.options.mode.chained_assignment = None

# tc = 0.3
# alpha = 0.95
# window = 4
# max_surrender = 400
# area = 'N2'
# date = '20-07-2016'
# discom = 'UPCL'


def allocation_optimization(config, date, tc, alpha, window,
                            max_surrender, discom, area):
    """Allocaion Optimisation."""
    date = pd.to_datetime(date, format='%d-%m-%Y')
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

    # table_ramp_up =
    unit_data = pd.DataFrame(unit_data)
    unique_unit = unit_data['unit_master_pk'].unique()
    ramp_up_limit = pd.DataFrame([])
    for j in range(0, len(unique_unit)):
        # for j in xrange(0,1):
        child_unit = unit_data[unit_data['unit_master_pk'] == unique_unit[j]]
        ramp_up = pd.DataFrame([])
        for i in range(0, len(child_unit)):
            test = child_unit.iloc[i]
            test['max_time_to_rampcap'] = \
                (test['cap_upper_limit'] - test['cap_low_limit']) /\
                test['ramp_up_mw_min']
            ramp_up = ramp_up.append(test)
            ramp_up['max_time_to_cap'] = ramp_up.max_time_to_rampcap.cumsum()
        ramp_up_limit = ramp_up_limit.append(ramp_up)
    ramp_up_limit['no_of_block_to_cap'] = \
        np.ceil(ramp_up_limit['max_time_to_cap'] / 15)
    ramp_up_limit['no_Of_block_to_rampcap'] = \
        np.ceil(ramp_up_limit['max_time_to_rampcap'] / 15)

    # /*Contracts and CounterParties*/
    contract_master = pd.read_sql_query("""SELECT a.contract_code,
        a.contract_name, a.contracted_capacity,
        a.surrender_vol_perc, a.buy_ind,
        c.counter_party_name cp1, d.counter_party_name cp2
        from power.contract_trade_master a,
             power.counter_party_master c,
             power.counter_party_master d
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
        """.format(date, date), engine, index_col=None)

    # internal_declared_capacity = pd.read_sql_query("""select
    #   a.date, a.block_no, a.revision,
    #   a.generator_name,
    #   a.schedule declared_capacity, c.unit_type_name
    #   from power.isgstentative_schedule_staging a,
    #        power.unit_master b,
    #        power.unit_type c
    #   where a.date = '{}'
    #   and a.discom = '{}'
    #   and b.unit_name = a.generator_name
    #   and c.unit_type_pk = b.unit_type_fk
    #   and b.delete_ind = 0
    #   and c.delete_ind = 0
    # """.format(valid_date, discom), engine2, index_col = None)

    internal_declared_capacity = pd.read_sql_query("""SELECT
        a.date, a.block_no, a.revision, a.generator_name unit_name,
        a.schedule declared_capacity, c.unit_type_name
        from power.isgstentative_schedule_staging a,
           (select date, discom, pool_name, max(revision)
            from power.isgstentative_schedule_staging
            where date = '{}'
            and discom = '{}'
            and pool_name = 'INT_GENERATION_ACT'
            group by date, discom, pool_name) d,
           power.unit_master b,
           power.unit_type c
        where a.date = d.date
        and a.discom = d.discom
        and a.pool_name = d.pool_name
        and a.pool_name = 'INT_GENERATION_ACT'
        and b.unit_name = a.generator_name
        and c.unit_type_pk = b.unit_type_fk
        and b.delete_ind = 0
        and c.delete_ind = 0
        """.format(date, discom), engine, index_col=None)

    isgs_capacity = pd.read_sql_query("""SELECT *
        from power.isgstentative_schedule_staging
        where discom = '{}'
        and pool_name <> 'OPT_SCHEDULE'
        and pool_name <> 'INT_GENERATION_ACT'
        and date = '{}'
        """.format(discom, date), engine, index_col=None)

    state = list(isgs_capacity['state'].unique())[0]
    isgs_capacity.rename(columns={'generator_name': 'unit_name',
                                  'schedule': 'allocated_schedule'},
                         inplace=True)
    internal_declared_capacity = \
        internal_declared_capacity[
            internal_declared_capacity['unit_type_name'] != 'SOLAR']

    internal_declared_capacity = \
        internal_declared_capacity[
            internal_declared_capacity['unit_type_name'] != 'WIND']

    max_revision_internal_dec = max(internal_declared_capacity.revision)

    internal_declared_capacity = \
        internal_declared_capacity[
            internal_declared_capacity['revision'] ==
            max_revision_internal_dec]
    isgs_declared_conventional = isgs_capacity.copy()
    max_revision = max(isgs_declared_conventional['revision'])
    isgs_declared_conventional = \
        isgs_declared_conventional[
            isgs_declared_conventional['revision'] == max_revision]
    isgs_units = isgs_declared_conventional['unit_name'].unique()
    internal_units = internal_declared_capacity['unit_name'].unique()
    internal_units = [x for x in internal_units if x is not None]
    isgs_units = [x for x in isgs_units if x is not None]
    isgs_units_final = \
        [x for x in isgs_units if x not in internal_units]

    internal_declared_capacity.rename(
        columns={'declared_capacity': 'allocated_schedule'}, inplace=True)
    internal_declared_capacity_final = internal_declared_capacity[
        internal_declared_capacity['unit_name'].isin(internal_units)]

    internal_declared_capacity_final = \
        internal_declared_capacity_final[['date',
                                          'block_no',
                                          'unit_name',
                                          'allocated_schedule']]

    isgs_declared_capacity_final = isgs_declared_conventional[
        isgs_declared_conventional['unit_name'].isin(isgs_units_final)]

    isgs_declared_capacity_final = \
        isgs_declared_capacity_final[['date',
                                      'block_no',
                                      'unit_name',
                                      'allocated_schedule']]

    declared_capacity_composit = pd.DataFrame([])
    declared_capacity_composit = \
        declared_capacity_composit.append(internal_declared_capacity_final)
    declared_capacity_composit = \
        declared_capacity_composit.append(isgs_declared_capacity_final)
    declared_capacity_composit = \
        declared_capacity_composit[
            declared_capacity_composit['allocated_schedule'] > 0]

    allocated_unit = declared_capacity_composit['unit_name'].unique()
    variable_cost['valid_from_dt'] = \
        pd.to_datetime(variable_cost['valid_from_dt'])
    variable_cost['valid_to_dt'] = pd.to_datetime(variable_cost['valid_to_dt'])

    valid_variable_cost = \
        variable_cost[(variable_cost['valid_from_dt'] <= date) &
                      (variable_cost['valid_to_dt'] >= date)]

    valid_ppa = variable_cost['contract_name'].unique()
    valid_unit_varcost = \
        contract_master[contract_master['contract_name'].isin(valid_ppa)]

    allunits_varcost = pd.merge(valid_unit_varcost, variable_cost,
                                how='left',
                                on='contract_name')
    unit_costs = allunits_varcost[['cp2', 'cost', 'surrender_vol_perc']]

    # table_ramp imternal
    internal_declared_capacity['date'] = \
        pd.to_datetime(internal_declared_capacity['date'])
    internal_declared_capacity = \
        internal_declared_capacity[
            internal_declared_capacity['allocated_schedule'] >= 0]
    internal_declared_capacity = \
        internal_declared_capacity[
            internal_declared_capacity['date'] == date]

    unique_block = internal_declared_capacity['block_no'].unique()
    unit_current_limit = pd.DataFrame([])
    for k in range(0, len(unique_block)):
        current_day_int_gen_block = \
            internal_declared_capacity[
                internal_declared_capacity['block_no'] == unique_block[k]]
        unique_entity = internal_declared_capacity['unit_name'].unique()
        for j in range(0, len(unique_entity)):
            child_unit = unit_data[unit_data['unit_name'] == unique_entity[j]]
            current_schedule = \
                current_day_int_gen_block[
                    current_day_int_gen_block['unit_name'] == unique_entity[j]]
            for i in range(0, len(child_unit)):
                test = child_unit.iloc[i]
                test['block_no'] = unique_block[k]
                T = current_schedule['allocated_schedule'].values
                if ((T[0] > test['cap_low_limit']) and
                        (T[0] <= test['cap_upper_limit'])):
                    unit_current_limit = unit_current_limit.append(test)

    unit_current_limit_internal = pd.DataFrame(unit_current_limit)
    unit_current_limit_internal['date'] = date

    declared_capacity_composit['date'] = \
        pd.to_datetime(declared_capacity_composit['date'])
    unit_current_limit_internal['date'] = \
        pd.to_datetime(unit_current_limit_internal['date'])

    declared_schedule_limit = \
        pd.merge(declared_capacity_composit,
                 unit_current_limit_internal,
                 how='left',
                 on=['date', 'block_no', 'unit_name'])
    declared_schedule_limit['ramp_up_perblock'] = \
        declared_schedule_limit['ramp_up_mw_min'] * 15
    declared_schedule_limit['ramp_down_perblock'] = \
        declared_schedule_limit['ramp_down_mw_min'] * 15

    schedule_cost = pd.merge(declared_schedule_limit,
                             unit_costs,
                             how='left',
                             left_on=['unit_name'],
                             right_on=['cp2'])

    declared_schedule_cost = schedule_cost.copy()

    declared_schedule_cost['cap_low_limit'] = \
        np.where((np.isfinite(declared_schedule_cost['cap_low_limit'])),
                 declared_schedule_cost['cap_low_limit'],
                 (declared_schedule_cost['allocated_schedule'] -
                 (declared_schedule_cost['allocated_schedule'] *
                  declared_schedule_cost['surrender_vol_perc'])))

    declared_schedule_cost['cap_low_limit'] = \
        np.where(declared_schedule_cost['cap_low_limit'] > 0,
                 declared_schedule_cost['cap_low_limit'], 0)

    declared_schedule_cost['cap_upper_limit'] = \
        np.where((np.isfinite(declared_schedule_cost['cap_upper_limit'])),
                 declared_schedule_cost['cap_upper_limit'],
                 declared_schedule_cost['allocated_schedule'])

    declared_schedule_cost['cap_upper_limit'] = \
        np.where(declared_schedule_cost['cap_upper_limit'] > 0,
                 declared_schedule_cost['cap_upper_limit'], 0)

    declared_schedule_cost['ramp_up_perblock'] = \
        np.where((np.isfinite(declared_schedule_cost['ramp_up_perblock'])),
                 declared_schedule_cost['ramp_up_perblock'],
                 declared_schedule_cost['allocated_schedule'])

    declared_schedule_cost['ramp_down_perblock'] = \
        np.where((np.isfinite(declared_schedule_cost['ramp_down_perblock'])),
                 declared_schedule_cost['ramp_down_perblock'],
                 (declared_schedule_cost['allocated_schedule'] *
                  declared_schedule_cost['surrender_vol_perc']))

    declared_schedule_cost['ramp_up_perblock'] = \
        np.where((declared_schedule_cost['ramp_up_perblock'] > 0),
                 declared_schedule_cost['ramp_up_perblock'], 0)

    declared_schedule_cost['ramp_down_perblock'] = \
        np.where((declared_schedule_cost['ramp_down_perblock'] > 0),
                 declared_schedule_cost['ramp_down_perblock'], 0)

    capacity_cost_table = declared_schedule_cost[
        ['date', 'block_no', 'unit_name',
         'allocated_schedule', 'cap_low_limit',
         'cap_upper_limit', 'ramp_up_perblock',
         'ramp_down_perblock', 'cost']]

    tentative_schedule = capacity_cost_table.copy()
    tentative_schedule['date'] = pd.to_datetime(tentative_schedule['date'])
    tentative_schedule = \
        tentative_schedule[tentative_schedule['date'] == date]
    tentative_schedule['schedule'] = tentative_schedule['allocated_schedule']
    tentative_schedule['percentage_surrender'] = \
        ((tentative_schedule['allocated_schedule'] -
          tentative_schedule['cap_low_limit']) /
         tentative_schedule['allocated_schedule'])
    tentative_schedule['percentage_surrender'].fillna(0, inplace=True)

    pred_price_final = pd.read_sql_query("""select * from
                                         pred_price_final
                                         where area = '{}'""".format(area),
                                         engine, index_col=None)
    pred_price_final['date'] = pd.to_datetime(pred_price_final['date'])
    pred_price_dayahead = \
        pred_price_final[pred_price_final['date'] == date]
    pred_price_dayahead = \
        pred_price_dayahead[pred_price_dayahead['alpha'] == alpha]

    # max_revision = max(tentative_schedule['revision'])

    tentative_dayahead = \
        tentative_schedule.loc[(tentative_schedule.date == date)]
    gen_master = tentative_schedule.copy()

    gen_master['avl_surrender_vol'] = \
        gen_master['allocated_schedule'] * gen_master['percentage_surrender']

    avail_gen = gen_master.loc[(gen_master.avl_surrender_vol > 0)]

    avail_gen = avail_gen[['date', 'block_no', 'unit_name',
                           'allocated_schedule', 'avl_surrender_vol']]

    avail_gen = avail_gen[avail_gen['avl_surrender_vol'] > 0]

    gen_scheopt_id = [col for col in gen_master.columns
                      if 'date' in col or
                      'block_no' in col or
                      'unit_name' in col or
                      'cost' in col or
                      'schedule' in col or
                      'percentage_surrender' in col or
                      '_surrender_' in col]

    gen_scheopt_id = gen_master[gen_scheopt_id]

    allocation_table = \
        pd.merge(gen_scheopt_id, pred_price_dayahead,
                 how='left', on=['date', 'block_no'])
    allocation_table['variable_cost'] = allocation_table['cost']
    allocation_table['indicator'] = \
        np.where((allocation_table['variable_cost'] >=
                  allocation_table['ul_price'] + tc), 1, 0)

    station = pd.unique(allocation_table['unit_name'])
    rolling_ind = pd.DataFrame([])
    for j in range(0, len(station)):
        test = allocation_table[allocation_table['unit_name'] == station[j]]
        test.sort_values(by=['date', 'block_no'],
                         ascending=[True, True],
                         inplace=True)
        test['cum_ind'] = \
            test['indicator'].rolling(window=window, center=False).sum()
        rolling_ind = rolling_ind.append(test)

    station = pd.unique(allocation_table['unit_name'])
    rolling_ind1 = pd.DataFrame([])

    for j in range(0, len(station)):
        test1 = \
            allocation_table[allocation_table['unit_name'] == station[j]]
        test1.sort_values(by=['date', 'block_no'],
                          ascending=[True, False],
                          inplace=True)
        test1['cum_ind1'] = \
            test1['indicator'].rolling(window=window, center=False).sum()
        test1 = test1[['unit_name',
                       'date',
                       'block_no',
                       'cum_ind1']]
        rolling_ind1 = rolling_ind1.append(test1)

    rolling_ind = pd.merge(rolling_ind, rolling_ind1,
                           how='left',
                           on=['unit_name',
                               'date',
                               'block_no'])
    rolling_ind['surrender'] = \
        np.where((rolling_ind['cum_ind'] == window) |
                 (rolling_ind['cum_ind1'] == window), 1, 0)

    block = pd.unique(rolling_ind['block_no'])
    block_wise_surrender = pd.DataFrame([])
    for j in range(0, len(block)):
        test = rolling_ind[rolling_ind['block_no'] == block[j]]
        test.sort_values(by=['variable_cost'], ascending=[False], inplace=True)
        test['cum_schedule_surrender'] = test.avl_surrender_vol.cumsum()
        block_wise_surrender = block_wise_surrender.append(test)

    block_wise_surrender['cum_schedule_surrender_lag1'] = \
        block_wise_surrender.groupby(['block_no'])['cum_schedule_surrender'].\
        transform(lambda x: x.shift(1))

    block_wise_surrender['cum_schedule_surrender_lag1'].fillna(0, inplace=True)

    block_wise_surrender['surrender_vol'] = \
        np.where((block_wise_surrender['cum_schedule_surrender'] <=
                  max_surrender),
                 block_wise_surrender['avl_surrender_vol'],
                 max_surrender -
                 block_wise_surrender['cum_schedule_surrender_lag1'])

    block_wise_surrender['surrender_vol'] = \
        np.where((block_wise_surrender['surrender_vol'] < 0), 0,
                 block_wise_surrender['surrender_vol'])

    block_wise_surrender['feasible_surrender_vol'] = \
        (block_wise_surrender['surrender_vol'] *
         block_wise_surrender['surrender'])

    block_wise_surrender['station_name'] = block_wise_surrender['unit_name']

    surrender_table = block_wise_surrender[['date',
                                            'block_no',
                                            'station_name',
                                            'schedule',
                                            'feasible_surrender_vol']]
    surrender_table['revised_schedule'] = \
        (surrender_table['schedule'] -
         surrender_table['feasible_surrender_vol'])

    optimized_schedule = \
        surrender_table[['date', 'block_no',
                         'station_name', 'revised_schedule']]
    optimized_schedule.rename(columns={'station_name': 'generator_name',
                                       'revised_schedule': 'schedule'},
                              inplace=True)
    optimized_schedule['revision'] = max_revision
    optimized_schedule['pool_name'] = "OPT_SCHEDULE"
    optimized_schedule['discom'] = discom
    optimized_schedule['state'] = state
    tabname = 'optimized_schedule_{}'.format(discom)
    optimized_schedule.to_sql(name=tabname, con=engine,
                              if_exists='replace')

    sql_str = """delete iss
        from isgstentative_schedule_staging iss,
        (select distinct
         date, block_no, revision, generator_name,
         pool_name, discom,
         state from {}) b
        where iss.date = b.date
        and iss.block_no = b.block_no
        and iss.revision = b.revision
        and iss.generator_name = b.generator_name
        and iss.pool_name = b.pool_name
        and iss.discom = b.discom
        and iss.state = b.state""".format(tabname)
    connection = engine.connect()
    connection.execute(sql_str)
    connection.close()
    sql_str = """insert into isgstentative_schedule_staging
        (date, block_no, revision, generator_name, pool_name, discom,
         state, schedule)
        select a.date, a.block_no, a.revision,
        a.generator_name, a.pool_name,
        a.discom, a.state, a.schedule
        from {} a
        on duplicate key
        update schedule = a.schedule""".format(tabname)
    connection = engine.connect()
    connection.execute(sql_str)
    connection.close()
    return

# allocation_optimization('mysql+mysqldb://root:quenext@2016@104.155.225.29/power', '09-08-2017',
#                         0.3, 0.95, 4, 1000, 'GUVNL', 'W2')

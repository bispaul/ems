from sqlalchemy import create_engine
import pandas as pd
import datetime as dt
import numpy as np
from scipy.stats import norm


def price_simulator(engine, alpha, date, area):
    # engine = create_engine('mysql://root:power@2012@localhost/power',
    #                        echo=False)
    # engine = create_engine(config, echo=False)
    rolling_median_resid_dev = pd.read_sql_query("""select *
                                        from
                                        rolling_median_resid_{}
                                        where date =
                                        str_to_date('{}', '%%d-%%m-%%Y')""".
                                                 format(area, date),
                                                 engine, index_col=None)
    valid_date = pd.to_datetime(dt.datetime.strptime(date, '%d-%m-%Y'))
    date = valid_date
    rolling_median_resid_dev = \
        rolling_median_resid_dev[rolling_median_resid_dev['date'] == date]
    rolling_median_resid_dev['UL_IEX_FINAL'] = \
        rolling_median_resid_dev['IEX_PRED_FINAL'] + \
        norm.ppf(alpha) * \
        rolling_median_resid_dev['mad_resid_dev']
    rolling_median_resid_dev['LL_IEX_FINAL'] = \
        rolling_median_resid_dev['IEX_PRED_FINAL'] - \
        norm.ppf(alpha) * \
        rolling_median_resid_dev['mad_resid_dev']
    return rolling_median_resid_dev


def trade(config, date, discom, demand_model_name,
          regen_model_name, lmin, lmax, ladder, step,
          area, demandz=1, generationz=1.65):
    """Trade Model."""
    pool_type = 'WIND'
    valid_date = pd.to_datetime(dt.datetime.strptime(date, '%d-%m-%Y'))
    lag_date = \
        pd.to_datetime(dt.datetime.strptime(date, '%d-%m-%Y')) - \
        pd.DateOffset(60)
    engine = create_engine(config, echo=False)

    # variable Cost
    variable_cost = pd.read_sql_query("""select a.contract_name,
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

    contract_master = pd.read_sql_query("""select a.contract_code,
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

    variable_cost_table = \
        pd.merge(variable_cost,
                 contract_master,
                 how='left',
                 on='contract_name')
    variable_cost_table = \
        variable_cost_table[['cp2', 'cost', 'valid_from_dt', 'valid_to_dt']]
    gen_cost = variable_cost_table.copy()

    gen_cost['valid_from_dt'] = pd.to_datetime(gen_cost['valid_from_dt'])
    gen_cost['valid_to_dt'] = pd.to_datetime(gen_cost['valid_to_dt'])
    gen_cost = gen_cost.loc[(gen_cost.valid_from_dt <= valid_date) &
                            (gen_cost.valid_to_dt >= valid_date)]

    tentative_schedule = pd.read_sql_query("""select *
        from isgstentative_schedule_staging
        where date = '{}'
        and discom = '{}'
        """.format(valid_date, discom),
        engine, index_col=None)
    tentative_schedule['date'] = pd.to_datetime(tentative_schedule['date'])

    forecast_stg = pd.read_sql_query("""select date as date,
        block_no as block_no, revision,
        demand_forecast as demand_forecast,
        demand_bias as demand_bias
        from forecast_stg where
        date >= '{}'
        and model_name = '{}'
        and discom = '{}'
        """.format(lag_date, demand_model_name, discom),
        engine, index_col=None)
    forecast_stg['date'] = pd.to_datetime(forecast_stg['date'])

    forecast_stg_max_revision = \
        ((forecast_stg.groupby(['date'], as_index=False).
         agg({'revision': 'max'})).
         rename(columns={'revision': 'max_revision'}))
    forecast_stg = pd.merge(forecast_stg, forecast_stg_max_revision,
                            how='left',
                            on=['date'])

    forecast_stg = forecast_stg[
        forecast_stg['revision'] == forecast_stg['max_revision']]

    load_table = pd.read_sql_query("""select date,
         block_no, constrained_load
         from drawl_staging
         where discom = '{}'
         and date >= '{}'
         """.format(discom, lag_date), engine, index_col=None)
    load_table['date'] = pd.to_datetime(load_table['date'])
    load_table['hour'] = np.ceil(load_table['block_no'] / 4)
    powercut_table = pd.read_sql_query("""select date, block_no,
        sum(powercut) as powercut
        from powercut_staging
        where discom = '{}'
        and date >= '{}'
        group by date, block_no
        """.format(discom, lag_date), engine, index_col=None)
    powercut_table['date'] = pd.to_datetime(powercut_table['date'])
    load_table = pd.merge(load_table, powercut_table,
                          how='left', on=['date', 'block_no'])
    load_table['powercut'].fillna(0, inplace=True)
    load_table['reported_load'] = \
        load_table['constrained_load'] + load_table['powercut']

    model_risk_load = pd.merge(forecast_stg, load_table,
                               how='left',
                               on=['date', 'block_no'])
    model_risk_load['residual'] = (model_risk_load['reported_load'] -
                                   model_risk_load['demand_forecast'])

    model_risk_load.sort_values(by=['date', 'block_no'],
                                ascending=[True, True], inplace=True)
    unique_block = model_risk_load['block_no'].unique()
    rolling_modelrisk_load = pd.DataFrame([])
    for j in range(0, len(unique_block)):
        signal = \
            model_risk_load[model_risk_load['block_no'] == unique_block[j]]
        signal.sort_values(by=['date'], ascending=[True], inplace=True)
        signal['residual_lag'] = signal['residual'].shift(2)
        signal['error_std'] = \
            signal['residual_lag'].rolling(window=15, center=False).std()
        rolling_modelrisk_load = rolling_modelrisk_load.append(signal)
    rolling_modelrisk_load['error_std'].fillna(method='ffill', inplace=True)
    rolling_modelrisk_load['model_risk_load'] = \
        rolling_modelrisk_load['error_std'] * demandz
    
    rolling_modelrisk_load['model_risk_load'] = \
        np.where((rolling_modelrisk_load['model_risk_load'] <=
                  rolling_modelrisk_load['demand_forecast']),
                 rolling_modelrisk_load['model_risk_load'],
                 rolling_modelrisk_load['demand_forecast'])

    rolling_modelrisk_load = \
        rolling_modelrisk_load[
            ['date', 'block_no', 'model_risk_load',
             'demand_forecast', 'demand_bias']]
    print('Here *** 176', rolling_modelrisk_load)
    gen_forecast_stg = pd.read_sql_query("""select date as date,
        block_no as block_no, revision,
        gen_forecast as gen_forecast, pool_type,
        model_name as model_name
        from gen_forecast_stg where
        date >= '{}'
        and model_name = '{}'
        and org_name = '{}'
        """.format(lag_date, regen_model_name, discom),
        engine, index_col=None)
    gen_forecast_stg['date'] = pd.to_datetime(gen_forecast_stg['date'])

    gen_forecast_max_revision = \
        ((gen_forecast_stg.groupby(['date'], as_index=False).
         agg({'revision': 'max'})).
         rename(columns={'revision': 'max_revision'}))
    gen_forecast_stg = pd.merge(gen_forecast_stg, gen_forecast_max_revision,
                                how='left',
                                on=['date'])

    gen_forecast_stg = gen_forecast_stg[
        gen_forecast_stg['revision'] == gen_forecast_stg['max_revision']]
    re_pool = ['WIND', 'SOLAR']
    print('Here *** 200')
    gen_forecast_stg = gen_forecast_stg[
        gen_forecast_stg['pool_type'].isin(re_pool)]
    gen_forecast_stg_wind = gen_forecast_stg[
        gen_forecast_stg['pool_type'] == 'WIND']
    WIND_generation_table = pd.read_sql_query("""select generator_name,
        date, block_no, generation
        from generation_staging
        where pool_type = '{}'
        and discom = '{}'
        and date >= '{}'
        """.format(pool_type, discom, lag_date),
        engine, index_col=None)
    WIND_generation_table['date'] = \
        pd.to_datetime(WIND_generation_table['date'])
    wind_gen_total = \
        WIND_generation_table.groupby(
            ['date', 'block_no'], as_index=False).agg({'generation': 'sum'})
    wind_gen_total['gen_mw'] = wind_gen_total['generation'] * 4

    model_risk_wind = pd.merge(gen_forecast_stg_wind, wind_gen_total,
                               how='left',
                               on=['date', 'block_no'])
    model_risk_wind['wind_forecast_error'] = \
        (model_risk_wind['gen_mw'] - model_risk_wind['gen_forecast'])
    model_risk_wind.sort_values(by=['date', 'block_no'],
                                ascending=[True, True], inplace=True)
    unique_block = model_risk_wind['block_no'].unique()
    rolling_modelrisk_wind = pd.DataFrame([])

    for j in range(0, len(unique_block)):
        signal = \
            model_risk_wind[model_risk_wind['block_no'] == unique_block[j]]
        signal.sort_values(by=['date'], ascending=[True], inplace=True)
        signal['wind_forecast_error_lag'] = \
            signal['wind_forecast_error'].shift(2)
        signal['error_std'] = \
            signal['wind_forecast_error_lag'].\
            rolling(window=15, center=False).std()
        rolling_modelrisk_wind = rolling_modelrisk_wind.append(signal)
    else:
        print(model_risk_wind.columns.tolist() + ['wind_forecast_error_lag', 'error_std'])
        rolling_modelrisk_wind = pd.DataFrame(columns = model_risk_wind.columns.tolist() + ['wind_forecast_error_lag', 'error_std'])
    rolling_modelrisk_wind['error_std'].fillna(method='ffill', inplace=True)
    rolling_modelrisk_wind['model_risk_wind'] = \
        rolling_modelrisk_wind['error_std'] * generationz

    rolling_modelrisk_wind['model_risk_wind'] = \
        np.where((rolling_modelrisk_wind['model_risk_wind'] <=
                rolling_modelrisk_wind['gen_forecast']),
                rolling_modelrisk_wind['model_risk_wind'],
                rolling_modelrisk_wind['gen_forecast'])

    rolling_modelrisk_wind = \
        rolling_modelrisk_wind[['date', 'block_no', 'model_risk_wind']]

    re_gen_forecast = \
        ((gen_forecast_stg.groupby(['date', 'block_no'], as_index=False).
        agg({'gen_forecast': 'sum'})).
        rename(columns={'gen_forecast': 're_gen_forecast'}))

    non_opt_pool = ['ISGS', 'INT_GENERATION_ACT']

    opt_pool_schedule = \
        tentative_schedule[
            tentative_schedule['pool_name'].isin(non_opt_pool) == False]

    opt_schedule_conventional = \
        ((opt_pool_schedule.groupby(['date', 'block_no'], as_index=False).
         agg({'schedule': 'sum'})).
         rename(columns={'schedule': 'opt_schedule_conventional'}))

    demand_forecast = forecast_stg.copy()
    print('Here *** 269')
    position_map = \
        pd.merge(pd.merge(pd.merge(rolling_modelrisk_load,
                                   opt_schedule_conventional,
                                   how='left',
                                   on=['date', 'block_no']),
                          re_gen_forecast,
                          how='left',
                          on=['date', 'block_no']),
                 rolling_modelrisk_wind,
                 how='left',
                 on=['date', 'block_no'])

    position_map['position_gap'] = \
        (position_map['demand_forecast'] +
         position_map['demand_bias'] +
         position_map['model_risk_wind'] +
         position_map['model_risk_load'] -
         position_map['opt_schedule_conventional'] -
         position_map['re_gen_forecast'])

    rolling_median_resid_dev = pd.read_sql_query("""select *
        from rolling_median_resid_{}
        where date = '{}'
        """.format(area, valid_date), engine, index_col=None)
    rolling_median_resid_dev['date'] = \
        pd.to_datetime(rolling_median_resid_dev['date'])
    print('Here *** 296')
    alphas = np.linspace(lmin, lmax, num=ladder)
    # alphas = np.array([0.950])
    pred_price = pd.DataFrame([])
    for j in range(0, len(alphas)):
        test = price_simulator(engine, alphas[j], date, area)
        test['alpha'] = alphas[j]
        test['ladder'] = j + 1
        pred_price = pred_price.append(test)
    pred_price = pd.DataFrame(pred_price)
    pred_id = [col for col in test.columns
               if 'date' in col or
               'block_no' in col or
               'endo_price' in col or
               'alpha' in col or
               'UL'in col or
               'LL' in col or
               'ladder' in col]
    pred_price_final = pred_price[pred_id]

    trade_table = \
        pd.merge(pred_price_final, position_map,
                 how='left', on=['date', 'block_no'])

    trade_table['bid_volume'] = trade_table['position_gap'] / ladder

    trade_table['buy_sell'] = \
        np.where((trade_table['bid_volume'] < 0), 'sell', 'buy')
    trade_table['bid_price'] = \
        np.where((trade_table['buy_sell'] == 'buy'),
                 trade_table['UL_IEX_FINAL'],
                 trade_table['LL_IEX_FINAL'])

    trade_table = trade_table[trade_table['date'] == valid_date]
    print('Here *** 330', trade_table)
    col_index = [col for col in trade_table.columns
                 if 'date' in col or
                 'block_no' in col or
                 'alpha' in col or
                 'ladder' in col or
                 'buy_sell' in col or
                 'bid_volume' in col or
                 'bid_price'in col]
    trade_report = trade_table[col_index]
    trade_report['demand_model_name'] = demand_model_name
    trade_report['gen_model_name'] = regen_model_name
    trade_report['discom'] = discom
    trade_report['ladder_vol'] = trade_report['bid_volume']
    trade_report.bid_volume = trade_report.bid_volume.astype(float)
    trade_report.bid_volume = np.ceil(trade_report.bid_volume / step) * step
    maxrev = pd.read_sql_query("""select
        case when max(revision) is null
        then 0 else max(revision) + 1 end as maxrev
        from trade_staging_tmp
        where date = '{}'
        and discom = '{}'
        and demand_model_name = '{}'
        and gen_model_name = '{}'
        """.format(valid_date, discom, demand_model_name, regen_model_name),
        engine, index_col=None)
    rev = maxrev['maxrev'].iloc[0]
    trade_report['revision'] = rev
    table_name = "dam_bid_{}".format(discom)
    trade_report.to_sql(con=engine, name=table_name,
                        if_exists='replace', index=False)
    sql_str = """insert into trade_staging_tmp
          (date, block_no, alpha, ladder, buy_sell,
           ladder_volume, bid_price, bid_volume,
           demand_model_name, gen_model_name,
           discom, revision)
           select date, block_no, round(alpha,3), ladder, upper(buy_sell),
           abs(round(ladder_vol,3)),
           case when upper(buy_sell) = 'SELL' and round(bid_price,3) < .75
           then 0.75
           else  round(bid_price,3) end,
           abs(round(bid_volume,3)),
           demand_model_name, gen_model_name, discom, revision
           from {}""".format(table_name)
    connection = engine.connect()
    connection.execute(sql_str)
    drop_str = """drop table {}""".format(table_name)
    connection.execute(drop_str)
    connection.close()
    engine.dispose()
    return

# config = 'mysql+mysqldb://root:quenext@2016@104.155.225.29/power'
# date = '11-08-2017'
# discom = 'GUVNL'
# lmin = 0.50
# lmax = 0.99
# ladder = 2
# step = 5
# area = 'W2'
# demand_model_name = 'DLN'
# regen_model_name = 'NN'
# trade(config, date, discom, demand_model_name,
#       regen_model_name, lmin, lmax, ladder, step, area)

config = 'mysql+pymysql://root:power@2020@localhost/power'
date = '08-07-2020'
discom = 'UPCL'
lmin = .98
lmax = .99
ladder = 2
step = 2
area = 'N2'
demand_model_name = 'NEAREST_NEIGHBOUR'
regen_model_name = 'UNKNOWN'
trade(config, date, discom, demand_model_name,
     regen_model_name, lmin, lmax, ladder, step, area)

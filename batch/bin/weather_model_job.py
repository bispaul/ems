# realtime_pos_map_forecast_update
import dbconn
import time
from datetime import datetime, timedelta
import pytz
import logging
import os
import shutil
import argparse
import MySQLdb

logging.info("Starting weather_model_job ...")
logger = logging.getLogger("weather_model_job")
logger.setLevel(logging.DEBUG)


def get_datetime():
    if time.tzname[0] == 'IST':
        local_now = datetime.today()
    else:
        dest_tz = pytz.timezone('Asia/Kolkata')
        ts = time.time()
        utc_now = datetime.utcfromtimestamp(ts)
        local_now = utc_now.replace(tzinfo=pytz.utc).astimezone(dest_tz)
    return local_now


def weather_job(ldir, adir, state, discom, dsn):
    import actualweather_wu
    import forecastweather_wu
    import sql_load_lib

    # ldir = "./ems/batch/data/weather/"
    # adir = "./ems/batch/data_archive/weather/"

    # state = 'GUJARAT'
    local_now = get_datetime()

    startdate = local_now + timedelta(-1)
    enddate = local_now
    logger.info("StartDate {} EndDate {}".
                format(startdate.strftime("%d-%m-%Y"),
                       enddate.strftime("%d-%m-%Y")))

    try:
        loc = forecastweather_wu.DbFetchLocations(dsn, state)
        all_loc = loc.fetch_locations()
        # forecast_type = ['hourly', '15min']
        forecast_type = ['hourly']
        for loc, lat, lon in all_loc:
            # print loc, lat, lon
            # forecast_type = 'hourly'
            for f_type in forecast_type:
                forecast = forecastweather_wu.ForecastData()
                forecast.fetch_data(lat, lon, f_type, 'm')
                data = forecast.parse_forecast(loc, f_type)
                # print data[1:]
                dbupdate = forecastweather_wu.DbUploadData(dsn, data=data[1:])
                # print dbupdate.data
                dbupdate.db_upload_data()
        sql_load_lib.sql_sp_wtr_ibm_forecast_hrblk_ins_upd(dsn, state)
    except Exception as e:
        logger.info("Exception: %s", e)

    logger.info("FInished IBMWEATHER Forecast Upload")
    logger.info("Started IBMWEATHER Actual Upload")

    try:
        for date in actualweather_wu.daterange(startdate, enddate):
            startdt = date.strftime('%m/%d/%Y')
            enddt = (date + timedelta(1)).strftime('%m/%d/%Y')
            loc = actualweather_wu.DbFetchLocations(dsn, state)
            for loc, lat, lon in loc.fetch_locations():
                actuals = actualweather_wu.ActualData()
                actuals.fetch_data(lat, lon, startdt, enddt)
                filename = actuals.save_file(ldir, loc + '_' +
                                             date.strftime('%d-%m-%Y'))
                dbupdate = actualweather_wu.DbUploadData(dsn, filename)
                dbupdate.csv_to_data(['Location'], [loc])
                dbupdate.db_upload_data()
                dst_file = os.path.join(adir, loc + '_' +
                                        date.strftime('%d-%m-%Y') + '.csv')
                if os.path.exists(dst_file):
                    os.remove(dst_file)
                shutil.move(filename, adir)
    except Exception as e:
        logger.info("Exception: %s", e)
    sql_load_lib.sql_sp_wtr_ibm_actualhrblk_ins_upd(dsn, state)
    logger.info("Finished IBMWEATHER Actual Upload")
    try:
        sql_load_lib.sql_sp_wtr_unified_ins_upd_v2(dsn, discom)
    except Exception as e:
        logger.info("Exception sql_sp_wtr_unified_ins_upd_v2: %s", e)
    try:
        sql_load_lib.sql_sp_wtr_unified2_ins_upd(dsn, discom)
    except Exception as e:
        logger.info("Exceptions sql_sp_wtr_unified2_ins_upd: %s", e)
    logger.info("Finished IBMWEATHER Actual Upload")


def forecast_tsk(discom, db_uri, state):
    logger.info("forecast_tsk started")
    logger.info("discom: %s ", discom)
    from analytics.data_prep_forecast_nn\
        import data_prep_forecast_nn
    # logger.debug("db_uri: %s", db_uri)
    # data_prep_forecast_nn(db_uri, discom, state)
    from analytics.forecast_hybrid_knn\
        import forecast_hybrid_knn
    # forecast_hybrid_knn(db_uri, discom, state)
    from analytics.forecast_svr_scoring_guvnl \
        import forecast_svr_scoring_guvnl
    # forecast_svr_scoring_guvnl(db_uri, discom, state)
    from analytics.forecast_dln_scoring_guvnl \
        import forecast_dln_scoring_guvnl
    # forecast_dln_scoring_guvnl(db_uri, discom, state)
    from analytics.forecast_mlp_scoring_guvnl \
        import forecast_mlp_scoring_guvnl
    # forecast_mlp_scoring_guvnl(db_uri, discom, state)
    from analytics.forecast_hybrid_guvnl \
        import forecast_hybrid_guvnl
    # forecast_hybrid_guvnl(db_uri, discom, state)
    from analytics.forecast_hybriddln_guvnl \
        import forecast_hybriddln_guvnl
    # forecast_hybriddln_guvnl(db_uri, discom, state)
    from analytics.forecast_solar_nn \
        import forecast_solar_nn
    # forecast_solar_nn(db_uri, discom, state)
    from analytics.forecast_wind_nn \
        import forecast_wind_nn
    # forecast_wind_nn(db_uri, discom, state)
    from analytics.forecast_wind_hybrid_nn \
        import forecast_wind_hybrid_nn
    from analytics.forecast_wind_hybrid \
        import forecast_wind_hybrid
    os.chdir('/opt/quenext_dev/')
    discom_func_map = {'UPCL': [data_prep_forecast_nn,
                                forecast_svr_scoring_guvnl,
                                forecast_dln_scoring_guvnl,
                                forecast_mlp_scoring_guvnl,
                                forecast_hybrid_guvnl,
                                forecast_hybriddln_guvnl],
                       'GUVNL': [data_prep_forecast_nn,
                                 forecast_hybrid_knn,
                                 forecast_svr_scoring_guvnl,
                                 forecast_dln_scoring_guvnl,
                                 forecast_mlp_scoring_guvnl,
                                 forecast_hybrid_guvnl,
                                 forecast_hybriddln_guvnl,
                                 forecast_solar_nn,
                                 forecast_wind_nn,
                                 forecast_wind_hybrid_nn,
                                 forecast_wind_hybrid],
                       'ADANI': [forecast_solar_nn]}
    for ftoe in discom_func_map.get(discom):
        logger.info('Starting Model Run: ' + ftoe.__name__)
        try:
            ftoe(db_uri, discom, state)
        except Exception as err:
            logger.error('Model Failed:' + str(err) + ' ' + ftoe.__name__)


def realtime_forecast_upd_tsk(model_type, db_uri,
                              date, model, mrr, discom, state):
    logger.info("realtime_forecast_upd_tsk started")
    logger.info("%s %s %s %s %s %s",
                model_type, date, model, mrr, discom, state)
    try:
        from analytics.realtime_forecast\
            import realtime_demand_forecast
        from analytics.realtime_forecast\
            import realtime_generation_forecast
        if model_type == 'Generation':
            realtime_generation_forecast(db_uri, date, model,
                                         mrr, discom, state)
        elif model_type == 'Demand':
            realtime_demand_forecast(db_uri, date, model, mrr, discom, state)
    except Exception as err:
        logger.info('Realtime Demand/Generation Failed:' + str(err))


def realtime_forecast_update(discom, state, dsn, db_uri):
    logger.info("Starting real time forecast for Demand scheduled job")
    date_today = get_datetime().strftime("%d-%m-%Y")
    py_date_today = datetime.strptime(date_today, '%d-%m-%Y')
    sql = """SELECT
        'Generation' type, c.model_short_name, b.mrr
        from
        (select max(a.Realtime_Forecast_Metadata_PK) id
        from `power`.`realtime_forecast_metadata` a,
        power.model_master b
        where a.model_master_fk = b.id
        and a.discom = '{0}'
        and a.date <= str_to_date('{1}', '%d-%m-%Y')
        and b.model_type = 'INJECTION'
        and b.delete_ind = 0) a,
        `power`.`realtime_forecast_metadata` b,
        power.model_master c
        where a.id = b.Realtime_Forecast_Metadata_PK
        and   c.id = b.model_master_fk
        union all
        select 'Generation' type, 'HYBRID' model_short_name, 0.2 mrr
        union all
        select 'Demand' type, c.model_short_name, b.mrr
        from
        (select max(a.Realtime_Forecast_Metadata_PK) id
        from `power`.`realtime_forecast_metadata` a,
        power.model_master b
        where a.model_master_fk = b.id
        and a.discom =  '{0}'
        and a.date <= str_to_date('{1}', '%d-%m-%Y')
        and b.model_type = 'SINK'
        and b.delete_ind = 0) a,
        `power`.`realtime_forecast_metadata` b,
        power.model_master c
        where a.id = b.Realtime_Forecast_Metadata_PK
        and   c.id = b.model_master_fk
        union all
        select 'Demand' type, 'HYBRID' model_short_name, 0.2 mrr
        """.\
        format(discom, date_today)

    try:
        conn = dbconn.connect(dsn)
        datacursor = conn.cursor(MySQLdb.cursors.DictCursor)
        datacursor.execute(sql)
        model_mrr = datacursor.fetchall()
        datacursor.close()
    except Exception as err:
        logger.error("Error : %s", str(err))
        if conn.open:
            datacursor.close()
        raise
    logger.info('model_mrr %s', model_mrr)
    # Get the 1st element of Generation or Demand type
    type_hold = ''
    results_keep = []
    for ele in model_mrr:
        if ele.get('type') != type_hold:
            results_keep.append(ele)
            type_hold = ele.get('type')
    logger.debug('results_keep %s', results_keep)
    for dt in ([py_date_today]):
        for el in results_keep:
            logger.debug("Date: " + dt.strftime('%d-%m-%Y'))
            logger.debug(el)
            try:
                realtime_forecast_upd_tsk(
                    el.get('type'), db_uri, dt.strftime('%d-%m-%Y'),
                    el.get('model_short_name'),
                    float(el.get('mrr')), discom,
                    state)
            except Exception as err:
                logger.error("realtime_forecast_upd_tsk error: %s", str(err))
    return


def guvnl_email_ftp(discom, dns, db_uri):
    """guvnl_email_ftp."""
    import email_ftp_job

    dirname = '/opt/quenext_dev/ems/batch/data/forecast/email'
    ftpdir = '/opt/quenext_dev/ems/batch/data/forecast/ftp'
    archivedir = '/opt/quenext_dev/ems/batch/data_archive/forecast'
    email_ftp_job.run(dirname, ftpdir, archivedir,
                      db_uri, dns, discom)


def realtime_scada_isgs_data_init(dsn, discom):
    """
        Calls stored procedure or sql statement
    """
    import dbconn as dbconn

    connection = dbconn.connect(dsn)
    cursor = connection.cursor()
    errm = ''
    errs = ''
    try:
        logger.debug("%s %s %s", discom, errm, errs)
        cursor.callproc('sp_load_realtime',
                        (discom, errm, errs))
        cursor.execute("SELECT @_sp_load_realtime_2,"
                       " @_sp_load_realtime_3")
        data = cursor.fetchone()
        logger.info("SP Ended %s %s", data[0], data[1])
        cursor.close()
        if data[0] != 'SUCCESS':
            raise ValueError('Stored Procedure {} exited with status'
                             ' {} other than SUCCESS'
                             .format('sp_load_realtime',
                                     data[0]))
    except Exception as err:
        logger.error("SP Failed %s", str(err))
        if connection.open:
            cursor.close()
        raise
    return


def main(args):
    sql = """SELECT b.state_name, c.organisation_code
            from power.organisation_master c,
                 power.state_master b
            where c.state_master_fk = b.state_master_pk
            and c.job_ind = 1
            and c.organisation_code = '{}'
            and c.delete_ind = 0
            and b.delete_ind = 0""".format(args.discom)
    try:
        conn = dbconn.connect(args.dsn)
        datacursor = conn.cursor()
        datacursor.execute(sql)
        results = datacursor.fetchall()
        datacursor.close()
    except Exception as err:
        logger.error("Error : %s", str(err))
        if conn.open:
            datacursor.close()
        raise
    date_today = get_datetime().strftime("%d-%m-%Y")
    for r in results:
        discom = r[1]
        state = r[0]
        if not args.ignoremodelrun:
            try:
                weather_job(args.dir, args.arc, state, discom, args.dsn)
            except Exception as err:
                logger.error("Error weather_job: %s %s %s",
                             str(err), state, discom)
            # Commented Temporarily
            try:
                forecast_tsk(discom, args.db_uri, state)
            except Exception as err:
                logger.error("Error forecast_tsk: %s %s %s",
                             str(err), state, discom)
            # Commented Temporarily
            try:
                import sql_load_lib as sql_load_lib
                sql_load_lib.sql_sp_load_realtime_v2(args.dsn, discom)
            except Exception as err:
                logger.error("Error sql_sp_load_realtime_v2: %s %s %s",
                             str(err), state, discom)
            try:
                realtime_scada_isgs_data_init(args.dsn, discom)
            except Exception as err:
                logger.error("Error realtime_scada_isgs_data_init: %s %s %s",
                             str(err), state, discom)
            # Commented Temporarily
            try:
                realtime_forecast_update(discom, state, args.dsn, args.db_uri)
            except Exception as err:
                logger.error("Error realtime_forecast_update: %s %s %s",
                             str(err), state, discom)
            # Commented Temporarily
            # try:
            #     import sql_load_lib as sql_load_lib
            #     sql_load_lib.sql_sp_load_realtime_v2(args.dsn, discom)
            # except Exception as err:
            #     logger.error("Error sql_sp_load_realtime_v2: %s %s %s",
            #                  str(err), state, discom)
            # try:
            #     realtime_scada_isgs_data_init(args.dsn, discom)
            # except Exception as err:
            #     logger.error("Error realtime_scada_isgs_data_init: %s %s %s",
            #                  str(err), state, discom)
            # Commented Temporarily
            if discom == 'GUVNL':
                # Wait for 10 minutes
                # time.sleep(10 * 60)
                guvnl_email_ftp(discom, args.dsn, args.db_uri)
            try:
                from analytics.realtime_surrender\
                    import realtime_surrender
                realtime_surrender(args.db_uri, date_today, 4, discom, state)
            except Exception as err:
                logger.error("Realtime Surrender Failed: %s %s %s",
                             str(err), state, discom)
        else:
            try:
                realtime_forecast_update(discom, state, args.dsn, args.db_uri)
            except Exception as err:
                logger.error("Error realtime_forecast_update: %s %s %s",
                             str(err), state, discom)            
            # print "in else."
            # Commented Temporarily
            if discom == 'GUVNL':
                time.sleep(10 * 60)
                guvnl_email_ftp(discom, args.dsn, args.db_uri)


if __name__ == '__main__':
    ARG = argparse.ArgumentParser(description="Runs the Weather and Models")
    ARG.add_argument('-m', '--dbdsnconfig', dest='dsn',
                     help='Full path and the file name of the db config',
                     required=True)
    ARG.add_argument('-u', '--dbconfig', dest='db_uri',
                     help='SQLAlchemy DB URI',
                     required=True)
    ARG.add_argument('-d', '--dir', dest='dir',
                     help=('Directory were the file will be saved.'))
    ARG.add_argument('-a', '--arc', dest='arc',
                     help=('Archive file dir.'
                           'File will be moved here'
                           ' after db update.'),
                     required=True)
    ARG.add_argument('-dc', '--discom', dest='discom',
                     help=('Discom Name.'),
                     required=True)
    ARG.add_argument('-i', '--ignoremodelrun', dest='ignoremodelrun',
                     help=('Skip model runs. Default is False.'),
                     action='store_true',
                     default=False,
                     required=False)
    main(ARG.parse_args())

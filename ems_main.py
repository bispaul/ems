# Copyright 2016 Quenext.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
# import random
# import time
import simplejson as json
import flask
from flask import request, render_template, redirect, \
    url_for, jsonify, Blueprint, current_app
# import logging
from .DataTables import DataTablesServer
# from MySQLdb import cursors, OperationalError
from MySQLdb import OperationalError
# from forms import ContactForm, SignupForm, SigninForm
from ems import mysql, celery, cross_origin, csrfprotect
from celery.utils.log import get_task_logger
import sys
from datetime import datetime, timedelta
# from flask.ext.cors import cross_origin
# from flask.ext.uploads import UploadSet
from werkzeug.utils import secure_filename
# For Agri
from geomet import wkt
import geojson
# from . import ems_storage
# from . import models
from flask_user import login_required, roles_required, current_user
from .csvvalidator import CSVValidator,\
    number_range_inclusive, datetime_string,\
    RecordError, enumeration
import petl as etl
import re
import time
import pytz
import shutil
from retrying import retry
import copy
# from models import db
from celery.task.schedules import crontab
from celery.decorators import periodic_task


ems = Blueprint('ems', __name__)
logger = get_task_logger(__name__)
# con = mysql.connect()
# con.ping(True)
# cur = con.cursor()
# cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")

# class DB:
#     def __init__(self):
#         self.connect()

#     def connect(self):
#         try:
#             self.conn = mysql.connect()
#         except (AttributeError, OperationalError), e:
#             current_app.logger.error(
#                 'Exception creating sql connection: %s',
#                 e)
#             raise e

#     def query(self, sql, flag=None, data=None):
#         try:
#             cursor = self.conn.cursor()
#             cursor.execute("SET SESSION TRANSACTION "
#                            "ISOLATION LEVEL READ COMMITTED")
#             if flag == 'insert':
#                 cursor.executemany(sql, data)
#             elif data:
#                 cursor.execute(sql, data)
#             else:
#                 cursor.execute(sql)
#         except (AttributeError, OperationalError) as e:
#             current_app.logger.error(
#                 'Exception generated during sql query: %s',
#                 e)
#             self.close()
#             self.connect()
#             cursor = self.conn.cursor()
#             cursor.execute("SET SESSION TRANSACTION "
#                            "ISOLATION LEVEL READ COMMITTED")
#             if flag == 'insert':
#                 cursor.executemany(sql, data)
#             elif data:
#                 cursor.execute(sql, data)
#             else:
#                 cursor.execute(sql)
#         return cursor

#     def query_dictcursor(self, sql, flag=None, data=None):
#         try:
#             current_app.logger.debug('sql: %s', sql)
#             cursor = self.conn.cursor(cursors.DictCursor)
#             current_app.logger.debug('debug 2')
#             cursor.execute("SET SESSION TRANSACTION "
#                            "ISOLATION LEVEL READ COMMITTED")
#             if flag == 'insert':
#                 cursor.executemany(sql, data)
#             elif data:
#                 cursor.execute(sql, data)
#             else:
#                 cursor.execute(sql)
#         except (AttributeError, OperationalError) as e:
#             current_app.logger.error(
#                 'Exception generated during sql query dictcursor: %s',
#                 e)
#             self.close()
#             self.connect()
#             cursor = self.conn.cursor(cursors.DictCursor)
#             cursor.execute("SET SESSION TRANSACTION "
#                            "ISOLATION LEVEL READ COMMITTED")
#             if flag == 'insert':
#                 cursor.executemany(sql, data)
#             elif data:
#                 cursor.execute(sql, data)
#             else:
#                 cursor.execute(sql)
#         return cursor

#     def close(self):
#         try:
#             if self.conn:
#                 self.conn.close()
#                 current_app.logger.info('...Closed Database Connection: %s',
#                                         str(self.conn))
#             else:
#                 current_app.logger.info('...No Database Connection to Close.')
#         except (AttributeError, OperationalError) as e:
#             current_app.logger.error(
#                 'Exception generated during sql connection close: %s',
#                 e)
#             raise e


class DB:
    """
    DB Object.
    """
    def __init__(self):
        """
        Constructor.
        """
        try:
            self.conn = mysql.connection
            self.cur = self.conn.cursor()
        except (AttributeError, OperationalError):
            current_app.logger.error(
                'Exception creating sql connection: %s %s',
                AttributeError, OperationalError)
            raise e

    @retry(stop_max_attempt_number=3)
    def query(self, sql, flag=None, data=None):
        """
        Query string.
        """
        try:
            # current_app.logger.debug('***** %s %s %s', sql, data, flag)
            cursor = self.cur
            cursor.execute("SET SESSION TRANSACTION "
                           "ISOLATION LEVEL READ COMMITTED")
            if flag == 'insert':
                # current_app.logger.debug('insert****')
                # current_app.logger.debug('*** %s %s', sql, data)
                cursor.executemany(sql, data)
            elif data:
                # current_app.logger.debug('data****')
                current_app.logger.debug('*** %s %s', sql, data)
                cursor.execute(sql, data)
            else:
                current_app.logger.debug('nodata *** %s', sql)
                cursor.execute(sql)
        except (AttributeError, OperationalError) as e:
            current_app.logger.error(
                'Exception generated during sql query: %s',
                e)
            self.cur = mysql.connection.cursor() if not self.cur \
                else self.cur.close()
            cursor = self.cur
            cursor.execute("SET SESSION TRANSACTION "
                           "ISOLATION LEVEL READ COMMITTED")
            if flag == 'insert':
                cursor.executemany(sql, data)
            elif data:
                # current_app.logger.debug('**** %s %s', sql, data)
                cursor.execute(sql, data)
            else:
                cursor.execute(sql)
        return cursor

    def query_dictcursor(self, sql, flag=None, data=None):
        # current_app.logger.debug('** %s %s', sql, data)
        return self.query(sql, flag=flag, data=data)

    def query_commit(self):
        self.conn.commit()

    def query_rollback(self):
        self.conn.rollback()

    def close(self):
        pass
        # try:
        #     if self.cur:
        #         self.cur.close()
        #         current_app.logger.info('...Closed Database Connection: %s',
        #                                 str(self.cur))
        #     else:
        #         current_app.logger.info('...No Database Connection to Close.')
        # except (AttributeError, OperationalError) as e:
        #     current_app.logger.error(
        #         'Exception generated during sql connection close: %s',
        #         e)
        #     raise e


@ems.route("/c3test")
def c3test():
    return flask.render_template("c3test.html")


@ems.context_processor
def serverdate():
    """
    When you request the root path, you'll get the index.html template.
    """
    if time.tzname[0] == 'IST':
        local_now = datetime.today()
    else:
        dest_tz = pytz.timezone('Asia/Kolkata')
        ts = time.time()
        utc_now = datetime.utcfromtimestamp(ts)
        local_now = utc_now.replace(tzinfo=pytz.utc).astimezone(dest_tz)
    # date = datetime.now()
    # dayaheaddate = datetime.now() + timedelta(days=1)
    # current_app.logger.debug(['dayaheaddate: ', dayaheaddate])
    # return dict(serverdate=date.strftime('%d-%m-%Y'),
    #             dayaheaddate=dayaheaddate.strftime('%d-%m-%Y'))
    dayaheaddate = local_now + timedelta(days=1)
    return dict(serverdate=local_now.strftime('%d-%m-%Y'),
                dayaheaddate=dayaheaddate.strftime('%d-%m-%Y'))


@login_required
@ems.context_processor
def get_org():
    if current_user.organisation_master_fk:
        # datacursor = con.cursor(cursors.DictCursor)
        db = DB()
        datacursor = db.query_dictcursor("""
            SELECT a.organisation_code, b.organisation_type_code, c.state_name
            from power.organisation_master a,
            power.organisation_type b,
            power.state_master c
            where a.organisation_type_fk = b.organisation_type_pk
            and a.state_master_fk = c.state_master_pk
            and (organisation_master_pk = %s or organisation_parent_fk = %s)
            and a.delete_ind = 0
            and b.delete_ind = 0
            and c.delete_ind = 0""", data=(current_user.organisation_master_fk,
                                         current_user.organisation_master_fk))                                        
        org_name = datacursor.fetchall()
        db.close()
    else:
        org_name = ({'organisation_code': None},
                    {'organisation_type_code': None},
                    {'state_name': None})
    org = [org.get('organisation_code') for org in org_name]
    org_type_code = [org_typ.get('organisation_type_code')
                     for org_typ in org_name][0]
    state_name = [st_nm.get('state_name')
                     for st_nm in org_name][0]
    current_app.logger.debug("######### %s %s %s %s",
                             org,
                             current_user.has_roles('admin'),
                             current_user.is_authenticated,
                             org_type_code)
    return dict(org_name=org, org_type_code=org_type_code, state_name=state_name)


@login_required
@ems.context_processor
def get_model_name():
    if current_user.organisation_master_fk:
        db = DB()
        # datacursor = con.cursor(cursors.DictCursor)
        datacursor = db.query_dictcursor("""SELECT
            a.model_name, a.model_short_name, a.model_type,
            c.organisation_code as discom
            from power.model_master a,
                 power.model_org_map b,
                 power.organisation_master c
            where a.id = b.model_master_fk
            and b.organisation_master_fk = c.organisation_master_pk
            and (c.organisation_master_pk = %s
            or c.organisation_parent_fk = %s)
            and a.delete_ind = 0
            and b.delete_ind = 0
            and c.delete_ind = 0""", data=(
            current_user.organisation_master_fk,
            current_user.organisation_master_fk))
        discom_model_name = datacursor.fetchall()
        db.close()
        current_app.logger.debug('discom_model_name %s', discom_model_name)
        return dict(model_name=tuple(discom_model_name))


@login_required
@ems.context_processor
def get_model_name_js():
    return dict(model_name_js=json.dumps(get_model_name().get('model')))


# @ems.route("/nrschedule")
# @login_required
# def index():
#     """
#     When you request the root path, you'll get the index.html template.
#     """
#     # columns = [ 'column_1', 'column_2', 'column_3', 'column_4']
#     columns = ['Date', 'Revision', 'Block_No', 'ISGS', 'LTA', 'MTOA',
#                'Shared', 'Bilateral', 'IEX_PXIL', 'Loss_Perc']
#     return flask.render_template("index.html", columns=columns)


# create an app.route for your javascript
# @ems.route("/retrieve_server_data")
# @login_required
# def get_server_data():
#     """
#     Get data from db
#     """
#     columns = ['date', 'revision', 'Block_No', 'ISGS', 'LTA', 'MTOA',
#                'Shared', 'Bilateral', 'IEX_PXIL', 'Loss_Perc']
#     index_column = "date"
#     table = "power.nrldc_state_drawl_summary_demo2"
#     where = ""
#     order = "order by date desc, revision desc, block_no "
#     cursor = con  # include a reference to your app mysqldb instance
#     logging.info('Finished collecting Data from get_server_data'
#                  ' fn or retrieve_server_data path')
#     results = DataTablesServer(request, columns, index_column,
#                                table, cursor, where, order).output_result()

#     return json.dumps(results)


# @ems.route("/market")
# @login_required
# def market():
#     """
#     When you request the root path, you'll get the index.html template.

#     """
#     db = DB()
#     # datacursor = con.cursor(cursors.DictCursor)
#     datacursor = db.query_dictcursor("""SELECT
#         ldc_name, ldc_org_name
#         from org_isgs_map a,
#         organisation_master b
#         where a.organisation_master_fk = b.organisation_master_pk
#         and (b.organisation_master_pk = %s
#         or b.organisation_parent_fk = %s)
#         and a.delete_ind = 0
#         and b.delete_ind = 0""", data=(
#         current_user.organisation_master_fk,
#         current_user.organisation_master_fk))
#     org = datacursor.fetchall()
#     db.close()
#     ldc = org[0].get('ldc_name')
#     if ldc == 'ERLDC':
#         schedule = ['Date', 'Revison', 'Discom', 'Block No', 'ISGS',
#                     'LTOA/MTOA', 'Bilateral', 'IEX', 'PXIL', 'Net Schedule',
#                     'Regulation']
#     elif ldc == 'NRLDC':
#         schedule = ['Date', 'Revison', 'Discom', 'Block No', 'ISGS',
#                     'LTOA', 'MTOA', 'Shared', 'Bilateral', 'IEX', 'PXIL',
#                     'Net Schedule']

#     market = ['Date', 'Block No', 'IEX A1', 'PXIL A1', 'IEX A2',
#               'PXIL A2', 'IEX E1', 'PXIL E1', 'IEX E2', 'PXIL E2', 'IEX N1',
#               'PXIL N1', 'IEX N2', 'PXIL N2', 'IEX N3', 'PXIL N3', 'IEX S1',
#               'PXIL S1', 'IEX S2', 'PXIL S2', 'IEX W1', 'PXIL W1', 'IEX W2',
#               'PXIL W2', 'IEX W3', 'PXIL W3']
#     return flask.render_template("schedule_market_jinja.html",
#                                  ldc_columns=schedule, market_columns=market)

@ems.route("/market")
@login_required
def market():
    """
    When you request the root path, you'll get the index.html template.

    """

    market = ['Date', 'Block No', 'IEX A1', 'PXIL A1', 'IEX A2',
              'PXIL A2', 'IEX E1', 'PXIL E1', 'IEX E2', 'PXIL E2', 'IEX N1',
              'PXIL N1', 'IEX N2', 'PXIL N2', 'IEX N3', 'PXIL N3', 'IEX S1',
              'PXIL S1', 'IEX S2', 'PXIL S2', 'IEX W1', 'PXIL W1', 'IEX W2',
              'PXIL W2', 'IEX W3', 'PXIL W3']
    return flask.render_template("schedule_market_jinja.html",
                                 market_columns=market)

# create an app.route for your javascript
# @ems.route("/get_erschedule_data")
# def get_erschedule_data():
#     """
#     Get data from db
#     """
#     columns = ['Date', 'Revision', 'Discom', 'Block_No', 'ISGS', 'LTOA_MTOA',
#                'Bilateral', 'IEX', 'PXIL', 'Net_Sch', 'Regulation']
#     index_column = "Date"
#     table = "power.vw_erldc_discom_drawl_summary_demo"
#     where = ""
#     order = ""
#     cursor = con # include a reference to your app mysqldb instance
#     print "Here"
#     #collection = [dict(zip(columns, [1,2,3,4,5,6,7])), dict(zip(columns, [5,5,5,5,5,5,5]))]
#     results = DataTablesServer(request, columns, index_column,
#                                table, cursor, where, order).output_result()

#     return json.dumps(results)


# create an app.route for your javascript
# @ems.route("/get_erschedule_data", methods=['POST'])
# @login_required
# def get_erschedule_data():
#     """
#     Get data from db
#     """
#     date = request.form['date']
#     columns = ['Date', 'Revision', 'Discom', 'Block_No', 'ISGS', 'LTOA_MTOA',
#                'Bilateral', 'IEX', 'PXIL', 'Net_Sch', 'Regulation']
#     index_column = "Date"
#     table = "power.vw_erldc_discom_drawl_summary_demo"
#     where = "where date = STR_TO_DATE('" + date + "','%d-%m-%Y')"
#     order = "order by date desc, revision desc, block_no "
#     cursor = con  # include a reference to your app mysqldb instance
#     logging.info(['Finished get_erschedule_data for date', date])
#     results = DataTablesServer(request, columns, index_column,
#                                table, cursor, where, order).output_result()
#     return json.dumps(results)


@ems.route("/get_schedule_data", methods=['POST'])
@login_required
def get_schedule_data():
    if request.json:
        date = request.json['date']
    else:
        date = request.form['date']
        # discom = request.form['discom']

    db = DB()
    # datacursor = con.cursor(cursors.DictCursor)
    datacursor = db.query_dictcursor("""SELECT
        ldc_name, ldc_org_name, organisation_code as discom
        from power.org_isgs_map a,
        power.organisation_master b
        where a.organisation_master_fk = b.organisation_master_pk
        and (b.organisation_master_pk = %s
        or b.organisation_parent_fk = %s)
        and a.delete_ind = 0
        and b.delete_ind = 0""", data=(
        current_user.organisation_master_fk,
        current_user.organisation_master_fk))
    org = datacursor.fetchall()
    ldc = org[0].get('ldc_name')
    discom = org[0].get('discom')
    current_app.logger.info("get_schedule_data %s %s %s", date, ldc, discom)
    # ldc = 'ERLDC'
    # discom = 'BSEB'
    if ldc == 'ERLDC':
        datacursor = db.query_dictcursor("""SELECT
            DATE_FORMAT(rpms.date, '%%d-%%m-%%Y') Date,
            rpms.Revision, rpms.Block_No,
            rpms.ISGS, rpms.LTOA_MTOA,
            rpms.Bilateral, rpms.IEX, rpms.PXIL,
            ROUND(COALESCE(case when rpms.INTERNAL_GENERATION_ACTUAL = 0
                     then NULL else rpms.INTERNAL_GENERATION_ACTUAL end,
                     rpms.RT_INTERNAL_GENERATION_FOR), 2)
                     INTERNAL_GENERATION,
            ROUND(rpms.ISGS + rpms.LTOA + rpms.MTOA +
            rpms.Shared + rpms.Bilateral + rpms.IEX + rpms.PXIL +
            COALESCE(case when rpms.INTERNAL_GENERATION_ACTUAL = 0
                     then NULL else rpms.INTERNAL_GENERATION_ACTUAL end,
                     rpms.RT_INTERNAL_GENERATION_FOR),2) NET_SCH,
            rpms.Regulation
            from
                (select a.date, a.block_no, a.revision,
                    sum(case when a.pool_name = 'ISGS'
                        then a.quantum else 0 end) ISGS,
                    sum(case when a.pool_name = 'LTOA_MTOA'
                        then a.quantum else 0 end) LTOA_MTOA,
                    sum(case when a.pool_name = 'Bilateral'
                        then a.quantum else 0 end) Bilateral,
                    sum(case when a.pool_name = 'IEX'
                        then a.quantum else 0 end) IEX,
                    sum(case when a.pool_name = 'PXIL'
                        then a.quantum else 0 end) PXIL,
                    sum(case when a.pool_name = 'Regulation'
                        then a.quantum else 0 end) Regulation,
                    sum(case when a.pool_name = 'INT_GENERATION_ACT'
                        then case when a.quantum is not null
                             then round(a.quantum + coalesce(a.bias, 0), 2)
                             when a.bias is not null and a.bias <> 0
                             then round(coalesce(a.quantum, 0) + a.bias, 2)
                             else null end
                        else 0 end) INTERNAL_GENERATION_ACTUAL,
                    sum(case when a.pool_name = 'INT_GENERATION_FOR'
                        then a.quantum
                        else null end) RT_INTERNAL_GENERATION_FOR,
                    sum(case when a.pool_name = 'DEMAND_ACT'
                        then case when a.quantum is not null
                             then round(a.quantum + coalesce(a.bias, 0), 2)
                             when a.bias is not null and a.bias <> 0
                             then round(coalesce(a.quantum, 0) + a.bias, 2)
                             else null end
                        else null end) DEMAND_ACTUAL,
                    sum(case when a.pool_name = 'REALTIME_DEMAND_FOR'
                        then a.quantum else 0 end) RT_DEMAND_FORECAST,
                    a.discom,
                    a.state
                from
                    power.realtime_position_map_staging a,
                    (select date, discom, state, pool_name,
                     max(revision) max_revision
                     from power.realtime_position_map_staging
                     where date = str_to_date(%s, '%%d-%%m-%%Y')
                     and discom = %s
                     group by date, discom, state, pool_name) b
                where a.date = b.date
                and a.discom = b.discom
                and a.state = b.state
                and a.pool_name = b.pool_name
                and a.revision = b.max_revision
                group by a.date, a.block_no, a.revision, a.discom, a.state) rpms""",
                                         data=(date, discom))
        columns = ['Date', 'Revision', 'Block_No', 'ISGS',
                   'LTOA_MTOA', 'Bilateral', 'IEX', 'PXIL',
                   'INTERNAL_GENERATION', 'NET_SCH',
                   'Regulation']
    elif ldc == 'WRLDC':
        datacursor = db.query_dictcursor("""SELECT
        DATE_FORMAT(rpms.date, '%%d-%%m-%%Y') Date,
        rpms.Revision, rpms.Block_No,
        rpms.ISGS, rpms.LTOA, rpms.MTOA, rpms.STOA,
        rpms.URS, rpms.RRAS, rpms.IEX, rpms.PXIL,
        @int_gen_oth := ROUND(COALESCE(case when
                 rpms.INTERNAL_GENERATION_OTH = 0
                 then NULL else rpms.INTERNAL_GENERATION_OTH end,
                 rpms.INTERNAL_GENERATION_OTH_SCH), 2)
                  INT_GEN_OTH,
        @int_gen_conv := ROUND(COALESCE(case when
                 rpms.INTERNAL_GENERATION_CONV = 0
                 then NULL else rpms.INTERNAL_GENERATION_CONV end,
                 rpms.RT_INTERNAL_GENERATION_CONV_FOR,
                 rpms.INTERNAL_GENERATION_CONV_SCH), 2)
                  INT_GEN_CONV,
        @int_gen_wind := ROUND(COALESCE(case when
                 rpms.INTERNAL_GENERATION_WIND = 0
                 then NULL else rpms.INTERNAL_GENERATION_WIND end,
                 rpms.RT_INTERNAL_GENERATION_WIND_FOR), 2)
                  INT_GEN_WIND,
        @int_gen_solar := ROUND(COALESCE(case when
                 rpms.INTERNAL_GENERATION_SOLAR = 0
                 then NULL else rpms.INTERNAL_GENERATION_SOLAR end,
                 rpms.RT_INTERNAL_GENERATION_SOLAR_FOR), 2)
                  INT_GEN_SOLAR,
        ROUND(rpms.ISGS + rpms.LTOA + rpms.MTOA + rpms.STOA + rpms.URS +
        rpms.RRAS + rpms.IEX + rpms.PXIL +
        @int_gen_oth + @int_gen_conv + @int_gen_wind +
        @int_gen_solar, 2) NET_SCH
        from
            (select a.date, a.block_no, a.revision,
                sum(case when a.pool_name = 'ISGS'
                    then a.quantum else 0 end) ISGS,
                sum(case when a.pool_name = 'LTA'
                    then a.quantum else 0 end) LTOA,
                sum(case when a.pool_name = 'MTOA'
                    then a.quantum else 0 end) MTOA,
                sum(case when a.pool_name = 'STOA'
                    then a.quantum else 0 end) STOA,
                sum(case when a.pool_name = 'URS'
                    then a.quantum else 0 end) URS,
                sum(case when a.pool_name = 'RRAS'
                    then a.quantum else 0 end) RRAS,
                sum(case when a.pool_name = 'IEX'
                    then a.quantum else 0 end) IEX,
                sum(case when a.pool_name = 'PXIL'
                    then a.quantum else 0 end) PXIL,
                sum(case when a.pool_name = 'INT_GENERATION_ACT'
                          and a.pool_type = 'OTHERS'
                    then case when a.quantum is not null
                         then round(a.quantum + coalesce(a.bias, 0), 2)
                         when a.bias is not null and a.bias <> 0
                         then round(coalesce(a.quantum, 0) + a.bias, 2)
                         else null end
                    else 0 end) INTERNAL_GENERATION_OTH,
                sum(case when a.pool_name = 'INT_GENERATION_SCH'
                          and a.pool_type = 'OTHERS'
                    then case when a.quantum is not null
                         then round(a.quantum + coalesce(a.bias, 0), 2)
                         when a.bias is not null and a.bias <> 0
                         then round(coalesce(a.quantum, 0) + a.bias, 2)
                         else null end
                    else 0 end) INTERNAL_GENERATION_OTH_SCH,
                sum(case when a.pool_name = 'INT_GENERATION_ACT'
                          and a.pool_type = 'CONVENTIONAL'
                    then case when a.quantum is not null
                         then round(a.quantum + coalesce(a.bias, 0), 2)
                         when a.bias is not null and a.bias <> 0
                         then round(coalesce(a.quantum, 0) + a.bias, 2)
                         else null end
                    else 0 end) INTERNAL_GENERATION_CONV,
                sum(case when a.pool_name = 'INT_GENERATION_SCH'
                          and a.pool_type = 'CONVENTIONAL'
                    then case when a.quantum is not null
                         then round(a.quantum + coalesce(a.bias, 0), 2)
                         when a.bias is not null and a.bias <> 0
                         then round(coalesce(a.quantum, 0) + a.bias, 2)
                         else null end
                    else 0 end) INTERNAL_GENERATION_CONV_SCH,
                sum(case when a.pool_name = 'INT_GENERATION_ACT'
                          and a.pool_type = 'WIND'
                    then case when a.quantum is not null
                         then round(a.quantum + coalesce(a.bias, 0), 2)
                         when a.bias is not null and a.bias <> 0
                         then round(coalesce(a.quantum, 0) + a.bias, 2)
                         else null end
                    else 0 end) INTERNAL_GENERATION_WIND,
                sum(case when a.pool_name = 'INT_GENERATION_SCH'
                          and a.pool_type = 'WIND'
                    then case when a.quantum is not null
                         then round(a.quantum + coalesce(a.bias, 0), 2)
                         when a.bias is not null and a.bias <> 0
                         then round(coalesce(a.quantum, 0) + a.bias, 2)
                         else null end
                    else 0 end) INTERNAL_GENERATION_WIND_SCH,
                sum(case when a.pool_name = 'INT_GENERATION_ACT'
                          and a.pool_type = 'SOLAR'
                    then case when a.quantum is not null
                         then round(a.quantum + coalesce(a.bias, 0), 2)
                         when a.bias is not null and a.bias <> 0
                         then round(coalesce(a.quantum, 0) + a.bias, 2)
                         else null end
                    else 0 end) INTERNAL_GENERATION_SOLAR,
                sum(case when a.pool_name = 'INT_GENERATION_SCH'
                          and a.pool_type = 'SOLAR'
                    then case when a.quantum is not null
                         then round(a.quantum + coalesce(a.bias, 0), 2)
                         when a.bias is not null and a.bias <> 0
                         then round(coalesce(a.quantum, 0) + a.bias, 2)
                         else null end
                    else 0 end) INTERNAL_GENERATION_SOLAR_SCH,
                sum(case when a.pool_name = 'INT_GENERATION_FOR'
                    and a.pool_type = 'CONVENTIONAL'
                    then a.quantum
                    else null end) RT_INTERNAL_GENERATION_CONV_FOR,
                sum(case when a.pool_name = 'INT_GENERATION_FOR'
                    and a.pool_type ='WIND'
                    then a.quantum
                    else null end) RT_INTERNAL_GENERATION_WIND_FOR,
                sum(case when a.pool_name = 'INT_GENERATION_FOR'
                    and a.pool_type ='SOLAR'
                    then a.quantum
                    else null end) RT_INTERNAL_GENERATION_SOLAR_FOR,
                sum(case when a.pool_name = 'DEMAND_ACT'
                    then case when a.quantum is not null
                         then round(a.quantum + coalesce(a.bias, 0), 2)
                         when a.bias is not null and a.bias <> 0
                         then round(coalesce(a.quantum, 0) + a.bias, 2)
                         else null end
                    else null end) DEMAND_ACTUAL,
                sum(case when a.pool_name = 'REALTIME_DEMAND_FOR'
                    then a.quantum else 0 end) RT_DEMAND_FORECAST,
                a.discom,
                a.state
            from
                power.realtime_position_map_staging a,
                (select date, discom, state, pool_name, pool_type, block_no,
                 max(revision) max_revision
                 from power.realtime_position_map_staging
                 where date = str_to_date(%s, '%%d-%%m-%%Y')
                 and discom = %s
                 group by date, discom, state, pool_name,
                 pool_type, block_no) b
            where a.date = b.date
            and a.discom = b.discom
            and a.state = b.state
            and a.pool_name = b.pool_name
            and a.pool_type = b.pool_type
            and a.block_no = b.block_no
            and a.revision = b.max_revision
            group by a.date, a.block_no, a.discom, a.state) rpms""",
                                         data=(date, discom))
        columns = ['Date', 'Revision', 'Block_No', 'ISGS',
                   'LTOA', 'MTOA', 'STOA', 'URS', 'RRAS', 'IEX', 'PXIL',
                   'INT_GEN_OTH', 'INT_GEN_CONV', 'INT_GEN_WIND',
                   'INT_GEN_SOLAR', 'NET_SCH']
    elif ldc == 'NRLDC':
        datacursor = db.query_dictcursor("""SELECT
            DATE_FORMAT(rpms.date, '%%d-%%m-%%Y') Date,
            rpms.Revision, rpms.Block_No,
            rpms.ISGS, rpms.LTOA, rpms.MTOA, rpms.Shared,
            rpms.Bilateral, rpms.IEX, rpms.PXIL,
            ROUND(COALESCE(case when rpms.INTERNAL_GENERATION_ACTUAL = 0
                     then NULL else rpms.INTERNAL_GENERATION_ACTUAL end,
                     rpms.RT_INTERNAL_GENERATION_FOR), 2) INTERNAL_GENERATION,
            ROUND(rpms.ISGS + rpms.LTOA + rpms.MTOA +
            rpms.Shared + rpms.Bilateral + rpms.IEX + rpms.PXIL +
            COALESCE(case when rpms.INTERNAL_GENERATION_ACTUAL = 0
                     then NULL else rpms.INTERNAL_GENERATION_ACTUAL end,
                     rpms.RT_INTERNAL_GENERATION_FOR),2) NET_SCH
            from
                (select a.date, a.block_no, a.revision,
                    sum(case when a.pool_name = 'ISGS'
                        then a.quantum else 0 end) ISGS,
                    sum(case when a.pool_name = 'LTOA'
                        then a.quantum else 0 end) LTOA,
                    sum(case when a.pool_name = 'MTOA'
                        then a.quantum else 0 end) MTOA,
                    sum(case when a.pool_name = 'Bilateral'
                        then a.quantum else 0 end) Bilateral,
                    sum(case when a.pool_name = 'Shared'
                        then a.quantum else 0 end) Shared,
                    sum(case when a.pool_name = 'IEX'
                        then a.quantum else 0 end) IEX,
                    sum(case when a.pool_name = 'PXIL'
                        then a.quantum else 0 end) PXIL,
                    sum(case when a.pool_name = 'INT_GENERATION_ACT'
                        then case when a.quantum is not null
                             then round(a.quantum + coalesce(a.bias, 0), 2)
                             when a.bias is not null and a.bias <> 0
                             then round(coalesce(a.quantum, 0) + a.bias, 2)
                             else null end
                        else 0 end) INTERNAL_GENERATION_ACTUAL,
                    sum(case when a.pool_name = 'INT_GENERATION_FOR'
                        then a.quantum
                        else null end) RT_INTERNAL_GENERATION_FOR,
                    sum(case when a.pool_name = 'DEMAND_ACT'
                        then case when a.quantum is not null
                             then round(a.quantum + coalesce(a.bias, 0), 2)
                             when a.bias is not null and a.bias <> 0
                             then round(coalesce(a.quantum, 0) + a.bias, 2)
                             else null end
                        else null end) DEMAND_ACTUAL,
                    sum(case when a.pool_name = 'REALTIME_DEMAND_FOR'
                        then a.quantum else 0 end) RT_DEMAND_FORECAST,
                    a.discom,
                    a.state
                from
                    power.realtime_position_map_staging a,
                    (select date, discom, state, pool_name,
                     max(revision) max_revision
                     from power.realtime_position_map_staging
                     where date = str_to_date(%s, '%%d-%%m-%%Y')
                     and discom = %s
                     group by date, discom, state, pool_name) b
                where a.date = b.date
                and a.discom = b.discom
                and a.state = b.state
                and a.pool_name = b.pool_name
                and a.revision = b.max_revision
                group by a.date, a.block_no, a.revision, a.discom, a.state) rpms""",
                                         data=(date, discom))
        columns = ['Date', 'Revision', 'Block_No', 'ISGS',
                   'LTOA', 'MTOA', 'Shared', 'Bilateral', 'IEX', 'PXIL',
                   'INTERNAL_GENERATION', 'NET_SCH']
    results = datacursor.fetchall()
    # datacursor = db.query_dictcursor("""SELECT FOUND_ROWS() as rows""")
    # rows = datacursor.fetchall()[0]['rows']
    # db.close()
    # current_app.logger.info('Total Number of Rows: %s', rows)
    # current_app.logger.debug('Result: %s', results)
    # dt_output = rawsql_to_datatables(columns, results, rows, rows)
    # print json.dumps(output)
    # return json.dumps(dt_output)
    return json.dumps([results, columns], use_decimal=True)


# @ems.route("/nrmarket")
# @login_required
# def nrmarket():
#     """
#     When you request the root path, you'll get the index.html template.
#     """
#     #columns = [ 'column_1', 'column_2', 'column_3', 'column_4']
#     columns = ['Delivery_Date', 'Block_No', 'IEX_Rate', 'PXIL_Rate']
#     return flask.render_template("nrmarket.html", columns=columns)


# @ems.route("/get_nrmarket_data")
# @login_required
# def get_nrmarket_data():
#     """
#     Get data from db
#     """
#     columns = ['Delivery_Date', 'Block', 'IEX_Rate', 'PXIL_Rate']
#     index_column = "Delivery_Date"
#     table = "power.exchange_price_summary"
#     where = "where delivery_date = date(sysdate())"
#     order = "order by delivery_date desc, block "
#     cursor = con # include a reference to your app mysqldb instance
#     logging.info('Finished collecting Data from get_nrmarket_data fn')
#     #collection = [dict(zip(columns, [1,2,3,4,5,6,7])), dict(zip(columns, [5,5,5,5,5,5,5]))]
#     results = DataTablesServer(request, columns, index_column,
#                                table, cursor, where, order).output_result()
#     #print "Here2",results
#     # return the results as json # import json
#     #results = {"aaData": [["1", "2", "3", "4"], ["5", "5", "5", "5"]]}
#     return json.dumps(results)


# @ems.route("/ermarket")
# @login_required
# def ermarket():
#     """
#     When you request the root path, you'll get the index.html template.
#     """
#     #columns = [ 'column_1', 'column_2', 'column_3', 'column_4']
#     columns = ['Delivery_Date', 'Block_No', 'IEX_Rate', 'PXIL_Rate']
#     return flask.render_template("ermarket.html", columns=columns)


# @ems.route("/get_ermarket_data")
# @login_required
# def get_ermarket_data():
#     """
#     Get data from db
#     """
#     columns = ['Delivery_Date', 'Block', 'IEX_Rate', 'PXIL_Rate']
#     index_column = "Delivery_Date"
#     table = "power.e1exchange_price_summary"
#     where = "where delivery_date = date(sysdate())"
#     order = "order by delivery_date desc, block "
#     cursor = con # include a reference to your app mysqldb instance
#     logging.info('Finished collecting Data from get_ermarket_data fn')
#     #collection = [dict(zip(columns, [1,2,3,4,5,6,7])), dict(zip(columns, [5,5,5,5,5,5,5]))]
#     results = DataTablesServer(request, columns, index_column,
#                                table, cursor, where, order).output_result()
#     #print "Here2",results
#     # return the results as json # import json
#     #results = {"aaData": [["1", "2", "3", "4"], ["5", "5", "5", "5"]]}
#     return json.dumps(results)


# @ems.context_processor
# def marketcols():
#     """
#     When you request the root path, you'll get the index.html template.
#     """
#     #columns = [ 'column_1', 'column_2', 'column_3', 'column_4']
#     columns = ['Delivery Date', 'Block No', 'IEX A1', 'PXIL A1', 'IEX A2', 'PXIL A2',
#                'IEX E1', 'PXIL E1', 'IEX E2', 'PXIL E2', 'IEX N1', 'PXIL N1', 'IEX N2', 'PXIL N2',
#                'IEX N3', 'PXIL N3', 'IEX S1', 'PXIL S1', 'IEX S2', 'PXIL S2', 'IEX W1', 'PXIL W1',
#                'IEX W2', 'PXIL W2', 'IEX W3', 'PXIL W3']
#     return dict(market_columns=columns)


# @ems.route("/market")
# @login_required
# def market():
#     """
#     When you request the root path, you'll get the index.html template.
#     """
#     #columns = [ 'column_1', 'column_2', 'column_3', 'column_4']
#     columns = ['Delivery Date', 'Block No', 'IEX A1', 'PXIL A1', 'IEX A2', 'PXIL A2',
#                'IEX E1', 'PXIL E1', 'IEX E2', 'PXIL E2', 'IEX N1', 'PXIL N1', 'IEX N2', 'PXIL N2',
#                'IEX N3', 'PXIL N3', 'IEX S1', 'PXIL S1', 'IEX S2', 'PXIL S2', 'IEX W1', 'PXIL W1',
#                'IEX W2', 'PXIL W2', 'IEX W3', 'PXIL W3']
#     return flask.render_template("market.html", columns=columns)


# @ems.route("/get_market_data")
# @ems.route("/get_market_data", methods=['POST'])
# @login_required
# def get_market_data():
#     """
#     Get data from db
#     """
#     date = request.form['date']
#     columns = ['Delivery_Date', 'Block', 'IEX_A1', 'PXIL_A1', 'IEX_A2', 'PXIL_A2',
#                'IEX_E1', 'PXIL_E1', 'IEX_E2', 'PXIL_E2', 'IEX_N1', 'PXIL_N1', 'IEX_N2', 'PXIL_N2',
#                'IEX_N3', 'PXIL_N3', 'IEX_S1', 'PXIL_S1', 'IEX_S2', 'PXIL_S2', 'IEX_W1', 'PXIL_W1',
#                'IEX_W2', 'PXIL_W2', 'IEX_W3', 'PXIL_W3']
#     index_column = "Delivery_Date"
#     table = "power.vw_market_price"
#     # where = "where delivery_date = date(sysdate())"
#     where = "where delivery_date = STR_TO_DATE('" + date + "','%d-%m-%Y')"
#     order = "order by delivery_date desc, block "
#     cursor = con # include a reference to your app mysqldb instance
#     logging.info('Finished collecting Data from get_market_data fn')
#     #collection = [dict(zip(columns, [1,2,3,4,5,6,7])), dict(zip(columns, [5,5,5,5,5,5,5]))]
#     results = DataTablesServer(request, columns, index_column,
#                                table, cursor, where, order).output_result()
#     #print "Here2",results
#     # return the results as json # import json
#     #results = {"aaData": [["1", "2", "3", "4"], ["5", "5", "5", "5"]]}
#     return json.dumps(results)

@ems.route("/get_market_data", methods=['POST'])
@login_required
def get_market_data():
    """
    Get data from db
    """
    date = request.form['date']
    current_app.logger.info('Market Date %s', date)
    columns = ['Delivery_Date', 'Block', 'IEX_A1', 'PXIL_A1', 'IEX_A2',
               'PXIL_A2', 'IEX_E1', 'PXIL_E1', 'IEX_E2', 'PXIL_E2', 'IEX_N1',
               'PXIL_N1', 'IEX_N2', 'PXIL_N2', 'IEX_N3', 'PXIL_N3', 'IEX_S1',
               'PXIL_S1', 'IEX_S2', 'PXIL_S2', 'IEX_W1', 'PXIL_W1', 'IEX_W2',
               'PXIL_W2', 'IEX_W3', 'PXIL_W3']
    db = DB()
    # datacursor = con.cursor(cursors.DictCursor)
    datacursor = db.query_dictcursor("""SELECT DATE_FORMAT(Delivery_Date, '%%d-%%m-%%Y') AS Delivery_Date,
        Block AS Block,
        SUM((CASE
            WHEN
                (Exchange_Name = 'IEX')
            THEN
                ROUND((A1_Price / 1000), 2)
        END)) AS IEX_A1,
        SUM((CASE
            WHEN
                (Exchange_Name = 'PXIL')
            THEN
                ROUND((A1_Price / 1000), 2)
        END)) AS PXIL_A1,
        SUM((CASE
            WHEN
                (Exchange_Name = 'IEX')
            THEN
                ROUND((A2_Price / 1000), 2)
        END)) AS IEX_A2,
        SUM((CASE
            WHEN
                (Exchange_Name = 'PXIL')
            THEN
                ROUND((A2_Price / 1000), 2)
        END)) AS PXIL_A2,
        SUM((CASE
            WHEN
                (Exchange_Name = 'IEX')
            THEN
                ROUND((E1_Price / 1000), 2)
        END)) AS IEX_E1,
        SUM((CASE
            WHEN
                (Exchange_Name = 'PXIL')
            THEN
                ROUND((E1_Price / 1000), 2)
        END)) AS PXIL_E1,
        SUM((CASE
            WHEN
                (Exchange_Name = 'IEX')
            THEN
                ROUND((E2_Price / 1000), 2)
        END)) AS IEX_E2,
        SUM((CASE
            WHEN
                (Exchange_Name = 'PXIL')
            THEN
                ROUND((E2_Price / 1000), 2)
        END)) AS PXIL_E2,
        SUM((CASE
            WHEN
                (Exchange_Name = 'IEX')
            THEN
                ROUND((N1_Price / 1000), 2)
        END)) AS IEX_N1,
        SUM((CASE
            WHEN
                (Exchange_Name = 'PXIL')
            THEN
                ROUND((N1_Price / 1000), 2)
        END)) AS PXIL_N1,
        SUM((CASE
            WHEN
                (Exchange_Name = 'IEX')
            THEN
                ROUND((N2_Price / 1000), 2)
        END)) AS IEX_N2,
        SUM((CASE
            WHEN
                (Exchange_Name = 'PXIL')
            THEN
                ROUND((N2_Price / 1000), 2)
        END)) AS PXIL_N2,
        SUM((CASE
            WHEN
                (Exchange_Name = 'IEX')
            THEN
                ROUND((N3_Price / 1000), 2)
        END)) AS IEX_N3,
        SUM((CASE
            WHEN
                (Exchange_Name = 'PXIL')
            THEN
                ROUND((N3_Price / 1000), 2)
        END)) AS PXIL_N3,
        SUM((CASE
            WHEN
                (Exchange_Name = 'IEX')
            THEN
                ROUND((S1_Price / 1000), 2)
        END)) AS IEX_S1,
        SUM((CASE
            WHEN
                (Exchange_Name = 'PXIL')
            THEN
                ROUND((S1_Price / 1000), 2)
        END)) AS PXIL_S1,
        SUM((CASE
            WHEN
                (Exchange_Name = 'IEX')
            THEN
                ROUND((S2_Price / 1000), 2)
        END)) AS IEX_S2,
        SUM((CASE
            WHEN
                (Exchange_Name = 'PXIL')
            THEN
                ROUND((S2_Price / 1000), 2)
        END)) AS PXIL_S2,
        SUM((CASE
            WHEN
                (Exchange_Name = 'IEX')
            THEN
                ROUND((W1_Price / 1000), 2)
        END)) AS IEX_W1,
        SUM((CASE
            WHEN
                (Exchange_Name = 'PXIL')
            THEN
                ROUND((W1_Price / 1000), 2)
        END)) AS PXIL_W1,
        SUM((CASE
            WHEN
                (Exchange_Name = 'IEX')
            THEN
                ROUND((W2_Price / 1000), 2)
        END)) AS IEX_W2,
        SUM((CASE
            WHEN
                (Exchange_Name = 'PXIL')
            THEN
                ROUND((W2_Price / 1000), 2)
        END)) AS PXIL_W2,
        SUM((CASE
            WHEN
                (Exchange_Name = 'IEX')
            THEN
                ROUND((W3_Price / 1000), 2)
        END)) AS IEX_W3,
        SUM((CASE
            WHEN
                (Exchange_Name = 'PXIL')
            THEN
                ROUND((W3_Price / 1000), 2)
        END)) AS PXIL_W3
    FROM power.exchange_areaprice_stg
    WHERE delivery_date = STR_TO_DATE(%s, "%%d-%%m-%%Y")
    GROUP BY Delivery_Date , Block""", data=(date,))

    results = datacursor.fetchall()
    datacursor = db.query_dictcursor("""SELECT FOUND_ROWS() _rows""")
    rows = datacursor.fetchall()[0]['_rows']
    db.close()
    current_app.logger.info('Total Number of Rows: %s', rows)
    current_app.logger.debug('Result: %s', results)
    dt_output = rawsql_to_datatables(columns, results, rows, rows)
    return json.dumps(dt_output)


# @ems.route("/rajlive")
# def rajlive():
#     """
#     When you request the root path, you'll get the index.html template.
#     """
#     #columns = [ 'column_1', 'column_2', 'column_3', 'column_4']
#     columns = ['Date', 'Block_No', 'Wind', 'Others', 'CPP', 'OA', 'OwnGen']
#     return flask.render_template("livedata.html", columns=columns)


# @ems.route("/get_rajlive_data")
# def get_rajlive_data():
#     """
#     Get data from db
#     """
#     columns = ['date', 'block_no', 'mw_wind', 'mw_others',
#                'mw_cpp', 'mw_oa', 'mw_own_gen']
#     index_column = "date"
#     table = "power.sldc_scada_snapshot_agg_live"
#     where = ""
#     order = ""
#     cursor = con # include a reference to your app mysqldb instance
#     logging.info('Finished collecting Data from get_rajlive_data fn')
#     #collection = [dict(zip(columns, [1,2,3,4,5,6,7])), dict(zip(columns, [5,5,5,5,5,5,5]))]
#     results = DataTablesServer(request, columns, index_column,
#                                table, cursor, where, order).output_result()
#     #print "Here2",results
#     # return the results as json # import json
#     #results = {"aaData": [["1", "2", "3", "4"], ["5", "5", "5", "5"]]}
#     return json.dumps(results)


# @ems.route("/rajmislive")
# def rajmislive():
#     """
#     When you request the root path, you'll get the index.html template.
#     """
#     #columns = [ 'column_1', 'column_2', 'column_3', 'column_4']
#     columns = ['Date', 'Block_No', 'Frequency', 'UI Rate', 'OD/UD', 'Drawl']
#     return flask.render_template("livemisdata.html", columns=columns)


# @ems.route("/get_rajmislive_data")
# def get_rajmislive_data():
#     """
#     Get data from db
#     """
#     columns = ['date', 'block_no', 'frequency', 'ui_rate',
#                'nr_od_ud', 'drawl']
#     index_column = "date"
#     table = "power.vw_sldc_scada_mis_w_blk_live"
#     where = ""
#     order = ""
#     cursor = con # include a reference to your app mysqldb instance
#     logging.info('Finished collecting Data from get_rajmislive_data fn')
#     #collection = [dict(zip(columns, [1,2,3,4,5,6,7])), dict(zip(columns, [5,5,5,5,5,5,5]))]
#     results = DataTablesServer(request, columns, index_column,
#                                table, cursor, where, order).output_result()
#     #print "Here2",results
#     # return the results as json # import json
#     #results = {"aaData": [["1", "2", "3", "4"], ["5", "5", "5", "5"]]}
#     return json.dumps(results)


# @ems.context_processor
# def biharmislivecolumn():
#     """
#     When you request the root path, you'll get the index.html template.
#     """

#     columns = ['Date', 'Block No', 'Revison', 'KBUNL', 'ISGS', 'BILATERAL',
#                'LTOA/MTOA', 'IEX', 'PXIL', 'Frequency', 'UI', 'OD/UD',
#                'Schedule', 'Forecast NBPDCL', 'Forecast SBPDCL',
#                'Forecast BPDCL', 'Live Demand NBPDCL', 'Live Forecast NBPDCL',
#                'Live Demand SBPDCL', 'Live Forecast SBPDCL',
#                'Live Demand BPDCL', 'Live Forecast BPDCL',
#                'Live Deficit/Surplus', 'Live Revised Position']
#     return dict(rtd_columns=columns)


# @ems.route("/biharmislive")
# @login_required
# def biharmislive():
#     """
#     When you request the root path, you'll get the index.html template.
#     """
#     columns = ['Date', 'Block No', 'Revison', 'KBUNL', 'ISGS', 'BILATERAL',
#                'LTOA/MTOA', 'IEX', 'PXIL', 'Frequency', 'UI', 'OD/UD',
#                'Schedule', 'Forecast NBPDCL', 'Forecast SBPDCL',
#                'Forecast BPDCL', 'Live Demand NBPDCL', 'Live Forecast NBPDCL',
#                'Live Demand SBPDCL', 'Live Forecast SBPDCL',
#                'Live Demand BPDCL', 'Live Forecast BPDCL',
#                'Live Deficit/Surplus', 'Live Revised Position']
#     # return flask.render_template("biharmislive.html", columns=columns)
#     return flask.render_template("biharmislive_jinja.html", columns=columns)
#     # return dict(columns=columns)


# @ems.route("/get_biharmislive_data")
# @ems.route("/get_biharmislive_data", methods=['POST'])
# @login_required
# def get_biharmislive_data():
#     """
#     Get data from db
#     """
#     date = request.form['date']
#     current_app.logger.info('Start get_biharmislive_data %s', date)
#     columns = ['date', 'block_no', 'revison', 'kbunl', 'isgs', 'bilateral',
#                'ltoa_mtoa', 'iex', 'pxil', 'frequency', 'ui', 'odud',
#                'schedule', 'forecast_NBPDCL', 'forecast_SBPDCL',
#                'forecast_BPDCL', 'live_demand_NBPDCL', 'live_forecast_NBPDCL',
#                'live_demand_SBPDCL', 'live_forecast_SBPDCL',
#                'live_demand_BPDCL', 'live_forecast_BPDCL',
#                'deficit_surplus_BPDCL', 'revised_position_BPDCL']
#     # index_column = "date"
#     # table = "power.bseb_real_time_forecast"
#     # # where = "where date = date(sysdate())"
#     # where = "where date = STR_TO_DATE('" + date + "','%d-%m-%Y')"
#     # order = "order by date desc, revison desc, block_no"
#     # db = DB()
#     # cursor = db.cur  # include a reference to your app mysqldb instance
#     # current_app.logger.info('Finished get_biharmislive_data fn for date: %s',
#     #                         date)
#     # results = DataTablesServer(request, columns, index_column,
#     #                            table, cursor, where, order).output_result()
#     # db.close()
#     # return json.dumps(results)
#     db = DB()
#     datacursor = db.query_dictcursor("""SELECT
#         DATE_FORMAT(date, '%%d-%%m-%%Y') date,
#         block_no, revison, kbunl, isgs, bilateral,
#         ltoa_mtoa, iex, pxil, frequency, ui, odud,
#         schedule, forecast_NBPDCL, forecast_SBPDCL,
#         forecast_BPDCL, live_demand_NBPDCL, live_forecast_NBPDCL,
#         live_demand_SBPDCL, live_forecast_SBPDCL,
#         live_demand_BPDCL, live_forecast_BPDCL,
#         deficit_surplus_BPDCL, revised_position_BPDCL
#         from bseb_real_time_forecast
#         where date = str_to_date(%s, '%%d-%%m-%%Y')
#         order by date desc, revison desc, block_no""", data=(date,))
#     results = datacursor.fetchall()
#     datacursor = db.query_dictcursor("""SELECT FOUND_ROWS() as rows""")
#     rows = datacursor.fetchall()[0]['rows']
#     db.close()
#     current_app.logger.info('Total Number of Rows: %s', rows)
#     current_app.logger.debug('Result: %s', results)
#     dt_output = rawsql_to_datatables(columns, results, rows, rows)
#     return json.dumps(dt_output)

@ems.context_processor
def livedefaultvalues(date=None):
    """
    When you request the root path, you'll get the index.html template.
    """
    date_today = date if date else serverdate().get('serverdate')
    db = DB()
    datacursor = db.query_dictcursor("""SELECT
        b.state_name, c.organisation_code
        from power.state_master b,
             power.organisation_master c
        where c.state_master_fk = b.state_master_pk
        and b.delete_ind = 0
        and c.delete_ind = 0
        and (c.organisation_master_pk = %s
        or c.organisation_parent_fk = %s)""", data=(
        current_user.organisation_master_fk,
        current_user.organisation_master_fk))
    org = datacursor.fetchall()
    # ldc = org[0].get('ldc_name')
    # state = org[0].get('state_name')
    discom = org[0].get('organisation_code')
    # current_app.logger.info('livedefaultvalues Discom %s', discom)
    # datacursor = db.query_dictcursor("""SELECT
    #     distinct 'Generation' type, c.model_short_name, b.mrr
    #     from
    #     (select max(load_date) max_load_date
    #     from realtime_forecast_staging a,
    #       model_master b
    #     where a.model_master_fk = b.id
    #     and a.discom =  %s
    #     and a.date = str_to_date(%s, '%%d-%%m-%%Y')
    #     and b.model_type = 'INJECTION'
    #     and b.delete_ind = 0) a,
    #     power.realtime_forecast_staging b,
    #     model_master c
    #     where a.max_load_date = b.load_date
    #     and b.model_master_fk = c.id
    #     union all
    #     select 'Generation' type, 'HYBRID' model_short_name, 0.2 mrr
    #     union all
    #     select distinct 'Demand' type, c.model_short_name, b.mrr
    #     from
    #     (select max(load_date) max_load_date
    #     from realtime_forecast_staging a,
    #       model_master b
    #     where a.model_master_fk = b.id
    #     and a.discom =  %s
    #     and a.date = str_to_date(%s, '%%d-%%m-%%Y')
    #     and b.model_type = 'SINK'
    #     and b.delete_ind = 0) a,
    #     power.realtime_forecast_staging b,
    #     model_master c
    #     where a.max_load_date = b.load_date
    #     and b.model_master_fk = c.id
    #     union all
    #     select 'Demand' type, 'HYBRID' model_short_name, 0.2 mrr""",
    #                                  data=(discom, date_today,
    #                                        discom, date_today))
    datacursor = db.query_dictcursor("""SELECT
        'Generation' type, c.model_short_name, b.mrr
        from
        (select max(a.Realtime_Forecast_Metadata_PK) id
        from `power`.`realtime_forecast_metadata` a,
        power.model_master b
        where a.model_master_fk = b.id
        and a.discom =  %s
        and a.date <= str_to_date(%s, '%%d-%%m-%%Y')
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
        and a.discom =  %s
        and a.date <= str_to_date(%s, '%%d-%%m-%%Y')
        and b.model_type = 'SINK'
        and b.delete_ind = 0) a,
        `power`.`realtime_forecast_metadata` b,
        power.model_master c
        where a.id = b.Realtime_Forecast_Metadata_PK
        and   c.id = b.model_master_fk
        union all
        select 'Demand' type, 'HYBRID' model_short_name, 0.2 mrr""",
                                     data=(discom, date_today,
                                           discom, date_today))
    results = datacursor.fetchall()
    db.close()
    current_app.logger.debug('livedefaultvalues results %s', results)
    # Get the 1st element of Generation or Demand type
    type_hold = ''
    results_keep = []
    for ele in results:
        if ele.get('type') != type_hold:
            results_keep.append(ele)
            type_hold = ele.get('type')
    current_app.logger.debug('livedefaultvalues results_keep %s', results_keep)
    return dict(def_model_mrr=results_keep)
    return json.dumps(results_keep, use_decimal=True)


@ems.route("/def_mrr_model_values/<date>")
@login_required
def def_mrr_model_values(date):
    """
    When you request the root path, you'll get the index.html template.
    """
    date_today = date if date else serverdate().get('serverdate')
    db = DB()
    datacursor = db.query_dictcursor("""SELECT
        b.state_name, c.organisation_code
        from power.state_master b,
             power.organisation_master c
        where c.state_master_fk = b.state_master_pk
        and b.delete_ind = 0
        and c.delete_ind = 0
        and (c.organisation_master_pk = %s
        or c.organisation_parent_fk = %s)""", data=(
        current_user.organisation_master_fk,
        current_user.organisation_master_fk))
    org = datacursor.fetchall()
    # ldc = org[0].get('ldc_name')
    # state = org[0].get('state_name')
    discom = org[0].get('organisation_code')
    # current_app.logger.info('livedefaultvalues Discom %s', discom)
    # datacursor = db.query_dictcursor("""SELECT
    #     distinct 'Generation' type, c.model_short_name, b.mrr
    #     from
    #     (select max(load_date) max_load_date
    #     from realtime_forecast_staging a,
    #       model_master b
    #     where a.model_master_fk = b.id
    #     and a.discom =  %s
    #     and a.date <= str_to_date(%s, '%%d-%%m-%%Y')
    #     and b.model_type = 'INJECTION'
    #     and b.delete_ind = 0) a,
    #     power.realtime_forecast_staging b,
    #     model_master c
    #     where a.max_load_date = b.load_date
    #     and b.model_master_fk = c.id
    #     union all
    #     select  'Generation' type, 'HYBRID' model_short_name, 0.2 mrr
    #     union all
    #     select distinct 'Demand' type, c.model_short_name, b.mrr
    #     from
    #     (select max(load_date) max_load_date
    #     from realtime_forecast_staging a,
    #       model_master b
    #     where a.model_master_fk = b.id
    #     and a.discom =  %s
    #     and a.date <= str_to_date(%s, '%%d-%%m-%%Y')
    #     and b.model_type = 'SINK'
    #     and b.delete_ind = 0) a,
    #     power.realtime_forecast_staging b,
    #     model_master c
    #     where a.max_load_date = b.load_date
    #     and b.model_master_fk = c.id
    #     union all
    #     select 'Demand' type, 'HYBRID' model_short_name, 0.2 mrr""",
    #                                  data=(discom, date_today,
    #                                        discom, date_today))
    datacursor = db.query_dictcursor("""SELECT
        'Generation' type, c.model_short_name, b.mrr
        from
        (select max(a.Realtime_Forecast_Metadata_PK) id
        from `power`.`realtime_forecast_metadata` a,
        power.model_master b
        where a.model_master_fk = b.id
        and a.discom =  %s
        and a.date <= str_to_date(%s, '%%d-%%m-%%Y')
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
        and a.discom =  %s
        and a.date <= str_to_date(%s, '%%d-%%m-%%Y')
        and b.model_type = 'SINK'
        and b.delete_ind = 0) a,
        `power`.`realtime_forecast_metadata` b,
        power.model_master c
        where a.id = b.Realtime_Forecast_Metadata_PK
        and   c.id = b.model_master_fk
        union all
        select 'Demand' type, 'HYBRID' model_short_name, 0.2 mrr""",
                                     data=(discom, date_today,
                                           discom, date_today))

    results = datacursor.fetchall()
    db.close()
    current_app.logger.debug('livedefaultvalues results %s', results)
    # Get the 1st element of Generation or Demand type
    type_hold = ''
    results_keep = []
    for ele in results:
        if ele.get('type') != type_hold:
            results_keep.append(ele)
            type_hold = ele.get('type')
    current_app.logger.debug('livedefaultvalues results_keep %s', results_keep)
    return json.dumps(results_keep, use_decimal=True)


@ems.context_processor
def liveposmapcolumns():
    """
    When you request the root path, you'll get the index.html template.
    """
    db = DB()
    datacursor = db.query_dictcursor("""SELECT
        ldc_name, ldc_org_name
        from power.org_isgs_map a,
        power.organisation_master b
        where a.organisation_master_fk = b.organisation_master_pk
        and (b.organisation_master_pk = %s
        or b.organisation_parent_fk = %s)
        and a.delete_ind = 0
        and b.delete_ind = 0""", data=(
        current_user.organisation_master_fk,
        current_user.organisation_master_fk))
    org = datacursor.fetchall()
    db.close()
    try:
        ldc = org[0].get('ldc_name')
    except Exception:
        ldc = None
    # ldc = 'ERLDC'
    if ldc == 'ERLDC':
        columns = ['Date', 'Block No', 'ISGS',
                   'LTOA MTOA', 'Bilateral', 'IEX', 'PXIL',
                   'Int Gen', 'Int Gen Forecast', 'Demand',
                   'Demand Forecast', 'Day Before Forecast',
                   'Availibility', 'Deficit Surplus',
                   'Revised Position']
        # isgseditcolumns = ['Date', 'Revision', 'Discom', 'Block_No', 'ISGS',
        #                    'LTOA_MTOA', 'Bilateral']
    elif ldc == 'NRLDC':
        columns = ['Date', 'Block No', 'ISGS',
                   'LTOA', 'MTOA', 'Shared', 'Bilateral',
                   'IEX', 'PXIL',
                   'Int Gen', 'Int Gen Forecast', 'Demand',
                   'Demand Forecast', 'Day Before Forecast',
                   'Availibility', 'Deficit Surplus',
                   'Revised Position']
        # isgseditcolumns = ['Date', 'Revision', 'Discom', 'Block_No', 'ISGS',
        #                    'LTOA_MTOA', 'Bilateral']
    elif ldc == 'WRLDC':
        columns = ['Date', 'Block No', 'ISGS',
                   'LTOA', 'MTOA', 'STOA', 'URS', 'RRAS',
                   'IEX', 'PXIL', 'Int Gen Others',
                   'Int Gen Conv', 'Int Gen Wind', 'Int Gen Solar', 'Demand',
                   'Demand Forecast', 'Wind Forecast', 'Solar Forecast',
                   'DB Demand Forecast',
                   'DB SOLAR Forecast', 'DB WIND Forecast',
                   'Availibility', 'Deficit Surplus',
                   'Revised Position']
        # isgseditcolumns = ['Date', 'Revision', 'Discom', 'Block_No', 'ISGS',
        #                    'LTOA_MTOA', 'Bilateral']
    else:
        columns = ['Date', 'Block No', 'ISGS',
                   'LTOA', 'MTOA', 'STOA', 'URS', 'RRAS',
                   'IEX', 'PXIL', 'Int Gen Others',
                   'Int Gen Conv', 'Int Gen Wind', 'Int Gen Solar', 'Demand',
                   'Demand Forecast',
                   'Availibility', 'Deficit Surplus',
                   'Revised Position']
    return dict(rtd_columns=columns)


@ems.route("/get_liveforecast_rev2", methods=['POST'])
@login_required
def get_liveforecast_rev2():
    """Get Live Forecast Rev."""
    date = request.json['date']
    discom = request.json['discom']
    db = DB()
    datacursor = db.query_dictcursor("""select
        DATE_FORMAT(a.date, '%%d-%%m-%%Y') date,
        d.model_type, max(revision) revision
        from position_map a,
             organisation_master b,
             (select *
              from
              pool_master
              where pool_name in ('INT_GENERATION_FOR', 'REALTIME_DEMAND_FOR')
              and pool_type in ('WIND', 'SOLAR', 'UNKNOWN')) c,
              power.model_master d
        where a.organisation_master_fk = b.organisation_master_pk
        and a.model_master_fk = d.id
        and a.pool_master_fk = c.pool_master_pk
        and a.date = str_to_date(%s, '%%d-%%m-%%Y')
        and b.organisation_code = %s
        group by a.date, d.model_type""", data=(date, discom))
    results = datacursor.fetchall()
    # db.close()
    return json.dumps(results, use_decimal=True)


@ems.route("/get_liveposmap_datav2", methods=['POST'])
@login_required
def get_liveposmap_datav2():
    """Get Live Position Map."""
    from batch.bin.sql_load_lib import sql_load_lib
    date = request.json['date']
    date = datetime.strptime(date, '%d-%m-%Y').strftime('%Y-%m-%d')
    # date = '2017-06-22'
    discom = request.json['discom']
    level = request.json['level']
    # discom = 'GUVNL'
    # level = 'POOL_TYPE'
    dsnfile = current_app.config['DB_CONNECT_FILE']
    # db = DB()
    # datacursor = db.query_dictcursor("""SELECT
    #     DATE_FORMAT(a.date, '%%d-%%m-%%Y') date,
    #     e.block_no, d.organisation_code,
    #     f.pool_name, f.pool_type, a.entity_name, a.quantum
    #     from  power.position_map a,
    #     power.organisation_master d,
    #     power.block_master e,
    #     power.pool_master f
    #     where a.block_no_fk = e.block_no_pk
    #     and a.organisation_master_fk = d.organisation_master_pk
    #     and a.pool_master_fk = f.pool_master_pk
    #     and a.date =  str_to_date(%s, '%%d-%%m-%%Y')
    #     and d.organisation_code = %s""", data=(date, discom))
    # results = datacursor.fetchall()
    # db.close()
    # print json.dumps(results)
    results = \
        sql_load_lib.sql_sp_realtime_data_fetch(dsnfile, date, discom, level)
    current_app.logger.debug('get_liveposmap_datav2: %s', results)
    return json.dumps(results, use_decimal=True)


@ems.route("/get_liveposmap_data", methods=['POST'])
@login_required
def get_liveposmap_data():
    """
    Get data from db
    """
    # date = request.form['date']
    # discom = request.form['discom']
    # model = request.form['model']
    current_app.logger.info('***** started get_liveposmap_data')
    # current_app.logger.debug("post liveposmapedit %s", request.json)
    if request.json:
        livedefaultvalues(request.json['date'])
        date = request.json['date']
        model = request.json['model']
        discom = request.json['discom']
        current_app.logger.info('Start get_liveposmap_data %s %s %s',
                                date, discom, model)
        # valid_discoms = get_org().get('org_name')
        db = DB()
        datacursor = db.query_dictcursor("""SELECT
            b.state_name, c.organisation_code,
            a.ldc_name, a.ldc_org_name
            from power.org_isgs_map a,
                 power.state_master b,
                 power.organisation_master c
            where c.state_master_fk = b.state_master_pk
            and a.organisation_master_fk = c.organisation_master_pk
            and a.delete_ind = 0
            and b.delete_ind = 0
            and c.delete_ind = 0
            and (c.organisation_master_pk = %s
            or c.organisation_parent_fk = %s)""", data=(
            current_user.organisation_master_fk,
            current_user.organisation_master_fk))
        org = datacursor.fetchall()
        db.close()
        ldc = org[0].get('ldc_name')
        # state = org[0].get('state_name')
        discom = org[0].get('organisation_code')
        # ldc = 'ERLDC'
        # date = '05-12-2015'
        # model = 'MLP'
        # discom = 'BPDCL'
        if ldc == 'ERLDC':
            columns = ['Date', 'Discom', 'Block_No', 'Revision', 'State',
                       'ISGS', 'LTOA_MTOA', 'Bilateral', 'IEX', 'PXIL',
                       'INTERNAL_GENERATION_ACTUAL',
                       'RT_INTERNAL_GENERATION_FOR',
                       'INTERNAL_GENERATION',
                       'DEMAND_ACTUAL', 'RT_DEMAND_FORECAST',
                       'DB_DEMAND_FORECAST',
                       'Availibility', 'Deficit_Surplus',
                       'Revised_Position'
                       ]
            datacursor = db.query_dictcursor("""SELECT
                DATE_FORMAT(rpms.date, '%%d-%%m-%%Y') Date,
                rpms.Discom, rpms.Block_No, rpms.Revision,
                rpms.State, rpms.ISGS, rpms.LTOA_MTOA,
                rpms.Bilateral, rpms.IEX,
                rpms.PXIL, rpms.INTERNAL_GENERATION_ACTUAL,
                rpms.RT_INTERNAL_GENERATION_FOR,
                ROUND(COALESCE(case when rpms.INTERNAL_GENERATION_ACTUAL = 0
                         then NULL else rpms.INTERNAL_GENERATION_ACTUAL end,
                         rpms.RT_INTERNAL_GENERATION_FOR), 2)
                         INTERNAL_GENERATION,
                rpms.DEMAND_ACTUAL,
                rpms.RT_DEMAND_FORECAST, fs.DB_DEMAND_FORECAST,
                @Availibility := ROUND(rpms.ISGS + rpms.LTOA_MTOA +
                rpms.Bilateral + rpms.IEX + rpms.PXIL +
                COALESCE(case when rpms.INTERNAL_GENERATION_ACTUAL = 0
                         then NULL else rpms.INTERNAL_GENERATION_ACTUAL end,
                         rpms.RT_INTERNAL_GENERATION_FOR), 2) Availibility,
                ROUND(fs.db_demand_forecast - @Availibility,2) Deficit_Surplus,
                ROUND(@Availibility -
                      COALESCE(case when rpms.DEMAND_ACTUAL = 0
                               then NULL else rpms.DEMAND_ACTUAL end,
                                rpms.RT_DEMAND_FORECAST), 2) Revised_Position
                from
                    (select a.date, a.block_no, a.revision,
                        sum(case when a.pool_name = 'ISGS'
                            then a.quantum else 0 end) ISGS,
                        sum(case when a.pool_name = 'LTOA_MTOA'
                            then a.quantum else 0 end) LTOA_MTOA,
                        sum(case when a.pool_name = 'Bilateral'
                            then a.quantum else 0 end) Bilateral,
                        sum(case when a.pool_name = 'IEX'
                            then a.quantum else 0 end) IEX,
                        sum(case when a.pool_name = 'PXIL'
                            then a.quantum else 0 end) PXIL,
                        sum(case when a.pool_name = 'INT_GENERATION_ACT'
                            then case when a.quantum is not null
                                 then round(a.quantum + coalesce(a.bias, 0), 2)
                                 when a.bias is not null and a.bias <> 0
                                 then round(coalesce(a.quantum, 0) + a.bias, 2)
                                 else null end
                            else 0 end) INTERNAL_GENERATION_ACTUAL,
                        sum(case when a.pool_name = 'INT_GENERATION_FOR'
                            then a.quantum
                            else null end) RT_INTERNAL_GENERATION_FOR,
                        sum(case when a.pool_name = 'DEMAND_ACT'
                            then case when a.quantum is not null
                                 then round(a.quantum + coalesce(a.bias, 0), 2)
                                 when a.bias is not null and a.bias <> 0
                                 then round(coalesce(a.quantum, 0) + a.bias, 2)
                                 else null end
                            else null end) DEMAND_ACTUAL,
                        sum(case when a.pool_name = 'REALTIME_DEMAND_FOR'
                            then a.quantum else null end) RT_DEMAND_FORECAST,
                        a.discom,
                        a.state
                    from
                        power.realtime_position_map_staging a,
                        (select date, discom, state, pool_name,
                         max(revision) max_revision
                         from power.realtime_position_map_staging
                         where date = str_to_date(%s, '%%d-%%m-%%Y')
                         and discom = %s
                         group by date, discom, state, pool_name) b
                    where a.date = b.date
                    and a.discom = b.discom
                    and a.state = b.state
                    and a.pool_name = b.pool_name
                    and a.revision = b.max_revision
                    group by a.date, a.block_no, a.discom, a.state) rpms
                left join
                    (select d.date, d.block_no,
                    round(schedule, 2) db_demand_forecast,
                    d.discom, d.state
                    from
                        (select date, discom,
                         max(revision) max_revision
                         from power.position_map_staging
                         where date = str_to_date(%s, '%%d-%%m-%%Y')
                         and discom = %s
                         group by date, discom) c,
                        power.position_map_staging d
                    where c.date = d.date
                    and c.discom = d.discom
                    and d.revision = c.max_revision
                    and d.pool_name = 'FORECAST') fs
                on(rpms.date = fs.date
                and rpms.block_no = fs.block_no
                and rpms.discom = fs.discom
                and rpms.state = fs.state)
                order by rpms.date, rpms.discom, rpms.block_no""",
                                             data=(date, discom, date, discom
                                                   ))
        elif ldc == 'NRLDC':
            columns = ['Date', 'Discom', 'Block_No', 'Revision', 'State',
                       'ISGS', 'LTOA', 'MTOA', 'Shared', 'Bilateral',
                       'IEX', 'PXIL', 'INTERNAL_GENERATION_ACTUAL',
                       'RT_INTERNAL_GENERATION_FOR', 'INTERNAL_GENERATION',
                       'DEMAND_ACTUAL',
                       'RT_DEMAND_FORECAST', 'DB_DEMAND_FORECAST',
                       'Availibility', 'Deficit_Surplus',
                       'Revised_Position']
            datacursor = db.query_dictcursor("""SELECT
                DATE_FORMAT(rpms.date, '%%d-%%m-%%Y') Date,
                rpms.Discom, rpms.Block_No, rpms.Revision,
                rpms.State,
                rpms.ISGS, rpms.LTOA, rpms.MTOA, rpms.Shared,
                rpms.Bilateral, rpms.IEX,
                rpms.PXIL, rpms.INTERNAL_GENERATION_ACTUAL,
                rpms.RT_INTERNAL_GENERATION_FOR,
                ROUND(COALESCE(case when rpms.INTERNAL_GENERATION_ACTUAL = 0
                         then NULL else rpms.INTERNAL_GENERATION_ACTUAL end,
                         rpms.RT_INTERNAL_GENERATION_FOR), 2)
                         INTERNAL_GENERATION,
                rpms.DEMAND_ACTUAL,
                rpms.RT_DEMAND_FORECAST, fs.DB_DEMAND_FORECAST,
                @Availibility := ROUND(rpms.ISGS + rpms.LTOA + rpms.MTOA +
                rpms.Shared + rpms.Bilateral + rpms.IEX + rpms.PXIL +
                COALESCE(case when rpms.INTERNAL_GENERATION_ACTUAL = 0
                         then NULL else rpms.INTERNAL_GENERATION_ACTUAL end,
                         rpms.RT_INTERNAL_GENERATION_FOR),2) Availibility,
                ROUND(fs.db_demand_forecast - @Availibility,2) Deficit_Surplus,
                ROUND(@Availibility -
                      COALESCE(case when rpms.DEMAND_ACTUAL = 0
                               then NULL else rpms.DEMAND_ACTUAL end,
                                rpms.RT_DEMAND_FORECAST), 2) Revised_Position
                from
                    (select a.date, a.block_no, a.revision,
                        sum(case when a.pool_name = 'ISGS'
                            then a.quantum else 0 end) ISGS,
                        sum(case when a.pool_name = 'LTOA'
                            then a.quantum else 0 end) LTOA,
                        sum(case when a.pool_name = 'MTOA'
                            then a.quantum else 0 end) MTOA,
                        sum(case when a.pool_name = 'Bilateral'
                            then a.quantum else 0 end) Bilateral,
                        sum(case when a.pool_name = 'Shared'
                            then a.quantum else 0 end) Shared,
                        sum(case when a.pool_name = 'IEX'
                            then a.quantum else 0 end) IEX,
                        sum(case when a.pool_name = 'PXIL'
                            then a.quantum else 0 end) PXIL,
                        sum(case when a.pool_name = 'INT_GENERATION_ACT'
                            then case when a.quantum is not null
                                 then round(a.quantum + coalesce(a.bias, 0), 2)
                                 when a.bias is not null and a.bias <> 0
                                 then round(coalesce(a.quantum, 0) + a.bias, 2)
                                 else null end
                            else 0 end) INTERNAL_GENERATION_ACTUAL,
                        sum(case when a.pool_name = 'INT_GENERATION_FOR'
                            then a.quantum
                            else null end) RT_INTERNAL_GENERATION_FOR,
                        sum(case when a.pool_name = 'DEMAND_ACT'
                            then case when a.quantum is not null
                                 then round(a.quantum + coalesce(a.bias, 0), 2)
                                 when a.bias is not null and a.bias <> 0
                                 then round(coalesce(a.quantum, 0) + a.bias, 2)
                                 else null end
                            else null end) DEMAND_ACTUAL,
                        sum(case when a.pool_name = 'REALTIME_DEMAND_FOR'
                            then a.quantum else 0 end) RT_DEMAND_FORECAST,
                        a.discom,
                        a.state
                    from
                        power.realtime_position_map_staging a,
                        (select date, discom, state, pool_name,
                         max(revision) max_revision
                         from power.realtime_position_map_staging
                         where date = str_to_date(%s, '%%d-%%m-%%Y')
                         and discom = %s
                         group by date, discom, state, pool_name) b
                    where a.date = b.date
                    and a.discom = b.discom
                    and a.state = b.state
                    and a.pool_name = b.pool_name
                    and a.revision = b.max_revision
                    group by a.date, a.block_no, a.revision, a.discom, a.state) rpms
                left join
                    (select d.date, d.block_no,
                    round(schedule, 2) db_demand_forecast,
                    d.discom, d.state
                from
                    (select date, discom,
                     max(revision) max_revision
                     from power.position_map_staging
                     where date = str_to_date(%s, '%%d-%%m-%%Y')
                     and discom = %s
                     group by date, discom) c,
                    power.position_map_staging d
                where c.date = d.date
                and c.discom = d.discom
                and d.revision = c.max_revision
                and d.pool_name = 'FORECAST') fs
                on(rpms.date = fs.date
                and rpms.block_no = fs.block_no
                and rpms.discom = fs.discom
                and rpms.state = fs.state)
                order by rpms.date, rpms.discom, rpms.block_no""",
                                             data=(date, discom, date, discom
                                                   ))
        elif ldc == 'WRLDC':
            columns = ['Date', 'Discom', 'Block_No', 'Revision', 'State',
                       'ISGS', 'LTOA', 'MTOA', 'STOA', 'URS', 'RRAS',
                       'IEX', 'PXIL', 'INT_GEN_OTH', 'INT_GEN_CONV',
                       'INT_GEN_WIND', 'INT_GEN_SOLAR',
                       'DEMAND_ACTUAL',
                       'RT_DEMAND_FORECAST',
                       'RT_INT_GEN_WIND_FOR',
                       'RT_INT_GEN_SOLAR_FOR',
                       'DB_DEMAND_FORECAST',
                       'DB_SOLAR_FORECAST',
                       'DB_WIND_FORECAST',
                       'Availibility', 'Deficit_Surplus',
                       'Revised_Position']
            datacursor = db.query_dictcursor("""SELECT
                DATE_FORMAT(rpms.date, '%%d-%%m-%%Y') Date,
                rpms.Discom, rpms.Block_No, rpms.Revision,
                rpms.State,
                rpms.ISGS, rpms.LTOA, rpms.MTOA, rpms.STOA,
                rpms.URS, rpms.RRAS, rpms.IEX,
                rpms.PXIL,
                @int_gen_oth := ROUND(COALESCE(case when
                         rpms.INTERNAL_GENERATION_OTH = 0
                         then NULL else rpms.INTERNAL_GENERATION_OTH end,
                         rpms.INTERNAL_GENERATION_OTH_SCH), 2)
                          INT_GEN_OTH,
                @int_gen_conv := ROUND(COALESCE(case when
                         rpms.INTERNAL_GENERATION_CONV = 0
                         then NULL else rpms.INTERNAL_GENERATION_CONV end,
                         rpms.RT_INTERNAL_GENERATION_CONV_FOR,
                         rpms.INTERNAL_GENERATION_CONV_SCH), 2)
                          INT_GEN_CONV,
                @int_gen_wind := ROUND(COALESCE(case when
                         rpms.INTERNAL_GENERATION_WIND = 0
                         then NULL else rpms.INTERNAL_GENERATION_WIND end,
                         rpms.RT_INTERNAL_GENERATION_WIND_FOR,
                         rpms.INTERNAL_GENERATION_WIND_SCH), 2)
                          INT_GEN_WIND,
                @int_gen_solar := ROUND(COALESCE(case when
                         rpms.INTERNAL_GENERATION_SOLAR = 0
                         then NULL else rpms.INTERNAL_GENERATION_SOLAR end,
                         rpms.RT_INTERNAL_GENERATION_SOLAR_FOR,
                         rpms.INTERNAL_GENERATION_SOLAR_SCH), 2)
                          INT_GEN_SOLAR,
                rpms.DEMAND_ACTUAL,
                rpms.RT_DEMAND_FORECAST,
                round(rpms.RT_INTERNAL_GENERATION_WIND_FOR,2)
                 RT_INT_GEN_WIND_FOR,
                round(rpms.RT_INTERNAL_GENERATION_SOLAR_FOR,2)
                 RT_INT_GEN_SOLAR_FOR,
                round(rpms.DB_DEMAND_FORECAST, 2) DB_DEMAND_FORECAST,
                round(rpms.DB_SOLAR_FORECAST, 2) DB_SOLAR_FORECAST,
                round(rpms.DB_WIND_FORECAST, 2) DB_WIND_FORECAST,
                @Availibility := ROUND(rpms.ISGS + rpms.LTOA + rpms.MTOA +
                rpms.STOA + rpms.URS + rpms.RRAS + rpms.IEX + rpms.PXIL +
                @int_gen_oth + @int_gen_conv +
                @int_gen_wind + @int_gen_solar, 2) Availibility,
                ROUND(rpms.db_demand_forecast -
                      @Availibility,2) Deficit_Surplus,
                ROUND(@Availibility -
                      COALESCE(case when rpms.DEMAND_ACTUAL = 0
                               then NULL else rpms.DEMAND_ACTUAL end,
                                rpms.RT_DEMAND_FORECAST), 2) Revised_Position
                from
                    (select a.date, a.block_no, a.revision,
                        sum(case when a.pool_name = 'ISGS'
                            then a.quantum else 0 end) ISGS,
                        sum(case when a.pool_name = 'LTA'
                            then a.quantum else 0 end) LTOA,
                        sum(case when a.pool_name = 'MTOA'
                            then a.quantum else 0 end) MTOA,
                        sum(case when a.pool_name = 'STOA'
                            then a.quantum else 0 end) STOA,
                        sum(case when a.pool_name = 'URS'
                            then a.quantum else 0 end) URS,
                        sum(case when a.pool_name = 'RRAS'
                            then a.quantum else 0 end) RRAS,
                        sum(case when a.pool_name = 'IEX'
                            then a.quantum else 0 end) IEX,
                        sum(case when a.pool_name = 'PXIL'
                            then a.quantum else 0 end) PXIL,
                        sum(case when a.pool_name = 'INT_GENERATION_ACT'
                                  and a.pool_type = 'OTHERS'
                            then case when a.quantum is not null
                                 then round(a.quantum + coalesce(a.bias, 0), 2)
                                 when a.bias is not null and a.bias <> 0
                                 then round(coalesce(a.quantum, 0) + a.bias, 2)
                                 else null end
                            else 0 end) INTERNAL_GENERATION_OTH,
                        sum(case when a.pool_name = 'INT_GENERATION_SCH'
                                  and a.pool_type = 'OTHERS'
                            then case when a.quantum is not null
                                 then round(a.quantum + coalesce(a.bias, 0), 2)
                                 when a.bias is not null and a.bias <> 0
                                 then round(coalesce(a.quantum, 0) + a.bias, 2)
                                 else null end
                            else 0 end) INTERNAL_GENERATION_OTH_SCH,
                        sum(case when a.pool_name = 'INT_GENERATION_ACT'
                                  and a.pool_type = 'CONVENTIONAL'
                            then case when a.quantum is not null
                                 then round(a.quantum + coalesce(a.bias, 0), 2)
                                 when a.bias is not null and a.bias <> 0
                                 then round(coalesce(a.quantum, 0) + a.bias, 2)
                                 else null end
                            else 0 end) INTERNAL_GENERATION_CONV,
                        sum(case when a.pool_name = 'INT_GENERATION_SCH'
                                  and a.pool_type = 'CONVENTIONAL'
                            then case when a.quantum is not null
                                 then round(a.quantum + coalesce(a.bias, 0), 2)
                                 when a.bias is not null and a.bias <> 0
                                 then round(coalesce(a.quantum, 0) + a.bias, 2)
                                 else null end
                            else 0 end) INTERNAL_GENERATION_CONV_SCH,
                        sum(case when a.pool_name = 'INT_GENERATION_ACT'
                                  and a.pool_type = 'WIND'
                            then case when a.quantum is not null
                                 then round(a.quantum + coalesce(a.bias, 0), 2)
                                 when a.bias is not null and a.bias <> 0
                                 then round(coalesce(a.quantum, 0) + a.bias, 2)
                                 else null end
                            else 0 end) INTERNAL_GENERATION_WIND,
                        sum(case when a.pool_name = 'INT_GENERATION_SCH'
                                  and a.pool_type = 'WIND'
                            then case when a.quantum is not null
                                 then round(a.quantum + coalesce(a.bias, 0), 2)
                                 when a.bias is not null and a.bias <> 0
                                 then round(coalesce(a.quantum, 0) + a.bias, 2)
                                 else null end
                            else 0 end) INTERNAL_GENERATION_WIND_SCH,
                        sum(case when a.pool_name = 'INT_GENERATION_ACT'
                                  and a.pool_type = 'SOLAR'
                            then case when a.quantum is not null
                                 then round(a.quantum + coalesce(a.bias, 0), 2)
                                 when a.bias is not null and a.bias <> 0
                                 then round(coalesce(a.quantum, 0) + a.bias, 2)
                                 else null end
                            else 0 end) INTERNAL_GENERATION_SOLAR,
                        sum(case when a.pool_name = 'INT_GENERATION_SCH'
                                  and a.pool_type = 'SOLAR'
                            then case when a.quantum is not null
                                 then round(a.quantum + coalesce(a.bias, 0), 2)
                                 when a.bias is not null and a.bias <> 0
                                 then round(coalesce(a.quantum, 0) + a.bias, 2)
                                 else null end
                            else 0 end) INTERNAL_GENERATION_SOLAR_SCH,
                        sum(case when a.pool_name = 'INT_GENERATION_FOR'
                            and a.pool_type = 'CONVENTIONAL'
                            then a.quantum
                            else null end) RT_INTERNAL_GENERATION_CONV_FOR,
                        sum(case when a.pool_name = 'INT_GENERATION_FOR'
                            and a.pool_type ='WIND'
                            then a.quantum
                            else null end) RT_INTERNAL_GENERATION_WIND_FOR,
                        sum(case when a.pool_name = 'INT_GENERATION_FOR'
                            and a.pool_type ='SOLAR'
                            then a.quantum
                            else null end) RT_INTERNAL_GENERATION_SOLAR_FOR,
                        sum(case when a.pool_name = 'DEMAND_ACT'
                            then case when a.quantum is not null
                                 then round(a.quantum + coalesce(a.bias, 0), 2)
                                 when a.bias is not null and a.bias <> 0
                                 then round(coalesce(a.quantum, 0) + a.bias, 2)
                                 else null end
                            else null end) DEMAND_ACTUAL,
                        sum(case when a.pool_name = 'REALTIME_DEMAND_FOR'
                            then a.quantum else 0 end) RT_DEMAND_FORECAST,
                        sum(case when a.pool_name = 'DB_DEMAND_FOR'
                            then a.quantum else 0 end) DB_DEMAND_FORECAST,
                        sum(case when a.pool_name = 'DB_INT_GENERATION_FOR'
                            and a.pool_type = 'SOLAR'
                            then a.quantum else 0 end) DB_SOLAR_FORECAST,
                        sum(case when a.pool_name = 'DB_INT_GENERATION_FOR'
                            and a.pool_type = 'WIND'
                            then a.quantum else 0 end) DB_WIND_FORECAST,
                        a.discom,
                        a.state
                    from
                        power.realtime_position_map_staging a,
                        (select date, discom, state, pool_name,
                         pool_type, block_no,
                         max(revision) max_revision
                         from power.realtime_position_map_staging
                         where date = str_to_date(%s, '%%d-%%m-%%Y')
                         and discom = %s
                         group by date, discom, state, pool_name,
                         pool_type, block_no) b
                    where a.date = b.date
                    and a.discom = b.discom
                    and a.state = b.state
                    and a.pool_name = b.pool_name
                    and a.pool_type = b.pool_type
                    and a.revision = b.max_revision
                    and a.block_no = b.block_no
                    group by a.date, a.block_no,
                             a.discom, a.state) rpms""",
                                             data=(date, discom
                                                   ))
        results = datacursor.fetchall()
        db.close()
        # print json.dumps(results)
        return json.dumps([results, columns], use_decimal=True)
        # datacursor = db.query_dictcursor("""SELECT FOUND_ROWS() as rows""")
        # rows = datacursor.fetchall()[0]['rows']
        # db.close()
        # current_app.logger.info('Total Number of Rows: %s', rows)
        # current_app.logger.debug('Result: %s', results)
        # dt_output = rawsql_to_datatables(columns, results, rows, rows)
        # if datatableflag:
        #     return json.dumps(dt_output)
        # else:
        #     return json.dumps(results, use_decimal=True)


@ems.route("/post_liveposmapedit_data", methods=['POST'])
@roles_required(['admin', 'jobrunner', 'analytics'])
def post_liveposmapedit_data():
    current_app.logger.info('***** started post_liveposmapedit_data')
    # current_app.logger.debug("post liveposmapedit %s", request.json)
    if request.json:
        data = json.loads(json.dumps(request.json))
        posmap_data = data.get('data')
        # current_app.logger.debug('post_liveposmapedit_data  %s',
        #                          posmap_data)
        posmap_data_list = []
        db = DB()
        datacursor = db.query_dictcursor("""SELECT
            b.state_name, c.organisation_code,
            a.ldc_name, a.ldc_org_name
            from power.org_isgs_map a,
                 power.state_master b,
                 power.organisation_master c
            where c.state_master_fk = b.state_master_pk
            and a.organisation_master_fk = c.organisation_master_pk
            and a.delete_ind = 0
            and b.delete_ind = 0
            and c.delete_ind = 0
            and (c.organisation_master_pk = %s
            or c.organisation_parent_fk = %s)""", data=(
            current_user.organisation_master_fk,
            current_user.organisation_master_fk))
        org = datacursor.fetchall()
        # ldc = org[0].get('ldc_name')
        state = org[0].get('state_name')
        discom = org[0].get('organisation_code')
        # state = 'BIHAR'
        # discom = 'BPDCL'

        keys = {'INTERNAL_GENERATION_ACTUAL': 'INT_GENERATION_ACT',
                'DEMAND_ACTUAL': 'DEMAND_ACT'}
        for el in posmap_data:
            # current_app.logger.info(el)
            if state == el.get('State') and discom == el.get('Discom'):
                # temp = [el.get('Date'), el.get('State'), el.get('Revision'),
                #         el.get('Discom'), el.get('Block_No')]
                temp = [el.get('Date'), el.get('State'),
                        el.get('Discom'), el.get('Block_No')]
            for k, v in keys.items():
                tmp = []
                tmp.append(el.get(k))
                tmp.extend(temp)
                tmp.append(v)
                posmap_data_list.append(tmp)

        data = map(tuple, posmap_data_list)
        current_app.logger.info("****data to upload***%s", data)
        sql = """UPDATE realtime_position_map_staging
                SET bias = round(%s - coalesce(quantum, 0), 3),
                load_date = null
                where date = STR_TO_DATE(%s,'%%d-%%m-%%Y')
                and state = %s
                and discom = %s
                and block_no = %s
                and pool_name = %s"""
        try:
            db.query_dictcursor(sql, 'insert', data)
            db.query_commit()
            db.close()
        except Exception as error:
            db.query_rollback()
            db.close()
            current_app.logger.error(
                "Error during Live Position Map update %s", error)
            raise
        return json.dumps({'success': True}), 200,\
            {'ContentType': 'application/json'}


@ems.route("/update_liveposmap_data", methods=['POST'])
@roles_required(['admin', 'jobrunner', 'analytics'])
def update_liveposmap_data():
    """
    Update the realtime live position data based on MRR and Model Name.
    """
    current_app.logger.info('***** started update_liveposmap_data')
    if request.json:
        date = request.json['date']
        model = request.json['model']
        discom = request.json['discom']
        mrr = request.json['mrr']
        model_type = request.json['type']
        current_app.logger.info('update_liveposmap_data %s %s %s %s %s',
                                date, discom, model, mrr, model_type)
        # valid_discoms = get_org().get('org_name')
        db = DB()
        datacursor = db.query_dictcursor("""SELECT
            b.state_name, c.organisation_code
            from power.state_master b,
                 power.organisation_master c
            where c.state_master_fk = b.state_master_pk
            and b.delete_ind = 0
            and c.delete_ind = 0
            and (c.organisation_master_pk = %s
            or c.organisation_parent_fk = %s)""", data=(
            current_user.organisation_master_fk,
            current_user.organisation_master_fk))
        org = datacursor.fetchall()
        db.close()
        # ldc = org[0].get('ldc_name')
        state = org[0].get('state_name')
        discom = org[0].get('organisation_code')
        db_uri = current_app.config['SQLALCHEMY_DATABASE_URI']
        task = realtime_forecast_upd_tsk.apply_async(
            (model_type, db_uri, date, model, mrr, discom, state))
    return jsonify({}), 202, {'Location': url_for('ems.taskstatus',
                              job_nm="realtime_forecast_upd_tsk",
                              task_id=task.id)}


@celery.task(bind=True)
def realtime_forecast_upd_tsk(self, model_type, db_uri,
                              date, model, mrr, discom, state):
    logger.info("realtime_forecast_upd_tsk started")
    logger.info("%s %s %s %s %s %s",
                model_type, date, model, mrr, discom, state)
    message = ''
    self.update_state(state='PROGRESS',
                      meta={'current': 1, 'total': 4,
                            'status': message})
    try:
        from batch.bin.analytics.realtime_forecast\
            import realtime_demand_forecast
        from batch.bin.analytics.realtime_forecast\
            import realtime_generation_forecast
        if model_type == 'Generation':
            realtime_generation_forecast(db_uri, date, model,
                                         mrr, discom, state)
        elif model_type == 'Demand':
            realtime_demand_forecast(db_uri, date, model, mrr, discom, state)
    except Exception as err:
        message = 'Realtime Demand/Generation Failed:' + str(err)

    self.update_state(state='PROGRESS',
                      meta={'current': 2, 'total': 4,
                            'status': message})
    try:
        #  Placeholder to call stored procedure sp_surplus_deficit_realtime
        realtime_surplus_deficit(discom)
    except Exception as err:
        message = 'Realtime Surplus Deficit SP Failed:' + str(err)
    self.update_state(state='PROGRESS',
                      meta={'current': 3, 'total': 4,
                            'status': message})
    try:
        from batch.bin.analytics.realtime_surrender\
            import realtime_surrender
        realtime_surrender(db_uri, date, 4, discom, state)
    except Exception as err:
        message = 'Realtime Surrender Failed:' + str(err)
    return {'current': 100, 'total': 100, 'status': 'Task completed!',
            'result': 'Done'}


def realtime_surplus_deficit(discom):
    """
        Call stored procedure or sql statement.
    """
    import ems.batch.bin.dbconn as dbconn

    dsnfile = current_app.config['DB_CONNECT_FILE']
    connection = dbconn.connect(dsnfile)
    cursor = connection.cursor()
    errm = ''
    errs = ''
    try:
        logger.debug("%s %s %s", discom, errm, errs)
        cursor.callproc('sp_surplus_deficit_realtime',
                        (discom, errm, errs))
        cursor.execute("SELECT @_sp_surplus_deficit_realtime_2,"
                       "@_sp_surplus_deficit_realtime_3")
        data = cursor.fetchone()
        logger.info("SP Ended %s %s", data[0], data[1])
        cursor.close()
        if data[0] != 'SUCCESS':
            raise ValueError('Stored Procedure {} exited with status'
                             ' {} other than SUCCESS'
                             .format('sp_surplus_deficit_realtime',
                                     data[0]))
    except Exception as err:
        logger.error("SP Failed %s", str(err))
        if connection.open:
            cursor.close()
        raise
    return


def realtime_scada_isgs_data_init(discom):
    """
        Calls stored procedure or sql statement
    """
    import ems.batch.bin.dbconn as dbconn

    dsnfile = current_app.config['DB_CONNECT_FILE']
    connection = dbconn.connect(dsnfile)
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


# @periodic_task(run_every=(crontab(minute='*/10')))
# def realtime_new_position_map():
#     """
#         Calls stored procedure or sql statement
#     """
#     import batch.bin.sql_load_lib as sql_load_lib
#     dsnfile = current_app.config['DB_CONNECT_FILE']
#     db = DB()
#     datacursor = db.query_dictcursor("""SELECT
#                     c.organisation_code
#                     from power.organisation_master c
#                     where c.job_ind = 1""")
#     results = datacursor.fetchall()
#     db.close()
#     discoms = [org.get('organisation_code') for org in results]
#     for discom in discoms:
#         try:
#             sql_load_lib.sql_sp_load_realtime_v2(dsnfile, discom)
#         except Exception as e:
#             logger.error('Error for Discom {} : {}'.format(discom, str(e)))
#     return


def realtime_scada_data_agg(discom):
    """
        Calls stored procedure or sql statement
    """
    import ems.batch.bin.dbconn as dbconn

    dsnfile = current_app.config['DB_CONNECT_FILE']
    connection = dbconn.connect(dsnfile)
    cursor = connection.cursor()
    errm = ''
    errs = ''
    try:
        logger.debug("%s %s %s", discom, errm, errs)
        cursor.callproc('sp_scada_agg_blk',
                        (discom, errm, errs))
        cursor.execute("SELECT @_sp_scada_agg_blk_2,"
                       " @_sp_scada_agg_blk_3")
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


def realtime_load_gen_update(discom):
    """
        Calls stored procedure or sql statement
    """
    import ems.batch.bin.dbconn as dbconn

    dsnfile = current_app.config['DB_CONNECT_FILE']
    connection = dbconn.connect(dsnfile)
    cursor = connection.cursor()
    errm = ''
    errs = ''
    try:
        logger.debug("%s %s %s", discom, errm, errs)
        cursor.callproc('sp_load_gen_update_realtime',
                        (discom, errm, errs))
        cursor.execute("SELECT @_sp_load_gen_update_realtime_2,"
                       "@_sp_load_gen_update_realtime_3")
        data = cursor.fetchone()
        logger.info("SP Ended %s %s", data[0], data[1])
        cursor.close()
        if data[0] != 'SUCCESS':
            raise ValueError('Stored Procedure {} exited with status'
                             ' {} other than SUCCESS'
                             .format('sp_load_gen_update_realtime',
                                     data[0]))
    except Exception as err:
        logger.error("SP Failed %s", str(err))
        if connection.open:
            cursor.close()
        raise
    return


# @periodic_task(run_every=(crontab('*')))
# def test_beat():
#     logger.info('Starting test beat function test_beat')
#     logger.info(current_app.config['DB_CONNECT_FILE'])
#     logger.info(current_app.config['SQLALCHEMY_DATABASE_URI'])


@periodic_task(run_every=(crontab(minute='*/30')))
def realtime_pos_map_rldc_update_schtsk():
    """
    UPCL task to fetch data from NRLDC
    """
    # discom = 'UPCL'
    db = DB()
    datacursor = db.query_dictcursor("""SELECT
                    c.organisation_code
                    from power.organisation_master c
                    where c.job_ind = 1""")
    results = datacursor.fetchall()
    db.close()
    discoms = [org.get('organisation_code') for org in results]
    for discom in discoms:
        try:
            realtime_pos_map_rldc_update(discom)
        except Exception as e:
            logger.error('Error for Discom {} : {}'.format(discom, str(e)))


def realtime_pos_map_rldc_update(discom):
    """
    Gets ISGS Data
    """
    db = DB()
    datacursor = db.query_dictcursor("""SELECT
        b.state_name, c.organisation_code,
        a.ldc_name, a.ldc_org_name
        from power.org_isgs_map a,
             power.state_master b,
             power.organisation_master c
        where c.state_master_fk = b.state_master_pk
        and a.organisation_master_fk = c.organisation_master_pk
        and a.delete_ind = 0
        and b.delete_ind = 0
        and c.delete_ind = 0
        and c.organisation_code = %s""", data=(discom,))
    results = datacursor.fetchall()
    db.close()
    logger.info("realtime_pos_map_update_schtsk %s", results)
    logger.info("Starting ISGS data fetch scheduled job")
    conn_file = current_app.config['DB_CONNECT_FILE']
    if results[0].get('ldc_name') == 'ERLDC':
        erldc_crawler_tsk.apply_async((conn_file,))
    elif results[0].get('ldc_name') == 'NRLDC':
        nrldc_crawler_tsk.apply_async(
            (results[0].get('ldc_org_name'), conn_file))
    elif results[0].get('ldc_name') == 'WRLDC':
        wrldc_crawler_tsk.apply_async(
            (results[0].get('ldc_org_name'), conn_file))


# @periodic_task(run_every=(crontab(minute='30', hour='1,4,7,10,13,16,19,22')))
def realtime_pos_map_forecast_update_schtsk():
    """
    UPCL Updates Realtime position map data with ISGS SCADA MANUAL DATA
    """
    # discom = 'UPCL'
    db = DB()
    datacursor = db.query_dictcursor("""SELECT
                    c.organisation_code
                    from power.organisation_master c
                    where c.job_ind = 1
                    and c.delete_ind = 0""")
    results = datacursor.fetchall()
    db.close()
    discoms = [org.get('organisation_code') for org in results]
    for discom in discoms:
        try:
            realtime_pos_map_forecast_update(discom)
        except Exception as e:
            logger.error('Error for Discom {} : {}'.format(discom, str(e)))


# @periodic_task(run_every=(crontab(minute='02, 17, 32, 47')))
@periodic_task(run_every=(crontab(minute='07, 22, 37, 52')))
def realtime_pos_map_actual_update_schtsk():
    """
    UPCL Updates Realtime position map data with ISGS SCADA MANUAL DATA
    """
    # discoms = 'UPCL'
    db = DB()
    datacursor = db.query_dictcursor("""SELECT
                    b.state_name, c.organisation_code
                    from power.organisation_master c,
                         power.state_master b
                    where c.state_master_fk = b.state_master_pk
                    and c.job_ind = 1
                    and c.delete_ind = 0
                    and b.delete_ind = 0""")
    results = datacursor.fetchall()
    db.close()
    # discoms = [org.get('organisation_code') for org in results]
    db_uri = current_app.config['SQLALCHEMY_DATABASE_URI']
    date = serverdate().get('serverdate')
    for result in results:
        discom = result.get('organisation_code')
        state = result.get('state_name')
        try:
            logger.info("Starting SCADA realtime data"
                        "to position map scheduled job")
            realtime_scada_data_agg(discom)
        except Exception as e:
            logger.error('Error for realtime_scada_data_agg Discom {} : {}'.
                         format(discom, str(e)))
        try:
            logger.info("Starting ISGS data and SCADA "
                        "to position map scheduled job")
            realtime_scada_isgs_data_init(discom)
            logger.info("Starting t-1 and t day load to"
                        " position map scheduled job")
            realtime_load_gen_update(discom)
        except Exception as e:
            logger.error('Error for Discom {} : {}'.format(discom, str(e)))
        try:
            logger.info("Starting ISGS data and SCADA "
                        "to new position map scheduled job")
            import ems.batch.bin.sql_load_lib as sql_load_lib
            dsnfile = current_app.config['DB_CONNECT_FILE']
            sql_load_lib.sql_sp_load_realtime_v2(dsnfile, discom)
        except Exception as e:
            logger.error('Error for Discom {} : {}'.format(discom, str(e)))
        try:
            realtime_surplus_deficit(discom)
        except Exception as e:
            logger.error('Error for realtime_surplus_deficit Discom {} : {}'.
                         format(discom, str(e)))
        try:
            from batch.bin.realtime_gen_schedule import realtime_gen_schedule
            realtime_gen_schedule(db_uri, date, discom, state)
        except Exception as e:
            logger.error('Error for Discom {} : {}'.format(discom, str(e)))
    return


def realtime_pos_map_forecast_update(discom):
    logger.info("Starting real time forecast for Demand scheduled job")
    date_today = serverdate().get('serverdate')
    py_date_today = datetime.strptime(serverdate().get('serverdate'),
                                      '%d-%m-%Y')
    db_uri = current_app.config['SQLALCHEMY_DATABASE_URI']
    db = DB()
    datacursor = db.query_dictcursor("""SELECT
        b.state_name, c.organisation_code,
        from power.state_master b,
             power.organisation_master c
        where c.state_master_fk = b.state_master_pk
        and b.delete_ind = 0
        and c.delete_ind = 0
        and c.organisation_code = %s""", data=(discom,))
    results = datacursor.fetchall()
    # datacursor = db.query_dictcursor("""SELECT
    #     distinct 'Generation' type, c.model_short_name, b.mrr
    #     from
    #     (select max(load_date) max_load_date
    #     from realtime_forecast_staging a,
    #       model_master b
    #     where a.model_master_fk = b.id
    #     and a.discom =  %s
    #     and a.date <= date_sub(str_to_date(%s, '%%d-%%m-%%Y'), INTERVAL 1 DAY)
    #     and b.model_type = 'INJECTION'
    #     and b.delete_ind = 0) a,
    #     power.realtime_forecast_staging b,
    #     model_master c
    #     where a.max_load_date = b.load_date
    #     and b.model_master_fk = c.id
    #     union all
    #     select 'Generation' type, 'HYBRID' model_short_name, 0.2 mrr
    #     union all
    #     select distinct 'Demand' type, c.model_short_name, b.mrr
    #     from
    #     (select max(load_date) max_load_date
    #     from realtime_forecast_staging a,
    #       model_master b
    #     where a.model_master_fk = b.id
    #     and a.discom =  %s
    #     and a.date <= date_sub(str_to_date(%s, '%%d-%%m-%%Y'), INTERVAL 1 DAY)
    #     and b.model_type = 'SINK'
    #     and b.delete_ind = 0) a,
    #     power.realtime_forecast_staging b,
    #     model_master c
    #     where a.max_load_date = b.load_date
    #     and b.model_master_fk = c.id
    #     union all
    #     select 'Demand' type, 'HYBRID' model_short_name, 0.2 mrr""",
    #                                  data=(discom, date_today,
    #                                        discom, date_today))
    datacursor = db.query_dictcursor("""SELECT
        'Generation' type, c.model_short_name, b.mrr
        from
        (select max(a.Realtime_Forecast_Metadata_PK) id
        from `power`.`realtime_forecast_metadata` a,
        power.model_master b
        where a.model_master_fk = b.id
        and a.discom =  %s
        and a.date <= str_to_date(%s, '%%d-%%m-%%Y')
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
        and a.discom =  %s
        and a.date <= str_to_date(%s, '%%d-%%m-%%Y')
        and b.model_type = 'SINK'
        and b.delete_ind = 0) a,
        `power`.`realtime_forecast_metadata` b,
        power.model_master c
        where a.id = b.Realtime_Forecast_Metadata_PK
        and   c.id = b.model_master_fk
        union all
        select 'Demand' type, 'HYBRID' model_short_name, 0.2 mrr""",
                                     data=(discom, date_today,
                                           discom, date_today))
    model_mrr = datacursor.fetchall()
    db.close()
    # Get the 1st element of Generation or Demand type
    type_hold = ''
    results_keep = []
    for ele in model_mrr:
        if ele.get('type') != type_hold:
            results_keep.append(ele)
            type_hold = ele.get('type')
    for dt in ([py_date_today - timedelta(days=1), py_date_today]):
        for el in results_keep:
            logger.debug("Date: " + dt.strftime('%d-%m-%Y'))
            logger.debug(el)
            try:
                realtime_forecast_upd_tsk.apply_async(
                    (el.get('type'), db_uri, dt.strftime('%d-%m-%Y'),
                     el.get('model_short_name'),
                     el.get('mrr'), discom,
                     results[0].get('state_name')))
            except Exception as err:
                logger.error("realtime_forecast_upd_tsk error: %s", str(err))
    return
    # Already indside the Realtime Forecast Update
    # logger.info("Calculating and updating SURPLUS/DEFECIT"
    #             " position map scheduled job")
    # realtime_surplus_deficit(discom)


@ems.context_processor
def surrender_revival_cols():
    """
    When you request the root path, you'll get the index.html template.
    """
    # columns = ['Date', 'Block No', 'Revison', 'Station Name', 'Fixed Cost',
    #            'Variable Cost', 'Pool Cost', 'Surrender']
    columns = ['Date', 'Revison', 'Block No', 'Generator Name',
               'Surrender/Revival']
    return dict(surr_columns=columns)


@ems.route("/get_surrender_revival_data", methods=['POST'])
@login_required
def get_surrender_revival_data():
    """
    Get data from db
    """
    date = request.json['date']
    discom = request.json['discom']
    current_app.logger.info("get_surrender_revival_data %s %s",
                            date, discom)
    db = DB()
    orgid = current_user.organisation_master_fk
    datacursor = db.query_dictcursor("""SELECT
        ldc_name, ldc_org_name, d.zone_code,
        b.organisation_code discom
        from power.org_isgs_map a,
             power.organisation_master b,
             power.state_master c,
             power.zone_master d
        where a.organisation_master_fk = b.organisation_master_pk
        and c.state_master_pk = b.state_master_fk
        and c.exchange_zone_master_fk = d.zone_master_pk
        and (b.organisation_master_pk = %s
            or b.organisation_parent_fk = %s)
        and a.delete_ind = 0
        and b.delete_ind = 0
        and c.delete_ind = 0
        and d.delete_ind = 0""", data=(orgid, orgid))
   
    results = datacursor.fetchone()
    db.close()
    discom = results.get('discom')
    # date = request.form['date']
    # discom = request.form['discom']
    # columns = ['date', 'revision', 'block_no', 'generator_name', 'quantum']
    db = DB()
    datacursor = db.query_dictcursor("""SELECT
        DATE_FORMAT(a.date, '%%d-%%m-%%Y') date,
        a.revision, a.block_no, a.generator_name,
        a.quantum
        from realtime_surr_rev_staging a,
        (select date, discom, pool_name,
         pool_type, generator_name, max(revision) max_revision
         from realtime_surr_rev_staging
         where date = str_to_date(%s, '%%d-%%m-%%Y')
         and discom = %s
         group by date, discom, pool_name, pool_type, generator_name
         having sum(quantum) <> 0) b
        where a.date = b.date
        and a.discom = b.discom
        and a.pool_name = b.pool_name
        and a.pool_type = b.pool_type
        and a.revision = b.max_revision
        and a.generator_name = b.generator_name
        order by a.date, a.revision, a.block_no""", data=(date, discom))
    results = datacursor.fetchall()
    # datacursor = db.query_dictcursor("""SELECT FOUND_ROWS() as rows""")
    # rows = datacursor.fetchall()[0]['rows']
    db.close()
    # current_app.logger.info('Total Number of Rows: %s', rows)
    # current_app.logger.debug('Result: %s', results)
    # dt_output = rawsql_to_datatables(columns, results, rows, rows)
    # return json.dumps(dt_output)
    return json.dumps(results, use_decimal=True)


# @ems.route("/biharsurrender")
# @login_required
# def biharsurrender():
#     """
#     When you request the root path, you'll get the index.html template.
#     """
#     columns = ['Date', 'Block No', 'Revison', 'Station Name',
#                'Fixed Cost', 'Variable Cost', 'Pool Cost', 'Surrender']
#     return flask.render_template("bihar_surrender.html", columns=columns)


# @ems.route("/get_biharsurrender_data")
@ems.route("/get_biharsurrender_data", methods=['POST'])
@login_required
def get_biharsurrender_data():
    """
    Get data from db
    """
    date = request.form['date']
    columns = ['date', 'block_no', 'revison', 'station_name',
               'fixed_cost', 'variable_cost', 'pool_cost', 'surrender']
    # index_column = "date"
    # table = "power.bseb_generator_surrender"
    # # where = "where date = date(sysdate())"
    # where = "where date = STR_TO_DATE('" + date + "','%d-%m-%Y')"
    # order = "order by date desc, revison desc, block_no, surrender desc"
    # db = DB()
    # cursor = db.cur
    # current_app.logger.info('Finished get_biharsurrender_data for date: %s',
    #                         date)
    # results = DataTablesServer(request, columns, index_column,
    #                            table, cursor, where, order).output_result()
    # # print "Here2",results
    # # return the results as json # import json
    # # results = {"aaData": [["1", "2", "3", "4"], ["5", "5", "5", "5"]]}
    # db.close()
    # return json.dumps(results)
    db = DB()
    datacursor = db.query_dictcursor("""SELECT
        DATE_FORMAT(date, '%%d-%%m-%%Y') date,
        block_no, revison, station_name,
        fixed_cost, variable_cost, pool_cost, surrender
        from bseb_generator_surrender
        where date = str_to_date(%s, '%%d-%%m-%%Y')
        order by date desc, revison desc,
        block_no, surrender desc""", data=(date,))
    results = datacursor.fetchall()
    datacursor = db.query_dictcursor("""SELECT FOUND_ROWS() _rows""")
    rows = datacursor.fetchall()[0]['_rows']
    db.close()
    current_app.logger.info('Total Number of Rows: %s', rows)
    current_app.logger.debug('Result: %s', results)
    dt_output = rawsql_to_datatables(columns, results, rows, rows)
    return json.dumps(dt_output)

# @ems.route("/tenisgs")
# @login_required
# def tenisgs():
#     """
#     When you request the root path, you'll get the index.html template.

#     """
#     columns = ['Date', 'Revision', 'Block_No', 'ISGS', 'LTA', 'MTOA',
#                'Shared', 'Bilateral', 'IEX_PXIL', 'Loss_Perc']
#     return flask.render_template("tenisgs.html", columns=columns)


# @ems.route("/get_tenisgs_data")
# @login_required
# def get_tenisgs_data():
#     """
#     Get data from db
#     """
#     columns = ['date', 'revision', 'Block_No', 'ISGS', 'LTA', 'MTOA',
#                'Shared', 'Bilateral', 'IEX_PXIL', 'Loss_Perc'
#                ]
#     index_column = "date"
#     table = "power.nrldc_state_drawl_summary_demoz2"
#     where = ""
#     order = "order by date desc, revision desc, block_no "
#     cursor = con # include a reference to your app mysqldb instance
#     logging.info('Finished collecting Data from get_tenisgs_data fn')
#     #collection = [dict(zip(columns, [1,2,3,4,5,6,7])), dict(zip(columns, [5,5,5,5,5,5,5]))]
#     results = DataTablesServer(request, columns, index_column,
#                                table, cursor, where, order).output_result()
#     #print "Here2",results
#     # return the results as json # import json
#     #results = {"aaData": [["1", "2", "3", "4"], ["5", "5", "5", "5"]]}
#     return json.dumps(results)


@ems.route("/dayahead")
@login_required
def dayahead():
    """
    When you request the root path, you'll get the index.html template.

    """
    db = DB()
    # datacursor = con.cursor(cursors.DictCursor)
    datacursor = db.query_dictcursor("""SELECT
        ldc_name, ldc_org_name
        from power.org_isgs_map a,
        power.organisation_master b
        where a.organisation_master_fk = b.organisation_master_pk
        and (b.organisation_master_pk = %s
        or b.organisation_parent_fk = %s)
        and a.delete_ind = 0
        and b.delete_ind = 0""", data=(
        current_user.organisation_master_fk,
        current_user.organisation_master_fk))
    org = datacursor.fetchall()
    db.close()
    try:
        ldc = org[0].get('ldc_name')
    except Exception:
        ldc = None
    if ldc == 'ERLDC':
        columns = ['Date', 'Revision', 'Discom', 'Block_No', 'ISGS',
                   'LTOA_MTOA', 'Bilateral', 'Open Access',
                   'Internal Generation', 'Net Schedule']
        isgseditcolumns = ['Date', 'Revision', 'Discom', 'Block_No', 'ISGS',
                           'LTOA_MTOA', 'Bilateral']
    elif ldc == 'NRLDC':
        columns = ['Date', 'Revision', 'Discom', 'Block_No', 'ISGS',
                   'MTOA', 'STOA', 'LTA', 'IEX', 'PXI', 'URS', 
                   'RRAS', 'SCED', 'REMC', 'RTM_PXI', 'RTM_IEX',
                   'INTERNAL GEN', 'NET SCHEDULE']
        isgseditcolumns = ['Date', 'Revision', 'Discom', 'Block_No', 'ISGS',
                           'MTOA', 'STOA', 'LTA', 'IEX', 'PXI', 'URS', 
                           'RRAS', 'SCED', 'REMC', 'RTM_PXI', 'RTM_IEX']
    elif ldc == 'WRLDC':
        columns = ['Date', 'Revision', 'Discom', 'Block_No', 'ISGS',
                   'LTOA', 'MTOA', 'STOA', 'URS', 'RRAS',
                   'INTERNAL GEN', 'NET SCHEDULE']
        isgseditcolumns = ['Date', 'Revision', 'Discom', 'Block_No', 'ISGS',
                           'LTOA', 'MTOA', 'STOA', 'URS', 'RRAS']
    else:
        columns = ['Date', 'Revision', 'Discom', 'Block_No', 'ISGS',
                   'LTOA', 'MTOA', 'STOA', 'URS', 'RRAS', 'Open Access',
                   'Internal Generation', 'Net Schedule']
        isgseditcolumns = ['Date', 'Revision', 'Discom', 'Block_No', 'ISGS',
                           'LTOA', 'MTOA', 'STOA', 'URS', 'RRAS']
    #internal generation 
    intsch_columns = ['Date', 'Revision', 'Discom', 'Block_No', 'INTERNAL_SCH', 'INTERNAL_OTHERS']
    intsch_edit_columns = ['Date', 'Revision', 'Discom', 'Block_No', 'INTERNAL_SCH', 'INTERNAL_OTHERS']
    return flask.render_template("dayahead_jinja.html",
                                 tenisgs_columns=columns,
                                 isgsedit_columns=isgseditcolumns,
                                 intsch_columns= intsch_columns,
                                 intsch_edit_columns= intsch_edit_columns)


@ems.route("/mediumterm")
@login_required
def mediumterm():
    """
    When you request the root path, you'll get the index.html template.

    """
    db = DB()
    # datacursor = con.cursor(cursors.DictCursor)
    datacursor = db.query_dictcursor("""SELECT
        ldc_name, ldc_org_name
        from power.org_isgs_map a,
        power.organisation_master b
        where a.organisation_master_fk = b.organisation_master_pk
        and (b.organisation_master_pk = %s
        or b.organisation_parent_fk = %s)
        and a.delete_ind = 0
        and b.delete_ind = 0""", data=(
        current_user.organisation_master_fk,
        current_user.organisation_master_fk))
    org = datacursor.fetchall()
    ldc = org[0].get('ldc_name')
    db.close()
    if ldc == 'ERLDC':
        columns = ['Date', 'Revison', 'Discom', 'Block_No', 'ISGS',
                   'LTOA/MTOA', 'Bilateral', 'IEX', 'PXIL', 'Net Schedule',
                   'Regulation']
    elif ldc == 'NRLDC':
        columns = ['Date', 'Revison', 'Discom', 'Block_No', 'ISGS',
                   'LTOA', 'MTOA', 'Shared', 'Bilateral', 'IEX', 'PXIL',
                   'Net Schedule']
    return flask.render_template("mediumterm_jinja.html",
                                 tenisgs_columns=columns)

# @ems.route("/get_tengenisgs_data")
# def get_tengenisgs_data():
#     """
#     Get data from db
#     """
#     columns = ['Date', 'Revision', 'Discom', 'Block_No', 'ISGS', 'LTOA_MTOA',
#                'Bilateral', 'IEX', 'PXIL', 'Net_Sch', 'Regulation']
#     index_column = "Date"
#     table = "power.vw_erldc_discom_drawl_summary_demo "
#     where = "where revision = 0 "
#     order = "order by date desc, revision desc, discom, block_no "
#     cursor = con # include a reference to your app mysqldb instance
#     print "Here"
#     #collection = [dict(zip(columns, [1,2,3,4,5,6,7])), dict(zip(columns, [5,5,5,5,5,5,5]))]
#     results = DataTablesServer(request, columns, index_column,
#                                table, cursor, where, order).output_result()
#     #print "Here2",results
#     # return the results as json # import json
#     #results = {"aaData": [["1", "2", "3", "4"], ["5", "5", "5", "5"]]}
#     return json.dumps(results)


# @ems.route("/get_tengenisgs_data", methods=['POST'])
# def get_tengenisgs_data():
#     """
#     Get data from db
#     """
#     date = request.form['date']
#     print "get_tengenisgs_data****" + date
#     columns = ['Date', 'Revision', 'Discom', 'Block_No', 'ISGS', 'LTOA_MTOA',
#                'Bilateral', 'IEX', 'PXIL', 'Net_Sch', 'Regulation']
#     index_column = "Date"
#     table = "power.vw_erldc_discom_drawl_summary_demo "
#     where = "where revision = 0 and date = adddate(STR_TO_DATE('" + date + "','%d-%m-%Y'), -1)"
#     order = "order by date desc, revision desc, discom, block_no "
#     cursor = con # include a reference to your app mysqldb instance
#     print "Here"
#     #collection = [dict(zip(columns, [1,2,3,4,5,6,7])), dict(zip(columns, [5,5,5,5,5,5,5]))]
#     results = DataTablesServer(request, columns, index_column,
#                                table, cursor, where, order).output_result()
#     print "Here2",results
#     # return the results as json # import json
#     #results = {"aaData": [["1", "2", "3", "4"], ["5", "5", "5", "5"]]}
#     return json.dumps(results)
@ems.route('/post_tenisgsgen_data', methods=['POST'])
@roles_required(['admin', 'jobrunner', 'analytics'])
def post_tenisgsgen_data():
    import pandas as pd
    current_app.logger.debug('***** started post_tenisgsgen_data')
    # current_app.logger.debug("post tenisgsgen %s", request.json)
    if request.json:
        current_app.logger.info('INside ***post_tenisgsgen_data')
        tenisgsgen_data = json.loads(json.dumps(request.json))
        #current_app.logger.info('INside *** %s', tenisgsgen_data)
        data = tenisgsgen_data.get('data')
        # df = pd.DataFrame(data)
        # df.to_csv('test.csv')
        category = tenisgsgen_data.get('category')
        date_for = tenisgsgen_data.get('date_for')
        rev = data[0].get('Revision')
        date_used = data[0].get('Date')
        ldc_discom = data[0].get('Discom')
        current_app.logger.info("""date_used %s ldc_discom %s
            revision %s category %s date_for %s""",
                                date_used, ldc_discom,
                                rev, category, date_for)
        db = DB()
        datacursor = db.query_dictcursor("""SELECT
            b.state_name, c.organisation_code,
            a.ldc_name, a.ldc_org_name
            from power.org_isgs_map a,
                 power.state_master b,
                 power.organisation_master c
            where c.state_master_fk = b.state_master_pk
            and a.organisation_master_fk = c.organisation_master_pk
            and a.delete_ind = 0
            and b.delete_ind = 0
            and c.delete_ind = 0
            and (c.organisation_master_pk = %s
            or c.organisation_parent_fk = %s)""", data=(
            current_user.organisation_master_fk,
            current_user.organisation_master_fk))
        org = datacursor.fetchall()
        db.close()
        ldc = org[0].get('ldc_name')
        state = org[0].get('state_name')
        discom = org[0].get('organisation_code')
        current_app.logger.debug('***** %s %s %s %s %s',
                                 ldc, state, discom, category, date_for)
        db = DB()
        if category == 'Drawl Schedule' and ldc == 'NRLDC':
            datacursor = db.query_dictcursor("""SELECT
                DATE_FORMAT(a.Date, '%%d-%%m-%%Y') Date,
                a.Block_No,
                a.Drawl_type,
                a.Station_Name,
                a.Schedule
                from nrldc_state_drawl_schedule_stg a
                where a.date = str_to_date(%s, '%%d-%%m-%%Y')
                and a.revision = %s
                and a.drawl_type = 'ISGS'
                and a.discom = %s
                and a.state = %s""", data=(date_used, rev, discom, state))
        elif category == 'Entitlement' and ldc == 'NRLDC':
            datacursor = db.query_dictcursor("""SELECT
                DATE_FORMAT(a.Date, '%%d-%%m-%%Y') Date,
                a.Block_No,
                'ISGS' Drawl_type,
                a.Station_Name,
                a.Schedule
                from nrldc_entitlements_stg   a
                where a.date = str_to_date(%s, '%%d-%%m-%%Y')
                and a.revision = %s
                and a.discom = %s
                and a.state = %s""", data=(date_used, rev, discom, state))
        elif category == 'Drawl Schedule' and ldc == 'ERLDC':
            datacursor = db.query_dictcursor("""SELECT
                DATE_FORMAT(a.date, '%%d-%%m-%%Y') Date,
                a.Block_No,
                a.Drawl_type,
                a.Station_Name,
                a.Schedule
                FROM power.erldc_state_drawl_schedule_stg a
                WHERE a.Date = str_to_date(%s, '%%d-%%m-%%Y')
                and   a.Revision = %s
                and   a.Discom = %s
                and   a.Drawl_Type = 'ISGS'
                and   a.Station_Name <> 'NET|DRAWAL|SCHD.'
                """, data=(date_used, rev, ldc_discom))
        elif category == 'Entitlement' and ldc == 'ERLDC':
            datacursor = db.query_dictcursor("""SELECT
                DATE_FORMAT(a.date, '%%d-%%m-%%Y') Date,
                a.Block_No,
                'ISGS' Drawl_type,
                a.Station_Name,
                a.Schedule
                FROM power.erldc_entitlements_stg a
                WHERE a.Date = str_to_date(%s, '%%d-%%m-%%Y')
                and   a.Revision = %s
                and   a.Discom = %s""", data=(date_used, rev, ldc_discom))
        elif category == 'Drawl Schedule' and ldc == 'WRLDC':
            datacursor = db.query_dictcursor("""SELECT
                DATE_FORMAT(a.date, '%%d-%%m-%%Y') Date,
                a.Block_No,
                a.Drawl_type,
                a.Station_Name,
                a.Schedule
                FROM power.wrldc_state_drawl_schedule_stg a
                WHERE a.Date = str_to_date(%s, '%%d-%%m-%%Y')
                and   a.Revision = %s
                and   a.State = %s
                and   a.Drawl_Type = 'ISGS'
                """, data=(date_used, rev, state))
        elif category == 'Entitlement' and ldc == 'WRLDC':
            datacursor = db.query_dictcursor("""SELECT
                DATE_FORMAT(a.date, '%%d-%%m-%%Y') Date,
                a.Block_No,
                'ISGS' Drawl_type,
                substr(a.Station_Name, 1,
                       locate('(', a.Station_Name,
                             length(a.Station_Name) - 4) - 1) as Station_Name,
                a.Schedule
                FROM power.wrldc_entitlements_stg a
                WHERE a.Date = str_to_date(%s, '%%d-%%m-%%Y')
                and   a.Revision = %s
                and   a.State = %s""", data=(date_used, rev, state))
        else:
            raise Exception('Undefined and unknown category!')
        isgs_data = datacursor.fetchall()
        datacursor = db.query_dictcursor("""SELECT
            coalesce(max(revision)+1, 0) as new_revision
            from power.isgstentative_schedule_staging
            where date = STR_TO_DATE(%s,'%%d-%%m-%%Y')
            and state = %s
            and discom = %s""", data=(date_for,
                                      state, discom))
        rev = datacursor.fetchall()
        newrev = rev[0].get('new_revision')
        # current_app.logger.debug("***** isgsdata %s", isgs_data)
        db.close()
        genkeys = {'isgs': ('ForDate', 'Block_No', 'ForRevision',
                            'Drawl_type', 'Generation_Type',
                            'Station_Name', 'Schedule', 'ActDiscom', 'State',
                            'Added_By_FK'),
                   'bilateral': ('ForDate', 'Block_No', 'ForRevision',
                                 'Generation_Entity_Name', 'Generation_Type',
                                 'Generator_Name', 'Bilateral', 'ActDiscom',
                                 'State',
                                 # 'Date', 'Revision', 'Schedule_Used',
                                 'Added_By_FK'),
                   'lta': ('ForDate', 'Block_No', 'ForRevision',
                            'Generation_Entity_Name', 'Generation_Type',
                            'Generator_Name', 'LTA', 'ActDiscom', 'State',
                            # 'Date', 'Revision', 'Schedule_Used',
                            'Added_By_FK'),
                   'mtoa': ('ForDate', 'Block_No', 'ForRevision',
                            'Generation_Entity_Name', 'Generation_Type',
                            'Generator_Name', 'MTOA', 'ActDiscom', 'State',
                            # 'Date', 'Revision', 'Schedule_Used',
                            'Added_By_FK'),
                   'stoa': ('ForDate', 'Block_No', 'ForRevision',
                            'Generation_Entity_Name', 'Generation_Type',
                            'Generator_Name', 'STOA', 'ActDiscom', 'State',
                            # 'Date', 'Revision', 'Schedule_Used',
                            'Added_By_FK'),
                   'urs': ('ForDate', 'Block_No', 'ForRevision',
                           'Generation_Entity_Name', 'Generation_Type',
                           'Generator_Name', 'URS', 'ActDiscom', 'State',
                           # 'Date', 'Revision', 'Schedule_Used',
                           'Added_By_FK'),
                   'rras': ('ForDate', 'Block_No', 'ForRevision',
                            'Generation_Entity_Name', 'Generation_Type',
                            'Generator_Name', 'RRAS', 'ActDiscom', 'State',
                            # 'Date', 'Revision', 'Schedule_Used',
                            'Added_By_FK'),                        
                   'iex': ('ForDate', 'Block_No', 'ForRevision',
                            'Generation_Entity_Name', 'Generation_Type',
                            'Generator_Name', 'IEX', 'ActDiscom', 'State',
                            # 'Date', 'Revision', 'Schedule_Used',
                            'Added_By_FK'), 
                   'pxi': ('ForDate', 'Block_No', 'ForRevision',
                            'Generation_Entity_Name', 'Generation_Type',
                            'Generator_Name', 'PXI', 'ActDiscom', 'State',
                            # 'Date', 'Revision', 'Schedule_Used',
                            'Added_By_FK'), 
                   'sced': ('ForDate', 'Block_No', 'ForRevision',
                            'Generation_Entity_Name', 'Generation_Type',
                            'Generator_Name', 'SCED', 'ActDiscom', 'State',
                            # 'Date', 'Revision', 'Schedule_Used',
                            'Added_By_FK'),  
                   'remc': ('ForDate', 'Block_No', 'ForRevision',
                            'Generation_Entity_Name', 'Generation_Type',
                            'Generator_Name', 'REMC', 'ActDiscom', 'State',
                            # 'Date', 'Revision', 'Schedule_Used',
                            'Added_By_FK'),
                   'rtm_pxi': ('ForDate', 'Block_No', 'ForRevision',
                            'Generation_Entity_Name', 'Generation_Type',
                            'Generator_Name', 'RTM_PXI', 'ActDiscom', 'State',
                            # 'Date', 'Revision', 'Schedule_Used',
                            'Added_By_FK'),
                   'rtm_iex': ('ForDate', 'Block_No', 'ForRevision',
                            'Generation_Entity_Name', 'Generation_Type',
                            'Generator_Name', 'RTM_IEX', 'ActDiscom', 'State',
                            # 'Date', 'Revision', 'Schedule_Used',
                            'Added_By_FK'),                                                                                                                                                                              
                   'shared': ('ForDate', 'Block_No', 'ForRevision',
                              'Generation_Entity_Name', 'Generation_Type',
                              'Generator_Name', 'Shared', 'ActDiscom', 'State',
                              # 'Date', 'Revision', 'Schedule_Used',
                              'Added_By_FK'),
                   'ltoa_mtoa': ('ForDate', 'Block_No', 'ForRevision',
                                 'Generation_Entity_Name', 'Generation_Type',
                                 'Generator_Name', 'LTOA_MTOA', 'ActDiscom',
                                 'State',
                                 # 'Date', 'Revision', 'Schedule_Used',
                                 'Added_By_FK')}

        isgs_data = [list(el.get(key) for key in genkeys.get('isgs'))
                     for el in isgs_data]
        # current_app.logger.debug('isgs_data: %s', isgs_data)
        for el in isgs_data:
            el[0] = date_for
            el[2] = newrev
            el[4] = 'UNKNOWN'
            el[7] = discom
            el[8] = state
            el[9] = current_user.id
        if ldc in ('ERLDC'):
            bilateral_data = [list(el.get(key)
                              for key in genkeys.get('bilateral'))
                              for el in data]
            for el in bilateral_data:
                el[0] = date_for
                el[2] = newrev
                el[3] = 'Bilateral'
                el[4] = 'UNKNOWN'
                el[7] = discom
                el[8] = state
                # el[11] = category
                el[9] = current_user.id
            upload_data = isgs_data[:]
            upload_data.extend(bilateral_data)
        # if ldc == 'NRLDC':
        #     ltoa_data = [list(el.get(key) for key in genkeys.get('ltoa'))
        #                  for el in data]
        #     for el in ltoa_data:
        #         el[0] = date_for
        #         el[2] = newrev
        #         el[3] = 'LTOA'
        #         el[4] = 'UNKNOWN'
        #         el[7] = discom
        #         el[8] = state
        #         # el[11] = category
        #         el[9] = current_user.id
        #     mtoa_data = [list(el.get(key) for key in genkeys.get('mtoa'))
        #                  for el in data]
        #     for el in mtoa_data:
        #         el[0] = date_for
        #         el[2] = newrev
        #         el[3] = 'MTOA'
        #         el[4] = 'UNKNOWN'
        #         el[7] = discom
        #         el[8] = state
        #         # el[11] = category
        #         el[9] = current_user.id
        #     shared_data = [list(el.get(key) for key in genkeys.get('shared'))
        #                    for el in data]
        #     for el in shared_data:
        #         el[0] = date_for
        #         el[2] = newrev
        #         el[3] = 'Shared'
        #         el[4] = 'UNKNOWN'
        #         el[7] = discom
        #         el[8] = state
        #         # el[11] = category
        #         el[9] = current_user.id
        #     upload_data.extend(ltoa_data)
        #     upload_data.extend(mtoa_data)
        #     upload_data.extend(shared_data)
        #elif ldc == 'ERLDC':
            ltoa_mtoa_data = [list(el.get(key)
                              for key in genkeys.get('ltoa_mtoa'))
                              for el in data]
            for el in ltoa_mtoa_data:
                el[0] = date_for
                el[2] = newrev
                el[3] = 'LTOA_MTOA'
                el[4] = 'UNKNOWN'
                el[7] = discom
                el[8] = state
                # el[11] = category
                el[9] = current_user.id
            upload_data.extend(ltoa_mtoa_data)
        elif ldc == 'WRLDC':
            ltoa_data = [list(el.get(key) for key in genkeys.get('ltoa'))
                         for el in data]
            for el in ltoa_data:
                el[0] = date_for
                el[2] = newrev
                el[3] = 'LTOA'
                el[4] = 'UNKNOWN'
                el[7] = discom
                el[8] = state
                # el[11] = category
                el[9] = current_user.id
            mtoa_data = [list(el.get(key) for key in genkeys.get('mtoa'))
                         for el in data]
            for el in mtoa_data:
                el[0] = date_for
                el[2] = newrev
                el[3] = 'MTOA'
                el[4] = 'UNKNOWN'
                el[7] = discom
                el[8] = state
                # el[11] = category
                el[9] = current_user.id
            stoa_data = [list(el.get(key) for key in genkeys.get('stoa'))
                         for el in data]
            for el in stoa_data:
                el[0] = date_for
                el[2] = newrev
                el[3] = 'STOA'
                el[4] = 'UNKNOWN'
                el[7] = discom
                el[8] = state
                # el[11] = category
                el[9] = current_user.id
            urs_data = [list(el.get(key) for key in genkeys.get('urs'))
                        for el in data]
            for el in urs_data:
                el[0] = date_for
                el[2] = newrev
                el[3] = 'URS'
                el[4] = 'UNKNOWN'
                el[7] = discom
                el[8] = state
                # el[11] = category
                el[9] = current_user.id
            rras_data = [list(el.get(key) for key in genkeys.get('rras'))
                         for el in data]
            for el in rras_data:
                el[0] = date_for
                el[2] = newrev
                el[3] = 'RRAS'
                el[4] = 'UNKNOWN'
                el[7] = discom
                el[8] = state
                # el[11] = category
                el[9] = current_user.id
            upload_data = isgs_data[:]
            upload_data.extend(ltoa_data)
            upload_data.extend(mtoa_data)
            upload_data.extend(stoa_data)
            upload_data.extend(urs_data)
            upload_data.extend(rras_data)
        elif ldc == 'NRLDC':
            ltoa_data = [list(el.get(key) for key in genkeys.get('lta'))
                         for el in data]
            # current_app.logger.debug('ltoa_data %s', ltoa_data)
            for el in ltoa_data:
                el[0] = date_for
                el[2] = newrev
                el[3] = 'LTA'
                el[4] = 'UNKNOWN'
                el[7] = discom
                el[8] = state
                # el[11] = category
                el[9] = current_user.id
            mtoa_data = [list(el.get(key) for key in genkeys.get('mtoa'))
                         for el in data]
            for el in mtoa_data:
                el[0] = date_for
                el[2] = newrev
                el[3] = 'MTOA'
                el[4] = 'UNKNOWN'
                el[7] = discom
                el[8] = state
                # el[11] = category
                el[9] = current_user.id
            stoa_data = [list(el.get(key) for key in genkeys.get('stoa'))
                         for el in data]
            for el in stoa_data:
                el[0] = date_for
                el[2] = newrev
                el[3] = 'STOA'
                el[4] = 'UNKNOWN'
                el[7] = discom
                el[8] = state
                # el[11] = category
                el[9] = current_user.id
            urs_data = [list(el.get(key) for key in genkeys.get('urs'))
                        for el in data]
            for el in urs_data:
                el[0] = date_for
                el[2] = newrev
                el[3] = 'URS'
                el[4] = 'UNKNOWN'
                el[7] = discom
                el[8] = state
                # el[11] = category
                el[9] = current_user.id
            rras_data = [list(el.get(key) for key in genkeys.get('rras'))
                         for el in data]
            for el in rras_data:
                el[0] = date_for
                el[2] = newrev
                el[3] = 'RRAS'
                el[4] = 'UNKNOWN'
                el[7] = discom
                el[8] = state
                # el[11] = category
                el[9] = current_user.id           
            iex_data = [list(el.get(key) for key in genkeys.get('iex'))
                         for el in data]
            for el in iex_data:
                el[0] = date_for
                el[2] = newrev
                el[3] = 'IEX'
                el[4] = 'UNKNOWN'
                el[7] = discom
                el[8] = state
                # el[11] = category
                el[9] = current_user.id
            pxi_data = [list(el.get(key) for key in genkeys.get('pxi'))
                         for el in data]
            for el in pxi_data:
                el[0] = date_for
                el[2] = newrev
                el[3] = 'PXI'
                el[4] = 'UNKNOWN'
                el[7] = discom
                el[8] = state
                # el[11] = category
                el[9] = current_user.id
            sced_data = [list(el.get(key) for key in genkeys.get('sced'))
                         for el in data]
            for el in sced_data:
                el[0] = date_for
                el[2] = newrev
                el[3] = 'SCED'
                el[4] = 'UNKNOWN'
                el[7] = discom
                el[8] = state
                # el[11] = category
                el[9] = current_user.id
            remc_data = [list(el.get(key) for key in genkeys.get('remc'))
                         for el in data]
            for el in remc_data:
                el[0] = date_for
                el[2] = newrev
                el[3] = 'REMC'
                el[4] = 'UNKNOWN'
                el[7] = discom
                el[8] = state
                # el[11] = category
                el[9] = current_user.id
            rpxi_data = [list(el.get(key) for key in genkeys.get('rtm_pxi'))
                         for el in data]
            for el in rpxi_data:
                el[0] = date_for
                el[2] = newrev
                el[3] = 'RTM_PXI'
                el[4] = 'UNKNOWN'
                el[7] = discom
                el[8] = state
                # el[11] = category
                el[9] = current_user.id
            riex_data = [list(el.get(key) for key in genkeys.get('rtm_iex'))
                         for el in data]
            for el in riex_data:
                el[0] = date_for
                el[2] = newrev
                el[3] = 'RTM_IEX'
                el[4] = 'UNKNOWN'
                el[7] = discom
                el[8] = state
                # el[11] = category
                el[9] = current_user.id                                                                                                              
            upload_data = isgs_data[:]
            upload_data.extend(mtoa_data)
            upload_data.extend(stoa_data)
            upload_data.extend(ltoa_data)
            upload_data.extend(iex_data)
            upload_data.extend(pxi_data)
            upload_data.extend(urs_data)
            upload_data.extend(rras_data)
            upload_data.extend(sced_data)
            upload_data.extend(remc_data)
            upload_data.extend(rpxi_data)
            upload_data.extend(riex_data)              
        # current_app.logger.debug('upload_data %s',
        #                          upload_data)
        # Change the lists of lists to list of tuples using map
        data = list(map(tuple, upload_data))
        #current_app.logger.debug("****data to upload***%s", data[0:3])
        sql = """INSERT INTO power.isgstentative_schedule_staging
                (date,
                block_no,
                revision,
                pool_name,
                pool_type,
                generator_name,
                schedule,
                discom,
                state,
                added_by_fk)
                VALUES
                (STR_TO_DATE(%s,'%%d-%%m-%%Y'),
                 %s,
                 %s,
                 %s,
                 %s,
                 %s,
                 ROUND(%s,3),
                 %s,
                 %s,
                 %s)"""
        try:
            current_app.logger.debug(sql % data[0])
            db = DB()
            db.query_dictcursor(sql, 'insert', data)
            db.query_commit()
            # db.cur.commit()
            db.close()
            current_app.logger.info('Inserted...')
        except Exception as error:
            # if db.cur.open:
            # db.cur.rollback()
            db.query_rollback()
            db.close()
            current_app.logger.error("Error during Tentative ISGS update %s",
                                     error)
            raise
        return json.dumps({'success': True}), 200,\
            {'ContentType': 'application/json'}


@ems.route("/get_genisgs_data", methods=['POST'])
@login_required
def get_genisgs_data():
    current_app.logger.info("get_genisgs_data %s %s",
                            request.json, request.form)
    datatype = request.json['datatype']
    date = request.json['date']
    rev = request.json['rev']
    current_app.logger.debug("get_genisgs_data %s %s %s",
                             datatype, date, rev)
    db = DB()
    # datacursor = con.cursor(cursors.DictCursor)
    datacursor = db.query_dictcursor("""SELECT
        ldc_name, ldc_org_name
        from power.org_isgs_map a,
        power.organisation_master b
        where a.organisation_master_fk = b.organisation_master_pk
        and (b.organisation_master_pk = %s
        or b.organisation_parent_fk = %s)
        and a.delete_ind = 0
        and b.delete_ind = 0""", data=(
        current_user.organisation_master_fk,
        current_user.organisation_master_fk))
    org = datacursor.fetchall()
    ldc = org[0].get('ldc_name')
    discom = org[0].get('ldc_org_name')
    current_app.logger.info("get_genisgs_data %s %s %s",
                            date, '***', discom)
    # ldc = 'ERLDC'
    # discom = 'BSEB'
    if ldc == 'ERLDC' and datatype == 'Drawl Schedule':
        datacursor = db.query_dictcursor("""SELECT
            DATE_FORMAT(a.date, '%%d-%%m-%%Y') AS Date,
            a.Revision AS Revision,
            a.Discom AS Discom,
            a.Block_No AS Block_No,
            SUM((CASE
                WHEN
                    ((a.Drawl_Type = 'ISGS')
                    AND (a.Station_Name <> 'NET|DRAWAL|SCHD.'))
                THEN
                    a.Schedule
            END)) AS ISGS,
            SUM((CASE
                WHEN
                    ((a.Drawl_Type = 'BILATERAL')
                        AND (a.Station_Name = 'BILAT|TOTAL'))
                THEN
                    a.Schedule
            END)) AS Bilateral,
            SUM((CASE
                WHEN (a.Drawl_Type = 'LTOA_MTOA')
                THEN a.Schedule
            END)) AS LTOA_MTOA
        FROM
            power.erldc_state_drawl_schedule_stg a
        WHERE a.Date = str_to_date(%s, '%%d-%%m-%%Y')
        and   a.Discom = %s
        and   a.Revision = %s
        and   a.Drawl_Type in
            ('ISGS', 'BILATERAL', 'LTOA_MTOA', 'IEX', 'PXIL')
        GROUP BY a.Date , a.Discom ,
                a.Revision , a.Block_No""", data=(date, discom, rev))
        # columns = ['Date', 'Revision', 'Discom', 'Block_No', 'ISGS',
        #            'LTOA_MTOA', 'Bilateral']
    elif ldc == 'ERLDC' and datatype == 'Entitlement':
        datacursor = db.query_dictcursor("""SELECT
            DATE_FORMAT(a.date, '%%d-%%m-%%Y') AS Date,
            a.Revision AS Revision,
            a.Discom AS Discom,
            a.Block_No AS Block_No,
            SUM((CASE
                WHEN (a.Station_Name <> 'NET|DRAWAL|SCHD.')
                THEN a.Schedule
                END)) AS ISGS,
            0 AS Bilateral,
            0 AS LTOA_MTOA
        FROM
            power.erldc_entitlements_stg a
        WHERE a.Date = str_to_date(%s, '%%d-%%m-%%Y')
        and   a.Discom = %s
        and   a.Revision = %s
        GROUP BY a.Date , a.Discom ,
                a.Revision , a.Block_No""", data=(date, discom, rev))
        # columns = ['Date', 'Revision', 'Discom', 'Block_No', 'ISGS',
        #            'LTOA_MTOA', 'Bilateral']
    elif ldc == 'NRLDC' and datatype == 'Drawl Schedule':    
        datacursor = db.query_dictcursor("""SELECT
            DATE_FORMAT(a.date, '%%d-%%m-%%Y') as Date,
            a.Revision AS Revision,
            a.Discom AS Discom,
            a.Block_No AS Block_No,
            SUM(CASE WHEN a.Drawl_Type = 'ISGS'
                THEN a.Schedule END) AS ISGS,
            SUM(CASE WHEN a.Drawl_Type = 'MTOA'
                THEN a.Schedule END) AS MTOA,
            SUM(CASE WHEN a.Drawl_Type = 'STOA'
                THEN a.Schedule END) AS STOA,
            SUM(CASE WHEN a.Drawl_Type = 'LTA'
                THEN a.Schedule END) AS LTA,
            SUM(CASE WHEN a.Drawl_Type = 'IEX'
                THEN a.Schedule END) AS IEX,
            SUM(CASE WHEN a.Drawl_Type = 'PXI'
                THEN a.Schedule END) AS PXI,
            SUM(CASE WHEN a.Drawl_Type = 'URS'
                THEN a.Schedule END) AS URS,
            SUM(CASE WHEN a.Drawl_Type = 'RRAS'
                THEN a.Schedule END) AS RRAS, 
            SUM(CASE WHEN a.Drawl_Type = 'SCED'
                THEN a.Schedule END) AS SCED,
            SUM(CASE WHEN a.Drawl_Type = 'REMC'
                THEN a.Schedule END) AS REMC,  
            SUM(CASE WHEN a.Drawl_Type = 'RTM_PXI'
                THEN a.Schedule END) AS RTM_PXI,  
            SUM(CASE WHEN a.Drawl_Type = 'RTM_IEX'
                THEN a.Schedule END) AS RTM_IEX                                                                                                                 
            from nrldc_state_drawl_schedule_stg a
            where a.date = str_to_date(%s, '%%d-%%m-%%Y')
            and a.revision = %s
            and a.state = %s
            group by a.Date, a.Revision, a.Discom,
                a.Block_No""", data=(date, rev, discom))
        # columns = ['Date', 'Revision', 'Discom', 'Block_No', 'ISGS',
        #            'LTOA', 'MTOA', 'Shared', 'Bilateral']
    elif ldc == 'NRLDC' and datatype == 'Entitlement':
        datacursor = db.query_dictcursor("""SELECT
            DATE_FORMAT(a.date, '%%d-%%m-%%Y') as Date,
            a.Revision AS Revision,
            a.Discom AS Discom,
            a.Block_No AS Block_No,
            SUM(a.schedule) AS ISGS,
            0 AS MTOA,
            0 AS STOA,
            0 AS LTA,
            0 AS IEX,
            0 AS PXI,
            0 AS URS,
            0 AS RRAS,
            0 AS SCED,
            0 AS REMC,
            0 AS RTM_PXI,
            0 AS RTM_IEX
            from nrldc_entitlements_stg a
            where a.date = str_to_date(%s, '%%d-%%m-%%Y')
            and a.revision = %s
            and a.state = %s
            group by a.Date, a.Revision, a.Discom,
                a.Block_No""", data=(date, rev, discom))
    elif ldc == 'WRLDC' and datatype == 'Drawl Schedule':
        datacursor = db.query_dictcursor("""SELECT DATE_FORMAT(a.date, '%%d-%%m-%%Y') as Date,
            a.Revision AS Revision,
            a.State AS Discom,
            a.Block_No AS Block_No,
            SUM(CASE WHEN a.Drawl_Type = 'ISGS'
                THEN a.Schedule END) AS ISGS,
            SUM(CASE WHEN a.Drawl_Type = 'LTA'
                THEN a.Schedule END) AS LTOA,
            SUM(CASE WHEN a.Drawl_Type = 'MTOA'
                THEN a.Schedule END) AS MTOA,
            SUM(CASE WHEN a.Drawl_Type = 'STOA'
                THEN a.Schedule END) AS STOA,
            SUM(CASE WHEN a.Drawl_Type = 'URS'
                THEN a.Schedule END) AS URS,
            SUM(CASE WHEN a.Drawl_Type = 'RRAS'
                THEN a.Schedule END) AS RRAS
            from power.wrldc_state_drawl_schedule_stg a
            where a.date = str_to_date(%s, '%%d-%%m-%%Y')
            and a.revision = %s
            and a.state = %s
            group by a.Date, a.Revision, a.State,
                a.Block_No""", data=(date, rev, discom))
    elif ldc == 'WRLDC' and datatype == 'Entitlement':
        datacursor = db.query_dictcursor("""SELECT
            DATE_FORMAT(a.date, '%%d-%%m-%%Y') as Date,
            a.Revision AS Revision,
            a.State AS Discom,
            a.Block_No AS Block_No,
            SUM(a.Schedule) AS ISGS,
            0 AS LTOA,
            0 AS MTOA,
            0 AS STOA,
            0 AS URS,
            0 AS RRAS
            from power.wrldc_entitlements_stg a
            where a.date = str_to_date(%s, '%%d-%%m-%%Y')
            and a.revision = %s
            and a.state = %s
            group by a.Date, a.Revision, a.State,
                a.Block_No""", data=(date, rev, discom))
        # columns = ['Date', 'Revision', 'Discom', 'Block_No', 'ISGS',
        #            'LTOA', 'MTOA', 'Shared', 'Bilateral']
    results = datacursor.fetchall()
    # datacursor = db.query_dictcursor("""SELECT FOUND_ROWS() as rows""")
    # rows = datacursor.fetchall()[0]['rows']
    db.close()
    # current_app.logger.info('Total Number of Rows: %s', rows)
    current_app.logger.debug('Result: %s', results)
    # dt_output = rawsql_to_datatables(columns, results, rows, rows)
    # # print json.dumps(dt_output)
    # return json.dumps(dt_output, use_decimal=True)
    return json.dumps(results, use_decimal=True)


@ems.route("/get_tengenisgs_data", methods=['POST'])
@login_required
def get_tengenisgs_data():
    date = request.form['date']
    # discom = request.form['discom']
    db = DB()
    # datacursor = con.cursor(cursors.DictCursor)
    datacursor = db.query_dictcursor("""SELECT
        ldc_name, ldc_org_name, organisation_code
        from power.org_isgs_map a,
        power.organisation_master b
        where a.organisation_master_fk = b.organisation_master_pk
        and (b.organisation_master_pk = %s
        or b.organisation_parent_fk = %s)
        and a.delete_ind = 0
        and b.delete_ind = 0""", data=(
        current_user.organisation_master_fk,
        current_user.organisation_master_fk))
    org = datacursor.fetchall()
    ldc = org[0].get('ldc_name')
    # ldc_discom = org[0].get('ldc_org_name')
    discom = org[0].get('organisation_code')
    current_app.logger.info("get_tengenisgs_data %s %s %s",
                            date, '***', discom)
    # ldc = 'ERLDC'
    # discom = 'BSEB'
    if ldc == 'ERLDC':
        # datacursor = db.query_dictcursor("""SELECT
        #     DATE_FORMAT(adddate(a.date, 1), '%%d-%%m-%%Y') AS Date,
        #     a.Revision AS Revision,
        #     a.Discom AS Discom,
        #     a.Block_No AS Block_No,
        #     SUM((CASE
        #         WHEN
        #             ((a.Drawl_Type = 'ISGS')
        #             AND (a.Station_Name <> 'NET|DRAWAL|SCHD.'))
        #         THEN
        #             a.Schedule
        #     END)) AS ISGS,
        #     SUM((CASE
        #         WHEN
        #             ((a.Drawl_Type = 'BILATERAL')
        #                 AND (a.Station_Name = 'BILAT|TOTAL'))
        #         THEN
        #             a.Schedule
        #     END)) AS Bilateral,
        #     SUM((CASE
        #         WHEN (a.Drawl_Type = 'LTOA_MTOA')
        #         THEN a.Schedule
        #     END)) AS LTOA_MTOA,
        #     SUM((CASE
        #         WHEN
        #             ((a.Drawl_Type = 'IEX')
        #                 AND (a.Station_Name = 'IEX|TOT.'))
        #         THEN
        #             a.Schedule
        #     END)) AS IEX,
        #     SUM((CASE
        #         WHEN
        #             ((a.Drawl_Type = 'PXIL')
        #                 AND (a.Station_Name = 'PXI|TOT.'))
        #         THEN
        #             a.Schedule
        #     END)) AS PXIL,
        #     SUM((CASE
        #         WHEN
        #             ((a.Drawl_Type = 'ISGS')
        #             AND (a.Station_Name = 'NET|DRAWAL|SCHD.'))
        #         THEN
        #             a.Schedule
        #     END)) AS Net_Sch,
        #     SUM((CASE
        #         WHEN (a.Drawl_Type = 'REGULATION')
        #         THEN a.Schedule
        #     END)) AS Regulation
        # FROM
        #     power.erldc_state_drawl_schedule_stg a,
        #     (
        #         select c.date, c.discom,
        #         min(c.revision) as revision
        #         from power.erldc_state_drawl_schedule_stg c
        #         where c.date = adddate(str_to_date(%s, '%%d-%%m-%%Y'), -1)
        #         and c.discom = %s
        #         group by c.date, c.discom
        #     ) b
        # WHERE a.Date = b.Date
        # and   a.Discom = b.Discom
        # and   a.Revision = b.Revision
        # and   a.Drawl_Type in
        #     ('ISGS', 'BILATERAL', 'LTOA_MTOA', 'IEX', 'PXIL')
        # GROUP BY a.Date , a.Discom ,
        #         a.Revision , a.Block_No""", data=(date, ldc_discom))
        datacursor = db.query_dictcursor("""SELECT
            DATE_FORMAT(coalesce(z.Date, y.Date), '%%d-%%m-%%Y') Date,
            z.Revision, coalesce(z.Discom, y.Discom) Discom,
            coalesce(z.Block_No, y.Block_No) Block_No, z.ISGS,
            z.LTOA_MTOA, z.Bilateral,  y.OPENACCESS, y.INTERNAL,
            z.ISGS + z.LTOA_MTOA + y.OPENACCESS + y.INTERNAL NET_SCH
            from
            (select a.Date, a.Block_No, a.Revision, a.Discom,
            round(sum(case when a.pool_name = 'ISGS'
                      then a.schedule else 0 end),2) ISGS,
            round(sum(case when a.pool_name = 'LTOA_MTOA'
                      then a.schedule else 0 end),2) LTOA_MTOA,
            round(sum(case when a.pool_name = 'Bilateral'
                      then a.schedule else 0 end),2) Bilateral
            from power.isgstentative_schedule_staging a,
                 (select date, discom, max(revision) max_revision
                  from power.isgstentative_schedule_staging
                  where date = str_to_date(%s, '%%d-%%m-%%Y')
                  and discom = %s
                  and pool_name not in ('OPT_SCHEDULE')
                  group by date, discom) b
            where a.date = b.date
            and a.discom = b.discom
            and a.revision = b.max_revision
            and a.pool_name not in ('OPT_SCHEDULE')
            group by a.date, a.block_no, a.revision, a.discom) z left join
            (select date, block_no, discom,
            round(sum(case when upper(generation_entity_name) = 'OPENACCESS'
                      then tentative_generation else 0 end),2) OPENACCESS,
            round(sum(case when upper(generation_entity_name) <> 'OPENACCESS'
                      then tentative_generation else 0 end),2) INTERNAL
            from power.tentative_schedule_staging
            where date = str_to_date(%s, '%%d-%%m-%%Y')
            and discom = %s
            and upper(generation_entity_name)
            not in ('ISGS', 'BILATERAL', 'MTOA',
                    'SHARED', 'BANKING', 'STOA', 'LTOA')
            group by date, block_no, discom) y
            on (z.date = y.date
            and z.block_no = y.block_no
            and z.discom = y.discom)
            union
            SELECT
            DATE_FORMAT(coalesce(z.Date, y.Date), '%%d-%%m-%%Y') Date,
            z.Revision, coalesce(z.Discom, y.Discom) Discom,
            coalesce(z.Block_No, y.Block_No) Block_No, z.ISGS,
            z.LTOA_MTOA, z.Bilateral,  y.OPENACCESS, y.INTERNAL,
            z.ISGS + z.LTOA_MTOA + y.OPENACCESS + y.INTERNAL NET_SCH
            from
            (select a.Date, a.Block_No, a.Revision, a.Discom,
            round(sum(case when a.pool_name = 'ISGS'
                      then a.schedule else 0 end),2) ISGS,
            round(sum(case when a.pool_name = 'LTOA_MTOA'
                      then a.schedule else 0 end),2) LTOA_MTOA,
            round(sum(case when a.pool_name = 'Bilateral'
                      then a.schedule else 0 end),2) Bilateral
            from power.isgstentative_schedule_staging a,
                 (select date, discom, max(revision) max_revision
                  from power.isgstentative_schedule_staging
                  where date = str_to_date(%s, '%%d-%%m-%%Y')
                  and discom = %s
                  and pool_name not in ('OPT_SCHEDULE')
                  group by date, discom) b
            where a.date = b.date
            and a.discom = b.discom
            and a.revision = b.max_revision
            and a.pool_name not in ('OPT_SCHEDULE')
            group by a.date, a.block_no, a.revision, a.discom) z left join
            (select date, block_no, discom,
            round(sum(case when upper(generation_entity_name) = 'OPENACCESS'
                      then tentative_generation else 0 end),2) OPENACCESS,
            round(sum(case when upper(generation_entity_name) <> 'OPENACCESS'
                      then tentative_generation else 0 end),2) INTERNAL
            from power.tentative_schedule_staging
            where date = str_to_date(%s, '%%d-%%m-%%Y')
            and discom = %s
            and upper(generation_entity_name)
            not in ('ISGS', 'BILATERAL', 'MTOA',
                    'SHARED', 'BANKING', 'STOA', 'LTOA')
            group by date, block_no, discom) y
            on (z.date = y.date
            and z.block_no = y.block_no
            and z.discom = y.discom)""", data=(date, discom, date, discom,
                                               date, discom, date, discom))
        columns = ['Date', 'Revision', 'Discom', 'Block_No', 'ISGS',
                   'LTOA_MTOA', 'Bilateral', 'OPENACCESS',
                   'INTERNAL', 'NET_SCH']
    elif ldc == 'NRLDC':
        # datacursor = db.query_dictcursor("""SELECT
        #     DATE_FORMAT(adddate(a.date, 1), '%%d-%%m-%%Y') as Date,
        #     a.Revision AS Revision,
        #     a.State AS Discom,
        #     a.Block_No AS Block_No,
        #     @isgs := SUM(CASE WHEN a.Drawl_Type = 'ISGS'
        #         THEN a.Schedule END) AS ISGS,
        #     @ltoa := SUM(CASE WHEN a.Drawl_Type = 'LTA'
        #         THEN a.Schedule END) AS LTOA,
        #     @mtoa := SUM(CASE WHEN a.Drawl_Type = 'MTOA'
        #         THEN a.Schedule END) AS MTOA,
        #     @shared := SUM(CASE WHEN a.Drawl_Type = 'Shared'
        #         THEN a.Schedule END) AS Shared,
        #     @biltarel := SUM(CASE WHEN a.Drawl_Type = 'Bilateral'
        #         THEN a.Schedule END) AS Bilateral,
        #     @iex := SUM(CASE WHEN a.Drawl_Type = 'IEX_PXIL'
        #         AND a.Head1 in ("(IEX Drawal)", "(IEX Injection)")
        #         THEN a.Schedule END) AS IEX,
        #     @pxil := SUM(CASE WHEN a.Drawl_Type = 'IEX_PXIL'
        #         AND a.Head1 in ("(PXIL Drawal)", "(PXIL Injection)")
        #         THEN a.Schedule END) AS PXIL,
        #     ifnull(@isgs, 0) + ifnull(@ltoa, 0) + ifnull(@mtoa, 0) +
        #         ifnull(@shared, 0) + ifnull(@bilateral, 0) +
        #         ifnull(@iex, 0) + ifnull(@pxil, 0) AS NET_SCH
        #     from power.panel_nrldc_state_drawl_schedule a,
        #          (select date as mindate, state as minstate,
        #          min(Revision) as minrev
        #          from
        #          power.panel_nrldc_state_drawl_schedule
        #          where date = adddate(str_to_date(%s, '%%d-%%m-%%Y'), -1)
        #          and state = %s
        #          and revision = 0
        #          group by date, state) b
        #     where a.date = b.mindate
        #     and a.revision = b.minrev
        #     and a.state = b.minstate
        #     group by a.Date, a.Revision, a.State,
        #         a.Block_No""", data=(date, discom))
        # datacursor = db.query_dictcursor("""SELECT
        #     DATE_FORMAT(coalesce(z.Date, y.Date), '%%d-%%m-%%Y') Date,
        #     z.Revision, coalesce(z.Discom, y.Discom) Discom,
        #     coalesce(z.Block_No, y.Block_No) Block_No,
        #     round(z.ISGS * ((100 - x.loss_perc)/100),2) ISGS,
        #     z.LTOA, z.MTOA, z.Shared, z.Bilateral,  y.OPENACCESS, y.INTERNAL,
        #     round(z.ISGS * ((100 - x.loss_perc)/100),2) + z.LTOA + z.MTOA +
        #     z.Shared + z.Bilateral +
        #     y.OPENACCESS + y.INTERNAL NET_SCH
        #     from
        #     (select a.Date, a.Block_No, a.Revision, a.Discom,
        #     round(sum(case when a.pool_name = 'ISGS'
        #               then a.schedule else 0 end),2) ISGS,
        #     round(sum(case when a.pool_name = 'LTOA'
        #               then a.schedule else 0 end),2) LTOA,
        #     round(sum(case when a.pool_name = 'MTOA'
        #               then a.schedule else 0 end),2) MTOA,
        #     round(sum(case when a.pool_name = 'Bilateral'
        #               then a.schedule else 0 end),2) Bilateral,
        #     round(sum(case when a.pool_name = 'Shared'
        #               then a.schedule else 0 end),2) Shared
        #     from power.isgstentative_schedule_staging a,
        #          (select date, discom, max(revision) max_revision
        #           from power.isgstentative_schedule_staging
        #           where date = str_to_date(%s, '%%d-%%m-%%Y')
        #           and discom = %s
        #           and pool_name not in ('OPT_SCHEDULE')
        #           group by date, discom) b
        #     where a.date = b.date
        #     and a.discom = b.discom
        #     and a.revision = b.max_revision
        #     and a.pool_name not in ('OPT_SCHEDULE')
        #     group by a.date, a.block_no, a.revision, a.discom) z left join
        #     (select date, block_no, discom,
        #     round(sum(case when upper(generation_entity_name) = 'OPENACCESS'
        #               then tentative_generation else 0 end),2) OPENACCESS,
        #     round(sum(case when upper(generation_entity_name) <> 'OPENACCESS'
        #               then tentative_generation else 0 end),2) INTERNAL
        #     from power.tentative_schedule_staging
        #     where date = str_to_date(%s, '%%d-%%m-%%Y')
        #     and discom = %s
        #     and upper(generation_entity_name)
        #     not in ('ISGS', 'BILATERAL', 'MTOA',
        #             'SHARED', 'BANKING', 'STOA', 'LTOA')
        #     group by date, block_no, discom) y
        #     on (z.date = y.date
        #     and z.block_no = y.block_no
        #     and z.discom = y.discom) left join
        #     (select case when count(loss_perc)=0
        #             then 0 else loss_perc end  loss_perc
        #     from power.nrldc_est_trans_loss_stg
        #     where start_date <= str_to_date(%s, '%%d-%%m-%%Y')
        #     and end_date >= str_to_date(%s, '%%d-%%m-%%Y')) x
        #     on (1 = 1)
        #     union
        #     SELECT
        #     DATE_FORMAT(coalesce(z.Date, y.Date), '%%d-%%m-%%Y') Date,
        #     z.Revision, coalesce(z.Discom, y.Discom) Discom,
        #     coalesce(z.Block_No, y.Block_No) Block_No,
        #     round(z.ISGS * ((100 - x.loss_perc)/100),2) ISGS,
        #     z.LTOA, z.MTOA, z.Shared, z.Bilateral,  y.OPENACCESS, y.INTERNAL,
        #     round(z.ISGS * ((100 - x.loss_perc)/100),2)  + z.LTOA + z.MTOA +
        #     z.Shared + z.Bilateral +
        #     y.OPENACCESS + y.INTERNAL NET_SCH
        #     from
        #     (select a.Date, a.Block_No, a.Revision, a.Discom,
        #     round(sum(case when a.pool_name = 'ISGS'
        #               then a.schedule else 0 end),2) ISGS,
        #     round(sum(case when a.pool_name = 'LTOA'
        #               then a.schedule else 0 end),2) LTOA,
        #     round(sum(case when a.pool_name = 'MTOA'
        #               then a.schedule else 0 end),2) MTOA,
        #     round(sum(case when a.pool_name = 'Bilateral'
        #               then a.schedule else 0 end),2) Bilateral,
        #     round(sum(case when a.pool_name = 'Shared'
        #               then a.schedule else 0 end),2) Shared
        #     from power.isgstentative_schedule_staging a,
        #          (select date, discom, max(revision) max_revision
        #           from power.isgstentative_schedule_staging
        #           where date = str_to_date(%s, '%%d-%%m-%%Y')
        #           and discom = %s
        #           and pool_name not in ('OPT_SCHEDULE')
        #           group by date, discom) b
        #     where a.date = b.date
        #     and a.discom = b.discom
        #     and a.revision = b.max_revision
        #     and a.pool_name not in ('OPT_SCHEDULE')
        #     group by a.date, a.block_no, a.revision, a.discom) z right join
        #     (select date, block_no, discom,
        #     round(sum(case when upper(generation_entity_name) = 'OPENACCESS'
        #               then tentative_generation else 0 end),2) OPENACCESS,
        #     round(sum(case when upper(generation_entity_name) <> 'OPENACCESS'
        #               then tentative_generation else 0 end),2) INTERNAL
        #     from power.tentative_schedule_staging
        #     where date = str_to_date(%s, '%%d-%%m-%%Y')
        #     and discom = %s
        #     and upper(generation_entity_name)
        #     not in ('ISGS', 'BILATERAL', 'MTOA',
        #             'SHARED', 'BANKING', 'STOA', 'LTOA')
        #     group by date, block_no, discom) y
        #     on (z.date = y.date
        #     and z.block_no = y.block_no
        #     and z.discom = y.discom) left join
        #     (select case when count(loss_perc)=0
        #             then 0 else loss_perc end  loss_perc
        #     from power.nrldc_est_trans_loss_stg
        #     where start_date <= str_to_date(%s, '%%d-%%m-%%Y')
        #     and end_date >= str_to_date(%s, '%%d-%%m-%%Y')) x
        #     on (1 = 1)""", data=(date, discom, date, discom, date, date,
        #                          date, discom, date, discom, date, date))
        # columns = ['Date', 'Revision', 'Discom', 'Block_No', 'ISGS',
        #            'LTOA', 'MTOA', 'Shared', 'Bilateral', 'OPENACCESS',
        #            'INTERNAL', 'NET_SCH']
        datacursor = db.query_dictcursor("""SELECT
            DATE_FORMAT(z.Date, '%%d-%%m-%%Y') Date,
            z.Revision, z.Discom Discom,
            z.Block_No Block_No,
            z.ISGS, z.MTOA, z.STOA, z.LTA, z.IEX, z.PXI,
            z.URS, z.RRAS, z.SCED, z.REMC, z.RTM_PXI, z.RTM_IEX,
            z.INTERNAL,
            z.ISGS + coalesce(z.MTOA, 0) + coalesce(z.STOA, 0) +
            coalesce(z.LTA, 0) + coalesce(z.IEX, 0) +  coalesce(z.PXI, 0) +
            coalesce(z.URS, 0)  + coalesce(z.RRAS, 0) + coalesce(z.SCED, 0) +
            coalesce(z.REMC, 0)  + coalesce(z.RTM_PXI, 0) + 
            coalesce(z.RTM_IEX, 0) + coalesce(z.INTERNAL, 0) NET_SCH
            from
            (select a.Date, a.Block_No, max(a.Revision) Revision, a.Discom,
            round(sum(CASE WHEN a.pool_name = 'ISGS'
                THEN a.Schedule END),2) AS ISGS,
            round(sum(CASE WHEN a.pool_name = 'MTOA'
                THEN a.Schedule END),2) AS MTOA,
            round(sum(CASE WHEN a.pool_name = 'STOA'
                THEN a.Schedule END),2) AS STOA,
            round(sum(CASE WHEN a.pool_name = 'LTA'
                THEN a.Schedule END),2) AS LTA,
            round(sum(CASE WHEN a.pool_name = 'IEX'
                THEN a.Schedule END),2) AS IEX,
            round(sum(CASE WHEN a.pool_name = 'PXI'
                THEN a.Schedule END),2) AS PXI,
            round(sum(CASE WHEN a.pool_name = 'URS'
                THEN a.Schedule END),2) AS URS,
            round(sum(CASE WHEN a.pool_name = 'RRAS'
                THEN a.Schedule END),2) AS RRAS, 
            round(sum(CASE WHEN a.pool_name = 'SCED'
                THEN a.Schedule END),2) AS SCED,
            round(sum(CASE WHEN a.pool_name = 'REMC'
                THEN a.Schedule END),2) AS REMC,  
            round(sum(CASE WHEN a.pool_name = 'RTM_PXI'
                THEN a.Schedule END),2) AS RTM_PXI,  
            round(sum(CASE WHEN a.pool_name = 'RTM_IEX'
                THEN a.Schedule END),2) AS RTM_IEX,    
            round(sum(case when a.pool_name = 'INT_GENERATION_ACT'
                      then a.schedule else 0 end),2) INTERNAL
            from power.isgstentative_schedule_staging a,
                 (select date, discom, pool_name, max(revision) max_revision
                  from power.isgstentative_schedule_staging
                  where date = str_to_date(%s, '%%d-%%m-%%Y')
                  and discom = %s
                  and pool_name not in ('OPT_SCHEDULE')
                  group by date, discom, pool_name) b
            where a.date = b.date
            and a.discom = b.discom
            and a.revision = b.max_revision
            and a.pool_name = b.pool_name
            and a.pool_name not in ('OPT_SCHEDULE')
            group by a.date, a.block_no, a.discom) z
            """, data=(date, discom))
        columns = ['Date', 'Revision', 'Discom', 'Block_No', 'ISGS',
                   'MTOA', 'STOA', 'LTA', 'IEX', 'PXI',  'URS', 'RRAS',
                   'SCED', 'REMC', 'RTM_PXI', 'RTM_IEX',
                   'INTERNAL', 'NET_SCH']        
    elif ldc == 'WRLDC':
        datacursor = db.query_dictcursor("""SELECT
            DATE_FORMAT(z.Date, '%%d-%%m-%%Y') Date,
            z.Revision, z.Discom Discom,
            z.Block_No Block_No,
            z.ISGS,
            z.LTOA, z.MTOA, z.STOA, z.URS, z.RRAS,
            z.INTERNAL,
            z.ISGS + coalesce(z.LTOA, 0) + coalesce(z.MTOA, 0) +
            coalesce(z.STOA, 0) + coalesce(z.URS, 0) +  coalesce(z.RRAS, 0) +
            coalesce(z.INTERNAL, 0)  NET_SCH
            from
            (select a.Date, a.Block_No, max(a.Revision) Revision, a.Discom,
            round(sum(case when a.pool_name = 'ISGS'
                      then a.schedule else 0 end),2) ISGS,
            round(sum(case when a.pool_name = 'LTOA'
                      then a.schedule else 0 end),2) LTOA,
            round(sum(case when a.pool_name = 'MTOA'
                      then a.schedule else 0 end),2) MTOA,
            round(sum(case when a.pool_name = 'STOA'
                      then a.schedule else 0 end),2) STOA,
            round(sum(case when a.pool_name = 'URS'
                      then a.schedule else 0 end),2) URS,
            round(sum(case when a.pool_name = 'RRAS'
                      then a.schedule else 0 end),2) RRAS,
            round(sum(case when a.pool_name = 'INT_GENERATION_ACT'
                      then a.schedule else 0 end),2) INTERNAL
            from power.isgstentative_schedule_staging a,
                 (select date, discom, pool_name, max(revision) max_revision
                  from power.isgstentative_schedule_staging
                  where date = str_to_date(%s, '%%d-%%m-%%Y')
                  and discom = %s
                  and pool_name not in ('OPT_SCHEDULE')
                  group by date, discom, pool_name) b
            where a.date = b.date
            and a.discom = b.discom
            and a.revision = b.max_revision
            and a.pool_name = b.pool_name
            and a.pool_name not in ('OPT_SCHEDULE')
            group by a.date, a.block_no, a.discom) z
            """, data=(date, discom))
        columns = ['Date', 'Revision', 'Discom', 'Block_No', 'ISGS',
                   'LTOA', 'MTOA', 'STOA', 'URS', 'RRAS',
                   'INTERNAL', 'NET_SCH']
    results = datacursor.fetchall()
    datacursor = db.query_dictcursor("""SELECT FOUND_ROWS() _rows""")
    rows = datacursor.fetchall()[0]['_rows']
    db.close()
    current_app.logger.info('Total Number of Rows: %s', rows)
    current_app.logger.debug('Result: %s', results)
    dt_output = rawsql_to_datatables(columns, results, rows, rows)
    # print json.dumps(output)
    return json.dumps(dt_output)


@ems.route('/post_tenintsch_data', methods=['POST'])
@roles_required(['admin', 'jobrunner', 'analytics'])
def post_tenintsch_data():
    current_app.logger.debug('***** started post_tenintsch_data')
    # current_app.logger.debug("post tenisgsgen %s", request.json)
    if request.json:
        current_app.logger.info('INside ***post_tenintsch_data')
        tenisgsgen_data = json.loads(json.dumps(request.json))
        # current_app.logger.info('INside *** %s', tenisgsgen_data)
        data = tenisgsgen_data.get('data')
        category = tenisgsgen_data.get('category')
        date_for = tenisgsgen_data.get('date_for')
        rev = data[0].get('Revision')
        date_used = data[0].get('Date')
        discom = data[0].get('Discom')
        current_app.logger.info("""date_used %s discom %s
            revision %s category %s date_for %s""",
                                date_used, discom,
                                rev, category, date_for)
        db = DB()
        datacursor = db.query_dictcursor("""SELECT
            b.state_name, c.organisation_code,
            a.ldc_name, a.ldc_org_name
            from power.org_isgs_map a,
                 power.state_master b,
                 power.organisation_master c
            where c.state_master_fk = b.state_master_pk
            and a.organisation_master_fk = c.organisation_master_pk
            and a.delete_ind = 0
            and b.delete_ind = 0
            and c.delete_ind = 0
            and (c.organisation_master_pk = %s
            or c.organisation_parent_fk = %s)""", data=(
            current_user.organisation_master_fk,
            current_user.organisation_master_fk))
        org = datacursor.fetchall()
        db.close()
        ldc = org[0].get('ldc_name')
        state = org[0].get('state_name')
        discom = org[0].get('organisation_code')
        current_app.logger.debug('***** %s %s %s %s %s',
                                 ldc, state, discom, category, date_for)
        db = DB()
        if category == 'Declared Capacity':
            datacursor = db.query_dictcursor("""select 
                DATE_FORMAT(a.date, '%%d-%%m-%%Y') AS Date, 
                a.block_No AS Block_No, 
                'INTERNAL_SCH' as Drawl_type,
                a.entity_name as  Station_Name,     
                a.schedule as Schedule
                from internal_declared_capacity_stg a
                where a.date = str_to_date(%s, '%%d-%%m-%%Y')
                and a.revision = %s
                and a.discom = %s""", data=(date_used, rev, discom))                
        elif category == 'Drawl Schedule':
            datacursor = db.query_dictcursor("""select 
                DATE_FORMAT(a.date, '%%d-%%m-%%Y') AS Date, 
                a.block_No AS Block_No,
                'INTERNAL_SCH' as Drawl_type,
                a.entity_name as  Station_Name,            
                a.schedule as Schedule
                from internal_drawl_schedule_stg a
                where a.date = str_to_date(%s, '%%d-%%m-%%Y')
                and a.revision = %s
                and a.discom = %s""", data=(date_used, rev, discom))                      
        else:
            raise Exception('Undefined and unknown category!')
        intsch_data = datacursor.fetchall()
        datacursor = db.query_dictcursor("""SELECT
            coalesce(max(revision)+1, 0) as new_revision
            from power.isgstentative_schedule_staging
            where date = STR_TO_DATE(%s,'%%d-%%m-%%Y')
            and state = %s
            and discom = %s
            and pool_name = 'INT_GENERATION_ACT'""", data=(date_for, state, discom))
        rev = datacursor.fetchall()
        newrev = rev[0].get('new_revision')
        # current_app.logger.debug("***** isgsdata %s", isgs_data)
        db.close()
        genkeys = {'internal_sch': ('ForDate', 'Block_No', 'ForRevision',
                            'Drawl_type', 'Generation_Type',
                            'Station_Name', 'Schedule', 'ActDiscom', 'State',
                            'Added_By_FK'),
                   'internal_others': ('ForDate', 'Block_No', 'ForRevision',
                                 'Generation_Entity_Name', 'Generation_Type',
                                 'Generator_Name', 'INTERNAL_OTHERS', 'ActDiscom',
                                 'State','Added_By_FK')}

        internal_sch_data = [list(el.get(key) for key in genkeys.get('internal_sch'))
                     for el in intsch_data]
        # current_app.logger.debug(isgs_data)
        for el in internal_sch_data:
            el[0] = date_for
            el[2] = newrev
            el[3] = 'INT_GENERATION_ACT'
            el[4] = 'UNKNOWN'
            el[7] = discom
            el[8] = state
            el[9] = current_user.id

        internal_others_data = [list(el.get(key) for key in genkeys.get('internal_others'))
                        for el in data]
        for el in internal_others_data:
            el[0] = date_for
            el[2] = newrev
            el[3] = 'INT_GENERATION_ACT'
            el[4] = 'UNKNOWN'
            el[5] = 'Others'
            el[7] = discom
            el[8] = state
            el[9] = current_user.id
                                                                                    
            upload_data = internal_sch_data[:]
            upload_data.extend(internal_others_data)
           
        # current_app.logger.debug('upload_data %s',upload_data)
        # Change the lists of lists to list of tuples using map
        data = list(map(tuple, upload_data))
        current_app.logger.debug("****data to upload***%s", data[0:3])
        sql = """INSERT INTO power.isgstentative_schedule_staging
                (date,
                block_no,
                revision,
                pool_name,
                pool_type,
                generator_name,
                schedule,
                discom,
                state,
                added_by_fk)
                VALUES
                (STR_TO_DATE(%s,'%%d-%%m-%%Y'),
                 %s,
                 %s,
                 %s,
                 %s,
                 %s,
                 ROUND(%s,3),
                 %s,
                 %s,
                 %s)"""
        try:
            current_app.logger.debug(sql % data[0])
            db = DB()
            db.query_dictcursor(sql, 'insert', data)
            db.query_commit()
            # db.cur.commit()
            db.close()
            current_app.logger.info('Inserted...')
        except Exception as error:
            # if db.cur.open:
            # db.cur.rollback()
            db.query_rollback()
            db.close()
            current_app.logger.error("Error during Tentative ISGS update %s",
                                     error)
            raise
        return json.dumps({'success': True}), 200,\
            {'ContentType': 'application/json'}


@ems.route("/get_int_sch_data", methods=['POST'])
@login_required
def get_int_sch_data():
    current_app.logger.info("get_int_sch_data %s %s",
                            request.json, request.form)
    datatype = request.json['datatype']
    date = request.json['date']
    rev = request.json['rev']
    current_app.logger.debug("get_tenint_sch_data %s %s %s",
                             datatype, date, rev)
    db = DB()
    # datacursor = con.cursor(cursors.DictCursor)
    datacursor = db.query_dictcursor("""SELECT
        ldc_name, ldc_org_name, organisation_code
        from power.org_isgs_map a,
        power.organisation_master b
        where a.organisation_master_fk = b.organisation_master_pk
        and (b.organisation_master_pk = %s
        or b.organisation_parent_fk = %s)
        and a.delete_ind = 0
        and b.delete_ind = 0""", data=(
        current_user.organisation_master_fk,
        current_user.organisation_master_fk))
    org = datacursor.fetchall()
    ldc = org[0].get('ldc_name')
    discom = org[0].get('organisation_code')
    current_app.logger.info("get_tenint_sch_data %s %s %s",
                            date, '***', discom)

    if datatype == 'Declared Capacity':
        datacursor = db.query_dictcursor("""select 
            DATE_FORMAT(a.date, '%%d-%%m-%%Y') AS Date, 
            a.revision AS Revision,
            a.discom AS Discom,
            a.block_No AS Block_No,            
            sum(a.schedule) as INTERNAL_SCH,
            0 as INTERNAL_OTHERS
            from internal_declared_capacity_stg a
            where a.date = str_to_date(%s, '%%d-%%m-%%Y')
            and a.revision = %s
            and a.discom = %s            
            group by date, revision, discom, block_no""", data=(date, rev, discom))
    elif datatype == 'Drawl Schedule':
        datacursor = db.query_dictcursor("""select 
            DATE_FORMAT(a.date, '%%d-%%m-%%Y') AS Date, 
            a.revision AS Revision,
            a.discom AS Discom,
            a.block_No AS Block_No,            
            sum(a.schedule) as INTERNAL_SCH,
            0 as INTERNAL_OTHERS
            from internal_drawl_schedule_stg a
            where a.date = str_to_date(%s, '%%d-%%m-%%Y')
            and a.revision = %s
            and a.discom = %s            
            group by date, revision, discom, block_no""", data=(date, rev, discom))        
    results = datacursor.fetchall()
    # datacursor = db.query_dictcursor("""SELECT FOUND_ROWS() as rows""")
    # rows = datacursor.fetchall()[0]['rows']
    db.close()
    # current_app.logger.info('Total Number of Rows: %s', rows)
    current_app.logger.debug('Result: %s', results)
    # dt_output = rawsql_to_datatables(columns, results, rows, rows)
    # # print json.dumps(dt_output)
    # return json.dumps(dt_output, use_decimal=True)
    return json.dumps(results, use_decimal=True)

@ems.route("/get_allocation_optimisation_data", methods=['POST'])
@login_required
def get_allocation_optimisation_data():
    date = request.json['date']
    discom = request.json['discom']
    # transaction_cost = request.json['tc']
    # alpha = request.json['alpha']
    # max_surrender_vol = request.json['maxsurr']
    # minimum_cont_block = request.json['mincontblk']
    current_app.logger.info("get_allocation_optimisation_data %s %s",
                            date, discom)
    db = DB()
    userid = current_user.organisation_master_fk
    datacursor = db.query_dictcursor("""SELECT
        ldc_name, ldc_org_name, d.zone_code,
        b.organisation_code discom
        from power.org_isgs_map a,
             power.organisation_master b,
             power.state_master c,
             power.zone_master d
        where a.organisation_master_fk = b.organisation_master_pk
        and c.state_master_pk = b.state_master_fk
        and c.exchange_zone_master_fk = d.zone_master_pk
        and (b.organisation_master_pk = %s
            or b.organisation_parent_fk = %s)
        and a.delete_ind = 0
        and b.delete_ind = 0
        and c.delete_ind = 0
        and d.delete_ind = 0""", data=(userid, userid))
    results = datacursor.fetchone()
    db.close()
    discom = results.get('discom')
    db = DB()
    datacursor = db.query_dictcursor("""SELECT
        DATE_FORMAT(optsch.date, '%%d-%%m-%%Y') date,
        optsch.block_no,
        optsch.revision,
        optsch.generator_name,
        optsch.schedule + inisch.schedule schedule
        from
            (select date, discom, pool_name, max(revision) max_revision
            from isgstentative_schedule_staging
            where date = str_to_date(%s, '%%d-%%m-%%Y')
            and discom = %s
            and pool_name = 'OPT_SCHEDULE'
            group by date, discom, pool_name) optmaxrev,
            (select date, block_no, discom,
             revision, generator_name,
             pool_name, 0 - schedule schedule
            from isgstentative_schedule_staging
            where date = str_to_date(%s, '%%d-%%m-%%Y')
            and discom = %s
            and pool_name = 'OPT_SCHEDULE') optsch,
            (select date, discom, pool_name, max(revision) max_revision
            from isgstentative_schedule_staging
            where date = str_to_date(%s, '%%d-%%m-%%Y')
            and discom = %s
            and pool_name != 'OPT_SCHEDULE'
            group by date, discom, pool_name) inimaxrev,
            (select date, block_no, discom,
             revision, generator_name, pool_name, schedule schedule
            from isgstentative_schedule_staging
            where date = str_to_date(%s, '%%d-%%m-%%Y')
            and discom = %s
            and pool_name != 'OPT_SCHEDULE') inisch,
            (select date, revision, generator_name, sum(schedule) sumschedule
             from isgstentative_schedule_staging
            where date = str_to_date(%s, '%%d-%%m-%%Y')
            and discom = %s
            and pool_name != 'OPT_SCHEDULE'
            group by date, revision, generator_name
            ) sinisch,
            (select date, revision, generator_name,
             0 - sum(schedule) sumschedule
             from isgstentative_schedule_staging
            where date = str_to_date(%s, '%%d-%%m-%%Y')
            and discom = %s
            and pool_name = 'OPT_SCHEDULE'
            group by date, revision, generator_name
            ) soptsch
        where optmaxrev.date = optsch.date
        and optmaxrev.discom = optsch.discom
        and optmaxrev.pool_name = optsch.pool_name
        and optmaxrev.max_revision = optsch.revision
        and inimaxrev.date = inisch.date
        and inimaxrev.discom = inisch.discom
        and inimaxrev.pool_name = inisch.pool_name
        and inimaxrev.max_revision = inisch.revision
        and optmaxrev.date = inimaxrev.date
        and optsch.generator_name = inisch.generator_name
        and optsch.block_no = inisch.block_no
        and soptsch.revision = optmaxrev.max_revision
        and sinisch.revision = inimaxrev.max_revision
        and soptsch.generator_name = sinisch.generator_name
        and sinisch.sumschedule + soptsch.sumschedule > 0
        and optsch.generator_name = soptsch.generator_name
        order by optsch.date, optsch.block_no,
         optsch.revision, optsch.generator_name
        """, data=(date, discom, date, discom,
                   date, discom, date, discom,
                   date, discom, date, discom))
    opt_schedule = datacursor.fetchall()
    db.close()
    # print opt_schedule
    return json.dumps(opt_schedule, use_decimal=True)


@ems.route("/get_trade_data", methods=['POST'])
@login_required
def get_trade_data():
    date = request.json['date']
    demandmodel = request.json['demandmodel']
    genmodel = request.json['genmodel']
    discom = request.json['discom']
    flag = request.json['flag']
    current_app.logger.info('get_trade_data %s %s %s %s %s',
                            date, demandmodel, genmodel, discom, flag)
    db = DB()
    userid = current_user.organisation_master_fk
    datacursor = db.query_dictcursor("""SELECT
        ldc_name, ldc_org_name, d.zone_code,
        b.organisation_code discom
        from power.org_isgs_map a,
             power.organisation_master b,
             power.state_master c,
             power.zone_master d
        where a.organisation_master_fk = b.organisation_master_pk
        and c.state_master_pk = b.state_master_fk
        and c.exchange_zone_master_fk = d.zone_master_pk
        and (b.organisation_master_pk = %s
            or b.organisation_parent_fk = %s)
        and a.delete_ind = 0
        and b.delete_ind = 0
        and c.delete_ind = 0
        and d.delete_ind = 0""", data=(userid, userid))
    results = datacursor.fetchone()
    discom = results.get('discom')
    current_app.logger.info('get_trade_data %s', discom)
    current_app.logger.info('get_trade_data %s', type(flag))
    if not flag:
        current_app.logger.info('get_trade_data %s', 'False')
        datacursor = db.query_dictcursor("""SELECT
            DATE_FORMAT(a.date, '%%d-%%m-%%Y') Date,
            a.block_no Block_No,
            a.discom Discom,
            a.demand_model_name Demand_Model_Name,
            a.gen_model_name Gen_Model_Name,
            a.alpha Alpha,
            a.ladder Ladder,
            a.buy_sell Buy_Sell,
            a.ladder_volume Ladder_Volume,
            a.bid_price Bid_Price,
            a.bid_volume Bid_Volume,
            a.revision Revision
            from trade_staging a,
            (select date, discom,
             demand_model_name, gen_model_name,
             max(revision) max_revision
             from trade_staging
             where date = str_to_date(%s, '%%d-%%m-%%Y')
             and discom = %s
             and demand_model_name = %s
             and gen_model_name = %s
             group by date, discom, demand_model_name, gen_model_name
             ) b
            where a.date = b.date
            and a.discom = b.discom
            and a.demand_model_name = b.demand_model_name
            and a.gen_model_name = b.gen_model_name
            and a.revision = b.max_revision
            order by a.date, a.block_no, a.ladder""",
                                         data=(date, discom,
                                               demandmodel, genmodel))
    elif flag:
        current_app.logger.info('get_trade_data %s', 'True')
        datacursor = db.query_dictcursor("""SELECT
            DATE_FORMAT(a.date, '%%d-%%m-%%Y') Date,
            a.block_no Block_No,
            a.discom Discom,
            a.demand_model_name Demand_Model_Name,
            a.gen_model_name Gen_Model_Name,
            a.alpha Alpha,
            a.ladder Ladder,
            a.buy_sell Buy_Sell,
            a.ladder_volume Ladder_Volume,
            a.bid_price Bid_Price,
            a.bid_volume Bid_Volume,
            a.revision Revision
            from trade_staging_tmp a,
            (select date, discom,
             demand_model_name, gen_model_name,
             max(revision) max_revision
             from trade_staging_tmp
             where date = str_to_date(%s, '%%d-%%m-%%Y')
             and discom = %s
             and demand_model_name = %s
             and gen_model_name = %s
             group by date, discom, demand_model_name, gen_model_name
             ) b
            where a.date = b.date
            and a.discom = b.discom
            and a.demand_model_name = b.demand_model_name
            and a.gen_model_name = b.gen_model_name
            and a.revision = b.max_revision
            order by a.date, a.block_no, a.ladder""",
                                         data=(date, discom,
                                               demandmodel, genmodel))
    results = datacursor.fetchall()
    db.close()
    return json.dumps(results, use_decimal=True)


@ems.route("/post_trade_data", methods=['POST'])
@roles_required(['admin', 'jobrunner', 'analytics'])
def post_trade_data():
    current_app.logger.info('Starting post_trade_data')
    if request.json:
        trade_data = json.loads(json.dumps(request.json))
        # current_app.logger.info('INside *** %s', tenisgsgen_data)
        post_trade_data = trade_data.get('data')
        date = trade_data.get('date')
        demandmodel = trade_data.get('demandmodel')
        genmodel = trade_data.get('genmodel')
        discom = trade_data.get('discom')
        current_app.logger.info('post_trade_data %s %s %s %s',
                                date, demandmodel, genmodel, discom)
        current_app.logger.info('post_trade_data %s', trade_data)
        db = DB()
        # userid = current_user.organisation_master_fk
        # datacursor = db.query_dictcursor("""SELECT
        #     ldc_name, ldc_org_name, d.zone_code,
        #     b.organisation_code discom
        #     from power.org_isgs_map a,
        #          power.organisation_master b,
        #          power.state_master c,
        #          power.zone_master d
        #     where a.organisation_master_fk = b.organisation_master_pk
        #     and c.state_master_pk = b.state_master_fk
        #     and c.exchange_zone_master_fk = d.zone_master_pk
        #     and (b.organisation_master_pk = %s
        #         or b.organisation_parent_fk = %s)
        #     and a.delete_ind = 0
        #     and b.delete_ind = 0
        #     and c.delete_ind = 0
        #     and d.delete_ind = 0""", data=(userid, userid))
        # results = datacursor.fetchone()
        # db.close()
        # discom = results.get('discom')

        current_app.logger.info('post_trade_data* %s', discom)
        datacursor = db.query_dictcursor("""SELECT
            coalesce(max(Revision) + 1, 0) as new_revision
            from trade_staging
            where date = STR_TO_DATE(%s,'%%d-%%m-%%Y')
            and discom = %s
            and demand_model_name = %s
            and gen_model_name = %s""", data=(date, discom,
                                              demandmodel, genmodel))
        rev = datacursor.fetchall()
        newrev = rev[0].get('new_revision')
        current_app.logger.info("***** new revision %s", newrev)
        # Add themissing columns data
        # keys = ('Date', 'Revision', 'Discom', 'Model_Name', 'Block_No',
        #         'Alpha', 'Ladder', 'Buy_Sell', 'Ladder_Volume',
        #         'Bid_Price', 'Bid_Volume')
        # data = [list(el.get(key) for key in keys)
        #         for el in post_trade_data]
        # current_app.logger.info("converted %s", data)
        # for el in data:
        #     el[1] = newrev
        #     el.append(current_user.id)
        data = []
        for el in post_trade_data:
            z = []
            z.append(el.get('Date'))
            z.append(newrev)
            z.append(el.get('Discom'))
            z.append(el.get('Demand_Model_Name'))
            z.append(el.get('Gen_Model_Name'))
            z.append(el.get('Block_No'))
            no_of_ladder = sorted(list(set(
                [int(ele[7:8]) for ele in el.get('columns') if 'Ladder' in ele]
            )))
            for i in no_of_ladder:
                a = copy.deepcopy(z)
                b = copy.deepcopy(z)
                if el.get('Ladder_' + str(i) + '_BUY_VOL') > 0 and \
                        el.get('Ladder_' + str(i) + '_BUY_PRICE') > 0:
                    a.append(el.get('Ladder_' + str(i) + '_Alpha'))
                    a.append(i)
                    a.append('BUY')
                    a.append(el.get('Ladder_' + str(i) + '_Vol'))
                    a.append(el.get('Ladder_' + str(i) + '_BUY_PRICE'))
                    a.append(el.get('Ladder_' + str(i) + '_BUY_VOL'))
                    a.append(current_user.id)
                    data.append(a)
                elif el.get('Ladder_' + str(i) + '_SELL_VOL') > 0 and \
                        el.get('Ladder_' + str(i) + '_SELL_PRICE') > 0:
                    b.append(el.get('Ladder_' + str(i) + '_Alpha'))
                    b.append(i)
                    b.append('SELL')
                    b.append(el.get('Ladder_' + str(i) + '_Vol'))
                    b.append(el.get('Ladder_' + str(i) + '_SELL_PRICE'))
                    b.append(el.get('Ladder_' + str(i) + '_SELL_VOL'))
                    b.append(current_user.id)
                    data.append(b)
        # Change the lists of lists to list of tuples using map
        data = map(tuple, data)
        current_app.logger.info("****data to upload***%s", data[0:3])
        sql = """INSERT INTO trade_staging
                (Date,
                Revision,
                Discom,
                Demand_Model_Name,
                Gen_Model_Name,
                Block_No,
                Alpha,
                Ladder,
                Buy_Sell,
                Ladder_Volume,
                Bid_Price,
                Bid_Volume,
                Added_By_FK)
                VALUES
                (STR_TO_DATE(%s,'%%d-%%m-%%Y'),
                 %s,
                 %s,
                 %s,
                 %s,
                 %s,
                 ROUND(%s,3),
                 %s,
                 %s,
                 ROUND(%s,3),
                 ROUND(%s,3),
                 ROUND(%s,3),
                 %s)"""
        try:
            current_app.logger.info(sql % data[0])
            db.query_dictcursor(sql, 'insert', data)
            db.query_commit()
            # db.cur.commit()
            db.close()
        except Exception as error:
            # if db.cur.open:
            # db.cur.rollback()
            db.query_rollback()
            db.close()
            current_app.logger.error("Error during Trade update %s", error)
            raise
        return json.dumps({'success': True}), 200,\
            {'ContentType': 'application/json'}


@ems.route("/get_power_xchng_data", methods=['POST'])
@login_required
def get_power_xchng_data():
    # date = request.form['date']
    # discom = request.form['discom']
    date = request.json['date']
    discom = request.json['discom']
    current_app.logger.info("get_power_xchng_data %s %s %s",
                            date, '***', discom)
    db = DB()
    # datacursor = con.cursor(cursors.DictCursor)
    datacursor = db.query_dictcursor("""SELECT DATE_FORMAT(a.date, '%%d-%%m-%%Y') as Date,
        a.block_no as Block_No, a.drawl_type as Drawl_Type,
        a.station_name as Station_Name,b.id as ID,
        a.schedule as Schedule, a.revision as Revision,
        b.fixed_cost as Fixed_Cost,
        b.variable_cost as Variable_Cost,
        b.pool_cost as Pool_Cost,
        b.generator_fuel as Generator_Fuel,
        case when schedule >= 200 and schedule < 300 then 50
             when schedule >= 300 and schedule < 400 then 100
             when schedule >= 400 and schedule < 450 then 200
             when schedule >= 450 then 250
             else 0 end as Surrender_Possibility,
        case when block_no <= 3 or block_no between 20 and 27
                or block_no between 44 and 51
                or block_no between 68 and 75
                or block_no between 92 and 96
             then 1.5 - 0.1
             when block_no between 4 and 9
                 or block_no between 42 and 43
                 or block_no between 52 and 58
             then 2.0 - 0.1
             when block_no between 10 and 11
                or block_no between 59 and 75
             then 2.5 - 0.1
             when block_no between 12 and 15
                or block_no between 80 and 91
             then 2.76 - 0.1
             when block_no between 16 and 19
                or block_no between 28 and 31
             then 3.14 - 0.1
             else 99999 end as Price_Threshold
        FROM power.bseb_generator_cost_dtls b,
         power.erldc_state_drawl_schedule_stg a
        where a.station_name = b.station_name
         and a.date = str_to_date('%s', '%%d-%%m-%%Y')
         and a.discom = '%s'
         and a.drawl_type = 'ISGS'
         and a.station_name <> 'NET|DRAWAL|SCHD.'
         and a.schedule > 0
         and a.revision = 0
         and b.valid_from_date <=
            str_to_date('%s', '%%d-%%m-%%Y')
         and b.valid_to_date >=
            str_to_date('%s', '%%d-%%m-%%Y')
        /*and b.generator_fuel = 'COAL'*/ /*Can be removed*/
        and  case when schedule >= 200 and schedule < 300
                then 50
             when schedule >= 300 and schedule < 400
                then 100
             when schedule >= 400 and schedule < 450
                then 200
             when schedule >= 450
                then 250
             else 0 end > 0
        order by a.date, a.block_no, b.variable_cost desc""" % (date, discom,
                                                                date, date))
    results = datacursor.fetchall()
    db.close()
    # datacursor.execute(""" as rows""")
    # rows = datacursor.fetchall()[0]['rows']
    # columns = ['Date', 'Block_No', 'Drawl_Type', 'Station_Name', 'ID',
    #             'Schedule', 'Revision', 'Fixed_Cost', 'Variable_Cost',
    # 'Pool_Cost', 'Generator_Fuel', 'Surrender_Possibility', 'Price_Threshold']
    # dt_output = rawsql_to_datatables(columns, results, rows, rows)
    # # print json.dumps(output)
    # return json.dumps(dt_output)
    return json.dumps(results, use_decimal=True)


def rawsql_to_datatables(columns, sqlresult, rectotal, recfilter):
    """
    raw sql to datatables format
    """
    output = {}
    aaData_rows = []
    for row in sqlresult:
        aaData_row = []
        for i in range(len(columns)):
            aaData_row.append(str(row[columns[i]]).replace('"', '\\"'))
        aaData_rows.append(aaData_row)

    output['draw'] = 1
    output['recordsTotal'] = rectotal
    output['recordsFiltered'] = recfilter
    output['data'] = aaData_rows
    return output


@ems.route("/possuply")
@login_required
def possuply():
    """
    When you request the root path, you'll get the index.html template.

    """
    db = DB()
    # datacursor = con.cursor(cursors.DictCursor)
    db.query_dictcursor("""CREATE temporary table if not exists
        power.raj_position_gap
        as
        select adddate(curdate(), 1) as Date, a.Block_no,
        ISGS, b.Bilateral, wind_gen_forecast as Wind,
        solar_gen_forecast as Solar, Internal_Generation,
        demand_forecast as Forecast
        , iex_rate
        ,@position_gap :=  (ISGS + b.Bilateral +
                            wind_gen_forecast + solar_gen_forecast +
                            internal_generation) - demand_forecast as Position_Gap
        ,@backdown := round(floor(
            IF(GREATEST(-350, -1 * @position_gap ,IF(@position_gap > 0,
                -1 * floor(GREATEST(350,0.50 * @position_gap)/100)*100,0))*.80<0,
                GREATEST(-350,-1 * @position_gap,IF(@position_gap >0 ,
                -1 * floor(GREATEST(350,.50*@position_gap)/100)*100,0))*0.8,0)/50)
                 * 50,0) as Backdown
        ,@isgs_surr := round(floor(IF(GREATEST(-400,-1 * (@position_gap + @backdown)
            ,IF(@position_gap > 0,-1 * floor(
                GREATEST(400,0.50 * @position_gap)/100)*100,0))*0.8<0,
                GREATEST(-400,-1 * (@position_gap + @backdown),IF(@position_gap > 0,
                -1 * floor(GREATEST(400,0.5 * @position_gap)/100)*100,0))*0.8,0)/50)
                * 50,0) as ISGS_Surrender
        ,@post_surrender_gap := round(@position_gap +
            @backdown + @isgs_surr,0) as post_surrender_gap
        ,@proposed_trade := 100*round(IF(@post_surrender_gap>0,
            @post_surrender_gap*1.1,@post_surrender_gap)/100,0)*-1 as Proposed_Trade
        ,@lot_size := IF(@proposed_trade>0,200,200) as lot_size
        ,@peak_factor := case when a.block_no >= 33 and a.block_no <= 48 then -0.005
                              when a.block_no >=  53 and a.block_no <= 60 then -0.005
                              else 0 end as peak_factor
        ,@beta := IF(@proposed_trade>0,.0125 + @peak_factor,.0125 - @peak_factor) as beta
        ,@proposed_price := IF(ABS(@proposed_trade)> 0,
            ROUND(iex_rate*(1+SIGN(@proposed_trade)*( IF(ABS(@proposed_trade)> 0,
            LOG(ABS(@proposed_trade)), NULL))*@peak_factor),2), NULL) as Proposed_Price
        from power.forecast_stg a,
        power.nrldc_state_drawl_summary_demoz b,
        power.exchange_price_summary c
        where a.date = b.date
        and a.block_no = b.block_no
        and a.date = c.delivery_date
        and a.block_no = c.block
        and a.date >= adddate(curdate(), -100)
        order by a.date, a.block_no
        limit 96""")
    db.close()
    columns = ['Date', 'Block No', 'ISGS', 'Bilateral', 'Wind',
               'Solar', 'Internal Generation', 'Forecast',
               'Position Gap', 'Backdown', 'ISGS Surrender',
               'Proposed Trade', 'Proposed Price'
               ]
    return flask.render_template("possuply.html", columns=columns)


@ems.route("/get_possuply_data")
@login_required
def get_possuply_data():
    """
    Get data from db
    """
    columns = ['Date', 'Block_No', 'ISGS', 'Bilateral', 'Wind',
               'Solar', 'Internal_Generation', 'Forecast',
               'Position_Gap', 'Backdown', 'ISGS_Surrender',
               'Proposed_Trade', 'Proposed_Price'
               ]
    index_column = "Date"
    table = "power.raj_position_gap"
    where = ""
    order = "order by date desc, block_no "
    db = DB()
    cursor = db.cur  # include a reference to your app mysqldb instance
    current_app.logger.info('Finished collecting Data'
                            'from get_possuply_data fn')
    results = DataTablesServer(request, columns, index_column,
                               table, cursor, where, order).output_result()
    db.close()
    # print "Here2",results
    # return the results as json # import json
    # results = {"aaData": [["1", "2", "3", "4"], ["5", "5", "5", "5"]]}
    return json.dumps(results)


@ems.route("/bihar_pos_map")
@login_required
def bihar_pos_map():
    """
    When you request the root path, you'll get the index.html template.

    """
    db = DB()
    # datacursor = con.cursor(cursors.DictCursor)
    db.query_dictcursor("""CREATE temporary table if not exists
        power.bihar_position_gap
        as
        select adddate(curdate(), 1) as Date, a.Block_no,
        round(ISGS,0) as ISGS, round(Bilateral) as Bilateral, NET_SCH,
        @int_gen := 100 as Internal_Generation,
        @demand_forecast := round(BPDCL,0) as Forecast
        , iex_rate
        ,@position_gap :=  round((ISGS + b.Bilateral +
                            @int_gen) - @demand_forecast, 0) as Position_Gap
        ,@backdown := round(floor(
            IF(GREATEST(-350, -1 * @position_gap ,IF(@position_gap > 0,
                -1 * floor(GREATEST(350,0.50 * @position_gap)/100)*100,0))*.80<0,
                GREATEST(-350,-1 * @position_gap,IF(@position_gap >0 ,
                -1 * floor(GREATEST(350,.50*@position_gap)/100)*100,0))*0.8,0)/50)
                 * 50,0) as Backdown
        ,@isgs_surr := round(floor(IF(GREATEST(-400,-1 * (@position_gap + @backdown)
            ,IF(@position_gap > 0,-1 * floor(
                GREATEST(400,0.50 * @position_gap)/100)*100,0))*0.8<0,
                GREATEST(-400,-1 * (@position_gap + @backdown),IF(@position_gap > 0,
                -1 * floor(GREATEST(400,0.5 * @position_gap)/100)*100,0))*0.8,0)/50)
                * 50,0) as ISGS_Surrender
        ,@post_surrender_gap := round(@position_gap +
            @backdown + @isgs_surr,0) as post_surrender_gap
        ,@proposed_trade := 100*round(IF(@post_surrender_gap>0,
            @post_surrender_gap*1.1,@post_surrender_gap)/100,0)*-1 as Proposed_Trade
        ,@lot_size := IF(@proposed_trade>0,200,200) as lot_size
        ,@peak_factor := case when a.block_no >= 33 and a.block_no <= 48 then -0.005
                              when a.block_no >=  53 and a.block_no <= 60 then -0.005
                              else 0 end as peak_factor
        ,@beta := IF(@proposed_trade>0,.0125 + @peak_factor,.0125 - @peak_factor) as beta
        ,@proposed_price := IF(ABS(@proposed_trade)> 0,
            ROUND(iex_rate*(1+SIGN(@proposed_trade)*( IF(ABS(@proposed_trade)> 0,
            LOG(ABS(@proposed_trade)), NULL))*@peak_factor),2), NULL) as Proposed_Price
        from power.bseb_forecast_demo a,
        power.vw_erldc_discom_drawl_summary_demo b,
        power.e1exchange_price_summary c,
        (select max(date) as max_date from power.bseb_forecast_demo) d
        where a.date = d.max_date
        and b.date = adddate(d.max_date,-1)
        and a.block_no = b.block_no
        and c.delivery_date = adddate(d.max_date,-1)
        and a.block_no = c.block
        and b.revision = 0
        order by a.date desc, a.block_no""")
    # columns = [ 'column_1', 'column_2', 'column_3', 'column_4']
    db.close()
    columns = ['Date', 'Block No', 'ISGS', 'Bilateral',
               'Internal Generation', 'Forecast',
               'Position Gap', 'Backdown', 'ISGS Surrender',
               'Proposed Trade', 'Proposed Price'
               ]
    return flask.render_template("bihar_pos_map.html", columns=columns)


@ems.context_processor
def positionmapcols():
    """
    When you request the root path, you'll get the index.html template.

    """
    # columns = ['Date', 'Block No', 'ISGS', 'Bilateral', 'Shared',
    #            'Open Access', 'Internal Gen', 'Forecast',
    #            'Position Gap', 'Backdown', 'ISGS Surrender',
    #            'Trade', 'Price']
    db = DB()
    datacursor = db.query_dictcursor("""SELECT
        ldc_name, ldc_org_name
        from power.org_isgs_map a,
        power.organisation_master b
        where a.organisation_master_fk = b.organisation_master_pk
        and (b.organisation_master_pk = %s
        or b.organisation_parent_fk = %s)
        and a.delete_ind = 0
        and b.delete_ind = 0""", data=(
        current_user.organisation_master_fk,
        current_user.organisation_master_fk))
    org = datacursor.fetchall()
    db.close()
    try:
        ldc = org[0].get('ldc_name')
    except Exception:
        ldc = None
    if ldc == 'NRLDC':
        columns = ['Date', 'Block No', 'ISGS', 'LTOA', 'MTOA',
                   'Shared', 'Bilateral', 'Open Access',
                   'Internal Generation', 'Surrender', 'Availibility',
                   'Forecast', 'Position Gap', 'Commited Trade']
    elif ldc == 'WRLDC':
        columns = ['Date', 'Block No', 'ISGS', 'LTOA', 'MTOA',
                   'STOA', 'URS', 'RRAS', 'Open Access',
                   'Internal Generation', 'Surrender', 'Availibility',
                   'Forecast', 'Position Gap', 'Commited Trade']
    else:
        columns = []
    return dict(positionmap_columns=columns)


@ems.context_processor
def powerreplacecols():
    """
    When you request the root path, you'll get the index.html template.

    """
    columns = ['Date', 'Block_No', 'Drawl_Type', 'Station_Name', 'ID',
               'Schedule', 'Revision', 'Fixed_Cost', 'Variable_Cost',
               'Pool_Cost', 'Generator_Fuel', 'Surrender_Possibility',
               'Price_Threshold']
    return dict(powerreplace_columns=columns)
# @ems.route("/get_biharposmap_data")
# def get_biharposmap_data():
#     """
#     Get data from db
#     """
#     columns = ['Date', 'Block_No', 'ISGS', 'Bilateral',
#                'Internal_Generation', 'Forecast',
#                'Position_Gap', 'Backdown', 'ISGS_Surrender',
#                'Proposed_Trade', 'Proposed_Price'
#                ]
#     index_column = "Date"
#     table = "power.bihar_position_gap"
#     where = ""
#     order = "order by date, block_no "
#     cursor = con # include a reference to your app mysqldb instance
#     print "Here"
#     #collection = [dict(zip(columns, [1,2,3,4,5,6,7])), dict(zip(columns, [5,5,5,5,5,5,5]))]
#     results = DataTablesServer(request, columns, index_column,
#                                table, cursor, where, order).output_result()
#     #print "Here2",results
#     # return the results as json # import json
#     #results = {"aaData": [["1", "2", "3", "4"], ["5", "5", "5", "5"]]}
#     return json.dumps(results)


@ems.route("/get_biharposmap_data", methods=['POST'])
@login_required
def get_biharposmap_data():
    """
    Get data from db
    """
    date = request.form['date']
    model = request.form['model']
    current_app.logger.info('get_biharposmap_data %s %s', date, model)
    db = DB()
    # datacursor = con.cursor(cursors.DictCursor)
    # datacursor.execute("""drop temporary table if exists power.bihar_position_gap""")
    # datacursor.execute("""create temporary table if not exists
    #                       power.bihar_position_gap
    #                       as
    #                         select a.date as Date, a.Block_no,
    #                         round(ISGS,0) as ISGS, round(Bilateral) as Bilateral, NET_SCH,
    #                         @int_gen := 100 as Internal_Generation,
    #                         @demand_forecast := round(BPDCL,0) as Forecast
    #                         , iex_rate
    #                         ,@position_gap :=  round((ISGS + b.Bilateral +
    #                                             @int_gen) - @demand_forecast, 0) as Position_Gap
    #                         ,@backdown := round(floor(
    #                             IF(GREATEST(-350, -1 * @position_gap ,IF(@position_gap > 0,
    #                                 -1 * floor(GREATEST(350,0.50 * @position_gap)/100)*100,0))*.80<0,
    #                                 GREATEST(-350,-1 * @position_gap,IF(@position_gap >0 ,
    #                                 -1 * floor(GREATEST(350,.50*@position_gap)/100)*100,0))*0.8,0)/50)
    #                                  * 50,0) as Backdown
    #                         ,@isgs_surr := round(floor(IF(GREATEST(-400,-1 * (@position_gap + @backdown)
    #                             ,IF(@position_gap > 0,-1 * floor(
    #                                 GREATEST(400,0.50 * @position_gap)/100)*100,0))*0.8<0,
    #                                 GREATEST(-400,-1 * (@position_gap + @backdown),IF(@position_gap > 0,
    #                                 -1 * floor(GREATEST(400,0.5 * @position_gap)/100)*100,0))*0.8,0)/50)
    #                                 * 50,0) as ISGS_Surrender
    #                         ,@post_surrender_gap := round(@position_gap +
    #                             @backdown + @isgs_surr,0) as post_surrender_gap
    #                         ,@proposed_trade := 100*round(IF(@post_surrender_gap>0,
    #                             @post_surrender_gap*1.1,@post_surrender_gap)/100,0)*-1 as Proposed_Trade
    #                         ,@lot_size := IF(@proposed_trade>0,200,200) as lot_size
    #                         ,@peak_factor := case when a.block_no >= 33 and a.block_no <= 48 then -0.005
    #                                               when a.block_no >=  53 and a.block_no <= 60 then -0.005
    #                                               else 0 end as peak_factor
    #                         ,@beta := IF(@proposed_trade>0,.0125 + @peak_factor,.0125 - @peak_factor) as beta
    #                         ,@proposed_price := IF(ABS(@proposed_trade)> 0,
    #                             ROUND(iex_rate*(1+SIGN(@proposed_trade)*( IF(ABS(@proposed_trade)> 0,
    #                             LOG(ABS(@proposed_trade)), NULL))*@peak_factor),2), NULL) as Proposed_Price
    #                         from power.bseb_forecast_demo a,
    #                         power.vw_erldc_discom_drawl_summary_demo b,
    #                         power.e1exchange_price_summary c
    #                         where a.date = STR_TO_DATE('%s', '%%d-%%m-%%Y')
    #                         and b.date = adddate(STR_TO_DATE('%s', '%%d-%%m-%%Y'),-1)
    #                         and a.block_no = b.block_no
    #                         and c.delivery_date = adddate(STR_TO_DATE('%s', '%%d-%%m-%%Y'),-1)
    #                         and a.block_no = c.block
    #                         and b.revision = 0
    #                         and a.model = '%s'
    #                         order by a.date desc, a.block_no
    #                         """ % (date, date, date, model)
    #                    )

    # columns = ['Date', 'Block_No', 'ISGS', 'Bilateral',
    #            'Internal_Generation', 'Forecast',
    #            'Position_Gap', 'Backdown', 'ISGS_Surrender',
    #            'Proposed_Trade', 'Proposed_Price'
    #            ]
    # index_column = "Date"
    # table = "power.bihar_position_gap"
    # where = ""
    # order = "order by date, block_no "
    # cursor = con # include a reference to your app mysqldb instance
    # print "Here"
    # #collection = [dict(zip(columns, [1,2,3,4,5,6,7])), dict(zip(columns, [5,5,5,5,5,5,5]))]
    # results = DataTablesServer(request, columns, index_column,
    #                            table, cursor, where, order).output_result()
    # #print "Here2",results
    # # return the results as json # import json
    # #results = {"aaData": [["1", "2", "3", "4"], ["5", "5", "5", "5"]]}
    # return json.dumps(results)
    datacursor = db.query_dictcursor("""SELECT Date, Block_No, ISGS, Bilateral,
        Internal_Generation, Forecast,
        Position_Gap, Backdown, ISGS_Surrender,
        Proposed_Trade, Proposed_Price from  (
        select DATE_FORMAT(a.date, '%%d-%%m-%%Y') as Date, a.Block_No,
        ISGS, Bilateral,
        @int_gen := 100 as Internal_Generation,
        @demand_forecast := round(BPDCL,0) as Forecast
        , iex_rate
        ,@position_gap :=  round((ISGS + b.Bilateral +
                            @int_gen) - @demand_forecast,0) as Position_Gap
        ,@backdown := round(floor(
            IF(GREATEST(-350, -1 * @position_gap ,IF(@position_gap > 0,
                -1 * floor(GREATEST(350,0.50 * @position_gap)/100)*100,0))*.80<0,
                GREATEST(-350,-1 * @position_gap,IF(@position_gap >0 ,
                -1 * floor(GREATEST(350,.50*@position_gap)/100)*100,0))*0.8,0)/50)
                 * 50,0) as Backdown
        ,@isgs_surr := round(floor(IF(GREATEST(-400,-1 * (@position_gap + @backdown)
            ,IF(@position_gap > 0,-1 * floor(
                GREATEST(400,0.50 * @position_gap)/100)*100,0))*0.8<0,
                GREATEST(-400,-1 * (@position_gap + @backdown),IF(@position_gap > 0,
                -1 * floor(GREATEST(400,0.5 * @position_gap)/100)*100,0))*0.8,0)/50)
                * 50,0) as ISGS_Surrender
        ,@post_surrender_gap := round(@position_gap +
            @backdown + @isgs_surr,0) as post_surrender_gap
        ,@proposed_trade := 100*round(IF(@post_surrender_gap>0,
            @post_surrender_gap*1.1,@post_surrender_gap)/100,0)*-1 as Proposed_Trade
        ,@lot_size := IF(@proposed_trade>0,200,200) as lot_size
        ,@peak_factor := case when a.block_no >= 33 and a.block_no <= 48 then -0.005
                              when a.block_no >=  53 and a.block_no <= 60 then -0.005
                              else 0 end as peak_factor
        ,@beta := IF(@proposed_trade>0,.0125 + @peak_factor,.0125 - @peak_factor) as beta
        ,@proposed_price := IF(ABS(@proposed_trade)> 0,
            ROUND(iex_rate*(1+SIGN(@proposed_trade)*( IF(ABS(@proposed_trade)> 0,
            LOG(ABS(@proposed_trade)), NULL))*@peak_factor),2), NULL) as Proposed_Price
        from (select * from power.bseb_forecast_demo where model = '%s' and date = STR_TO_DATE('%s', '%%d-%%m-%%Y')) a,
        (select Date AS date,
                Block_No AS block_no,
                Revision AS revision,
                Discom AS discom,
                SUM((CASE
                    WHEN
                        ((Drawl_Type = 'ISGS')
                            AND (Station_Name <> 'NET|DRAWAL|SCHD.'))
                    THEN
                        Schedule
                END)) AS ISGS,
                SUM((CASE
                    WHEN
                        ((Drawl_Type = 'BILATERAL')
                            AND (Station_Name = 'BILAT|TOTAL'))
                    THEN
                        Schedule
                END)) AS BILATERAL,
                SUM((CASE
                    WHEN (Drawl_Type = 'LTOA_MTOA') THEN Schedule
                END)) AS LTOA_MTOA,
                SUM((CASE
                    WHEN
                        ((Drawl_Type = 'IEX')
                            AND (Station_Name = 'IEX|TOT.'))
                    THEN
                        Schedule
                END)) AS IEX,
                SUM((CASE
                    WHEN
                        ((Drawl_Type = 'PXIL')
                            AND (Station_Name = 'PXI|TOT.'))
                    THEN
                        Schedule
                END)) AS PXIL,
                SUM((CASE
                    WHEN
                        ((Drawl_Type = 'ISGS')
                            AND (Station_Name = 'NET|DRAWAL|SCHD.'))
                    THEN
                        Schedule
                END)) AS NET_SCH,
                SUM((CASE
                    WHEN (Drawl_Type = 'REGULATION') THEN Schedule
                END)) AS REGULATION
            from power.erldc_state_drawl_schedule_stg
            where date = adddate(STR_TO_DATE('%s', '%%d-%%m-%%Y'),-1)
            and discom = 'BSEB'
            and revision = (select min(revision) from
                            power.erldc_state_drawl_schedule_stg
                            where date =
                             adddate(STR_TO_DATE('%s', '%%d-%%m-%%Y'),-1)
                            and discom = 'BSEB')
            group by Date , Discom , Revision , Block_No, Load_Date
            having load_date = max(load_date)) b,
        (select * from power.e1exchange_price_summary
        where delivery_date = adddate(STR_TO_DATE('%s', '%%d-%%m-%%Y'),-1)) c
        where  a.block_no = b.block_no
        and a.block_no = c.block
        order by a.date desc, a.block_no
        ) posmap""" % (model, date, date, date, date))
    results = datacursor.fetchall()
    current_app.logger.debug(json.dumps(results))
    datacursor = db.query_dictcursor("""SELECT FOUND_ROWS() _rows""")
    rows = datacursor.fetchall()[0]['_rows']
    db.close()
    columns = ['Date', 'Block_No', 'ISGS', 'Bilateral',
               'Internal_Generation', 'Forecast',
               'Position_Gap', 'Backdown', 'ISGS_Surrender',
               'Proposed_Trade', 'Proposed_Price'
               ]
    dt_output = rawsql_to_datatables(columns, results, rows, rows)
    # print json.dumps(dt_output)
    return json.dumps(dt_output)


@ems.route("/possuplychart")
@login_required
def possuplychart():
    """
    When you request the root path, you'll get the index.html template.

    """
    return flask.render_template("possuplychart.html")


@ems.route("/get_possuply2_data")
@login_required
def get_possuply2_data():
    """
    When you request the root path, you'll get the index.html template.

    """
    db = DB()
    # datacursor = con.cursor(cursors.DictCursor)
    datacursor = db.query_dictcursor("""SELECT
        DATE_FORMAT(adddate(curdate(), 1), '%d-%m-%Y') as Date, a.Block_no,
        ISGS, b.Bilateral, wind_gen_forecast as Wind,
        solar_gen_forecast as Solar, Internal_Generation,
        demand_forecast as Forecast
        , iex_rate
        ,@position_gap :=  (ISGS + b.Bilateral +
                            wind_gen_forecast + solar_gen_forecast +
                            internal_generation) - demand_forecast
                            as Position_Gap
        ,@backdown := round(floor(
            IF(GREATEST(-350, -1 * @position_gap ,IF(@position_gap > 0,
                -1 * floor(GREATEST(350,0.50 * @position_gap)/100)*100,0))
                *.80<0,
                GREATEST(-350,-1 * @position_gap,IF(@position_gap >0 ,
                -1 * floor(GREATEST(350,.50*@position_gap)/100)*100,0))
                 *0.8,0)/50)
                 * 50,0) as Backdown
        ,@isgs_surr := round(floor(IF(GREATEST(-400,
        -1 * (@position_gap + @backdown)
            ,IF(@position_gap > 0,-1 * floor(
                GREATEST(400,0.50 * @position_gap)/100)*100,0))*0.8<0,
                GREATEST(-400,-1 * (@position_gap + @backdown),
                IF(@position_gap > 0,
                -1 * floor(GREATEST(400,0.5 * @position_gap)/100)*100,0))
                *0.8,0)/50) * 50,0) as ISGS_Surrender
        ,@post_surrender_gap := round(@position_gap +
            @backdown + @isgs_surr,0) as post_surrender_gap
        ,@proposed_trade := 100*round(IF(@post_surrender_gap>0,
            @post_surrender_gap*1.1,@post_surrender_gap)/100,0)*-1
            as Proposed_Trade
        ,@lot_size := IF(@proposed_trade>0,200,200) as lot_size
        ,@peak_factor := case when a.block_no >= 33 and a.block_no <= 48
                              then -0.005
                              when a.block_no >=  53 and a.block_no <= 60
                              then -0.005
                              else 0 end as peak_factor
        ,@beta := IF(@proposed_trade>0,
                     .0125 + @peak_factor,.0125 - @peak_factor) as beta
        ,@proposed_price := IF(ABS(@proposed_trade)> 0,
            ROUND(iex_rate*(1+SIGN(@proposed_trade)*( IF(ABS(@proposed_trade)> 0,
            LOG(ABS(@proposed_trade)), NULL))*@peak_factor),2), NULL)
            as Proposed_Price
        from power.forecast_stg a,
        power.nrldc_state_drawl_summary_demoz b,
        power.exchange_price_summary c
        where a.date = b.date
        and a.block_no = b.block_no
        and a.date = c.delivery_date
        and a.block_no = c.block
        and a.date >= adddate(curdate(), -100)
        order by a.date, a.block_no
        limit 96""")
    results = datacursor.fetchall()
    db.close()
    # results = [row["Id"] for row in datacursor.fetchall()]
    # results = ['AD_06-03-2013','AD_07-03-2013','AD_08-03-2013']
    # print "QSEarch", results
    # return flask.render_template("test.html" , data=json.dumps(results))
    return json.dumps(results, use_decimal=True)


@ems.route("/bihar_posmap_chart")
@login_required
def bihar_posmap_chart():
    """
    When you request the root path, you'll get the index.html template.

    """
    return flask.render_template("bihar_posmap_chart.html")


# @ems.route("/get_biharposmap2_data")
# def get_biharposmap2_data():
@ems.route("/get_biharposmap2_data/<date>/<model>")
@login_required
def get_biharposmap2_data(date, model):
    """
    When you request the root path, you'll get the index.html template.

    """
    current_app.logger.info("%s %s", date, model)
    db = DB()
    # datacursor = con.cursor(cursors.DictCursor)
    datacursor = db.query_dictcursor("""SELECT
        DATE_FORMAT(a.date, '%%d-%%m-%%Y') as Date, a.Block_no,
        ISGS, Bilateral, NET_SCH,
        @int_gen := 100 as Internal_Generation,
        @demand_forecast := BPDCL as Forecast
        , iex_rate
        ,@position_gap :=  (ISGS + b.Bilateral +
                            @int_gen) - @demand_forecast as Position_Gap
        ,@backdown := round(floor(
            IF(GREATEST(-350, -1 * @position_gap ,IF(@position_gap > 0,
                -1 * floor(GREATEST(350,0.50 * @position_gap)/100)*100,0))*.80<0,
                GREATEST(-350,-1 * @position_gap,IF(@position_gap >0 ,
                -1 * floor(GREATEST(350,.50*@position_gap)/100)*100,0))*0.8,0)/50)
                 * 50,0) as Backdown
        ,@isgs_surr := round(floor(IF(GREATEST(-400,-1 * (@position_gap + @backdown)
            ,IF(@position_gap > 0,-1 * floor(
                GREATEST(400,0.50 * @position_gap)/100)*100,0))*0.8<0,
                GREATEST(-400,-1 * (@position_gap + @backdown),IF(@position_gap > 0,
                -1 * floor(GREATEST(400,0.5 * @position_gap)/100)*100,0))*0.8,0)/50)
                * 50,0) as ISGS_Surrender
        ,@post_surrender_gap := round(@position_gap +
            @backdown + @isgs_surr,0) as post_surrender_gap
        ,@proposed_trade := 100*round(IF(@post_surrender_gap>0,
            @post_surrender_gap*1.1,@post_surrender_gap)/100,0)*-1 as Proposed_Trade
        ,@lot_size := IF(@proposed_trade>0,200,200) as lot_size
        ,@peak_factor := case when a.block_no >= 33 and a.block_no <= 48 then -0.005
                              when a.block_no >=  53 and a.block_no <= 60 then -0.005
                              else 0 end as peak_factor
        ,@beta := IF(@proposed_trade>0,.0125 + @peak_factor,.0125 - @peak_factor) as beta
        ,@proposed_price := IF(ABS(@proposed_trade)> 0,
            ROUND(iex_rate*(1+SIGN(@proposed_trade)*( IF(ABS(@proposed_trade)> 0,
            LOG(ABS(@proposed_trade)), NULL))*@peak_factor),2), NULL) as Proposed_Price
        from (select * from power.bseb_forecast_demo where model = '%s' and date = STR_TO_DATE('%s', '%%d-%%m-%%Y')) a,
        (select Date AS date,
                Block_No AS block_no,
                Revision AS revision,
                Discom AS discom,
                SUM((CASE
                    WHEN
                        ((Drawl_Type = 'ISGS')
                            AND (Station_Name <> 'NET|DRAWAL|SCHD.'))
                    THEN
                        Schedule
                END)) AS ISGS,
                SUM((CASE
                    WHEN
                        ((Drawl_Type = 'BILATERAL')
                            AND (Station_Name = 'BILAT|TOTAL'))
                    THEN
                        Schedule
                END)) AS BILATERAL,
                SUM((CASE
                    WHEN (Drawl_Type = 'LTOA_MTOA') THEN Schedule
                END)) AS LTOA_MTOA,
                SUM((CASE
                    WHEN
                        ((Drawl_Type = 'IEX')
                            AND (Station_Name = 'IEX|TOT.'))
                    THEN
                        Schedule
                END)) AS IEX,
                SUM((CASE
                    WHEN
                        ((Drawl_Type = 'PXIL')
                            AND (Station_Name = 'PXI|TOT.'))
                    THEN
                        Schedule
                END)) AS PXIL,
                SUM((CASE
                    WHEN
                        ((Drawl_Type = 'ISGS')
                            AND (Station_Name = 'NET|DRAWAL|SCHD.'))
                    THEN
                        Schedule
                END)) AS NET_SCH,
                SUM((CASE
                    WHEN (Drawl_Type = 'REGULATION') THEN Schedule
                END)) AS REGULATION
            from power.erldc_state_drawl_schedule_stg
            where Discom = 'BSEB'
            and date = adddate(STR_TO_DATE('%s', '%%d-%%m-%%Y'),-1)
            and revision = 0 and discom = 'BSEB'
            group by Date , Discom , Revision , Block_No, Load_Date
            having load_date = max(load_date)) b,
        (select *
        from power.e1exchange_price_summary
        where delivery_date = adddate(STR_TO_DATE('%s', '%%d-%%m-%%Y'),-1)) c
        where  a.block_no = b.block_no
        and a.block_no = c.block
        order by a.date desc, a.block_no""" % (model, date, date, date))
    current_app.logger.info(datetime.now()
                            .strftime("%Y-%m-%d %H:%M:%S"))
    results = datacursor.fetchall()
    current_app.logger.info(datetime.now()
                            .strftime("%Y-%m-%d %H:%M:%S"))
    db.close()
    # results = [row["Id"] for row in datacursor.fetchall()]
    # results = ['AD_06-03-2013','AD_07-03-2013','AD_08-03-2013']
    # print "QSEarch", results
    # return flask.render_template("test.html" , data=json.dumps(results))
    return json.dumps(results, use_decimal=True)


# @ems.route("/get_positionmap_data/<date>/<model>")
# @login_required
# def get_positionmap_data(date, model, dest=None):
#     """
#     When you request the root path, you'll get the index.html template.
#     """
#     current_app.logger.info("get_positionmap_data %s %s", date, model)
#     db = DB()
#     userid = current_user.organisation_master_fk
#     datacursor = db.query_dictcursor("""SELECT
#         ldc_name, ldc_org_name, d.zone_code,
#         b.organisation_code discom
#         from power.org_isgs_map a,
#              power.organisation_master b,
#              power.state_master c,
#              power.zone_master d
#         where a.organisation_master_fk = b.organisation_master_pk
#         and c.state_master_pk = b.state_master_fk
#         and c.exchange_zone_master_fk = d.zone_master_pk
#         and (b.organisation_master_pk = %s
#             or b.organisation_parent_fk = %s)
#         and a.delete_ind = 0
#         and b.delete_ind = 0
#         and c.delete_ind = 0
#         and d.delete_ind = 0""", data=(userid, userid))
#     results = datacursor.fetchone()
#     # ldc_name, ldc_org_name, zone_code, discom
#     current_app.logger.debug("get_positionmap_data results %s", results)
#     zone = {'zone': results.get('zone_code')}
#     exch_query = """SELECT
#             Delivery_Date
#                 as date,
#             Block as Block_No,
#             SUM((CASE
#                 WHEN
#                     (Exchange_Name = 'IEX')
#                 THEN
#                     ROUND(({zone}_Price / 1000), 2)
#             END)) AS IEX_rate,
#             SUM((CASE
#                 WHEN
#                     (Exchange_Name = 'PXIL')
#                 THEN
#                     ROUND(({zone}_Price / 1000), 2)
#             END)) AS PXIL_rate
#         FROM power.exchange_areaprice_stg
#         WHERE delivery_date = adddate(STR_TO_DATE(%s, "%%d-%%m-%%Y"), -1)
#         GROUP BY Delivery_Date , Block""" \
#         .format(**zone)
#     current_app.logger.debug("get_positionmap_data exch_query %s", exch_query)
#     erldc_query = """SELECT Date AS date,
#             Block_No AS block_no,
#             Revision AS revision,
#             Discom AS discom,
#             SUM((CASE
#                 WHEN
#                     ((Drawl_Type = 'ISGS')
#                         AND (Station_Name <> 'NET|DRAWAL|SCHD.'))
#                 THEN
#                     Schedule
#             END)) AS ISGS,
#             SUM((CASE
#                 WHEN
#                     ((Drawl_Type = 'BILATERAL')
#                         AND (Station_Name = 'BILAT|TOTAL'))
#                 THEN
#                     Schedule
#             END)) AS BILATERAL,
#             SUM((CASE
#                 WHEN (Drawl_Type = 'LTOA_MTOA') THEN Schedule
#             END)) AS LTOA_MTOA,
#             SUM((CASE
#                 WHEN
#                     ((Drawl_Type = 'IEX')
#                         AND (Station_Name = 'IEX|TOT.'))
#                 THEN
#                     Schedule
#             END)) AS IEX,
#             SUM((CASE
#                 WHEN
#                     ((Drawl_Type = 'PXIL')
#                         AND (Station_Name = 'PXI|TOT.'))
#                 THEN
#                     Schedule
#             END)) AS PXIL,
#             SUM((CASE
#                 WHEN
#                     ((Drawl_Type = 'ISGS')
#                         AND (Station_Name = 'NET|DRAWAL|SCHD.'))
#                 THEN
#                     Schedule
#             END)) AS NET_SCH,
#             SUM((CASE
#                 WHEN (Drawl_Type = 'REGULATION') THEN Schedule
#             END)) AS REGULATION
#         from power.erldc_state_drawl_schedule_stg
#         where date = adddate(STR_TO_DATE(%s, '%%d-%%m-%%Y'),-1)
#         and revision = 0 and discom = %s
#         group by Date , Discom , Revision , Block_No, Load_Date
#         having load_date = max(load_date)"""
#     nrldc_query = """SELECT
#             date,
#             a.Revision AS Revision,
#             a.State AS Discom,
#             a.Block_No AS Block_No,
#             @isgs := SUM(CASE WHEN a.Drawl_Type = 'ISGS'
#                 THEN a.Schedule END) AS ISGS,
#             @ltoa := SUM(CASE WHEN a.Drawl_Type = 'LTA'
#                 THEN a.Schedule END) AS LTOA,
#             @mtoa := SUM(CASE WHEN a.Drawl_Type = 'MTOA'
#                 THEN a.Schedule END) AS MTOA,
#             @shared := SUM(CASE WHEN a.Drawl_Type = 'Shared'
#                 THEN a.Schedule END) AS Shared,
#             @biltarel := SUM(CASE WHEN a.Drawl_Type = 'Bilateral'
#                 THEN a.Schedule END) AS Bilateral,
#             @iex := SUM(CASE WHEN a.Drawl_Type = 'IEX_PXIL'
#                 AND a.Head1 in ("(IEX Drawal)", "(IEX Injection)")
#                 THEN a.Schedule END) AS IEX,
#             @pxil := SUM(CASE WHEN a.Drawl_Type = 'IEX_PXIL'
#                 AND a.Head1 in ("(PXIL Drawal)", "(PXIL Injection)")
#                 THEN a.Schedule END) AS PXIL,
#             ifnull(@isgs, 0) + ifnull(@ltoa, 0) + ifnull(@mtoa, 0) +
#                 ifnull(@shared, 0) + ifnull(@bilateral, 0) +
#                 ifnull(@iex, 0) + ifnull(@pxil, 0) AS NET_SCH
#         from power.panel_nrldc_state_drawl_schedule a,
#              (select date as mindate, state as minstate,
#              min(Revision) as minrev
#              from
#              power.panel_nrldc_state_drawl_schedule
#              where date = adddate(str_to_date(%s, '%%d-%%m-%%Y'),-1)
#              and state = %s
#              and revision = 0
#              group by date, state) b
#         where a.date = b.mindate
#         and a.revision = b.minrev
#         and a.state = b.minstate
#         group by Date, Revision, State, Block_No"""
#     forecast_query = """SELECT
#             a.date, a.Discom
#             , a.Model_Name, a.Block_No
#             , a.Demand_Forecast + coalesce(a.Demand_Bias,0) biased_forecast
#         from power.forecast_stg a,
#              (select date, discom, model_name,
#               max(revision) max_revision
#               from power.forecast_stg
#               where date = str_to_date(%s, '%%d-%%m-%%Y')
#               and discom = %s
#               and model_name = %s
#               group by date, discom, model_name) b
#         where a.date = b.date
#         and a.discom = b.discom
#         and a.model_name = b.model_name
#         and a.revision = b.max_revision"""
#     int_tentative_str = """SELECT date,
#             declared_date, block_no,
#             sum(tentative_generation) internal_generation
#         from power.tentative_schedule_staging
#         where date = str_to_date(%s, '%%d-%%m-%%Y')
#         and generation_entity_name
#             not in ('ISGS', 'BILATERAL', 'MTOA',
#                     'SHARED', 'BANKING', 'STOA', 'LTOA')
#         and discom = %s
#         group by date, declared_date, block_no
#         having declared_date = max(declared_date)"""
#     sel_dict = {'NRLDC': [('coalesce(Shared,0) Shared, coalesce(LTOA,0) + '
#                            'coalesce(MTOA,0) Open_Access'),
#                           ('@position_gap := round((ISGS + '
#                            'coalesce(Bilateral,0) + '
#                            'coalesce(Shared,0) + coalesce(LTOA,0) + '
#                            'coalesce(MTOA,0) + internal_generation) - '
#                            'biased_forecast,2) as Position_Gap '),
#                           nrldc_query],
#                 'ERLDC': [('0 Shared, LTOA_MTOA as Open_Access'),
#                           ('@position_gap :=  round((ISGS + '
#                            'coalesce(Bilateral,0) + '
#                            'coalesce(LTOA_MTOA,0) +'
#                            'internal_generation) - biased_forecast,2) '
#                            'as Position_Gap '),
#                           erldc_query]}
#     select_str = """SELECT
#         DATE_FORMAT(f.date, '%%d-%%m-%%Y') as Date, f.Block_no,
#         ISGS, Bilateral, {d[0]},
#         Internal_Generation,
#         biased_forecast Forecast,
#         iex_rate
#         ,{d[1]}
#         ,@backdown := round(floor(
#             IF(GREATEST(-350, -1 * @position_gap ,IF(@position_gap > 0,
#                 -1 * floor(GREATEST(350,0.50 * @position_gap)/100)*100,0))
#                 *.80<0,
#                 GREATEST(-350,-1 * @position_gap,IF(@position_gap >0 ,
#                 -1 * floor(GREATEST(350,0.50*@position_gap)/100)*100,0))
#                  *0.8,0)/50)
#                  * 50,0) as Backdown
#         ,@isgs_surr := round(floor(IF(GREATEST(-400,
#         -1 * (@position_gap + @backdown)
#             ,IF(@position_gap > 0,-1 * floor(
#                 GREATEST(400,0.50 * @position_gap)/100)*100,0))*0.8<0,
#                 GREATEST(-400,-1 * (@position_gap + @backdown),
#                 IF(@position_gap > 0,
#                 -1 * floor(GREATEST(400,0.50 * @position_gap)/100)*100,0))
#                 *0.8,0)/50) * 50,0) as ISGS_Surrender
#         ,@post_surrender_gap := round(@position_gap +
#             @backdown + @isgs_surr,0) as post_surrender_gap
#         ,@proposed_trade := 100*round(IF(@post_surrender_gap>0,
#             @post_surrender_gap*1.1,@post_surrender_gap)/100,0)*-1
#             as Proposed_Trade
#         ,@lot_size := IF(@proposed_trade>0,200,200) as lot_size
#         ,@peak_factor := case when f.block_no >= 21 and f.block_no <= 31
#                               then -0.005
#                               when f.block_no >=  73 and f.block_no <= 92
#                               then -0.005
#                               else 0 end as peak_factor
#         ,@beta := IF(@proposed_trade>0,
#                      .0125 + @peak_factor,.0125 - @peak_factor) as beta
#         ,@proposed_price := IF(ABS(@proposed_trade)> 0,
#             ROUND(iex_rate*(1+SIGN(@proposed_trade)*
#             (IF(ABS(@proposed_trade)> 0,
#             LOG(ABS(@proposed_trade)), NULL))*@peak_factor),2), NULL)
#             as Proposed_Price"""\
#             .format(d=sel_dict.get(results.get('ldc_name')))

#     sql_str = ('{} from ({}) f left join ({}) ldc '
#                'on (f.block_no = ldc.block_no) '
#                'left join ({}) int_gen '
#                'on (f.block_no = int_gen.block_no and f.date = int_gen.date) '
#                'left join ({}) exc '
#                'on (f.block_no = exc.block_no) '
#                'order by f.date, f.block_no'
#                .format(select_str, forecast_query,
#                        sel_dict.get(results.get('ldc_name'))[2],
#                        int_tentative_str, exch_query))

#     if dest == 'datatables':
#         sql_str = ('SELECT Date, Block_No, ISGS, Bilateral, Shared, '
#                    'Open_Access, '
#                    'Internal_Generation, Forecast, Position_Gap, '
#                    'Backdown, ISGS_Surrender, Proposed_Trade, '
#                    'Proposed_Price from ({}) posmap'.format(sql_str))

#     current_app.logger.debug("sqlstr get_positionmap_data %s", sql_str)
#     datacursor = db.query_dictcursor(sql_str, data=(date,
#                                      results.get('discom'),
#                                      model, date,
#                                      results.get('ldc_org_name'),
#                                      date,
#                                      results.get('discom'),
#                                      date))
#     results = datacursor.fetchall()
#     if dest == 'datatables':
#         datacursor = db.query_dictcursor("""SELECT FOUND_ROWS() as rows""")
#         rows = datacursor.fetchone().get('rows')
#         columns = ['Date', 'Block_No', 'ISGS', 'Bilateral', 'Shared',
#                    'Open_Access', 'Internal_Generation', 'Forecast',
#                    'Position_Gap', 'Backdown', 'ISGS_Surrender',
#                    'Proposed_Trade', 'Proposed_Price']
#         results = rawsql_to_datatables(columns, results, rows, rows)

#     db.close()
#     return json.dumps(results, use_decimal=True)

@ems.route('/post_positionmap_data', methods=['POST'])
@roles_required(['admin', 'jobrunner', 'analytics'])
def post_positionmap_data():
    current_app.logger.info('***** started post_positionmap_data')
    # current_app.logger.debug("post forecast %s", request.json)
    if request.json:
        data = json.loads(json.dumps(request.json))
        posmap_data = data.get('data')
        current_app.logger.info('post_positionmap_data  %s',
                                posmap_data)

        keys = {'ISGS': 'ISGS', 'LTOA': 'LTOA', 'MTOA': 'MTOA',
                'Shared': 'Shared', 'Bilateral': 'Bilateral',
                'STOA': 'STOA', 'URS': 'URS', 'RRAS': 'RRAS',
                'WIND': 'WIND', 'SOLAR': 'SOLAR',
                'Open_Access': 'OPENACCESS',
                'Internal_Generation': 'INT_GENERATION_ACT',
                'Surrender': 'SURRENDER',
                'Availibility': 'AVAILIBILITY',
                'Demand_Forecast': 'DEMAND_FOR',
                'Position_Gap': 'POSITION_GAP',
                'Commited_Trade': 'COMMITED_TRADE'}
        current_app.logger.info('Passed Keys')
        posmap_data_list = []

        db = DB()
        datacursor = db.query_dictcursor("""SELECT
            b.state_name, c.organisation_code
            from power.state_master b,
                 power.organisation_master c
            where c.state_master_fk = b.state_master_pk
            and (c.organisation_master_pk = %s
            or c.organisation_parent_fk = %s)""", data=(
            current_user.organisation_master_fk,
            current_user.organisation_master_fk))
        org = datacursor.fetchall()
        state = org[0].get('state_name')
        discom = org[0].get('organisation_code')
        current_app.logger.debug('post_positionmap_data* %s %s', state, discom)
        datacursor = db.query_dictcursor("""SELECT
            coalesce(max(Revision) + 1, 0) as new_revision
            from position_map_staging
            where date = STR_TO_DATE(%s,'%%d-%%m-%%Y')
            and state = %s
            and discom = %s""", data=(posmap_data[0].get('Date'),
                                      state, discom))
        rev = datacursor.fetchall()
        newrev = rev[0].get('new_revision')
        current_app.logger.info("***** new revision %s", newrev)
        # Add themissing columns data
        for el in posmap_data:
            temp = [el.get('Date'), state, newrev, discom, el.get('Block_No')]
            for k, v in el.items():
                if keys.get(k):
                    tmp = []
                    tmp.extend(temp)
                    if keys.get(k) not in ('WIND', 'SOLAR'):
                        tmp.append(keys.get(k))
                        tmp.append('UNKNOWN')
                    else:
                        tmp.append('INT_GENERATION_FOR')
                        tmp.append(keys.get(k))
                    tmp.append(v)
                    tmp.append(current_user.id)
                    posmap_data_list.append(tmp)
        # for el in data:
        #     el.insert(1, state)
        #     el.insert(2, newrev)
        #     el.insert(3, discom)
        #     el.append(current_user.id)
        # Change the lists of lists to list of tuples using map
        data = map(tuple, posmap_data_list)
        current_app.logger.info("****data to upload***%s", data[0:3])
        sql = """INSERT INTO position_map_staging
                (Date,
                State,
                Revision,
                Discom,
                Block_No,
                Pool_Name,
                Pool_Type,
                Schedule,
                Added_By_FK)
                VALUES
                (STR_TO_DATE(%s,'%%d-%%m-%%Y'),
                 %s,
                 %s,
                 %s,
                 %s,
                 %s,
                 %s,
                 ROUND(%s,3),
                 %s)"""
        try:
            current_app.logger.debug(sql % data[0])
            db.query_dictcursor(sql, 'insert', data)
            db.query_commit()
            # db.cur.commit()
            db.close()
            current_app.logger.info("****position_map_staging upload finished")
        except Exception as error:
            # if db.cur.open:
            # db.cur.rollback()
            db.query_rollback()
            db.close()
            current_app.logger.error("Error during Position Map update %s",
                                     error)
            raise
        return json.dumps({'success': True}), 200,\
            {'ContentType': 'application/json'}


# @ems.route("/get_positionmap_data/<date>/<model>")
# @login_required
# def get_positionmap_data(date, model, dest=None):
#     """
#     When you request the root path, you'll get the index.html template.
#     """
#     current_app.logger.info("get_positionmap_data %s %s", date, model)
#     db = DB()
#     userid = current_user.organisation_master_fk
#     datacursor = db.query_dictcursor("""SELECT
#         ldc_name, ldc_org_name, d.zone_code,
#         b.organisation_code discom
#         from power.org_isgs_map a,
#              power.organisation_master b,
#              power.state_master c,
#              power.zone_master d
#         where a.organisation_master_fk = b.organisation_master_pk
#         and c.state_master_pk = b.state_master_fk
#         and c.exchange_zone_master_fk = d.zone_master_pk
#         and (b.organisation_master_pk = %s
#             or b.organisation_parent_fk = %s)
#         and a.delete_ind = 0
#         and b.delete_ind = 0
#         and c.delete_ind = 0
#         and d.delete_ind = 0""", data=(userid, userid))
#     results = datacursor.fetchone()
#     # ldc_name, ldc_org_name, zone_code, discom
#     current_app.logger.debug("get_positionmap_data results %s", results)
#     zone = {'zone': results.get('zone_code')}
#     if results.get('ldc_name') == 'ERLDC':
#         exch_query = """SELECT
#                 Delivery_Date
#                     as date,
#                 Block as Block_No,
#                 SUM((CASE
#                     WHEN
#                         (Exchange_Name = 'IEX')
#                     THEN
#                         ROUND(({zone}_Price / 1000), 2)
#                 END)) AS IEX_rate,
#                 SUM((CASE
#                     WHEN
#                         (Exchange_Name = 'PXIL')
#                     THEN
#                         ROUND(({zone}_Price / 1000), 2)
#                 END)) AS PXIL_rate
#             FROM power.exchange_areaprice_stg
#             WHERE delivery_date = adddate(STR_TO_DATE(%s, "%%d-%%m-%%Y"), -1)
#             GROUP BY Delivery_Date , Block""" \
#             .format(**zone)
#         current_app.logger.debug("get_positionmap_data exch_query %s",
#                                  exch_query)
#         erldc_query = """SELECT Date AS date,
#                 Block_No AS block_no,
#                 Revision AS revision,
#                 Discom AS discom,
#                 SUM((CASE
#                     WHEN
#                         ((Drawl_Type = 'ISGS')
#                             AND (Station_Name <> 'NET|DRAWAL|SCHD.'))
#                     THEN
#                         Schedule
#                 END)) AS ISGS,
#                 SUM((CASE
#                     WHEN
#                         ((Drawl_Type = 'BILATERAL')
#                             AND (Station_Name = 'BILAT|TOTAL'))
#                     THEN
#                         Schedule
#                 END)) AS BILATERAL,
#                 SUM((CASE
#                     WHEN (Drawl_Type = 'LTOA_MTOA') THEN Schedule
#                 END)) AS LTOA_MTOA,
#                 SUM((CASE
#                     WHEN
#                         ((Drawl_Type = 'IEX')
#                             AND (Station_Name = 'IEX|TOT.'))
#                     THEN
#                         Schedule
#                 END)) AS IEX,
#                 SUM((CASE
#                     WHEN
#                         ((Drawl_Type = 'PXIL')
#                             AND (Station_Name = 'PXI|TOT.'))
#                     THEN
#                         Schedule
#                 END)) AS PXIL,
#                 SUM((CASE
#                     WHEN
#                         ((Drawl_Type = 'ISGS')
#                             AND (Station_Name = 'NET|DRAWAL|SCHD.'))
#                     THEN
#                         Schedule
#                 END)) AS NET_SCH,
#                 SUM((CASE
#                     WHEN (Drawl_Type = 'REGULATION') THEN Schedule
#                 END)) AS REGULATION
#             from power.erldc_state_drawl_schedule_stg
#             where date = adddate(STR_TO_DATE(%s, '%%d-%%m-%%Y'),-1)
#             and revision = 0 and discom = %s
#             group by Date , Discom , Revision , Block_No, Load_Date
#             having load_date = max(load_date)"""
#         forecast_query = """SELECT
#                 a.date, a.Discom
#                 , a.Model_Name, a.Block_No
#                 , a.Demand_Forecast + coalesce(a.Demand_Bias,0) biased_forecast
#             from power.forecast_stg a,
#                  (select date, discom, model_name,
#                   max(revision) max_revision
#                   from power.forecast_stg
#                   where date = str_to_date(%s, '%%d-%%m-%%Y')
#                   and discom = %s
#                   and model_name = %s
#                   group by date, discom, model_name) b
#             where a.date = b.date
#             and a.discom = b.discom
#             and a.model_name = b.model_name
#             and a.revision = b.max_revision"""
#         int_tentative_str = """SELECT date,
#             declared_date, block_no,
#             round(sum(case when
#                         upper(generation_entity_name) = 'OPENACCESS'
#                       then tentative_generation else 0 end),2) OPENACCESS,
#             round(sum(case when
#                         upper(generation_entity_name) <> 'OPENACCESS'
#                       then tentative_generation else 0 end),2) INTERNAL
#             from power.tentative_schedule_staging
#             where date = str_to_date(%s, '%%d-%%m-%%Y')
#             and generation_entity_name
#                 not in ('ISGS', 'BILATERAL', 'MTOA',
#                         'SHARED', 'BANKING', 'STOA', 'LTOA')
#             and discom = %s
#             group by date, declared_date, block_no
#             having declared_date = max(declared_date)"""
#         sel_dict = {'ERLDC': [('0 Shared, LTOA_MTOA as Open_Access'),
#                               ('@position_gap :=  round((ISGS + '
#                                'coalesce(Bilateral,0) + '
#                                'coalesce(LTOA_MTOA,0) +'
#                                'internal_generation) - biased_forecast,2) '
#                                'as Position_Gap '),
#                               erldc_query]}
#         select_str = """SELECT
#             DATE_FORMAT(f.date, '%%d-%%m-%%Y') as Date, f.Block_no,
#             ISGS, Bilateral, {d[0]},
#             Internal_Generation,
#             biased_forecast Forecast,
#             iex_rate
#             ,{d[1]}
#             ,@backdown := round(floor(
#                 IF(GREATEST(-350, -1 * @position_gap ,IF(@position_gap > 0,
#                     -1 * floor(GREATEST(350,0.50 * @position_gap)/100)*100,0))
#                     *.80<0,
#                     GREATEST(-350,-1 * @position_gap,IF(@position_gap >0 ,
#                     -1 * floor(GREATEST(350,0.50*@position_gap)/100)*100,0))
#                      *0.8,0)/50)
#                      * 50,0) as Backdown
#             ,@isgs_surr := round(floor(IF(GREATEST(-400,
#             -1 * (@position_gap + @backdown)
#                 ,IF(@position_gap > 0,-1 * floor(
#                     GREATEST(400,0.50 * @position_gap)/100)*100,0))*0.8<0,
#                     GREATEST(-400,-1 * (@position_gap + @backdown),
#                     IF(@position_gap > 0,
#                     -1 * floor(GREATEST(400,0.50 * @position_gap)/100)*100,0))
#                     *0.8,0)/50) * 50,0) as ISGS_Surrender
#             ,@post_surrender_gap := round(@position_gap +
#                 @backdown + @isgs_surr,0) as post_surrender_gap
#             ,@proposed_trade := 100*round(IF(@post_surrender_gap>0,
#                 @post_surrender_gap*1.1,@post_surrender_gap)/100,0)*-1
#                 as Proposed_Trade
#             ,@lot_size := IF(@proposed_trade>0,200,200) as lot_size
#             ,@peak_factor := case when f.block_no >= 21 and f.block_no <= 31
#                                   then -0.005
#                                   when f.block_no >=  73 and f.block_no <= 92
#                                   then -0.005
#                                   else 0 end as peak_factor
#             ,@beta := IF(@proposed_trade>0,
#                          .0125 + @peak_factor,.0125 - @peak_factor) as beta
#             ,@proposed_price := IF(ABS(@proposed_trade)> 0,
#                 ROUND(iex_rate*(1+SIGN(@proposed_trade)*
#                 (IF(ABS(@proposed_trade)> 0,
#                 LOG(ABS(@proposed_trade)), NULL))*@peak_factor),2), NULL)
#                 as Proposed_Price"""\
#                 .format(d=sel_dict.get(results.get('ldc_name')))

#         sql_str = ('{} from ({}) f left join ({}) ldc '
#                    'on (f.block_no = ldc.block_no) '
#                    'left join ({}) int_gen '
#                    'on (f.block_no = int_gen.block_no '
#                    'and f.date = int_gen.date) '
#                    'left join ({}) exc '
#                    'on (f.block_no = exc.block_no) '
#                    'order by f.date, f.block_no'
#                    .format(select_str, forecast_query,
#                            sel_dict.get(results.get('ldc_name'))[2],
#                            int_tentative_str, exch_query))

#         current_app.logger.debug("sqlstr get_positionmap_data %s", sql_str)
#         datacursor = db.query_dictcursor(sql_str, data=(date,
#                                          results.get('discom'),
#                                          model, date,
#                                          results.get('ldc_org_name'),
#                                          date,
#                                          results.get('discom'),
#                                          date))
#         results = datacursor.fetchall()
#         if dest == 'datatables':
#             datacursor = db.query_dictcursor("""SELECT FOUND_ROWS() as rows""")
#             rows = datacursor.fetchone().get('rows')
#             columns = ['Date', 'Block_No', 'ISGS', 'Bilateral', 'Shared',
#                        'Open_Access', 'Internal_Generation', 'Forecast',
#                        'Position_Gap', 'Backdown', 'ISGS_Surrender',
#                        'Proposed_Trade', 'Proposed_Price']
#             results = rawsql_to_datatables(columns, results, rows, rows)
#     elif results.get('ldc_name') == 'NRLDC':
#         sql_str = """SELECT
#         DATE_FORMAT(isgs_internal.Date, '%%d-%%m-%%Y') Date ,
#         isgs_internal.Block_No,
#         ISGS, LTOA, MTOA, Shared,
#         Bilateral, round(OPENACCESS, 2) Open_Access,
#         round(INTERNAL, 2) Internal_Generation,
#         round(0 - (NET_SCH - OPT_SCH), 2) Surrender, OPT_SCH Availibility,
#         round(Biased_Forecast, 2) Forecast,
#         round(OPT_SCH - Biased_Forecast, 2) Position_Gap,
#         Trade Commited_Trade
#         from
#         (SELECT  coalesce(z.Date, y.Date) Date,
#         z.Revision, coalesce(z.Discom, y.Discom) Discom,
#         coalesce(z.Block_No, y.Block_No) Block_No, z.ISGS,
#         z.LTOA, z.MTOA, z.Shared, z.Bilateral, z.OPT_SCHEDULE,
#          y.OPENACCESS, y.INTERNAL,
#         z.OPT_SCHEDULE + z.LTOA + z.MTOA + z.Shared + z.Bilateral +
#         y.OPENACCESS + y.INTERNAL OPT_SCH,
#         z.ISGS + z.LTOA + z.MTOA + z.Shared + z.Bilateral +
#         y.OPENACCESS + y.INTERNAL NET_SCH
#         from
#         (select a.Date, a.Block_No, a.Revision, a.Discom,
#         round(sum(case when a.pool_name = 'ISGS'
#                   then a.schedule * ((100 - x.loss_perc)/100)
#                   else 0 end),2) ISGS,
#         round(sum(case when a.pool_name = 'LTOA'
#                   then a.schedule else 0 end),2) LTOA,
#         round(sum(case when a.pool_name = 'MTOA'
#                   then a.schedule else 0 end),2) MTOA,
#         round(sum(case when a.pool_name = 'Bilateral'
#                   then a.schedule else 0 end),2) Bilateral,
#         round(sum(case when a.pool_name = 'Shared'
#                   then a.schedule else 0 end),2) Shared,
#         round(sum(case when a.pool_name = 'OPT_SCHEDULE'
#                   then a.schedule * ((100 - x.loss_perc)/100)
#                   else 0 end),2) OPT_SCHEDULE
#         from power.isgstentative_schedule_staging a,
#              (select date, discom, max(revision) max_revision
#               from power.isgstentative_schedule_staging
#               where date = str_to_date(%s, '%%d-%%m-%%Y')
#               and discom = %s
#               group by date, discom) b left join
#              (select case when count(loss_perc)=0 then 0
#                           else loss_perc end  loss_perc
#               from power.nrldc_est_trans_loss_stg
#               where start_date <= str_to_date(%s, '%%d-%%m-%%Y')
#               and end_date >= str_to_date(%s, '%%d-%%m-%%Y')) x
#             on (1 = 1)
#         where a.date = b.date
#         and a.discom = b.discom
#         and a.revision = b.max_revision
#         group by a.date, a.block_no, a.revision, a.discom) z left join
#         (select date, block_no, discom,
#         round(sum(case when upper(generation_entity_name) = 'OPENACCESS'
#                   then tentative_generation else 0 end),2) OPENACCESS,
#         round(sum(case when upper(generation_entity_name) <> 'OPENACCESS'
#                   then tentative_generation else 0 end),2) INTERNAL
#         from power.tentative_schedule_staging
#               where date = str_to_date(%s, '%%d-%%m-%%Y')
#               and discom = %s
#         and upper(generation_entity_name)
#         not in ('ISGS', 'BILATERAL', 'MTOA',
#                 'SHARED', 'BANKING', 'STOA', 'LTOA')
#         group by date, block_no, discom) y
#         on (z.date = y.date
#         and z.block_no = y.block_no
#         and z.discom = y.discom)
#         union
#         SELECT coalesce(z.Date, y.Date) Date,
#         z.Revision, coalesce(z.Discom, y.Discom) Discom,
#         coalesce(z.Block_No, y.Block_No) Block_No, z.ISGS,
#         z.LTOA, z.MTOA, z.Shared, z.Bilateral, z.OPT_SCHEDULE,
#         y.OPENACCESS, y.INTERNAL,
#         z.OPT_SCHEDULE + z.LTOA + z.MTOA + z.Shared + z.Bilateral +
#         y.OPENACCESS + y.INTERNAL OPT_SCH,
#         z.ISGS + z.LTOA + z.MTOA + z.Shared + z.Bilateral +
#         y.OPENACCESS + y.INTERNAL NET_SCH
#         from
#         (select a.Date, a.Block_No, a.Revision, a.Discom,
#         round(sum(case when a.pool_name = 'ISGS'
#                   then a.schedule * ((100 - x.loss_perc)/100)
#                   else 0 end),2) ISGS,
#         round(sum(case when a.pool_name = 'LTOA'
#                   then a.schedule else 0 end),2) LTOA,
#         round(sum(case when a.pool_name = 'MTOA'
#                   then a.schedule else 0 end),2) MTOA,
#         round(sum(case when a.pool_name = 'Bilateral'
#                   then a.schedule else 0 end),2) Bilateral,
#         round(sum(case when a.pool_name = 'Shared'
#                   then a.schedule else 0 end),2) Shared,
#         round(sum(case when a.pool_name = 'OPT_SCHEDULE'
#                   then a.schedule * ((100 - x.loss_perc)/100)
#                   else 0 end),2) OPT_SCHEDULE
#         from power.isgstentative_schedule_staging a,
#              (select date, discom, max(revision) max_revision
#               from power.isgstentative_schedule_staging
#               where date = str_to_date(%s, '%%d-%%m-%%Y')
#               and discom = %s
#               group by date, discom) b left join
#              (select case when count(loss_perc)=0 then 0
#                           else loss_perc end  loss_perc
#               from power.nrldc_est_trans_loss_stg
#               where start_date <= str_to_date(%s, '%%d-%%m-%%Y')
#               and end_date >= str_to_date(%s, '%%d-%%m-%%Y')) x
#             on (1 = 1)
#         where a.date = b.date
#         and a.discom = b.discom
#         and a.revision = b.max_revision
#         group by a.date, a.block_no, a.revision, a.discom) z right join
#         (select date, block_no, discom,
#         round(sum(case when upper(generation_entity_name) = 'OPENACCESS'
#                   then tentative_generation else 0 end),2) OPENACCESS,
#         round(sum(case when upper(generation_entity_name) <> 'OPENACCESS'
#                   then tentative_generation else 0 end),2) INTERNAL
#         from power.tentative_schedule_staging
#               where date = str_to_date(%s, '%%d-%%m-%%Y')
#               and discom = %s
#         and upper(generation_entity_name)
#         not in ('ISGS', 'BILATERAL', 'MTOA',
#                 'SHARED', 'BANKING', 'STOA', 'LTOA')
#         group by date, block_no, discom) y
#         on (z.date = y.date
#         and z.block_no = y.block_no
#         and z.discom = y.discom)) isgs_internal
#         left join
#         (select a.Date, a.Block_No, a.Discom, a.Model_Name,
#         round(case when a.buy_sell = 'SELL'
#              then 0 - a.bid_volume
#              when a.buy_sell = 'BUY'
#              then a.bid_volume else NULL end) Trade
#         from trade_staging a,
#              (select date, discom, model_name,
#               max(revision) max_revision
#               from power.trade_staging
#               where date = str_to_date(%s, '%%d-%%m-%%Y')
#               and discom = %s
#               and model_name = %s
#               group by date, discom, model_name) b
#         where a.date =b.date
#         and a.discom = b.discom
#         and a.model_name = b.model_name
#         and a.revision = b.max_revision
#         and ladder = 1) trade
#         on (isgs_internal.date = trade.date
#            and isgs_internal.block_no = trade.block_no)
#         left join
#         (select a.Date, a.Discom , a.Model_Name, a.Block_No,
#         a.Demand_Forecast + coalesce(a.Demand_Bias,0) Biased_Forecast
#         from power.forecast_stg a,
#              (select date, discom, model_name,
#               max(revision) max_revision
#               from power.forecast_stg
#               where date = str_to_date(%s, '%%d-%%m-%%Y')
#               and discom = %s
#               and model_name = %s
#               group by date, discom, model_name) b
#         where a.date = b.date
#         and a.discom = b.discom
#         and a.model_name = b.model_name
#         and a.revision = b.max_revision) forecast
#         on (isgs_internal.date = forecast.date
#            and isgs_internal.block_no = forecast.block_no)"""

#         current_app.logger.info("sqlstr get_positionmap_data %s",
#                                 sql_str % (date,
#                                            results.get('discom'),
#                                            date, date,
#                                            date,
#                                            results.get('discom'),
#                                            date,
#                                            results.get('discom'),
#                                            date, date,
#                                            date,
#                                            results.get('discom'),
#                                            date,
#                                            results.get('discom'),
#                                            model,
#                                            date,
#                                            results.get('discom'),
#                                            model))
#         datacursor = db.query_dictcursor(sql_str, data=(date,
#                                          results.get('discom'),
#                                          date, date,
#                                          date,
#                                          results.get('discom'),
#                                          date,
#                                          results.get('discom'),
#                                          date, date,
#                                          date,
#                                          results.get('discom'),
#                                          date,
#                                          results.get('discom'),
#                                          model,
#                                          date,
#                                          results.get('discom'),
#                                          model))
#         results = datacursor.fetchall()
#         if dest == 'datatables':
#             datacursor = db.query_dictcursor("""SELECT FOUND_ROWS() as rows""")
#             rows = datacursor.fetchone().get('rows')
#             columns = ['Date', 'Block_No', 'ISGS', 'LTOA', 'MTOA',
#                        'Shared', 'Bilateral', 'Open_Access',
#                        'Internal_Generation', 'Surrender', 'Availibility',
#                        'Forecast', 'Position_Gap', 'Commited_Trade']
#             results = rawsql_to_datatables(columns, results, rows, rows)
#             current_app.logger.info("get_positionmap_data %s", results)
#     elif results.get('ldc_name') == 'WRLDC':
#         sql_str = """SELECT
#         DATE_FORMAT(isgs_internal.Date, '%%d-%%m-%%Y') Date ,
#         isgs_internal.Block_No,
#         ISGS, LTOA, MTOA, STOA, URS, RRAS,
#         round(OPENACCESS, 2) Open_Access,
#         round(INTERNAL, 2) Internal_Generation,
#         round(0 - (NET_SCH - OPT_SCH), 2) Surrender, OPT_SCH Availibility,
#         round(Biased_Forecast, 2) Forecast,
#         round(OPT_SCH - Biased_Forecast, 2) Position_Gap,
#         Trade Commited_Trade
#         from
#         (SELECT  coalesce(z.Date, y.Date) Date,
#         z.Revision, coalesce(z.Discom, y.Discom) Discom,
#         coalesce(z.Block_No, y.Block_No) Block_No, z.ISGS,
#         z.LTOA, z.MTOA, z.STOA, z.URS, z.RRAS, z.OPT_SCHEDULE,
#          y.OPENACCESS, y.INTERNAL,
#         z.OPT_SCHEDULE + z.LTOA + z.MTOA + z.STOA + z.URS + z.RRAS +
#         y.OPENACCESS + y.INTERNAL OPT_SCH,
#         z.ISGS + z.LTOA + z.MTOA + z.STOA + z.URS + z.RRAS +
#         y.OPENACCESS + y.INTERNAL NET_SCH
#         from
#         (select a.Date, a.Block_No, a.Revision, a.Discom,
#         round(sum(case when a.pool_name = 'ISGS'
#                   then a.schedule
#                   else 0 end),2) ISGS,
#         round(sum(case when a.pool_name = 'LTOA'
#                   then a.schedule else 0 end),2) LTOA,
#         round(sum(case when a.pool_name = 'MTOA'
#                   then a.schedule else 0 end),2) MTOA,
#         round(sum(case when a.pool_name = 'STOA'
#                   then a.schedule else 0 end),2) STOA,
#         round(sum(case when a.pool_name = 'URS'
#                   then a.schedule else 0 end),2) URS,
#         round(sum(case when a.pool_name = 'RRAS'
#                   then a.schedule else 0 end),2) RRAS,
#         round(sum(case when a.pool_name = 'OPT_SCHEDULE'
#                   then a.schedule
#                   else 0 end),2) OPT_SCHEDULE
#         from power.isgstentative_schedule_staging a,
#              (select date, discom, max(revision) max_revision
#               from power.isgstentative_schedule_staging
#               where date = str_to_date(%s, '%%d-%%m-%%Y')
#               and discom = %s
#               group by date, discom) b
#         where a.date = b.date
#         and a.discom = b.discom
#         and a.revision = b.max_revision
#         group by a.date, a.block_no, a.revision, a.discom) z left join
#         (select date, block_no, discom,
#         round(sum(case when upper(generation_entity_name) = 'OPENACCESS'
#                   then tentative_generation else 0 end),2) OPENACCESS,
#         round(sum(case when upper(generation_entity_name) <> 'OPENACCESS'
#                   then tentative_generation else 0 end),2) INTERNAL
#         from power.tentative_schedule_staging
#               where date = str_to_date(%s, '%%d-%%m-%%Y')
#               and discom = %s
#         and upper(generation_entity_name)
#         not in ('ISGS', 'BILATERAL', 'MTOA',
#                 'SHARED', 'BANKING', 'STOA', 'LTOA')
#         group by date, block_no, discom) y
#         on (z.date = y.date
#         and z.block_no = y.block_no
#         and z.discom = y.discom)) isgs_internal
#         left join
#         (select a.Date, a.Block_No, a.Discom, a.Model_Name,
#         round(case when a.buy_sell = 'SELL'
#              then 0 - a.bid_volume
#              when a.buy_sell = 'BUY'
#              then a.bid_volume else NULL end) Trade
#         from power.trade_staging a,
#              (select date, discom, model_name,
#               max(revision) max_revision
#               from power.trade_staging
#               where date = str_to_date(%s, '%%d-%%m-%%Y')
#               and discom = %s
#               and model_name = %s
#               group by date, discom, model_name) b
#         where a.date =b.date
#         and a.discom = b.discom
#         and a.model_name = b.model_name
#         and a.revision = b.max_revision
#         and ladder = 1) trade
#         on (isgs_internal.date = trade.date
#            and isgs_internal.block_no = trade.block_no)
#         left join
#         (select a.Date, a.Discom , a.Model_Name, a.Block_No,
#         a.Demand_Forecast + coalesce(a.Demand_Bias,0) Biased_Forecast
#         from power.forecast_stg a,
#              (select date, discom, model_name,
#               max(revision) max_revision
#               from power.forecast_stg
#               where date = str_to_date(%s, '%%d-%%m-%%Y')
#               and discom = %s
#               and model_name = %s
#               group by date, discom, model_name) b
#         where a.date = b.date
#         and a.discom = b.discom
#         and a.model_name = b.model_name
#         and a.revision = b.max_revision) forecast
#         on (isgs_internal.date = forecast.date
#            and isgs_internal.block_no = forecast.block_no)"""

#         current_app.logger.info("sqlstr get_positionmap_data %s",
#                                 sql_str % (date,
#                                            results.get('discom'),
#                                            date,
#                                            results.get('discom'),
#                                            date,
#                                            results.get('discom'),
#                                            model,
#                                            date,
#                                            results.get('discom'),
#                                            model))
#         datacursor = db.query_dictcursor(sql_str, data=(date,
#                                          results.get('discom'),
#                                          date,
#                                          results.get('discom'),
#                                          date,
#                                          results.get('discom'),
#                                          model,
#                                          date,
#                                          results.get('discom'),
#                                          model))
#         results = datacursor.fetchall()
#         if dest == 'datatables':
#             datacursor = db.query_dictcursor("""SELECT FOUND_ROWS() as rows""")
#             rows = datacursor.fetchone().get('rows')
#             columns = ['Date', 'Block_No', 'ISGS', 'LTOA', 'MTOA',
#                        'STOA', 'URS', 'RRAS', 'Open_Access',
#                        'Internal_Generation', 'Surrender', 'Availibility',
#                        'Forecast', 'Position_Gap', 'Commited_Trade']
#             results = rawsql_to_datatables(columns, results, rows, rows)
#             current_app.logger.info("get_positionmap_data %s", results)
#     db.close()
#     return json.dumps(results, use_decimal=True)


@ems.route("/get_positionmap_data", methods=['POST'])
@login_required
def get_positionmap_data():
    """Get DayAHead PositionMap Data."""
    from sqlalchemy import create_engine
    import pandas as pd
    import numpy as np
    from collections import OrderedDict
    """
    When you request the root path, you'll get the index.html template.
    """
    date = request.json['date']
    discom = request.json['discom']
    demmodel = request.json['demmodel']
    genmodel = request.json['genmodel']
    current_app.logger.info("get_positionmap_data %s %s %s %s",
                            date, discom, demmodel, genmodel)

    db_uri = current_app.config['SQLALCHEMY_DATABASE_URI']
    engine = create_engine(db_uri, echo=False)
    valid_date = pd.to_datetime(date, format='%d-%m-%Y')

    ten_gen = pd.read_sql_query("""SELECT a.Date, a.Block_No,
        a.pool_name, a.pool_type, a.generator_name, a.schedule, a.Discom
        from power.isgstentative_schedule_staging a,
             (select date, discom, pool_name, max(revision) max_revision
              from power.isgstentative_schedule_staging
              where date = '{}'
              and discom = '{}'
              group by date, discom, pool_name) b
        where a.date = b.date
        and a.discom = b.discom
        and a.revision = b.max_revision
        and a.pool_name = b.pool_name
        """.format(valid_date, discom),
        engine, index_col=None)
    ten_gen_pv = pd.pivot_table(ten_gen, index=["Date", 'Block_No', 'Discom'],
                                columns=['pool_name'],
                                values='schedule', aggfunc=np.sum)
    new_ten_gen = ten_gen_pv.reset_index()

    dem_forecast = pd.read_sql_query("""SELECT a.Date, a.Block_No,
        a.demand_forecast + coalesce(a.demand_bias,0) Demand_Forecast
        from power.forecast_stg a,
             (select date, discom, model_name,
              max(revision) max_revision
              from power.forecast_stg
              where date = '{}'
              and discom = '{}'
              and model_name = '{}'
              group by date, discom, model_name) b
        where a.date = b.date
        and a.discom = b.discom
        and a.model_name = b.model_name
        and a.revision = b.max_revision
        """.format(valid_date, discom, demmodel),
        engine, index_col=None)

    gen_forecast = pd.read_sql_query("""SELECT
        a.Date, a.Block_No, a.pool_name,
        a.pool_type, a.entity_name,
        a.gen_forecast
        from gen_forecast_stg a,
         (select date, org_name, model_name, pool_name, pool_type,
          max(revision) max_revision
          from power.gen_forecast_stg
          where date = '{}'
          and org_name = '{}'
          and model_name = '{}'
          group by date, org_name, model_name, pool_name, pool_type) b
        where a.date = b.date
        and a.org_name = b.org_name
        and a.model_name = b.model_name
        and a.revision = b.max_revision
        and a.pool_name = b.pool_name
        and a.pool_type = b.pool_type
        """.format(valid_date, discom, genmodel),
        engine, index_col=None)
    gen_forecast_pv = pd.pivot_table(gen_forecast,
                                     index=["Date", 'Block_No', 'pool_name'],
                                     columns=['pool_type'],
                                     values='gen_forecast', aggfunc=np.sum)
    new_gen_forecast = gen_forecast_pv.reset_index()

    trade = pd.read_sql_query("""SELECT a.Date, a.Block_No,
        round(sum(case when a.buy_sell = 'SELL'
             then 0 - a.bid_volume
             when a.buy_sell = 'BUY'
             then a.bid_volume else NULL end)) Trade
        from power.trade_staging a,
             (select date, discom, demand_model_name, gen_model_name,
              max(revision) max_revision
              from power.trade_staging
              where date = '{}'
              and discom = '{}'
              and demand_model_name = '{}'
              and gen_model_name = coalesce('{}', 'UNKNOWN')
              group by date, discom, demand_model_name, gen_model_name) b
        where a.date =b.date
        and a.discom = b.discom
        and a.demand_model_name = b.demand_model_name
        and a.gen_model_name = b.gen_model_name
        and a.revision = b.max_revision
        group by a.Date, a.Block_No
        """.format(valid_date, discom, demmodel, genmodel),
        engine, index_col=None)

    master_date = pd.read_sql_query("""select  a.Date,
        b.block_no Block_No
        from calendar_master a,
          block_master b
        where a.date = '{}'
        """.format(valid_date),
        engine, index_col=None)
    try:
        merge_flag = False
        if len(new_ten_gen):
            da_pos_map = master_date.merge(new_ten_gen,
                                           on=['Date', 'Block_No'],
                                           how='left')
            merge_flag = True

        if len(dem_forecast) and merge_flag:
            da_pos_map = da_pos_map.merge(dem_forecast,
                                          on=['Date', 'Block_No'],
                                          how='left')
        elif len(dem_forecast) and not merge_flag:
            da_pos_map = master_date.merge(dem_forecast,
                                           on=['Date', 'Block_No'],
                                           how='left')
            merge_flag = True

        if len(new_gen_forecast) and merge_flag:
            da_pos_map = da_pos_map.merge(new_gen_forecast,
                                          on=['Date', 'Block_No'],
                                          how='left')
            da_pos_map.drop(['pool_name', 'pool_type'], inplace=True,
                            axis=1, errors='ignore')
        elif len(new_gen_forecast) and not merge_flag:
            da_pos_map = master_date.merge(new_gen_forecast,
                                           on=['Date', 'Block_No'],
                                           how='left')
            da_pos_map.drop(['pool_name', 'pool_type'],
                            inplace=True, axis=1, errors='ignore')
            merge_flag = True

        if len(trade) and merge_flag:
            da_pos_map = da_pos_map.merge(trade, on=['Date', 'Block_No'],
                                          how='left')
        elif len(trade) and not merge_flag:
            da_pos_map = master_date.merge(trade, on=['Date', 'Block_No'],
                                           how='left')

        da_pos_map = da_pos_map.fillna(0)
        da_pos_map = da_pos_map.round()

        # Calc Net Availibility
        na_lst_ign = ['Date', 'Block_No', 'Discom', 'Trade',
                      'OPT_SCHEDULE', 'Demand_Forecast']
        na_cols_to_sum = \
            [cols for cols in da_pos_map.columns if cols not in na_lst_ign]
        da_pos_map_clone = da_pos_map.copy()
        da_pos_map_clone['NET_SCH'] = 0
        for cols in na_cols_to_sum:
            da_pos_map_clone['NET_SCH'] = \
                da_pos_map_clone['NET_SCH'] + da_pos_map_clone[cols]

        if 'OPT_SCHEDULE' in da_pos_map.columns:
            os_lst_ign = ['Date', 'Block_No', 'Discom', 'Trade', 'ISGS',
                          'INT_GENERATION_ACT', 'Demand_Forecast', 'NET_SCH']
            os_cols_to_sum = [cols for cols in da_pos_map.columns
                              if cols not in os_lst_ign]
            da_pos_map_clone['OPT_SCH'] = 0
            for cols in os_cols_to_sum:
                da_pos_map_clone['OPT_SCH'] = \
                    da_pos_map_clone['OPT_SCH'] + da_pos_map_clone[cols]
        if 'OPT_SCH' in da_pos_map_clone.columns and \
                'NET_SCH' in da_pos_map_clone.columns:
            da_pos_map_clone['SURRENDER'] = \
                0 - (da_pos_map_clone['NET_SCH'] - da_pos_map_clone['OPT_SCH'])
            da_pos_map_clone['POSITION_GAP'] = da_pos_map_clone['OPT_SCH'] - \
                da_pos_map_clone['Demand_Forecast']

        if 'Trade' in da_pos_map_clone.columns:
            da_pos_map_clone['Commited_Trade'] = da_pos_map_clone['Trade']

        renamedict = {'INT_GENERATION_ACT': 'Internal_Gen',
                      'OPT_SCH': 'Availibility',
                      'SURRENDER': 'Surrender',
                      'POSITION_GAP': 'Position_Gap'}
        dropcols = ['OPT_SCHEDULE', 'NET_SCH', 'Trade']
        da_pos_map_clone.drop(dropcols, inplace=True, axis=1, errors='ignore')
        da_pos_map_clone.rename(columns=renamedict, inplace=True)
        # Date object to string
        da_pos_map_clone['Date'] = \
            pd.to_datetime(da_pos_map_clone['Date'], errors='coerce')
        da_pos_map_clone['Date'] = \
            da_pos_map_clone['Date'].dt.strftime('%d-%m-%Y')
        # rowcount
        # rows = da_pos_map_clone.shape[0]
        # columns = [cols for cols in da_pos_map_clone.columns]
        results = [OrderedDict(row) for i, row in da_pos_map_clone.iterrows()]
    except Exception:
        results = None
    engine.dispose()
    return json.dumps(results, use_decimal=True)


# @ems.route("/dtget_positionmap_data", methods=['POST'])
# @login_required
# def dtget_positionmap_data():
#     """
#     Get data from db
#     """
#     date = request.form['date']
#     model = request.form['model']
#     current_app.logger.info('dtget_positionmap_data %s %s', date, model)
#     return get_positionmap_data(date, model, dest='datatables')


@ems.route("/forecast")
@login_required
def forecast():
    """
    When you request the root path, you'll get the index.html template.

    """
    columns = ['Date', 'Block No', 'Forecast']
    return flask.render_template("forecast.html", columns=columns)


@ems.route("/get_forecast_data", methods=['POST'])
@login_required
def get_forecast_data():
    """
    Get data from db
    """
    # date = "03-10-2014"
    date = request.form['date']
    # try:
    #     print "Latets", date
    #     print request.form['date']
    #     print request.form['example2']
    # except:
    #     pass
    columns = ['Date', 'Block_No', 'Demand_forecast']
    index_column = "Date"
    table = "power.forecast_stg"
    where = "where date = STR_TO_DATE('" + date + "','%d-%m-%Y')"
    order = "order by date desc, block_no "
    db = DB()
    cursor = db.cur  # include a reference to your app mysqldb instance
    current_app.logger.info('Finished collecting Data'
                            'from get_forecast_data fn')
    # collection = [dict(zip(columns, [1,2,3,4,5,6,7])), dict(zip(columns, [5,5,5,5,5,5,5]))]
    results = DataTablesServer(request, columns, index_column,
                               table, cursor, where, order).output_result()
    db.close()
    # print "Here2",results
    # return the results as json # import json
    # results = {"aaData": [["1", "2", "3", "4"], ["5", "5", "5", "5"]]}
    return json.dumps(results)


@ems.route("/bforecast")
@login_required
def bforecast():
    """
    When you request the root path, you'll get the index.html template.

    """
    columns = ['Date', 'Block No', 'NBPDCL Forecast',
               'SBPDCL Forecast', 'BPDCL Forecast']
    return flask.render_template("bforecast.html", columns=columns)


@ems.context_processor
def forecastcols():
    """
    When you request the root path, you'll get the index.html template.

    """
    columns = ['Date', 'Block No', 'NBPDCL Forecast',
               'SBPDCL Forecast', 'BPDCL Forecast']
    return dict(forecast_columns=columns)


@ems.context_processor
def forecastcols2():
    """
    When you request the root path, you'll get the index.html template.

    """
    columns = ['Date', 'Discom', 'Model', 'Block_No',
               'Forecast', 'Bias', 'Total']
    return dict(forecast_columns2=columns)


@ems.context_processor
def genforecastcols():
    """
    When you request the root path, you'll get the index.html template.
    """
    columns = ['Date', 'Org_Name', 'Pool_Name', 'Pool_Type',
               'Entity_Name', 'Model_Name', 'Block_No',
               'Gen_Forecast']
    return dict(gen_forecast_columns=columns)


@ems.route('/get_bforecast_data2', methods=['POST'])
@login_required
def get_bforecast_data2():
    # print "modelx", model
    date = request.form['date']
    model = request.form['model']
    discom = request.form['discom']
    current_app.logger.info('%s %s %s', date, model, discom)
    columns = ['Date', 'Discom', 'Model_Name', 'Block_No',
               'Demand_Forecast', 'Demand_Bias']
    index_column = "Date"
    table = "power.forecast_stg"
    if discom == 'ALL' and model == 'ALL':
        where = "where date = STR_TO_DATE('" + date + "','%d-%m-%Y')"
    elif discom == 'ALL' and model != 'ALL':
        where = "where date = STR_TO_DATE('" + date + "','%d-%m-%Y') and model_name = '" + model + "'"
    elif discom != 'ALL' and model == 'ALL':
        where = "where date = STR_TO_DATE('" + date + "','%d-%m-%Y') and discom = '" + discom + "'"
    else:
        where = "where date = STR_TO_DATE('" + date + "','%d-%m-%Y') and model_name = '" + model + "' and discom = '" + discom + "'"
    order = "order by Date, Discom, Model_Name, Block_No "
    db = DB()
    cursor = db.cur  # include a reference to your app mysqldb instance
    # print "Here"
    # collection = [dict(zip(columns, [1,2,3,4,5,6,7])), dict(zip(columns, [5,5,5,5,5,5,5]))]
    results = DataTablesServer(request, columns, index_column,
                               table, cursor, where, order).output_result()
    db.close()
    current_app.logger.info("bforecast %s", results)
    current_app.logger.debug(json.dumps(results))
    # Adding Total column
    for elements in results.get('data'):
        total = float(elements[-2]) + 0\
            if elements[-1] == 'None'\
            else elements[:1]
        # print elements, total
        elements.append(total)

    return json.dumps(results)


@ems.route('/get_bforecast_data3', methods=['POST'])
@login_required
def get_bforecast_data3():
    current_app.logger.info("modelx123 %s %s", request.json, request.form)
    date = request.json['date']
    model = request.json['model']
    discom = request.json['discom']
    # date = "10-10-2015"
    # model = 'ALL'
    # discom = 'ALL'
    current_app.logger.info("%s %s %s", date, model, discom)
    auth_discom = get_org()

    if discom == 'ALL' and model == 'ALL':
        where = " date = STR_TO_DATE('" + date + "','%d-%m-%Y')"
    elif discom == 'ALL' and model != 'ALL':
        where = (" date = STR_TO_DATE('" + date + "','%d-%m-%Y')"
                 " and model_name = '" + model + "'")
    elif discom != 'ALL' and model == 'ALL':
        where = (" date = STR_TO_DATE('" + date + "','%d-%m-%Y')"
                 " and discom = '" + discom + "'")
    else:
        where = (" date = STR_TO_DATE('" + date + "','%d-%m-%Y')"
                 " and model_name = '" + model + "' and"
                 " discom = '" + discom + "'")
    user = current_user.organisation_master_fk
    current_app.logger.info(where)
    if discom in auth_discom.get('org_name', None):
        db = DB()
        # datacursor = con.cursor(cursors.DictCursor)
        datacursor = db.query_dictcursor("""SELECT
            DATE_FORMAT(a.date, '%d-%m-%Y') as Date, a.Discom
            , a.Model_Name, a.Block_No
            , a.Demand_Forecast, a.Demand_Bias
            FROM power.forecast_stg a,
                 (select date, discom, model_name,
                  max(revision) max_revision
                  from power.forecast_stg
                  where {0}
                  group by date, discom, model_name) b,
                (select a.model_name, c.organisation_code as discom
                from power.model_master a,
                     power.model_org_map b,
                     power.organisation_master c
                where a.id = b.model_master_fk
                and b.organisation_master_fk = c.organisation_master_pk
                and (c.organisation_master_pk = {1}
                or c.organisation_parent_fk = {1})
                and a.model_type = 'SINK'
                and a.delete_ind = 0
                and b.delete_ind = 0
                and c.delete_ind = 0 ) c
            where a.date = b.date
            and a.discom = b.discom
            and a.model_name = b.model_name
            and a.revision = b.max_revision
            and a.discom = c.discom
            and a.model_name = c.model_name
            order by a.Date, a.Discom,
            a.Model_Name, a.Block_No""".format(where, user))
        results = datacursor.fetchall()
        db.close()
        return json.dumps(results, use_decimal=True)


@ems.route('/post_forecast_data', methods=['POST'])
@roles_required(['admin', 'jobrunner', 'analytics'])
def post_forecast_data():
    current_app.logger.info('***** started post_forecast_data')
    # current_app.logger.debug("post forecast %s", request.json)
    if request.json:
        forecast_bias_data = json.loads(json.dumps(request.json))
        current_app.logger.debug('post_forecast_data forecast_bias_data %s',
                                 forecast_bias_data)
        keys = ('Date', 'Discom', 'Model_Name', 'Block_No',
                'Demand_Forecast', 'Demand_Bias')
        current_app.logger.info('Passed Keys')
        data = [list(el.get(key) for key in keys)
                for el in forecast_bias_data]
        # current_app.logger.debug('post_forecast_data data %s', data)
        db = DB()
        datacursor = db.query_dictcursor("""SELECT
            b.state_name, c.organisation_code
            from power.state_master b,
                 power.organisation_master c
            where c.state_master_fk = b.state_master_pk
            and (c.organisation_master_pk = %s
            or c.organisation_parent_fk = %s)""", data=(
            current_user.organisation_master_fk,
            current_user.organisation_master_fk))
        org = datacursor.fetchall()
        state = org[0].get('state_name')
        discom = org[0].get('organisation_code')
        current_app.logger.debug('post_forecast_data* %s %s', state, discom)
        datacursor = db.query_dictcursor("""SELECT
            max(Revision) + 1 as new_revision
            from power.forecast_stg
            where date = STR_TO_DATE(%s,'%%d-%%m-%%Y')
            and state = %s
            and discom = %s""", data=(forecast_bias_data[0].get('Date'),
                                      state, discom))
        rev = datacursor.fetchall()
        newrev = rev[0].get('new_revision')
        current_app.logger.info("***** new revision %s", newrev)
        # Add themissing columns data
        for el in data:
            el.insert(1, state)
            el.insert(2, newrev)
            el.append(current_user.id)
        # Change the lists of lists to list of tuples using map
        data = list(map(tuple, data))
        current_app.logger.info("****data to upload***%s", data[0:3])
        sql = """INSERT INTO power.forecast_stg
                (Date,
                State,
                Revision,
                Discom,
                Model_Name,
                Block_No,
                Demand_Forecast,
                Demand_Bias,
                Added_By_FK)
                VALUES
                (STR_TO_DATE(%s,'%%d-%%m-%%Y'),
                 %s,
                 %s,
                 %s,
                 %s,
                 %s,
                 ROUND(%s,3),
                 ROUND(%s,3),
                 %s)"""
        try:
            current_app.logger.debug(sql % data[0])
            db.query_dictcursor(sql, 'insert', data)
            db.query_commit()
            # db.cur.commit()
            db.close()
        except Exception as error:
            # if db.cur.open:
            # db.cur.rollback()
            db.query_rollback()
            db.close()
            current_app.logger.error("Error during Forecast update %s", error)
            raise
        return json.dumps({'success': True}), 200,\
            {'ContentType': 'application/json'}


@ems.route('/get_bforecast_data', methods=['POST'])
@login_required
def get_bforecast_data():
    """
    Get data from db
    """
    # date = "05-08-2015"
    date = request.form['date']
    model = request.form['model']
    # date = dat
    # model = 'GLM'
    current_app.logger.info("model2 %s", model)
    # try:
    #     print "Latets", date
    #     print request.form['date']
    #     print request.form['example2']
    # except:
    #     pass
    columns = ['Date', 'Block_No', 'NBPDCL', 'SBPDCL', 'BPDCL']
    index_column = "Date"
    table = "power.bseb_forecast_demo"
    where = "where date = STR_TO_DATE('" + date + "','%d-%m-%Y') and model = '" + model + "'"
    order = "order by date desc, block_no "
    db = DB()
    cursor = db.cur  # include a reference to your app mysqldb instance
    current_app.logger.info('Finished collecting Data'
                            'from get_bforecast_data fn')
    # collection = [dict(zip(columns, [1,2,3,4,5,6,7])), dict(zip(columns, [5,5,5,5,5,5,5]))]
    results = DataTablesServer(request, columns, index_column,
                               table, cursor, where, order).output_result()
    db.close()
    current_app.logger.debug("Here2 %s", results)
    # return the results as json # import json
    # results = {"aaData": [["1", "2", "3", "4"], ["5", "5", "5", "5"]]}
    # datacursor = con.cursor(cursors.DictCursor)
    # print """select SQL_CALC_FOUND_ROWS date, block_no,
                #       sum(case when discom_name = 'NBPDCL' then demand_forecast end) as NBPDCL,
                #       sum(case when discom_name = 'SBPDCL' then demand_forecast end) as SBPDCL,
                #       sum(demand_forecast) as BPDCL
                #       from power.forecast_stg
                #       where state = 'BIHAR'
                #       and date = STR_TO_DATE('%s','%%d-%%m-%%Y')
                #       group by date, state, block_no""" % date
    # datacursor.execute("""select SQL_CALC_FOUND_ROWS DATE_FORMAT(date, '%%Y-%%m-%%d') as date, block_no,
                #       sum(case when discom_name = 'NBPDCL' then demand_forecast end) as NBPDCL,
                #       sum(case when discom_name = 'SBPDCL' then demand_forecast end) as SBPDCL,
                #       sum(demand_forecast) as BPDCL
                #       from power.forecast_stg
                #       where state = 'BIHAR'
                #       and date = STR_TO_DATE('%s','%%d-%%m-%%Y')
                #       group by date, state, block_no""" % date)
    # results2 = datacursor.fetchall()
    # print results2
    return json.dumps(results)


@ems.route('/get_genforecast_data', methods=['POST'])
@login_required
def get_genforecast_data():
    current_app.logger.info("get_genforecast_data %s %s", request.json,
                            request.form)
    # date = request.json['date']
    # model = request.json['model']
    # org_name = request.json['org_name']
    date = request.form['date']
    model = request.form['model']
    org_name = request.form['org_name']

    current_app.logger.info("%s %s %s", date, model, org_name)
    auth_discom = get_org()

    if org_name == 'ALL' and model == 'ALL':
        where = " date = STR_TO_DATE('" + date + "','%d-%m-%Y')"
    elif org_name == 'ALL' and model != 'ALL':
        where = (" date = STR_TO_DATE('" + date + "','%d-%m-%Y')"
                 " and model_name = '" + model + "'")
    elif org_name != 'ALL' and model == 'ALL':
        where = (" date = STR_TO_DATE('" + date + "','%d-%m-%Y')"
                 " and org_name = '" + org_name + "'")
    else:
        where = (" date = STR_TO_DATE('" + date + "','%d-%m-%Y')"
                 " and model_name = '" + model + "' and"
                 " org_name = '" + org_name + "'")
    user = current_user.organisation_master_fk
    current_app.logger.info(where)
    if org_name in auth_discom.get('org_name', None):
        db = DB()
        # datacursor = con.cursor(cursors.DictCursor)
        datacursor = db.query_dictcursor("""SELECT
            DATE_FORMAT(a.date, '%d-%m-%Y') as Date, a.Org_Name,
            a.Pool_Name, a.Pool_Type, a.Entity_Name
            , a.Model_Name, a.Block_No
            , a.Gen_Forecast
            FROM power.gen_forecast_stg a,
                 (select date, org_name,
                  pool_name, pool_type,
                  entity_name, model_name,
                  max(revision) max_revision
                  from power.gen_forecast_stg
                  where {0}
                  group by date, org_name, pool_name,
                  pool_type, entity_name, model_name) b,
                (select a.model_name, c.organisation_code as org_name
                from power.model_master a,
                     power.model_org_map b,
                     power.organisation_master c
                where a.id = b.model_master_fk
                and b.organisation_master_fk = c.organisation_master_pk
                and (c.organisation_master_pk = {1}
                or c.organisation_parent_fk = {1})
                and a.model_type = 'INJECTION'
                and a.delete_ind = 0
                and b.delete_ind = 0
                and c.delete_ind = 0 ) c
            where a.date = b.date
            and a.org_name = b.org_name
            and a.model_name = b.model_name
            and a.revision = b.max_revision
            and a.org_name = c.org_name
            and a.model_name = c.model_name
            and a.pool_name = b.pool_name
            and a.pool_type = b.pool_type
            and a.entity_name = b.entity_name
            order by a.Date, a.Block_No, a.Org_Name,
            a.Pool_Name,  a.Pool_Type, a.Entity_Name,
            a.Model_Name""".format(where, user))
        results = datacursor.fetchall()
        datacursor = db.query_dictcursor("""SELECT FOUND_ROWS() _rows""")
        rows = datacursor.fetchall()[0]['_rows']
        db.close()
        columns = ['Date', 'Org_Name', 'Pool_Name', 'Pool_Type', 'Entity_Name',
                   'Model_Name', 'Block_No', 'Gen_Forecast']
        current_app.logger.info('Total Number of Rows: %s', rows)
        current_app.logger.debug('Resultdt: %s', results)
        dt_output = rawsql_to_datatables(columns, results, rows, rows)
        return json.dumps(dt_output, use_decimal=True)
        # return json.dumps(results, use_decimal=True)


@ems.route('/get_genforecast_graph', methods=['POST'])
@login_required
def get_genforecast_graph():
    current_app.logger.info("get_genforecast_graph %s %s", request.json,
                            request.form)
    date = request.json['date']
    model = request.json['model']
    org_name = request.json['org_name']

    current_app.logger.info("%s %s %s", date, model, org_name)
    auth_discom = get_org()

    if org_name == 'ALL' and model == 'ALL':
        where = " date = STR_TO_DATE('" + date + "','%d-%m-%Y')"
    elif org_name == 'ALL' and model != 'ALL':
        where = (" date = STR_TO_DATE('" + date + "','%d-%m-%Y')"
                 " and model_name = '" + model + "'")
    elif org_name != 'ALL' and model == 'ALL':
        where = (" date = STR_TO_DATE('" + date + "','%d-%m-%Y')"
                 " and org_name = '" + org_name + "'")
    else:
        where = (" date = STR_TO_DATE('" + date + "','%d-%m-%Y')"
                 " and model_name = '" + model + "' and"
                 " org_name = '" + org_name + "'")
    user = current_user.organisation_master_fk
    current_app.logger.info(where)
    if org_name in auth_discom.get('org_name', None):
        db = DB()
        # datacursor = con.cursor(cursors.DictCursor)
        datacursor = db.query_dictcursor("""SELECT
            DATE_FORMAT(a.date, '%d-%m-%Y') as Date, a.Org_Name,
            a.Pool_Name, a.Pool_Type, a.Entity_Name
            , a.Model_Name, a.Block_No
            , a.Gen_Forecast
            FROM power.gen_forecast_stg a,
                 (select date, org_name,
                  pool_name, pool_type,
                  entity_name, model_name,
                  max(revision) max_revision
                  from power.gen_forecast_stg
                  where {0}
                  group by date, org_name, pool_name,
                  pool_type, entity_name, model_name) b,
                (select a.model_name, c.organisation_code as org_name
                from power.model_master a,
                     power.model_org_map b,
                     power.organisation_master c
                where a.id = b.model_master_fk
                and b.organisation_master_fk = c.organisation_master_pk
                and (c.organisation_master_pk = {1}
                or c.organisation_parent_fk = {1})
                and a.model_type = 'INJECTION'
                and a.delete_ind = 0
                and b.delete_ind = 0
                and c.delete_ind = 0 ) c
            where a.date = b.date
            and a.org_name = b.org_name
            and a.model_name = b.model_name
            and a.revision = b.max_revision
            and a.org_name = c.org_name
            and a.model_name = c.model_name
            and a.pool_name = b.pool_name
            and a.pool_type = b.pool_type
            and a.entity_name = b.entity_name
            order by a.Date, a.Org_Name,
            a.Pool_Name, a.Pool_Type, a.Entity_Name,
            a.Model_Name, a.Block_No""".format(where, user))
        results = datacursor.fetchall()
        datacursor = db.query_dictcursor("""SELECT FOUND_ROWS() _rows""")
        rows = datacursor.fetchall()[0]['_rows']
        db.close()
        current_app.logger.info('Total Number of Rows: %s', rows)
        # current_app.logger.debug('ResultGr: %s', results)
        return json.dumps(results, use_decimal=True)


@ems.route("/tenint")
@login_required
def tenint():
    """
    When you request the root path, you'll get the index.html template.

    """
    # columns = [ 'column_1', 'column_2', 'column_3', 'column_4']
    columns = ['Date', 'Station Name', 'Hour1', 'Hour2', 'Hour3', 'Hour4',
               'Hour5', 'Hour6', 'Hour7', 'Hour8', 'Hour9', 'Hour10',
               'Hour11', 'Hour12', 'Hour13', 'Hour14', 'Hour15', 'Hour16',
               'Hour17', 'Hour18', 'Hour19', 'Hour20', 'Hour21', 'Hour22',
               'Hour23', 'Hour24'
               ]
    return flask.render_template("tenint.html", columns=columns)


@ems.route("/get_tenint_data")
@login_required
def get_tenint_data():
    """
    Get data from db
    """
    columns = ['Date', 'Attribute_Name', 'Hr1', 'Hr2', 'Hr3', 'Hr4',
               'Hr5', 'Hr6', 'Hr7', 'Hr8', 'Hr9', 'Hr10',
               'Hr11', 'Hr12', 'Hr13', 'Hr14', 'Hr15', 'Hr16',
               'Hr17', 'Hr18', 'Hr19', 'Hr20', 'Hr21', 'Hr22',
               'Hr23', 'Hr24'
               ]
    index_column = "Date"
    table = "power.tentative_schedule_stg"
    where = "where date >= (CURDATE() + INTERVAL -(5) DAY) "
    order = "order by date desc"
    db = DB()
    cursor = db.cur  # include a reference to your app mysqldb instance
    current_app.logger.info('Finished collecting Data from get_tenint_data fn')
    # collection = [dict(zip(columns, [1,2,3,4,5,6,7])), dict(zip(columns, [5,5,5,5,5,5,5]))]
    results = DataTablesServer(request, columns, index_column,
                               table, cursor, where, order).output_result()
    db.close()
    # print "Here2",results
    # return the results as json # import json
    # results = {"aaData": [["1", "2", "3", "4"], ["5", "5", "5", "5"]]}
    return json.dumps(results)


@ems.route("/")
def login():
    """
    When you request the root path, you'll get the index.html template.
    """
    return flask.render_template("login.html")


@ems.route("/loginpost", methods=['POST'])
@login_required
def loginpost():
    """
    When you request the root path, you'll get the index.html template.

    """
    # return flask.redirect("/nav")
    # return flask.redirect("/biharlive")
    # return flask.redirect(url_for('ems.biharlive'))
    return flask.redirect(url_for('ems.live'))


@ems.route("/heat")
@login_required
def heat():
    """
    When you request the root path, you'll get the index.html template.

    """
    return flask.render_template("heatmap2.html")


@ems.route("/get_heat_data")
@login_required
def get_heat_data():
    """
    When you request the root path, you'll get the index.html template.

    """
    db = DB()
    # datacursor = con.cursor(cursors.DictCursor)
    datacursor = db.query_dictcursor("""SELECT
                            convert(DATE_FORMAT(a.Date, '%d'),
                                UNSIGNED INTEGER) AS row,
                            b.Block_No AS col,
                            # AVG(a.Frequency) AS frequency,
                            # AVG(a.UI_Rate) AS ui_rate,
                            # AVG(a.NR_OD_UD) AS nr_od_ud,
                            # case when AVG(a.Drawl) >= 9500 then 9500/9500
                            # else AVG(a.Drawl)/9500  end AS val
                             AVG(a.Drawl) AS val
                        FROM
                             power.sldc_scada_mis a,
                             power.block_master b
                        WHERE
                            (a.Time BETWEEN b.Start_Time
                                AND b.End_Time
                             and a.Date >= '2013-11-01'
                             and a.Date <= '2013-11-30')
                        GROUP BY a.Date , b.Block_No""")
    results = datacursor.fetchall()
    db.close()
    return json.dumps(results, use_decimal=True)


@ems.route("/bseb_heatmap")
@login_required
def bseb_heatmap():
    """
    When you request the root path, you'll get the index.html template.

    """
    # return flask.render_template("bseb_heatmap.html")
    # return flask.render_template("testheatmap.html")
    # return flask.render_template("calendarheatmap.html")
    return flask.render_template("bseb_heatmap2.html")
    # return flask.render_template("axisexample.html")


@ems.route("/get_bsebheatmap_data/<month>")
@login_required
def get_bsebheatmap_data(month):
    """
    When you request the root path, you'll get the index.html template.
    """
    db = DB()
    # datacursor = con.cursor(cursors.DictCursor)
    datacursor = db.query_dictcursor("""SELECT date_format(a.date, '%%d-%%m-%%Y') as date,
                        a.block_no, max(a.constrained_load) +
                        coalesce(max(a.powercut),0) as total_drawl
                        from (
                        select date, block_no, constrained_load,
                        null as powercut
                        from power.drawl_staging
                        where state = 'BIHAR'
                        and discom = 'BPDCL'
                        and month(date) =
                        month(STR_TO_DATE('%s','%%m-%%Y'))
                        and year(date) =
                        year(STR_TO_DATE('%s','%%m-%%Y'))
                        union all
                        select date, block_no, null as constrained_load,
                        powercut
                        from power.powercut_staging
                        where state = 'BIHAR'
                        and discom = 'BPDCL'
                        and month(date) =
                        month(STR_TO_DATE('%s','%%m-%%Y'))
                        and year(date) =
                        year(STR_TO_DATE('%s','%%m-%%Y'))) a
                        group by a.date, a.block_no""" % (month, month,
                                                          month, month))
    results = datacursor.fetchall()
    db.close()
    return json.dumps(results, use_decimal=True)


@ems.route("/heatmap")
@login_required
def heatmap():
    """
    When you request the root path, you'll get the index.html template.

    """
    # return flask.render_template("bseb_heatmap.html")
    # return flask.render_template("testheatmap.html")
    # return flask.render_template("calendarheatmap.html")
    return flask.render_template("heatmap_jinja.html")
    # return flask.render_template("axisexample.html")


@ems.route("/get_heatmap_data/<month>")
@login_required
def get_heatmap_data(month):
    """
    When you request the root path, you'll get the index.html template.
    """
    db = DB()
    # datacursor = con.cursor(cursors.DictCursor)
    datacursor = db.query_dictcursor("""SELECT
        b.state_name, c.organisation_code
        from power.state_master b,
             power.organisation_master c
        where c.state_master_fk = b.state_master_pk
        and (c.organisation_master_pk = %s
        or c.organisation_parent_fk = %s)""",
                                     data=(current_user.organisation_master_fk,
                                           current_user.organisation_master_fk)
                                     )
    org = datacursor.fetchall()
    state = org[0].get('state_name')
    discom = org[0].get('organisation_code')
    # Create a valid date as using STRICT database option
    # make the STR_TO_DATE return NULL
    date = '01-' + month
    current_app.logger.info("get_heatmap_data %s %s %s %s",
                            date, month, state, discom)
    datacursor = db.query_dictcursor("""SELECT date_format(a.date, '%%d-%%m-%%Y') as date,
        a.block_no, sum(a.constrained_load) +
        coalesce(sum(a.powercut),0) as total_drawl
        from (
        select date, block_no, constrained_load,
        null as powercut
        from power.drawl_staging
        where state = %s
        and discom = %s
        and month(date) = month(STR_TO_DATE(%s, '%%d-%%m-%%Y'))
        and year(date) = year(STR_TO_DATE(%s, '%%d-%%m-%%Y'))
        union all
        select date, block_no, null as constrained_load,
        powercut
        from power.powercut_staging
        where state = %s
        and discom = %s
        and month(date) = month(STR_TO_DATE(%s, '%%d-%%m-%%Y'))
        and year(date) = year(STR_TO_DATE(%s, '%%d-%%m-%%Y'))) a
        group by a.date, a.block_no""", data=(state, discom, date, date,
                                              state, discom, date, date))
    results = datacursor.fetchall()
    db.close()
    return json.dumps(results, use_decimal=True)


@ems.route("/treemap")
@login_required
def treemap():
    """
    When you request the root path, you'll get the index.html template.

    """
    return flask.render_template("treemap3.html")


@ems.route("/bseb_treemap")
@login_required
def bseb_treemap():
    """
    When you request the root path, you'll get the index.html template.

    """
    # return flask.render_template("bseb_treemap.html")
    return flask.render_template("bseb_treemap2.html")


@ems.route("/get_bsebtreemap_data")
@login_required
def get_bsebtreemap_data():
    """
    When you request the root path, you'll get the index.html template.

    """
    # date = request.form['date']
    db = DB()
    # datacursor = con.cursor(cursors.DictCursor)

    #datacursor.execute("""
                #       select date_format(a.date,'%d-%m-%Y') as date, block_no, 'BPDCL' as Parent, 'NBPDCL' as Discom ,
                #       'purnea_pg_purnea_bseb' as tieline,
                #       round(avg(coalesce(a.purnea_pg_purnea_bseb,0)),0) as value
                #       from power.bseb_scada_stg a ,
                #            power.block_master b
                #       where (a.time BETWEEN b.Start_Time AND b.End_Time)
                #       and a.date = '2015-02-15'
                #       and b.block_no = 1
                #       group by date, block_no
                #       union all
                #       select date_format(a.date,'%d-%m-%Y') as date, block_no, 'BPDCL' as Parent, 'NBPDCL' as Discom ,
                #       'purnea_pg_kishangunj' as tieline,
                #       round(avg(coalesce(a.purnea_pg_kishangunj,0)),0) as value
                #       from power.bseb_scada_stg a ,
                #            power.block_master b
                #       where (a.time BETWEEN b.Start_Time AND b.End_Time)
                #       and a.date = '2015-02-15'
                #       and b.block_no = 1
                #       group by date, block_no
                #       union all
                #       select date_format(a.date,'%d-%m-%Y') as date, block_no, 'BPDCL' as Parent, 'NBPDCL' as Discom ,
                #       'muzaffarpur_pg_mtps_kanti' as tieline,
                #       round(avg(coalesce(a.muzaffarpur_pg_mtps_kanti,0)),0) as value
                #       from power.bseb_scada_stg a ,
                #            power.block_master b
                #       where (a.time BETWEEN b.Start_Time AND b.End_Time)
                #       and a.date = '2015-02-15'
                #       and b.block_no = 1
                #       group by date, block_no
                #       union all
                #       select date_format(a.date,'%d-%m-%Y') as date, block_no, 'BPDCL' as Parent, 'SBPDCL' as Discom ,
                #       'purnea_pg_purnea_bseb' as tieline,
                #       round(avg(coalesce(a.purnea_pg_purnea_bseb,0)),0) as value
                #       from power.bseb_scada_stg a ,
                #            power.block_master b
                #       where (a.time BETWEEN b.Start_Time AND b.End_Time)
                #       and a.date = '2015-02-15'
                #       and b.block_no = 1
                #       group by date, block_no
                #       union all
                #       select date_format(a.date,'%d-%m-%Y') as date, block_no, 'BPDCL' as Parent, 'SBPDCL' as Discom ,
                #       'purnea_pg_kishangunj' as tieline,
                #       round(avg(coalesce(a.purnea_pg_kishangunj,0)),0) as value
                #       from power.bseb_scada_stg a ,
                #            power.block_master b
                #       where (a.time BETWEEN b.Start_Time AND b.End_Time)
                #       and a.date = '2015-02-15'
                #       and b.block_no = 1
                #       group by date, block_no
                #       union all
                #       select date_format(a.date,'%d-%m-%Y') as date, block_no, 'BPDCL' as Parent, 'SBPDCL' as Discom ,
                #       'muzaffarpur_pg_mtps_kanti' as tieline,
                #       round(avg(coalesce(a.muzaffarpur_pg_mtps_kanti,0)),0) as value
                #       from power.bseb_scada_stg a ,
                #            power.block_master b
                #       where (a.time BETWEEN b.Start_Time AND b.End_Time)
                #       and a.date = '2015-02-15'
                #       and b.block_no = 1
                #       group by date, block_no
                #       """)
    datacursor = db.query_dictcursor("""SELECT * from
        power.bseb_tieline_tabular a
        where block_no = (select max(block_no)
        from power.bseb_tieline_tabular b
        where a.date= b.date)""")
    results = datacursor.fetchall()
    db.close()
    return json.dumps(results, use_decimal=True)


@ems.route("/heatmap3")
@login_required
def heatmap3():
    """
    When you request the root path, you'll get the index.html template.

    """
    return flask.render_template("heatmap3.html")


@ems.route("/nav")
@login_required
def nav():
    """
    When you request the root path, you'll get the index.html template.

    """
    return flask.render_template("navigation_page2.html")


# @ems.route("/biharlive")
# @login_required
# def biharlive():
#     """
#     When you request the root path, you'll get the index.html template.

#     """
#     # return flask.render_template("biharlive.html")
#     return flask.render_template("biharlive_jinja.html")
#     # return flask.render_template("biharlivetest_jinja.html")


@ems.route("/live")
@login_required
def live():
    try:
        db = DB()
        # datacursor = con.cursor(cursors.DictCursor)
        datacursor = db.query_dictcursor("""SELECT
            ldc_name, ldc_org_name
            from power.org_isgs_map a,
            power.organisation_master b
            where a.organisation_master_fk = b.organisation_master_pk
            and (b.organisation_master_pk = %s
            or b.organisation_parent_fk = %s)
            and a.delete_ind = 0
            and b.delete_ind = 0""", data=(
            current_user.organisation_master_fk,
            current_user.organisation_master_fk))
        org = datacursor.fetchall()
        db.close()
        ldc = org[0].get('ldc_name')
        if ldc == 'ERLDC':
            schedule = ['Date', 'Revison', 'Block No', 'ISGS',
                        'LTOA/MTOA', 'Bilateral', 'IEX', 'PXIL', 'Int Gen',
                        'Net Schedule', 'Regulation']
        elif ldc == 'NRLDC':
            schedule = ['Date', 'Revison', 'Block No', 'ISGS',
                        'LTOA', 'MTOA', 'Shared', 'Bilateral', 'IEX', 'PXIL',
                        'Int Gen', 'Net Schedule']
        elif ldc == 'WRLDC':
            schedule = ['Date', 'Revison', 'Block No', 'ISGS',
                        'LTOA', 'MTOA', 'Shared', 'Bilateral', 'IEX', 'PXIL',
                        'Int Gen', 'Net Schedule']
        else:
            schedule = []
    except Exception:
        schedule = []
    return flask.render_template("live_jinja.html", gen_columns=schedule)


@ems.route("/bootstraptest")
def bootstraptest():
    """
    When you request the root path, you'll get the index.html template.

    """
    # return flask.render_template("bootstraptest.html")
    return flask.render_template("testtemplate.html")


@ems.route("/get_nav_data")
@login_required
def get_nav_data():
    """
    When you request the root path, you'll get the index.html template.

    """
    db = DB()
    # datacursor = con.cursor(cursors.DictCursor)
    datacursor = db.query_dictcursor("""SELECT DATE_FORMAT(d.date, '%d-%m-%Y') as Date
        ,d.block_no
        ,@int_gen := round(localgen.MW_OWN_GEN -
          localgen.MW_WIND,2) as int_gen
        ,@nr_gen := round(isgsgen.ISGS +
          isgsgen.LTA + isgsgen.MTOA + isgsgen.Shared +
          isgsgen.Bilateral + isgsgen.IEX_PXIL,2) as NR_gen
        ,@oa := round(localgen.MW_OA,2) as OA
        ,round(d.Wind_Gen_Forecast,2) as Wind_Forecast
        ,@wind := round(localgen.MW_WIND,2) as Wind
        ,@solar:= round(d.Solar_Gen_Forecast,2) as Solar
        ,@tot_own_gen := round(@int_gen +
          @wind + @solar,2) as tot_own_gen
        ,@forecast := round(d.demand_forecast,2) as forecast
        ,@availibility := round(@int_gen + @nr_gen +
          @oa + @wind + @solar,2) as Availibility
        ,@pos_gap := round(@availibility -
            @forecast,2) as position_gap
        ,@drawl := round(drawl.drawl,2) as live_demand
        ,@frequency := round(drawl.frequency,2) as frequency
        from
        (
            SELECT
                a.Date AS date,
                b.Block_No AS block_no,
                AVG(a.Frequency) AS frequency,
                AVG(a.UI_Rate) AS ui_rate,
                AVG(a.NR_OD_UD) AS nr_od_ud,
                AVG(a.Drawl) AS drawl
            FROM
                (power.sldc_scada_mis a
                JOIN power.block_master b)
            WHERE
                (a.Time BETWEEN b.Start_Time
                    AND b.End_Time
                 and a.Date = '2014-09-22')
            GROUP BY a.Date , b.Block_No
        ) drawl,
            (SELECT
                CAST(d.snapshot_date_time
                    AS DATE) AS date,
                d.block_no AS block_no,
                AVG(d.MW_WIND) AS MW_WIND,
                AVG(d.MW_OTHERS) AS MW_OTHERS,
                AVG(d.MW_CPP) AS MW_CPP,
                AVG(d.MW_OA) AS MW_OA,
                AVG(d.MW_OWN_GEN) AS MW_OWN_GEN
            FROM
                (    SELECT
                        c.snapshot_date_time
                          AS snapshot_date_time,
                        b.Block_No AS block_no,
                        SUM(c.MW_WIND) AS MW_WIND,
                        SUM(c.MW_OTHERS) AS MW_OTHERS,
                        SUM(c.MW_CPP) AS MW_CPP,
                        SUM(c.MW_OA) AS MW_OA,
                        SUM(c.MW_OWN_GEN) AS MW_OWN_GEN
                    FROM
                        ( SELECT
                            a.Snapshot_Date_Time AS snapshot_date_time,
                            (CASE
                                WHEN (UCASE(a.Station_Name) LIKE '%WIND%') THEN SUM(a.MW)
                            END) AS MW_WIND,
                            (CASE
                                WHEN
                                    ((UCASE(a.Station_Name) NOT IN ('CPP' , 'OPEN ACCESS', 'OWN GEN.'))
                                        AND (NOT ((UCASE(a.Station_Name) LIKE '%WIND%'))))
                                THEN
                                    SUM(a.MW)
                            END) AS MW_OTHERS,
                            (CASE
                                WHEN (UCASE(a.Station_Name) = 'CPP') THEN SUM(a.MW)
                            END) AS MW_CPP,
                            (CASE
                                WHEN (UCASE(a.Station_Name) = 'OPEN ACCESS') THEN SUM(a.MW)
                            END) AS MW_OA,
                            (CASE
                                WHEN (UCASE(a.Station_Name) = 'OWN GEN.') THEN SUM(a.MW)
                            END) AS MW_OWN_GEN
                            FROM
                                power.sldc_scada_snapshot a
                            WHERE
                                (CAST(a.Snapshot_Date_Time AS DATE) = '2014-09-22')
                            GROUP BY a.Snapshot_Date_Time , UCASE(a.Station_Name)
                        ) c
                        JOIN power.block_master b
                    WHERE
                        (CAST(c.snapshot_date_time AS TIME) BETWEEN b.Start_Time AND b.End_Time)
                    GROUP BY c.snapshot_date_time
                ) d
                group by CAST(d.snapshot_date_time AS DATE) , d.block_no
                ) localgen,
        (    SELECT
                a.Date AS date,
                a.Revision AS revision,
                a.Block_No AS Block_No,
                SUM((CASE
                    WHEN (a.Drawl_Type = 'ISGS') THEN a.Schedule
                    ELSE 0
                END)) AS ISGS,
                SUM((CASE
                    WHEN (a.Drawl_Type = 'LTA') THEN a.Schedule
                    ELSE 0
                END)) AS LTA,
                SUM((CASE
                    WHEN (a.Drawl_Type = 'MTOA') THEN a.Schedule
                    ELSE 0
                END)) AS MTOA,
                SUM((CASE
                    WHEN (a.Drawl_Type = 'Shared') THEN a.Schedule
                    ELSE 0
                END)) AS Shared,
                SUM((CASE
                    WHEN (a.Drawl_Type = 'Bilateral') THEN a.Schedule
                    ELSE 0
                END)) AS Bilateral,
                SUM((CASE
                    WHEN (a.Drawl_Type = 'IEX_PXIL') THEN a.Schedule
                    ELSE 0
                END)) AS IEX_PXIL
            FROM power.panel_nrldc_state_drawl_schedule a
            WHERE a.Date >= '2014-09-22'
            GROUP BY a.Date , a.Revision , a.Block_No , a.State) isgsgen,
        power.forecast_stg d
        where drawl.date = localgen.date
        and drawl.block_no = localgen.block_no
        and drawl.date = isgsgen.date
        and drawl.block_no = isgsgen.block_no
        and drawl.date = d.date
        and drawl.block_no = d.block_no""")
    results = datacursor.fetchall()
    db.close()
    # results = [row["Id"] for row in datacursor.fetchall()]
    # results = ['AD_06-03-2013','AD_07-03-2013','AD_08-03-2013']
    # print "QSEarch", results
    # return flask.render_template("test.html" , data=json.dumps(results))
    return json.dumps(results, use_decimal=True)


@ems.route("/stacked")
@login_required
def stacked():
    """
    When you request the root path, you'll get the index.html template.

    """
    #print "In Chart3"
    return flask.render_template("stackedToGroupChart.html")


@ems.route("/get_stacked_data")
@login_required
def get_stacked_data():
    """
    When you request the root path, you'll get the index.html template.

    """
    db = DB()
    # datacursor = con.cursor(cursors.DictCursor)
    datacursor = db.query_dictcursor("""SELECT date, drawl_type, x , y
        from (
        (select  DATE_FORMAT(date, '%d-%m-%Y') as date,
        drawl_type, Block_No as x,
        sum(Schedule) as y
        from
        power.panel_nrldc_state_drawl_schedule a
        where Date = '2014-09-22' and Revision =64
        and State = 'RAJASTHAN'
        group  by Date, State,Drawl_Type, Block_No)
        union
        (SELECT DATE_FORMAT(date, '%d-%m-%Y') as date,
         drawl_type, block_no as x,
         avg(schedule) as y
        FROM
        (SELECT
        CAST(a.Snapshot_Date_Time AS DATE) AS date,
        b.block_no,
        CASE
        WHEN (UCASE(a.Station_Name) LIKE '%WIND%')
         THEN 'WIND'
        WHEN
        ((UCASE(a.Station_Name)
        NOT IN ('CPP' , 'OPEN ACCESS', 'OWN GEN.'))
        AND (NOT ((UCASE(a.Station_Name)
        LIKE '%WIND%')))) THEN 'OTHERS'
        WHEN (UCASE(a.Station_Name) = 'CPP') THEN 'CPP'
        WHEN (UCASE(a.Station_Name) = 'OPEN ACCESS')
        THEN 'OA'
        WHEN (UCASE(a.Station_Name) = 'OWN GEN.')
        THEN 'OWNGEN'
        END as Drawl_Type,
        a.MW as Schedule
        FROM
        power.sldc_scada_snapshot a,
        power.block_master b
        WHERE (CAST(a.Snapshot_Date_Time AS DATE)
        = '2014-09-22')
        AND (CAST(a.snapshot_date_time AS TIME)
        BETWEEN b.Start_Time AND b.End_Time)
        ) a
        group by date, drawl_type, block_no)) b
        order by date, drawl_type, x""")
    results = datacursor.fetchall()
    db.close()
    # results = [row["Id"] for row in datacursor.fetchall()]
    # results = ['AD_06-03-2013','AD_07-03-2013','AD_08-03-2013']
    # print "QSEarch", results
    # return flask.render_template("test.html" , data=json.dumps(results))
    return json.dumps(results, use_decimal=True)


@ems.route("/bseb_gen_stacked")
@login_required
def bseb_gen_stacked():
    """
    When you request the root path, you'll get the index.html template.

    """
    #print "In Chart3"
    # return flask.render_template("bseb_gen_stacked.html")
    return flask.render_template("stacked_test.html")


# @ems.route("/get_bsebgenstacked_data")
# def get_bsebgenstacked_data():
@ems.route("/get_bsebgenstacked_data/<date>")
@login_required
def get_bsebgenstacked_data(date):
    """
    When you request the root path, you'll get the index.html template.

    """
    db = DB()
    # datacursor = con.cursor(cursors.DictCursor)
    datacursor = db.query_dictcursor("""SELECT
        DATE_FORMAT(date, '%%d-%%m-%%Y') date, drawl_type, x, y
        from(select a.date, a.block_no as x, a.drawl_type
            ,sum(case when drawl_type = 'ISGS'
                      and station_name <> 'NET|DRAWAL|SCHD.' then schedule
                      when drawl_type = 'BILATERAL'
                        and station_name = 'BILAT|TOTAL' then schedule
                      when drawl_type = 'LTOA_MTOA' then schedule
                      when drawl_type = 'IEX' and station_name = 'IEX|TOT.'
                        then schedule
                      when drawl_type = 'PXIL' and station_name = 'PXI|TOT.'
                        then schedule end) as y
            from erldc_state_drawl_schedule_stg a,
                 (select date, discom, max(revision) as revision
                  from erldc_state_drawl_schedule_stg
                  where date = STR_TO_DATE('%s', '%%d-%%m-%%Y')
                  and discom = 'BSEB' group by date, discom) b
            where a.date = b.date
            and a.discom = b.discom
            and a.revision = b.revision
            and a.drawl_type in ('ISGS', 'BILATERAL', 'LTOA_MTOA',
                                 'IEX', 'PXIL')
            group by a.date, a.block_no, a.revision, a.discom, a.drawl_type
            union all
            select c.date, c.block_no as x, 'INTERNAL' as drawl_type,
            c.kbunl_projected as y
            from power.bseb_real_time_forecast c,
                 (select date, max(revison) max_revison
                  from power.bseb_real_time_forecast
                  where date = STR_TO_DATE('%s', '%%d-%%m-%%Y')
                  group by date) d
            where d.date = c.date
            and c.revison = d.max_revison
        ) z
        order by date, drawl_type, x""" % (date, date))
    results = datacursor.fetchall()
    db.close()
    return json.dumps(results, use_decimal=True)


@ems.route("/get_genstacked_data/<date>")
@login_required
def get_genstacked_data(date):
    """
    When you request the root path, you'll get the index.html template.
    TODO: Seperate Internal Generation, Renewable Genration
    """
    current_app.logger.info("Inside get_genstacked_data")
    db = DB()
    # datacursor = con.cursor(cursors.DictCursor)
    datacursor = db.query_dictcursor("""SELECT
        ldc_name, ldc_org_name, organisation_code,
        d.zone_code
        from org_isgs_map a,
             organisation_master b,
             state_master c,
             zone_master d
        where a.organisation_master_fk = b.organisation_master_pk
        and c.state_master_pk = b.state_master_fk
        and c.exchange_zone_master_fk = d.zone_master_pk
        and (b.organisation_master_pk = %s
            or b.organisation_parent_fk = %s)""", data=(
        current_user.organisation_master_fk,
        current_user.organisation_master_fk))
    org = datacursor.fetchall()
    ldc = org[0].get('ldc_name')
    discom = org[0].get('ldc_org_name')
    current_app.logger.info("get_genstacked_data %s %s %s", ldc, discom, date)
    # ldc = 'ERLDC'
    # discom = 'BSEB'
    if ldc == 'ERLDC' and discom == 'BSEB':
        datacursor = db.query_dictcursor("""SELECT
            DATE_FORMAT(date, '%%d-%%m-%%Y') date, drawl_type, x, y
            from(select a.date, a.block_no as x, a.drawl_type
                ,sum(case when a.drawl_type = 'ISGS'
                          and a.station_name <> 'NET|DRAWAL|SCHD.'
                           then a.schedule
                          when a.drawl_type = 'BILATERAL'
                            and a.station_name = 'BILAT|TOTAL' then a.schedule
                          when a.drawl_type = 'LTOA_MTOA' then a.schedule
                          when a.drawl_type = 'IEX' and
                          a.station_name = 'IEX|TOT.'
                            then a.schedule
                          when a.drawl_type = 'PXIL'
                          and a.station_name = 'PXI|TOT.'
                            then a.schedule end) as y
                from erldc_state_drawl_schedule_stg a,
                     (select date, discom, drawl_type,
                      max(revision) as revision
                      from erldc_state_drawl_schedule_stg
                      where date = STR_TO_DATE(%s, '%%d-%%m-%%Y')
                      and discom = '%s'
                      group by date, discom, drawl_type) b
                where a.date = b.date
                and a.discom = b.discom
                and a.revision = b.revision
                and a.drawl_type = b.drawl_type
                and a.drawl_type in ('ISGS', 'BILATERAL', 'LTOA_MTOA',
                                     'IEX', 'PXIL')
                group by a.date, a.block_no, a.revision, a.discom, a.drawl_type
                union all
                select c.date, c.block_no as x, 'INTERNAL' as drawl_type,
                c.kbunl_projected as y
                from power.bseb_real_time_forecast c,
                     (select date, max(revison) max_revison
                      from power.bseb_real_time_forecast
                      where date = STR_TO_DATE(%s, '%%d-%%m-%%Y')
                      group by date) d
                where d.date = c.date
                and c.revison = d.max_revison) z
            order by date, drawl_type, x""", data=(date, discom, date))
    elif ldc == 'NRLDC':
        datacursor = db.query_dictcursor("""SELECT
              DATE_FORMAT(a.date, '%%d-%%m-%%Y') as date,
              case when a.drawl_type = 'IEX_PXIL'
                and a.Head1 in ("(PXIL Drawal)", "(PXIL Injection)")
              then 'PXIL'
              when a.drawl_type = 'IEX_PXIL'
                and a.Head1 in ("(IEX Drawal)", "(IEX Injection)")
              then 'IEX'
              else a.drawl_type END as drawl_type,
              a.block_no as x,
              sum(a.schedule) as y
              from panel_nrldc_state_drawl_schedule a,
                  (select date as maxdate, state as maxstate,
                        drawl_type, max(revision) as maxrev
                   from panel_nrldc_state_drawl_schedule
                   where date = STR_TO_DATE(%s, '%%d-%%m-%%Y')
                   and state = %s
                   group by date, state, drawl_type) b
              where a.date = b.maxdate
              and a.revision = b.maxrev
              and a.state = b.maxstate
              and a.drawl_type = b.drawl_type
              group by a.date, a.state,
              case when a.drawl_type = 'IEX_PXIL'
              and a.Head1 in ("(PXIL Drawal)", "(PXIL Injection)")
              then 'PXIL'
              when a.drawl_type = 'IEX_PXIL'
              and a.Head1 in ("(IEX Drawal)", "(IEX Injection)")
              then 'IEX'
              else a.drawl_type
              END, a.block_no""", data=(date, discom))
    results = datacursor.fetchall()
    db.close()
    return json.dumps(results, use_decimal=True)


@ems.route("/test")
def test():
    """
    When you request the root path, you'll get the index.html template.

    """
    #print "In Test"
    return flask.render_template("test.html")


@ems.route("/chart")
@login_required
def chart():
    """
    When you request the root path, you'll get the index.html template.

    """
    #print "In Chart"
    return flask.render_template("chart_combination.html")


@ems.route("/qsearch", methods=['GET'])
@login_required
def quick_search():
    """
    Quick Search Data Method
    """
    db = DB()
    # datacursor = con.cursor(cursors.DictCursor)
    datacursor = db.query_dictcursor("""select
        b.state_name, c.organisation_code
        from power.state_master b,
             power.organisation_master c
        where c.state_master_fk = b.state_master_pk
        and (c.organisation_master_pk = %s
        or c.organisation_parent_fk = %s)""" % (
        current_user.organisation_master_fk,
        current_user.organisation_master_fk))
    org = datacursor.fetchall()
    state = org[0].get('state_name')
    discom = org[0].get('organisation_code')
    # ToDo Fix the Discom like get_compare_data and sql injection problem
    current_app.logger.info("quick_search %s %s", state, discom)
    if request.values['term'][:2] == 'FD':
        datacursor = db.query_dictcursor("""SELECT distinct
            concat('FD_', discom, '_', model_name,
                   '_', DATE_FORMAT(date, '%%d-%%m-%%Y'))
            as Id
            FROM power.forecast_stg where
            discom = '%s' and state = '%s' and
            concat('FD_', discom, '_', model_name,
                   '_', DATE_FORMAT(date, '%%d-%%m-%%Y'))
            like '%%%s%%'
            order by date desc""" % (discom, state, request.values['term']))
    elif request.values['term'][:2] == 'AD':
        datacursor = db.query_dictcursor("""SELECT distinct
            concat('AD_', discom, '_', DATE_FORMAT(date, '%%d-%%m-%%Y'))
            as Id
            FROM power.drawl_staging where
            discom = '%s' and state = '%s' and
            concat('AD_', discom, '_', DATE_FORMAT(date, '%%d-%%m-%%Y'))
            like '%%%s%%'
            order by date desc""" % (discom, state, request.values['term']))
    # results = datacursor.fetchall()
    results = [row["Id"] for row in datacursor.fetchall()]
    db.close()
    # results = ['AD_06-03-2013','AD_07-03-2013','AD_08-03-2013']
    # print "QSEarch", results
    # return flask.render_template("test.html" , data=json.dumps(results))
    return json.dumps(results)


@ems.route("/get_compare_data/<data>")
@login_required
def get_compare_data(data):
    """
    Quick Serach Data Method
    """
    current_app.logger.info("****%s", data)  # request.values['data']
    db = DB()
    # datacursor = con.cursor(cursors.DictCursor)
    datacursor = db.query_dictcursor("""SELECT
        b.state_name, c.organisation_code
        from power.state_master b,
             power.organisation_master c
        where c.state_master_fk = b.state_master_pk
        and (c.organisation_master_pk = %s
        or c.organisation_parent_fk = %s)""" % (
        current_user.organisation_master_fk,
        current_user.organisation_master_fk))
    org = datacursor.fetchall()
    state = org[0].get('state_name')
    discom = org[0].get('organisation_code')
    # ToDo Implement this
    valid_discoms = get_org().get('org_name')
    placeholder = ", ".join(["%s"] * len(valid_discoms))
    if data[:3].upper() == 'FD_':
        sql = """SELECT
                DATE_FORMAT(a.date, '%%d-%%m-%%Y') as date,
                a.block_no as x,
                ifnull(a.demand_forecast,0) + ifnull(a.demand_bias,0) as y
                FROM power.forecast_stg a,
                (select date, state, discom,
                        model_name, max(revision) max_revision
                 from power.forecast_stg
                 where discom in ({0})
                 and state = %s
                 and concat('FD_', discom, '_', model_name, '_',
                            DATE_FORMAT(date, '%%d-%%m-%%Y')) = %s
                 group by date, state, discom, model_name) b
                where a.date = b.date
                and a.state = b.state
                and a.discom = b.discom
                and a.model_name = b.model_name
                and a.revision = b.max_revision""".format(placeholder)
        current_app.logger.debug('{} {}'.format(sql, (valid_discoms,
                                                      state)))
        current_app.logger.info("quick_search %s %s", state, discom)
        # Fix the data
        sqldata = []
        sqldata.extend(valid_discoms) if isinstance(valid_discoms, list) \
            else [].append(valid_discoms)
        sqldata.append(state)
        sqldata.append(data)
        datacursor = db.query_dictcursor(sql, data=tuple(sqldata))
    elif data[:3].upper() == 'AD_':
        sql = """SELECT date_format(a.date, '%%d-%%m-%%Y') as date,
            a.block_no as x, sum(a.constrained_load) +
            coalesce(sum(a.powercut),0) as y
            from (
                select date, block_no, constrained_load,
                null as powercut
                from power.drawl_staging
                where discom in ({0})
                and state = %s
                and concat('AD_',discom,
                           '_',DATE_FORMAT(date, '%%d-%%m-%%Y')) = %s
            union all
                select date, block_no, null as constrained_load,
                powercut
                from power.powercut_staging
                where discom in ({0})
                and state = %s
                and concat('AD_',discom,
                           '_',DATE_FORMAT(date, '%%d-%%m-%%Y')) = %s) a
            group by a.date, a.block_no""".format(placeholder)
        current_app.logger.debug('{} {}'.format(sql, (valid_discoms,
                                                      state)))
        current_app.logger.info("quick_search %s %s", state, discom)
        # Fix the data
        sqldata = []
        sqldata.extend(valid_discoms) if isinstance(valid_discoms, list) \
            else [].append(valid_discoms)
        sqldata.append(state)
        sqldata.append(data)
        datacursor = db.query_dictcursor(sql, data=tuple(sqldata * 2))
    results = datacursor.fetchall()
    db.close()
    # results = [row["Id"] for row in datacursor.fetchall()]
    # results = ['AD_06-03-2013','AD_07-03-2013','AD_08-03-2013']
    # print "QSEarch", results
    # return flask.render_template("test.html" , data=json.dumps(results))
    return json.dumps(results, use_decimal=True)


@ems.route("/fulldata/<data>")
@login_required
def get_fd_data(data):
    """
    Quick Serach Data Method
    """
    current_app.logger.info("%s", data)  # request.values['data']
    db = DB()
    # datacursor = con.cursor(cursors.DictCursor)
    if data[:3].upper() == 'FD_':
        datacursor = db.query_dictcursor("""SELECT
            concat(DATE_FORMAT(date, '%%d-%%m-%%Y')) as date,
            block_no as x, demand_forecast as y
            FROM power.forecast_stg where
            concat('FD_',DATE_FORMAT(date, '%%d-%%m-%%Y'))
            = '%s'""" % (data))
    elif data[:3].upper() == 'AD_':
        datacursor = db.query_dictcursor("""SELECT
            DATE_FORMAT(date, '%%d-%%m-%%Y') as date,
            block_no_fk as x, actual_drawl as y
            FROM power.drawl_data where
            concat('AD_',DATE_FORMAT(date, '%%d-%%m-%%Y'))
            = '%s'""" % (data))
    results = datacursor.fetchall()
    db.close()
    # results = [row["Id"] for row in datacursor.fetchall()]
    # results = ['AD_06-03-2013','AD_07-03-2013','AD_08-03-2013']
    # print "QSEarch", results
    # return flask.render_template("test.html" , data=json.dumps(results))
    return json.dumps(results, use_decimal=True)


@ems.route("/report")
@login_required
def report():
    """
    When you request the root path, you'll get the index.html template.
    """
    current_app.logger.info("report")
    # return flask.render_template("bseb_mape_chart.html")
    return flask.render_template("report_jinja.html")


@ems.route("/bseb_mape_search", methods=['GET'])
@login_required
def bseb_mape_search():
    """
    Quick Serach Data Method
    """
    db = DB()
    # datacursor = con.cursor(cursors.DictCursor)
    current_app.logger.info(request.values['term'][:2])
    if request.values['term'][:2] == 'FD':
        datacursor = db.query_dictcursor("""SELECT distinct
            concat('FD_',DATE_FORMAT(date, '%%d-%%m-%%Y'))
            as Id
            FROM power.bseb_forecast_demo where
            concat('FD_',DATE_FORMAT(date, '%%d-%%m-%%Y'))
            like '%%%s%%' order by date desc""" % (request.values['term']))
    elif request.values['term'][:2] == 'AD':
        datacursor = db.query_dictcursor("""SELECT distinct
            concat('AD_',DATE_FORMAT(date, '%%d-%%m-%%Y'))
            as Id
            FROM power.drawl_staging  where
            concat('AD_',DATE_FORMAT(date, '%%d-%%m-%%Y'))
            like '%%%s%%' order by date DESC""" % (request.values['term']))
    # datacursor.execute("""(SELECT distinct\
    #                    concat('FD_',DATE_FORMAT(date, '%%d-%%m-%%Y'))\
    #                    as Id\
    #                    FROM power.bseb_forecast_demo where\
    #                    concat('FD_',DATE_FORMAT(date, '%%d-%%m-%%Y'))\
    #                    like '%%%s%%' order by date DESC)
    #                    union all\
    #                    (SELECT distinct\
    #                    concat('AD_',DATE_FORMAT(date, '%%d-%%m-%%Y'))\
    #                    as Id\
    #                    FROM power.bseb_scada_agg where\
    #                    concat('AD_',DATE_FORMAT(date, '%%d-%%m-%%Y'))\
    #                    like '%%%s%%' order by date DESC)"""
    #                    % (request.values['term'], request.values['term'])
    #                    )
    # results = datacursor.fetchall()
    results = [row["Id"] for row in datacursor.fetchall()]
    db.close()
    # results = ['AD_06-03-2013','AD_07-03-2013','AD_08-03-2013']
    # print results
    # return flask.render_template("test.html" , data=json.dumps(results))
    return json.dumps(results)


@ems.route("/get_bsebmape_data/<data>")
@login_required
def get_bsebmape_data(data):
    """
    Quick Serach Data Method
    """
    db = DB()
    current_app.logger.info("****%s", data)  # request.values['data']
    # datacursor = con.cursor(cursors.DictCursor)
    if data[:3].upper() == 'FD_':
        datacursor = db.query_dictcursor("""SELECT
            DATE_FORMAT(date, '%%d-%%m-%%Y') as date,
            block_no as x, BPDCL as y
            FROM power.bseb_forecast_demo where
            concat('FD_',DATE_FORMAT(date, '%%d-%%m-%%Y'))
            = '%s'""" % (data))
    elif data[:3].upper() == 'AD_':
        # datacursor.execute("""SELECT DATE_FORMAT(date, '%%d-%%m-%%Y') as date, \
        #                    block_no as x, round(total_drawl,2) as y\
        #                    FROM power.bseb_scada_agg where\
        #                    concat('AD_',DATE_FORMAT(date, '%%d-%%m-%%Y'))\
        #                    = '%s'""" % (data)
        #                    )
        datacursor = db.query_dictcursor("""SELECT
            date_format(a.date,'%%d-%%m-%%Y') as date, a.block_no as x,
            round(a.constrained_load + coalesce(b.powercut,0)) as y
            from (select * from power.drawl_staging
            where state = 'BIHAR'
            and discom = 'BPDCL'
            and concat('AD_',DATE_FORMAT(date, '%%d-%%m-%%Y')) = '%s')a
            left join
            (select * from power.powercut_staging where state = 'BIHAR'
            and discom = 'BPDCL'
            and concat('AD_',DATE_FORMAT(date, '%%d-%%m-%%Y')) = '%s') b
            on(a.date = b.date
               and a.block_no = b.block_no)
            order by a.date, a.block_no """ % (data, data))
    results = datacursor.fetchall()
    db.close()
    # results = [row["Id"] for row in datacursor.fetchall()]
    # results = ['AD_06-03-2013','AD_07-03-2013','AD_08-03-2013']
    # print "QSEarch", results
    # return flask.render_template("test.html" , data=json.dumps(results))
    return json.dumps(results, use_decimal=True)


@ems.route("/chart3")
@login_required
def chart3():
    """
    When you request the root path, you'll get the index.html template.

    """
    current_app.logger.info("In Chart3")
    return flask.render_template("stackedAreaChart.html")


@ems.route("/isgssearch", methods=['GET'])
@login_required
def isgs_search():
    """
    Quick Serach Data Method
    """
    current_app.logger.info('*****%s', request.values['term'])
    db = DB()
    # datacursor = con.cursor(cursors.DictCursor)
    datacursor = db.query_dictcursor("""SELECT
        distinct DATE_FORMAT(date, '%%d-%%m-%%Y') as date
        FROM power.nrldc_state_drawl_summary_demoz2  where
        DATE_FORMAT(date, '%%d-%%m-%%Y') like '%%%s%%'
        order by date""" % (request.values['term']))
    # results = datacursor.fetchall()
    results = [row["date"] for row in datacursor.fetchall()]
    db.close()
    # results = ['AD_06-03-2013','AD_07-03-2013','AD_08-03-2013']
    # print "QSEarch", results
    # return flask.render_template("test.html" , data=json.dumps(results))
    return json.dumps(results)


@ems.route("/nrldcdata/<state>/<date>")
@login_required
def get_nr_data(state, date):
    """
    Get NRLDC data
    """
    current_app.logger.info('%s %s', state, date)
    db = DB()
    # datacursor = con.cursor(cursors.DictCursor)
    datacursor = db.query_dictcursor("""SELECT
        DATE_FORMAT(date, '%%d-%%m-%%Y') as date,
        drawl_type, block_no as x, sum(schedule) as y
        FROM power.panel_nrldc_state_drawl_sch_rev0  where
        DATE_FORMAT(date, '%%d-%%m-%%Y') = '%s' and state = '%s'
        group by date, drawl_type,
        block_no""" % (date, state))
    results = datacursor.fetchall()
    db.close()
    # results = [row["Id"] for row in datacursor.fetchall()]
    # results = ['AD_06-03-2013','AD_07-03-2013','AD_08-03-2013']
    # print "QSEarch", results
    # return flask.render_template("test.html" , data=json.dumps(results))
    return json.dumps(results, use_decimal=True)


@ems.route("/bseb_tenisgs_stacked")
@login_required
def bseb_tenisgs_stacked():
    """
    When you request the root path, you'll get the index.html template.

    """
    current_app.logger.info("In bseb_tenisgs_stacked")
    return flask.render_template("bseb_tenisgs_stacked.html")


@ems.route("/bseb_tenisgs_search", methods=['GET'])
@login_required
def bseb_tenisgs_search():
    """
    Quick Serach Data Method
    """
    current_app.logger.info('***** %s', request.values['term'])
    db = DB()
    # datacursor = con.cursor(cursors.DictCursor)
    datacursor = db.query_dictcursor("""SELECT
        distinct DATE_FORMAT(date, '%%d-%%m-%%Y')
        as date
        FROM a where
        DATE_FORMAT(date, '%%d-%%m-%%Y') like '%%%s%%'
        and discom = 'BSEB' and revision = 0
        order by date""" % (request.values['term']))
    # results = datacursor.fetchall()
    results = [row["date"] for row in datacursor.fetchall()]
    db.close()
    # results = ['AD_06-03-2013','AD_07-03-2013','AD_08-03-2013']
    # print "QSEarch", results
    # return flask.render_template("test.html" , data=json.dumps(results))
    return json.dumps(results)


@ems.route("/get_tenisgsgengraph_data/<date>")
@login_required
def get_tenisgsgengraph_data(date):
    """
    Get NRLDC data
    """
    current_app.logger.info("Inside get_tenisgsgengraph_data")
    db = DB()
    # datacursor = con.cursor(cursors.DictCursor)
    datacursor = db.query_dictcursor("""select
        ldc_name, ldc_org_name, organisation_code,
        d.zone_code
        from power.org_isgs_map a,
             power.organisation_master b,
             power.state_master c,
             power.zone_master d
        where a.organisation_master_fk = b.organisation_master_pk
        and c.state_master_pk = b.state_master_fk
        and c.exchange_zone_master_fk = d.zone_master_pk
        and (b.organisation_master_pk = %s
            or b.organisation_parent_fk = %s)""", data=(
        current_user.organisation_master_fk,
        current_user.organisation_master_fk))
    org = datacursor.fetchall()

    ldc = org[0].get('ldc_name')
    # ldc_discom = org[0].get('ldc_org_name')
    discom = org[0].get('organisation_code')

    current_app.logger.info("get_tenisgsgengraph_data %s %s %s",
                            ldc, discom, date)

    # if ldc == 'ERLDC' and discom == 'BSEB':
    #     datacursor = db.query_dictcursor("""SELECT
    #         DATE_FORMAT(adddate(a.date, 1), '%%d-%%m-%%Y')
    #         as date, a.drawl_type, a.block_no as x
    #         ,sum(case when drawl_type = 'ISGS'
    #                   and station_name <> 'NET|DRAWAL|SCHD.' then schedule
    #                   when drawl_type = 'BILATERAL'
    #                   and station_name = 'BILAT|TOTAL' then schedule
    #                   when drawl_type = 'LTOA_MTOA' then schedule
    #                   when drawl_type = 'IEX'
    #                     and station_name = 'IEX|TOT.' then schedule
    #                   when drawl_type = 'PXIL'
    #                     and station_name = 'PXI|TOT.' then schedule end) as y
    #         FROM erldc_state_drawl_schedule_stg a,
    #              (select date, discom, min(revision) as revision
    #              from erldc_state_drawl_schedule_stg
    #              where date = adddate(str_to_date(%s, '%%d-%%m-%%Y'), -1)
    #              and discom = %s group by date, discom) b
    #         where a.date = b.date
    #         and a.discom = b.discom
    #         and a.revision = b.revision
    #         and a.drawl_type in
    #             ('ISGS', 'BILATERAL', 'LTOA_MTOA', 'IEX', 'PXIL')
    #         group by a.date, a.block_no, a.revision, a.discom, a.drawl_type
    #         order by a.date, a.drawl_type, a.block_no""", data=(date, discom))
    # elif ldc == 'NRLDC':
    #     datacursor = db.query_dictcursor("""SELECT
    #         DATE_FORMAT(adddate(a.date, 1), '%%d-%%m-%%Y') as Date,
    #         CASE
    #         WHEN a.Drawl_Type = 'IEX_PXIL'
    #              AND a.Head1 in ("(PXIL Drawal)", "(PXIL Injection)")
    #              THEN 'PXIL'
    #         WHEN a.Drawl_Type = 'IEX_PXIL'
    #              AND a.Head1 in ("(IEX Drawal)", "(IEX Injection)")
    #              THEN 'IEX'
    #         ELSE
    #             a.Drawl_Type
    #         END as drawl_type,
    #         a.Block_No AS x,
    #         SUM(a.SChedule) as y
    #         from power.panel_nrldc_state_drawl_schedule a,
    #              (select date as mindate, state as minstate,
    #              min(Revision) as minrev
    #              from
    #              power.panel_nrldc_state_drawl_schedule
    #              where date =
    #              adddate(str_to_date(%s, '%%d-%%m-%%Y'), -1)
    #              and state = %s
    #              and revision > -1
    #              group by date, state) b
    #         where a.date = b.mindate
    #         and a.revision = b.minrev
    #         and a.state = b.minstate
    #         group by a.Date, a.Revision, a.State,
    #         CASE
    #         WHEN a.Drawl_Type = 'IEX_PXIL'
    #              AND a.Head1 in ("(PXIL Drawal)", "(PXIL Injection)")
    #              THEN 'IEX'
    #         WHEN a.Drawl_Type = 'IEX_PXIL'
    #              AND a.Head1 in ("(PXIL Drawal)", "(PXIL Injection)")
    #              THEN 'PXIL'
    #         ELSE
    #             a.Drawl_Type
    #         END,
    #         a.Block_No""", data=(date, discom))
    if ldc == 'NRLDC':
        # datacursor = db.query_dictcursor("""SELECT
        #     DATE_FORMAT(date, '%%d-%%m-%%Y') as Date,
        #     generation_entity_name as drawl_type, block_no as x,
        #     round(sum(tentative_generation), 2) as y
        #     from(
        #             (select a.date, a.block_no, a.discom,
        #              pool_name generation_entity_name,
        #              sum(case when pool_name = 'ISGS' then
        #                     a.schedule * ((100 - x.loss_perc)/100) else
        #                     a.schedule end) tentative_generation
        #             from isgstentative_schedule_staging a,
        #                  (select date, discom, max(revision) max_revision
        #                   from isgstentative_schedule_staging
        #                   where date = str_to_date(%s, '%%d-%%m-%%Y')
        #                   and discom = %s
        #                   and pool_name not in ('OPT_SCHEDULE')
        #                   group by date, discom) b  left join
        #                 (select case when count(loss_perc)=0 then 0
        #                              else loss_perc end  loss_perc
        #                 from power.nrldc_est_trans_loss_stg
        #                 where start_date <= str_to_date(%s, '%%d-%%m-%%Y')
        #                 and end_date >= str_to_date(%s, '%%d-%%m-%%Y')) x
        #                 on (1 = 1)
        #             where a.date = b.date
        #             and a.discom = b.discom
        #             and a.revision = b.max_revision
        #             and a.pool_name not in ('OPT_SCHEDULE')
        #             group by a.date, a.block_no, a.discom, a.pool_name)
        #             union all
        #             (SELECT date, block_no, discom,
        #              case when upper(generation_entity_name) = 'OPENACCESS'
        #              then 'OPENACCESS' else 'INTERNAL' end
        #              generation_entity_name,
        #              sum(tentative_generation) tentative_generation
        #             from tentative_schedule_staging
        #             where date = str_to_date(%s, '%%d-%%m-%%Y')
        #             and discom = %s
        #             and upper(generation_entity_name)
        #             not in ('ISGS', 'BILATERAL', 'MTOA',
        #                     'SHARED', 'BANKING', 'STOA', 'LTOA')
        #             group by date, block_no, discom,
        #             case when upper(generation_entity_name) = 'OPENACCESS'
        #             then 'OPENACCESS' else 'INTERNAL' end)
        #     ) z
        #     group by date, block_no, discom, generation_entity_name
        #     order by date, discom, generation_entity_name, block_no""", data=(
        #     date, discom, date, date, date, discom))
        datacursor = db.query_dictcursor("""SELECT
            DATE_FORMAT(date, '%%d-%%m-%%Y') as Date,
            generation_entity_name as drawl_type, block_no as x,
            coalesce(round(sum(tentative_generation), 2),0) as y
            from(
                    (select a.date, a.block_no, a.discom,
                     case when a.pool_name = 'INT_GENERATION_ACT'
                     then 'INTERNAL' else a.pool_name end
                      generation_entity_name,
                     sum(a.schedule) tentative_generation
                    from power.isgstentative_schedule_staging a,
                         (select date, discom, pool_name,
                          max(revision) max_revision
                          from power.isgstentative_schedule_staging
                          where date = str_to_date(%s, '%%d-%%m-%%Y')
                          and discom = %s
                          and pool_name not in ('OPT_SCHEDULE')
                          group by date, discom, pool_name) b
                    where a.date = b.date
                    and a.discom = b.discom
                    and a.revision = b.max_revision
                    and a.pool_name = b.pool_name
                    and a.pool_name not in ('OPT_SCHEDULE')
                    group by a.date, a.block_no, a.discom, a.pool_name)
                    union all
                    (SELECT date, block_no, discom,
                     case when upper(generation_entity_name) = 'OPENACCESS'
                     then 'OPENACCESS' else 'OTHERS' end
                     generation_entity_name,
                     sum(tentative_generation) tentative_generation
                    from power.tentative_schedule_staging
                    where date = str_to_date(%s, '%%d-%%m-%%Y')
                    and discom = %s
                    and upper(generation_entity_name)
                    not in ('ISGS', 'BILATERAL', 'MTOA',
                            'SHARED', 'BANKING', 'STOA', 'LTOA')
                    group by date, block_no, discom,
                    case when upper(generation_entity_name) = 'OPENACCESS'
                    then 'OPENACCESS' else 'OTHERS' end)
            ) z
            group by date, block_no, discom, generation_entity_name
            order by date, discom, generation_entity_name, block_no
            """, data=(date, discom, date, discom))            
    elif ldc == 'WRLDC':
        datacursor = db.query_dictcursor("""SELECT
            DATE_FORMAT(date, '%%d-%%m-%%Y') as Date,
            generation_entity_name as drawl_type, block_no as x,
            coalesce(round(sum(tentative_generation), 2),0) as y
            from(
                    (select a.date, a.block_no, a.discom,
                     case when a.pool_name = 'INT_GENERATION_ACT'
                     then 'INTERNAL' else a.pool_name end
                      generation_entity_name,
                     sum(a.schedule) tentative_generation
                    from power.isgstentative_schedule_staging a,
                         (select date, discom, pool_name,
                          max(revision) max_revision
                          from power.isgstentative_schedule_staging
                          where date = str_to_date(%s, '%%d-%%m-%%Y')
                          and discom = %s
                          and pool_name not in ('OPT_SCHEDULE')
                          group by date, discom, pool_name) b
                    where a.date = b.date
                    and a.discom = b.discom
                    and a.revision = b.max_revision
                    and a.pool_name = b.pool_name
                    and a.pool_name not in ('OPT_SCHEDULE')
                    group by a.date, a.block_no, a.discom, a.pool_name)
                    union all
                    (SELECT date, block_no, discom,
                     case when upper(generation_entity_name) = 'OPENACCESS'
                     then 'OPENACCESS' else 'OTHERS' end
                     generation_entity_name,
                     sum(tentative_generation) tentative_generation
                    from power.tentative_schedule_staging
                    where date = str_to_date(%s, '%%d-%%m-%%Y')
                    and discom = %s
                    and upper(generation_entity_name)
                    not in ('ISGS', 'BILATERAL', 'MTOA',
                            'SHARED', 'BANKING', 'STOA', 'LTOA')
                    group by date, block_no, discom,
                    case when upper(generation_entity_name) = 'OPENACCESS'
                    then 'OPENACCESS' else 'OTHERS' end)
            ) z
            group by date, block_no, discom, generation_entity_name
            order by date, discom, generation_entity_name, block_no
            """, data=(date, discom, date, discom))
    results = datacursor.fetchall()
    db.close()
    current_app.logger.debug("get_tenisgsgengraph_data %s", results)
    return json.dumps(results, use_decimal=True)


# @ems.route("/get_biharlive_data")
# def get_biharlive_data():
@ems.route("/get_biharlive_data/<date>")
@login_required
def get_biharlive_data(date):
    """
    When you request the root path, you'll get the index.html template.

    """
    db = DB()
    # datacursor = con.cursor(cursors.DictCursor)
    datacursor = db.query_dictcursor("""SELECT
        DATE_FORMAT(a.date, '%%d-%%m-%%Y') as Date,
        a.block_no, a.kbunl_projected as int_gen,
        a.net_availibility as ER_gen,
        a.forecast_NBPDCL, a.forecast_SBPDCL,
        a.forecast_BPDCL as forecast,
        a.availibility, a.position_gap, a.live_demand_NBPDCL,
        a.live_forecast_NBPDCL, a.live_demand_SBPDCL,
        a.live_forecast_SBPDCL,
        a.live_demand_BPDCL as live_demand,
        a.live_forecast_BPDCL as live_forecast,
        a.deficit_surplus_BPDCL,
        a.revised_position_BPDCL, a.schedule, a.odud
        from power.bseb_real_time_forecast a,
        (select date, max(revison) max_revison from
        power.bseb_real_time_forecast
        where date = STR_TO_DATE('%s','%%d-%%m-%%Y')) b
        where a.date = b.date
        and a.revison = b.max_revison
        order by a.date, a.block_no""" % date)

    results = datacursor.fetchall()
    # results = [row["Id"] for row in datacursor.fetchall()]
    # results = ['AD_06-03-2013','AD_07-03-2013','AD_08-03-2013']
    current_app.logger.debug("biharlive %s", results)
    db.close()
    # return flask.render_template("test.html" , data=json.dumps(results))
    return json.dumps(results, use_decimal=True)


# @celery.task(bind=True)
# def long_task(self):
#     """Background task that runs a long function with progress reports."""
#     verb = ['Starting up', 'Booting', 'Repairing', 'Loading', 'Checking']
#     adjective = ['master', 'radiant', 'silent', 'harmonic', 'fast']
#     noun = ['solar array', 'particle reshaper', 'cosmic ray', 'orbiter', 'bit']
#     message = ''
#     total = random.randint(10, 50)
#     print total
#     for i in range(total):
#         if not message or random.random() < 0.25:
#             message = '{0} {1} {2}...'.format(random.choice(verb),
#                                               random.choice(adjective),
#                                               random.choice(noun))
#         self.update_state(state='PROGRESS',
#                           meta={'current': i, 'total': total,
#                                 'status': message})
#         time.sleep(1)
#     return {'current': 100, 'total': 100, 'status': 'Task completed!',
#             'result': 42}

# @ems.route('/longtask', methods=['POST'])
# def longtask():
#     task = long_task.apply_async()
#     return jsonify({}), 202, {'Location': url_for('taskstatus', task_id=task.id)}


@ems.route('/status/<job_nm>/<task_id>')
@login_required
def taskstatus(job_nm, task_id):
    jobdict = {'erldc_crawler_tsk': erldc_crawler_tsk,
               'nrldc_crawler_tsk': nrldc_crawler_tsk,
               'wrldc_crawler_tsk': wrldc_crawler_tsk,
               'bseb_weatherupload_tsk': bseb_weatherupload_tsk,
               'upcl_weatherupload_tsk': upcl_weatherupload_tsk,
               'guvnl_weatherupload_tsk': guvnl_weatherupload_tsk,
               'adani_weatherupload_tsk': adani_weatherupload_tsk,
               'exchange_crawler_tsk': exchange_crawler_tsk,
               'bseb_dataupload_tsk': bseb_dataupload_tsk,
               'data_upload_tsk': data_upload_tsk,
               'forecast_tsk': forecast_tsk,
               'tentativedata_upload_tsk': tentativedata_upload_tsk,
               'trade_tsk': trade_tsk,
               'alloc_opt_tsk': alloc_opt_tsk,
               'realtime_forecast_upd_tsk': realtime_forecast_upd_tsk,
               'int_declaredcapacity_tsk': int_declaredcapacity_tsk,
               'upcl_intsch_crawler_tsk': upcl_intsch_crawler_tsk}
    task = jobdict[job_nm].AsyncResult(task_id)
    if task.state == 'PENDING':
        # // job did not start yet
        response = {
            'state': task.state,
            'current': 0,
            'total': 1,
            'status': 'Pending...'
        }
    elif task.state != 'FAILURE':
        response = {
            'state': task.state,
            'current': task.info.get('current', 0),
            'total': task.info.get('total', 1),
            'status': task.info.get('status', '')
        }
        if 'result' in task.info:
            response['result'] = task.info['result']
    else:
        # something went wrong in the background job
        response = {
            'state': task.state,
            'current': 1,
            'total': 1,
            'status': str(task.info),  # this is the exception raised
        }
    return jsonify(response)


# @ems.route('/celerytest', methods=['GET', 'POST'])
# def celerytest():
#     if request.method == 'GET':
#         return render_template('celerytest.html', email=session.get('email', ''))
#     email = request.form['email']
#     session['email'] = email

#     # send the email
#     msg = Message('Hello from Flask',
#                   recipients=[request.form['email']])
#     msg.body = 'This is a test email sent from a background Celery task.'
#     if request.form['submit'] == 'Send':
#         # send right away
#         send_async_email.delay(msg)
#         flash('Sending email to {0}'.format(email))
#     else:
#         # send in one minute
#         send_async_email.apply_async(args=[msg], countdown=60)
#         flash('An email will be sent to {0} in one minute'.format(email))

#     return redirect(url_for('celerytest'))


# @celery.task(bind=True)
# def erldc_crawler_tsk(self):
#     import erldc_crawler
#     message = ''
#     self.update_state(state='PROGRESS',
#                   meta={'current': 0, 'total': 3,
#                         'status': message})
#     print "Here*****"
#     self.update_state(state='PROGRESS',
#                   meta={'current': 1, 'total': 3,
#                         'status': message})
#     date = datetime.datetime.now()
#     # erldc_sch = erldc_crawler.ErldcSchedule(date, "http://www.erldc.org", homedir + "/Projects/batch/Data/")
#     erldc_sch = erldc_crawler.ErldcSchedule(date, "http://103.7.131.195", homedir + "/Projects/batch/Data/")
#     print "Here"
#     self.update_state(state='PROGRESS',
#                   meta={'current': 2, 'total': 3,
#                         'status': message})
#     erldc_sch.get_file(homedir + '/Projects/batch/config/sqldb_connection_config.txt')
#     # self.update_state(state='PROGRESS',
#     #               meta={'current': 3, 'total': 3,
#     #                     'status': message})
#     return {'current': 100, 'total': 100, 'status': 'Task completed!',
#             'result': 'Done'}

@celery.task(bind=True)
def erldc_crawler_tsk(self, dsn):
    import ems.batch.bin.erldc_crawler as erldc_crawler

    ldir = "./ems/batch/data/erldc/"
    # dsn = "./ems/batch/config/sqldb_gcloud.txt"
    message = ''
    self.update_state(state='PROGRESS',
                      meta={'current': 0, 'total': 3,
                            'status': message})
    logger.debug("erldc_crawler_tsk started")
    self.update_state(state='PROGRESS',
                      meta={'current': 1, 'total': 3,
                            'status': message})
    # date = datetime.datetime.now()
    if time.tzname[0] == 'IST':
        date = datetime.today()
    else:
        dest_tz = pytz.timezone('Asia/Kolkata')
        ts = time.time()
        utc_now = datetime.utcfromtimestamp(ts)
        date = utc_now.replace(tzinfo=pytz.utc).astimezone(dest_tz)

    erldc_sch = erldc_crawler.ErldcSchedule(date, "http://www.erldc.org", ldir)
    # erldc_sch = erldc_crawler.ErldcSchedule(date, "http://103.7.131.195", homedir + "/Projects/batch/Data/")
    logger.info('Finished running crawler'
                'from erldc_crawler_tsk fn')
    self.update_state(state='PROGRESS',
                      meta={'current': 2, 'total': 3,
                            'status': message})
    erldc_sch.get_file(dsn)
    # self.update_state(state='PROGRESS',
    #               meta={'current': 3, 'total': 3,
    #                     'status': message})
    return {'current': 100, 'total': 100, 'status': 'Task completed!',
            'result': 'Done'}


@ems.route('/erldc_crawler_task', methods=['POST'])
@login_required
def erldc_crawler_task():
    conn_file = current_app.config['DB_CONNECT_FILE']
    task = erldc_crawler_tsk.apply_async((conn_file, ))
    return jsonify({}), 202, {'Location': url_for('ems.taskstatus',
                              job_nm="erldc_crawler_tsk", task_id=task.id)}


# @celery.task(bind=True)
# def nrldc_crawler_tsk(self, org_schedule, dsn):
#     import ems.batch.bin.nrldc_crawler_v4 as nrldc_crawler_v4
#     import argparse
#     # import datetime

#     ldir = "./ems/batch/data/nrldc/"
#     # dsn = "./ems/batch/config/sqldb_gcloud.txt"
#     message = ''

#     if time.tzname[0] == 'IST':
#         local_now = datetime.today()
#     else:
#         dest_tz = pytz.timezone('Asia/Kolkata')
#         ts = time.time()
#         utc_now = datetime.utcfromtimestamp(ts)
#         local_now = utc_now.replace(tzinfo=pytz.utc).astimezone(dest_tz)

#     startdate = local_now + timedelta(-1)
#     enddate = local_now + timedelta(1)
#     urls = {'NRLDC_ISGS': ['http://wbs.nrldc.in/WBS/DrwlSch.aspx',
#                            'nrldc_state_drawl_schedule_stg'],
#             'NRLDC_LTA': ['http://wbs.nrldc.in/WBS/OATrans.aspx',
#                           'nrldc_state_drawl_schedule_stg'],
#             'NRLDC_IEX_PXIL': ['http://wbs.nrldc.in/WBS/PXData.aspx',
#                                'nrldc_state_drawl_schedule_stg'],
#             'NRLDC_MTOA': ['http://wbs.nrldc.in/WBS/OATrans.aspx',
#                            'nrldc_state_drawl_schedule_stg'],
#             'NRLDC_Shared': ['http://wbs.nrldc.in/WBS/OATrans.aspx',
#                              'nrldc_state_drawl_schedule_stg'],
#             'NRLDC_Bilateral': ['http://wbs.nrldc.in/WBS/OATrans.aspx',
#                                 'nrldc_state_drawl_schedule_stg'],
#             'NRLDC_Entitlement': ['http://wbs.nrldc.in/WBS/finalentt.aspx',
#                                   'nrldc_entitlements_stg'],
#             'NRLDC_Schedule_Est_Loss': [('http://nrldc.in/Websitedata/'
#                                          'Commercial/SemData/NRSchloss.htm'),
#                                         'nrldc_est_trans_loss_stg']}
#     self.update_state(state='PROGRESS',
#                       meta={'current': 0, 'total': len(urls),
#                             'status': message})
#     logger.debug("nrldc_crawler_tsk started")
#     i = 0
#     for key, value in urls.iteritems():
#         args = argparse.Namespace(alerts=False,
#                                   url=value[0],
#                                   state=org_schedule,
#                                   dir=ldir, filenm=key,
#                                   start_date=startdate.strftime("%d-%m-%Y"),
#                                   end_date=enddate.strftime("%d-%m-%Y"),
#                                   tabname=value[1],
#                                   dsn=dsn, revflag=True)
#         nrldc_crawler_v4.main(args)
#         i += 1
#         self.update_state(state='PROGRESS',
#                           meta={'current': i, 'total': len(urls),
#                                 'status': message})
#     return {'current': 100, 'total': 100, 'status': 'Task completed!',
#             'result': 'Done'}

@celery.task(bind=True)
def nrldc_crawler_tsk(self, org_schedule, dsn, discom):
    import ems.batch.bin.nrldc_crawler as nrldc_crawler
    import argparse
    # import datetime

    # ldir = "./ems/batch/data/nrldc/"
    # dsn = "./ems/batch/config/sqldb_gcloud.txt"
    # message = ''
    if time.tzname[0] == 'IST':
        local_now = datetime.today()
    else:
        dest_tz = pytz.timezone('Asia/Kolkata')
        ts = time.time()
        utc_now = datetime.utcfromtimestamp(ts)
        local_now = utc_now.replace(tzinfo=pytz.utc).astimezone(dest_tz)

    startdate = local_now + timedelta(-1)
    enddate = local_now + timedelta(1)

    crawlerdict = {'UTTARAKHAND': {'StateSchedule': 'UTTARAKHAND_STATE',
                                   'Entitlement': 'UTTARAKHAND'}}
    crawlertype = crawlerdict.get(org_schedule)
    len_crawlertype = len(crawlertype)
    logger.debug("nrldc_crawler_tsk started")
    i = 0
    for key, value in crawlertype.items():
        self.update_state(state='PROGRESS',
                          meta={'current': i,
                                'total': len_crawlertype,
                                'status': key + ' Started...'})
        args = argparse.Namespace(alerts=False,
                                  state=org_schedule,
                                  nrldcbuyer=value,
                                  start_date=startdate.strftime("%d-%m-%Y"),
                                  end_date=enddate.strftime("%d-%m-%Y"),
                                  dns=dsn,
                                  ftype=key,
                                  discom=discom)
        nrldc_crawler.main(args)
        self.update_state(state='PROGRESS',
                          meta={'current': i,
                                'total': len_crawlertype,
                                'status': key + ' Finished...'})
        i += 1
    return {'current': 100, 'total': 100, 'status': 'Task completed!',
            'result': 'Done'}


@celery.task(bind=True)
def wrldc_crawler_tsk(self, org_schedule, dsn):
    import ems.batch.bin.wrldc_crawler_latestv2 as wrldc_crawler_latestv2
    import argparse
    # import datetime

    # ldir = "./ems/batch/data/nrldc/"
    # dsn = "./ems/batch/config/sqldb_gcloud.txt"
    # message = ''
    if time.tzname[0] == 'IST':
        local_now = datetime.today()
    else:
        dest_tz = pytz.timezone('Asia/Kolkata')
        ts = time.time()
        utc_now = datetime.utcfromtimestamp(ts)
        local_now = utc_now.replace(tzinfo=pytz.utc).astimezone(dest_tz)

    startdate = local_now + timedelta(-1)
    enddate = local_now + timedelta(1)

    crawlerdict = {'GUJARAT': {'StateSchedule': 'GEB_State',
                               'Entitlement': 'GEB_Beneficiary'}}
    crawlertype = crawlerdict.get(org_schedule)
    len_crawlertype = len(crawlertype)
    logger.debug("wrldc_crawler_tsk started")
    i = 0
    for key, value in crawlertype.iteritems():
        self.update_state(state='PROGRESS',
                          meta={'current': i,
                                'total': len_crawlertype,
                                'status': key + ' Started...'})
        args = argparse.Namespace(alerts=False,
                                  state=org_schedule,
                                  wrldcbuyer=value,
                                  start_date=startdate.strftime("%d-%m-%Y"),
                                  end_date=enddate.strftime("%d-%m-%Y"),
                                  dns=dsn,
                                  ftype=key)
        wrldc_crawler_latestv2.main(args)
        self.update_state(state='PROGRESS',
                          meta={'current': i,
                                'total': len_crawlertype,
                                'status': key + ' Finished...'})
        i += 1
    return {'current': 100, 'total': 100, 'status': 'Task completed!',
            'result': 'Done'}


@ems.route('/isgs_crawler_task', methods=['POST'])
@login_required
def isgs_crawler_task():
    db = DB()
    # datacursor = con.cursor(cursors.DictCursor)
    datacursor = db.query_dictcursor("""select
    ldc_name, ldc_org_name, b.organisation_code
    from power.org_isgs_map a,
    power.organisation_master b
    where a.organisation_master_fk = b.organisation_master_pk
    and (b.organisation_master_pk = %s
    or b.organisation_parent_fk = %s)""" % (
        current_user.organisation_master_fk,
        current_user.organisation_master_fk))

    results = datacursor.fetchall()
    db.close()
    current_app.logger.info("isgs_crawler_task %s", results)
    conn_file = current_app.config['DB_CONNECT_FILE']
    if results[0].get('ldc_name') == 'ERLDC':
        task = erldc_crawler_tsk.apply_async((conn_file, ))
        return jsonify({}), 202, {'Location': url_for('ems.taskstatus',
                                  job_nm="erldc_crawler_tsk", task_id=task.id)}
    elif results[0].get('ldc_name') == 'NRLDC':
        task = nrldc_crawler_tsk.apply_async((results[0].get('ldc_org_name'),
                                              conn_file, results[0].get('organisation_code')))
        return jsonify({}), 202, {'Location': url_for('ems.taskstatus',
                                  job_nm="nrldc_crawler_tsk", task_id=task.id)}
    elif results[0].get('ldc_name') == 'WRLDC':
        task = wrldc_crawler_tsk.apply_async((results[0].get('ldc_org_name'),
                                              conn_file))
        return jsonify({}), 202, {'Location': url_for('ems.taskstatus',
                                  job_nm="wrldc_crawler_tsk", task_id=task.id)}

@celery.task(bind=True)
def upcl_intsch_crawler_tsk(self, dsn, discom, state):
    import ems.batch.bin.upcl_genschedule_crawler as upcl_genschedule_crawler
    import argparse
    # import datetime

    # ldir = "./ems/batch/data/nrldc/"
    # dsn = "./ems/batch/config/sqldb_gcloud.txt"
    # message = ''
    if time.tzname[0] == 'IST':
        local_now = datetime.today()
    else:
        dest_tz = pytz.timezone('Asia/Kolkata')
        ts = time.time()
        utc_now = datetime.utcfromtimestamp(ts)
        local_now = utc_now.replace(tzinfo=pytz.utc).astimezone(dest_tz)

    startdate = local_now + timedelta(-1)
    enddate = local_now + timedelta(1)

    crawlerdict = {'UPCL': {'StateSchedule': '',
                            'DeclaredCapacity': ''}}
    crawlertype = crawlerdict.get(discom)
    len_crawlertype = len(crawlertype)
    logger.debug("upcl_intsch_crawler_tsk started")
    i = 0
    for key, value in crawlertype.items():
        self.update_state(state='PROGRESS',
                          meta={'current': i,
                                'total': len_crawlertype,
                                'status': key + ' Started...'})
        args = argparse.Namespace(alerts=False,
                                  state=state,
                                  start_date=startdate.strftime("%d-%m-%Y"),
                                  end_date=enddate.strftime("%d-%m-%Y"),
                                  dns=dsn,
                                  ftype=key,
                                  discom=discom)
        upcl_genschedule_crawler.main(args)
        self.update_state(state='PROGRESS',
                          meta={'current': i,
                                'total': len_crawlertype,
                                'status': key + ' Finished...'})
        i += 1
    return {'current': 100, 'total': 100, 'status': 'Task completed!',
            'result': 'Done'}


@ems.route('/intsch_crawler_task', methods=['POST'])
@login_required
def intsch_crawler_task():
    db = DB()
    # datacursor = con.cursor(cursors.DictCursor)
    datacursor = db.query_dictcursor("""select
    ldc_name, ldc_org_name, b.organisation_code
    from power.org_isgs_map a,
    power.organisation_master b
    where a.organisation_master_fk = b.organisation_master_pk
    and (b.organisation_master_pk = %s
    or b.organisation_parent_fk = %s)""" % (
        current_user.organisation_master_fk,
        current_user.organisation_master_fk))

    results = datacursor.fetchall()
    db.close()
    current_app.logger.info("intsch_crawler_task %s", results)
    conn_file = current_app.config['DB_CONNECT_FILE']
    discom = results[0].get('organisation_code')
    state = results[0].get('ldc_org_name')
    if results[0].get('organisation_code') == 'UPCL':
        task = upcl_intsch_crawler_tsk.apply_async((conn_file, discom, state))
        return jsonify({}), 202, {'Location': url_for('ems.taskstatus',
                                  job_nm="upcl_intsch_crawler_tsk", task_id=task.id)}       


@ems.route('/intdc_upload_task', methods=['POST'])
@login_required
def intdc_upload_task():
    db = DB()
    # datacursor = con.cursor(cursors.DictCursor)
    datacursor = db.query_dictcursor("""select
    ldc_name, ldc_org_name, organisation_code
    from power.org_isgs_map a,
    power.organisation_master b
    where a.organisation_master_fk = b.organisation_master_pk
    and (b.organisation_master_pk = %s
    or b.organisation_parent_fk = %s)""" % (
        current_user.organisation_master_fk,
        current_user.organisation_master_fk))
    results = datacursor.fetchall()
    db.close()
    current_app.logger.info("isgs_crawler_task %s", results)
    conn_file = current_app.config['DB_CONNECT_FILE']
    discom = results[0].get('organisation_code')
    task = \
        int_declaredcapacity_tsk.apply_async((conn_file, discom))
    return jsonify({}), 202, {'Location': url_for('ems.taskstatus',
                              job_nm="int_declaredcapacity_tsk",
                              task_id=task.id)}


@celery.task(bind=True)
def int_declaredcapacity_tsk(self, dsn, discom):
    import ems.batch.bin.sql_load_lib as sql_load_lib
    logger.debug("int_declaredcapacity_tsk started")

    self.update_state(state='PROGRESS',
                      meta={'current': 1,
                            'total': 3,
                            'status': ' Started...'})
    sql_load_lib.sql_sp_dayahead_int_dc_load(dsn, discom)
    self.update_state(state='PROGRESS',
                      meta={'current': 2,
                            'total': 3,
                            'status': ' Finished...'})
    return {'current': 100, 'total': 100, 'status': 'Task completed!',
            'result': 'Done'}


@ems.route('/jobs', methods=['GET', 'POST'])
@login_required
def jobs():
    if request.method == 'GET':
        return render_template('jobs.html')
    return redirect(url_for('jobs'))


@csrfprotect.exempt
@ems.route('/get_isgs_dates', methods=['POST'])
@login_required
def get_isgs_dates():
    current_app.logger.info("In get_isgs_dates")
    current_app.logger.debug("%s %s",
                             request.method,
                             request.form)
    if request.form.get('depdrop_parents[0]'):
        db = DB()
        datacursor = db.query_dictcursor("""select
        ldc_name, ldc_org_name
        from power.org_isgs_map a,
        power.organisation_master b
        where a.organisation_master_fk = b.organisation_master_pk
        and (b.organisation_master_pk = %s
        or b.organisation_parent_fk = %s)""", data=(
            current_user.organisation_master_fk,
            current_user.organisation_master_fk))
        results = datacursor.fetchall()
        db.close()
        if results[0].get('ldc_name') == 'NRLDC' and \
                request.form.get('depdrop_parents[0]') == 'Entitlement':
            # tablename = 'panel_nrldc_entitlements'
            query = """select date_format(date, '%%d-%%m-%%Y') id,
                       date_format(date, '%%d-%%m-%%Y') name
                       from (select distinct date
                             from nrldc_entitlements_stg
                             where state = %s order by date desc) a limit 10"""
        elif results[0].get('ldc_name') == 'NRLDC' and \
                request.form.get('depdrop_parents[0]') == 'Drawl Schedule':
            # tablename = 'panel_nrldc_state_drawl_schedule'
            query = """select date_format(date, '%%d-%%m-%%Y') id,
                       date_format(date, '%%d-%%m-%%Y') name
                       from (select distinct date
                             from nrldc_state_drawl_schedule_stg
                             where state = %s order by date desc) a limit 10"""
        elif results[0].get('ldc_name') == 'ERLDC' and \
                request.form.get('depdrop_parents[0]') == 'Drawl Schedule':
            # tablename = 'erldc_state_drawl_schedule_stg'
            query = """select date_format(date, '%%d-%%m-%%Y') id,
                       date_format(date, '%%d-%%m-%%Y') name
                       from (select distinct date
                             from erldc_state_drawl_schedule_stg
                             where discom = %s order by  date desc) a
                       limit 10"""
        elif results[0].get('ldc_name') == 'ERLDC' and \
                request.form.get('depdrop_parents[0]') == 'Entitlement':
            # tablename = 'erldc_entitlements_stg'
            query = """select date_format(date, '%%d-%%m-%%Y') id,
                       date_format(date, '%%d-%%m-%%Y') name
                       from (select distinct date
                             from erldc_entitlements_stg
                             where discom = %s order by  date desc) a
                             limit 10"""
        elif results[0].get('ldc_name') == 'WRLDC' and \
                request.form.get('depdrop_parents[0]') == 'Entitlement':
            query = """select date_format(date, '%%d-%%m-%%Y') id,
                       date_format(date, '%%d-%%m-%%Y') name
                       from (select distinct date
                             from wrldc_entitlements_stg
                             where state = %s order by date desc) a limit 10"""
        elif results[0].get('ldc_name') == 'WRLDC' and \
                request.form.get('depdrop_parents[0]') == 'Drawl Schedule':
            query = """select date_format(date, '%%d-%%m-%%Y') id,
                       date_format(date, '%%d-%%m-%%Y') name
                       from (select distinct date
                             from wrldc_state_drawl_schedule_stg
                             where state = %s
                             and date >= CURRENT_DATE - INTERVAL 20 DAY
                             order by date desc) a"""
        db = DB()
        datacursor = db.query_dictcursor(
            query, data=(results[0].get('ldc_org_name'),))
        dateobj = datacursor.fetchall()
        db.close()
        current_app.logger.debug("results: %s", dateobj)
        date_json = {"output": list(dateobj), "selected": ""}
    else:
        date_json = {"output": [], "selected": ""}
    return jsonify(date_json)


@csrfprotect.exempt
@ems.route('/get_isgs_revs', methods=['POST'])
@login_required
def get_isgs_revs():
    current_app.logger.info("In get_isgs_revs")
    current_app.logger.debug("%s %s",
                             request.method,
                             request.form)
    if request.form.get('depdrop_parents[1]'):
        db = DB()
        datacursor = db.query_dictcursor("""select
        ldc_name, ldc_org_name
        from power.org_isgs_map a,
        power.organisation_master b
        where a.organisation_master_fk = b.organisation_master_pk
        and (b.organisation_master_pk = %s
        or b.organisation_parent_fk = %s)""", data=(
            current_user.organisation_master_fk,
            current_user.organisation_master_fk))
        results = datacursor.fetchall()
        db.close()
        if results[0].get('ldc_name') == 'NRLDC' and \
                request.form.get('depdrop_parents[0]') == 'Entitlement':
            # tablename = 'panel_nrldc_entitlements'
            query = """select distinct cast(revision as char) id,
                       cast(revision as char) name
                       from nrldc_entitlements_stg
                       where state = %s
                       and date = str_to_date(%s, '%%d-%%m-%%Y')"""                     
        elif results[0].get('ldc_name') == 'NRLDC' and \
                request.form.get('depdrop_parents[0]') == 'Drawl Schedule':
            # tablename = 'panel_nrldc_state_drawl_schedule'
            query = """select distinct cast(revision as char) id,
                       cast(revision as char) name
                       from nrldc_state_drawl_schedule_stg
                       where state = %s
                       and date = str_to_date(%s, '%%d-%%m-%%Y')"""
        elif results[0].get('ldc_name') == 'ERLDC' and \
                request.form.get('depdrop_parents[0]') == 'Drawl Schedule':
            # tablename = 'erldc_state_drawl_schedule_stg'
            query = """select distinct cast(revision as char) id,
                       cast(revision as char) name
                       from erldc_state_drawl_schedule_stg
                       where discom = %s
                       and date = str_to_date(%s, '%%d-%%m-%%Y')"""
        elif results[0].get('ldc_name') == 'ERLDC' and \
                request.form.get('depdrop_parents[0]') == 'Entitlement':
            # tablename = 'erldc_entitlements_stg'
            query = """select distinct cast(revision as char) id,
                       cast(revision as char) name
                       from erldc_entitlements_stg
                       where discom = %s
                       and date = str_to_date(%s, '%%d-%%m-%%Y')"""
        elif results[0].get('ldc_name') == 'WRLDC' and \
                request.form.get('depdrop_parents[0]') == 'Entitlement':
            # tablename = 'panel_nrldc_entitlements'
            query = """select distinct cast(revision as char) id,
                       cast(revision as char) name
                       from wrldc_entitlements_stg
                       where state = %s
                       and date = str_to_date(%s, '%%d-%%m-%%Y')"""
        elif results[0].get('ldc_name') == 'WRLDC' and \
                request.form.get('depdrop_parents[0]') == 'Drawl Schedule':
            # tablename = 'panel_nrldc_state_drawl_schedule'
            query = """select distinct cast(revision as char) id,
                       cast(revision as char) name
                       from wrldc_state_drawl_schedule_stg
                       where state = %s
                       and date = str_to_date(%s, '%%d-%%m-%%Y')"""
        db = DB()
        datacursor = db.query_dictcursor(
            query, data=(results[0].get('ldc_org_name'),
                         request.form.get('depdrop_parents[1]')))
        revobj = datacursor.fetchall()
        db.close()
        current_app.logger.debug("results: %s", revobj)
        rev_json = {"output": list(revobj), "selected": ""}
    else:
        rev_json = {"output": [], "selected": ""}
    return jsonify(rev_json)

@csrfprotect.exempt
@ems.route('/get_intsch_dates', methods=['POST'])
@login_required
def get_intsch_dates():
    current_app.logger.info("In get_intsch_dates")
    current_app.logger.debug("%s %s",
                             request.method,
                             request.form)
    if request.form.get('depdrop_parents[0]'):
        db = DB()
        datacursor = db.query_dictcursor("""select
        ldc_name, ldc_org_name, organisation_code
        from org_isgs_map a,
        organisation_master b
        where a.organisation_master_fk = b.organisation_master_pk
        and (b.organisation_master_pk = %s
        or b.organisation_parent_fk = %s)""", data=(
            current_user.organisation_master_fk,
            current_user.organisation_master_fk))
        results = datacursor.fetchall()
        db.close()
        if request.form.get('depdrop_parents[0]') == 'Declared Capacity':
            query = """select date_format(date, '%%d-%%m-%%Y') id,
                       date_format(date, '%%d-%%m-%%Y') name
                       from (select distinct date
                             from internal_declared_capacity_stg
                             where discom = %s order by date desc) a limit 10"""
        elif request.form.get('depdrop_parents[0]') == 'Drawl Schedule':
            query = """select date_format(date, '%%d-%%m-%%Y') id,
                       date_format(date, '%%d-%%m-%%Y') name
                       from (select distinct date
                             from internal_drawl_schedule_stg
                             where discom = %s order by date desc) a limit 10"""                             
        db = DB()
        datacursor = db.query_dictcursor(
            query, data=(results[0].get('organisation_code'),))
        dateobj = datacursor.fetchall()
        db.close()
        current_app.logger.debug("results: %s", dateobj)
        date_json = {"output": list(dateobj), "selected": ""}
    else:
        date_json = {"output": [], "selected": ""}
    return jsonify(date_json)


@csrfprotect.exempt
@ems.route('/get_intsch_revs', methods=['POST'])
@login_required
def get_intsch_revs():
    current_app.logger.info("In get_intsch_revs")
    current_app.logger.debug("%s %s",
                             request.method,
                             request.form)
    if request.form.get('depdrop_parents[1]'):
        db = DB()
        datacursor = db.query_dictcursor("""select
        ldc_name, ldc_org_name, organisation_code
        from power.org_isgs_map a,
        power.organisation_master b
        where a.organisation_master_fk = b.organisation_master_pk
        and (b.organisation_master_pk = %s
        or b.organisation_parent_fk = %s)""", data=(
            current_user.organisation_master_fk,
            current_user.organisation_master_fk))
        results = datacursor.fetchall()
        db.close()
        if request.form.get('depdrop_parents[0]') == 'Declared Capacity':
            # tablename = 'panel_nrldc_entitlements'
            query = """select distinct cast(revision as char) id,
                       cast(revision as char) name
                       from internal_declared_capacity_stg
                       where discom = %s
                       and date = str_to_date(%s, '%%d-%%m-%%Y')"""                     
        elif request.form.get('depdrop_parents[0]') == 'Drawl Schedule':
            # tablename = 'panel_nrldc_state_drawl_schedule'
            query = """select distinct cast(revision as char) id,
                       cast(revision as char) name
                       from internal_drawl_schedule_stg
                       where discom = %s
                       and date = str_to_date(%s, '%%d-%%m-%%Y')"""

        db = DB()
        datacursor = db.query_dictcursor(
            query, data=(results[0].get('organisation_code'),
                         request.form.get('depdrop_parents[1]')))
        revobj = datacursor.fetchall()
        db.close()
        current_app.logger.debug("results: %s", revobj)
        rev_json = {"output": list(revobj), "selected": ""}
    else:
        rev_json = {"output": [], "selected": ""}
    return jsonify(rev_json)

# @celery.task(bind=True)
# def bseb_crawler_tsk(self):
#     import bseb_scada
#     import sql_load_lib
#     import shutil
#     message = ''
#     #--- constant connection values
#     print "\n-- Retreiving Files----\n"
#     ldir = homedir + '/Projects/bihar/scada/'
#     dsn = homedir + '/Projects/batch/config/sqldb_connection_config.txt'
#     tabnm = 'bseb_scada_stg'
#     onlynewfile = True  # set to true to grab & overwrite all files locally

#     fname = bseb_scada.ftpfiles("180.179.52.110", "dsmopt", "Mon$5zX", "DSM/ERLDC_BR",
#                      ldir, "BSEB*.csv", onlynewfile)
#     total = len(fname)
#     for indx, fil in enumerate(fname):
#         self.update_state(state='PROGRESS',
#                       meta={'current': indx, 'total': total,
#                             'status': message})
#         print 'Processing : ' + fil
#         if fil[:5] != 'BSEB_':
#             if os.path.exists(os.path.join(ldir, fil[:4])):
#                 bseb_scada.csvclean(os.path.join(ldir, fil),
#                          os.path.join(ldir, fil[:4] + '.tmp'))
#                 bseb_scada.csvdiff(os.path.join(ldir, fil[:4]),
#                         os.path.join(ldir, fil[:4] + '.tmp'),
#                         os.path.join(ldir, fil[:4] + '.load'))
#                 shutil.copy2(os.path.join(ldir, fil[:4]) + '.tmp',
#                              os.path.join(ldir, fil[:4]))
#             else:
#                 bseb_scada.csvclean(os.path.join(ldir, fil),
#                          os.path.join(ldir, fil[:4]))
#                 shutil.copy2(os.path.join(ldir, fil[:4]),
#                              os.path.join(ldir, fil[:4] + '.load'))

#             sql_load_lib.sql_table_load_exec(dsn,
#                                              tabnm,
#                                              os.path.join(ldir, fil[:4]
#                                                           + '.load'))
#         else:
#             sql_load_lib.sql_table_load_exec(dsn,
#                                              tabnm,
#                                              os.path.join(args.ldir, fil))
#         self.update_state(state='PROGRESS',
#                       meta={'current': total - 1, 'total': total,
#                             'status': message})
#         sql_load_lib.sql_bseb_realtime_forecast_surrender(dsn, None)
#     return {'current': 100, 'total': 100, 'status': 'Task completed!',
#             'result': 'Done'}


@celery.task(bind=True)
def bseb_crawler_tsk(self, dsn):
    import ems.batch.bin.bseb_scada as bseb_scada
    import ems.batch.bin.sql_load_lib as sql_load_lib

    message = 'In Progress'

    logger.debug("bseb_crawler_tsk started")
    # ldir = homedir + '/Projects/bihar/scada/'
    # dsn = homedir + '/Projects/batch/config/sqldb_connection_config.txt'
    ldir = "./ems/batch/data/bseb/"
    # dsn = "./ems/batch/config/sqldb_gcloud.txt"

    tabnm = 'bseb_scada_stg'
    onlynewfile = True  # set to true to grab & overwrite all files locally

    fname = bseb_scada.ftpfiles("180.179.52.110", "dsmopt",
                                "Mon$5zX", "DSM/ERLDC_BR",
                                ldir, "BSEB*.csv", onlynewfile)
    total = len(fname)
    for indx, fil in enumerate(fname):
        self.update_state(state='PROGRESS',
                          meta={'current': indx, 'total': total,
                                'status': message})
        logger.info('Processing : %s', fil)
        if fil[:5] != 'BSEB_':
            if os.path.exists(os.path.join(ldir, fil[:4])):
                bseb_scada.csvclean(os.path.join(ldir, fil),
                                    os.path.join(ldir, fil[:4] + '.tmp'))
                bseb_scada.csvdiff(os.path.join(ldir, fil[:4]),
                                   os.path.join(ldir, fil[:4] + '.tmp'),
                                   os.path.join(ldir, fil[:4] + '.load'))
                shutil.copy2(os.path.join(ldir, fil[:4]) + '.tmp',
                             os.path.join(ldir, fil[:4]))
            else:
                bseb_scada.csvclean(os.path.join(ldir, fil),
                                    os.path.join(ldir, fil[:4]))
                shutil.copy2(os.path.join(ldir, fil[:4]),
                             os.path.join(ldir, fil[:4] + '.load'))

            sql_load_lib.sql_table_load_exec(dsn,
                                             tabnm,
                                             os.path.join(ldir,
                                                          fil[:4] + '.load'))
        else:
            sql_load_lib.sql_table_load_exec(dsn,
                                             tabnm,
                                             os.path.join(ldir, fil))
        self.update_state(state='PROGRESS',
                          meta={'current': total - 1, 'total': total,
                                'status': message})
        sql_load_lib.sql_bseb_realtime_forecast_surrender(dsn, None)
    return {'current': 100, 'total': 100, 'status': 'Task completed!',
            'result': 'Done'}


@ems.route('/bseb_crawler_task', methods=['POST'])
@login_required
def bseb_crawler_task():
    conn_file = current_app.config['DB_CONNECT_FILE']
    task = bseb_crawler_tsk.apply_async((conn_file,))
    return jsonify({}), 202, {'Location': url_for('ems.taskstatus',
                                                  job_nm="bseb_crawler_tsk",
                                                  task_id=task.id)}


# @celery.task(bind=True)
# def exchange_crawler_tsk(self):
#     import power_exchange_crawler
#     import sql_load_lib
#     import datetime
#     message = ''
#     exchange = {'IEX':['http://www.iexindia.com/marketdata/areaprice.aspx', 'IEX_AreaPrice'],
#                 'PXIL':['http://www.powerexindia.com/PXILReport/pages/MCPReport_New.aspx', 'PXIL_AreaPrice']}
#     dsn = homedir + '/Projects/batch/config/sqldb_connection_config.txt'
#     tabnm = 'exchange_areaprice_stg'
#     ldir = homedir + "/Projects/batch/Data/"
#     date = datetime.datetime.now()
#     total = len(exchange)
#     for indx, key in enumerate(exchange):
#         self.update_state(state='PROGRESS',
#                       meta={'current': indx, 'total': total,
#                             'status': message})
#         filename = power_exchange_crawler.energy_crawler(key, exchange[key][0], ldir,
#                             exchange[key][1], date.strftime('%d/%m/%Y'))
#         exdata = power_exchange_crawler.exchange_xls_to_list(filename, key)
#         sql_load_lib.sql_table_insert_exec(dsn, tabnm, exdata)
#         self.update_state(state='PROGRESS',
#                       meta={'current': total, 'total': total,
#                             'status': message})
#     return {'current': 100, 'total': 100, 'status': 'Task completed!',
#             'result': 'Done'}


@celery.task(bind=True)
def exchange_crawler_tsk(self, dsn):
    import ems.batch.bin.power_exchange_crawler as power_exchange_crawler
    import ems.batch.bin.sql_load_lib as sql_load_lib

    logger.debug("exchange_crawler_tsk started")
    message = 'In Progress'
    # exchange = {'IEX': ['https://www.iexindia.com/marketdata/areaprice.aspx',
    #                     'IEX_AreaPrice'],
    #             'PXIL': [('http://www.powerexindia.com/PXILReport'
    #                       '/pages/MCPReport_New.aspx'),
    #                      'PXIL_AreaPrice']}
    # dsn = "./ems/batch/config/sqldb_gcloud.txt"
    exchange = {'IEX': ['https://www.iexindia.com/marketdata/areaprice.aspx',
                        'IEX_AreaPrice']}    
    tabnm = 'exchange_areaprice_stg'
    # ldir = homedir + "/Projects/batch/Data/"
    ldir = "./ems/batch/data/exchange/"
    archive = "./ems/batch/data_archive/exchange/"
    # date = datetime.datetime.now()
    if time.tzname[0] == 'IST':
        date = datetime.today()
    else:
        dest_tz = pytz.timezone('Asia/Kolkata')
        ts = time.time()
        utc_now = datetime.utcfromtimestamp(ts)
        date = utc_now.replace(tzinfo=pytz.utc).astimezone(dest_tz)

    total = len(exchange)
    for indx, key in enumerate(exchange):
        self.update_state(state='PROGRESS',
                          meta={'current': indx, 'total': total,
                                'status': message})
        filename = power_exchange_crawler\
            .energy_crawler(key, exchange[key][0],
                            ldir,
                            exchange[key][1],
                            date.strftime('%d/%m/%Y'))
        exdata = power_exchange_crawler.exchange_xls_to_list(filename, key)
        sql_load_lib.sql_table_insert_exec(dsn, tabnm, exdata)
        self.update_state(state='PROGRESS',
                          meta={'current': total, 'total': total,
                                'status': message})
        newpath = os.path.join(archive, os.path.basename(filename))
        logger.debug('path %s', newpath)
        logger.debug('filename %s', filename)
        # if os.path.exists(newpath):
        #     os.remove(newpath)
        shutil.move(filename, newpath)
    return {'current': 100, 'total': 100, 'status': 'Task completed!',
            'result': 'Done'}


@ems.route('/exchange_upload_task', methods=['POST'])
@login_required
def exchange_upload_task():
    conn_file = current_app.config['DB_CONNECT_FILE']
    task = exchange_crawler_tsk.apply_async((conn_file, ))
    return jsonify({}), 202, {'Location': url_for('ems.taskstatus',
                              job_nm="exchange_crawler_tsk", task_id=task.id)}


# @celery.task(bind=True)
# def forecast_upload_tsk(self):
#     import bseb_forecast_upload
#     import sql_load_lib
#     import shutil
#     message = ''
#     dsn = homedir + '/Projects/batch/config/sqldb_connection_config.txt'
#     tabnm = 'forecast_stg_bseb'
#     filenm = homedir + "/Projects/bihar/final_forecast/Annexure A*.xlsx"
#     archive = homedir + "/Projects/bihar/final_forecast/archive/"
#     filenm_dir = os.path.split(filenm)[0]
#     filenm_pattern = os.path.split(filenm)[1]
#     #print filenm_dir, filenm_pattern
#     # archive = os.path.join(archive, filenm_pattern)
#     # print archive
#     total = len(bseb_forecast_upload.dir_diff_in_files(filenm, archive))
#     for indx, filenmx in enumerate(bseb_forecast_upload.dir_diff_in_files(filenm, archive)):
#         self.update_state(state='PROGRESS',
#                       meta={'current': indx, 'total': total,
#                             'status': message})
#         full_path_filenm = os.path.join(filenm_dir, filenmx)
#         afull_path_filenm = os.path.join(archive, filenmx)
#         print full_path_filenm, afull_path_filenm
#         data_list_out = bseb_forecast_upload.excel_to_tuple(full_path_filenm)
#         # print data_list_out
#         sql_load_lib.sql_table_insert_exec(dsn,
#                                            tabnm,
#                                            data_list_out)
#         shutil.copyfile(full_path_filenm, afull_path_filenm)
#     return {'current': 100, 'total': 100, 'status': 'Task completed!',
#             'result': 'Done'}


# @ems.route('/forecast_upload_task', methods=['POST'])
# @login_required
# def forecast_upload_task():
#     task = forecast_upload_tsk.apply_async()
#     return jsonify({}), 202, {'Location': url_for('ems.taskstatus', job_nm="forecast_upload_tsk", task_id=task.id)}


@celery.task(bind=True)
def bseb_dataupload_tsk(self, filenm, dsn):
    import ems.batch.bin.constrained_load_upload as constrained_load_upload
    import ems.batch.bin.sql_load_lib as sql_load_lib
    # import os

    logger.info("Starting bseb_dataupload_tsk %s", filenm)
    message = 'In Progress'

    # dsn = "./ems/batch/config/sqldb_gcloud.txt"
    state = 'BIHAR'
    total = len(filenm) + 1
    count = 0
    powercut_sp_execute_flg = False
    self.update_state(state='PROGRESS',
                      meta={'current': 0, 'total': total,
                            'status': message})
    for file_nm in filenm:
        #print "processing file: " + file_nm
        basepath, filename = os.path.split(file_nm)
        if filename[:4] == 'Load':
            data = constrained_load_upload\
                .contrained_load(file_nm, state.upper())
            # print data
            sql_load_lib.sql_table_insert_exec(dsn,
                                               'drawl_staging',
                                               data)
        elif filename[:8] == 'Powercut':
            powercut_sp_execute_flg = True
            data = constrained_load_upload\
                .powercut_load(file_nm, state.upper())
            sql_load_lib.sql_table_insert_exec(dsn,
                                               'powercut_staging',
                                               data)
        count = count + 1
        self.update_state(state='PROGRESS',
                          meta={'current': count, 'total': total,
                                'status': message})
        if powercut_sp_execute_flg:
            sql_load_lib.sql_powercut_station_discom_upd(dsn)

    return {'current': 100, 'total': 100, 'status': 'Task completed!',
            'result': 'Done'}


def demand_csv_validator(valid_discom, data, ftype="DEFAULT"):
    """
    Validates the CSV file based on rules
    """
    logger.info("Started demand_csv_validator %s %s %s",
                valid_discom, data, ftype)
    field_names = {"HOURLY": ('Date',
                              'Hour',
                              'Type',
                              'Discom',
                              'Unit',
                              'Frequency',
                              'UIRate',
                              'InternalGeneration',
                              'Schedule',
                              'Demand'),
                   "DEFAULT": ('Date',
                               'BlockNo',
                               'Type',
                               'Discom',
                               'Unit',
                               'Frequency',
                               'UIRate',
                               'InternalGeneration',
                               'Schedule',
                               'Demand')}

    def check_frequency_uirate(r):
        # logger.info(r)
        colname = ("Frequency", "UIRate")
        try:
            for col in colname:
                logger.debug('instance %s',
                             isinstance(r[col], (str)))
                logger.debug('value **%s**', r[col])
                logger.debug('type **%s**', type(r[col]))
                if not (isinstance(r[col], (str)) and
                        r[col] in (u'', '')):
                    logger.debug('else %s', r[col])
                    validator.add_value_check(float(r[col]), float,
                                              'EX8',
                                              'Invalid %s' % (col))
        except Exception as e:
            logger.error("Error check_frequency_uirate %s", e)
            raise RecordError('X4', '%s Not a valid value' % (r[col]))

    validator = CSVValidator(field_names.get(ftype))
    # basic header and record length checks
    validator.add_header_check('EX1', 'bad header')
    # Use if all the fields require data
    # validator.add_record_length_check('EX2', 'unexpected record length')
    # some simple value checks
    validator.add_value_check('Date', datetime_string('%d/%m/%Y'),
                              'EX3', 'Invalid date')
    if ftype == "HOURLY":
        validator.add_value_check('Hour', number_range_inclusive(1, 24, int),
                                  'EX4', 'Hour must be a integer')
        validator.add_unique_check(('Date', 'Hour', 'Type', 'Discom', 'Unit'),
                                   'X5', 'Unqiueness of Records Failed.'
                                   'Check if Date Hour Type Discom are unique')
    else:
        validator.add_value_check('BlockNo',
                                  number_range_inclusive(1, 96, int),
                                  'EX4', 'BlockNo must be a integer')
        validator.add_unique_check(('Date', 'BlockNo', 'Type',
                                   'Discom', 'Unit'),
                                   'X5', 'Unqiueness of Records Failed.'
                                   'Check if Date Hour Type Discom are unique')
    validator.add_value_check('Type', str,
                              'EX5', 'Type must be a string')
    validator.add_value_check('Discom', enumeration(",".join(valid_discom)),
                              'EX6', 'Invalid Discom')
    validator.add_value_check('Unit', str,
                              'EX7', 'Invalid Unit')
    # validator.add_value_check('Frequency', float,
    #                           'EX8', 'Invalid Frequency')
    # validator.add_value_check('UIRate', float,
    #                           'EX9', 'Invalid UIRate')
    validator.add_record_check(check_frequency_uirate)
    validator.add_value_check('InternalGeneration', float,
                              'EX10', 'Invalid InternalGeneration')
    validator.add_value_check('Schedule', float,
                              'EX11', 'Invalid Schedule')
    validator.add_value_check('Demand', float,
                              'EX12', 'Invalid Demand')
    problems = validator.validate(data)
    logger.debug("Problems demand_csv_validator %s",
                 problems)
    return problems


def powercut_csv_validator(valid_discom, data):
    """
    Validates the CSV file based on rules
    """
    logger.info("Started powercut_csv_validator %s %s",
                valid_discom, data)
    field_names = ('Date', 'StationID', 'Description',
                   'BlockNo', 'PowerCut(MW)', 'Discom')

    def check_stationid_description(r):
        logger.info(r)
        empty = []
        colname = ('StationID', 'Description')
        try:
            for col in colname:
                if (isinstance(r[col], (str)) and
                        r[col] in (u'', '')):
                    empty.append(col)
            if len(empty) == 2:
                raise RecordError('X1', 'Both %s and %s cannot be empty'
                                  % colname)
        except Exception as e:
            logger.error("Error %s", e)
            raise RecordError('X4', 'Unknown error for %s' % r)

    validator = CSVValidator(field_names)
    # basic header and record length checks
    validator.add_header_check('EX1', 'Bad header')
    # Use if all the fields require data
    # validator.add_record_length_check('EX2', 'Unexpected record length')
    # some simple value checks
    validator.add_value_check('Date', datetime_string('%d/%m/%Y'),
                              'EX3', 'Invalid date')
    validator.add_value_check('StationID', str,
                              'EX4', 'Type must be a string')
    validator.add_value_check('Description', str,
                              'EX5', 'Type must be a string')
    validator.add_value_check('BlockNo',
                              number_range_inclusive(1, 96, int),
                              'EX6', 'BlockNo must be a integer')
    validator.add_value_check('PowerCut(MW)', float,
                              'EX7', 'PowerCut must be a numeric')
    validator.add_value_check('Discom', enumeration(",".join(valid_discom)),
                              'EX8', 'Invalid Discom')
    validator.add_record_check(check_stationid_description)
    validator.add_unique_check(('Date', 'StationID', 'Description',
                                'BlockNo', 'Discom'), 'X3',
                               'Unqiueness of Records Failed.'
                               'Check if Date, StationID,'
                               'Description, BlockNo and Discom'
                               'are unique')
    problems = validator.validate(data)
    logger.debug("Problems powercut_csv_validator %s",
                 problems)
    return problems


def demand_data_clnup(filename, ftype="DEFAULT"):
    """
    Clean the constrained load data file
    """
    logger.info('Processing file: %s', filename)
    field_names = {"HOURLY": ['date', 'block_hour_no', 'discom', 'frequency',
                              'ui_rate', 'internal_generation',
                              'central_generation', 'constrained_load'],
                   "DEFAULT": ['date', 'block_no', 'discom', 'frequency',
                               'ui_rate', 'internal_generation',
                               'central_generation', 'constrained_load']}
    columns = field_names.get(ftype)

    date = etl.dateparser('%d/%m/%Y', strict=True)
    table1 = etl\
        .fromcsv(filename)\
        .cutout('Unit')\
        .cutout(2)\
        .setheader(columns)\
        .convert('date', date)\
        .convert(('frequency', 'ui_rate', 'internal_generation',
                  'central_generation', 'constrained_load'), float)\
        .addfield('schedule', lambda row:
                  row['internal_generation'] + row['central_generation']
                  if row['internal_generation'] and row['central_generation']
                  else None)
    table2 = etl.convert(table1, 'block_hour_no', int) \
        if ftype == 'HOURLY' else \
        etl.convert(table1, 'block_no', int)
    return table2


def powercut_data_clnup(filename):
    """
    Clean the powercut load data file
    """
    logger.info('Processing file: %s', filename)
    columns = ['date', 'station', 'description',
               'block_no', 'powercut', 'discom']
    date = etl.dateparser('%d/%m/%Y', strict=True)
    table1 = etl\
        .fromcsv(filename)\
        .setheader(columns)\
        .convert('date', date)\
        .convert('block_no', int)\
        .convert('powercut', float)

    logger.info("table1: %s", table1)
    logger.info("Rowcount: %s", etl.nrows(table1))
    table2 = etl.select(table1,
                        "{block_no} is not None and {powercut} is not None")
    logger.info("Rowcount after filter for None: %s", etl.nrows(table2))
    logger.info("table2: %s", table2)
    return table2


def transform_hourly_to_block(conn, load_data_hourly):
    """
    Transform houly to blockwise using table block_master
    """
    block_master = etl.fromdb(conn, "select block_no, block_hour_no "
                              "from block_master where delete_ind = 0")
    logger.debug('Data**** block_master {}'.format(block_master))
    demand_block = etl.join(block_master,
                            load_data_hourly,
                            key='block_hour_no')\
                      .cutout('block_hour_no')
    return demand_block


def get_discom_state(conn, data, valid_discoms):
    """
    Transform houly to blockwise using table block_master
    """
    logger.debug('state_master**** ('"{}"')'.format('","'.join(valid_discoms)))
    placeholder = ", ".join(["'%s'"] * len(valid_discoms))
    sql = """select distinct b.organisation_code as discom,
             c.state_name as state
             from power.organisation_master b,
             power.state_master c,
             power.organisation_type a
             where c.state_master_pk = b.state_master_fk
             and b.organisation_type_fk
                = a.organisation_type_pk
             and b.organisation_code in ({})
             and a.organisation_type_code = 'DU'""".format(placeholder)

    logger.info('{} {}'.format(sql, tuple(valid_discoms)))
    state_master = etl.fromdb(conn, sql % tuple(valid_discoms))

    logger.debug('Data**** state_master {}'.format(state_master))
    logger.debug('Data**** data {}'.format(data))
    data_with_state = etl.join(data,
                               state_master,
                               key='discom')
    return data_with_state


def upload_filename_validation(filenm, valid_discom):
    """
    Rules for checking the files being uploaded
    """
    filename_pattern = ("(^%s)(_LOAD_|_POWERCUT_)"
                        "(\w*)(\d{2}\d{2}\d{4})"
                        "(_\d{2}\d{2})*"
                        % "|".join(valid_discom))
    fn_cp = re.compile(filename_pattern, re.IGNORECASE)
    demand_cp = re.compile('_LOAD_', re.IGNORECASE)
    powercut_cp = re.compile('_POWERCUT_', re.IGNORECASE)
    datetime_cp = re.compile("(\d{2}\d{2}\d{4})(_\d{2}\d{2})*")

    def filelist_sortkey(filename):
        """
        Sort key for sorting the files
        """
        basepath, filename = os.path.split(filename)
        if re.search(fn_cp, filename) and \
                len(re.search(datetime_cp, filename).group(0)) == 8:
            return datetime.strptime(re.search(datetime_cp, filename).group(0),
                                     '%d%m%Y')
        elif re.search(fn_cp, filename) and \
                len(re.search(datetime_cp, filename).group(0)) > 8:
            return datetime.strptime(re.search(datetime_cp, filename).group(0),
                                     '%d%m%Y_%H%M')

    return fn_cp, demand_cp, powercut_cp, datetime_cp, filelist_sortkey


def demand_file_db_load(conn, file_nm, clean_ftype, valid_discom):
    """
    Load the file data to db
    """
    # cursor = conn.cursor().execute('SET SQL_MODE=ANSI_QUOTES')
    load_data = demand_data_clnup(file_nm, clean_ftype)
    logger.debug('Data****load_data {}'.format(load_data))
    if clean_ftype == 'HOURLY':
        load_data_block = transform_hourly_to_block(conn,
                                                    load_data)
        logger.debug('Data****load_data_block {}'.format(load_data_block))
    else:
        load_data_block = load_data
    logger.debug('Data****load_data_block2 {}'.format(load_data_block))
    load_data_state = get_discom_state(conn,
                                       load_data_block,
                                       valid_discom)
    logger.debug('Data****load_data_state {}'.format(load_data_state))
    if load_data_state:
        sql = """insert into drawl_staging
               (block_no, date, discom, frequency,
                ui_rate, internal_generation,
                central_generation, constrained_load,
                schedule, state)
                values (%s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s)
                on duplicate key update
                frequency = values(frequency),
                ui_rate = values(ui_rate),
                internal_generation = values(internal_generation),
                central_generation = values(central_generation),
                schedule = values(schedule),
                constrained_load = values(constrained_load),
                processed_ind = 0"""
        colname = etl.header(load_data_state)
        data = list(etl.records(load_data_state))
        logger.debug('Header**** {}'.format(colname))
        logger.debug('Records**** {}'.format(data))
        # logger.debug(sql % data[0])
        # connection = conn.connect()
        # connection.execute(sql, data)
        # connection.close()
        datacursor = conn.cursor()
        try:
            rv = datacursor.executemany(sql, data)
            logger.info("Return Value %s", str(rv))
            logger.info("Load Status %s", str(conn.info()))
            conn.commit()
            datacursor.close()
        except Exception as error:
            conn.rollback()
            datacursor.close()
            logger.info("Error %s", str(error))
            return False
    return


def powercut_file_db_load(conn, file_nm, valid_discom):
    """
    Load powercut file data to db
    """
    powercut_data = powercut_data_clnup(file_nm)
    logger.debug('Data**** {}'.format(powercut_data))
    powercut_data_state = get_discom_state(conn,
                                           powercut_data,
                                           valid_discom)
    logger.debug('Data**** {}'.format(powercut_data_state))
    if powercut_data_state:
        sql = """insert into powercut_staging
               (date, station, description,
                block_no, powercut, discom, state)
                values (%s, %s, %s,
                        %s, %s, %s, %s)
                on duplicate key update
                powercut = values(powercut),
                processed_ind = 0,
                load_date = NULL"""
        colname = etl.header(powercut_data_state)
        data = list(etl.records(powercut_data_state))
        logger.debug('Header**** {}'.format(colname))
        logger.debug('Records**** {}'.format(data))
        # logger.debug(sql % data[0])
        # connection = conn.connect()
        # connection.execute(sql, data)
        # connection.close()
        datacursor = conn.cursor()
        try:
            rv = datacursor.executemany(sql, data)
            logger.debug("Return Value %s", str(rv))
            logger.debug("Load Status %s", str(conn.info()))
            conn.commit()
            datacursor.close()
        except Exception as error:
            conn.rollback()
            datacursor.close()
            logger.debug("Error %s", str(error))
    return


@celery.task(bind=True)
def data_upload_tsk(self, filenm, valid_discom, dsn):
    import ems.batch.bin.dbconn as dbconn

    logger.info("Started data_upload_tsk %s", filenm)
    message = 'In Progress'
    total = len(filenm) + 1
    count = 0
    self.update_state(state='PROGRESS',
                      meta={'current': 0,
                            'total': total,
                            'status': message})

    fn_cp, demand_cp, powercut_cp, datetime_cp, filelist_sortkey = \
        upload_filename_validation(filenm, valid_discom)

    sorted_filenm = sorted(filenm, key=filelist_sortkey)

    for file_nm in sorted_filenm:
        logger.info("Processing file %s %s", valid_discom, file_nm)
        basepath, filename = os.path.split(file_nm)
        logger.debug("Processing file %s %s",
                    filename, re.search(fn_cp, filename))
        if re.search(fn_cp, filename) and re.search(demand_cp, filename):
            table = etl\
                .fromcsv(file_nm)
            ftype = re.search(fn_cp, filename).group(3)
            clean_ftype = ftype.strip('_')        
            problems = demand_csv_validator(valid_discom, table,
                                            clean_ftype)
            if not problems:
                conn = dbconn.connect(dsn)
                try:
                    demand_file_db_load(conn, file_nm,
                                        clean_ftype, valid_discom)
                finally:
                    conn.close()
            else:
                raise Exception('Record Errors in File {}: {}'
                                .format(file_nm.split('/')[-1],
                                        problems))
        elif re.search(fn_cp, filename) and re.search(powercut_cp, filename):
            table = etl\
                .fromcsv(file_nm)
            problems = powercut_csv_validator(valid_discom, table)
            logger.debug('Problems**** {}'.format(problems))
            if not problems:
                conn = dbconn.connect(dsn)
                try:
                    powercut_file_db_load(conn, file_nm, valid_discom)
                finally:
                    conn.close()
            else:
                message = problems
                self.update_state(state='FAILED',
                                  meta={'current': count,
                                        'total': total,
                                        'status': message})
                raise Exception('Record Errors in File {}: {}'
                                .format(file_nm.split('/')[-1],
                                        problems))
        else:
            message = ('Warning: File {} is not a valid'
                       'file name and not processed.'
                       .format(file_nm.split('/')[-1]))
        count = count + 1
        self.update_state(state='PROGRESS',
                          meta={'current': count,
                                'total': total,
                                'status': message})
    if message == 'In Progress':
        message = 'Task completed!'
    return {'current': 100, 'total': 100, 'status': message,
            'result': 'Done'}


def tentativedata_data_conversion(filename):
    """Convert the data to tabular form"""
    logger.info('Processing file: %s', filename)

    date = etl.dateparser('%d/%m/%Y', strict=True)
    table1 = etl\
        .fromcsv(filename)
    header = list(table1[2][0:4])
    header_right = zip(table1[0], table1[1], table1[2])[4:]
    header.extend(header_right)
    table2 = etl.skip(table1, 2).convertnumbers().setheader(header)
    logger.debug("table2: %s", table2)
    logger.info("Rowcount table2: %s", etl.nrows(table2))
    table3 = etl\
        .melt(table2, key=['DATE', 'DATE_FOR', 'BLOCK', 'DISCOM'],
              variablefield='GENERATOR_NAME',
              valuefield='TENTATIVE_GENERATION')\
        .convert(('DATE', 'DATE_FOR'), date)\
        .convert('BLOCK', int)\
        .unpack('GENERATOR_NAME', ['GENERATOR_COMPANY_NAME',
                                   'GENERATOR_TYPE',
                                   'GENERATOR_NAME'])
    logger.debug("table3: %s", table3)
    logger.info("Rowcount table3: %s", etl.nrows(table3))
    table4 = etl.select(table3,
                        ("{BLOCK} is not None and "
                         "{TENTATIVE_GENERATION} is not None"))
    logger.info("Rowcount after filter for None: %s", etl.nrows(table4))
    logger.debug("table4: %s", table4)
    return table4


def tentativedata_csv_validator(valid_discom, data):
    """
    Validates the CSV file based on rules
    """
    logger.info("Started tentativedata_csv_validator %s %s",
                valid_discom, data)
    field_names = ('DATE', 'DATE_FOR', 'BLOCK', 'DISCOM',
                   'TENTATIVE_GENERATION', 'GENERATOR_COMPANY_NAME',
                   'GENERATOR_TYPE', 'GENERATOR_NAME')

    validator = CSVValidator(field_names)
    # basic header and record length checks
    validator.add_header_check('EX1', 'Bad header')
    # Use if all the fields require data
    # validator.add_record_length_check('EX2', 'Unexpected record length')
    # some simple value checks
    # validator.add_value_check('DATE', datetime_string('%d/%m/%Y'),
    #                           'EX2', 'Invalid DATE')
    # validator.add_value_check('DATE_FOR', datetime_string('%d/%m/%Y'),
    #                           'EX3', 'Invalid DATE_FOR')
    validator.add_value_check('BLOCK',
                              number_range_inclusive(1, 96, int),
                              'EX4', 'BLOCK must be a integer')
    validator.add_value_check('DISCOM', str,
                              'EX5', 'Type must be a string')
    validator.add_value_check('DISCOM', enumeration(",".join(valid_discom)),
                              'EX6', 'Invalid Discom')
    validator.add_value_check('TENTATIVE_GENERATION', float,
                              'EX7', 'Generation values must be numeric')
    validator.add_value_check('GENERATOR_COMPANY_NAME', str,
                              'EX8', 'Company Name must be a string')
    validator.add_value_check('GENERATOR_TYPE', str,
                              'EX9', 'Generator Type must be a string')
    validator.add_value_check('GENERATOR_NAME', str,
                              'EX10', 'Generator Name must be a string')
    validator.add_unique_check(('DATE', 'DATE_FOR', 'BLOCK', 'DISCOM',
                                'GENERATOR_COMPANY_NAME', 'GENERATOR_TYPE',
                                'GENERATOR_NAME'), 'X1',
                               'Unqiueness of Records Failed.')
    problems = validator.validate(data)
    logger.debug("Problems tentativedata_csv_validator %s",
                 problems)
    return problems


def tentativedata_validation(filenm, valid_discom):
    """
    Rules for checking the files being uploaded
    """
    filename_pattern = ("^(%s)(.*DAY.*AHEAD.*ENTITLEMENT)"
                        "(\w*)(\d{2}\d{2}\d{4})*"
                        % "|".join(valid_discom))
    fn_cp = re.compile(filename_pattern, re.IGNORECASE)
    date_cp = re.compile("(\d{2}\d{2}\d{4})*")

    def filelist_sortkey(filename):
        """
        Sort key for sorting the files
        """
        basepath, filename = os.path.split(filename)
        if re.search(fn_cp, filename) and \
                len(re.search(date_cp, filename).group(0)) == 8:
            return datetime.strptime(re.search(date_cp, filename).group(0),
                                     '%d%m%Y')

    return fn_cp, date_cp, filelist_sortkey


def tentativedata_db_load(conn, data, valid_discom):
    """
    Load powercut file data to db
    """
    logger.debug('Data**** {}'.format(data))
    # lowercase the headers
    data = etl.setheader(data, [s.lower() for s in data[0]])
    tentative_data_state = get_discom_state(conn,
                                            data,
                                            valid_discom)
    logger.info('Data**** {}'.format(tentative_data_state))
    if tentative_data_state:
        sql = """insert into tentative_schedule_staging
               (declared_date, date, block_no, discom,
                tentative_generation, generation_entity_name,
                generation_type, generator_name, state)
                values (%s, %s, %s, %s,
                        %s, %s,
                        %s, %s, %s)
                on duplicate key update
                tentative_generation = values(tentative_generation),
                processed_ind = 0,
                load_date = NULL"""
        colname = etl.header(tentative_data_state)
        data = list(etl.records(tentative_data_state))
        logger.debug('Header**** {}'.format(colname))
        logger.debug('Records**** {}'.format(data))
        # logger.debug(sql % data[0])
        # connection = conn.connect()
        # connection.execute(sql, data)
        # connection.close()
        datacursor = conn.cursor()
        try:
            rv = datacursor.executemany(sql, data)
            logger.debug("Return Value %s", str(rv))
            logger.info("Load Status %s", str(conn.info()))
            conn.commit()
            datacursor.close()
        except Exception as error:
            conn.rollback()
            datacursor.close()
            logger.debug("Error %s", str(error))
    return


@celery.task(bind=True)
def tentativedata_upload_tsk(self, filenm, valid_discom, dsn):
    import ems.batch.bin.dbconn as dbconn

    logger.info("Started data_upload_tsk %s", filenm)
    message = 'In Progress'
    total = len(filenm) + 1
    count = 0
    self.update_state(state='PROGRESS',
                      meta={'current': 0,
                            'total': total,
                            'status': message})
    fn_cp, date_cp, filelist_sortkey = \
        tentativedata_validation(filenm, valid_discom)

    sorted_filenm = sorted(filenm, key=filelist_sortkey)
    for file_nm in sorted_filenm:
        logger.info("Processing file %s %s", valid_discom, file_nm)
        basepath, filename = os.path.split(file_nm)
        logger.info("Processing file %s %s",
                    filename, re.search(fn_cp, filename))
        if re.search(fn_cp, filename):
            # table = etl.fromcsv(file_nm)
            table = tentativedata_data_conversion(file_nm)
            problems = tentativedata_csv_validator(valid_discom, table)
            if not problems:
                conn = dbconn.connect(dsn)
                try:
                    tentativedata_db_load(conn, table, valid_discom)
                finally:
                    conn.close()
            else:
                raise Exception('Record Errors in File {}: {}'
                                .format(file_nm.split('/')[-1],
                                        problems))
        else:
            message = ('Warning: File {} is not a valid'
                       'file name and not processed.'
                       .format(file_nm.split('/')[-1]))
        count = count + 1
        self.update_state(state='PROGRESS',
                          meta={'current': count,
                                'total': total,
                                'status': message})
    if message == 'In Progress':
        message = 'Task completed!'
    return {'current': 100, 'total': 100, 'status': message,
            'result': 'Done'}

# @celery.task(bind=True)
# def bseb_dataupload_tsk(self):
#     print "Started bseb_dataupload_tsk started"
#     # import constrained_load_upload
#     # import sql_load_lib
#     import batch.bin.constrained_load_upload as constrained_load_upload
#     import batch.bin.sql_load_lib as sql_load_lib
#     import glob
#     import shutil
#     basedir = os.path.abspath(os.path.dirname(__file__))
#     message = ''
#     # dsn = homedir + '/Projects/batch/config/sqldb_connection_config.txt'
#     dsn = basedir + "/batch/config/sqldb_gcloud.txt"
#     filenm = homedir + "/Projects/bihar/io/daily/*.csv"
#     archive = homedir + "/Projects/bihar/io/daily/archive/"
#     state = 'BIHAR'
#     total = len(glob.glob(filenm)) + 1
#     powercut_sp_execute_flg = False;
#     for indx, filex in enumerate(glob.iglob(filenm)):
#         self.update_state(state='PROGRESS',
#                       meta={'current': indx, 'total': total,
#                             'status': message})
#         print filex
#         filenm = os.path.abspath(filex).replace("\\", "/")
#         print "processing file: " + filenm
#         basepath, filename = os.path.split(filenm)
#         if filename[:4] == 'Load':
#             data = constrained_load_upload.contrained_load(filenm, state.upper())
#             # print data
#             sql_load_lib.sql_table_insert_exec(dsn,
#                                                'drawl_staging',
#                                                data)
#         else:
#             powercut_sp_execute_flg = True;
#             sql_load_lib.sql_table_load_exec(dsn,
#                                              'powercut_staging',
#                                              (filenm, state.upper()))
#         newpath = os.path.join(archive, filename)
#         if os.path.exists(newpath):
#             os.remove(newpath)
#         shutil.move(filenm, archive)
#         self.update_state(state='PROGRESS',
#                       meta={'current': total - 1, 'total': total,
#                             'status': message})
#     if powercut_sp_execute_flg:
#         #Execute stored procedure
#         sql_load_lib.sql_powercut_station_discom_upd(dsn)
#     self.update_state(state='PROGRESS',
#                   meta={'current': total, 'total': total,
#                         'status': message})
#     return {'current': 100, 'total': 100, 'status': 'Task completed!',
#             'result': 'Done'}


@ems.route('/bseb_dataupload_task', methods=['POST'])
@login_required
def bseb_dataupload_task(filenm):
    current_app.logger.info('bseb_dataupload_task*** %s', filenm)
    task = bseb_dataupload_tsk.apply_async((filenm,))
    return jsonify({}), 202, {'Location': url_for('ems.taskstatus',
                                                  job_nm="bseb_dataupload_tsk",
                                                  task_id=task.id)}


@ems.route('/data_upload_task', methods=['POST'])
@roles_required(['admin', 'jobrunner', 'analytics'])
def data_upload_task(filenm, valid_discoms):
    current_app.logger.info('data_upload_task*** %s', filenm)
    conn_file = current_app.config['DB_CONNECT_FILE']
    task = data_upload_tsk.apply_async((filenm, valid_discoms, conn_file))
    return (jsonify({}),
            202,
            {'status_url': url_for('ems.taskstatus',
                                   job_nm="data_upload_tsk",
                                   task_id=task.id)})


@ems.route('/data_upload_task', methods=['POST'])
@roles_required(['admin', 'jobrunner', 'analytics'])
def tentativedata_upload_task(filenm, valid_discoms):
    current_app.logger.info('data_upload_task*** %s', filenm)
    conn_file = current_app.config['DB_CONNECT_FILE']
    task = tentativedata_upload_tsk.apply_async((filenm, valid_discoms,
                                                 conn_file))
    return (jsonify({}),
            202,
            {'status_url': url_for('ems.taskstatus',
                                   job_nm="tentativedata_upload_tsk",
                                   task_id=task.id)})
# @celery.task(bind=True)
# def bseb_weatherupload_tsk(self):
#     print "\n-- Retreiving Files----\n"
#     import weatherzeerone
#     import sql_load_lib
#     import weather_data_upload
#     import shutil
#     import datetime
#     import argparse
#     import glob
#     import re

#     message = ''
#     print "\n-- Retreiving Files----\n"
#     dsn = homedir + '/Projects/batch/config/sqldb_connection_config.txt'
#     filenm = homedir + "/Projects/bihar/weather/*.csv"
#     ldir = homedir + "/Projects/bihar/weather/"
#     archive = homedir + "/Projects/bihar/weather/archive/"
#     location = 'india/patna'
#     state = 'BIHAR'
#     loc = 'PATNA'
#     # total = len(glob.iglob(args.filenm)) + 1
#     startdate = datetime.datetime.now() + datetime.timedelta(-1)
#     enddate = datetime.datetime.now()
#     print "Starting Weather Download"
#     self.update_state(state='PROGRESS',
#               meta={'current': 0, 'total': 3,
#                     'status': message})
#     args = argparse.Namespace(alerts=False, dir=ldir, end_date=enddate,
#                     forecast=False, history=False, hourly=False,
#                     hourly10day=True, location=[location], metric=True,
#                     now=False, start_date=startdate, yesterday=False)
#     weatherzeerone.main(args)
#     self.update_state(state='PROGRESS',
#               meta={'current': 1, 'total': 3,
#                     'status': message})
#     args = argparse.Namespace(alerts=False, dir=ldir, end_date=enddate,
#                 forecast=False, history=True, hourly=False,
#                 hourly10day=False, location=[location], metric=True,
#                 now=False, start_date=startdate, yesterday=False)
#     weatherzeerone.main(args)
#     self.update_state(state='PROGRESS',
#               meta={'current': 2, 'total': 3,
#                     'status': message})
#     for filex in glob.iglob(filenm):
#         print filex
#         filenm = os.path.abspath(filex).replace("\\", "/")
#         print "###processing file: " + filenm
#         logger.info("###processing file: " + filenm)
#         basepath, filename = os.path.split(filenm)

#         if filename[:7] == 'Actuals':
#             data = (filenm, state.upper(), loc.upper())
#             # print data
#             sql_load_lib.sql_table_load_exec(dsn,
#                                              'actual_weather_staging',
#                                              data)
#             sql_load_lib.sql_wtr_actual_blk_ins_upd(dsn)
#         elif filename[:5] == 'fh10d':
#             loadtimestamp = re.findall(r"\d{2}-\d{2}-\d{8}", filename)[0]
#             loadtimestamp = datetime.datetime.strptime(loadtimestamp,
#                             '%d-%m-%Y%H%M').strftime('%Y-%m-%d %H:%M:%S')
#             data = (filenm, state.upper(),
#                     loc.upper(), loadtimestamp)
#             sql_load_lib.sql_table_load_exec(dsn,
#                                              'forecast_weather_staging',
#                                              data)
#             sql_load_lib.sql_wtr_forecast_blk_ins_upd(dsn)

#         newpath = os.path.join(archive, filename)
#         if os.path.exists(newpath):
#             os.remove(newpath)
#         shutil.move(filenm, archive)

#     self.update_state(state='PROGRESS',
#               meta={'current': 3, 'total': 3,
#                     'status': message})
#     return {'current': 100, 'total': 100, 'status': 'Task completed!',
#             'result': 'Done'}


@celery.task(bind=True)
def bseb_weatherupload_tsk(self, dsn):
    import ems.batch.bin.weatherzeerone as weatherzeerone
    import ems.batch.bin.sql_load_lib as sql_load_lib
    import ems.batch.bin.weather_data_upload as weather_data_upload
    # import datetime
    import argparse
    import glob
    # import re

    message = 'In Progress'
    logger.debug("bseb_weatherupload_tsk started")
    # dsn = homedir + '/Projects/batch/config/sqldb_connection_config.txt'
    # filenm = homedir + "/Projects/bihar/weather/*.csv"
    # ldir = homedir + "/Projects/bihar/weather/"
    # archive = homedir + "/Projects/bihar/weather/archive/"
    # dsn = "./ems/batch/config/sqldb_gcloud.txt"
    ldir = "./ems/batch/data/weather/"
    filenm = ldir + "*patna*.csv"
    archive = "./ems/batch/data_archive/weather/"
    location = 'india/patna'
    state = 'BIHAR'
    loc = 'PATNA'
    # total = len(glob.iglob(args.filenm)) + 1
    if time.tzname[0] == 'IST':
        local_now = datetime.today()
    else:
        dest_tz = pytz.timezone('Asia/Kolkata')
        ts = time.time()
        utc_now = datetime.utcfromtimestamp(ts)
        local_now = utc_now.replace(tzinfo=pytz.utc).astimezone(dest_tz)

    startdate = local_now + timedelta(-1)
    enddate = local_now

    logger.info("Starting Weather Download")
    self.update_state(state='PROGRESS',
                      meta={'current': 0, 'total': 3,
                            'status': message})
    args = argparse.Namespace(alerts=False, dir=ldir, end_date=enddate,
                              forecast=False, history=False, hourly=False,
                              hourly10day=True, location=[location],
                              metric=True, now=False, start_date=startdate,
                              yesterday=False)
    weatherzeerone.main(args)
    self.update_state(state='PROGRESS',
                      meta={'current': 1, 'total': 3,
                            'status': message})
    args = argparse.Namespace(alerts=False, dir=ldir, end_date=enddate,
                              forecast=False, history=True, hourly=False,
                              hourly10day=False, location=[location],
                              metric=True, now=False, start_date=startdate,
                              yesterday=False)
    weatherzeerone.main(args)
    self.update_state(state='PROGRESS',
                      meta={'current': 2, 'total': 3,
                            'status': message})
    for filex in glob.iglob(filenm):
        logger.info(filex)
        filenm = os.path.abspath(filex).replace("\\", "/")
        logger.info("### processing file: %s", filenm)
        basepath, filename = os.path.split(filenm)

        if filename[:7] == 'Actuals':
            data = weather_data_upload.weather_actual_load(filenm, filename,
                                                           state, loc)
            sql_load_lib.sql_table_insert_exec(dsn,
                                               'actual_weather_staging',
                                               data)
            sql_load_lib.sql_wtr_actual_blk_ins_upd(dsn)
        elif filename[:5] == 'fh10d':
            data = weather_data_upload.weather_forecast_load(filenm, filename,
                                                             state, loc)
            sql_load_lib.sql_table_insert_exec(dsn,
                                               'forecast_weather_staging',
                                               data)
            sql_load_lib.sql_wtr_forecast_blk_ins_upd(dsn)

        newpath = os.path.join(archive, filename)
        if os.path.exists(newpath):
            os.remove(newpath)
        shutil.move(filenm, archive)

    self.update_state(state='PROGRESS',
                      meta={'current': 3, 'total': 3,
                            'status': message})
    return {'current': 100, 'total': 100, 'status': 'Task completed!',
            'result': 'Done'}


@retry(wait_fixed=1000, stop_max_attempt_number=3)
def lgetweatheractual(ldir, startdate, enddate, dsn, state, discom):
    import ems.batch.bin.imdaws_crawl_v3 as imdaws_crawl_v3
    import argparse
    try:
        args = argparse.Namespace(
            alerts=False, dir=ldir, filenm='IMDAWS',
            start_date=startdate.strftime("%d-%m-%Y"),
            end_date=enddate.strftime("%d-%m-%Y"),
            table='imdaws_weather_stg',
            dsn=dsn, state=state, allstate=False,
            discom=discom)
        imdaws_crawl_v3.main(args)
    except Exception as e:
        logger.info("Exception: %s", e)


@retry(wait_fixed=1000, stop_max_attempt_number=3)
def lgetweatherforecast(ldir, enddate, locname):
    import ems.batch.bin.weatherzeerone as weatherzeerone
    import argparse
    try:
        args = argparse.Namespace(alerts=False, dir=ldir, end_date=enddate,
                                  forecast=False, history=False,
                                  hourly=False, hourly10day=True,
                                  location=[locname], metric=True,
                                  now=False, start_date=False,
                                  yesterday=False)
        weatherzeerone.main(args)
    except Exception as e:
        logger.info("Exception: %s", e)


@celery.task(bind=True)
def upcl_weatherupload_tsk(self, dsn, discom, state):
    logger.debug("Starting upcl_weatherupload_tsk", discom, state)
    # import batch.bin.weatherzeerone as weatherzeerone
    # import batch.bin.sql_load_lib as sql_load_lib
    # import ems.batch.bin.weather_data_upload as weather_data_upload
    # import batch.bin.imdaws_crawl_v3 as imdaws_crawl_v3
    import ems.batch.bin.actualweather_wu as actualweather_wu
    import ems.batch.bin.forecastweather_wu as forecastweather_wu
    import ems.batch.bin.sql_load_lib as sql_load_lib
    # import shutil
    from datetime import timedelta, datetime
    import argparse
    # import glob
    # import re

    message = 'In Progress'
    # dsn = homedir + '/Projects/batch/config/sqldb_connection_config.txt'
    # filenm = homedir + "/Projects/bihar/weather/*.csv"
    # ldir = homedir + "/Projects/bihar/weather/"
    # archive = homedir + "/Projects/bihar/weather/archive/"
    # dsn = "./ems/batch/config/sqldb_gcloud.txt"
    # ldir = "./ems/batch/data/weather/"
    # adir = "./ems/batch/data_archive/weather/"
    # filenm = ["*almora*", "*bazpur*", "*bhagwanpur*", "*bhowali*",
    #           "*dehradun*", "*tehri*", "*herbertpur*", "*haldwani*",
    #           "*haridwar*", "*jaspur*", "*jwalapur*", "*kashipur*",
    #           "*kathgodam*", "*kichha*", "*kotdwara*", "*laksar*",
    #           "*majra*", "*pantnagar*", "*pithoragarh*", "*ranikhet*",
    #           "*rishikesh*", "*roorkee*", "*satpuli*", "*simli*",
    #           "*sitarganj*", "*barkot*", "*karnaprayag*", "*chamoli*",
    #           "*joshimath*", "*lalkua*", "*bageshwar*", "*lohaghat*",
    #           "*uttarkashi*", "*gangotri*", "*yamnotri*"]
    # archive = "./ems/batch/data_archive/weather/"
    # location = ['locid:INUL1018', 'locid:INXX2297', 'india/uttarkashi',
    #             'india/lohaghat', 'india/bageshwar', 'locid:INUL0604',
    #             'india/joshimath', 'india/chamoli', 'locid:INUL0503',
    #             'india/barkot', 'india/sitarganj', 'locid:INUL0922',
    #             'locid:INUL0883', 'india/roorkee', 'india/rishikesh',
    #             'india/ranikhet', 'india/pithoragarh', 'india/pantnagar',
    #             'locid:INHP0350', 'india/laksar', 'india/kotdwara',
    #             'india/kichha', 'locid:INUL0509', 'india/kashipur',
    #             'locid:INXX2927', 'india/jaspur', 'india/haridwar',
    #             'india/haldwani', 'india/herbertpur', 'india/tehri',
    #             'india/dehradun', 'india/bhowali', 'locid:INHR0151',
    #             'india/bazpur', 'india/almora']
    # state = 'UTTARAKHAND'

    # if time.tzname[0] == 'IST':
    #     local_now = datetime.today()
    # else:
    #     dest_tz = pytz.timezone('Asia/Kolkata')
    #     ts = time.time()
    #     utc_now = datetime.utcfromtimestamp(ts)
    #     local_now = utc_now.replace(tzinfo=pytz.utc).astimezone(dest_tz)

    # startdate = local_now + timedelta(-2)
    # enddate = local_now

    startdate = datetime.utcnow() + timedelta(-2)
    enddate = datetime.utcnow() + timedelta(1)

    # i = 0
    # total = (len(location) * 4) + 2
    # self.update_state(state='PROGRESS', meta={'current': i, 'total': total,
    #                                           'status': message})
    # for locname in location:
    #     lgetweatherforecast(ldir, enddate, locname)
    #     i += 1
    #     self.update_state(state='PROGRESS', meta={'current': i, 'total': total,
    #                                               'status': message})
    #     # commented out as key has been bought
    #     # To not overpass the api set limit of 10 hits per min
    #     # time.sleep(8)

    # try:
    #     newfilenm = ["".join([ldir, file, ".csv"]) for file in filenm]
    #     args = argparse.Namespace(alerts=False, dir=archive, filenm=newfilenm,
    #                               dsn=dsn, state=state, location=None)
    #     weather_data_upload.main(args)
    # except Exception, e:
    #     logger.info("Exception: %s", e)
    # i += len(newfilenm)
    # self.update_state(state='PROGRESS', meta={'current': i, 'total': total,
    #                                           'status': message})
    # logger.info("Finished Weather Upload")
    # # IMDAWS data not coming from 1st Jun 2017
    # # logger.info("Started IMDAWS Upload")
    # # lgetweatheractual(ldir, startdate, enddate, dsn, state, discom)
    # # logger.info("Finished IMDAWS Upload")
    i = 1
    total = 4
    self.update_state(state='PROGRESS', meta={'current': i, 'total': total,
                                              'status': message})
    i = i + 1
    self.update_state(state='PROGRESS', meta={'current': i, 'total': total,
                                              'status': message})
    logger.info("Started IBMWEATHER Forecast Upload")
    try:
        loc = forecastweather_wu.DbFetchLocations(dsn, state)
        all_loc = loc.fetch_locations()
        # forecast_type = ['hourly', '15min']
        forecast_type = ['hourly']
        for loc, lat, lng in all_loc:
            # print( loc, lat, lon)
            for f_type in forecast_type:
                forecast = forecastweather_wu.ForecastData()
                forecast.fetch_data(lat, lng, f_type, 'm')
                data = forecast.parse_forecast2(loc, f_type)
                dbupdate = forecastweather_wu.DbUploadData(dsn, data=data)
                dbupdate.db_upload_data2()
    except Exception as e:
        logger.info("Exception: %s", e)
    self.update_state(state='PROGRESS', meta={'current': i + 1, 'total': total,
                                              'status': message})
    # try:
    #     sql_load_lib.sql_sp_wtr_ibm_forecast_hrblk_ins_upd(dsn, state)
    # except Exception as  e:
    #     logger.info("Exceptions sql_sp_wtr_ibm_forecast_hrblk_ins_upd: %s", e)
    logger.info("FInished IBMWEATHER Forecast Upload")
    i = i + 1
    self.update_state(state='PROGRESS', meta={'current': i, 'total': total,
                                              'status': message})
    logger.info("Started IBMWEATHER Actual Upload")
    try:
        for date in actualweather_wu.daterange(startdate, enddate):
            startdt = date.strftime('%Y%m%d%H%M')
            if  date + timedelta(1) < datetime.utcnow() - timedelta(hours=3):
                enddt = (date + timedelta(1)).strftime('%Y%m%d%H%M')
            elif date < datetime.utcnow() - timedelta(hours=3):
                enddt = (datetime.utcnow() - timedelta(hours=3)).strftime('%Y%m%d%H%M')
            else:
                break
            logger.info("startdt {} and enddt {}".format(startdt, enddt))
            # startdt = date.strftime('%m/%d/%Y')
            # enddt = (date + timedelta(1)).strftime('%m/%d/%Y')
            # loc = actualweather_wu.DbFetchLocations(dsn, state)
            # for loc, lat, lon in loc.fetch_locations():
                # actuals = actualweather_wu.ActualData()
                # actuals.fetch_data(lat, lon, startdt, enddt)
                # filename = actuals.save_file(ldir, loc + '_' +
                #                              date.strftime('%d-%m-%Y'))
                # dbupdate = actualweather_wu.DbUploadData(dsn, filename)
                # dbupdate.csv_to_data(['Location'], [loc])
                # dbupdate.db_upload_data()
                # dst_file = os.path.join(adir, loc + '_' +
                #                         date.strftime('%d-%m-%Y') + '.csv')
                # if os.path.exists(dst_file):
                #     os.remove(dst_file)
                # shutil.move(filename, adir)
            loc = actualweather_wu.DbFetchLocations(dsn, state)
            for loc, lat, lng in loc.fetch_locations():
                # print((loc, lat, lng))
                actuals = actualweather_wu.ActualData()
                actuals.fetch_data(lat, lng, startdt, enddt)
                data = actuals.parse_actual(loc)
                dbupdate = actualweather_wu.DbUploadData(dsn, data=data)
                dbupdate.db_upload_data2()
    except Exception as e:
        logger.info("Exception: %s", e)
    # sql_load_lib.sql_sp_wtr_ibm_actualhrblk_ins_upd(dsn, state)
    logger.info("Finished IBMWEATHER Actual Upload")
    self.update_state(state='PROGRESS', meta={'current': i + 1, 'total': total,
                                              'status': message})
    # try:
    #     sql_load_lib.sql_sp_wtr_unified_ins_upd_v2(dsn, discom)
    # except Exception as e:
    #     logger.info("Exception sql_sp_wtr_unified_ins_upd_v2: %s", e)
    try:
        sql_load_lib.sql_sp_wtr_unified2_ins_upd(dsn, discom, state)
    except Exception as e:
        logger.info("Exceptions sql_sp_wtr_unified2_ins_upd: %s", e)
    logger.info("Finished IBMWEATHER Actual Upload")
    return {'current': total, 'total': total, 'status': 'Task completed!',
            'result': 'Done'}


@celery.task(bind=True)
def guvnl_weatherupload_tsk(self, dsn, discom):
    logger.debug("Starting guvnl_weatherupload_tsk")
    import ems.batch.bin.actualweather_wu as actualweather_wu
    import ems.batch.bin.forecastweather_wu as forecastweather_wu
    import ems.batch.bin.sql_load_lib as sql_load_lib
    # import argparse

    message = 'In Progress'
    ldir = "./ems/batch/data/weather/"
    adir = "./ems/batch/data_archive/weather/"
    logger.info("Started IBMWEATHER Forecast Upload")
    state = 'GUJARAT'
    if time.tzname[0] == 'IST':
        local_now = datetime.today()
    else:
        dest_tz = pytz.timezone('Asia/Kolkata')
        ts = time.time()
        utc_now = datetime.utcfromtimestamp(ts)
        local_now = utc_now.replace(tzinfo=pytz.utc).astimezone(dest_tz)

    startdate = local_now + timedelta(-2)
    enddate = local_now

    logger.info("StartDate {} EndDate {}".
                format(startdate.strftime("%d-%m-%Y"),
                       enddate.strftime("%d-%m-%Y")))
    total = 4
    self.update_state(state='PROGRESS', meta={'current': 0, 'total': total,
                                              'status': message})
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
    self.update_state(state='PROGRESS', meta={'current': 2, 'total': total,
                                              'status': message})
    logger.info("Started IBMWEATHER Actual Upload")
    # try:
    #     args = argparse.Namespace(
    #         alerts=False, dir=ldir,
    #         start_date=startdate.strftime("%d-%m-%Y"),
    #         end_date=enddate.strftime("%d-%m-%Y"),
    #         dsn=dsn, state=state, arc=adir)
    #     actualweather_wu.main(args)
    #     sql_load_lib.sql_sp_wtr_ibm_actualhrblk_ins_upd(dsn, state)
    # except Exception, e:
    #     logger.info("Exception: %s", e)
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
    self.update_state(state='PROGRESS', meta={'current': 3, 'total': total,
                                              'status': message})
    sql_load_lib.sql_sp_wtr_ibm_actualhrblk_ins_upd(dsn, state)
    logger.info("Finished IBMWEATHER Actual Upload")
    # try:
    #     logger.info("Started IMDAWS Upload")
    #     lgetweatheractual(ldir, startdate, enddate, dsn, state, discom)
    #     self.update_state(state='PROGRESS', meta={'current': 3, 'total': total,
    #                                               'status': message})
    #     logger.info("Finished IMDAWS Upload")
    # except Exception, e:
    #     logger.info("Exception: %s", e)
    try:
        sql_load_lib.sql_sp_wtr_unified_ins_upd_v2(dsn, discom)
    except Exception as e:
        logger.info("Exception sql_sp_wtr_unified_ins_upd_v2: %s", e)
    try:
        sql_load_lib.sql_sp_wtr_unified2_ins_upd(dsn, state)
    except Exception as e:
        logger.info("Exceptions sql_sp_wtr_unified2_ins_upd: %s", e)
    logger.info("Finished IBMWEATHER Actual Upload")
    return {'current': total, 'total': total, 'status': 'Task completed!',
            'result': 'Done'}


@celery.task(bind=True)
def adani_weatherupload_tsk(self, dsn, discom):
    logger.debug("Starting guvnl_weatherupload_tsk")
    import ems.batch.bin.actualweather_wu as actualweather_wu
    import ems.batch.bin.forecastweather_wu as forecastweather_wu
    import ems.batch.bin.sql_load_lib as sql_load_lib
    # import argparse

    message = 'In Progress'
    ldir = "./ems/batch/data/weather/"
    adir = "./ems/batch/data_archive/weather/"
    logger.info("Started IBMWEATHER Forecast Upload")
    state = 'TAMIL NADU'
    if time.tzname[0] == 'IST':
        local_now = datetime.today()
    else:
        dest_tz = pytz.timezone('Asia/Kolkata')
        ts = time.time()
        utc_now = datetime.utcfromtimestamp(ts)
        local_now = utc_now.replace(tzinfo=pytz.utc).astimezone(dest_tz)

    startdate = local_now + timedelta(-2)
    enddate = local_now
    logger.info("StartDate {} EndDate {}".
                format(startdate.strftime("%d-%m-%Y"),
                       enddate.strftime("%d-%m-%Y")))
    total = 4
    self.update_state(state='PROGRESS', meta={'current': 0, 'total': total,
                                              'status': message})
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
    self.update_state(state='PROGRESS', meta={'current': 2, 'total': total,
                                              'status': message})
    logger.info("Started IBMWEATHER Actual Upload")
    # try:
    #     args = argparse.Namespace(
    #         alerts=False, dir=ldir,
    #         start_date=startdate.strftime("%d-%m-%Y"),
    #         end_date=enddate.strftime("%d-%m-%Y"),
    #         dsn=dsn, state=state, arc=adir)
    #     actualweather_wu.main(args)
    #     sql_load_lib.sql_sp_wtr_ibm_actualhrblk_ins_upd(dsn, state)
    # except Exception, e:
    #     logger.info("Exception: %s", e)
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
    self.update_state(state='PROGRESS', meta={'current': 3, 'total': total,
                                              'status': message})
    sql_load_lib.sql_sp_wtr_ibm_actualhrblk_ins_upd(dsn, state)
    logger.info("Finished IBMWEATHER Actual Upload")
    # try:
    #     logger.info("Started IMDAWS Upload")
    #     lgetweatheractual(ldir, startdate, enddate, dsn, state, discom)
    #     self.update_state(state='PROGRESS', meta={'current': 3, 'total': total,
    #                                               'status': message})
    #     logger.info("Finished IMDAWS Upload")
    # except Exception, e:
    #     logger.info("Exception: %s", e)
    try:
        sql_load_lib.sql_sp_wtr_unified_ins_upd_v2(dsn, discom)
    except Exception as e:
        logger.info("Exception sql_sp_wtr_unified_ins_upd_v2: %s", e)
    try:
        sql_load_lib.sql_sp_wtr_unified2_ins_upd(dsn, discom)
    except Exception as e:
        logger.info("Exceptions sql_sp_wtr_unified2_ins_upd: %s", e)
    logger.info("Finished IBMWEATHER Actual Upload")
    return {'current': total, 'total': total, 'status': 'Task completed!',
            'result': 'Done'}


@ems.route('/weatherupload_task/<discom>', methods=['POST'])
@roles_required(['admin', 'jobrunner', 'analytics'])
def weatherupload_task(discom):
    current_app.logger.info("Inside weatherupload_task for %s", discom)
    auth_discom = get_org()
    current_app.logger.debug("Valid Discoms %s", auth_discom.get('org_name'))
    func_dict = {'BSEB': bseb_weatherupload_tsk,
                 'UPCL': upcl_weatherupload_tsk,
                 'GUVNL': guvnl_weatherupload_tsk,
                 'ADANI': adani_weatherupload_tsk}
    conn_file = current_app.config['DB_CONNECT_FILE']
    if discom in auth_discom.get('org_name', None):
        state_name = auth_discom.get('state_name', None)
        task = func_dict.get(discom).apply_async((conn_file, discom, state_name))
        return jsonify({}), 202, {'Location': url_for('ems.taskstatus',
                                  job_nm=func_dict.get(discom).__name__,
                                  task_id=task.id)}
    else:
        return jsonify({}), 404
    # if discom in auth_disom:
    #     if discom == 'BSEB':
    #         logging.info("Inside weatherupload_task for BSEB")
    #         task = bseb_weatherupload_tsk.apply_async()
    #         return jsonify({}), 202, {'Location': url_for('ems.taskstatus', job_nm="bseb_weatherupload_tsk", task_id=task.id)}
    #     elif discom == 'UPCL':
    #         logging.info("Inside weatherupload_task for UPCL")
    #         task = bseb_weatherupload_tsk.apply_async()
    #         return jsonify({}), 202, {'Location': url_for('ems.taskstatus', job_nm="bseb_weatherupload_tsk", task_id=task.id)}

# @celery.task(bind=True)
# def bseb_rforecast_tsk(self):
#     import rpy2.robjects as robjects
#     message = ''
#     self.update_state(state='PROGRESS',
#               meta={'current': 0, 'total': 3,
#                     'status': message})
#     r = robjects.r
#     r.source(homedir + "/Projects/batch/analytics/rcode/BIHAR_Scoring_Data_Preparation_final.R")
#     self.update_state(state='PROGRESS',
#               meta={'current': 2, 'total': 3,
#                     'status': message})
#     sql ="""INSERT INTO power.forecast_stg
#             (State,
#             Date,
#             Block_No,
#             Discom_Name,
#             Demand_Forecast
#             )
#             select 'BIHAR' as state, date, block_no, 'BPDCL' as discom,
#             bias_corrected_forecast as Demand_Forecast
#             from power.R_TB_DayAhead_Forecast_Report
#             ON DUPLICATE KEY
#             UPDATE Demand_Forecast = VALUES(Demand_Forecast)"""
#     datacursor = con.cursor(cursors.DictCursor)
#     datacursor.execute(sql)
#     con.commit()
#     self.update_state(state='PROGRESS',
#               meta={'current': 3, 'total': 3,
#                     'status': message})
#     return {'current': 100, 'total': 100, 'status': 'Task completed!',
#             'result': 'Done'}


# @celery.task(bind=True)
# def bseb_rforecast_tsk2(self, model):
#     print "\n-- Starting rforecast----\n"
#     import rpy2.robjects as robjects

#     message = ''

#     # total = len(glob.iglob(args.filenm)) + 1

#     self.update_state(state='PROGRESS',
#               meta={'current': 0, 'total': 3,
#                     'status': message})
#     print "Initializing R", model
#     r = robjects.r
#     self.update_state(state='PROGRESS',
#               meta={'current': 1, 'total': 3,
#                     'status': message})
#     if model == 'NNET':
#         r.source(homedir + "/Projects/batch/analytics/rcode/BIHAR_NNET_Scoring_Final.R")
#         self.update_state(state='PROGRESS',
#                   meta={'current': 2, 'total': 3,
#                         'status': message})
#         sql ="""INSERT INTO power.forecast_stg
#                 (State,
#                 Date,
#                 Block_No,
#                 Discom_Name,
#                 Model_Name,
#                 Demand_Forecast
#                 )
#                 select 'BIHAR' as state, date, block_no, 'BPDCL' as discom,
#                 'NNET' as station,
#                 TB_NNET_Bias_corrected_forecast as Demand_Forecast
#                 from power.R_TB_DayAhead_NNET_Forecast_Report
#                 ON DUPLICATE KEY
#                 UPDATE Demand_Forecast = VALUES(Demand_Forecast)"""
#         datacursor = con.cursor(cursors.DictCursor)
#         datacursor.execute(sql)
#         con.commit()
#     elif model == 'GLM':
#         r.source(homedir + "/Projects/batch/analytics/rcode/BIHAR_GLMBOOST_Scoring_Final.R")
#         self.update_state(state='PROGRESS',
#                   meta={'current': 2, 'total': 3,
#                         'status': message})
#         sql ="""INSERT INTO power.forecast_stg
#                 (State,
#                 Date,
#                 Block_No,
#                 Discom_Name,
#                 Model_Name,
#                 Demand_Forecast
#                 )
#                 select 'BIHAR' as state, date, block_no, 'BPDCL' as discom,
#                 'GLM' as station,
#                 bias_corrected_forecast as Demand_Forecast
#                 from power.R_TB_DayAhead_GLM_Forecast_Report
#                 ON DUPLICATE KEY
#                 UPDATE Demand_Forecast = VALUES(Demand_Forecast)"""
#         datacursor = con.cursor(cursors.DictCursor)
#         datacursor.execute(sql)
#         con.commit()
#     elif model == 'MLP':
#         r.source(homedir + "/Projects/batch/analytics/rcode/BIHAR_MLP_Scoring_Final.R")
#         self.update_state(state='PROGRESS',
#                   meta={'current': 2, 'total': 3,
#                         'status': message})
#         sql ="""INSERT INTO power.forecast_stg
#                 (State,
#                 Date,
#                 Block_No,
#                 Discom_Name,
#                 Model_Name,
#                 Demand_Forecast
#                 )
#                 select 'BIHAR' as state, date, block_no, 'BPDCL' as discom,
#                 'MLP' as station,
#                 TB_MLP_Bias_corrected_forecast as Demand_Forecast
#                 from power.R_TB_DayAhead_MLP_Forecast_Report
#                 ON DUPLICATE KEY
#                 UPDATE Demand_Forecast = VALUES(Demand_Forecast)"""
#         datacursor = con.cursor(cursors.DictCursor)
#         datacursor.execute(sql)
#         con.commit()
#     self.update_state(state='PROGRESS',
#               meta={'current': 3, 'total': 3,
#                     'status': message})
#     return {'current': 100, 'total': 100, 'status': 'Task completed!',
#             'result': 'Done'}


# @ems.route('/bseb_rforecast_task/<model>', methods=['POST'])
# @login_required
# def bseb_rforecast_task(model):
#     task = bseb_rforecast_tsk2.apply_async((model,))
#     return jsonify({}), 202, {'Location': url_for('ems.taskstatus',
#                               job_nm="bseb_rforecast_tsk2", task_id=task.id)}


@celery.task(bind=True)
def forecast_tsk(self, discom, model, db_uri, scoring, area, state):
    logger.info("forecast_tsk started")
    logger.info("%s %s %s", discom, model, scoring)
    message = ''
    self.update_state(state='PROGRESS',
                      meta={'current': 0, 'total': 3,
                            'status': message})
    # if discom == 'UPCL':
    #     if model == 'NEAREST_NEIGHBOUR':
    #         self.update_state(state='PROGRESS',
    #                           meta={'current': 1, 'total': 3,
    #                                 'status': message})
    #         # import batch.bin.unified_weather_actual_forecast
    #         from batch.bin.analytics.data_prep_forecast_unified_weather\
    #             import data_prep_forecast_uw
    #         logger.debug("db_uri: %s", db_uri)
    #         data_prep_forecast_uw(db_uri, discom)
    #         self.update_state(state='PROGRESS',
    #                           meta={'current': 2, 'total': 3,
    #                                 'status': message})
    #         # import batch.bin.forecast_data_prep
    #         from batch.bin.analytics.forecast_nn import forecast_nn
    #         forecast_nn(db_uri)
    #     elif model == 'GRNN':
    #         self.update_state(state='PROGRESS',
    #                           meta={'current': 1, 'total': 2,
    #                                 'status': message})
    #         from batch.bin.analytics.forecast_grnn import forecast_grnn
    #         forecast_grnn(db_uri)
    #     elif model == 'SVR' and scoring == 'true':
    #         self.update_state(state='PROGRESS',
    #                           meta={'current': 1, 'total': 2,
    #                                 'status': message})
    #         from batch.bin.analytics.forecast_svr_scoring \
    #             import forecast_svr_scoring
    #         forecast_svr_scoring(db_uri)
    #     elif model == 'SVR' and scoring == 'false':
    #         self.update_state(state='PROGRESS',
    #                           meta={'current': 1, 'total': 2,
    #                                 'status': message})
    #         from batch.bin.analytics.forecast_svr_training \
    #             import forecast_svr_training
    #         forecast_svr_training(db_uri)
    #     elif model == 'DLN' and scoring == 'true':
    #         self.update_state(state='PROGRESS',
    #                           meta={'current': 1, 'total': 2,
    #                                 'status': message})
    #         from batch.bin.analytics.forecast_dln_scoring \
    #             import forecast_dln_scoring
    #         forecast_dln_scoring(db_uri)
    #     elif model == 'DLN' and scoring == 'false':
    #         self.update_state(state='PROGRESS',
    #                           meta={'current': 1, 'total': 2,
    #                                 'status': message})
    #         from batch.bin.analytics.forecast_dln_training \
    #             import forecast_dln_training
    #         forecast_dln_training(db_uri)
    #     elif model == 'MLP' and scoring == 'true':
    #         self.update_state(state='PROGRESS',
    #                           meta={'current': 1, 'total': 2,
    #                                 'status': message})
    #         from batch.bin.analytics.forecast_mlp_scoring \
    #             import forecast_mlp_scoring
    #         forecast_mlp_scoring(db_uri)
    #     elif model == 'MLP' and scoring == 'false':
    #         self.update_state(state='PROGRESS',
    #                           meta={'current': 1, 'total': 2,
    #                                 'status': message})
    #         from batch.bin.analytics.forecast_mlp_training \
    #             import forecast_mlp_training
    #         forecast_mlp_training(db_uri)
    #     elif model == 'HYBRID':
    #         self.update_state(state='PROGRESS',
    #                           meta={'current': 1, 'total': 2,
    #                                 'status': message})
    #         from batch.bin.analytics.forecast_hybrid import forecast_hybrid
    #         forecast_hybrid(db_uri)
    #     elif model == 'HYBRID_DLN':
    #         self.update_state(state='PROGRESS',
    #                           meta={'current': 1, 'total': 2,
    #                                 'status': message})
    #     elif model == 'PRICESIM':
    #         self.update_state(state='PROGRESS',
    #                           meta={'current': 1, 'total': 2,
    #                                 'status': message})
    #         from batch.bin.analytics.price_simulator import price_forecast
    #         price_forecast(db_uri, area)
    # elif discom == 'GUVNL':
    #     if model == 'NEAREST_NEIGHBOUR':
    #         self.update_state(state='PROGRESS',
    #                           meta={'current': 1, 'total': 3,
    #                                 'status': 'Starting Data Prep...'})
    #         # import batch.bin.unified_weather_actual_forecast
    #         from batch.bin.analytics.data_prep_forecast_unified_weather\
    #             import data_prep_forecast_uw
    #         logger.debug("db_uri: %s", db_uri)
    #         data_prep_forecast_uw(db_uri, discom)
    #         self.update_state(state='PROGRESS',
    #                           meta={'current': 2, 'total': 3,
    #                                 'status': 'Finished Data Prep...'})
    #         # import batch.bin.forecast_data_prep
    #         from batch.bin.analytics.forecast_nn_guvnl import forecast_nn_guvnl
    #         forecast_nn_guvnl(db_uri, discom)
    #     # elif model == 'GRNN':
    #     #     self.update_state(state='PROGRESS',
    #     #                       meta={'current': 1, 'total': 2,
    #     #                             'status': message})
    #     #     # from batch.bin.analytics.forecast_grnn import forecast_grnn
    #     #     # forecast_grnn(db_uri)
    #     elif model == 'SVR' and scoring == 'true':
    #         self.update_state(state='PROGRESS',
    #                           meta={'current': 1, 'total': 2,
    #                                 'status': message})
    #         from batch.bin.analytics.forecast_svr_scoring_guvnl \
    #             import forecast_svr_scoring_guvnl
    #         forecast_svr_scoring_guvnl(db_uri, discom)
    #     elif model == 'SVR' and scoring == 'false':
    #         self.update_state(state='PROGRESS',
    #                           meta={'current': 1, 'total': 2,
    #                                 'status': message})
    #         from batch.bin.analytics.forecast_svr_training_guvnl \
    #             import forecast_svr_training_guvnl
    #         forecast_svr_training_guvnl(db_uri, discom)
    #     elif model == 'DLN' and scoring == 'true':
    #         self.update_state(state='PROGRESS',
    #                           meta={'current': 1, 'total': 2,
    #                                 'status': message})
    #         from batch.bin.analytics.forecast_dln_scoring_guvnl \
    #             import forecast_dln_scoring_guvnl
    #         forecast_dln_scoring_guvnl(db_uri, discom)
    #     elif model == 'DLN' and scoring == 'false':
    #         self.update_state(state='PROGRESS',
    #                           meta={'current': 1, 'total': 2,
    #                                 'status': message})
    #         from batch.bin.analytics.forecast_dln_training_guvnl \
    #             import forecast_dln_training_guvnl
    #         forecast_dln_training_guvnl(db_uri, discom)
    #     elif model == 'MLP' and scoring == 'false':
    #         self.update_state(state='PROGRESS',
    #                           meta={'current': 1, 'total': 2,
    #                                 'status': message})
    #         from batch.bin.analytics.forecast_mlp_training_guvnl \
    #             import forecast_mlp_training_guvnl
    #         forecast_mlp_training_guvnl(db_uri, discom)
    #     elif model == 'MLP' and scoring == 'true':
    #         self.update_state(state='PROGRESS',
    #                           meta={'current': 1, 'total': 2,
    #                                 'status': message})
    #         from batch.bin.analytics.forecast_mlp_scoring_guvnl \
    #             import forecast_mlp_scoring_guvnl
    #         forecast_mlp_scoring_guvnl(db_uri, discom)
    #     elif model == 'HYBRID':
    #         self.update_state(state='PROGRESS',
    #                           meta={'current': 1, 'total': 2,
    #                                 'status': message})
    #         from batch.bin.analytics.forecast_hybrid_guvnl \
    #             import forecast_hybrid_guvnl
    #         forecast_hybrid_guvnl(db_uri, discom)
    #     elif model == 'HYBRID_DLN':
    #         self.update_state(state='PROGRESS',
    #                           meta={'current': 1, 'total': 2,
    #                                 'status': message})
    #         from batch.bin.analytics.forecast_hybriddln_guvnl \
    #             import forecast_hybriddln_guvnl
    #         forecast_hybriddln_guvnl(db_uri, discom)
    #     elif model == 'PRICESIM':
    #         self.update_state(state='PROGRESS',
    #                           meta={'current': 1, 'total': 2,
    #                                 'status': message})
    #         from batch.bin.analytics.price_simulator import price_forecast
    #         price_forecast(db_uri, area)
    #     self.update_state(state='PROGRESS',
    #                       meta={'current': 3, 'total': 3,
    #                             'status': message})
    if model == 'NEAREST_NEIGHBOUR':
        self.update_state(state='PROGRESS',
                          meta={'current': 1, 'total': 2,
                                'status': message})
        # import batch.bin.unified_weather_actual_forecast
        from ems.batch.bin.analytics.data_prep_forecast_nn\
            import data_prep_forecast_nn
        logger.debug("db_uri: %s", db_uri)
        data_prep_forecast_nn(db_uri, discom, state)
        # self.update_state(state='PROGRESS',
        #                   meta={'current': 2, 'total': 3,
        #                         'status': 'Finished Data Prep...'})
        # # import batch.bin.forecast_data_prep
        # from batch.bin.analytics.forecast_nn_guvnl import forecast_nn_guvnl
        # forecast_nn_guvnl(db_uri, discom, state)
    # elif model == 'GRNN':
    #     self.update_state(state='PROGRESS',
    #                       meta={'current': 1, 'total': 2,
    #                             'status': message})
    #     # from batch.bin.analytics.forecast_grnn import forecast_grnn
    #     # forecast_grnn(db_uri)
    elif model == 'HYBRID_KNN':
        self.update_state(state='PROGRESS',
                          meta={'current': 1, 'total': 2,
                                'status': message})
        # import batch.bin.unified_weather_actual_forecast
        from ems.batch.bin.analytics.forecast_hybrid_knn\
            import forecast_hybrid_knn
        logger.debug("db_uri: %s", db_uri)
        forecast_hybrid_knn(db_uri, discom, state)
    elif model == 'SVR' and scoring == 'true':
        self.update_state(state='PROGRESS',
                          meta={'current': 1, 'total': 2,
                                'status': message})
        from ems.batch.bin.analytics.forecast_svr_scoring_guvnl \
            import forecast_svr_scoring_guvnl
        forecast_svr_scoring_guvnl(db_uri, discom, state)
    elif model == 'SVR' and scoring == 'false':
        self.update_state(state='PROGRESS',
                          meta={'current': 1, 'total': 2,
                                'status': message})
        from ems.batch.bin.analytics.forecast_svr_training_guvnl \
            import forecast_svr_training_guvnl
        forecast_svr_training_guvnl(db_uri, discom)
    elif model == 'DLN' and scoring == 'true':
        self.update_state(state='PROGRESS',
                          meta={'current': 1, 'total': 2,
                                'status': message})
        from ems.batch.bin.analytics.forecast_dln_scoring_guvnl \
            import forecast_dln_scoring_guvnl
        forecast_dln_scoring_guvnl(db_uri, discom, state)
    elif model == 'DLN' and scoring == 'false':
        self.update_state(state='PROGRESS',
                          meta={'current': 1, 'total': 2,
                                'status': message})
        from ems.batch.bin.analytics.forecast_dln_training_guvnl \
            import forecast_dln_training_guvnl
        forecast_dln_training_guvnl(db_uri, discom)
    elif model == 'MLP' and scoring == 'false':
        self.update_state(state='PROGRESS',
                          meta={'current': 1, 'total': 2,
                                'status': message})
        from ems.batch.bin.analytics.forecast_mlp_training_guvnl \
            import forecast_mlp_training_guvnl
        forecast_mlp_training_guvnl(db_uri, discom)
    elif model == 'MLP' and scoring == 'true':
        self.update_state(state='PROGRESS',
                          meta={'current': 1, 'total': 2,
                                'status': message})
        from ems.batch.bin.analytics.forecast_mlp_scoring_guvnl \
            import forecast_mlp_scoring_guvnl
        forecast_mlp_scoring_guvnl(db_uri, discom, state)
    elif model == 'HYBRID':
        self.update_state(state='PROGRESS',
                          meta={'current': 1, 'total': 2,
                                'status': message})
        from ems.batch.bin.analytics.forecast_hybrid_guvnl \
            import forecast_hybrid_guvnl
        forecast_hybrid_guvnl(db_uri, discom, state)
    elif model == 'HYBRID_DLN':
        self.update_state(state='PROGRESS',
                          meta={'current': 1, 'total': 2,
                                'status': message})
        from ems.batch.bin.analytics.forecast_hybriddln_guvnl \
            import forecast_hybriddln_guvnl
        forecast_hybriddln_guvnl(db_uri, discom, state)
    elif model == 'GDBOOST':
        self.update_state(state='PROGRESS',
                          meta={'current': 1, 'total': 2,
                                'status': message})
        from ems.batch.bin.analytics.forecast_gbdt \
            import forecast_gbdt
        forecast_gbdt(db_uri, discom, state)        
    elif model == 'SOLAR_NN':
        self.update_state(state='PROGRESS',
                          meta={'current': 1, 'total': 2,
                                'status': message})
        from ems.batch.bin.analytics.forecast_solar_nn import forecast_solar_nn
        forecast_solar_nn(db_uri, discom, state)
    elif model == 'WIND_NN':
        self.update_state(state='PROGRESS',
                          meta={'current': 1, 'total': 2,
                                'status': message})
        from ems.batch.bin.analytics.forecast_wind_nn import forecast_wind_nn
        forecast_wind_nn(db_uri, discom, state)
    elif model == 'WIND_NN1':
        self.update_state(state='PROGRESS',
                          meta={'current': 1, 'total': 2,
                                'status': message})
        from ems.batch.bin.analytics.forecast_wind_nn1 import forecast_wind_nn1
        forecast_wind_nn1(db_uri, discom, state)
    elif model == 'WIND_HYBRID_NN':
        self.update_state(state='PROGRESS',
                          meta={'current': 1, 'total': 2,
                                'status': message})
        from ems.batch.bin.analytics.forecast_wind_hybrid_nn \
            import forecast_wind_hybrid_nn
        forecast_wind_hybrid_nn(db_uri, discom, state)
    elif model == 'WIND_HYBRID':
        self.update_state(state='PROGRESS',
                          meta={'current': 1, 'total': 2,
                                'status': message})
        from ems.batch.bin.analytics.forecast_wind_hybrid \
            import forecast_wind_hybrid
        forecast_wind_hybrid(db_uri, discom, state)
    elif model == 'PRICESIM':
        self.update_state(state='PROGRESS',
                          meta={'current': 1, 'total': 2,
                                'status': message})
        from ems.batch.bin.analytics.price_simulator import price_forecast
        price_forecast(db_uri, area, state)
    # elif model == 'NN':
    #     self.update_state(state='PROGRESS',
    #                       meta={'current': 1, 'total': 2,
    #                             'status': message})
    #     from batch.bin.analytics.forecast_solar_nn import forecast_solar_nn
    #     forecast_solar_nn(db_uri, discom, state)
    self.update_state(state='PROGRESS',
                      meta={'current': 3, 'total': 3,
                            'status': message})
    return {'current': 100, 'total': 100, 'status': 'Task completed!',
            'result': 'Done'}


@ems.route('/forecast_task/<discom>/<model>/<scoring>', methods=['POST'])
@roles_required(['admin', 'jobrunner', 'analytics'])
def forecast_task(discom, model, scoring):
    db = DB()
    userid = current_user.organisation_master_fk
    datacursor = db.query_dictcursor("""SELECT
        d.zone_code,
        b.organisation_code discom, c.state_name state
        from power.organisation_master b,
             power.state_master c,
             power.zone_master d
        where c.state_master_pk = b.state_master_fk
        and c.exchange_zone_master_fk = d.zone_master_pk
        and (b.organisation_master_pk = %s
            or b.organisation_parent_fk = %s)
        and b.delete_ind = 0
        and c.delete_ind = 0
        and d.delete_ind = 0""", data=(userid, userid))
    results = datacursor.fetchone()
    db.close()
    area = results.get('zone_code')
    # To be used for passing state name
    state = results.get('state')
    db_uri = current_app.config['SQLALCHEMY_DATABASE_URI']
    # logger.info("forecast_task started {} {} {} {} {} {}".format(discom, model, db_uri,
    #                                  scoring, area, state))
    task = forecast_tsk.apply_async((discom, model, db_uri,
                                     scoring, area, state))
    return jsonify({}), 202, {'Location': url_for('ems.taskstatus',
                              job_nm="forecast_tsk", task_id=task.id)}


@celery.task(bind=True)
def trade_tsk(self, db_uri, date, discom, demmodel,
              genmodel, minalpha, maxalpha, ladder,
              step, area, demandz, generationz):
    logger.info("trade_tsk started")
    logger.info("%s %s %s %s %s %s %s %s %s %s %s",
                date, discom, demmodel, genmodel,
                minalpha, maxalpha,
                ladder, step, area, demandz, generationz)
    message = ''
    self.update_state(state='PROGRESS',
                      meta={'current': 1, 'total': 3,
                            'status': message})
    from ems.batch.bin.analytics.trade import trade
    self.update_state(state='PROGRESS',
                      meta={'current': 2, 'total': 3,
                            'status': message})
    trade(db_uri, date, discom, demmodel, genmodel, minalpha,
          maxalpha, ladder, step, area, demandz, generationz)
    return {'current': 100, 'total': 100, 'status': 'Task completed!',
            'result': 'Done'}


@celery.task(bind=True)
def alloc_opt_tsk(self, db_uri, date, discom, transaction_cost,
                  alpha, max_surrender_vol, minimum_cont_block, area):
    logger.info("alloc_opt_tsk started")
    logger.info("%s %s %s %s %s %s %s",
                date, discom, transaction_cost,
                alpha, max_surrender_vol,
                minimum_cont_block, area)
    message = ''
    self.update_state(state='PROGRESS',
                      meta={'current': 1, 'total': 4,
                            'status': message})
    from ems.batch.bin.analytics.price_simulator import price_simulator
    from ems.batch.bin.analytics.allocation_optimization\
        import allocation_optimization
    self.update_state(state='PROGRESS',
                      meta={'current': 2, 'total': 4,
                            'status': message})
    price_simulator(db_uri, alpha, date, area)
    self.update_state(state='PROGRESS',
                      meta={'current': 3, 'total': 4,
                            'status': message})
    allocation_optimization(db_uri, date, transaction_cost,
                            alpha, minimum_cont_block,
                            max_surrender_vol, discom, area)
    return {'current': 100, 'total': 100, 'status': 'Task completed!',
            'result': 'Done'}


@ems.route('/trade_task', methods=['POST'])
@roles_required(['admin', 'jobrunner', 'analytics'])
def trade_task():
    date = request.json['date']
    discom = request.json['discom']
    demmodel = request.json['demmodel']
    genmodel = request.json['genmodel']
    minalpha = request.json['minalpha']
    maxalpha = request.json['maxalpha']
    ladder = request.json['ladder']
    step = request.json['step']
    demandz = request.json['demandz']
    generationz = request.json['generationz']
    current_app.logger.info("trade_task %s %s %s %s %s %s %s %s %s %s",
                            date, discom, demmodel, genmodel, minalpha,
                            maxalpha, ladder, step, demandz, generationz)
    db = DB()
    userid = current_user.organisation_master_fk
    datacursor = db.query_dictcursor("""SELECT
        ldc_name, ldc_org_name, d.zone_code,
        b.organisation_code discom
        from power.org_isgs_map a,
             power.organisation_master b,
             power.state_master c,
             power.zone_master d
        where a.organisation_master_fk = b.organisation_master_pk
        and c.state_master_pk = b.state_master_fk
        and c.exchange_zone_master_fk = d.zone_master_pk
        and (b.organisation_master_pk = %s
            or b.organisation_parent_fk = %s)
        and a.delete_ind = 0
        and b.delete_ind = 0
        and c.delete_ind = 0
        and d.delete_ind = 0""", data=(userid, userid))
    results = datacursor.fetchone()
    db.close()
    area = results.get('zone_code')
    discom = results.get('discom')
    db_uri = current_app.config['SQLALCHEMY_DATABASE_URI']
    task = trade_tsk.apply_async((db_uri, date, discom, demmodel, genmodel,
                                 minalpha, maxalpha, ladder, step, area,
                                 demandz, generationz))
    return jsonify({}), 202, {'Location': url_for('ems.taskstatus',
                              job_nm="trade_tsk", task_id=task.id)}


@ems.route('/alloc_opt_task', methods=['POST'])
@roles_required(['admin', 'jobrunner', 'analytics'])
def alloc_opt_task():
    date = request.json['date']
    discom = request.json['discom']
    transaction_cost = request.json['tc']
    alpha = request.json['alpha']
    max_surrender_vol = request.json['maxsurr']
    minimum_cont_block = request.json['mcb']
    current_app.logger.info("alloc_opt_task %s %s %s %s %s %s",
                            date, discom, transaction_cost, alpha,
                            max_surrender_vol, minimum_cont_block)
    db = DB()
    userid = current_user.organisation_master_fk
    datacursor = db.query_dictcursor("""SELECT
        ldc_name, ldc_org_name, d.zone_code,
        b.organisation_code discom
        from power.org_isgs_map a,
             power.organisation_master b,
             power.state_master c,
             power.zone_master d
        where a.organisation_master_fk = b.organisation_master_pk
        and c.state_master_pk = b.state_master_fk
        and c.exchange_zone_master_fk = d.zone_master_pk
        and (b.organisation_master_pk = %s
            or b.organisation_parent_fk = %s)
        and a.delete_ind = 0
        and b.delete_ind = 0
        and c.delete_ind = 0
        and d.delete_ind = 0""", data=(userid, userid))
    results = datacursor.fetchone()
    db.close()
    area = results.get('zone_code')
    discom = results.get('discom')
    db_uri = current_app.config['SQLALCHEMY_DATABASE_URI']
    task = alloc_opt_tsk.apply_async((db_uri, date, discom, transaction_cost,
                                     alpha, max_surrender_vol,
                                     minimum_cont_block, area))
    return jsonify({}), 202, {'Location': url_for('ems.taskstatus',
                              job_nm="alloc_opt_tsk", task_id=task.id)}


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1] in set(['csv'])

# @ems.route('/upload', methods=['GET', 'POST'])
# def upload():
#     print "In Upload"
#     print request.method , request.files.getlist("file[]")
#     # csvfiles = UploadSet('file', set(['csv']), homedir + '/Projects/bihar/io/daily/')
#     if request.method == 'POST':
#         files = request.files.getlist("file[]")
#         for file in files:
#             if file and allowed_file(file.filename):
#                 filename = secure_filename(file.filename)
#                 file.save(os.path.join(current_app.config['UPLOAD_FOLDER'], filename))
#         print "Redirecting"
#         bseb_dataupload_task()
#         # redirect(url_for('bseb_dataupload_task'))
#         # rec = Photo(filename=filename, user=g.user.id)
#         # rec.store()
#         # flash("Photo saved.")
#         # return redirect(url_for('show', id=rec.id))
#     # return render_template('upload.html')
#     return redirect(url_for('ems.tengenisgs'))


def upload_csv_lfile(file):
    """Upload file to local folder"""
    if not file:
        return None    
    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        file.save(os.path.join(current_app.config['UPLOAD_FOLDER'], filename))
        return current_app.config['UPLOAD_FOLDER'] +  filename
    return None



def upload_csv_file(file):
    """
    Upload the user-uploaded file to Google Cloud Storage and retrieve its
    publicly-accessible URL.
    """
    if not file:
        return None

    public_url = ems_storage.upload_file(
        file.read(),
        file.filename,
        file.content_type
    )

    current_app.logger.info(
        "Uploaded file %s as %s.", file.filename, public_url)

    return public_url


@ems.route('/tmanualdataupload', methods=['GET', 'POST'])
@roles_required(['admin', 'jobrunner', 'analytics'])
def tmanualupload():
    current_app.logger.info("In tmanualupload")
    current_app.logger.debug("%s %s",
                             request.method,
                             request.files.getlist("file-5[]"))
    valid_discoms = get_org().get('org_name')
    if request.method == 'POST':
        files = request.files.getlist("file-5[]")
        file_url = []
        for file in files:
            file_url.append(upload_csv_lfile(file))
        current_app.logger.debug("Filenamebuckets: %s %s", file_url, files)
        ret = tentativedata_upload_task(file_url, valid_discoms)
        current_app.logger.debug("%s", ret)
        newret = {'append': 'false'}
        newret.update(ret[2])
    return jsonify(newret)


@ems.route('/loadupload', methods=['GET', 'POST'])
@roles_required(['admin', 'jobrunner', 'analytics'])
def loadupload():
    # print current_app.config.
    current_app.logger.info("In loadupload")
    current_app.logger.info("%s %s",
                            request.method,
                            request.files.getlist("file-4[]"))
    # csvfiles = UploadSet('file', set(['csv']),
    #                       homedir + '/Projects/bihar/io/daily/')
    valid_discoms = get_org().get('org_name')
    file_url = []
    if request.method == 'POST':
        files = request.files.getlist("file-4[]")
        for file in files:
            file_url.append(upload_csv_lfile(file))
        current_app.logger.info("Filenamebuckets: %s", file_url)
    if 'BPDCL' in valid_discoms:
        bseb_dataupload_task(file_url)
    else:
        ret = data_upload_task(file_url, valid_discoms)
        current_app.logger.info("%s", ret)
        # [START enqueue]
        # for fileurl in file_url:
        #     q = ems_tasks.get_ems_queue()
        #     # q.enqueue(ems_tasks.bseb_dataupload_tsk, fileurl)
        #     q.enqueue(ems_tasks.slow_task)
        #     q.enqueue(ems_tasks.print_task, "Hello, World")
        #     r = q.enqueue(ems_tasks.adder, 1, 5)
        #     print(r.result(timeout=10))

        # [END enqueue]
        #     bseb_dataupload_task(fileurl)
        # redirect(url_for('bseb_dataupload_task'))
        # rec = Photo(filename=filename, user=g.user.id)
        # rec.store()
        # flash("Photo saved.")
        # return redirect(url_for('show', id=rec.id))
    # return render_template('upload.html')
    # return redirect(url_for('ems.dayahead'))
    # return jsonify({})
    # return flask.render_template("dayahead_jinja.html",
    #                              upload_job_status_url=ret[2].get('Location'))
        # file-imput plugin return params
        newret = {'append': 'false'}
        newret.update(ret[2])
    return jsonify(newret)


@ems.route("/gmap")
@cross_origin(origin='*', headers=['Content-Type', 'Authorization'])
def gmap():
    """
    When you request the root path, you'll get the index.html template.
    """
    current_app.logger.info("In Map")
    return flask.render_template("googlemapsweatheroverlay2.html")
    # return flask.render_template("map4.html")


@ems.route("/weatherwatch")
@cross_origin(origin='*', headers=['Content-Type', 'Authorization'])
def weatherwatch():
    """
    When you request the root path, you'll get the index.html template.
    """
    db = DB()
    datacursor = db.query_dictcursor("""SELECT
        b.state_name, c.organisation_code,
        a.ldc_name, a.ldc_org_name
        from power.org_isgs_map a,
             power.state_master b,
             power.organisation_master c
        where c.state_master_fk = b.state_master_pk
        and a.organisation_master_fk = c.organisation_master_pk
        and a.delete_ind = 0
        and b.delete_ind = 0
        and c.delete_ind = 0
        and (c.organisation_master_pk = %s
        or c.organisation_parent_fk = %s)""", data=(
        current_user.organisation_master_fk,
        current_user.organisation_master_fk))
    org = datacursor.fetchall()
    db.close()
    state = org[0].get('state_name')
    current_app.logger.info("In weatherwatch")
    return flask.render_template("weatherwatch.html", state=state)
    # return flask.render_template("map4.html")


@ems.route("/weatherwatchdata/<west_lng>/<north_lat>/<east_lng>/<south_lat>")
@cross_origin(origin='*', headers=['Content-Type', 'Authorization'])
def weatherwatchdata(west_lng, north_lat, east_lng, south_lat):
    """
    When you request the root path, you'll get the index.html template.
    """
    current_app.logger.info("In weatherwatch")
    # date_today = serverdate().get('serverdate')
    db = DB()
    datacursor = db.query_dictcursor("""SELECT
        date_format(a.date, '%d-%m-%Y') date,
        a.block_hour_no hour, a.temperature,
        case when d.temperature is not null
        then  round(((a.temperature + 273.15) - (d.temperature + 273.15))
                     /(d.temperature + 273.15) * 100,3)
        else null end perc_change_temp_frdaybf,
        coalesce(case when coalesce(a.rainfall_mm, 0) > 0
                      then 'rain'
                      else 'clear' end, a.conditions) conditions,
        case when a.rainfall_mm is null
             and a.qpf is not null
             and a.pop is not null
             then a.qpf * (a.pop/100)
             else a.rainfall_mm end rainfall,
        ((case when a.rainfall_mm is null
             and a.qpf is not null
             and a.pop is not null
             then a.qpf * (a.pop/100)
             else a.rainfall_mm end -
        case when d.rainfall_mm is null
             and d.qpf is not null
             and d.pop is not null
             then d.qpf * (d.pop/100)
             else d.rainfall_mm end)/
        case when d.rainfall_mm is null
             and d.qpf is not null
             and d.pop is not null
             and d.qpf > 0
             and d.pop > 0
             then d.qpf * (d.pop/100)
             when d.rainfall_mm is not null
              or d.rainfall_mm > 0
             then d.rainfall_mm
             else 1 end) perc_change_rainfall_frdaybf,
        a.windspeed, a.winddir_deg, b.latitude, b.longitude,
        b.display_city_name city, date_format(c.cur_date, '%d-%m-%Y') cur_date,
        c.cur_block_hour_no
        from power.unified_weather a,
        power.imdaws_wunderground_map b,
        (select b.cur_date, a.block_hour_no cur_block_hour_no
         from power.block_master a,
             (select date(now()) cur_date, time(now()) cur_time) b
         where start_time <= cur_time
         and end_time >= cur_time) c,
        power.unified_weather d
        where a.location = b.mapped_location_name
        and a.date >= DATE_SUB(CURDATE(), INTERVAL 3 DAY)
        and a.date <= DATE_ADD(CURDATE(), INTERVAL 3 DAY)
        and d.location = b.mapped_location_name
        and d.date = DATE_SUB(a.date, INTERVAL 1 DAY)
        and a.block_hour_no = d.block_hour_no""")
        # and %s >= b.latitude
        # and b.latitude <= %s
        # and %s <= b.longitude
        # and b.longitude >= %s""", data=(north_lat, south_lat,
        #                                 east_lng, west_lng))
    results = datacursor.fetchall()
    db.close()
    # current_app.logger.info("weatherwatch data: %s", results)
    return json.dumps(results, use_decimal=True)


@ems.route("/weather_slider")
def weather_slider():
    """
    When you request the root path, you'll get the index.html template.
    """
    current_app.logger.info("In weather_slider")
    # return flask.render_template("googlemapsweatheroverlay.html")
    # return flask.render_template("slidertest.html")
    db = DB()
    datacursor = db.query_dictcursor("""SELECT
        b.state_name, c.organisation_code,
        a.ldc_name, a.ldc_org_name
        from power.org_isgs_map a,
             power.state_master b,
             power.organisation_master c
        where c.state_master_fk = b.state_master_pk
        and a.organisation_master_fk = c.organisation_master_pk
        and a.delete_ind = 0
        and b.delete_ind = 0
        and c.delete_ind = 0
        and (c.organisation_master_pk = %s
        or c.organisation_parent_fk = %s)""", data=(
        current_user.organisation_master_fk,
        current_user.organisation_master_fk))
    org = datacursor.fetchall()
    db.close()
    state = org[0].get('state_name')
    current_app.logger.info("In weather_slider %s", org)
    return flask.render_template("weather_slider.html", state=state)


@ems.route("/weather_heatmap")
def weather_heatmap():
    """Local Weather Watch."""
    current_app.logger.info("In weather_heatmap")
    # return flask.render_template("googlemapsweatheroverlay.html")
    # return flask.render_template("slidertest.html")
    db = DB()
    datacursor = db.query_dictcursor("""SELECT
        b.state_name, c.organisation_code
        from power.state_master b,
             power.organisation_master c
        where c.state_master_fk = b.state_master_pk
        and b.delete_ind = 0
        and c.delete_ind = 0
        and (c.organisation_master_pk = %s
        or c.organisation_parent_fk = %s)""", data=(
        current_user.organisation_master_fk,
        current_user.organisation_master_fk))
    org = datacursor.fetchall()
    db.close()
    state = org[0].get('state_name')
    current_app.logger.info("In weather_heatmap %s", org)
    return flask.render_template("weather_slider_heatmap_icon.html",
                                 state=state)


# @ems.route("/get_substation/<category>")
@ems.route("/get_substation")
@cross_origin(origin='*', headers=['Content-Type', 'Authorization'])
def get_substation(category=None):
    """Substation for a organisation."""
    current_app.logger.info("In substation")
    # category = 'A'
    db = DB()
    # datacursor = db.query_dictcursor("""SELECT
    #     b.state_name, c.organisation_code,
    #     a.ldc_name, a.ldc_org_name
    #     from power.org_isgs_map a,
    #          power.state_master b,
    #          power.organisation_master c
    #     where c.state_master_fk = b.state_master_pk
    #     and a.organisation_master_fk = c.organisation_master_pk
    #     and a.delete_ind = 0
    #     and b.delete_ind = 0
    #     and c.delete_ind = 0
    #     and (c.organisation_master_pk = %s
    #     or c.organisation_parent_fk = %s)""", data=(
    #     current_user.organisation_master_fk,
    #     current_user.organisation_master_fk))
    # org = datacursor.fetchall()
    # org_name = org[0].get('organisation_code')
    org_id = 20
    # datacursor = db.query_dictcursor("""SELECT
    #     a.Substation_Master_Parent_FK parent,
    #     a.Substation_Master_SK child,
    #     b.substation_name parent_name,
    #     b.substation_code parent_code, b.latitude par_lat,
    #     b.longitude par_long, b.output_rating,
    #     a.substation_name child_name,
    #     a.substation_code child_code, a.latitude child_lat,
    #     a.longitude child_long, AsText(a.connection) connection
    #     from power.substation_master a
    #     right join
    #     power.substation_master b
    #     on a.Substation_Master_Parent_FK = b.Substation_Master_SK
    #     where a.owner_fk = %s""", data=(org_id,))
    #     and a.Substation_Master_SK >= 153
    #     and a.Substation_Master_SK <= 178
    #     or a.Substation_Master_SK = 23
    if category:
        datacursor = db.query_dictcursor("""SELECT parent.substation_master_conn_pk id,
            parent.parent, child.child, parent.parent_name, parent.parent_code,
            par_lat,
            parent.par_long,
            parent.parent_rating,
            child.child_name, child.child_code,
            child_lat,
            child.child_long,parent.max_substation_master_pk,
            child.max_substation_master_pk,
            case when parent.max_substation_master_pk is null
                and child.max_substation_master_pk is null
                then parent.connection
                else null end connection,
            coalesce(parent.substation_type_name, child.substation_type_name)
                    substation_type_name,
            parent.parent_flag,
            coalesce(child.ringoff_ind, parent.ringoff_ind) ringoff_ind,
            parent.normally_open_ind,
            parent.loop_no,
            coalesce(child.priority, parent.priority) priority,
            coalesce(child.accessibility_description,
                     parent.accessibility_description)
                                     accessibility_description,
            coalesce(child.number_of_customers,
                     parent.number_of_customers) number_of_customers
            from
            (select  b.substation_master_conn_pk,
                    b.substation_master_from_fk parent,
                    a.substation_name parent_name,
                    a.substation_code parent_code, a.latitude par_lat,
                    a.longitude par_long, a.output_rating parent_rating,
                    ST_ASTEXT(b.path)connection,
                    d.substation_type_name,
                    a.parent_ind parent_flag,
                    b.normally_open_ind,
                    a.ringoff_ind,
                    a.max_substation_master_pk,
                    b.loop_no,
                    a.priority,
                    sat.accessibility_description,
                    a.number_of_customers
            from (select sm.substation_master_pk, sm.substation_name,
                sm.substation_code, sm.substation_type_fk,
                coalesce(dll.latitude - 0.0000500, sm.latitude) latitude,
                sm.longitude,
                sm.output_rating, sm.ringoff_ind, sm.owner_fk, sm.parent_ind,
                dll.max_substation_master_pk,
                sm.priority,
                sm.substation_accessibility_fk,
                sm.number_of_customers
                from power.substation_master sm
                left join
                (select a.substation_master_pk max_substation_master_pk,
                 a.substation_type_fk, a.latitude , a.longitude
                -- a.substation_code, b.substation_code , b.latitude,
                -- b.longitude,
                -- ST_Distance_Sphere(POINT(a.latitude,a.longitude),
                -- POINT(b.latitude,b.longitude)) distance_in_meters
                from power.substation_master a,
                     power.substation_master b
                where a.substation_master_pk > b.substation_master_pk
                and b.substation_type_fk is not null
                and a.substation_type_fk is not null
                and a.substation_type_fk = b.substation_type_fk
                and a.parent_ind = 0
                and b.parent_ind = 0
                -- and a.substation_type_fk= 5
                and ST_Distance_Sphere(POINT(a.latitude,a.longitude),
                                       POINT(b.latitude,b.longitude))
                    < 0.15) dll
                on (sm.substation_type_fk = dll.substation_type_fk
                    and sm.latitude = dll.latitude
                    and sm.longitude = dll.longitude
                    and sm.substation_master_pk = dll.max_substation_master_pk)
            ) a
            left join
                 power.substation_type d
            on (a.substation_type_fk = d.substation_type_pk)
            left join
                power.substation_accessibility_type sat
            on (a.substation_accessibility_fk =
                sat.substation_accessibility_type_pk),
                 power.substation_master_conn b
            where a.substation_master_pk = b.substation_master_from_fk
            and a.owner_fk = %s) parent,
            (select  b.substation_master_conn_pk,
                    b.substation_master_to_fk child,
                    a.substation_name child_name,
                    a.substation_code child_code, a.latitude child_lat,
                    a.longitude child_long, a.output_rating child_rating,
                    d.substation_type_name,
                    a.ringoff_ind, a.max_substation_master_pk,
                    a.priority,
                    sat.accessibility_description,
                    a.number_of_customers
            from (select sm.substation_master_pk, sm.substation_name,
                sm.substation_code, sm.substation_type_fk,
                coalesce(dll.latitude - 0.0000500, sm.latitude) latitude,
                sm.longitude,
                sm.output_rating, sm.ringoff_ind, sm.owner_fk, sm.parent_ind,
                dll.max_substation_master_pk,
                sm.priority,
                sm.substation_accessibility_fk,
                sm.number_of_customers
                from power.substation_master sm
                left join
                (select a.substation_master_pk max_substation_master_pk,
                 a.substation_type_fk, a.latitude , a.longitude
                -- a.substation_code, b.substation_code , b.latitude,
                -- b.longitude,
                -- ST_Distance_Sphere(POINT(a.latitude,a.longitude),
                --                    POINT(b.latitude,b.longitude))
                -- distance_in_meters
                from power.substation_master a,
                     power.substation_master b
                where a.substation_master_pk > b.substation_master_pk
                and b.substation_type_fk is not null
                and a.substation_type_fk is not null
                and a.substation_type_fk = b.substation_type_fk
                and a.parent_ind = 0
                and b.parent_ind = 0
                -- and a.substation_type_fk= 5
                and ST_Distance_Sphere(POINT(a.latitude,a.longitude),
                                       POINT(b.latitude,b.longitude))
                    < 0.15) dll
                on (sm.substation_type_fk = dll.substation_type_fk
                    and sm.latitude = dll.latitude
                    and sm.longitude = dll.longitude
                    and sm.substation_master_pk = dll.max_substation_master_pk)
            ) a
            left join
                 power.substation_type d
            on (a.substation_type_fk = d.substation_type_pk)
            left join
                power.substation_accessibility_type sat
            on (a.substation_accessibility_fk =
                sat.substation_accessibility_type_pk),
                 power.substation_master_conn b
            where a.substation_master_pk = b.substation_master_to_fk
            and a.owner_fk = %s) child
            where parent.substation_master_conn_pk =
                  child.substation_master_conn_pk
            and coalesce(parent.substation_type_name,
                         child.substation_type_name) = %s""",
                                         data=(org_id, org_id, category))
    else:
        datacursor = db.query_dictcursor("""SELECT parent.substation_master_conn_pk id,
            parent.parent, child.child, parent.parent_name, parent.parent_code,
            par_lat,
            parent.par_long,
            parent.parent_rating,
            child.child_name, child.child_code,
            child_lat,
            child.child_long,parent.substation_master_pk,
            child.substation_master_pk,
            parent.substation_master_conn_pk,
            parent.connection,
            coalesce(parent.substation_type_name,
                child.substation_type_name) substation_type_name,
            parent.parent_flag,
            child.ringoff_ind child_ringoff_ind,
            parent.ringoff_ind parent_ringoff_ind,
            parent.normally_open_ind parent_no,
            child.normally_open_ind child_no,
            parent.loop_no,
            parent.priority parent_priority,
            child.priority child_priority,
            parent.accessibility_description parent_ad,
            child.accessibility_description child_ad,
            parent.number_of_customers parent_noc,
            child.number_of_customers child_noc
            from
            (select  b.substation_master_conn_pk,
                    b.substation_master_from_fk parent,
                    a.substation_name parent_name,
                    a.substation_code parent_code, a.latitude par_lat,
                    a.longitude par_long, a.output_rating parent_rating,
                    ST_ASTEXT(b.path)connection,
                    d.substation_type_name,
                    a.parent_ind parent_flag,
                    b.normally_open_ind,
                    a.ringoff_ind,
                    a.substation_master_pk,
                    b.loop_no,
                    a.priority,
                    sat.accessibility_description,
                    a.number_of_customers
            from (select sm.substation_master_pk, sm.substation_name,
                sm.substation_code, sm.substation_type_fk,
                coalesce(dll.newlatitude, sm.latitude) latitude,
                sm.longitude,
                sm.output_rating, sm.ringoff_ind,
                sm.owner_fk, sm.parent_ind,
                -- dll.substation_master_pk,
                sm.priority,
                sm.substation_accessibility_fk,
                sm.number_of_customers
                from power.substation_master sm
                left join
                (select c.*
                ,       @sum := if(@cat = category, @sum,0) + cnt as CatTotal
                ,       @cat := category
                , c.latitude - (0.0000500 * (@sum - 1)) as newlatitude
                from
                (select b.substation_master_pk, b.substation_code,
                 b.substation_type_fk, b.latitude, b.longitude,
                 concat(b.substation_type_fk,
                        b.latitude, b.longitude) category,
                 count(1) cnt
                from
                (select  substation_type_fk,
                        latitude, longitude, count(1) cnt
                from    power.substation_master a
                where a.substation_type_fk is not null
                and a.owner_fk = %s
                and a.delete_ind = 0
                group by substation_type_fk, latitude, longitude
                having count(1) > 1 ) a,
                power.substation_master b
                where a.substation_type_fk = b.substation_type_fk
                and a.latitude = b.latitude
                and a.longitude = b.longitude
                and b.delete_ind = 0
                group by b.substation_master_pk, b.substation_code,
                b.substation_type_fk, b.latitude, b.longitude) c,
                (select @cat := '', @sum := 0) d
                order by c.substation_type_fk, c.latitude,
                c.longitude, c.substation_code
                ) dll
                on (sm.substation_type_fk = dll.substation_type_fk
                    and sm.substation_master_pk =
                    dll.substation_master_pk
                    and sm.delete_ind = 0)) a
            left join
                 power.substation_type d
            on (a.substation_type_fk = d.substation_type_pk
                and d.delete_ind = 0)
            left join
                power.substation_accessibility_type sat
            on (a.substation_accessibility_fk =
                sat.substation_accessibility_type_pk
                and sat.delete_ind = 0),
                 power.substation_master_conn b
            where a.substation_master_pk = b.substation_master_from_fk
            and b.delete_ind = 0
            and a.owner_fk = %s) parent,
            (select  b.substation_master_conn_pk,
                    b.substation_master_to_fk child,
                    a.substation_name child_name,
                    a.substation_code child_code, a.latitude child_lat,
                    a.longitude child_long, a.output_rating child_rating,
                    d.substation_type_name,
                    b.normally_open_ind,
                    a.ringoff_ind,
                    a.substation_master_pk,
                    b.loop_no,
                    a.priority,
                    sat.accessibility_description,
                    a.number_of_customers
            from (select sm.substation_master_pk, sm.substation_name,
                sm.substation_code, sm.substation_type_fk,
                coalesce(dll.newlatitude, sm.latitude) latitude,
                sm.longitude,
                sm.output_rating,
                sm.ringoff_ind, sm.owner_fk,
                sm.parent_ind,
                -- dll.max_substation_master_pk,
                sm.priority,
                sm.substation_accessibility_fk,
                sm.number_of_customers
                from power.substation_master sm
                left join
                (select c.*
                ,       @sum := if(@cat = category, @sum,0) + cnt as CatTotal
                ,       @cat := category
                , c.latitude - (0.0000500 * (@sum - 1)) as newlatitude
                from
                (select b.substation_master_pk, b.substation_code,
                 b.substation_type_fk, b.latitude, b.longitude,
                concat(b.substation_type_fk, b.latitude, b.longitude) category
                , count(1) cnt
                from
                (select  substation_type_fk,
                        latitude, longitude, count(1) cnt
                from    power.substation_master a
                where a.substation_type_fk is not null
                and a.delete_ind = 0
                and a.owner_fk = %s
                group by substation_type_fk, latitude, longitude
                having count(1) > 1 ) a,
                power.substation_master b
                where a.substation_type_fk = b.substation_type_fk
                and a.latitude = b.latitude
                and a.longitude = b.longitude
                and b.delete_ind = 0
                group by b.substation_master_pk, b.substation_code,
                b.substation_type_fk, b.latitude, b.longitude) c,
                (select @cat := '', @sum := 0) d
                order by c.substation_type_fk, c.latitude,
                c.longitude, c.substation_code
                ) dll
                on (sm.substation_type_fk = dll.substation_type_fk
                    and sm.substation_master_pk
                    = dll.substation_master_pk
                    and sm.delete_ind = 0)) a
            left join
                 power.substation_type d
            on (a.substation_type_fk = d.substation_type_pk
                and d.delete_ind = 0)
            left join
                power.substation_accessibility_type sat
            on (a.substation_accessibility_fk =
                sat.substation_accessibility_type_pk
                and sat.delete_ind = 0),
                 power.substation_master_conn b
            where a.substation_master_pk = b.substation_master_to_fk
            and b.delete_ind = 0
            and a.owner_fk = %s) child
            where parent.substation_master_conn_pk =
            child.substation_master_conn_pk
            -- and coalesce(parent.substation_type_name,
            --     child.substation_type_name) = 'H'
            order by id""", data=(org_id, org_id,
                                  org_id, org_id))
    results = datacursor.fetchall()
    db.close()
    # current_app.logger.info("substation data: %s", results)
    return json.dumps(results, use_decimal=True)


@ems.route("/substation_overlay")
def substation_overlay():
    """
    When you request the root path, you'll get the index.html template.
    """
    current_app.logger.info("In substation_overlay")
    return flask.render_template("spiderify.html")
    # return flask.render_template("substation_overlay.html")


@ems.route("/substation_overlayB")
def substation_overlayB():
    """
    When you request the root path, you'll get the index.html template.
    """
    current_app.logger.info("In substation_overlay")
    return flask.render_template("spiderifyB.html")
    # return flask.render_template("substation_overlay.html")


@ems.route("/post_user_gis", methods=['POST'])
def post_user_gis():
    current_app.logger.info('***** started post_user_gis')
    latitude = request.json.get('latitude')
    longitude = request.json.get('longitude')
    userid = current_user.id
    db = DB()
    # org_id = 21
    sql = """INSERT INTO fast_restoration_plan_tracker
            (`user_id_fk`,
            `Latitude`,
            `Longitude`)
            VALUES
            (%s, %s, %s)
            on duplicate key update
            Latitude = values(Latitude),
            Longitude = values(Longitude)"""
    try:
        db.query_dictcursor(sql, 'insert', [(userid, latitude, longitude), ])
        db.query_commit()
        db.close()
    except Exception as error:
        db.query_rollback()
        db.close()
        current_app.logger.error(
            "Error during Live Position Map update %s", error)
        raise
    return json.dumps({'success': True}), 200,\
        {'ContentType': 'application/json'}


@ems.route("/get_user_gis")
def get_user_gis():
    current_app.logger.info('***** started get_user_gis')
    # userid = current_user.id
    db = DB()
    # org_id = 21
    datacursor = db.query_dictcursor("""SELECT
            b.username, a.latitude, a.longitude
            from power.fast_restoration_plan_tracker a,
                 power.users_flask_user_mod b
            where a.user_id_fk = b.id""")
    results = datacursor.fetchall()
    db.close()
    current_app.logger.info("get_user_gis data: %s", results)
    return json.dumps(results, use_decimal=True)


@ems.route("/frp_get_station")
def frp_get_station():
    """
    Get all the stations for which no alarms have been raised.
    """
    current_app.logger.info('***** started frp_get_station')
    userid = current_user.id
    db = DB()
    org_id = 20
    datacursor = db.query_dictcursor("""SELECT
        b.substation_code, b.substation_master_pk, d.substation_type_code
        from power.substation_master b
             left join
             power.substation_type d
        on(b.substation_type_fk = d.substation_type_pk)
        -- left join
        -- (select distinct substation_master_fk
        --  from power.fast_restoration_plan_alarm
        --  where alarm_closed_dt is null) a
        -- on(b.substation_master_pk != a.substation_master_fk)
        where b.owner_fk = %s
        -- and d.substation_type_code is not null
        """, data=(org_id,))
    results = datacursor.fetchall()
    db.close()
    current_app.logger.info('***** started frp_get_station %s', results)
    return json.dumps(results)


@ems.route("/frp_get_noalarm_station")
def frp_get_noalarm_station():
    """
    Get the stations for which no alarms have been raised.
    """
    current_app.logger.info('***** started frp_get_noalarm_station')
    userid = current_user.id
    db = DB()
    org_id = 20
    datacursor = db.query_dictcursor("""SELECT
        b.substation_code, b.substation_master_pk, d.substation_type_code,
        b.parent_ind
        from power.substation_master b
             left join
             power.substation_type d
        on(b.substation_type_fk = d.substation_type_pk)
        left join
        (select distinct substation_master_fk
         from power.fast_restoration_plan_alarm
         where alarm_closed_dt is null) a
        on(b.substation_master_pk = a.substation_master_fk)
        where b.owner_fk = %s
        -- and d.substation_type_code is not null
        and a.substation_master_fk is null
        -- group by d.substation_type_code, b.substation_code
        """, data=(org_id,))
    results = datacursor.fetchall()
    db.close()
    current_app.logger.info('***** started frp_get_station %s', results)
    return json.dumps(results)


@ems.route("/frp_create_alarm", methods=['POST'])
def frp_create_alarm():
    current_app.logger.info('***** started frp_create_alarm')
    userid = current_user.id
    org_id = 20
    substation_id = request.json.get('id')
    scenario = request.json.get('scenario')
    current_app.logger.info('frp_create_alarm %s %s',
                            substation_id, scenario)
    if (scenario == 'SUBSTATION'):
        db = DB()
        datacursor = db.query_dictcursor("""SELECT
            count(1) cnt
            from power.fast_restoration_plan_alarm a,
                 power.substation_master b
            where a.substation_master_fk = b.substation_master_pk
            and a.alarm_closed_dt is null
            and b.owner_fk = %s
            and a.substation_master_fk = %s""", data=(org_id, substation_id))
        results = datacursor.fetchone()
        db.close()
        current_app.logger.info('***** started frp_create_alarm %s', results)
        if results.get('cnt') == 0:
            db = DB()
            sql = """INSERT INTO fast_restoration_plan_alarm
                    (`Substation_Master_FK`,
                    `Alarm_Raised_By_Id_FK`,
                    `Alarm_Raised_Dt`,
                    `Alarm_Type`)
                    VALUES
                    (%s, %s, now(), %s)"""
            try:
                db.query_dictcursor(sql, 'insert',
                                    [(substation_id, userid, 'SUBSTATION')])
                db.query_commit()
                db.close()
            except Exception as error:
                db.query_rollback()
                db.close()
                current_app.logger.error(
                    "Error creating alarm %s", error)
                raise
            return json.dumps({'success': True}), 200,\
                {'ContentType': 'application/json'}
    elif (scenario == 'FEEDER'):
        # current_app.logger.info('***** started frp_create_alarm FEEDER')
        db = DB()
        # datacursor = db.query_dictcursor("""SELECT
        #     b.Substation_Master_PK
        #     from power.fast_restoration_plan_alarm a
        #     right join
        #     power.substation_master b
        #     on (a.substation_master_fk = b.substation_master_pk
        #     and a.alarm_closed_dt is null)
        #     where b.feeder_master_fk = %s
        #     and b.owner_fk = %s""", data=(substation_id, org_id))
        # results = datacursor.fetchall()
        # current_app.logger.info('***** results %s', results)
        sql = """INSERT INTO fast_restoration_plan_alarm
                (`Substation_Master_FK`,
                `Alarm_Raised_By_Id_FK`,
                `Alarm_Raised_Dt`,
                `Alarm_Type`)
                select b.Substation_Master_PK, %s as Alarm_Raised_By_Id_FK,
                now() as Alarm_Raised_Dt,
                %s as Alarm_Type
                from power.fast_restoration_plan_alarm a
                     right join
                     power.substation_master b
                on (a.substation_master_fk = b.substation_master_pk
                and a.alarm_closed_dt is null)
                where b.feeder_master_fk = %s
                and a.substation_master_fk is null
                and b.owner_fk = %s"""
        try:
            db.query_dictcursor(
                sql, data=(userid, 'FEEDER', substation_id, org_id))
            db.query_commit()
            db.close()
        except Exception as error:
            db.query_rollback()
            db.close()
            current_app.logger.error(
                "Error creating alarm %s", error)
            raise
        return json.dumps({'success': True}), 200,\
            {'ContentType': 'application/json'}
    return "OK"


@ems.route("/frp_get_alarm")
def frp_get_alarm():
    current_app.logger.info('***** started frp_get_alarm')
    userid = current_user.id
    db = DB()
    org_id = 20
    datacursor = db.query_dictcursor("""SELECT
        a.fast_restoration_plan_alarm_pk,
        b.substation_code, a.substation_master_fk,
        b.latitude, b.longitude, d.substation_type_code,
        a.alarm_type, b.parent_ind, b.feeder_master_fk
        from power.fast_restoration_plan_alarm a,
             power.substation_master b
             left join
             power.substation_type d
        on (b.substation_type_fk = d.substation_type_pk)
        where a.substation_master_fk = b.substation_master_pk
        and a.alarm_closed_dt is null
        and b.owner_fk = %s""", data=(org_id,))
    results = datacursor.fetchall()
    db.close()
    current_app.logger.info("frp_get_alarm data: %s", results)
    return json.dumps(results, use_decimal=True)


@ems.route("/frp_close_alarm", methods=['POST'])
def frp_close_alarm():
    """FRP Close Alarm API."""
    current_app.logger.info('***** started frp_close_alarm')
    userid = current_user.id
    db = DB()
    org_id = 20
    # substation_id = request.json.get('id')
    alarm_id = request.json.get('alarm_id')
    alarm_type = request.json.get('alarm_type')
    current_app.logger.info("frp_get_alarm alarm_id: %s %s",
                            alarm_id, alarm_type)
    if alarm_type == 'SUBSTATION':
        datacursor = db.query_dictcursor("""SELECT
            count(1) cnt
            from power.fast_restoration_plan_alarm a,
                 power.substation_master b
            where a.substation_master_fk = b.substation_master_pk
            and a.alarm_closed_dt is null
            and b.owner_fk = %s
            and a.fast_restoration_plan_alarm_pk = %s
            and a.alarm_type = %s""", data=(org_id, alarm_id, alarm_type))
        results = datacursor.fetchone()
        db.close()
        current_app.logger.info('***** cnt frp_close_alarm %s', results)
        if results.get('cnt') >= 1:
            sql = """UPDATE fast_restoration_plan_alarm
                     SET Alarm_Closed_By_Id_FK = %s,
                     Alarm_Closed_Dt = now()
                     where fast_restoration_plan_alarm_pk = %s
                     and Alarm_Closed_Dt is null
                     and Alarm_Closed_By_Id_FK is null"""
            try:
                db.query_dictcursor(sql, 'insert',
                                    [(userid, alarm_id)])
                db.query_commit()
                db.close()
            except Exception as error:
                db.query_rollback()
                db.close()
                current_app.logger.error(
                    "Error during Live Position Map update %s", error)
                raise
            return json.dumps({'success': True}), 200,\
                {'ContentType': 'application/json'}
        return "OK"
    elif alarm_type == 'FEEDER':
        datacursor = db.query_dictcursor("""SELECT count(a.substation_master_fk) cnt
            from power.fast_restoration_plan_alarm a
                 right join
                 power.substation_master b
            on (a.substation_master_fk = b.substation_master_pk
            and a.alarm_closed_dt is null)
            where b.owner_fk = %s
            and a.substation_master_fk is not null
            and b.feeder_master_fk = %s
            and a.alarm_type = %s""", data=(org_id, alarm_id, alarm_type))
        results = datacursor.fetchone()
        db.close()
        current_app.logger.info('***** cnt frp_close_alarm %s', results)
        if results.get('cnt') >= 1:
            sql = """UPDATE fast_restoration_plan_alarm a,
                            substation_master b
                     SET a.Alarm_Closed_By_Id_FK = %s,
                         a.Alarm_Closed_Dt = now()
                     where a.substation_master_fk = b.substation_master_pk
                     and b.feeder_master_fk = %s
                     and a.Alarm_Closed_Dt is null
                     and a.Alarm_Closed_By_Id_FK is null"""
            try:
                db.query_dictcursor(sql, 'insert',
                                    [(userid, alarm_id)])
                db.query_commit()
                db.close()
            except Exception as error:
                db.query_rollback()
                db.close()
                current_app.logger.error(
                    "Error during Live Position Map update %s", error)
                raise
            return json.dumps({'success': True}), 200,\
                {'ContentType': 'application/json'}
        return "OK"


@ems.route("/grid_data")
def grid_data():
    """
    Substations for a organisation
    """
    current_app.logger.info("In slidertest grid_data")
    current_app.logger.info("In substation")
    # category = 'H'
    db = DB()
    # datacursor = db.query_dictcursor("""SELECT
    #     b.state_name, c.organisation_code,
    #     a.ldc_name, a.ldc_org_name
    #     from power.org_isgs_map a,
    #          power.state_master b,
    #          power.organisation_master c
    #     where c.state_master_fk = b.state_master_pk
    #     and a.organisation_master_fk = c.organisation_master_pk
    #     and a.delete_ind = 0
    #     and b.delete_ind = 0
    #     and c.delete_ind = 0
    #     and (c.organisation_master_pk = %s
    #     or c.organisation_parent_fk = %s)""", data=(
    #     current_user.organisation_master_fk,
    #     current_user.organisation_master_fk))
    # org = datacursor.fetchall()
    # org_name = org[0].get('organisation_code')
    # state_name = org[0].get('state_name')
    org_id = current_user.organisation_master_fk

    datacursor = db.query_dictcursor("""SELECT parent.substation_master_conn_pk id,
        parent.parent, child.child, parent.parent_name, parent.parent_code,
        par_lat,
        parent.par_long,
        parent.parent_rating,
        child.child_name, child.child_code,
        child_lat,
        child.child_long,parent.max_substation_master_pk,
        child.max_substation_master_pk,
        parent.connection,
        coalesce(parent.substation_type_name,
                 child.substation_type_name) substation_type_name,
        parent.parent_flag,
        coalesce(child.ringoff_ind, parent.ringoff_ind) ringoff_ind,
        parent.normally_open_ind,
        parent.loop_no
        from
        (select  b.substation_master_conn_pk,
                b.substation_master_from_fk parent,
                a.substation_name parent_name,
                a.substation_code parent_code, a.latitude par_lat,
                a.longitude par_long, a.output_rating parent_rating,
                ST_ASTEXT(b.path)connection,
                d.substation_type_name,
                a.parent_ind parent_flag,
                b.normally_open_ind,
                a.ringoff_ind,
                a.max_substation_master_pk,
                b.loop_no
        from (select sm.substation_master_pk, sm.substation_name,
            sm.substation_code, sm.substation_type_fk,
            coalesce(dll.latitude - 0.0000500, sm.latitude) latitude,
            sm.longitude,
            sm.output_rating, sm.ringoff_ind, sm.owner_fk, sm.parent_ind,
            dll.max_substation_master_pk
            from power.substation_master sm
            left join
            (select substation_type_fk, latitude, longitude,
             max(substation_master_pk) max_substation_master_pk
             from power.substation_master a
             where substation_type_fk is not null
             group by substation_type_fk, latitude, longitude
             having count(1) > 1) dll
            on (sm.substation_type_fk = dll.substation_type_fk
                and sm.latitude = dll.latitude
                and sm.longitude = dll.longitude
                and sm.substation_master_pk = dll.max_substation_master_pk)) a
        left join
             power.substation_type d
        on (a.substation_type_fk = d.substation_type_pk),
             power.substation_master_conn b
        where a.substation_master_pk = b.substation_master_from_fk
        and a.owner_fk = %s) parent,
        (select  b.substation_master_conn_pk,
                b.substation_master_to_fk child,
                a.substation_name child_name,
                a.substation_code child_code, a.latitude child_lat,
                a.longitude child_long, a.output_rating child_rating,
                d.substation_type_name,
                a.ringoff_ind, a.max_substation_master_pk
        from (select sm.substation_master_pk, sm.substation_name,
            sm.substation_code, sm.substation_type_fk,
            coalesce(dll.latitude - 0.0000500, sm.latitude) latitude,
            sm.longitude,
            sm.output_rating, sm.ringoff_ind, sm.owner_fk,
            sm.parent_ind, dll.max_substation_master_pk
            from power.substation_master sm
            left join
            (select substation_type_fk, latitude, longitude,
             max(substation_master_pk) max_substation_master_pk
            from power.substation_master a
            where substation_type_fk is not null
            group by substation_type_fk, latitude, longitude
            having count(1) > 1) dll
            on (sm.substation_type_fk = dll.substation_type_fk
                and sm.latitude = dll.latitude
                and sm.longitude = dll.longitude
                and sm.substation_master_pk = dll.max_substation_master_pk)) a
        left join
             power.substation_type d
        on (a.substation_type_fk = d.substation_type_pk),
             power.substation_master_conn b
        where a.substation_master_pk = b.substation_master_to_fk
        and a.owner_fk = %s) child
        where parent.substation_master_conn_pk =
        child.substation_master_conn_pk""", data=(org_id, org_id))
    results = datacursor.fetchall()
    db.close()
    # current_app.logger.info("substation data: %s", results)
    return json.dumps(results, use_decimal=True)


@ems.route("/generator_unit_location/<gen_type>")
@cross_origin(origin='*', headers=['Content-Type', 'Authorization'])
def generator_unit_location(gen_type):
    """Generator Unit Location."""
    current_app.logger.info("In slidertest grid_data")
    current_app.logger.info("In substation")
    db = DB()
    datacursor = db.query_dictcursor("""SELECT
        b.state_name, c.organisation_code,
        a.ldc_name, a.ldc_org_name
        from power.org_isgs_map a,
             power.state_master b,
             power.organisation_master c
        where c.state_master_fk = b.state_master_pk
        and a.organisation_master_fk = c.organisation_master_pk
        and a.delete_ind = 0
        and b.delete_ind = 0
        and c.delete_ind = 0
        and (c.organisation_master_pk = %s
        or c.organisation_parent_fk = %s)""", data=(
        current_user.organisation_master_fk,
        current_user.organisation_master_fk))
    org = datacursor.fetchall()
    org_name = org[0].get('organisation_code')

    datacursor = db.query_dictcursor("""SELECT
        a.contracted_capacity, f.rated_unit_capacity, f.unit_name,
        f.unit_code, f.unit_master_pk, f.latitude,
        f.longitude, g.unit_type_name
        from power.contract_trade_master a,
             power.counter_party_master c,
             power.counter_party_master d,
             power.counter_party_type e,
             power.unit_master f,
             power.unit_type g
        where a.counter_party_1_fk = c.counter_party_master_pk
        and   a.counter_party_2_fk = d.counter_party_master_pk
        and e.counter_party_type_pk = d.counter_party_type_fk
        and d.counter_party_name = f.unit_name
        and g.unit_type_pk = f.unit_type_fk
        and c.counter_party_name = %s
        and g.unit_type_name = %s
        and f.latitude is not null
        and f.longitude is not null
        and a.delete_ind = 0
        and c.delete_ind = 0
        and d.delete_ind = 0
        and e.delete_ind = 0
        and f.delete_ind = 0""", data=(org_name, gen_type))
    results = datacursor.fetchall()
    db.close()
    # current_app.logger.info("substation data: %s", results)
    return json.dumps(results, use_decimal=True)


@ems.route("/zms")
@cross_origin(origin='*', headers=['Content-Type', 'Authorization'])
def zms():
    """
    When you request the root path, you'll get the index.html template.
    """
    current_app.logger.info("In Map")
    # return flask.render_template("googlemapsweatheroverlay.html")
    return flask.render_template("map7.html")


@ems.route("/zms2")
@cross_origin(origin='*', headers=['Content-Type', 'Authorization'])
def zms2():
    """
    When you request the root path, you'll get the index.html template.
    """
    current_app.logger.info("In Map")
    # return flask.render_template("googlemapsweatheroverlay.html")
    return flask.render_template("map4.html")


@ems.route("/plot_save", methods=['POST'])
def plot_save():
    """
    When you request the root path, you'll get the index.html template.
    """
    current_app.logger.info('Plot Saving')
    # print '*****' + request.values['name']
    # print request.values['group']
    json = request.json
    current_app.logger.debug(json)
    feature = geojson.loads(geojson.dumps(json))

    # print wkt.dumps(feature.geometry)
    sql = """INSERT INTO agri.user_plots
            (user_master_fk,
            user_plot_id,
            user_plot_name,
            user_plot_group,
            selected_plot_id_fk,
            plot_geometry,
            area_acre,
            cost_per_acre,
            value_per_acre,
            own_ind,
            irrigated_ind,
            user_notes,
            district,
            village,
            state,
            pin,
            country)
            VALUES
            (1,
            %s,
            '%s',
            '%s',
            %s,
            GeomFromText('%s'),
            round('%s', 3),
            round('%s', 3),
            round('%s', 3),
            %s,
            %s,
            '%s',
            '%s',
            '%s',
            '%s',
            '%s',
            '%s')"""
    # print sql % (feature.properties["plotid"],
    #              feature.properties["name"],
    #              feature.properties["group"],
    #              'NULL',
    #              wkt.dumps(feature.geometry),
    #              feature.properties["area"],
    #              feature.properties["costperacre"],
    #              feature.properties["valueperacre"],
    #              feature.properties["optionsown"],
    #              feature.properties["irrigation_checkbox"],
    #              feature.properties["notes"],
    #              feature.properties["district"],
    #              feature.properties["village"],
    #              feature.properties["state"],
    #              feature.properties["pin"],
    #              feature.properties["country"])
    db = DB()
    # datacursor = con.cursor(cursors.DictCursor)
    try:
        db.query_dictcursor(sql % (
            feature.properties["plotid"],
            feature.properties["name"],
            feature.properties["group"],
            'NULL',
            wkt.dumps(feature.geometry),
            feature.properties["area"],
            feature.properties["costperacre"],
            feature.properties["valueperacre"],
            feature.properties["optionsown"],
            feature.properties["irrigation_checkbox"],
            feature.properties["notes"],
            feature.properties["district"],
            feature.properties["village"],
            feature.properties["state"],
            feature.properties["pin"],
            feature.properties["country"]))
        db.cur.commit()
        db.close()
    except Exception:
        db.cur.rollback()
    # return jsonify({"Status": "OK"})
    # return flask.redirect("/get_myplot", code=307)

    # return flask.render_template("test.html" , data=json.dumps(results))
    # return json.dumps(results, use_decimal=True)
    # print feature.geometry.type
    # print feature.geometry.coordinates
    # print feature.properties
    # print feature.id
    # for feature in geojson.load(json):
        # print feature.geometry.type
        # print feature.geometry.coordinates
        # print feature.properties
    return flask.redirect("/map")


@ems.route("/get_myplot")
def get_myplot():
    """
    When you request the root path, you'll get the index.html template.
    """
    current_app.logger.info('Getting Plots')

    # print wkt.dumps(feature.geometry)
    sql = """select
            user_plot_id as plotid,
            user_plot_name as name,
            user_plot_group as "group",
             coalesce(selected_plot_id_fk, user_plots_pk) as id,
             ST_ASTEXT(plot_geometry) as plot_geometry,
            area_acre as area,
            cost_per_acre as costperacre,
            value_per_acre as valueperacre,
            own_ind as optionsown,
            irrigated_ind as irrigation_checkbox,
            user_notes as notes,
            district,
            village,
            state,
            pin,
            country
            from agri.user_plots
            where user_master_fk = %s
            and delete_ind = 0
            order by user_plots_pk"""
    current_app.logger.debug(sql % ('1'))
    db = DB()
    # datacursor = con.cursor(cursors.DictCursor)
    try:
        datacursor = db.query_dictcursor(sql % ('1'))
        results = datacursor.fetchall()
        db.close()
        # datacursor.close()
        # create a new list which will store the single GeoJSON features
        featureCollection = list()
        for row in results:
            current_app.logger.info("In Loop row")
            wkt_geom = wkt.loads(row['plot_geometry'])
            current_app.logger.info('**** %s', wkt_geom)
            geojson_geom = geojson.loads(geojson.dumps(wkt_geom))
            current_app.logger.info('###### %s', geojson_geom)
            # remove the geometry field from the current's row's dictionary
            row.pop('plot_geometry')
            # create a new GeoJSON feature and pass the geometry columns
            # as well as all remaining attributes which are stored
            # in the row dictionary
            feature = geojson.Feature(geometry=geojson_geom, properties=row)
            # append the current feature to the list of all features
            featureCollection.append(feature)
        # when single features for each row from the database table are created
        # , pass the list to the FeatureCollection constructor which will
        # merge them together into one object
        featureCollection = geojson.FeatureCollection(featureCollection)
        current_app.logger.debug(geojson.dumps(featureCollection, indent=2))
    except Exception as e:
        print("Error" + str(e))
        # abort(400, 'Error fetching data')
    # return json.dumps(results, use_decimal=True)
    return geojson.dumps(featureCollection)


# @periodic_task(run_every=(crontab(minute='*/15')))
# def weather_forecast_task():
#     """Weather Forecast Task."""
#     import batch.bin.forecastweather_wu as forecastweather
#     dsnfile = current_app.config['DB_CONNECT_FILE']
#     loc = forecastweather.DbFetchLocations(dsnfile, 'UTTARAKHAND')
#     all_loc = loc.fetch_locations()
#     forecast_type = ['hourly', '15min']
#     for loc, lat, lon in all_loc:
#         print loc, lat, lon
#         for f_type in forecast_type:
#             forecast = forecastweather.ForecastData()
#             forecast.fetch_data(lat, lon, f_type, 'm')
#             data = forecast.parse_forecast(loc, f_type)
#             # print data[1:]
#             dbupdate = forecastweather.DbUploadData(dsnfile, data=data[1:])
#             # print dbupdate.data
#             dbupdate.db_upload_data()


# @periodic_task(run_every=(crontab(minute='*/15')))
# def weather_currentconditions_task():
#     """Weather Current Condition Task."""
#     import batch.bin.currentconditionsweather_wu as ccw
#     dsnfile = current_app.config['DB_CONNECT_FILE']
#     loc = ccw.DbFetchLocations(dsnfile, 'UTTARAKHAND')
#     all_loc = loc.fetch_locations()
#     for loc, lat, lon in all_loc:
#         print loc, lat, lon
#         current_conditions = ccw.CurrentConditionsData()
#         current_conditions.fetch_data(lat, lon, 'm')
#         data = current_conditions.parse_current(loc)
#         # print data[1:]
#         dbupdate = ccw.DbUploadData(dsnfile, data=data[1:])
#         # print dbupdate.data
#         dbupdate.db_upload_data()

@periodic_task(run_every=(crontab(minute=0, hour=0)))
def clean_old_data_files_os():
    """Delete files older than 7 days in data and data_archive dir."""
    now = time.time()
    cutoff = now - (7 * 86400)
    dir_to_search = os.getcwd() + '/ems/batch/'
    folder = ['data', 'data_archive']
    pattern = ['csv', 'dsf', 'xls', 'xlsx']
    for path in [dir_to_search + d for d in folder]:
        for r, d, f in os.walk(path):
            for fil in [os.path.join(r, files) for p in pattern
                        for files in f
                        if files.endswith(p) and
                        os.path.isfile(os.path.join(r, files))]:
                try:
                    t = os.stat(fil)
                    c = t.st_ctime
                    if c < cutoff:
                        print( "Removing " + fil)
                        os.remove(fil)
                except Exception as e:
                        print( "Error" + str(e))
                else:
                    print (fil + " removed")

# Shifted to Cron
# @periodic_task(run_every=timedelta(seconds=90))
# def gujsldc_crawl():
#     """Crawl the gujsldc site every 1min."""
#     import batch.bin.gujsldc_webcrawler as gujsldc_webcrawler
#     dsnfile = current_app.config['DB_CONNECT_FILE']
#     gujsldc_webcrawler.get_rtd_agg(dsnfile)


# @periodic_task(run_every=timedelta(seconds=90))
# def gujsldc_wind_crawl():
#     """Crawl the gujsldc wind site every 1min."""
#     import batch.bin.gujsldc_webcrawler as gujsldc_webcrawler
#     dsnfile = current_app.config['DB_CONNECT_FILE']
#     gujsldc_webcrawler.get_rtd_wind(dsnfile)


# @periodic_task(run_every=(crontab(minute=0, hour='*/2')))
def gujsldc_declared_capacity():
    """Weather Current Condition Task."""
    logger.info("Started GUJSLDC Declared Capcity")
    import ems.batch.bin.gujsldc_declaredcapacity_crawler as gdc
    import ems.batch.bin.sql_load_lib as sql_load_lib
    dsnfile = current_app.config['DB_CONNECT_FILE']
    discom = 'GUVNL'
    if time.tzname[0] == 'IST':
        local_now = datetime.today()
    else:
        dest_tz = pytz.timezone('Asia/Kolkata')
        ts = time.time()
        utc_now = datetime.utcfromtimestamp(ts)
        local_now = utc_now.replace(tzinfo=pytz.utc).astimezone(dest_tz)

    startdate = local_now
    enddate = local_now + timedelta(1)
    logger.info("StartDate {} EndDate {}".
                format(startdate.strftime("%d-%m-%Y"),
                       enddate.strftime("%d-%m-%Y")))
    for date in gdc.daterange(startdate, enddate):
        try:
            gdc.get_generator_schedule(dsnfile, date.strftime("%d-%m-%Y"))
        except Exception as e:
            logger.info("Exception: %s", e)
    sql_load_lib.sql_sp_internal_sch_update(dsnfile, discom)
    logger.info("Finished GUJSLDC Declared Capcity")


# @periodic_task(run_every=(crontab(minute='*/16')))
def adani_scada_load():
    """Weather Current Condition Task."""
    logger.info("Started ADANI SCADA Load")
    import ems.batch.bin.adani_scada_data_load as asdl
    import ems.batch.bin.sql_load_lib as sql_load_lib
    dsnfile = current_app.config['DB_CONNECT_FILE']
    discom = 'ADANI'
    try:
        asdl.run()
    except Exception as e:
        logger.info("Exception: %s", e)
    sql_load_lib.sql_sp_adani_scada_blk(dsnfile, discom)
    logger.info("Finished ADANI SCADA")


@ems.route('/get_state_geojson/<state>')
@cross_origin(origin='*', headers=['Content-Type', 'Authorization'])
def get_state_geojson(state):
    """Filter the state geojson from a file."""
    import geojson
    filename = current_app.config['STATE_JSON_FILE']
    geojson_template = {
        "type": "FeatureCollection",
        "features": []}
    with open(filename) as data_file:
        data = geojson.load(data_file)
        for i, f in enumerate(data['features']):
            if f.get('id').upper() == state.upper():
                geojson_template["features"].append(f)
                return geojson.dumps(f)

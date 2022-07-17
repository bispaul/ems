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

import logging
from flask_mysqldb import MySQL
from flask import Flask, request, redirect, url_for  # , render_template
from celery import Celery
from config import config, Config, GoogleConfig, GoogleDevConfig
from flask_mail import Mail
# from flask_sqlalchemy import SQLAlchemy
from flask_bootstrap import Bootstrap
from flask_wtf.csrf import CSRFProtect
from flask_user import SQLAlchemyAdapter, UserManager
from flask_user.signals import user_logged_in
from flask_compress import Compress
import datetime
# from healthcheck import HealthCheck, EnvironmentDump
# from flask_script import Manager
from flask_cors import CORS, cross_origin


mysql = MySQL()
celery = Celery(__name__, broker=Config.CELERY_BROKER_URL)
# celery = Celery(__name__, broker=GoogleConfig.CELERY_BROKER_URL)
# celery = Celery(__name__, broker=GoogleDevConfig.CELERY_BROKER_URL)
# # db = SQLAlchemy()
mail = Mail()
compress = Compress()
bootstrap = Bootstrap()
csrfprotect = CSRFProtect()
cors = CORS()
cross_origin = cross_origin


def create_app(config_name):
    app = Flask(__name__)
    # wrap the flask app and give a heathcheck url
    # health = HealthCheck(app, "/healthcheck")
    # envdump = EnvironmentDump(app, "/environment")
    app.config.from_object(config[config_name])
    config[config_name].init_app(app)
    with app.app_context():
        # Initialize Flask-SQLAlchemy
        from . import models
        models.db.init_app(app)
        # models.db.drop_all()
        # models.db.create_all()
        # Setup Flask-User
        db_adapter = SQLAlchemyAdapter(models.db, models.User)
        user_manager = UserManager(db_adapter, app)
        # Initialize Flask-Mail
        mail.init_app(app)
        mysql.init_app(app)
        compress.init_app(app)
        bootstrap.init_app(app)
        csrfprotect.init_app(app)
        celery.conf.update(app.config)
        cors.init_app(app)
        # celery.config_from_object(config[config_name])
        # Setup WTForms CsrfProtect
        # CsrfProtect(app)
        # Define bootstrap_is_hidden_field for
        # flask-bootstrap's bootstrap_wtf.html
        from wtforms.fields import HiddenField

        def is_hidden_field_filter(field):
            return isinstance(field, HiddenField)

        app.jinja_env.globals['bootstrap_is_hidden_field'] = \
            is_hidden_field_filter
        # Configure logging
        if not app.testing:
            logging.basicConfig(level=logging.INFO)

        # Register the ems blueprint.
        from . import ems_main
        app.register_blueprint(ems_main.ems, url_prefix='/ems')

        # from .mod_auth.controllers import mod_auth as auth_module
        # app.register_blueprint(auth_module)
        # Add a default root route.
        @app.route("/")
        def home_page():
            # return redirect(url_for('ems.login'))
            return redirect(url_for('user.login'))

        @app.errorhandler(500)
        def server_error_500(e):
            # return render_template("500.html")
            return """
            An internal error occurred: <pre>{}</pre>
            See logs for full stacktrace.
            """.format(e), 500

        @app.errorhandler(404)
        def server_error_404(e):
            # return render_template("404.html")
            return """
            An internal error occurred: <pre>{}</pre>
            See logs for full stacktrace.
            """.format(e), 404

        # Create a health check handler. Health checks are used when running on
        # Google Compute Engine by the load balancer to determine which
        # instances can serve traffic. Google App Engine also uses health
        #  checking, but accepts any non-500 response as healthy.
        @app.route('/_ah/health')
        def health_check():
            return 'ok', 200

        # Flask-User Signals
        @user_logged_in.connect_via(app)
        def _track_logins(sender, user, **extra):
            logging.info('Track Login: User %s logged in', user)
            if user.login_count:
                user.login_count += 1
            else:
                user.login_count = 1
            user.last_login_ip = user.current_login_ip
            user.last_login_at = user.current_login_at
            user.current_login_ip = request.remote_addr
            user.current_login_at = datetime.datetime.now()
            models.db.session.commit()
    return app

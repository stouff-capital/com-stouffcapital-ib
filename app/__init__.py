import logging
import os

from flask import Flask, request, current_app
from flask_basicauth import BasicAuth
from flask_migrate import Migrate
from flask_sqlalchemy import SQLAlchemy
from flask_alchemydumps import AlchemyDumps

import sentry_sdk
from sentry_sdk.integrations.flask import FlaskIntegration

from config import Config


sentry_sdk.init(
    dsn=Config.SENTRY_DSN,
    integrations=[FlaskIntegration()]
)

basic_auth = BasicAuth()
db = SQLAlchemy()
alchemydumps = AlchemyDumps()
migrate = Migrate()

def create_app(config_class=Config):
    app = Flask(__name__)
    app.config.from_object(config_class)

    basic_auth.init_app(app)
    db.init_app(app)
    migrate.init_app(app, db)

    alchemydumps.init_app(app, db)

    from app.reports import bp as reports_bp
    app.register_blueprint(reports_bp)

    from app.ibcontracts import bp as ibcontracts_bp
    app.register_blueprint(ibcontracts_bp)

    from app.ibexecutionrestfuls import bp as ibexecutionrestfuls_bp
    app.register_blueprint(ibexecutionrestfuls_bp)

    from app.ibsymbology import bp as ibsymbology_bp
    app.register_blueprint(ibsymbology_bp)


    if not app.debug and not app.testing:
        app.logger.setLevel(logging.INFO)
        app.logger.info('com-stouffcapital-ib startup')

    return app


from app import models
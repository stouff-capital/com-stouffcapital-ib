import logging
import os

from flask import Flask, request, current_app
from flask_basicauth import BasicAuth
from flask_migrate import Migrate
from flask_sqlalchemy import SQLAlchemy

from config import Config

basic_auth = BasicAuth()
db = SQLAlchemy()
migrate = Migrate()

def create_app(config_class=Config):
    app = Flask(__name__)
    app.config.from_object(config_class)

    basic_auth.init_app(app)
    db.init_app(app)
    migrate.init_app(app, db)

    from app.executions import bp as executions_bp
    app.register_blueprint(executions_bp)

    from app.bridge import bp as bridge_bp
    app.register_blueprint(bridge_bp)

    from app.reports import bp as report_bp
    app.register_blueprint(report_bp)

    if not app.debug and not app.testing:
        app.logger.setLevel(logging.INFO)
        app.logger.info('com-stouffcapital-ib startup')

    return app

from app import models

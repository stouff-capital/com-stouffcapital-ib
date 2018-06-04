from flask import Blueprint

bp = Blueprint('executions', __name__)

from app.executions import routes

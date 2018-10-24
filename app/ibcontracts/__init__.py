from flask import Blueprint

bp = Blueprint('ibcontracts', __name__)

from app.ibcontracts import routes

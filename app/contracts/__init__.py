from flask import Blueprint

bp = Blueprint('contracts', __name__)

from app.contracts import routes

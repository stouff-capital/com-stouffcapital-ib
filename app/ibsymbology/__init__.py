from flask import Blueprint

bp = Blueprint('ibsymbology', __name__)

from app.ibsymbology import routes

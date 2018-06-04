from flask import Blueprint

bp = Blueprint('bridge', __name__)

from app.bridge import routes

from flask import Blueprint

bp = Blueprint('ibexecutionrestfuls', __name__)

from app.ibexecutionrestfuls import routes

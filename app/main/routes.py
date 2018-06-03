from datetime import datetime
from flask import render_template, flash, redirect, url_for, request, g, jsonify, current_app
from app import db
from app.models import Contract, Execution
from app.main import bp

@bp.route('/', methods=['GET', 'POST'])
@bp.route('/index', methods=['GET', 'POST'])
def index():
    return jsonify( {'status': 'ok'} )


@bp.route('/executions', methods=['GET'])
def list():
    return jsonify( {'status': 'ok'} )


@bp.route('/executions', methods=['GET'])
def read():
    return jsonify( {'status': 'ok'} )


@bp.route('/executions', methods=['POST'])
def create():
    data = request.get_json()

    # query if contract already exists

    # insert executions

    return jsonify( {'status': 'ok'} )

from datetime import datetime
from flask import render_template, flash, redirect, url_for, request, g, jsonify, current_app
from app import db
from app.models import Contract, Execution, Bbg
from app.executions import bp

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
    try:
        data = request.get_json()
    except:
        return jsonify( {'status': 'error', 'error': 'missing data'} )

    if data == None:
        return jsonify( {'status': 'error', 'error': 'missing data'} )

    # query if contract already exists
    current_app.logger.info('check contract: ' + data['localSymbol'])
    contract = Contract.query.get(data['localSymbol'])
    if contract != None:
        current_app.logger.info('existing contract')
    else:
        if data['localSymbol'] == '':
            localSymbol = data['symbol']
        else:
            localSymbol=data['localSymbol']

        contract = Contract(
            secType=data['secType'],
            localSymbol=localSymbol,
            symbol=data['symbol'],
            currency=data['currency'],
            exchange=data['exchange'],
            primaryExchange=data['primaryExchange'],
            lastTradeDateOrcontractMonth=data['lastTradeDateOrcontractMonth'],
            multiplier=data['multiplier'],
            strike=data['strike'],
            right=data['right']
        )
        db.session.add(contract)
        db.session.commit()
        current_app.logger.info('new contract')


    # insert executions
    current_app.logger.info('check execution: ' + data['execId'])
    execution = Execution.query.get(data['execId'])
    if execution != None:
        current_app.logger.info('existing execution')
    else:

        shares = abs(data['shares'])
        cumQty = abs(data['cumQty'])
        if data['side'][0].upper() == 'B':
            pass
        elif data['side'][0].upper() == 'S':
            shares = -shares
            cumQty = -cumQty

        execution = Execution(
            execId=data['execId'],
            orderId=data['orderId'],
            asset=contract, # object
            # time=data['orderId'],
            acctNumber=data['acctNumber'],
            exchange=data['exchange'],
            side=data['side'],
            shares=shares,
            price=data['price'],
            avgPrice=data['avgPrice'],
            permId=data['permId']

        )
        db.session.add(execution)
        db.session.commit()
        current_app.logger.info('new execution')

    return jsonify( {
            'status': 'ok',
            'data': data,
            'contract': {'localSymbol': contract.localSymbol},
            'order': {'orderId': execution.orderId},
            'execution': {'execId': execution.execId}
    } )

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
    executions = Execution.query.all()
    for exec in executions:
        current_app.logger.info(exec.contract.localSymbol)
        bbg = exec.contract.bbg
        if bbg != None:
            current_app.logger.info(exec.contract.bbg.ticker)
    return jsonify( {'status': 'ok'} )



@bp.route('/executions/time/<date_str>', methods=['GET'])
def list_limit_date(date_str):
    executions = Execution.query.filter(Execution.time >= date_str).all()

    execs = {} # dict with execId as key
    for exec in executions:
        one_exec = {}

        one_exec['execId'] = exec.execId
        one_exec['orderId'] = exec.orderId
        one_exec['time'] = exec.time
        one_exec['acctNumber'] = exec.acctNumber
        one_exec['exchange'] = exec.exchange
        one_exec['side'] = exec.side
        one_exec['shares'] = exec.shares  # execQty
        one_exec['cumQty'] = exec.cumQty
        one_exec['price'] = exec.price
        one_exec['avgPrice'] = exec.avgPrice
        one_exec['permId'] = exec.permId


        one_exec['contract'] = {
            'localSymbol': exec.contract.localSymbol
        }
        bbg = exec.contract.bbg
        if bbg != None:
            one_exec['bbg'] = {
                'ticker': exec.contract.bbg.ticker,
                'bbgIdentifier': exec.contract.bbg.bbgIdentifier,
                'bbgUnderylingId': exec.contract.bbg.bbgUnderylingId,
                'internalUnderlying': exec.contract.bbg.internalUnderlying
            }
        execs[exec.execId] = one_exec

    return jsonify( {
        'status': 'ok',
        'time': date_str,
        'count': len(executions),
        'executions': execs
    } )



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

        if data['strike'] > 0:
            strike = data['strike']
        else:
            strike = None

        if data['right'] != '':
            right = data['right']
        else:
            right = None

        contract = Contract(
            secType=data['secType'],
            localSymbol=localSymbol,
            symbol=data['symbol'],
            currency=data['currency'],
            exchange=data['exchange'],
            primaryExchange=data['primaryExchange'],
            lastTradeDateOrContractMonth=data['lastTradeDateOrContractMonth'], #datetime
            multiplier=data['multiplier'],
            strike=strike,
            right=right
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
            time=datetime.strptime( data['time'], '%Y%m%d  %H:%M:%S'), # 20180604  13:32:52
            acctNumber=data['acctNumber'],
            exchange=data['exchange'],
            side=data['side'],
            shares=shares,
            cumQty=cumQty,
            price=data['price'],
            avgPrice=data['avgPrice'],
            permId=data['permId']

        )
        db.session.add(execution)
        db.session.commit()
        current_app.logger.info('new execution')

    return jsonify( {
            'status': 'ok',
            'inputData': data,
            'contract': {'localSymbol': contract.localSymbol},
            'order': {'orderId': execution.orderId},
            'execution': {'execId': execution.execId}
    } )

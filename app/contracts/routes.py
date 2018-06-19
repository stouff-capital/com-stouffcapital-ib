from datetime import datetime
from flask import render_template, flash, redirect, url_for, request, g, jsonify, current_app
from app import db
from app.models import Contract, Bbg
from app.contracts import bp


@bp.route('/contracts', methods=['GET'])
def contract_index():
    return jsonify( {'status': 'ok'} )


@bp.route('/contracts', methods=['POST'])
def contract_create():

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
        if 'localSymbol' in data:
            if data['localSymbol'] == '':
                localSymbol = data['symbol']
            else:
                contract = Contract(
                    localSymbol=localSymbol
                )
        else:
            return jsonify( {'status': 'error', 'error': 'missing localSymbol'} )

        if 'strike' in data:
            if data['strike'] > 0:
                contract.strike = data['strike']

        if 'right' in data:
            if data['right'] != '':
                contract.right = data['right']

        # no processing
        std_fields = ['secType', 'symbol', 'currency', 'exchange', 'primaryExchange', 'lastTradeDateOrContractMonth', 'multiplier' ]
        for std_field in std_fields:
            if std_field in data:
                setattr(contract, std_field, data[std_field])

        db.session.add(contract)
        db.session.commit()
        current_app.logger.info('new contract')



    return jsonify( {'status': 'ok'} )

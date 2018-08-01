from datetime import datetime
from flask import render_template, flash, redirect, url_for, request, g, jsonify, current_app
from app import db
from app.models import Contract, Execution, Bbg
from app.bridge import bp


@bp.route('/bridge', methods=['GET'])
def list():
    return jsonify( {'status': 'ok'} )


@bp.route('/bridge/backup', methods=['GET'])
def brige_backup():
    output = {'status': 'ok', 'data': []}
    bridges = Bbg.query.all()
    for bridge in bridges:
        contract = bridge.contract
        output['data'].append({
            'ticker': bridge.ticker,
            'bbgIdentifier': bridge.bbgIdentifier,
            'bbgUnderylingId': bridge.bbgUnderylingId,
            'internalUnderlying': bridge.internalUnderlying,
            'contract_localSymbol': contract.localSymbol
        })

    return jsonify( output )


@bp.route('/bridge/missing', methods=['GET'])
def missing():
    existing_ids = [bbg.contract_localSymbol for bbg in Bbg.query.all() ]
    contracts = Contract.query.filter(Contract.localSymbol.notin_(existing_ids))

    contracts_ib = [contract.localSymbol for contract in contracts ]
    return jsonify( {'status': 'ok', 'ib': contracts_ib, 'missing': contracts_ib} )


@bp.route('/bridge', methods=['POST'])
def create():
    try:
        data = request.get_json()
    except:
        return jsonify( {'status': 'error', 'error': 'missing data'} )

    if data == None:
        return jsonify( {'status': 'error', 'error': 'missing data'} )

    contract = Contract.query.get(data['localSymbol'])
    if contract != None:
        current_app.logger.info('bridge:: existing contract')
        bbg = Bbg.query.get(data['localSymbol'])
        if bbg != None:
            # update ?
            current_app.logger.info('bridge:: already in bridge')
            return jsonify( {'status': 'error', 'error': 'already in bridge'} )
            pass
        else:
            bbg = Bbg(
                ticker=data['ticker'],
                contract=contract #object
            )

            bbg_fields = ['bbgIdentifier', 'bbgUnderylingId', 'internalUnderlying']
            for bbg_field in bbg_fields:
                if bbg_field in data:
                    setattr(bbg, bbg_field, data[bbg_field])



            db.session.add(bbg)
            db.session.commit()
            current_app.logger.info('new bridge')

            return jsonify( {'status': 'ok', 'inputData': data} )
    else:
        current_app.logger.info('bridge:: missing contract')
        return jsonify( {'status': 'error', 'error': 'missing contract'} )


def patch_ticker_marketplace(ticker):
    ticker = ticker.upper()
    mps = {
        "GY": "GR",
        "JT": "JP",
        "UN": "US",
        "UQ": "US",
        "UW": "US",
        "UR": "US",
        "SQ": "SM",
        "SE": "SW"
    }

    for mp in mps:
        ticker = ticker.replace(f' {mp} ', f' {mps[mp]} ')

    return ticker


def bbgticker_to_ibsymbol(desired_ticker, fullBridge = None):
    bridge = Bbg.query.filter_by(ticker=desired_ticker.upper()).first()

    if bridge == None:
        bridge = Bbg.query.filter_by(ticker=patch_ticker_marketplace(desired_ticker).upper()).first()

        if bridge == None and fullBridge != None:
            for entry in fullBridge:
                if patch_ticker_marketplace(entry.ticker) == patch_ticker_marketplace(desired_ticker):
                    bridge = entry
                    break

    return bridge


@bp.route('/bridge/bbg/ib', methods=['POST'])
def conv_bbgticker_into_ibsymbol():
    try:
        data = request.get_json()
    except:
        return jsonify( {'status': 'error', 'error': 'missing data'} )

    if data == None or 'ticker' not in data:
        return jsonify( {'status': 'error', 'error': 'missing data'} )

    bridge = bbgticker_to_ibsymbol(data['ticker'])

    if bridge == None:
        return jsonify( {'status': 'error',
            'error': 'not able to translate, ask user',
            'ticker': data['ticker'] } )

    contract = bridge.contract
    return jsonify({'status': 'ok', 'input': data['ticker'].upper(),
        'localSymbol': contract.localSymbol,
        'bbgIdentifier': bridge.bbgIdentifier, 'bbgUnderylingId': bridge.bbgUnderylingId, 'internalUnderlying': bridge.internalUnderlying})


@bp.route('/bridge/bbgs/ibs', methods=['POST'])
def conv_bbgtickers_into_ibsymbols():
    try:
        data = request.get_json()
    except:
        return jsonify( {'status': 'error', 'error': 'missing data'} )

    if data == None or 'tickers' not in data:
        return jsonify( {'status': 'error', 'error': 'missing data'} )

    output = {'input': [],
        'output': []}

    bridges = Bbg.query.all()
    for ticker in data['tickers']:
        output['input'].append(ticker)

        bridge = bbgticker_to_ibsymbol(ticker, bridges)

        if bridge == None:
            output['output'].append( {'status': 'error', 'error': 'not able to translate, ask user', 'ticker': ticker} )
        else:
            contract = bridge.contract
            output['output'].append({'status': 'ok', 'input': ticker.upper(),
                'localSymbol': contract.localSymbol,
                'bbgIdentifier': bridge.bbgIdentifier, 'bbgUnderylingId': bridge.bbgUnderylingId, 'internalUnderlying': bridge.internalUnderlying})

    return jsonify( output )

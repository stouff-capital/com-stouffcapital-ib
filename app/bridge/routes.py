from datetime import datetime
from flask import render_template, flash, redirect, url_for, request, g, jsonify, current_app
from app import db
from app.models import Contract, Execution, Bbg
from app.bridge import bp


@bp.route('/bridge', methods=['GET'])
def list():
    return jsonify( {'status': 'ok'} )


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

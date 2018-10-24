from datetime import datetime
from flask import render_template, flash, redirect, url_for, request, g, jsonify, current_app
from app import db
from app.models import Ibcontract, Ibexecutionrestful, Ibsymbology
from app.ibsymbology import bp

from app.bridge import routes as routes_bridge


@bp.route('/ibsymbology', methods=['GET'])
def list():
    return jsonify( {'status': 'ok', 'controller': 'ibsymbology'} )


@bp.route('/ibsymbology/backup', methods=['GET'])
def ibsymbology_backup():
    return jsonify( {'status': 'ok', 'controller': 'ibsymbology', 'data': [{
            'ticker': ibsymbology.ticker,
            'bbgIdentifier': ibsymbology.bbgIdentifier,
            'bbgUnderylingId': ibsymbology.bbgUnderylingId,
            'internalUnderlying': ibsymbology.internalUnderlying,
            'contract_conid': ibsymbology.ibcontract.conid,
            'contract_symbol': ibsymbology.ibcontract.symbol
            } for ibsymbology in Ibsymbology.query.all()
        ]
                     })


@bp.route('/ibsymbology/missing', methods=['GET'])
def missing():
    existing_ids = [asset.ibcontract_conid for asset in Ibsymbology.query.all() ]
    ibcontracts = Ibcontract.query.filter(Ibcontract.conid.notin_(existing_ids))

    ibcontracts_ib = [{'conid': ibcontract.conid, 'assetCategory': ibcontract.assetCategory, 'symbol': ibcontract.symbol, 'multiplier': float(ibcontract.multiplier), 'underlyingSymbol': ibcontract.underlyingSymbol}
                      for ibcontract
                      in ibcontracts
                      ]
    return jsonify( {'status': 'ok', 'missing': ibcontracts_ib, 'controller': 'ibsymbology'} )


@bp.route('/ibsymbology', methods=['POST'])
def create():
    try:
        data = request.get_json()
    except:
        return jsonify( {'status': 'error', 'error': 'missing input data', 'controller': 'ibsymbology'} )

    if data == None:
        return jsonify( {'status': 'error', 'error': 'missing input data', 'controller': 'ibsymbology'} )

    try:
        ibcontract = Ibcontract.query.get(data['conid'])
    except:
        return jsonify( {'status': 'error', 'error': 'missing conid', 'inputData': data, 'controller': 'ibsymbology'} )
    if ibcontract != None:
        current_app.logger.info(f'ibsymbology:: check IbContract {data["conid"]} done, already in db')
        ibsymbology = Ibsymbology.query.get(data['conid'])
        if ibsymbology != None:
            # update ?
            current_app.logger.info(f'{data["conid"]} already in ibsymbology')
            return jsonify( {'status': 'error', 'error': 'already in ibsymbology', 'inputData': data, 'ibsymbology': {'conid': ibsymbology.ibcontract_conid, 'ticker': ibsymbology.ticker}, 'controller': 'ibsymbology'} )
            pass
        else:
            try:
                ibsymbology = Ibsymbology(
                    ticker=data['ticker'],
                    ibcontract=ibcontract #object
                )

                bbg_fields = ['bbgIdentifier', 'bbgUnderylingId', 'internalUnderlying']
                for bbg_field in bbg_fields:
                    if bbg_field in data:
                        setattr(ibsymbology, bbg_field, data[bbg_field])


                db.session.add(ibsymbology)
                db.session.commit()
                current_app.logger.info(f'new ibsymbology:: conid: {data["conid"]}, ticker: {data["ticker"]}')

                return jsonify( {'status': 'ok', 'message': 'successfully created in ibsymbology', 'inputData': data, 'ibsymbology': {'conid': ibsymbology.ibcontract_conid, 'ticker': ibsymbology.ticker}, 'controller': 'ibsymbology'} )
            except:
                return jsonify( {'status': 'error', 'error': 'missing ticker', 'inputData': data, 'controller': 'ibsymbology'} )

    else:
        current_app.logger.info(f'ibsymbology:: {data["conid"]} not in Ibcontract')
        return jsonify( {'status': 'error', 'error': 'missing ibcontract', 'inputData': data, 'controller': 'ibsymbology'} )

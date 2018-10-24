from datetime import datetime
from flask import render_template, flash, redirect, url_for, request, g, jsonify, current_app
from app import db
from app.models import Ibcontract, Ibsymbology
from app.ibcontracts import bp


@bp.route('/ibcontracts', methods=['GET'])
def ibcontract_index():
    return jsonify( {'status': 'ok', 'controller': 'ibcontracts'} )


@bp.route('/ibcontracts/<conid>/exists', methods=['GET'])
def ibcontract_exists(conid):

    ibcontract = Ibcontract.query.get(conid)
    if ibcontract != None:
        return jsonify( {'status': 'ok', 'controller': 'ibcontracts', 'input': conid, 'exists': True} )
    else:
        return jsonify( {'status': 'ok', 'controller': 'ibcontracts', 'input': conid, 'exists': False} )


def ibcontract_create_one(data):
    # query if ibcontract already exists
    try:
        current_app.logger.info(f'check ibcontract: {data["conid"]}')

        data['conid'] = int(data['conid'])

        ibcontract = Ibcontract.query.get(data['conid'])
    except:
        return jsonify( {'status': 'error', 'error': 'missing conid', 'controller': 'ibcontracts'} )
    if ibcontract != None:
        current_app.logger.info('existing ibcontract')
    else:
        ibcontract = Ibcontract(
                    conid=data['conid']
                )

        if 'strike' in data:
            if data['strike'] != '':
                data['strike']  = float(data['strike'] )
                if data['strike'] > 0:
                    ibcontract.strike = data['strike']

        # text: no processing
        std_fields = [ 'assetCategory', 'symbol', 'description', 'isin', 'listingExchange', 'underlyingSymbol', 'underlyingSecurityID', 'underlyingListingExchange', 'putCall', 'underlyingCategory', 'subCategory', 'currency' ]
        for std_field in std_fields:
            if std_field in data:
                if data[std_field] != '':
                    setattr(ibcontract, std_field, data[std_field])

        # numeric fields
        std_fields = [ 'underlyingConid' ]
        for std_field in std_fields:
            if std_field in data:
                if data[std_field] != '':
                    data[std_field] = int(data[std_field])
                    setattr(ibcontract, std_field, data[std_field])

        std_fields = [ 'multiplier' ]
        for std_field in std_fields:
            if std_field in data:
                if data[std_field] != '':
                    data[std_field] = float(data[std_field])
                    setattr(ibcontract, std_field, data[std_field])

        # date
        std_fields = [ 'expiry', 'maturity', 'issueDate' ]
        for std_field in std_fields:
            if std_field in data:
                if data[std_field] != '':
                    if len(data[std_field]) == 8:
                        date_format = '%Y%m%d'
                    elif len(data[std_field]) == 10:
                        date_format = '%Y-%m-%d'
                    setattr(ibcontract, std_field, datetime.strptime(data[std_field], date_format) )

        db.session.add(ibcontract)
        db.session.commit()
        current_app.logger.info(f'new ibcontract: {data["conid"]}')

    return jsonify( {'status': 'ok', 'message': f'ibcontract {data["conid"]} successfully created', 'controller': 'ibcontracts'} )


@bp.route('/ibcontracts', methods=['POST'])
def ibcontract_create():

    try:
        data = request.get_json()
    except:
        return jsonify( {'status': 'error', 'error': 'missing data', 'controller': 'ibcontracts'} )

    if data == None:
        return jsonify( {'status': 'error', 'error': 'missing data', 'controller': 'ibcontracts'} )

    return ibcontract_create_one(data)

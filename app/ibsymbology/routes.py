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
    bridge = Ibsymbology.query.filter_by(ticker=desired_ticker.upper()).first()

    if bridge == None:
        bridge = Ibsymbology.query.filter_by(ticker=patch_ticker_marketplace(desired_ticker).upper()).first()

        if bridge == None and fullBridge != None:
            for entry in fullBridge:
                if patch_ticker_marketplace(entry.ticker) == patch_ticker_marketplace(desired_ticker):
                    bridge = entry
                    break

    return bridge

@bp.route('/ibsymbology/bbgs/ibs', methods=['POST'])
def conv_bbgtickers_into_ibsymbols():
    try:
        data = request.get_json()
    except:
        return jsonify( {'status': 'error', 'error': 'missing input data', 'controller': 'ibsymbology'} )

    if data == None or 'tickers' not in data:
        return jsonify( {'status': 'error', 'error': 'missing input data', 'controller': 'ibsymbology'} )

    output = {'input': [],
        'output': [],
        'controller': 'ibsymbology'}

    bridges = Ibsymbology.query.all()
    for ticker in data['tickers']:
        output['input'].append(ticker)

        bridge = bbgticker_to_ibsymbol(ticker, bridges)

        if bridge == None:
            output['output'].append( {'status': 'error', 'error': 'not able to translate, ask user', 'ticker': ticker, 'input': ticker} )
        else:
            output['output'].append({'status': 'ok', 'input': ticker.upper(),
                'localSymbol': bridge.ibcontract.symbol,
                'bbgIdentifier': bridge.bbgIdentifier, 'bbgUnderylingId': bridge.bbgUnderylingId, 'internalUnderlying': bridge.internalUnderlying})

    return jsonify( output )


@bp.route('/ibsymbology/recent/<date_str>', methods=['GET'])
def new_manual_mapping(date_str):

    return jsonify( {
        'date_limit': date_str,
        'contracts': [
            {
                'ticker': ibsymbology.ticker,
                'bbgIdentifier': ibsymbology.bbgIdentifier,
                'bbgUnderylingId': ibsymbology.bbgUnderylingId,
                'internalUnderlying': ibsymbology.internalUnderlying,
                'ibcontract_conid': int(ibsymbology.ibcontract_conid),
                'ibcontract': {
                    'assetCategory': ibsymbology.ibcontract.assetCategory,
                    'symbol': ibsymbology.ibcontract.symbol,
                    'description': ibsymbology.ibcontract.description,
                    'conid': int(ibsymbology.ibcontract.conid),
                    'underlyingConid': ibsymbology.ibcontract.underlyingConid,
                    'underlyingSymbol': ibsymbology.ibcontract.underlyingSymbol,
                    'multiplier': int(ibsymbology.ibcontract.multiplier) if ibsymbology.ibcontract.multiplier != None else None,
                    'strike': float(ibsymbology.ibcontract.strike) if ibsymbology.ibcontract.strike != None else None,
                    'expiry': ibsymbology.ibcontract.expiry,
                    'putCall': ibsymbology.ibcontract.putCall,
                    'maturity': ibsymbology.ibcontract.maturity,
                    'currency': ibsymbology.ibcontract.currency
                }
            }
            for ibsymbology in Ibsymbology.query.filter(Ibsymbology.created >= date_str).all()
        ]
    } )


@bp.route('/ibsymbology/recent/<date_str>/checks', methods=['GET'])
def new_manual_mapping_opportunistic_checks(date_str):
    new_contracts = [
        {
            'ticker': ibsymbology.ticker,
            'bbgIdentifier': ibsymbology.bbgIdentifier,
            'bbgUnderylingId': ibsymbology.bbgUnderylingId,
            'internalUnderlying': ibsymbology.internalUnderlying,
            'ibcontract_conid': int(ibsymbology.ibcontract_conid),
            'ibcontract': {
                'assetCategory': ibsymbology.ibcontract.assetCategory,
                'symbol': ibsymbology.ibcontract.symbol,
                'description': ibsymbology.ibcontract.description,
                'conid': int(ibsymbology.ibcontract.conid),
                'underlyingConid': ibsymbology.ibcontract.underlyingConid,
                'underlyingSymbol': ibsymbology.ibcontract.underlyingSymbol,
                'multiplier': int(ibsymbology.ibcontract.multiplier) if ibsymbology.ibcontract.multiplier != None else None,
                'strike': float(ibsymbology.ibcontract.strike) if ibsymbology.ibcontract.strike != None else None,
                'expiry': ibsymbology.ibcontract.expiry,
                'putCall': ibsymbology.ibcontract.putCall,
                'maturity': ibsymbology.ibcontract.maturity,
                'currency': ibsymbology.ibcontract.currency
            }
        }
        for ibsymbology in Ibsymbology.query.filter(Ibsymbology.created >= date_str).all()
        ]

    data = []
    for newAsset in new_contracts:
        for subkey_ibcontract in newAsset['ibcontract']:
            newAsset["ibcontract_" + subkey_ibcontract] = newAsset['ibcontract'][subkey_ibcontract]

        del newAsset['ibcontract']
        data.append(newAsset)

    df = pd.DataFrame(data)

    df.ibcontract_expiry = pd.to_datetime( df.ibcontract_expiry, format="%a, %d %b %Y %H:%M:%S %Z" )
    df['ibExpiryPart'] = df.ibcontract_expiry.dt.strftime("%m/%d/%y")
    df['bbgTicker_expiry'] = df.ticker.str.extract(r'([\d]{2}/[\d]{2}/[\d]{2})')

    df['bbgTicker_putCall'] = df.ticker.str.extract(r'\s([PpCp])[\d]+')

    df_options = df[df.ibcontract_assetCategory == 'OPT']


    return jsonify( {
        'new_assets': [],
        'option_issues': {
            'expiryDate': [],
            'putCall': []
        }
        } )



@bp.route('/ibsymbology/remove/<int:conid>', methods=['GET'])
def remove_mapping(conid):
    try:
        ibsymbology = Ibsymbology.query.get(conid)
    except:
        return jsonify( {'status': 'error', 'error': 'missing conid', 'inputData': conid, 'controller': 'ibsymbology'} )
    if ibsymbology != None:
        db.session.delete(ibsymbology)
        db.session.commit()
        current_app.logger.info(f'delete ibsymbology:: conid: {conid}')

        return jsonify({'status': 'ok', 'input': conid, 'controller': 'ibsymbology'})
    else:
        return jsonify( {'status': 'error', 'error': 'missing entry in database', 'inputData': conid, 'controller': 'ibsymbology'} )

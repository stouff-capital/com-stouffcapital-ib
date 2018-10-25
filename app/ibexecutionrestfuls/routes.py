import datetime
from flask import render_template, flash, redirect, url_for, request, g, jsonify, current_app
from app import db
from app.models import Ibcontract, Ibexecutionrestful, Ibsymbology
from app.ibexecutionrestfuls import bp

@bp.route('/ibexecutionrestfuls', methods=['GET'])
def index():
    return jsonify( {'status': 'ok', 'controller': 'ibexecutionrestfuls'} )


def ibexecutionrestfuls_insert_one(data):
    # query if contract already exists
    try:
        current_app.logger.info(f'check ibcontract: {data["contract"]["m_conId"]}')
    except:
        return jsonify( {'status': 'error', 'error': 'missing conid', 'controller': 'ibexecutionrestfuls'} )

    data["contract"]['m_conId'] = int(data["contract"]["m_conId"]) # could be a string from xml report
    ibcontract = Ibcontract.query.get(data["contract"]["m_conId"])

    if ibcontract != None:
        current_app.logger.info(f'controller: ibexecutionrestfuls, existing contract: {data["contract"]["m_conId"]}')
    else:
        current_app.logger.info(f'controller: ibexecutionrestfuls, missing contract: {data["contract"]["m_conId"]} will be added now')
        if data["contract"]["m_strike"] > 0:
            strike = float(data["contract"]["m_strike"])
        else:
            strike = None


        if data["contract"]["m_secType"] == "STK":
            ibcontract = Ibcontract(
                assetCategory = data["contract"]["m_secType"],
                symbol = data["contract"]["m_symbol"],
                # description -> in daily statement
                conid = data["contract"]["m_conId"],
                # isin -> in daily statement
                # listingExchange -> data quality issue for stock
                multiplier = 1,
                currency = data["contract"]["m_currency"]
            )
        else: # derivative
            ibcontract = Ibcontract(
                assetCategory = data["contract"]["m_secType"],
                symbol = data["contract"]["m_localSymbol"],
                # description -> in daily statement
                conid = data["contract"]["m_conId"],
                # isin -> in daily statement
                listingExchange = data["contract"]["m_exchange"],
                # underlyingConid -> in daily statement
                underlyingSymbol = data["contract"]["m_symbol"],
                # underlyingSecurityID
                # underlyingListingExchange
                multiplier =  float(data["contract"]["m_multiplier"]),
                strike = strike,
                expiry = datetime.datetime.strptime(data["contract"]["m_expiry"], '%Y%m%d'),
                putCall = data["contract"]["m_right"],
                currency = data["contract"]["m_currency"]
            )

        db.session.add(ibcontract)
        db.session.commit()
        current_app.logger.info(f'controller: ibexecutionrestfuls, new contract: {data["contract"]["m_conId"]}')


    # insert executions
    current_app.logger.info(f'check existing execution: {data["execution"]["m_execId"]}')
    ibexecutionrestful = Ibexecutionrestful.query.get(data["execution"]["m_execId"])
    if ibexecutionrestful != None:
        current_app.logger.info(f'ibexecutionrestful {data["execution"]["m_execId"]} already existing in db')
    else:

        shares = abs(data["execution"]["m_shares"])
        cumQty = abs(data["execution"]["m_cumQty"])
        if data["execution"]["m_side"][0].upper() == 'B':
            pass
        elif data["execution"]["m_side"][0].upper() == 'S':
            shares = -shares
            cumQty = -cumQty

        if data["contract"]["m_secType"] == "STK":
            symbol = data["contract"]["m_symbol"]
            multiplier = 1
            listingExchange = None
            underlyingSymbol = None
            strike = None
            expiry = None
            putCall = None
        else:
            symbol = data["contract"]["m_localSymbol"]
            try:
                multiplier =  float(data["contract"]["m_multiplier"])
            except:
                multiplier =  data["contract"]["m_multiplier"]
            listingExchange = data["contract"]["m_exchange"]
            underlyingSymbol = data["contract"]["m_symbol"]
            strike =  float(data["contract"]["m_strike"])
            expiry = datetime.datetime.strptime(data["contract"]["m_expiry"], '%Y%m%d')
            putCall = data["contract"]["m_right"]


        ibexecutionrestful = Ibexecutionrestful(
            execution_m_acctNumber = data["execution"]["m_acctNumber"],
            execution_m_orderId = int(data["execution"]["m_orderId"]),
            execution_m_time = datetime.datetime.strptime( data["execution"]["m_time"], '%Y%m%d  %H:%M:%S'),
            execution_m_permId = int(data["execution"]["m_permId"]),
            execution_m_price = float(data["execution"]["m_price"]),
            execution_avgPrice = float(data["execution"]["m_avgPrice"]),
            execution_m_cumQty = cumQty,
            execution_m_side = data["execution"]["m_side"],
            execution_m_clientId = int(data["execution"]["m_clientId"]),
            execution_m_shares = shares,
            execution_m_execId = data["execution"]["m_execId"],
            execution_m_exchange = data["execution"]["m_exchange"],

            contract_m_tradingClass = data["contract"]["m_tradingClass"],
            contract_m_symbol = data["contract"]["m_symbol"],
            contract_m_conId = data["contract"]["m_conId"],
            contract_m_secType = data["contract"]["m_secType"],
            contract_m_right = data["contract"]["m_right"],
            contract_m_multiplier = multiplier,
            contract_m_expiry = expiry, # parasing date
            contract_m_localSymbol = data["contract"]["m_localSymbol"],
            contract_m_exchange = data["contract"]["m_exchange"],
            contract_m_strike = strike,

            ibasset = ibcontract, # object

        )

        db.session.add(ibexecutionrestful)
        db.session.commit()
        current_app.logger.info(f'new ibexecutionrestful: {data["execution"]["m_execId"]}')

    return jsonify( {
            'status': 'ok',
            'controller': 'ibexecutionrestfuls',
            'inputData': data,
            'ibcontract': {'conid': ibcontract.conid},
            'order': {'orderId': ibexecutionrestful.execution_m_orderId},
            'execution': {'execId': ibexecutionrestful.execution_m_execId}
    } )


@bp.route('/ibexecutionrestfuls', methods=['POST'])
def create_one():
    try:
        data = request.get_json()
    except:
        return jsonify( {'status': 'error', 'error': 'missing data', 'controller': 'ibexecutionrestfuls'} )

    if data == None:
        return jsonify( {'status': 'error', 'error': 'missing data', 'controller': 'ibexecutionrestfuls'} )

    return ibexecutionrestfuls_insert_one(data)


def ibexecutionrestfuls_insert_many(data):
    for ibexecutionrestful in data:
        ibexecutionrestfuls_insert_one(ibexecutionrestful)

    return jsonify( {'status': 'ok', 'message': 'bulk', 'executions': len(data), 'controller': 'ibexecutionrestfuls'} )


@bp.route('/ibexecutionrestfuls/bulk', methods=['POST'])
def create_many():
    try:
        data = request.get_json()
    except:
        return jsonify( {'status': 'error', 'error': 'missing data', 'controller': 'ibexecutionrestfuls'} )

    if data == None:
        return jsonify( {'status': 'error', 'error': 'missing data', 'controller': 'ibexecutionrestfuls'} )

    return ibexecutionrestfuls_insert_many(data)


@bp.route('/ibexecutionrestfuls/time/<date_str>', methods=['GET'])
def list_limit_date(date_str):
    ibexecutionrestfuls = Ibexecutionrestful.query.filter(Ibexecutionrestful.execution_m_time >= date_str).all()

    execs = {} # dict with execId as key
    for ibexecutionrestful in ibexecutionrestfuls:
        one_exec = {}

        one_exec['execution_m_execId'] = ibexecutionrestful.execution_m_execId
        one_exec['execution_m_orderId'] = ibexecutionrestful.execution_m_orderId
        one_exec['execution_m_time'] = ibexecutionrestful.execution_m_time
        one_exec['execution_m_acctNumber'] = ibexecutionrestful.execution_m_acctNumber
        one_exec['execution_m_exchange'] = ibexecutionrestful.execution_m_exchange
        one_exec['execution_m_side'] = ibexecutionrestful.execution_m_side
        one_exec['execution_m_shares'] = ibexecutionrestful.execution_m_shares  # execQty
        one_exec['execution_m_cumQty'] = ibexecutionrestful.execution_m_cumQty
        one_exec['execution_m_price'] = float(ibexecutionrestful.execution_m_price)
        one_exec['execution_avgPrice'] = float(ibexecutionrestful.execution_avgPrice)
        one_exec['execution_m_permId'] = ibexecutionrestful.execution_m_permId

        one_exec['contract_m_symbol'] = ibexecutionrestful.contract_m_symbol
        one_exec['contract_m_conId'] = ibexecutionrestful.contract_m_conId
        one_exec['contract_m_secType'] = ibexecutionrestful.contract_m_secType
        one_exec['contract_m_multiplier'] = int(ibexecutionrestful.contract_m_multiplier)
        one_exec['contract_m_localSymbol'] = ibexecutionrestful.contract_m_localSymbol


        one_exec['ibasset'] = {
            'conid': ibexecutionrestful.ibasset.conid,
            'symbol': ibexecutionrestful.ibasset.symbol,
            'multiplier': int(ibexecutionrestful.ibasset.multiplier)
        }
        bloom = ibexecutionrestful.ibasset.bloom
        if bloom != None:
            one_exec['ibsymbology'] = {
                'ticker': ibexecutionrestful.ibasset.bloom.ticker,
                'bbgIdentifier': ibexecutionrestful.ibasset.bloom.bbgIdentifier,
                'bbgUnderylingId': ibexecutionrestful.ibasset.bloom.bbgUnderylingId,
                'internalUnderlying': ibexecutionrestful.ibasset.bloom.internalUnderlying
            }
        execs[ibexecutionrestful.execution_m_execId] = one_exec

    return jsonify( {
        'status': 'ok',
        'time': date_str,
        'count': len(ibexecutionrestfuls),
        'executions': execs,
        'controller': 'ibexecutionrestfuls'
    } )

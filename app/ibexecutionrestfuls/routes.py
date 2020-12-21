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
        #current_app.logger.info(f'controller: ibexecutionrestfuls, existing contract: {data["contract"]["m_conId"]}')
        pass
    else:
        current_app.logger.info(f'controller: ibexecutionrestfuls, missing contract: {data["contract"]["m_conId"]} will be added now')

        if "m_secType" in data["contract"] and data["contract"]["m_secType"] is not None and data["contract"]["m_secType"] != '':
            assetCategory = data["contract"]["m_secType"]
        else:
            assetCategory = None


        if ( "m_secType" in data["contract"] and data["contract"]["m_secType"] is not None and data["contract"]["m_secType"] == "STK" and "m_symbol" in data["contract"] ):
            symbol = data["contract"]["m_symbol"]
            underlyingSymbol = data["contract"]["m_symbol"]
        elif 'm_localSymbol' in data["contract"] and data["contract"]["m_localSymbol"] != '':
            symbol = data["contract"] and data["contract"]["m_localSymbol"]

            if 'm_symbol' in data["contract"]:
                underlyingSymbol = data["contract"]["m_symbol"]
        else:
            symbol = None


        if "m_description" in data["contract"] and data["contract"]["m_description"] is not None and data["contract"]["m_description"] != "":
            description = data["contract"]["m_description"]
        else:
            description = None


        conid = int(data["contract"]["m_conId"])


        if "m_isin" in data["contract"] and data["contract"]["m_isin"] is not None and data["contract"]["m_isin"] != "":
            isin = data["contract"]["m_isin"]
        else:
            isin = None


        if "m_listingExchange" in data["contract"] and data["contract"]["m_listingExchange"] is not None and ( "m_secType" in data["contract"] and data["contract"]["m_secType"] is not None and data["contract"]["m_secType"] != "STK" ) :
            listingExchange = data["contract"]["m_listingExchange"]
        else:
            listingExchange = None


        if "m_underlyingConid" in data["contract"] and data["contract"]["m_underlyingConid"] is not None and data["contract"]["m_underlyingConid"] != "":
            underlyingConid = data["contract"]["m_underlyingConid"]
        else:
            underlyingConid = None


        if "m_underlyingSecurityID" in data["contract"] and data["contract"]["m_underlyingSecurityID"] is not None and data["contract"]["m_underlyingSecurityID"] != "":
            underlyingSecurityID = data["contract"]["m_underlyingSecurityID"]
        else:
            underlyingSecurityID = None


        if "m_underlyingListingExchange" in data["contract"] and data["contract"]["m_underlyingListingExchange"] is not None and data["contract"]["m_underlyingListingExchange"] != "":
            underlyingListingExchange = data["contract"]["m_underlyingListingExchange"]
        else:
            underlyingListingExchange = None


        if "m_multiplier" in data["contract"] and data["contract"]["m_multiplier"] is not None and data["contract"]["m_multiplier"] != '' :
            try:
                multiplier =  float( data["contract"]["m_multiplier"] )
            except:
                multiplier = 1
        else:
            multiplier = 1


        if "m_strike" in data["contract"] and data["contract"]["m_strike"] is not None and data["contract"]["m_strike"] != '' and data["contract"]["m_strike"] > 0:
            try:
                strike = float( data["contract"]["m_strike"] )
            except:
                strike = None
        else:
            strike = None


        if "m_expiry" in data["contract"] and data["contract"]["m_expiry"] is not None and data["contract"]["m_expiry"] != '':
            try:
                if len( data["contract"]["m_expiry"] ) == 8:
                    expiry = datetime.datetime.strptime( data["contract"]["m_expiry"], '%Y%m%d' )
                elif len( data["contract"]["m_expiry"] ) == 10:
                    expiry = datetime.datetime.strptime( data["contract"]["m_expiry"], '%Y-%m-%d' )
                else:
                    expiry = None
            except:
                expiry = None
        else:
            expiry = None


        if "m_right" in data["contract"] and data["contract"]["m_right"] is not None and data["contract"]["m_right"] != '' and (data["contract"]["m_right"][:1].upper() == 'C' or data["contract"]["m_right"][:1].upper() == 'P'):
            putCall = data["contract"]["m_right"]
        elif "m_putCall" in data["contract"] and data["contract"]["m_putCall"] is not None and data["contract"]["m_putCall"] != '' and (data["contract"]["m_putCall"][:1].upper() == 'C' or data["contract"]["m_putCall"][:1].upper() == 'P'):
            putCall = data["contract"]["m_putCall"]
        else:
            putCall = None


        if "m_maturity" in data["contract"] and data["contract"]["m_maturity"] is not None and data["contract"]["m_maturity"] != '':
            try:
                if len(data["contract"]["m_maturity"]) == 8:
                    maturity = datetime.datetime.strptime(data["contract"]["m_maturity"], '%Y%m%d')
                elif len(data["contract"]["m_maturity"]) == 10:
                    maturity = datetime.datetime.strptime(data["contract"]["m_maturity"], '%Y-%m-%d')
                else:
                    maturity = None
            except:
                maturity = None
        else:
            maturity = None


        if "m_issueDate" in data["contract"] and data["contract"]["m_issueDate"] is not None and data["contract"]["m_issueDate"] != '':
            try:
                if len(data["contract"]["m_issueDate"]) == 8:
                    issueDate = datetime.datetime.strptime(data["contract"]["m_issueDate"], '%Y%m%d')
                elif len(data["contract"]["m_maturity"]) == 10:
                    issueDate = datetime.datetime.strptime(data["contract"]["m_issueDate"], '%Y-%m-%d')
                else:
                    issueDate = None
            except:
                issueDate = None
        else:
            issueDate = None


        if "m_underlyingCategory" in data["contract"] and data["contract"]["m_underlyingCategory"] is not None and data["contract"]["m_underlyingCategory"] != '':
            underlyingCategory = data["contract"]["m_underlyingCategory"]
        else:
            underlyingCategory = None


        if "m_subCategory" in data["contract"] and data["contract"]["m_subCategory"] is not None and data["contract"]["m_subCategory"] != '':
            subCategory = data["contract"]["m_subCategory"]
        else:
            subCategory = None


        if "m_currency" in data["contract"] and data["contract"]["m_currency"] is not None and data["contract"]["m_currency"] != '':
            currency = data["contract"]["m_currency"]
        else:
            currency = None

        ibcontract = Ibcontract(
            assetCategory  = assetCategory,
            symbol = symbol,
            description = description,
            conid = conid,
            isin = isin,
            listingExchange = listingExchange,
            underlyingConid = underlyingConid,
            underlyingSymbol = underlyingSymbol,
            underlyingSecurityID = underlyingSecurityID,
            underlyingListingExchange = underlyingListingExchange,
            multiplier = multiplier,
            strike = strike,
            expiry = expiry,
            putCall = putCall,
            maturity = maturity,
            issueDate = issueDate,
            underlyingCategory = underlyingCategory,
            subCategory = subCategory,
            currency = currency
        )

        try:
            db.session.add(ibcontract)
            db.session.commit()
            current_app.logger.info(f'controller: ibexecutionrestfuls, new contract: {data["contract"]["m_conId"]}')
        except:
            current_app.logger.error(f'ISSUE controller: ibexecutionrestfuls, unable to add contract: {data["contract"]["m_conId"]}')


    if int(data["execution"]["m_execId"].split(".")[-1]) != 1:
        former_execs = []
        for i in range(int(data["execution"]["m_execId"].split(".")[-1])-1, 0, -1 ):
           execId_suffix = '00' + str(i) 
           execId_suffix = execId_suffix[-2:]
           former_execs.append( Ibexecutionrestful.query.get( f'{".".join(data["execution"]["m_execId"].split(".")[:-1])}.{execId_suffix}' ) )
        
        for execId in former_execs:
            if execId != None:
                return jsonify( {
                        'status': 'ok', 
                        'message': 'duplicate, rebooking',
                        'controller': 'ibexecutionrestfuls',
                        'inputData': data,
                        'ibcontract': {'conid': ibcontract.conid},
                        'order': {'orderId': execId.execution_m_orderId},
                        'execution': {'execId': execId.execution_m_execId}
                } )



    # insert executions
    current_app.logger.info(f'check existing execution: {data["execution"]["m_execId"]}')
    ibexecutionrestful = Ibexecutionrestful.query.get(data["execution"]["m_execId"])
    if ibexecutionrestful != None:
        current_app.logger.info(f'ibexecutionrestful {data["execution"]["m_execId"]} already existing in db')
    else:

        # clean variables because it went not necessarily through new ibcontract

        try:
            multiplier =  float(data["contract"]["m_multiplier"])
        except:
            multiplier =  1

        try:
            strike =  float(data["contract"]["m_strike"])
        except:
            strike = None

        try:
            expiry = datetime.datetime.strptime(data["contract"]["m_expiry"], '%Y%m%d')
        except:
            expiry = None



        shares = abs(data["execution"]["m_shares"])
        cumQty = abs(data["execution"]["m_cumQty"])
        if data["execution"]["m_side"][0].upper() == 'B':
            pass
        elif data["execution"]["m_side"][0].upper() == 'S':
            shares = -shares
            cumQty = -cumQty


        ibexecutionrestful = Ibexecutionrestful(
            execution_m_acctNumber = data["execution"]["m_acctNumber"],
            execution_m_orderId = int(data["execution"]["m_orderId"]),
            execution_m_time = datetime.datetime.strptime( data["execution"]["m_time"], '%Y%m%d  %H:%M:%S'),
            execution_m_permId = int(data["execution"]["m_permId"]),
            execution_m_price = float(data["execution"]["m_price"]),
            execution_m_avgPrice = float(data["execution"]["m_avgPrice"]),
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

        try:
            db.session.add(ibexecutionrestful)
            db.session.commit()
            current_app.logger.info(f'new ibexecutionrestful: {data["execution"]["m_execId"]}')
        except:
            current_app.logger.error(f'ISSUE controller: ibexecutionrestfuls, unable to add exec: {data["execution"]["m_execId"]}')

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

    ibexecutionrestfuls = [exec[0] for exec in Ibexecutionrestful.query.with_entities(Ibexecutionrestful.execution_m_execId).all() ]

    newExec_count = 0
    for ibexecutionrestful in data:
        if ibexecutionrestful["execution"]["m_execId"] not in ibexecutionrestfuls:
            ibexecutionrestfuls_insert_one(ibexecutionrestful)
            newExec_count += 1


    return jsonify( {'status': 'ok', 'message': 'bulk', 'executions': len(data), 'controller': 'ibexecutionrestfuls', 'newExecs_count': newExec_count} )


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
        one_exec['execution_m_avgPrice'] = float(ibexecutionrestful.execution_m_avgPrice) if ibexecutionrestful.execution_m_avgPrice != None else None
        one_exec['execution_m_permId'] = ibexecutionrestful.execution_m_permId

        one_exec['contract_m_symbol'] = ibexecutionrestful.contract_m_symbol
        one_exec['contract_m_conId'] = ibexecutionrestful.contract_m_conId
        one_exec['contract_m_secType'] = ibexecutionrestful.contract_m_secType
        one_exec['contract_m_multiplier'] = 1 if ibexecutionrestful.contract_m_multiplier == None else int(ibexecutionrestful.contract_m_multiplier)
        one_exec['contract_m_localSymbol'] = ibexecutionrestful.contract_m_localSymbol


        one_exec['ibasset'] = {
            'conid': ibexecutionrestful.ibasset.conid,
            'symbol': ibexecutionrestful.ibasset.symbol,
            'multiplier': 1 if ibexecutionrestful.ibasset.multiplier == None else int(ibexecutionrestful.ibasset.multiplier)
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

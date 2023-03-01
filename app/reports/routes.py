from datetime import datetime, timedelta
from flask import render_template, flash, json, redirect, url_for, request, Response, send_file, g, jsonify, current_app
from werkzeug.utils import secure_filename
import os
import time
import csv
import pandas as pd
import numpy as np
import requests
import xml.etree.ElementTree as ET
import xmltodict
from app import db, alchemydumps
from app.models import Ibexecutionrestful
from app.reports import bp
from app.ibcontracts import routes as routes_ibcontract
from app.ibexecutionrestfuls import routes as routes_ibexecutionrestfuls
import tarfile
from flask_alchemydumps.database import AlchemyDumpsDatabase
from flask_alchemydumps.backup import Backup

SCRIPT_ROOT = os.path.dirname(os.path.abspath(__file__))
IB_FILE_LAST = 'MULTI_last.csv'
IB_FQ_LAST = 'ws_discovery.xml'
ALLOWED_EXTENSIONS = set(['csv'])

# utilities
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def get_file(filename):
    try:
        src = os.path.join(SCRIPT_ROOT, filename)
        return open(src).read()
    except IOError as exc:
        return str(exc)


def patch_missing_date(input_date):
    if pd.isnull(input_date):
        return None
    else:
        return input_date.strftime('%Y-%m-%d')


def patch_missing_datetime(input_datetime):
    if pd.isnull(input_datetime):
        return None
    else:
        return input_datetime.isoformat()


def ibkr_patch_option_expiry_dt(expiry_dt):
    if len(expiry_dt) == 8:
        return f'{expiry_dt[:4]}-{expiry_dt[4:6]}-{expiry_dt[-2:]}'
    else:
        return None


def df_for_parquet(df, dm):

    df_parquet = df.copy(deep=True)

    for f in dm:
        
        if f['parquetDatatype'] == 'VARCHAR':
            df_parquet[f['field']] = df_parquet[f['field']].astype('string')
        elif f['parquetDatatype'] == 'DOUBLE':
            df_parquet[f['field']] = df_parquet[f['field']].astype('float64')
        elif f['parquetDatatype'] == 'BIGINT':
            df_parquet[f['field']] = df_parquet[f['field']].astype('Int64')
        elif f['parquetDatatype'] == 'INTEGER':
            df_parquet[f['field']] = df_parquet[f['field']].astype('Int32')
        elif f['parquetDatatype'] == 'BOOLEAN':
            df_parquet[f['field']] = df_parquet[f['field']].astype('bool')
        elif f['parquetDatatype'] == 'DATE':
            df_parquet[f['field']] = pd.to_datetime(
                df_parquet[f['field']], format='%Y-%m-%d').dt.date
        elif f['parquetDatatype'] == 'TIMESTAMP':
            df_parquet[f['field']] = pd.to_datetime(
                df_parquet[f['field']], format='%Y-%m-%d %H:%M:%S')
        else:
            print(f"unknow type: {f['parquetDatatype']}")

    return df_parquet[[f['field'] for f in dm]]  # subset



# routes
@bp.route('/reports', methods=['GET'])
def reports_list():
    return jsonify( {'status': 'ok', 'controller': 'reports'} )


@bp.route('/reports/ib/eod/v2/reportDate', methods=['GET'])
def ib_upload_eod_report_date_v2():
    try:
        with open(os.path.join(SCRIPT_ROOT + '/data/', IB_FQ_LAST), 'r') as fd:
            doc = xmltodict.parse( fd.read() )
    except:
        return jsonify({'status': 'error', 'error': f'{IB_FQ_LAST} not available', 'controller': 'reports'})

    list_FlexStatements = ib_fq_dailyStatement_FlexStatements(doc)
    for FlexStatement in list_FlexStatements:
        reportDate_str = FlexStatement['EquitySummaryInBase']['EquitySummaryByReportDateInBase']['@reportDate']
        return jsonify( {'status': 'ok', 'reportDate': f'{reportDate_str[:4]}-{reportDate_str[4:6]}-{reportDate_str[6:]}', 'controller': 'reports'} )


# autodnl with one single token
@bp.route('/reports/ib/eod/v2', methods=['POST'])
def ib_upload_eod_report_v2():
    try:
        data = request.get_json()
    except:
        return jsonify( {'status': 'error', 'error': 'missing token', 'controller': 'reports'} )

    QUERY_ID = data['QUERY_ID']
    TOKEN = data['TOKEN']
    VERSION = '3'

    res = requests.get(f'https://gdcdyn.interactivebrokers.com/Universal/servlet/FlexStatementService.SendRequest?t={TOKEN}&q={QUERY_ID}&v={VERSION}')

    doc = xmltodict.parse(res.content)

    if doc['FlexStatementResponse']['Status'] == 'Success':
        REFERENCE_CODE = doc['FlexStatementResponse']['ReferenceCode']

        res = requests.get(f'https://gdcdyn.interactivebrokers.com/Universal/servlet/FlexStatementService.GetStatement?q={REFERENCE_CODE}&t={TOKEN}&v={VERSION}')
        doc = xmltodict.parse(res.content)

        dnl_report_try = 0
        dnl_report_retry = 6
        import time
        # https://www.interactivebrokers.com/en/software/am/am/reports/version_3_error_codes.htm
        while 'FlexStatementResponse' in doc and doc['FlexStatementResponse']['Status'] == 'Warn' and dnl_report_try < dnl_report_retry:
                if doc['FlexStatementResponse']['ErrorCode'] == '1019':
                    current_app.logger.info(f'flex query report not yet ready... please wait {dnl_report_try+1}')
                    time.sleep(10)
                    res = requests.get(f'https://gdcdyn.interactivebrokers.com/Universal/servlet/FlexStatementService.GetStatement?q={REFERENCE_CODE}&t={TOKEN}&v={VERSION}')
                    doc = xmltodict.parse(res.content)
                    dnl_report_try += 1

                    if dnl_report_try == dnl_report_retry:
                        return jsonify( {'status': 'error', 'error': 'report not ready', 'controller': 'reports'} )


        with open(os.path.join(SCRIPT_ROOT + '/data/', IB_FQ_LAST), 'w') as file:
            file.write(res.text)


        if 'FlexQueryResponse' in doc:

            reportDate_str = ib_fq_dailyStatement_reportDate(doc).replace('-', '')

            # update Ibcontract
            df_openPositions = ib_fq_dailyStatement_OpenPositions(doc)
            list_ibcontractsToCheck = []
            fields_Ibcontract = ['assetCategory', 'symbol', 'description', 'conid', 'isin', 'listingExchange', 'underlyingConid', 'underlyingSymbol', 'underlyingSecurityID', 'underlyingListingExchange', 'multiplier', 'strike', 'expiry', 'putCall', 'maturity', 'issueDate', 'underlyingCategory', 'subCategory', 'currency']
            for position in df_openPositions.to_dict(orient='records'):
                # transforme into an ibcontract
                api_Ibcontract = {}
                for key_Ibcontract in fields_Ibcontract:
                    if key_Ibcontract in position:
                        api_Ibcontract[key_Ibcontract] = position[key_Ibcontract]
                list_ibcontractsToCheck.append(api_Ibcontract)

                #routes_ibcontract.ibcontract_create_one(api_Ibcontract)
            current_app.logger.info(f'ib_upload_eod_report_v2:: check ibcontracts OpenPositions')
            current_app.logger.info( f'ib_upload_eod_report_v2:: new assets from OpenPositions: {json.loads(routes_ibcontract.ibcontracts_insert_many(list_ibcontractsToCheck).data)["newAssets_count"]}' )


            # create ibcontract from monthly perf
            df_MTDYTDPerformanceSummary = ib_fq_dailyStatement_MTDYTDPerformanceSummary(doc)
            dict_positions = {}
            mtd_pnl = []
            for position in df_MTDYTDPerformanceSummary.to_dict(orient='records'):
                if position['conid'] not in dict_positions:
                    dict_positions[ position['conid'] ] = position
                    dict_positions[ position['conid'] ]['accountId'] = 'MULTI'
                else:
                    dict_positions[position['conid']]['mtmMTD'] = position['mtmMTD']

            for position in dict_positions:
                mtd_pnl.append(dict_positions[position])
            df_MTDYTDPerformanceSummary = pd.DataFrame(mtd_pnl)

            current_app.logger.info(f'ib_upload_eod_report_v2:: check ibcontracts MTDYTDPerformanceSummary')
            current_app.logger.info( f'ib_upload_eod_report_v2:: new asset from MTDYTDPerformanceSummary: {json.loads(routes_ibcontract.ibcontracts_insert_many(mtd_pnl).data)["newAssets_count"]}' )



            # clean former execs
            current_app.logger.info(f'ib_upload_eod_report_v2:: clean former executions pool before {reportDate_str[:4]}-{reportDate_str[4:6]}-{reportDate_str[6:]}')
            db.session.query(Ibexecutionrestful).filter(Ibexecutionrestful.execution_m_time <= f'{reportDate_str[:4]}-{reportDate_str[4:6]}-{reportDate_str[6:]}' ).delete()
            db.session.commit()


            return jsonify( {'status': 'ok', 'message': 'ib:: successfully retrive flex query start of the day', 'reportDate': f'{reportDate_str[:4]}-{reportDate_str[4:6]}-{reportDate_str[6:]}', 'controller': 'reports'} )
        else:
            return jsonify( {'status': 'error', 'error': 'flex query report is invalid', 'controller': 'reports'} )

    else:
        print(doc)
        return jsonify( {'status': 'error', 'error': 'unable to retrieve flex query', 'controller': 'reports'} )


# autodnl with multiple tokens

dm_openPosition = [
    {'field': 'provider', 'parquetDatatype': 'VARCHAR'}, 
    {'field': 'provider_account', 'parquetDatatype': 'VARCHAR'}, 
    {'field': 'strategy', 'parquetDatatype': 'VARCHAR'}, 
    {'field': 'position_current', 'parquetDatatype': 'BIGINT'}, 
    {'field': 'pnl_d_local', 'parquetDatatype': 'DOUBLE'}, 
    {'field': 'pnl_y_local', 'parquetDatatype': 'DOUBLE'}, 
    {'field': 'pnl_y_eod_local', 'parquetDatatype': 'DOUBLE'}, 
    {'field': 'position_eod', 'parquetDatatype': 'BIGINT'}, 
    {'field': 'position_d_chg', 'parquetDatatype': 'BIGINT'}, 
    {'field': 'price_eod', 'parquetDatatype': 'DOUBLE'}, 
    {'field': 'ntcf_d_local', 'parquetDatatype': 'DOUBLE'}, 
    {'field': 'Symbole', 'parquetDatatype': 'VARCHAR'}, 
    {'field': 'conid', 'parquetDatatype': 'BIGINT'}, 
    {'field': 'costBasisMoney_d_long', 'parquetDatatype': 'DOUBLE'}, 
    {'field': 'costBasisMoney_d_short', 'parquetDatatype': 'DOUBLE'}, 
    {'field': 'costBasisPrice_eod', 'parquetDatatype': 'DOUBLE'}, 
    {'field': 'fifoPnlUnrealized_eod', 'parquetDatatype': 'DOUBLE'}, 
    {'field': 'costBasisPrice_d', 'parquetDatatype': 'DOUBLE'}, 
    {'field': 'reportDate', 'parquetDatatype': 'DATE'}, 

]

dm_pnlMTD = [
    {'field': 'accountId', 'parquetDatatype': 'VARCHAR'}, 
    {'field': 'assetCategory', 'parquetDatatype': 'VARCHAR'}, 
    {'field': 'symbol', 'parquetDatatype': 'VARCHAR'}, 
    {'field': 'description', 'parquetDatatype': 'VARCHAR'}, 
    {'field': 'conid', 'parquetDatatype': 'BIGINT'}, 
    {'field': 'securityID', 'parquetDatatype': 'VARCHAR'}, 
    {'field': 'isin', 'parquetDatatype': 'VARCHAR'}, 
    {'field': 'listingExchange', 'parquetDatatype': 'VARCHAR'}, 
    {'field': 'underlyingConid', 'parquetDatatype': 'BIGINT'}, 
    {'field': 'underlyingSymbol', 'parquetDatatype': 'VARCHAR'}, 
    {'field': 'underlyingSecurityID', 'parquetDatatype': 'VARCHAR'}, 
    {'field': 'underlyingListingExchange', 'parquetDatatype': 'VARCHAR'}, 
    {'field': 'multiplier', 'parquetDatatype': 'DOUBLE'}, 
    {'field': 'strike', 'parquetDatatype': 'DOUBLE'}, 
    {'field': 'expiry', 'parquetDatatype': 'DATE'}, 
    {'field': 'putCall', 'parquetDatatype': 'VARCHAR'}, 
    {'field': 'mtmMTD', 'parquetDatatype': 'DOUBLE'}, 
    {'field': 'reportDate', 'parquetDatatype': 'DATE'}, 

]


@bp.route('/reports/ib/eod/tokens', methods=['POST'])
def ib_upload_eod_report_tokens():
    try:
        data = request.get_json()
    except:
        return jsonify( {'status': 'error', 'error': 'missing tokens', 'controller': 'reports'} )

    xml = None

    # --- retrieve raw report from IBKR through FlexQuery service, merge everything into one single big report ---
    for fq in data["flexQueries"]:
        current_app.logger.info(f'processing {fq["queryId"]}')

        QUERY_ID = fq["queryId"]
        TOKEN = fq["token"]
        VERSION = '3'

        res = requests.get(f'https://gdcdyn.interactivebrokers.com/Universal/servlet/FlexStatementService.SendRequest?t={TOKEN}&q={QUERY_ID}&v={VERSION}')

        doc = xmltodict.parse(res.content)

        if doc['FlexStatementResponse']['Status'] == 'Success':
            REFERENCE_CODE = doc['FlexStatementResponse']['ReferenceCode']

            res = requests.get(f'https://gdcdyn.interactivebrokers.com/Universal/servlet/FlexStatementService.GetStatement?q={REFERENCE_CODE}&t={TOKEN}&v={VERSION}')

            root =  ET.fromstring( res.content )

            dnl_report_try = 0
            dnl_report_retry = 6
            while root.tag == 'FlexStatementResponse' and root.find("Status").text == 'Warn' and dnl_report_try < dnl_report_retry:

                if root.find("ErrorCode").text == '1019':
                    current_app.logger.info(f'flex query {QUERY_ID} report not yet ready... please wait {dnl_report_try+1}')
                    time.sleep(10)
                    res = requests.get(f'https://gdcdyn.interactivebrokers.com/Universal/servlet/FlexStatementService.GetStatement?q={REFERENCE_CODE}&t={TOKEN}&v={VERSION}')
                    root =  ET.fromstring( res.content )
                    dnl_report_try += 1

                    if dnl_report_try == dnl_report_retry:
                        return jsonify( {'status': 'error', 'error': 'report not ready', 'controller': 'reports'} )
            
            with open(os.path.join(SCRIPT_ROOT + '/data/', f'{QUERY_ID}.xml'), 'wb') as file:
                file.write( ET.tostring(root) )
            
            if xml is None:
                xml = root
            else:
                FlexStatements = xml.find("FlexStatements") # only one

                for FlexStatement in root.iter('FlexStatement'):
                    FlexStatements.append(FlexStatement)
            
            with open(os.path.join(SCRIPT_ROOT + '/data/', IB_FQ_LAST), 'wb') as file:
                file.write( ET.tostring(xml) )
        
        else:
            print(doc)
            return jsonify( {'status': 'error', 'error': f'unable to retrieve flex query {QUERY_ID}', 'controller': 'reports'} )

            
    # --- post processing ---
    with open(os.path.join(SCRIPT_ROOT + '/data/', IB_FQ_LAST)) as fd:
        doc = xmltodict.parse(fd.read())


    if 'FlexQueryResponse' in doc:

        reportDate_str = ib_fq_dailyStatement_reportDate(doc).replace('-', '')

        # open positions
        df_openPositions = ib_fq_dailyStatement_OpenPositions(doc)

        df_openPositions_parquet = df_for_parquet(df_openPositions, dm_openPosition)
        df_openPositions_parquet.to_parquet(
            os.path.join(SCRIPT_ROOT + '/data/', 'ibkr_OpenPositions.parquet.gzip'), 
            engine='pyarrow', 
            compression='gzip',
            index=False, 
            flavor='spark'
        )
        current_app.logger.info(f'ib_upload_eod_report_v2:: save OpenPositions as parquet')

        # update Ibcontract
        list_ibcontractsToCheck = []
        fields_Ibcontract = ['assetCategory', 'symbol', 'description', 'conid', 'isin', 'listingExchange', 'underlyingConid', 'underlyingSymbol', 'underlyingSecurityID', 'underlyingListingExchange', 'multiplier', 'strike', 'expiry', 'putCall', 'maturity', 'issueDate', 'underlyingCategory', 'subCategory', 'currency']
        for position in df_openPositions.to_dict(orient='records'):
            # transforme into an ibcontract
            api_Ibcontract = {}
            for key_Ibcontract in fields_Ibcontract:
                if key_Ibcontract in position:
                    api_Ibcontract[key_Ibcontract] = position[key_Ibcontract]
            list_ibcontractsToCheck.append(api_Ibcontract)

        current_app.logger.info(f'ib_upload_eod_report_v2:: check ibcontracts OpenPositions')
        current_app.logger.info( f'ib_upload_eod_report_v2:: new assets from OpenPositions: {json.loads(routes_ibcontract.ibcontracts_insert_many(list_ibcontractsToCheck).data)["newAssets_count"]}' )


        # create ibcontract from monthly perf
        df_MTDYTDPerformanceSummary = ib_fq_dailyStatement_MTDYTDPerformanceSummary(doc)

        df_MTDYTDPerformanceSummary_parquet = df_for_parquet(df_MTDYTDPerformanceSummary[ df_MTDYTDPerformanceSummary['mtmMTD'] != 0 ], dm_pnlMTD)
        df_MTDYTDPerformanceSummary_parquet.to_parquet(
            os.path.join(SCRIPT_ROOT + '/data/', 'ibkr_MTDYTDPerformanceSummary.parquet.gzip'), 
            engine='pyarrow', 
            compression='gzip',
            index=False, 
            flavor='spark'
        )
        current_app.logger.info(f'ib_upload_eod_report_v2:: save MTDYTDPerformanceSummary as parquet')

        dict_positions = {}
        mtd_pnl = []
        for position in df_MTDYTDPerformanceSummary.to_dict(orient='records'):
            if position['conid'] not in dict_positions:
                dict_positions[ position['conid'] ] = position
                dict_positions[ position['conid'] ]['accountId'] = 'MULTI'
            else:
                dict_positions[position['conid']]['mtmMTD'] = position['mtmMTD']

        for position in dict_positions:
            mtd_pnl.append(dict_positions[position])
        df_MTDYTDPerformanceSummary = pd.DataFrame(mtd_pnl)

        current_app.logger.info(f'ib_upload_eod_report_v2:: check ibcontracts MTDYTDPerformanceSummary')
        current_app.logger.info( f'ib_upload_eod_report_v2:: new asset from MTDYTDPerformanceSummary: {json.loads(routes_ibcontract.ibcontracts_insert_many(mtd_pnl).data)["newAssets_count"]}' )



        # clean former execs
        current_app.logger.info(f'ib_upload_eod_report_v2:: clean former executions pool before {reportDate_str[:4]}-{reportDate_str[4:6]}-{reportDate_str[6:]}')
        db.session.query(Ibexecutionrestful).filter(Ibexecutionrestful.execution_m_time <= f'{reportDate_str[:4]}-{reportDate_str[4:6]}-{reportDate_str[6:]}' ).delete()
        db.session.commit()


        return jsonify( {'status': 'ok', 'message': 'ib:: successfully retrive flex query start of the day', 'reportDate': f'{reportDate_str[:4]}-{reportDate_str[4:6]}-{reportDate_str[6:]}', 'controller': 'reports'} )
    else:
        return jsonify( {'status': 'error', 'error': 'flex query report is invalid', 'controller': 'reports'} )



@bp.route('/reports/ib/eod/v2/xls', methods=['POST'])
def ib_report_eod_v2_xls():
    current_app.logger.info('in /reports/ib/eod/v2/xls')

    current_app.logger.info('before reading user post content (exec, limit account)')
    input_data = request.get_json()
    current_app.logger.info('after reading user post content')

    if input_data != None and 'execDetails' in input_data:
        current_app.logger.info('found execDetails')
        # push dans le pool in 1 call
        current_app.logger.info('before pushing execDetails in pool')
        routes_ibexecutionrestfuls.ibexecutionrestfuls_insert_many(input_data['execDetails'])
        current_app.logger.info('after pushing execDetails in pool, done with user post executions')

    with open(os.path.join(SCRIPT_ROOT + '/data/', IB_FQ_LAST), 'r') as fd:
        doc = xmltodict.parse(fd.read())
    current_app.logger.info('after opening full report fq')

    current_app.logger.info('before reading open positions full report')
    df_openPositions = ib_fq_dailyStatement_OpenPositions(doc)
    current_app.logger.info('after reading open positions full report')
    
    # limit for 1 account
    if 'account' in input_data:
        current_app.logger.info('found limit account in user data')
        df_openPositions = df_openPositions[ df_openPositions['provider_account'] == input_data['account'] ]
        current_app.logger.info(f'after limiting df_openPositions with account { input_data["account"]}')
    list_openPositions = df_openPositions.to_dict(orient='records')

    current_app.logger.info('after retrieving open position from fq report')


    # inject intraday executions
    current_app.logger.info('before call ib_upload_eod_report_date_v2()')
    date_obj = json.loads( ib_upload_eod_report_date_v2().get_data() )
    current_app.logger.info('after call ib_upload_eod_report_date_v2()')
    dt_report = datetime.strptime( date_obj['reportDate'], '%Y-%m-%d')
    dt_refExec = dt_report + timedelta(days=1)

    current_app.logger.info(f'ib_report_eod_v2_xls:: check refDate for executions {dt_refExec.strftime("%Y-%m-%d")}')


    current_executions = json.loads( routes_ibexecutionrestfuls.list_limit_date( dt_refExec.strftime("%Y-%m-%d") ).get_data() )
    current_app.logger.info(f'after retrieving today executions')

    for ibexecutionrestful_execution_m_execId in current_executions['executions']: #dict

        ibexecutionrestful = current_executions['executions'][ibexecutionrestful_execution_m_execId]

        if ibexecutionrestful['execution_m_acctNumber'][-1] == 'F':
            ibexecutionrestful['execution_m_acctNumber'] = ibexecutionrestful['execution_m_acctNumber'][:-1]

        found_in_daily_statement = False

        for openPosition in list_openPositions:

            if openPosition['conid'] == ibexecutionrestful['contract_m_conId'] and openPosition['provider_account'] == ibexecutionrestful['execution_m_acctNumber']: # same asset & same account
                openPosition['position_current'] += ibexecutionrestful['execution_m_shares']

                openPosition['position_d_chg'] = openPosition['position_current'] - openPosition['position_eod']

                openPosition['ntcf_d_local'] -= ibexecutionrestful['execution_m_shares'] * ibexecutionrestful['execution_m_price'] * ibexecutionrestful['contract_m_multiplier']

                openPosition['costBasisMoney_d_long'] += ibexecutionrestful['execution_m_shares'] * ibexecutionrestful['execution_m_price'] if ibexecutionrestful['execution_m_shares'] > 0 else 0

                openPosition['costBasisMoney_d_short'] += ibexecutionrestful['execution_m_shares'] * ibexecutionrestful['execution_m_price'] if ibexecutionrestful['execution_m_shares'] < 0 else 0

                if openPosition['position_d_chg'] == 0:
                    openPosition['costBasisPrice_d'] = 0
                elif openPosition['position_d_chg'] > 0:
                    openPosition['costBasisPrice_d'] = ( openPosition['costBasisMoney_d_long'] + openPosition['costBasisMoney_d_short'] ) / openPosition['position_d_chg']
                elif openPosition['position_d_chg'] < 0:
                    openPosition['costBasisPrice_d'] = ( openPosition['costBasisMoney_d_short'] + openPosition['costBasisMoney_d_long'] ) / openPosition['position_d_chg']


                found_in_daily_statement = True
                break

        if found_in_daily_statement == False:

            list_openPositions.append({
                'provider': 'IB', 
                'provider_account': ibexecutionrestful['execution_m_acctNumber'],
                'strategy': "MULTI",
                'position_current': ibexecutionrestful['execution_m_shares'],
                'pnl_d_local': 0,
                'pnl_y_local': 0,
                'pnl_y_eod_local': 0,
                'position_eod': 0,
                'price_eod': 0,
                'ntcf_d_local': -1 * ibexecutionrestful['execution_m_shares'] * ibexecutionrestful['execution_m_price'] * ibexecutionrestful['contract_m_multiplier'] ,
                'Symbole': f'{ibexecutionrestful["contract_m_symbol"]}/{ibexecutionrestful["contract_m_localSymbol"]}',
                'conid': ibexecutionrestful['contract_m_conId'],
                'costBasisMoney_d_long': ibexecutionrestful['execution_m_shares'] * ibexecutionrestful['execution_m_price'] if ibexecutionrestful['execution_m_shares'] > 0 else 0,
                'costBasisMoney_d_short': ibexecutionrestful['execution_m_shares'] * ibexecutionrestful['execution_m_price'] if ibexecutionrestful['execution_m_shares'] < 0 else 0,
                'costBasisPrice_eod': 0,
                'fifoPnlUnrealized_eod': 0,
                'costBasisPrice_d': ibexecutionrestful['execution_m_price'] / abs(ibexecutionrestful['execution_m_shares']) if ibexecutionrestful['execution_m_shares'] != 0 else 0
            })


    df_openPositions = pd.DataFrame(list_openPositions)

    current_app.logger.info(f'after merging open positions with intraday executions, positions: {len(df_openPositions)}')    


    # inject monthly pnl in base currency - ibcontracts are created with flex query results are retrieved
    df_MTDYTDPerformanceSummary = ib_fq_dailyStatement_MTDYTDPerformanceSummary(doc)
    current_app.logger.info(f'after reading mtd pnl report')

    if 'account' in input_data:
        current_app.logger.info('found limit account in user data')
        df_MTDYTDPerformanceSummary = df_MTDYTDPerformanceSummary[ df_MTDYTDPerformanceSummary['accountId'] == input_data['account'] ]
        current_app.logger.info(f'after limiting df_MTDYTDPerformanceSummary with account { input_data["account"]}')

    list_openPositions = df_openPositions.to_dict(orient='records')

    for monthly_pnl in df_MTDYTDPerformanceSummary[ df_MTDYTDPerformanceSummary["mtmMTD"] != 0 ].to_dict(orient='records'):
        found_in_daily_statement = False
        for openPosition in list_openPositions:
            # if monthly_pnl['conid'] == openPosition['conid']:
            if monthly_pnl['conid'] == openPosition['conid'] and monthly_pnl['accountId'] == openPosition['provider_account']:
                found_in_daily_statement = True
                openPosition['pnl_m_eod_base'] = monthly_pnl['mtmMTD']
                break

        if found_in_daily_statement == False:
            list_openPositions.append({
                'provider': 'IB',
                'provider_account': monthly_pnl['accountId'],
                'strategy': "MULTI",
                'position_current': 0,
                'pnl_d_local': 0,
                'pnl_y_local': 0,
                'pnl_y_eod_local': 0,
                'position_eod': 0,
                'price_eod': 0,
                'ntcf_d_local': 0,
                'pnl_m_eod_base': monthly_pnl['mtmMTD'],
                'Symbole': f'{monthly_pnl["symbol"]}',
                'conid': monthly_pnl['conid'],
            })

    df_openPositions = pd.DataFrame(list_openPositions)
    current_app.logger.info(f'after merging open positions with mtd pnl: {len(df_openPositions)}')  


    for h in ['ntcf_d_local', 'pnl_d_local', 'pnl_m_eod_base', 'pnl_y_eod_local', 'pnl_y_local', 'position_current', 'position_eod', 'price_eod', 'costBasisPrice_eod', 'costBasisPrice_d']: # pnl_m_eod_base
        df_openPositions[h] = df_openPositions[h].fillna(0)

    current_app.logger.info(f'before retrieving bridge from db')   
    df_bridge = pd.read_sql(sql="SELECT ticker, bbgIdentifier, bbgUnderylingId, internalUnderlying, ibcontract_conid FROM ibsymbology", con=db.engine)

    df_bridge.rename(columns={'ticker': 'bbg_ticker',
                      'bbgIdentifier': 'Identifier',
                      'bbgUnderylingId': 'bbg_underyling_id',
                      'internalUnderlying': 'Underlying',
                      'ibcontract_conid': 'conid', }
             , inplace=True)
    current_app.logger.info(f'after retrieving bridge from db')  

    df_openPositions = pd.merge(df_openPositions, df_bridge, on=['conid'], how='left')
    current_app.logger.info(f'after merging bridge with positions')  

    # patch np.nan to json null
    #df_openPositions = df_openPositions.where((pd.notnull(df_openPositions)), None) # doesn't seem to work anymore
    df_openPositions = df_openPositions.astype(object).where((pd.notnull(df_openPositions)), None)

    headers_subset = ['conid', 'Identifier', 'Symbole', 'bbg_ticker', 'bbg_underyling_id', 'Underlying', 'provider', 'provider_account', 'strategy', 'ntcf_d_local', 'pnl_d_local', 'pnl_m_eod_base', 'pnl_y_eod_local', 'pnl_y_local', 'position_current', 'position_eod', 'price_eod', 'costBasisPrice_eod', 'costBasisPrice_d' ]

    return jsonify( {'status': 'ok', 'controller': 'reports', 'positionsCount': len(df_openPositions), 'data': df_openPositions[headers_subset].to_dict(orient='records') } )



@bp.route('/reports/ib/eod/v2/xls/fast', methods=['POST'])
def ib_report_eod_v2_xls_fast():
    current_app.logger.info('in /reports/ib/eod/v2/xls/fast')

    current_app.logger.info('before reading user post content (exec, limit account)')
    input_data = request.get_json()
    #print(input_data)
    current_app.logger.info('after reading user post content')

    if input_data != None and 'execDetails' in input_data:
        current_app.logger.info('found execDetails')
        # push dans le pool in 1 call
        current_app.logger.info('before pushing execDetails in pool')
        routes_ibexecutionrestfuls.ibexecutionrestfuls_insert_many(input_data['execDetails'])
        current_app.logger.info('after pushing execDetails in pool, done with user post executions')

    with open(os.path.join(SCRIPT_ROOT + '/data/', IB_FQ_LAST), 'r') as fd:
        doc = xmltodict.parse(fd.read())
    current_app.logger.info('after opening full report fq')

    current_app.logger.info('before reading open positions full report')
    df_openPositions = ib_fq_dailyStatement_OpenPositions(doc)
    current_app.logger.info('after reading open positions full report')
    
    # limit for 1 account
    if 'account' in input_data:
        current_app.logger.info('found limit account in user data')
        df_openPositions = df_openPositions[ df_openPositions['provider_account'] == input_data['account'] ]
        current_app.logger.info(f'after limiting df_openPositions with account { input_data["account"]}')
    list_openPositions = df_openPositions.to_dict(orient='records')

    current_app.logger.info('after retrieving open position from fq report')


    # inject intraday executions
    current_app.logger.info('before call ib_upload_eod_report_date_v2()')
    date_obj = json.loads( ib_upload_eod_report_date_v2().get_data() )
    current_app.logger.info('after call ib_upload_eod_report_date_v2()')
    dt_report = datetime.strptime( date_obj['reportDate'], '%Y-%m-%d')
    dt_refExec = dt_report + timedelta(days=1)

    current_app.logger.info(f'ib_report_eod_v2_xls:: check refDate for executions {dt_refExec.strftime("%Y-%m-%d")}')


    current_executions = json.loads( routes_ibexecutionrestfuls.list_limit_date( dt_refExec.strftime("%Y-%m-%d") ).get_data() )
    #print( current_executions )
    current_app.logger.info(f'after retrieving today executions')

    for ibexecutionrestful_execution_m_execId in current_executions['executions']: #dict

        ibexecutionrestful = current_executions['executions'][ibexecutionrestful_execution_m_execId]

        if ibexecutionrestful['execution_m_acctNumber'][-1] == 'F':
            ibexecutionrestful['execution_m_acctNumber'] = ibexecutionrestful['execution_m_acctNumber'][:-1]

        #current_app.logger.info(f'found exec {ibexecutionrestful["execution_m_execId"]} for {ibexecutionrestful["contract_m_symbol"]} ({ibexecutionrestful["contract_m_multiplier"]}): {ibexecutionrestful["execution_m_shares"]} @ {ibexecutionrestful["execution_m_price"]}')

        found_in_daily_statement = False

        for openPosition in list_openPositions:

            # if openPosition['conid'] == ibexecutionrestful['contract_m_conId']:
            if openPosition['conid'] == ibexecutionrestful['contract_m_conId'] and openPosition['provider_account'] == ibexecutionrestful['execution_m_acctNumber']: # same asset & same account
                openPosition['position_current'] += ibexecutionrestful['execution_m_shares']

                openPosition['ntcf_d_local'] -= ibexecutionrestful['execution_m_shares'] * ibexecutionrestful['execution_m_price'] * ibexecutionrestful['contract_m_multiplier']


                found_in_daily_statement = True
                break

        if found_in_daily_statement == False:

            #current_app.logger.info(f'found exec which needs a new positions: {ibexecutionrestful["contract_m_symbol"]} ({ibexecutionrestful["contract_m_multiplier"]})')

            list_openPositions.append({
                'provider': 'IB', 
                'provider_account': ibexecutionrestful['execution_m_acctNumber'],
                'strategy': "MULTI",
                'position_current': ibexecutionrestful['execution_m_shares'], 
                'position_eod': 0,
                'ntcf_d_local': -1 * ibexecutionrestful['execution_m_shares'] * ibexecutionrestful['execution_m_price'] * ibexecutionrestful['contract_m_multiplier'] ,
                'conid': ibexecutionrestful['contract_m_conId'],

            })


    df_openPositions = pd.DataFrame(list_openPositions)

    current_app.logger.info(f'after merging open positions with intraday executions, positions: {len(df_openPositions)}')    


    for h in ['ntcf_d_local', 'position_current', 'position_eod']:
        df_openPositions[h] = df_openPositions[h].fillna(0)

    current_app.logger.info(f'before retrieving bridge from db')   
    df_bridge = pd.read_sql(sql="SELECT ticker, bbgIdentifier, bbgUnderylingId, internalUnderlying, ibcontract_conid FROM ibsymbology", con=db.engine)

    df_bridge.rename(columns={'ticker': 'bbg_ticker',
                      'bbgIdentifier': 'Identifier',
                      'bbgUnderylingId': 'bbg_underyling_id',
                      'internalUnderlying': 'Underlying',
                      'ibcontract_conid': 'conid', }
             , inplace=True)
    current_app.logger.info(f'after retrieving bridge from db')  

    df_openPositions = pd.merge(df_openPositions, df_bridge, on=['conid'], how='left')
    current_app.logger.info(f'after merging bridge with positions')

    if 'account' in input_data:
        current_app.logger.info('found limit account in user data')
        df_openPositions = df_openPositions[ df_openPositions['provider_account'] == input_data['account'] ]
        current_app.logger.info(f'after limiting with account { input_data["account"]}')
    

    # new fields
    df_openPositions['CUSTOM_accpbpid'] = [f'MULTI_IB_{Identifier}' for Identifier in df_openPositions['Identifier'] ]


    # subset with trading only
    df_openPositions = df_openPositions[ (df_openPositions['ntcf_d_local'] !=0) | (df_openPositions['position_current'] != df_openPositions['position_eod']) ]

    current_app.logger.info(f'positions with intraday trading: {len(df_openPositions)}')    
    

    # patch np.nan to json null
    #df_openPositions = df_openPositions.where((pd.notnull(df_openPositions)), None) # doesn't seem to work anymore
    df_openPositions = df_openPositions.astype(object).where((pd.notnull(df_openPositions)), None)

    headers_subset = [ 'conid', 'bbg_ticker', 'provider_account', 'CUSTOM_accpbpid', 'ntcf_d_local', 'position_current']
    headers_subset = [ 'CUSTOM_accpbpid', 'ntcf_d_local', 'position_current']

    return jsonify( {'status': 'ok', 'controller': 'reports', 'positionsCount': len(df_openPositions), 'data': df_openPositions[headers_subset].to_dict(orient='records') } )





@bp.route('/reports/ib/eod/v3/xls', methods=['POST'])
def ib_report_eod_v3_xls():
    current_app.logger.info('in /reports/ib/eod/v3/xls')

    current_app.logger.info('before reading user post content (exec, limit account)')
    input_data = request.get_json()
    current_app.logger.info('after reading user post content')

    if input_data != None and 'execDetails' in input_data:
        current_app.logger.info('found execDetails')
        # push dans le pool in 1 call
        current_app.logger.info('before pushing execDetails in pool')
        routes_ibexecutionrestfuls.ibexecutionrestfuls_insert_many(input_data['execDetails'])
        current_app.logger.info('after pushing execDetails in pool, done with user post executions')


    df_openPositions = pd.read_parquet( os.path.join(SCRIPT_ROOT + '/data/', 'ibkr_OpenPositions.parquet.gzip') )
    current_app.logger.info('after reading open positions parquet dataset')
    
    # limit for 1 account
    if 'account' in input_data:
        current_app.logger.info('found limit account in user data')
        df_openPositions = df_openPositions[ df_openPositions['provider_account'] == input_data['account'] ]
        current_app.logger.info(f'after limiting df_openPositions with account { input_data["account"]}')
    list_openPositions = df_openPositions.to_dict(orient='records')

    current_app.logger.info('after retrieving open position from fq report')


    # inject intraday executions
    current_app.logger.info('before call ib_upload_eod_report_date_v2()')
    date_obj = json.loads( ib_upload_eod_report_date_v2().get_data() )
    current_app.logger.info('after call ib_upload_eod_report_date_v2()')
    dt_report = datetime.strptime( date_obj['reportDate'], '%Y-%m-%d')
    dt_refExec = dt_report + timedelta(days=1)

    current_app.logger.info(f'ib_report_eod_v2_xls:: check refDate for executions {dt_refExec.strftime("%Y-%m-%d")}')


    current_executions = json.loads( routes_ibexecutionrestfuls.list_limit_date( dt_refExec.strftime("%Y-%m-%d") ).get_data() )
    current_app.logger.info(f'after retrieving today executions')

    for ibexecutionrestful_execution_m_execId in current_executions['executions']: #dict

        ibexecutionrestful = current_executions['executions'][ibexecutionrestful_execution_m_execId]

        if ibexecutionrestful['execution_m_acctNumber'][-1] == 'F':
            ibexecutionrestful['execution_m_acctNumber'] = ibexecutionrestful['execution_m_acctNumber'][:-1]
        found_in_daily_statement = False

        for openPosition in list_openPositions:

            if openPosition['conid'] == ibexecutionrestful['contract_m_conId'] and openPosition['provider_account'] == ibexecutionrestful['execution_m_acctNumber']: # same asset & same account
                openPosition['position_current'] += ibexecutionrestful['execution_m_shares']

                openPosition['position_d_chg'] = openPosition['position_current'] - openPosition['position_eod']

                openPosition['ntcf_d_local'] -= ibexecutionrestful['execution_m_shares'] * ibexecutionrestful['execution_m_price'] * ibexecutionrestful['contract_m_multiplier']

                openPosition['costBasisMoney_d_long'] += ibexecutionrestful['execution_m_shares'] * ibexecutionrestful['execution_m_price'] if ibexecutionrestful['execution_m_shares'] > 0 else 0

                openPosition['costBasisMoney_d_short'] += ibexecutionrestful['execution_m_shares'] * ibexecutionrestful['execution_m_price'] if ibexecutionrestful['execution_m_shares'] < 0 else 0

                if openPosition['position_d_chg'] == 0:
                    openPosition['costBasisPrice_d'] = 0
                elif openPosition['position_d_chg'] > 0:
                    openPosition['costBasisPrice_d'] = ( openPosition['costBasisMoney_d_long'] + openPosition['costBasisMoney_d_short'] ) / openPosition['position_d_chg']
                elif openPosition['position_d_chg'] < 0:
                    openPosition['costBasisPrice_d'] = ( openPosition['costBasisMoney_d_short'] + openPosition['costBasisMoney_d_long'] ) / openPosition['position_d_chg']


                found_in_daily_statement = True
                break

        if found_in_daily_statement == False:


            list_openPositions.append({
                'provider': 'IB', 
                'provider_account': ibexecutionrestful['execution_m_acctNumber'],
                'strategy': "MULTI",
                'position_current': ibexecutionrestful['execution_m_shares'],
                'pnl_d_local': 0,
                'pnl_y_local': 0,
                'pnl_y_eod_local': 0,
                'position_eod': 0,
                'price_eod': 0,
                'ntcf_d_local': -1 * ibexecutionrestful['execution_m_shares'] * ibexecutionrestful['execution_m_price'] * ibexecutionrestful['contract_m_multiplier'] ,
                'Symbole': f'{ibexecutionrestful["contract_m_symbol"]}/{ibexecutionrestful["contract_m_localSymbol"]}',
                'conid': ibexecutionrestful['contract_m_conId'],
                'costBasisMoney_d_long': ibexecutionrestful['execution_m_shares'] * ibexecutionrestful['execution_m_price'] if ibexecutionrestful['execution_m_shares'] > 0 else 0,
                'costBasisMoney_d_short': ibexecutionrestful['execution_m_shares'] * ibexecutionrestful['execution_m_price'] if ibexecutionrestful['execution_m_shares'] < 0 else 0,
                'costBasisPrice_eod': 0,
                'fifoPnlUnrealized_eod': 0,
                'costBasisPrice_d': ibexecutionrestful['execution_m_price'] / abs(ibexecutionrestful['execution_m_shares']) if ibexecutionrestful['execution_m_shares'] != 0 else 0
            })


    df_openPositions = pd.DataFrame(list_openPositions)

    current_app.logger.info(f'after merging open positions with intraday executions, positions: {len(df_openPositions)}')    


    # inject monthly pnl in base currency - ibcontracts are created with flex query results are retrieved
    df_MTDYTDPerformanceSummary = pd.read_parquet( os.path.join(SCRIPT_ROOT + '/data/', 'ibkr_MTDYTDPerformanceSummary.parquet.gzip') )
    current_app.logger.info(f'after reading mtd pnl parquet dataset')
    

    if 'account' in input_data:
        current_app.logger.info('found limit account in user data')
        df_MTDYTDPerformanceSummary = df_MTDYTDPerformanceSummary[ df_MTDYTDPerformanceSummary['accountId'] == input_data['account'] ]
        current_app.logger.info(f'after limiting df_MTDYTDPerformanceSummary with account { input_data["account"]}')


    list_openPositions = df_openPositions.to_dict(orient='records')

    for monthly_pnl in df_MTDYTDPerformanceSummary[ df_MTDYTDPerformanceSummary["mtmMTD"] != 0 ].to_dict(orient='records'):
        found_in_daily_statement = False
        for openPosition in list_openPositions:
            if monthly_pnl['conid'] == openPosition['conid'] and monthly_pnl['accountId'] == openPosition['provider_account']:
                found_in_daily_statement = True
                openPosition['pnl_m_eod_base'] = monthly_pnl['mtmMTD']
                break

        if found_in_daily_statement == False:
            list_openPositions.append({
                'provider': 'IB',
                'provider_account': monthly_pnl['accountId'],
                'strategy': "MULTI",
                'position_current': 0,
                'pnl_d_local': 0,
                'pnl_y_local': 0,
                'pnl_y_eod_local': 0,
                'position_eod': 0,
                'price_eod': 0,
                'ntcf_d_local': 0,
                'pnl_m_eod_base': monthly_pnl['mtmMTD'],
                'Symbole': f'{monthly_pnl["symbol"]}',
                'conid': monthly_pnl['conid'],
            })

    df_openPositions = pd.DataFrame(list_openPositions)
    current_app.logger.info(f'after merging open positions with mtd pnl: {len(df_openPositions)}')  


    for h in ['ntcf_d_local', 'pnl_d_local', 'pnl_m_eod_base', 'pnl_y_eod_local', 'pnl_y_local', 'position_current', 'position_eod', 'price_eod', 'costBasisPrice_eod', 'costBasisPrice_d']: # pnl_m_eod_base
        df_openPositions[h] = df_openPositions[h].fillna(0)

    current_app.logger.info(f'before retrieving bridge from db')   
    df_bridge = pd.read_sql(sql="SELECT ticker, bbgIdentifier, bbgUnderylingId, internalUnderlying, ibcontract_conid FROM ibsymbology", con=db.engine)

    df_bridge.rename(columns={'ticker': 'bbg_ticker',
                      'bbgIdentifier': 'Identifier',
                      'bbgUnderylingId': 'bbg_underyling_id',
                      'internalUnderlying': 'Underlying',
                      'ibcontract_conid': 'conid', }
             , inplace=True)
    current_app.logger.info(f'after retrieving symbology from db')

    df_openPositions = pd.merge(df_openPositions, df_bridge, on=['conid'], how='left')
    current_app.logger.info(f'after merging ibsymbology with positions')  

    # helpers symbology / mtd pnl
    df_underlying_pnl_m_eod_base = df_openPositions.groupby(by='Underlying', as_index=False)[['pnl_m_eod_base']].sum()
    dict_underyling_mtd_pnl = {}
    for row in df_underlying_pnl_m_eod_base.to_dict(orient='records'):
        dict_underyling_mtd_pnl[ row['Underlying'] ] = row['pnl_m_eod_base']
    
    df_conid_pnl_m_eod_base = df_openPositions.groupby(by='conid', as_index=False)[['pnl_m_eod_base']].sum()
    dict_conid_mtd_pnl = {}
    for row in df_conid_pnl_m_eod_base.to_dict(orient='records'):
        dict_conid_mtd_pnl[ row['conid'] ] = row['pnl_m_eod_base']

    df_openPositions_open_only = df_openPositions[ (df_openPositions['position_current'] != 0) | (df_openPositions['position_eod'] != 0) | (df_openPositions['ntcf_d_local'] != 0) ].reset_index(drop=True)
    current_app.logger.info(f'after dataset open positions only') 

    df_openPositions_underylingClose_with_m_pnl = df_openPositions[ df_openPositions['Underlying'].isin(df_underlying_pnl_m_eod_base[ ~df_underlying_pnl_m_eod_base['Underlying'].isin( df_openPositions_open_only['Underlying'] ) ]['Underlying']) ].reset_index(drop=True)
    current_app.logger.info(f'after dataset underyling with monthly pnl without open positions') 

    df_openPositions_min_rows = pd.concat([df_openPositions_open_only, df_openPositions_underylingClose_with_m_pnl], ignore_index=True).sort_values(by='bbg_ticker', ignore_index=True)
    current_app.logger.info(f'after smaller new dataset for fa_data, positions: {len(df_openPositions_min_rows)}') 
    
    
    list_underyling_proceeding = []
    data = []
    for row in df_openPositions_min_rows.to_dict(orient='records'):
        if pd.isnull(row['Underlying']) == False:
            if row['Underlying'] in list_underyling_proceeding:
                row['pnl_m_eod_base'] = 0
            else:
                if row['Underlying'] in dict_underyling_mtd_pnl:
                    row['pnl_m_eod_base'] = dict_underyling_mtd_pnl[ row['Underlying'] ]
                    list_underyling_proceeding.append( row['Underlying'] )
                else:
                    row['pnl_m_eod_base'] = 0
                    current_app.logger.info(f'missing monthly pnl row["Underlying"]')  
        else:
            if row['conid'] in dict_conid_mtd_pnl:
                row['pnl_m_eod_base'] = dict_conid_mtd_pnl[ row['conid'] ]
            else:
                row['pnl_m_eod_base'] = 0
                current_app.logger.info(f'missing monthly pnl row["conid"]')  
        data.append( row )
    df_openPositions_min_rows_with_monthly_pnl_once = pd.DataFrame( data )


    # patch np.nan to json null
    df_openPositions_min_rows_with_monthly_pnl_once = df_openPositions_min_rows_with_monthly_pnl_once.astype(object).where((pd.notnull(df_openPositions_min_rows_with_monthly_pnl_once)), None)

    headers_subset = ['conid', 'Identifier', 'Symbole', 'bbg_ticker', 'bbg_underyling_id', 'Underlying', 'provider', 'provider_account', 'strategy', 'ntcf_d_local', 'pnl_d_local', 'pnl_m_eod_base', 'pnl_y_eod_local', 'pnl_y_local', 'position_current', 'position_eod', 'price_eod', 'costBasisPrice_eod', 'costBasisPrice_d' ]

    return jsonify( {'status': 'ok', 'controller': 'reports', 'positionsCount': len(df_openPositions_min_rows_with_monthly_pnl_once), 'data': df_openPositions_min_rows_with_monthly_pnl_once[headers_subset].to_dict(orient='records') } )





# ibkr xml report - daily statement blocs processing
def ib_fq_dailyStatement_FlexStatements(doc):
    if "FlexQueryResponse" in doc:
        if isinstance( doc["FlexQueryResponse"]["FlexStatements"]["FlexStatement"], list): # more than 1 item
            return doc["FlexQueryResponse"]["FlexStatements"]["FlexStatement"]
        else:
            return [ doc["FlexQueryResponse"]["FlexStatements"]["FlexStatement"] ]


def ib_fq_dailyStatement_reportDate(doc):
    data = []

    list_FlexStatements = ib_fq_dailyStatement_FlexStatements(doc)

    for FlexStatement in list_FlexStatements:
        reportDate_str = FlexStatement["EquitySummaryInBase"]["EquitySummaryByReportDateInBase"]["@reportDate"]

        return f'{reportDate_str[0:4]}-{reportDate_str[4:6]}-{reportDate_str[6:]}'


def ib_fq_dailyStatement_EquitysummaryInBase(doc):
    data = []

    list_FlexStatements = ib_fq_dailyStatement_FlexStatements(doc)

    reportDate = ib_fq_dailyStatement_reportDate(doc)
    for FlexStatement in list_FlexStatements:

        dict_data = {}
        for dict_key in FlexStatement["EquitySummaryInBase"]["EquitySummaryByReportDateInBase"]:
            dict_data[dict_key[1:]] = FlexStatement["EquitySummaryInBase"]["EquitySummaryByReportDateInBase"][dict_key]

        # processing / transfo
        dict_data["reportDate"] = reportDate

        numFields = ['cash', 'brokerCashComponent', 'fdicInsuredBankSweepAccountCashComponent', 'slbCashCollateral', 'stock', 'slbDirectSecuritiesBorrowed', 'slbDirectSecuritiesLent', 'options', 'notes', 'funds', 'dividendAccruals', 'interestAccruals', 'brokerInterestAccrualsComponent', 'fdicInsuredAccountInterestAccrualsComponent', 'softDollars', 'forexCfdUnrealizedPl', 'cfdUnrealizedPl', 'total']
        for numField in numFields:
            dict_data[numField] = float(dict_data[numField])

        data.append(dict_data)

    return pd.DataFrame(data)


def ib_fq_dailyStatement_MTDYTDPerformanceSummary(doc):

    list_FlexStatements = ib_fq_dailyStatement_FlexStatements(doc)

    reportDate = ib_fq_dailyStatement_reportDate(doc)

    data = []

    if len(list_FlexStatements) > 0:
        
        for FlexStatement in list_FlexStatements:

            if 'MTDYTDPerformanceSummary' in FlexStatement:
                
                if FlexStatement['MTDYTDPerformanceSummary'] is None:
                    list_MTDYTDPerformanceSummaryUnderlying = []
                else:
                    if isinstance( FlexStatement['MTDYTDPerformanceSummary']['MTDYTDPerformanceSummaryUnderlying'], list): # more than 1 item
                        list_MTDYTDPerformanceSummaryUnderlying = FlexStatement['MTDYTDPerformanceSummary']['MTDYTDPerformanceSummaryUnderlying']
                    else:
                        list_MTDYTDPerformanceSummaryUnderlying = [ FlexStatement['MTDYTDPerformanceSummary']['MTDYTDPerformanceSummaryUnderlying'] ]

                    for MTDYTDPerformanceSummaryUnderlying in list_MTDYTDPerformanceSummaryUnderlying:

                        if MTDYTDPerformanceSummaryUnderlying['@description'] != 'Total' and  MTDYTDPerformanceSummaryUnderlying['@conid'] != '' :
                            dict_data = {}
                            for dict_key in MTDYTDPerformanceSummaryUnderlying:
                                dict_data[dict_key[1:]] = MTDYTDPerformanceSummaryUnderlying[dict_key]

                            int_fields = ['conid', 'underlyingConid']
                            for field in int_fields:
                                try:
                                    dict_data[field] = int(dict_data[field])
                                except:
                                    dict_data[field] = None

                            float_fields = ['strike', 'mtmMTD', 'multiplier']
                            for field in float_fields:
                                try:
                                    dict_data[field] = float(dict_data[field])
                                except:
                                    dict_data[field] = None

                            dict_data['reportDate'] = reportDate

                            dict_data['expiry'] = ibkr_patch_option_expiry_dt(dict_data['expiry'])

                            if dict_data['accountId'][-1] == 'F':
                                dict_data['accountId'] = dict_data['accountId'][:-1]
                            

                            found_in_pnl = False
                            for pnl in data:
                                if pnl['conid'] == dict_data['conid'] and pnl['accountId'] == dict_data['accountId']:
                                    # merge
                                    pnl['mtmMTD'] += dict_data['mtmMTD']
                                    found_in_pnl = True
                                    break
                            
                            if found_in_pnl == False:
                                data.append(dict_data)
            else:
                return pd.DataFrame([])

    return pd.DataFrame(data)


def ib_fq_dailyStatement_CashReport(doc):
    data = []

    list_FlexStatements = ib_fq_dailyStatement_FlexStatements(doc)

    if len(list_FlexStatements) > 0:
        reportDate = ib_fq_dailyStatement_reportDate(doc)
        for FlexStatement in list_FlexStatements:

            if isinstance( FlexStatement['CashReport']['CashReportCurrency'], list): # more than 1 item
                list_CashReportCurrency = FlexStatement['CashReport']['CashReportCurrency']
            else:
                list_CashReportCurrency = [ FlexStatement['CashReport']['CashReportCurrency'] ]

            for CashReportCurrency in list_CashReportCurrency:

                if CashReportCurrency["@currency"] != 'BASE_SUMMARY':
                    dict_data = {}
                    for dict_key in CashReportCurrency:
                        dict_data[dict_key[1:]] = CashReportCurrency[dict_key]

                    dict_data['reportDate'] = reportDate

                    del dict_data['fromDate']
                    del dict_data['toDate']

                    data.append(dict_data)

    return pd.DataFrame(data)


def ib_fq_dailyStatement_OpenPositions(doc):

    list_FlexStatements = ib_fq_dailyStatement_FlexStatements(doc)

    reportDate = ib_fq_dailyStatement_reportDate(doc)

    data_openPositions = []
    distinct_positions = {}
    for FlexStatement in list_FlexStatements:
        if FlexStatement['OpenPositions'] is None:
            list_OpenPositions = []
        else:
            if isinstance( FlexStatement['OpenPositions']['OpenPosition'], list): # more than 1 item
                list_OpenPositions = FlexStatement['OpenPositions']['OpenPosition']
            else:
                list_OpenPositions = [ FlexStatement['OpenPositions']['OpenPosition'] ]

        floatfields_OpenPosition = ['fxRateToBase', 'strike', 'markPrice', 'positionValue', 'percentOfNAV', 'costBasisMoney', 'costBasisPrice', 'fifoPnlUnrealized', 'multiplier']
        intfields_OpenPosition = ['conid', 'position']
        datefields_OpenPosition = ['reportDate']


        for OpenPosition in list_OpenPositions:
            api_OpenPosition = {}
            for key_OpenPosition in OpenPosition:
                if OpenPosition[key_OpenPosition] == '':
                    pass
                else:
                    if key_OpenPosition[1:] in floatfields_OpenPosition:
                        api_OpenPosition[key_OpenPosition[1:]] = float(OpenPosition[key_OpenPosition])
                    elif key_OpenPosition[1:] in intfields_OpenPosition:
                        api_OpenPosition[key_OpenPosition[1:]] = int(OpenPosition[key_OpenPosition])
                    elif key_OpenPosition[1:] in datefields_OpenPosition:
                        api_OpenPosition[key_OpenPosition[1:]] = f'{OpenPosition[key_OpenPosition][0:4]}-{OpenPosition[key_OpenPosition][4:6]}-{OpenPosition[key_OpenPosition][6:]}'
                    else:
                        api_OpenPosition[key_OpenPosition[1:]] = OpenPosition[key_OpenPosition]

            if api_OpenPosition['accountId'][-1] == 'F':
                api_OpenPosition['accountId'] = api_OpenPosition['accountId'][:-1]

            already_in_dataset = False
            for openPosition in data_openPositions:
                if openPosition['conid'] == api_OpenPosition['conid'] and openPosition['provider_account'] == api_OpenPosition['accountId']:
                    # merge
                    openPosition['position_current'] += api_OpenPosition['position']
                    openPosition['position_eod'] += api_OpenPosition['position']

                    if openPosition['price_eod'] != api_OpenPosition['markPrice']:
                        current_app.logger.warning(f'ib_fq_dailyStatement_OpenPositions:: merge 2 positions {openPosition["conid"]} with different eod prices.')
                    already_in_dataset = True
                    break
            
            if already_in_dataset == False:
                
                data_openPositions.append({
                    'provider': 'IB',
                    'provider_account': api_OpenPosition['accountId'],
                    'strategy': "MULTI",
                    'position_current': api_OpenPosition['position'],
                    'pnl_d_local': 0,
                    'pnl_y_local': 0,
                    'pnl_y_eod_local': 0,
                    'position_eod': api_OpenPosition['position'],
                    'position_d_chg': 0,
                    'price_eod': api_OpenPosition['markPrice'],
                    'ntcf_d_local': 0,
                    'Symbole': api_OpenPosition['symbol'],
                    'conid': api_OpenPosition['conid'],
                    'costBasisMoney_d_long': 0,
                    'costBasisMoney_d_short': 0,
                    'costBasisPrice_eod': api_OpenPosition['costBasisPrice'] if 'costBasisPrice' in api_OpenPosition else 0,
                    'fifoPnlUnrealized_eod': api_OpenPosition['fifoPnlUnrealized'],
                    'costBasisPrice_d': 0, 
                    'reportDate': reportDate,
                })


    return pd.DataFrame(data_openPositions)


def ib_fq_dailyStatement_Trades(doc):
    data = []

    list_FlexStatements = ib_fq_dailyStatement_FlexStatements(doc)

    if len(list_FlexStatements) > 0:
        reportDate = ib_fq_dailyStatement_reportDate(doc)
        for FlexStatement in list_FlexStatements:
            if 'Trades' in FlexStatement:
                if FlexStatement['Trades'] is not None:

                    if isinstance( FlexStatement['Trades']['Trade'], list): # more than 1 item
                        list_Trades = FlexStatement['Trades']['Trade']
                    else:
                        list_Trades = [ FlexStatement['Trades']['Trade'] ]

                    for Trade in list_Trades:
                        dict_data = {}
                        for dict_key in Trade:
                            dict_data[dict_key[1:]] = Trade[dict_key]
                        dict_data['reportDate'] = reportDate
                        data.append(dict_data)

    return pd.DataFrame(data)


def ib_fq_TransactionTaxes(doc):
    data = []

    list_FlexStatements = ib_fq_dailyStatement_FlexStatements(doc)

    if len(list_FlexStatements) > 0:
        reportDate = ib_fq_dailyStatement_reportDate(doc)
        for FlexStatement in list_FlexStatements:
            if 'TransactionTaxes' in FlexStatement:
                if FlexStatement['TransactionTaxes'] is not None:

                    if isinstance( FlexStatement['TransactionTaxes']['TransactionTax'], list): # more than 1 item
                        list_TransactionTaxes = FlexStatement['TransactionTaxes']['TransactionTax']
                    else:
                        list_TransactionTaxes = [ FlexStatement['TransactionTaxes']['TransactionTax'] ]

                    for TransactionTax in list_TransactionTaxes:
                        dict_data = {}

                        for dict_key in TransactionTax:
                            dict_data[dict_key[1:]] = TransactionTax[dict_key]

                        dict_data['reportDate'] = reportDate
                        data.append(dict_data)

    return pd.DataFrame(data)


def ib_fq_CFDCharges(doc):
    data = []

    list_FlexStatements = ib_fq_dailyStatement_FlexStatements(doc)

    if len(list_FlexStatements) > 0:
        reportDate = ib_fq_dailyStatement_reportDate(doc)
        for FlexStatement in list_FlexStatements:
            if 'CFDCharges' in FlexStatement:
                if FlexStatement['CFDCharges'] is not None:
                    if isinstance( FlexStatement['CFDCharges']['CFDCharge'], list): # more than 1 item
                        list_CFDCharges = FlexStatement['CFDCharges']['CFDCharge']
                    else:
                        list_CFDCharges = [ FlexStatement['CFDCharges']['CFDCharge'] ]

                    for CFDCharge in list_CFDCharges:
                        dict_data = {}

                        for dict_key in CFDCharge:
                            dict_data[dict_key[1:]] = CFDCharge[dict_key]

                        dict_data['reportDate'] = reportDate
                        data.append(dict_data)

    return pd.DataFrame(data)


def ib_fq_InterestAccruals(doc):
    data = []

    list_FlexStatements = ib_fq_dailyStatement_FlexStatements(doc)

    if len(list_FlexStatements) > 0:
        reportDate = ib_fq_dailyStatement_reportDate(doc)
        for FlexStatement in list_FlexStatements:
            if 'InterestAccruals' in FlexStatement:
                if FlexStatement['InterestAccruals'] is not None:
                    if isinstance( FlexStatement['InterestAccruals']['InterestAccrualsCurrency'], list): # more than 1 item
                        list_InterestAccruals = FlexStatement['InterestAccruals']['InterestAccrualsCurrency']
                    else:
                        list_InterestAccruals = [ FlexStatement['InterestAccruals']['InterestAccrualsCurrency'] ]

                    for InterestAccrualsCurrency in list_InterestAccruals:

                        if InterestAccrualsCurrency['@currency'] != 'BASE_SUMMARY':
                            dict_data = {}

                            for dict_key in InterestAccrualsCurrency:
                                dict_data[dict_key[1:]] = InterestAccrualsCurrency[dict_key]

                            dict_data['reportDate'] = reportDate
                            data.append(dict_data)

    return pd.DataFrame(data)


def ib_fq_ChangeInDividendAccruals(doc):
    data = []

    list_FlexStatements = ib_fq_dailyStatement_FlexStatements(doc)

    if len(list_FlexStatements) > 0:
        reportDate = ib_fq_dailyStatement_reportDate(doc)
        for FlexStatement in list_FlexStatements:
            if 'ChangeInDividendAccruals' in FlexStatement:
                if FlexStatement['ChangeInDividendAccruals'] is not None:
                    if isinstance( FlexStatement['ChangeInDividendAccruals']['ChangeInDividendAccrual'], list): # more than 1 item
                        list_ChangeInDividendAccruals = FlexStatement['ChangeInDividendAccruals']['ChangeInDividendAccrual']
                    else:
                        list_ChangeInDividendAccruals = [ FlexStatement['ChangeInDividendAccruals']['ChangeInDividendAccrual'] ]

                    for ChangeInDividendAccrual in list_ChangeInDividendAccruals:
                        dict_data = {}

                        for dict_key in ChangeInDividendAccrual:
                            dict_data[dict_key[1:]] = ChangeInDividendAccrual[dict_key]

                        dict_data['reportDate'] = reportDate
                        data.append(dict_data)

    return pd.DataFrame(data)


def ib_fq_ConversionRates(doc):
    data = []

    list_FlexStatements = ib_fq_dailyStatement_FlexStatements(doc)

    if len(list_FlexStatements) > 0:
        reportDate = ib_fq_dailyStatement_reportDate(doc)
        for FlexStatement in list_FlexStatements:
            if 'ConversionRates' in FlexStatement:
                if FlexStatement['ConversionRates'] is not None:
                    if isinstance( FlexStatement['ConversionRates']['ConversionRate'], list): # more than 1 item
                        list_ChangeInDividendAccruals = FlexStatement['ConversionRates']['ConversionRate']
                    else:
                        list_ChangeInDividendAccruals = [ FlexStatement['ConversionRates']['ConversionRate'] ]

                    for ConversionRate in list_ChangeInDividendAccruals:
                        dict_data = {}

                        for dict_key in ConversionRate:
                            dict_data[dict_key[1:]] = ConversionRate[dict_key]

                        dict_data['reportDate'] = reportDate
                        data.append(dict_data)

    return pd.DataFrame(data)


@bp.route('/backup/ibkr', methods=['GET'])
def ib_backup_ibkr():
    return send_file(os.path.join(SCRIPT_ROOT + '/data/', IB_FQ_LAST))


@bp.route('/backup/db', methods=['GET'])
def ib_backup_db():
    alchemy = AlchemyDumpsDatabase()
    data = alchemy.get_data()
    backup = Backup()
    files = []
    for class_name in data.keys():
        name = backup.get_name(class_name)
        full_path = backup.target.create_file(name, data[class_name])
        files.append(full_path)
        rows = len(alchemy.parse_data(data[class_name]))
        if full_path:
            current_app.logger.info(f'successfully backup {full_path}')
            pass
        else:
            pass
    backup.close_ftp()

    if len(files) > 0:
        with tarfile.open(os.path.join(SCRIPT_ROOT + '/data/', 'backup_db.gz'), 'w:gz') as fp:
            for p in files:
                fp.add(p)

        return send_file(os.path.join(SCRIPT_ROOT + '/data/', 'backup_db.gz'))
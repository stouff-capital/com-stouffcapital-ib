from datetime import datetime
from flask import render_template, flash, json, redirect, url_for, request, g, jsonify, current_app
from werkzeug.utils import secure_filename
import os
import csv
import pandas as pd
import numpy as np
import requests
from app import db
from app.models import Contract, Bbg
from app.reports import bp
from app.contracts import routes as routes_contract


SCRIPT_ROOT = os.path.dirname(os.path.abspath(__file__))
ALLOWED_EXTENSIONS = set(['csv'])

# utilities
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@bp.route('/reports', methods=['GET'])
def reports_list():
    return jsonify( {'status': 'ok'} )


@bp.route('/reports/ib/eod/upload', methods=['GET'])
def ib_upload_eod_report_template():
    return render_template('upload.html', page_title="Upload IB eod report", upload_url="/reports/ib/eod")


@bp.route('/reports/ib/eod', methods=['POST'])
def ib_upload_eod_report():

    if 'file' not in request.files:
        current_app.logger.warning('missing file')
        return jsonify( {'error': 'missing file'} )

    file = request.files['file']
    if file.filename == '':
        current_app.logger.warning('file is empty')
        return jsonify( {'error': 'file is empty'} )

    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        filename = filename.split("_")[0] + "_last.csv"
        file.save(os.path.join(SCRIPT_ROOT + '/data/', filename))

        return jsonify( {'status': 'ok', 'message': 'successfully uploaded'} )


@bp.route('/reports/ib/eod', methods=['GET'])
def ib_eod_positions():

    '''
    if 'file' not in request.files:
        current_app.logger.warning('missing file')
        return jsonify( {'error': 'missing file'} )

    file = request.files['file']
    if file.filename == '':
        current_app.logger.warning('file is empty')
        return jsonify( {'error': 'file is empty'} )

    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)


        file.save(os.path.join(SCRIPT_ROOT + '/data/', filename))
    '''

    data = {}
    filename = 'U1160693_last.csv'

    if os.path.isfile(os.path.join(SCRIPT_ROOT + '/data/', filename)):
        pass
    else:
        current_app.logger.warning('missing file')
        return jsonify( {'error': 'missing file'} )


    with open(os.path.join(SCRIPT_ROOT + '/data/', filename), 'r', encoding='utf-8-sig') as f:
        for row in f:
            for one_row in csv.reader([row]):
                break

            if one_row[0] != '':
                if one_row[0] in data:
                    pass
                else:
                    data[ one_row[0] ] = []

                data[ one_row[0] ].append(one_row)


        headers_intraday = ["intraday_positionChg", "intraday_ntcf"]



        # Informations instrument financier (open pos only)
        header = 'Informations instrument financier'

        list_multiple_assetClasses = data[header]


        dict_assetClasses = {}
        k = 0
        last_header = ''
        for row in list_multiple_assetClasses:

            if row[1] == 'Header':
                last_header = str(k)
                k = k + 1
                dict_assetClasses[ last_header ] = []
            dict_assetClasses[ last_header ].append( row )


        list_refData = []

        cols = []

        numeric_fields = ['Multiplicateur']

        for assetClass in dict_assetClasses:
            cols = cols + dict_assetClasses[assetClass][0]
            cols = list(set(cols))

        for assetClass in dict_assetClasses:
            df_refData = pd.DataFrame(
                data=dict_assetClasses[assetClass][1:],
                columns=dict_assetClasses[assetClass][0]
            )
            df_refData = df_refData[ df_refData['Header'] == 'Data' ]


            list_refData.append(df_refData)

        #cols
        #df_refData = pd.concat(list_refData, axis=0, ignore_index=True, sort=False)
        df_refData = pd.concat(list_refData, axis=0, ignore_index=True)

        for field in numeric_fields:
            df_refData[field] = df_refData[field].replace(to_replace='--', value=np.nan)
            df_refData[field] = pd.to_numeric( df_refData[field].str.replace(',', '') )


        date_fields = ['Expiration']
        for field in date_fields:
            df_refData[field] = pd.to_datetime( df_refData[field], format="%Y-%m-%d" )


        headers_refData = ['Catégorie d\'actifs', 'Symbole', 'Description', 'Conid', 'Multiplicateur', 'Expiration', 'Type', 'Prix de Levée' ]
        headers_toShow = headers_refData


        list_refData = df_refData.to_dict(orient='record')

        refData = {}
        '''
        for asset in list_refData:
            refData[ asset['Symbole'] ] = asset
            refData[ asset['Description'] ] = asset # for options
        '''
        for asset in list_refData:
            #if asset['Catégorie d\'actifs'][0:6].upper() == 'Option'.upper():
                #tmp_description = pnl['Symbole']
                #tmp_symbole = pnl['Description']

                #pnl['Symbole'] = tmp_symbole
                #pnl['Description'] = tmp_description

            refData[ asset['Symbole'] ] = asset

        df_refData = pd.DataFrame( list_refData )


        # Synthèse de la performance pour le mois et l'année en cours (everything)
        header = 'Synthèse de la performance pour le mois et l\'année en cours'

        numeric_fields = ['Évalué-au-Marché MJM', 'Évalué-au-Marché YTD', 'Réalisé C/T MJM', 'Réalisé C/T YTD', 'Realisé L/T MJM', 'Realisé L/T YTD']

        try:
            df_mthYtd_Pnl = pd.DataFrame(
                data=data[header][1:],
                columns=data[header][0])

            df_mthYtd_Pnl = df_mthYtd_Pnl[ df_mthYtd_Pnl['Header'] == 'Data' ]
            df_mthYtd_Pnl = df_mthYtd_Pnl[ df_mthYtd_Pnl['Catégorie d\'actifs'] != 'Total' ]
            df_mthYtd_Pnl = df_mthYtd_Pnl[ df_mthYtd_Pnl['Catégorie d\'actifs'] != 'Total (Tous les actifs)' ]

            for field in numeric_fields:
                df_mthYtd_Pnl[field] = df_mthYtd_Pnl[field].replace(to_replace='--', value=np.nan)
                df_mthYtd_Pnl[field] = pd.to_numeric( df_mthYtd_Pnl[field].str.replace(',', '') )

            # inject intraday
            for intraday_field in headers_intraday:
                df_mthYtd_Pnl[intraday_field] = 0.0

            headers_pnl = ['Catégorie d\'actifs', 'Symbole', 'Évalué-au-Marché YTD' ] + headers_intraday
            headers_toShow = headers_pnl
        except:
            df_mthYtd_Pnl = pd.DataFrame([])


        # positions ouvertes
        header = 'Positions ouvertes'

        #numeric_fields = ['Quantité', 'Mult', 'Prix d\'origine', 'Valeur d\'Origine', 'Prix de Fermeture', 'Valeur', 'P/L Non-Réalisé', 'P&L non réalisé %']
        numeric_fields = ['Quantité', 'Mult', 'Prix de Fermeture', 'Valeur']

        df_openPositions = pd.DataFrame(
            data=data[header][1:],
            columns=data[header][0])


        df_openPositions = df_openPositions[ df_openPositions['Header'] == 'Data' ]

        for field in numeric_fields:
            df_openPositions[field] = df_openPositions[field].replace(to_replace='--', value=np.nan)
            df_openPositions[field] = pd.to_numeric( df_openPositions[field].str.replace(',', '') )

        # inject intraday
        for intraday_field in headers_intraday:
            df_openPositions[intraday_field] = 0.0

        #headers_open = ['Catégorie d\'actifs', 'Devise', 'Symbole', 'Quantité', 'Mult', 'Prix de Fermeture', 'Valeur', 'P/L Non-Réalisé'] + headers_intraday
        headers_open = ['Catégorie d\'actifs', 'Devise', 'Symbole', 'Quantité', 'Mult', 'Prix de Fermeture', 'Valeur'] + headers_intraday
        headers_toShow = headers_open


        # try to patch option beautiful symbole
        list_openPositions = df_openPositions.to_dict(orient='record')
        for openPos in list_openPositions:
            if openPos['Catégorie d\'actifs'][:6].upper() == 'Option'.upper():
                 mask = df_refData[ df_refData['Description'] == openPos['Symbole'] ]
                 if len(mask) > 0:
                     openPos['Symbole'] = mask['Symbole'].values[0]
        df_openPositions = pd.DataFrame(list_openPositions)

        list_openPositions = df_openPositions.to_dict(orient='record')

        for pos in list_openPositions:
            if pos['Symbole'] in refData:

                #merged content
                #for refData_field in refData[ pos['Symbole'] ]:
                for refData_field in headers_refData:
                    if refData_field not in pos:
                        pos[ refData_field ] = refData[ pos['Symbole'] ] [ refData_field ]

            else:
                current_app.logger.warning('cannot find refData for openPositions: ' + pos['Symbole'])

        df_openPositions = pd.DataFrame(list_openPositions)


        list_mthYtd_Pnl = df_mthYtd_Pnl.to_dict(orient='record')


        list_closePosToAdd = []
        for pnl in list_mthYtd_Pnl:
            found_pnl = False
            for pos in list_openPositions:
                if pos['Symbole'] == pnl['Symbole'] or pos['Symbole'] == pnl['Description']: #options #
                    found_pnl = True
                    for pnl_field in headers_pnl:
                        if pnl_field not in pos:
                            pos[ pnl_field ] = pnl [ pnl_field ]
            if found_pnl == False:

                # create empty pos
                tmp_pnl = {}

                tmp_pnl['P/L Non-Réalisé'] = 0
                tmp_pnl['P&L non réalisé %'] = 0
                tmp_pnl['Positions ouvertes'] = 0
                tmp_pnl['Quantité'] = 0
                tmp_pnl['Valeur'] = 0
                for pnl_field in headers_pnl + headers_intraday:
                    tmp_pnl[pnl_field] = pnl[pnl_field]

                list_closePosToAdd.append(tmp_pnl)

        df_openPositions = pd.DataFrame(list_openPositions + list_closePosToAdd)



        # merge with input list with transactions
        list_openPositions = df_openPositions.to_dict(orient='record')

        new_openPos = []
        input_data = request.get_json()
        if input_data != None and 'execDetails' in input_data:
            for exec in input_data['execDetails']:
                already_in_dataset = False

                side = 1
                if exec['execution']['m_side'][:1].upper() == "B":
                    side = 1
                elif exec['execution']['m_side'][:1].upper() == "S":
                    side = -1

                if exec['contract']['m_multiplier']!= None and exec['contract']['m_multiplier'].lstrip('-').replace('.','',1).isdigit():
                    contractSize = float(exec['contract']['m_multiplier'])
                else:
                    contractSize = 1

                for pos in list_openPositions:
                    if pos['Symbole'] == exec['contract']['m_localSymbol'] or pos['Description'] == exec['contract']['m_localSymbol']: #options #
                        already_in_dataset = True

                        pos['intraday_positionChg'] += side * exec['execution']['m_shares']
                        pos['intraday_ntcf'] += -side * exec['execution']['m_shares'] * exec['execution']['m_price'] * contractSize


                        #current_app.logger.info((exec['contract']['m_localSymbol'] + ' already in db')
                        break

                if already_in_dataset == False:
                    current_app.logger.info("from exec, totally new symbol: " + exec['contract']['m_localSymbol'])

                    # browse new pos
                    already_in_new_pos = False
                    for new_pos in new_openPos:
                        if new_pos['Symbole'] == exec['contract']['m_localSymbol'] or new_pos['Description'] == exec['contract']['m_localSymbol']:

                            already_in_new_pos = True

                            new_pos['intraday_positionChg'] += side * exec['execution']['m_shares']
                            new_pos['intraday_ntcf'] += -side * exec['execution']['m_shares'] * exec['execution']['m_price'] * contractSize

                    if already_in_new_pos == False:

                        if exec['contract']['m_expiry'] != None and exec['contract']['m_expiry'] != '' and exec['contract']['m_expiry'].lstrip('-').replace('.','',1).isdigit():
                            expiryDate = pd.to_datetime(exec['contract']['m_expiry'], format="%Y%m%d")
                        else:
                            expiryDate = pd.NaT


                        new_pos = {
                            'intraday_positionChg': side * exec['execution']['m_shares'],
                            'intraday_ntcf':  -side * exec['execution']['m_shares'] * exec['execution']['m_price'] * contractSize,
                            'Catégorie d\'actifs': exec['contract']['m_secType'],
                            'Conid': exec['contract']['m_conId'],
                            'Devise': exec['contract']['m_currency'],
                            'Expiration': exec['contract']['m_expiry'],
                            'Mult': contractSize,
                            'Multiplicateur': contractSize,
                            'P&L non réalisé %': 0,
                            'P/L Non-Réalisé': 0,
                            'Positions ouvertes': 0,
                            'Prix d\'origine': 0,
                            'Prix de Fermeture': 0,
                            'Type': exec['contract']['m_right'],
                            'Prix de Levée': exec['contract']['m_strike'],
                            'Quantité': 0,
                            'Symbole': exec['contract']['m_localSymbol'],
                            'Description': exec['contract']['m_localSymbol'], # ?
                            'Valeur': 0,
                            'Valeur d\'Origine': 0,
                            'Évalué-au-Marché YTD': 0

                        }
                        new_openPos.append(new_pos)



            df_openPositions = pd.DataFrame(list_openPositions + new_openPos)



        # final report
        headers_intraday = ["intraday_positionChg", "intraday_ntcf"]


        df_openPositions['provider'] = "IB"
        df_openPositions['strategy'] = filename.split('_')[0]
        df_openPositions['CUSTOM_accpbpid'] = df_openPositions[['strategy', 'provider', 'Symbole']].apply(lambda x: '_'.join(x), axis=1)
        df_openPositions['position_current'] = df_openPositions['Quantité'] + df_openPositions['intraday_positionChg']
        df_openPositions['pnl_d_local'] = 0
        df_openPositions['pnl_y_local'] = 0
        try:
            df_openPositions['pnl_y_eod_local'] = df_openPositions['Évalué-au-Marché YTD']
        except:
            df_openPositions['pnl_y_eod_local'] = 0
        df_openPositions['position_eod'] = df_openPositions['Quantité']
        df_openPositions['price_eod'] = df_openPositions['Prix de Fermeture']
        df_openPositions['ntcf_d_local'] = df_openPositions['intraday_ntcf']


        # merge avec CV bbg
        df_bridge = pd.read_sql(sql="SELECT * FROM bbg", con=db.engine)

        df_bridge.rename(columns={'ticker': 'bbg_ticker',
                          'bbgIdentifier': 'Identifier',
                          'bbgUnderylingId': 'bbg_underyling_id',
                          'internalUnderlying': 'Underlying',
                          'contract_localSymbol': 'Symbole', }
                 , inplace=True)

        df_openPositions = pd.merge(df_openPositions, df_bridge, on=['Symbole'], how='left')

        # patch np.nan to json null
        df = df_openPositions.where((pd.notnull(df_openPositions)), None)

        if len(df_bridge) == 0:
            col_to_export = ['provider', 'strategy', 'CUSTOM_accpbpid', 'position_current', 'pnl_d_local', 'pnl_y_local', 'pnl_y_eod_local', 'position_eod', 'price_eod', 'ntcf_d_local', 'Symbole', 'Description']

            list_openPositions = df_openPositions.to_dict(orient='record')

            for pos in list_openPositions:
                check_existing_contrat = json.loads( routes_contract.contract_exists(pos['Symbole']).get_data() )
                if check_existing_contrat['exists'] == False:
                    contract = Contract(
                        localSymbol=pos['Symbole']
                    )
                    db.session.add(contract)
                    db.session.commit()
                    current_app.logger.info('new contract in openPositions: ' + pos['Symbole'])

        else:
            col_to_export = ['Identifier', 'bbg_underyling_id', 'Underlying', 'bbg_ticker', 'provider', 'strategy', 'CUSTOM_accpbpid', 'position_current', 'pnl_d_local', 'pnl_y_local', 'pnl_y_eod_local', 'position_eod', 'price_eod', 'ntcf_d_local', 'Symbole', 'Description']

            # creation des contracts si necessaire
            list_openPositions = df_openPositions.to_dict(orient='record')
            for pos in list_openPositions:
                if pd.isnull(pos['Identifier']):
                    check_existing_contrat = json.loads( routes_contract.contract_exists(pos['Symbole']).get_data() )
                    if check_existing_contrat['exists'] == False:
                        contract = Contract(
                            localSymbol=pos['Symbole']
                        )
                        db.session.add(contract)
                        db.session.commit()
                        current_app.logger.info('new contract in openPositions: ' + pos['Symbole'])


        return jsonify( df[:][col_to_export].to_dict(orient='records') )

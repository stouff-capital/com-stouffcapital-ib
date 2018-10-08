from datetime import datetime
from flask import render_template, flash, json, redirect, url_for, request, Response, g, jsonify, current_app
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
from app.executions import routes as routes_executions


SCRIPT_ROOT = os.path.dirname(os.path.abspath(__file__))
IB_FILE_LAST = 'MULTI_last.csv'
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
        return input_datetime.strftime('%Y-%m-%d %H:%M:%S')


# ib data preparation
def ib_eod_data_preparation():
    data = {}

    if os.path.isfile(os.path.join(SCRIPT_ROOT + '/data/', IB_FILE_LAST)):
        pass
    else:
        current_app.logger.warning('missing file: ' + IB_FILE_LAST)
        return {}


    with open(os.path.join(SCRIPT_ROOT + '/data/', IB_FILE_LAST), 'r', encoding='utf-8-sig') as f:
        for row in f:
            for one_row in csv.reader([row]):
                break

            if one_row[0] != '':
                if one_row[0] in data:
                    pass
                else:
                    data[ one_row[0] ] = []

                data[ one_row[0] ].append(one_row)

    return data


def ib_eod_data_statement():
    data = ib_eod_data_preparation()

    if len(data) == 0:
        return pd.DataFrame( [] )
    else:
        header = "Statement"
        df_statement = pd.DataFrame(
                        data=data[header][1:],
                        columns=data[header][0])

        df_statement = df_statement[ df_statement['Header'] == 'Data' ]

        df_statement.drop(columns=['Header', 'Statement'], inplace=True)

        return df_statement


def ib_eod_data_informationsDuCompte():
    data = ib_eod_data_preparation()

    if len(data) == 0:
        return pd.DataFrame( [] )
    else:
        header = "Informations du compte"
        df_informationsDuCompte = pd.DataFrame(
                        data=data[header][1:],
                        columns=data[header][0])

        df_informationsDuCompte = df_informationsDuCompte[ df_informationsDuCompte['Header'] == 'Data' ]

        df_informationsDuCompte.drop(columns=['Header', 'Informations du compte'], inplace=True)

        list_informationsDuCompte = df_informationsDuCompte.to_dict(orient='record')

        dict_informationsDuCompte = {}
        for data in list_informationsDuCompte:
            dict_informationsDuCompte[ data['Nom champ'] ] = data['Valeur champ']

        #return df_informationsDuCompte
        return dict_informationsDuCompte


def ib_eod_data_actifNet():
    data = ib_eod_data_preparation()

    if len(data) == 0:
        return pd.DataFrame( [] )
    else:
        header = "Actif net"
        df_actifNet = pd.DataFrame(
                        data=data[header][1:],
                        columns=data[header][0])

        df_actifNet = df_actifNet[ df_actifNet['Header'] == 'Data' ]

        numeric_fields = ['Total précédent', 'Actuel long', 'Actuel short', 'Total actuel', 'Variation']
        for field in numeric_fields:
            df_actifNet[field] = df_actifNet[field].replace(to_replace='--', value=np.nan)
            df_actifNet[field] = pd.to_numeric( df_actifNet[field].str.replace(',', '') )

        df_actifNet.drop(columns=['Header', 'Actif net'], inplace=True)

        return df_actifNet


def ib_eod_data_changementsActifNet():
    data = ib_eod_data_preparation()

    if len(data) == 0:
        return pd.DataFrame( [] )
    else:
        header = 'Changements de l\'actif net'

        df_changementsActifNet = pd.DataFrame(
            data=data[header][1:],
            columns=data[header][0])

        numeric_fields = ['Valeur champ']
        for field in numeric_fields:
            df_changementsActifNet[field] = df_changementsActifNet[field].replace(to_replace='--', value=np.nan)
            df_changementsActifNet[field] = pd.to_numeric( df_changementsActifNet[field].str.replace(',', '') )

        df_changementsActifNet.drop(columns=['Header', 'Changements de l\'actif net'], inplace=True)

        list_changementsActifNet = df_changementsActifNet.to_dict(orient='record')

        dict_changementsActifNet = {}
        for data in list_changementsActifNet:
            dict_changementsActifNet[ data['Nom champ'] ] = data['Valeur champ']

        #return df_changementsActifNet
        return dict_changementsActifNet


def ib_eod_data_instruments():
    data = ib_eod_data_preparation()

    if len(data) == 0:
        return pd.DataFrame( [] )
    else:

        # bridge ib <-> bbg
        df_bridge = pd.read_sql(sql="SELECT * FROM bbg", con=db.engine)

        df_bridge.rename(columns={'ticker': 'bbg_ticker',
                          'bbgIdentifier': 'Identifier',
                          'bbgUnderylingId': 'bbg_underyling_id',
                          'internalUnderlying': 'Underlying',
                          'contract_localSymbol': 'Symbole', }
                 , inplace=True)

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
        df_refData = pd.concat(list_refData, axis=0, ignore_index=True)

        for field in numeric_fields:
            df_refData[field] = df_refData[field].replace(to_replace='--', value=np.nan)
            df_refData[field] = pd.to_numeric( df_refData[field].str.replace(',', '') )


        date_fields = ['Expiration']
        for field in date_fields:
            df_refData[field] = pd.to_datetime( df_refData[field], format="%Y-%m-%d" )


        df_refData = pd.merge(df_refData, df_bridge, on=['Symbole'], how='left')


        headers_refData = ['Catégorie d\'actifs', 'Symbole', 'Description', 'Conid', 'Multiplicateur', 'Expiration', 'Type', 'Prix de Levée' ]
        headers_toShow = headers_refData


        list_refData = df_refData.to_dict(orient='record')

        refData = {}
        for asset in list_refData:
            refData[ asset['Symbole'] ] = asset

        df_refData = pd.DataFrame( list_refData )

        df_refData.drop(columns=['Header', 'Informations instrument financier'], inplace=True)

        return df_refData


def ib_eod_data_fxrate():
    data = ib_eod_data_preparation()

    if len(data) == 0:
        return pd.DataFrame( [] )
    else:
        header = 'Taux de Change Devise de Base'

        df_fxRate = pd.DataFrame(
            data=data[header][1:],
            columns=data[header][0])

        numeric_fields = ['Taux']
        for field in numeric_fields:
            df_fxRate[field] = df_fxRate[field].replace(to_replace='--', value=np.nan)
            df_fxRate[field] = pd.to_numeric( df_fxRate[field].str.replace(',', '') )

    df_fxRate.drop(columns=['Header', 'Taux de Change Devise de Base'], inplace=True)

    return df_fxRate


def ib_eod_data_positions():
    data = ib_eod_data_preparation()

    if len(data) == 0:
        return pd.DataFrame( [] )
    else:
        # mise a zero des ytd pnl
        df_mthYtd_Pnl = pd.DataFrame([])

        df_fxRate = pd.concat( [ pd.DataFrame( [{'Devise': 'EUR', 'Taux': 1.0}] ), ib_eod_data_fxrate() ] )
        df_fxRate.set_index('Devise', inplace=True)

        df_refData = ib_eod_data_instruments()

        list_refData = df_refData.to_dict(orient='record')

        refData = {}
        for asset in list_refData:
            refData[ asset['Symbole'] ] = asset


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

        #headers_open = ['Catégorie d\'actifs', 'Devise', 'Symbole', 'Quantité', 'Mult', 'Prix de Fermeture', 'Valeur', 'P/L Non-Réalisé']
        headers_open = ['Catégorie d\'actifs', 'Devise', 'Symbole', 'Quantité', 'Mult', 'Prix de Fermeture', 'Valeur']
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
                for refData_field in list(df_refData.columns.values):
                    if refData_field not in pos:
                        pos[ refData_field ] = refData[ pos['Symbole'] ] [ refData_field ]

            else:
                current_app.logger.warning('cannot find refData for openPositions: ' + pos['Symbole'])

        df_openPositions = pd.DataFrame(list_openPositions)


        list_openPositions = df_openPositions.to_dict(orient='records')
        for openPos in list_openPositions:
            openPos['fxRate'] = df_fxRate.loc[openPos['Devise']]['Taux']
            openPos['valeurBase'] = openPos['Valeur'] * openPos['fxRate']
            openPos['valeurGrossBase'] = abs(openPos['valeurBase'])
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

        df_openPositions.drop(columns=['Positions ouvertes', 'Header'], inplace=True)

        return df_openPositions


def ib_eod_data_transactions():
    data = ib_eod_data_preparation()

    if len(data) == 0:
        return pd.DataFrame( [] )
    else:
        df_fxRate = pd.concat( [ pd.DataFrame( [{'Devise': 'EUR', 'Taux': 1.0}] ), ib_eod_data_fxrate() ] )
        df_fxRate.set_index('Devise', inplace=True)

        df_refData = ib_eod_data_instruments()
        list_refData = df_refData.to_dict(orient='record')
        refData = {}
        for asset in list_refData:
            refData[ asset['Symbole'] ] = asset
        df_refData.drop(columns=['Catégorie d\'actifs', 'Code'], inplace=True)

        header = "Transactions"
        df_transactions = pd.DataFrame(
                        data=data[header][1:],
                        columns=data[header][0])

        df_transactions = df_transactions[ df_transactions['Header'] == 'Data' ]

        # patch option symbole
        list_transactions = df_transactions.to_dict(orient='record')
        for transaction in list_transactions:
            if transaction['Catégorie d\'actifs'][:6].upper() == 'Option'.upper():
                 mask = df_refData[ df_refData['Description'] == transaction['Symbole'] ]
                 if len(mask) > 0:
                     transaction['Symbole'] = mask['Symbole'].values[0]
        df_transactions = pd.DataFrame(list_transactions)


        df_transactions = pd.merge(df_transactions, df_refData, on=['Symbole'], how='left')

        numeric_fields = ['Comm/Tarif', 'P/L MTM', 'Prix Ferm.', 'Prix Trans.', 'Quantité', 'Produits']
        for field in numeric_fields:
            df_transactions[field] = df_transactions[field].replace(to_replace='--', value=np.nan)
            df_transactions[field] = pd.to_numeric( df_transactions[field].str.replace(',', '') )

        date_fields = ['Date/Heure'] # "2018-07-27, 04:50:46"
        for field in date_fields:
            df_transactions[field] = pd.to_datetime( df_transactions[field], format="%Y-%m-%d, %H:%M:%S" )

        # inject Forex
        list_transactions = df_transactions.to_dict(orient='records')
        for transaction in list_transactions:
            transaction['fxRate'] = df_fxRate.loc[transaction['Devise']]['Taux']
            transaction['valeurBase'] = transaction['Produits'] * transaction['fxRate']
            transaction['valeurGrossBase'] = abs(transaction['valeurBase'])
            transaction['Comm/TarifBase'] = -abs(transaction['Comm/Tarif']) * transaction['fxRate']
            try:
                transaction['TarifBps'] = 10000 * ( -abs(transaction['Comm/Tarif']) / abs(transaction['Produits']) )
            except:
                transaction['TarifBps'] = 0

        df_transactions = pd.DataFrame(list_transactions)

        df_transactions.drop(columns=['Header', 'Transactions'], inplace=True)

        return df_transactions



# routes
@bp.route('/reports', methods=['GET'])
def reports_list():
    return jsonify( {'status': 'ok'} )


@bp.route('/reports/ib/eod/csv', methods=['GET'])
def ib_expose_eod_file():
    return Response( get_file('data/U1160693_last.csv'), headers={'Content-Disposition': 'attachment; filename=ib_eod.csv'}, mimetype="text/csv")


# bi
@bp.route('/reports/ib/eod/statement', methods=['GET'])
def ib_eod_statement():
    df = ib_eod_data_statement()
    return jsonify( df.to_dict(orient='records') )


@bp.route('/reports/ib/eod/informationsducompte', methods=['GET'])
def ib_eod_informationsDuCompte():
    #df = ib_eod_data_informationsDuCompte()
    #return jsonify( df.to_dict(orient='records') )

    return jsonify( ib_eod_data_informationsDuCompte() )


@bp.route('/reports/ib/eod/actifnet', methods=['GET'])
def ib_eod_actifNet():
    df = ib_eod_data_actifNet()
    return jsonify( df.to_dict(orient='records') )


@bp.route('/reports/ib/eod/changementsactifnet', methods=['GET'])
def ib_eod_changementsActifNet():
    #df = ib_eod_data_changementsActifNet()
    #return jsonify( df.to_dict(orient='records') )

    return jsonify( ib_eod_data_changementsActifNet() )


@bp.route('/reports/ib/eod/instruments', methods=['GET'])
def ib_eod_instruments():
    df_refData = ib_eod_data_instruments()

    headers_date = ['Expiration']
    for field_date in headers_date:
        df_refData[field_date] = df_refData[field_date].apply(patch_missing_date)

    df_refData = df_refData.where((pd.notnull(df_refData)), None)

    return jsonify( df_refData.to_dict(orient='records') )


@bp.route('/reports/ib/eod/fxrate', methods=['GET'])
def ib_eod_fxrate():
    df_fxrate = ib_eod_data_fxrate()

    df_fxrate = df_fxrate.where((pd.notnull(df_fxrate)), None)

    return jsonify( df_fxrate.to_dict(orient='records') )

@bp.route('/reports/ib/eod/transactions', methods=['GET'])
def ib_eod_transactions():
    df_transactions = ib_eod_data_transactions()

    headers_datetime = ['Date/Heure']
    for field_date in headers_datetime:
        df_transactions[field_date] = df_transactions[field_date].apply(patch_missing_datetime)

    headers_date = ['Expiration']
    for field_date in headers_date:
        df_transactions[field_date] = df_transactions[field_date].apply(patch_missing_date)

    df_transactions = df_transactions.where((pd.notnull(df_transactions)), None)

    return jsonify( df_transactions.to_dict(orient='records') )


@bp.route('/reports/ib/eod/positions', methods=['GET'])
def ib_eod_positions():
    df_openPositions = ib_eod_data_positions()

    headers_date = ['Expiration']
    for field_date in headers_date:
        df_openPositions[field_date] = df_openPositions[field_date].apply(patch_missing_date)

    df_openPositions = df_openPositions.where((pd.notnull(df_openPositions)), None)

    return jsonify( df_openPositions.to_dict(orient='records') )


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
def ib_eod():

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

    if os.path.isfile(os.path.join(SCRIPT_ROOT + '/data/', IB_FILE_LAST)):
        pass
    else:
        current_app.logger.warning('missing file')
        return jsonify( {'error': 'missing file'} )


    with open(os.path.join(SCRIPT_ROOT + '/data/', IB_FILE_LAST), 'r', encoding='utf-8-sig') as f:
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


        # statement pour date report <-> pour pouvoir ajuster l intraday
        header = "Statement"
        df_statement = pd.DataFrame(
                        data=data[header][1:],
                        columns=data[header][0])

        df_statement = df_statement[ df_statement['Header'] == 'Data' ]
        date_str = df_statement[ df_statement['Nom champ'] == 'WhenGenerated' ]['Valeur champ'].values[0][:10]



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
        df_refData = pd.concat(list_refData, axis=0, ignore_index=True, sort=False) # pandas-0.23
        #df_refData = pd.concat(list_refData, axis=0, ignore_index=True)

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
        for asset in list_refData:
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

        # mise a zero des ytd pnl
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
        # ' {"execution":{"m_acctNumber":"DU15197","m_orderId":2147483647,"m_evRule":null,"m_time":"20180806  09:48:00","m_permId":1726481144,"m_evMultiplier":0,"m_liquidation":0,"m_orderRef":null,"m_price":59.48,"m_avgPrice":59.48,"m_cumQty":100,"m_side":"BOT","m_clientId":0,"m_shares":100,"m_execId":"00004466.5b67c84a.01.01","m_exchange":"ISLAND"},"contract":{"m_tradingClass":"NMS","m_symbol":"IBKR","m_conId":43645865,"m_secType":"STK","m_includeExpired":false,"m_right":null,"m_multiplier":null,"m_expiry":null,"m_currency":"USD","m_localSymbol":"IBKR","m_exchange":"ISLAND","m_strike":0}}
        list_openPositions = df_openPositions.to_dict(orient='record')

        new_openPos = []
        input_data = request.get_json()

        #current_executions = json.loads( routes_executions.list_limit_date( '2018-08-01' ).get_data() )

        if input_data != None and 'execDetails' in input_data:
            for exec in input_data['execDetails']:
                already_in_dataset = False

                side = 1
                if exec['execution']['m_side'][:1].upper() == "B":
                    side = 1
                elif exec['execution']['m_side'][:1].upper() == "S":
                    side = -1

                if exec['contract']['m_multiplier'] != None and exec['contract']['m_multiplier'].lstrip('-').replace('.', '', 1).isdigit():
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
        df_openPositions['strategy'] = IB_FILE_LAST.split('_')[0]
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

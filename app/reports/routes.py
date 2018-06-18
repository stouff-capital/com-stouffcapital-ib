from datetime import datetime
from flask import render_template, flash, redirect, url_for, request, g, jsonify, current_app
from werkzeug.utils import secure_filename
import os
import csv
import pandas as pd
import numpy as np
from app import db
from app.models import Contract, Execution, Bbg
from app.reports import bp


SCRIPT_ROOT = os.path.dirname(os.path.abspath(__file__))
ALLOWED_EXTENSIONS = set(['csv'])

# utilities
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@bp.route('/reports', methods=['GET'])
def reports_list():
    return jsonify( {'status': 'ok'} )

@bp.route('/reports/ib/eod', methods=['POST'])
def ib_eod():

    #return jsonify( {'data': len(request.data() ) } )

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

        data = {}
        with open(os.path.join(SCRIPT_ROOT + '/data/', filename), 'r', encoding='utf-8-sig') as f:
            k = 0
            for row in f:
                for one_row in csv.reader([row]):
                    break

                if one_row[0] != '':
                    if one_row[0] in data:
                        pass
                    else:
                        data[ one_row[0] ] = []

                    data[ one_row[0] ].append(one_row)
                    k += 1

        list_headers = []
        for header in data:
            list_headers.append(header)


        headers_intraday = ["intraday_positionChg", "intraday_ntcf"]


        # Synthèse de la performance évaluée au prix du marché (open pos only) intraday ?
        header = 'Synthèse de la performance évaluée au prix du marché'

        # issue with 'Avant Prix'
        numeric_fields = ['Avant Quantité', 'Courant Quantité', 'Courant Prix', 'Pertes et profits au prix du marché Position', 'Pertes et profits au prix du marché Commissions', 'Pertes et profits au prix du marché Autre', 'Pertes et profits au prix du marché Total']

        df_lastDayPerf = pd.DataFrame(
            data=data[header][1:],
            columns=data[header][0])


        df_lastDayPerf = df_lastDayPerf[ df_lastDayPerf['Header'] == 'Data' ]

        for field in numeric_fields:
            df_lastDayPerf[field] = pd.to_numeric( df_lastDayPerf[field].str.replace(',', '') )

        headers_toShow = ['Catégorie d\'actifs', 'Symbole', 'Avant Quantité', 'Courant Quantité', 'Courant Prix', 'Pertes et profits au prix du marché Total']


        # Synthèse de la performance réalisée et non-réalisée (open pos only) intraday?
        header = 'Synthèse de la performance réalisée et non-réalisée'

        numeric_fields = ['Aj. coût', 'Realisé Profit C/T', 'Realisé Perte C/T', 'Realisé Profit L/T', 'Realisé Perte L/T', 'Realisé Total', 'Non-Réalisé Profit C/T', 'Non-Réalisé Perte C/T', 'Non-Réalisé Profit L/T', 'Non-Réalisé Perte L/T', 'Non-Réalisé Total', 'Total']

        df_pnl = pd.DataFrame(
            data=data[header][1:],
            columns=data[header][0])


        df_pnl = df_pnl[ df_pnl['Header'] == 'Data' ]

        for field in numeric_fields:
            df_pnl[field] = pd.to_numeric( df_pnl[field].str.replace(',', '') )

        headers_toShow = ['Catégorie d\'actifs', 'Symbole', 'Realisé Total', 'Non-Réalisé Total', 'Total']


        # Synthèse de la performance pour le mois et l'année en cours (everything)
        header = 'Synthèse de la performance pour le mois et l\'année en cours'

        numeric_fields = ['Évalué-au-Marché MJM', 'Évalué-au-Marché YTD', 'Réalisé C/T MJM', 'Réalisé C/T YTD', 'Realisé L/T MJM', 'Realisé L/T YTD']

        df_mthYtd_Pnl = pd.DataFrame(
            data=data[header][1:],
            columns=data[header][0])


        df_mthYtd_Pnl = df_mthYtd_Pnl[ df_mthYtd_Pnl['Header'] == 'Data' ]
        df_mthYtd_Pnl = df_mthYtd_Pnl[ df_mthYtd_Pnl['Catégorie d\'actifs'] != 'Total' ]
        df_mthYtd_Pnl = df_mthYtd_Pnl[ df_mthYtd_Pnl['Catégorie d\'actifs'] != 'Total (Tous les actifs)' ]

        for field in numeric_fields:
            df_mthYtd_Pnl[field] = pd.to_numeric( df_mthYtd_Pnl[field].str.replace(',', '') )

        # inject intraday
        for intraday_field in headers_intraday:
            df_mthYtd_Pnl[intraday_field] = 0.0

        headers_pnl = ['Catégorie d\'actifs', 'Symbole', 'Évalué-au-Marché YTD', 'Réalisé C/T YTD', 'Realisé L/T YTD' ] + headers_intraday
        headers_pnl = ['Catégorie d\'actifs', 'Symbole', 'Évalué-au-Marché YTD' ]
        headers_toShow = headers_pnl


        # positions ouvertes
        header = 'Positions ouvertes'

        numeric_fields = ['Quantité', 'Mult', 'Prix d\'origine', 'Valeur d\'Origine', 'Prix de Fermeture', 'Valeur', 'P/L Non-Réalisé', 'P&L non réalisé %']

        df_openPositions = pd.DataFrame(
            data=data[header][1:],
            columns=data[header][0])


        df_openPositions = df_openPositions[ df_openPositions['Header'] == 'Data' ]

        for field in numeric_fields:
            df_openPositions[field] = pd.to_numeric( df_openPositions[field].str.replace(',', '') )

        # inject intraday
        for intraday_field in headers_intraday:
            df_openPositions[intraday_field] = 0.0

        headers_open = ['Catégorie d\'actifs', 'Devise', 'Symbole', 'Quantité', 'Mult', 'Prix de Fermeture', 'Valeur', 'P/L Non-Réalisé'] + headers_intraday
        headers_toShow = headers_open


        # Transcations
        header = 'Transactions'

        numeric_fields = ['Quantité', 'Prix Trans.', 'Prix Ferm.', 'Produits', 'Comm/Tarif', 'Base', 'P/L Réalisé', '% P&L réalisé', 'P/L MTM']

        df_transactions = pd.DataFrame(
            data=data[header][1:],
            columns=data[header][0])


        df_transactions = df_transactions[ df_transactions['Header'] == 'Data' ]

        for field in numeric_fields:
            df_transactions[field] = pd.to_numeric( df_transactions[field].str.replace(',', '') )

        headers_toShow = ['Catégorie d\'actifs', 'Devise', 'Symbole', 'Date/Heure', 'Quantité', 'Prix Trans.', 'Prix Ferm.', 'Produits', 'Comm/Tarif', 'Base', 'P/L Réalisé', 'P/L MTM', 'Code']


        datetime_fields = ['Date/Heure']
        for field in datetime_fields:
            df_transactions[field] = pd.to_datetime( df_transactions[field], format="%Y-%m-%d, %H:%M:%S" )


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
            refData[ asset['Description'] ] = asset # for options


        list_openPositions = df_openPositions.to_dict(orient='record')

        for pos in list_openPositions:
            if pos['Symbole'] in refData:

                #merged content
                #for refData_field in refData[ pos['Symbole'] ]:
                for refData_field in headers_refData:
                    if refData_field not in pos:
                        pos[ refData_field ] = refData[ pos['Symbole'] ] [ refData_field ]

            else:
                print('missing ', pos['Symbole'])

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


        headers_intraday = ["intraday_positionChg", "intraday_ntcf"]


        #df_openPositions[ list(set(headers_pnl + headers_refData + headers_open + headers_intraday)) ].to_csv(os.path.join(SCRIPT_ROOT + '/data/', 'out.csv'), index=False, encoding='utf-8-sig')

        # export datamodel fa
        df_openPositions['Identifier'] = np.nan
        df_openPositions['bbg_underyling_id'] = np.nan
        df_openPositions['Underlying'] = np.nan
        df_openPositions['bbg_ticker'] = df_openPositions['Symbole']
        df_openPositions['provider'] = "IB"
        df_openPositions['strategy'] = filename.split('_')[0]
        df_openPositions['CUSTOM_accpbpid'] = df_openPositions[['strategy', 'provider', 'Symbole']].apply(lambda x: '_'.join(x), axis=1)
        df_openPositions['position_current'] = df_openPositions['Quantité'] + df_openPositions['intraday_positionChg']
        df_openPositions['pnl_d_local'] = 0
        df_openPositions['pnl_y_local'] = 0
        df_openPositions['pnl_y_eod_local'] = df_openPositions['Évalué-au-Marché YTD']
        df_openPositions['position_eod'] = df_openPositions['Quantité']
        df_openPositions['price_eod'] = df_openPositions['Prix de Fermeture']
        df_openPositions['ntcf_d_local'] = df_openPositions['intraday_ntcf']




        df = df_openPositions.where((pd.notnull(df_openPositions)), None)

        col_to_export = ['Identifier', 'bbg_underyling_id', 'Underlying', 'bbg_ticker', 'provider', 'strategy', 'CUSTOM_accpbpid', 'position_current', 'pnl_d_local', 'pnl_y_local', 'pnl_y_eod_local', 'position_eod', 'price_eod', 'ntcf_d_local', 'Symbole', 'Description']

        return jsonify( df[0:5][col_to_export].to_dict(orient='records') )
        return jsonify( {'status': 'ok', 'file': filename, 'path': os.path.join(SCRIPT_ROOT + '/data/', filename), 'nbrehHaders': len(data), 'headers': list_headers   } )

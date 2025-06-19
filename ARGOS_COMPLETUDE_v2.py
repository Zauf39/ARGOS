import os
import json
from datetime import datetime, timedelta
import pandas as pd
import re
from openpyxl import load_workbook
import streamlit as st
import tempfile
import sys
import subprocess

def flatten_json(y):
    out = {}
    def flatten(x, name=''):
        if isinstance(x, dict):
            for key in x:
                flatten(x[key], f'{name}{key}_')
        elif isinstance(x, list):
            for i, item in enumerate(x):
                flatten(item, f'{name}{i}_')
        else:
            out[name[:-1]] = x
    flatten(y)
    return out

def extraire_chiffres(val):
    if pd.isna(val):
        return ""
    chiffres = re.findall(r'\d+', str(val))
    return ''.join(chiffres) if chiffres else ""

def normaliser_indice(val):
    if pd.isna(val):
        return ""
    return str(val)[:6]

def convertir_datetime(val):
    if pd.isna(val):
        return val
    match = re.match(r'^[A-Za-z]{3} ([A-Za-z]{3} \d{1,2} \d{2}:\d{2}:\d{2} \d{4})', str(val))
    if match:
        try:
            dt = datetime.strptime(match.group(1), "%b %d %H:%M:%S %Y")
            return dt.strftime("%d/%m/%Y %H:%M")
        except Exception:
            return val
    return val

def controle_lambda_indice(df_params, df_hors_normes):
    anomalies = []
    for _, row in df_params.iterrows():
        lambda_val = str(row.get('Lambda', '')).lower().replace('nm', '').replace(' ', '').replace('\xa0', '')
        indice_val = str(row.get('Indice de Réfraction', '')).replace(',', '.').strip()
        try:
            indice_float = round(float(indice_val), 4)
        except Exception:
            indice_float = None
        if lambda_val == "1310":
            if indice_float is None or abs(indice_float - 1.4675) > 0.0001:
                anomalies.append({
                    'Fichier': row.get('Fichier', ''),
                    'MétaNommage': row.get('MétaNommage', ''),
                    'Indice de Réfraction': row.get('Indice de Réfraction', ''),
                    'Impulsion': row.get('Impulsion', ''),
                    'Lambda': row.get('Lambda', ''),
                    'Anomalie': "Indice de Réfraction NOK"
                })
        if lambda_val == "1550":
            if indice_float is None or abs(indice_float - 1.4680) > 0.0001:
                anomalies.append({
                    'Fichier': row.get('Fichier', ''),
                    'MétaNommage': row.get('MétaNommage', ''),
                    'Indice de Réfraction': row.get('Indice de Réfraction', ''),
                    'Impulsion': row.get('Impulsion', ''),
                    'Lambda': row.get('Lambda', ''),
                    'Anomalie': "Indice de Réfraction NOK"
                })
    if anomalies:
        df_anomalies = pd.DataFrame(anomalies)
        colonnes_hn = list(df_hors_normes.columns)
        for col in df_anomalies.columns:
            if col not in colonnes_hn:
                df_hors_normes[col] = ""
        df_hors_normes = pd.concat([df_hors_normes, df_anomalies], ignore_index=True)
    return df_hors_normes

def controle_longueur_fibres(df_params, df_hors_normes, tolerance_m=30):
    anomalies = []
    tolerance_km = tolerance_m / 1000.0
    if 'Distance Totale(km)' in df_params.columns:
        df_params['Distance Totale(km)'] = pd.to_numeric(df_params['Distance Totale(km)'], errors='coerce')
    if 'cable ID' in df_params.columns:
        df_params['cable ID'] = df_params['cable ID'].astype(str).str.strip()
    for cable_id, group in df_params.groupby('cable ID'):
        if pd.isna(cable_id) or cable_id == '' or group['Distance Totale(km)'].isnull().all():
            continue
        dists = group['Distance Totale(km)'].dropna().astype(float)
        if len(dists) < 2:
            continue
        min_dist = dists.min()
        max_dist = dists.max()
        if (max_dist - min_dist) > tolerance_km:
            for idx, row in group.iterrows():
                anomalies.append({
                    'Fichier': row.get('Fichier', ''),
                    'MétaNommage': row.get('MétaNommage', ''),
                    'Indice de Réfraction': row.get('Indice de Réfraction', ''),
                    'Impulsion': row.get('Impulsion', ''),
                    'Lambda': row.get('Lambda', ''),
                    'cable ID': row.get('cable ID', ''),
                    'Distance Totale(km)': row.get('Distance Totale(km)', ''),
                    'Anomalie': "Longueurs d'une même fibres NOK"
                })
    if anomalies:
        df_anomalies = pd.DataFrame(anomalies)
        colonnes_hn = list(df_hors_normes.columns)
        for col in df_anomalies.columns:
            if col not in colonnes_hn:
                df_hors_normes[col] = ""
        df_hors_normes = pd.concat([df_hors_normes, df_anomalies], ignore_index=True)
    return df_hors_normes

def controle_parametres(df_params, df_hors_normes, indice_ref, impulsion_ref):
    lignes_anomalies = []
    for _, row in df_params.iterrows():
        val_indice = normaliser_indice(row.get('Indice de Réfraction', ''))
        if val_indice != normaliser_indice(indice_ref):
            lignes_anomalies.append({
                'Fichier': row.get('Fichier', ''),
                'MétaNommage': row.get('MétaNommage', ''),
                'Indice de Réfraction': row.get('Indice de Réfraction', ''),
                'Impulsion': row.get('Impulsion', ''),
                'Anomalie': "Indice de Réfraction NOK"
            })
        val_impulsion = extraire_chiffres(row.get('Impulsion', ''))
        if val_impulsion != extraire_chiffres(impulsion_ref):
            lignes_anomalies.append({
                'Fichier': row.get('Fichier', ''),
                'MétaNommage': row.get('MétaNommage', ''),
                'Indice de Réfraction': row.get('Indice de Réfraction', ''),
                'Impulsion': row.get('Impulsion', ''),
                'Anomalie': "Impulsion NOK"
            })
    if lignes_anomalies:
        df_anomalies = pd.DataFrame(lignes_anomalies)
        colonnes_hn = list(df_hors_normes.columns)
        for col in df_anomalies.columns:
            if col not in colonnes_hn:
                df_hors_normes[col] = ""
        df_hors_normes = pd.concat([df_hors_normes, df_anomalies], ignore_index=True)
        st.info(f"{len(lignes_anomalies)} anomalies paramètre ajoutées dans 'Hors Normes'.")
    return df_hors_normes

def analyse_temps_mesures(df_params, df_hors_normes):
    if 'date/time' in df_params.columns:
        df_params['date/time'] = pd.to_datetime(df_params['date/time'], errors='coerce', dayfirst=True)
    def nom_base(nom):
        return re.sub(r'(_\d+)?\.sor$', '.sor', str(nom), flags=re.IGNORECASE)
    df_params['NomBase'] = df_params['Fichier'].apply(nom_base)
    anomalies_temps = []
    for nom, group in df_params.groupby('NomBase'):
        group_sorted = group.sort_values('date/time')
        times = group_sorted['date/time'].tolist()
        fichiers = group_sorted['Fichier'].tolist()
        metas = group_sorted['MétaNommage'].tolist() if 'MétaNommage' in group_sorted.columns else fichiers
        lambdas = group_sorted['Lambda'].tolist() if 'Lambda' in group_sorted.columns else [None]*len(group_sorted)
        for i in range(1, len(times)):
            lambda_i = str(lambdas[i]).lower().replace('nm', '').replace(' ', '').replace('\xa0', '')
            lambda_i_1 = str(lambdas[i-1]).lower().replace('nm', '').replace(' ', '').replace('\xa0', '')
            if (
                pd.notnull(times[i]) and
                pd.notnull(times[i-1]) and
                lambda_i == lambda_i_1
            ):
                delta = (times[i] - times[i-1])
                if delta < timedelta(minutes=1, seconds=30):
                    for idx in [i-1, i]:
                        anomalies_temps.append({
                            'Fichier': fichiers[idx],
                            'MétaNommage': metas[idx],
                            'Indice de Réfraction': group_sorted.iloc[idx].get('Indice de Réfraction', ''),
                            'Impulsion': group_sorted.iloc[idx].get('Impulsion', ''),
                            'Lambda': group_sorted.iloc[idx].get('Lambda', ''),
                            'Anomalie': "Temps de mesures <1min 30"
                        })
    if anomalies_temps:
        df_anomalies_temps = pd.DataFrame(anomalies_temps)
        colonnes_hn = list(df_hors_normes.columns)
        for col in df_anomalies_temps.columns:
            if col not in colonnes_hn:
                df_hors_normes[col] = ""
        df_hors_normes = pd.concat([df_hors_normes, df_anomalies_temps], ignore_index=True)
    return df_hors_normes

def analyser_doublons_courbes(df_params, df_hors_normes):
    def nom_base(nom):
        return re.sub(r'(_\d+)?\.sor$', '.sor', str(nom), flags=re.IGNORECASE)
    if 'NomBase' not in df_params.columns:
        df_params['NomBase'] = df_params['Fichier'].apply(nom_base)
    anomalies_doublons = []
    for nom_base_group, group in df_params.groupby('NomBase'):
        if len(group) > 1:
            fichiers_traites = set()
            for i, row1 in group.iterrows():
                for j, row2 in group.iterrows():
                    lambda1 = str(row1.get('Lambda', '')).lower().replace('nm', '').replace(' ', '').replace('\xa0', '')
                    lambda2 = str(row2.get('Lambda', '')).lower().replace('nm', '').replace(' ', '').replace('\xa0', '')
                    if (
                        i < j and
                        row1['date/time'] == row2['date/time'] and
                        pd.notnull(row1['date/time']) and
                        row1['Fichier'] not in fichiers_traites and
                        row2['Fichier'] not in fichiers_traites and
                        lambda1 == lambda2
                    ):
                        for _, row in [(i, row1), (j, row2)]:
                            anomalies_doublons.append({
                                'Fichier': row['Fichier'],
                                'MétaNommage': row.get('MétaNommage', ''),
                                'Indice de Réfraction': row.get('Indice de Réfraction', ''),
                                'Impulsion': row.get('Impulsion', ''),
                                'Lambda': row.get('Lambda', ''),
                                'date/time': row.get('date/time', ''),
                                'Anomalie': "Courbes en doublons"
                            })
                        fichiers_traites.add(row1['Fichier'])
                        fichiers_traites.add(row2['Fichier'])
    if anomalies_doublons:
        df_anomalies_doublons = pd.DataFrame(anomalies_doublons)
        colonnes_hn = list(df_hors_normes.columns)
        for col in df_anomalies_doublons.columns:
            if col not in colonnes_hn:
                df_hors_normes[col] = ""
        df_hors_normes = pd.concat([df_hors_normes, df_anomalies_doublons], ignore_index=True)
        st.info(f"{len(anomalies_doublons)} courbes en doublons détectées.")
    return df_hors_normes

def analyser_nommage_courbes(df_params, df_hors_normes):
    anomalies_nommage = []
    for _, row in df_params.iterrows():
        nom_fichier = os.path.splitext(row['Fichier'])[0]
        metanommage = str(row.get('MétaNommage', ''))
        nom_metanommage = os.path.splitext(metanommage)[0] if metanommage else ''
        if nom_fichier.lower() != nom_metanommage.lower() and nom_metanommage != '':
            anomalies_nommage.append({
                'Fichier': row['Fichier'],
                'MétaNommage': row.get('MétaNommage', ''),
                'Indice de Réfraction': row.get('Indice de Réfraction', ''),
                'Impulsion': row.get('Impulsion', ''),
                'Anomalie': "Nommage courbes incorrect"
            })
    if anomalies_nommage:
        df_anomalies_nommage = pd.DataFrame(anomalies_nommage)
        colonnes_hn = list(df_hors_normes.columns)
        for col in df_anomalies_nommage.columns:
            if col not in colonnes_hn:
                df_hors_normes[col] = ""
        df_hors_normes = pd.concat([df_hors_normes, df_anomalies_nommage], ignore_index=True)
        st.info(f"{len(anomalies_nommage)} erreurs de nommage détectées.")
    return df_hors_normes

def traitement_otdr(indice_ref, impulsion_ref, sor_files):
    progress_bar = st.progress(0)
    status_text = st.empty()
    try:
        if not sor_files:
            st.error("Aucun fichier sélectionné.")
            return

        with tempfile.TemporaryDirectory() as temp_dir:
            nb_fichiers = len(sor_files)
            total_steps = nb_fichiers + 8
            step = 0

            sor_file_paths = []
            for uploaded_file in sor_files:
                file_path = os.path.join(temp_dir, uploaded_file.name)
                with open(file_path, "wb") as f:
                    f.write(uploaded_file.getbuffer())
                sor_file_paths.append(file_path)

            status_text.info("Conversion des fichiers .sor...")
            flags = 0
            if sys.platform == "win32":
                flags = 0x08000000  # subprocess.CREATE_NO_WINDOW

            for i, sor_file in enumerate(sor_file_paths):
                sor_filename = os.path.basename(sor_file)
                status_text.info(f"Conversion : {sor_filename}")
                try:
                    subprocess.run(['pyotdr', sor_filename], cwd=temp_dir, check=True, creationflags=flags)
                except Exception as e:
                    st.error(f"❌ Erreur sur {sor_filename} : {e}")
                step += 1
                progress_bar.progress(step / total_steps)

            status_text.info("Analyse des fichiers ...")
            json_files = [f for f in os.listdir(temp_dir) if f.lower().endswith('.json')]
            all_params = []
            all_events = []

            colonnes_a_supprimer_params = [
                'BC', 'EOT thr', 'X1', 'X2', 'Y1', 'Y2',
                'acquisition offset', 'acquisition offset distance',
                'averaging time', 'front panel offset', 'loss thr',
                'noise floor level', 'num averages', 'num data points',
                'number of pulse width entries', 'power offset first point',
                'refl thr', 'resolution', 'sample spacing', 'trace type',
                'unit', 'build condition', 'cable code/fiber type',
                'fiber type', 'language', 'user offset', 'user offset distance',
                'noise floor scaling factor','acquisition range distance', 'OTDR S/N'
            ]

            colonnes_a_supprimer_events = [
                'comments', 'end of curr', 'end of prev', 'peak', 'start of curr', 'start of next', 'Type de ROP'
            ]

            def convertir_datetime(val):
                if pd.isna(val):
                    return val
                match = re.match(r'^[A-Za-z]{3} ([A-Za-z]{3} \d{1,2} \d{2}:\d{2}:\d{2} \d{4})', str(val))
                if match:
                    try:
                        dt = datetime.strptime(match.group(1), "%b %d %H:%M:%S %Y")
                        return dt.strftime("%d/%m/%Y %H:%M:%S")
                    except Exception:
                        return val
                return val

            for filename in json_files:
                try:
                    filepath = os.path.join(temp_dir, filename)
                    with open(filepath, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    fichier_sor = filename.replace('-dump.json', '.sor')
                    nom_sor = data.get('filename', fichier_sor)
                    fxd_params = data.get('FxdParams', {})
                    gen_params = data.get('GenParams', {})
                    sup_params = data.get('SupParams', {})
                    key_events_summary = data.get('KeyEvents', {}).get('Summary', {})
                    params = {
                        'Fichier': fichier_sor,
                        'filename': nom_sor,
                        'loss end': key_events_summary.get('loss end', None),
                        **fxd_params,
                        **gen_params,
                        **sup_params
                    }
                    all_params.append(params)
                    key_events = data.get('KeyEvents', {})
                    for key in key_events:
                        if key.lower().startswith('event '):
                            event = key_events[key]
                            event_copy = event.copy()
                            event_copy['Fichier'] = fichier_sor
                            event_copy['filename'] = nom_sor
                            event_copy['Event ID'] = key.split()[1]
                            all_events.append(event_copy)
                except Exception as e:
                    st.error(f"Erreur avec {filename} : {e}")

            df_params = pd.DataFrame(all_params)
            df_params = df_params.drop(columns=colonnes_a_supprimer_params, errors='ignore')
            df_params = df_params.rename(columns={
                'filename': 'MétaNommage',
                'index': 'Indice de Réfraction',
                'pulse width': 'Impulsion',
                'range': 'Portée(km)',
                'comments': 'Commentaire',
                'operator': 'Technicien',
                'software': 'Version',
                'supplier': 'Fabricant',
                'acquisition range distance': 'Portée(km)',
                'wavelength': 'Lambda',
                'loss end': 'Distance Totale(km)'
            }, errors='ignore')

            if 'Indice de Réfraction' in df_params.columns:
                df_params['Indice de Réfraction'] = df_params['Indice de Réfraction'].astype(str).apply(lambda x: x[:6])
            if 'Distance Totale(km)' in df_params.columns:
                df_params['Distance Totale(km)'] = pd.to_numeric(df_params['Distance Totale(km)'], errors='coerce').round(3)
            if 'date/time' in df_params.columns:
                df_params['date/time'] = df_params['date/time'].apply(convertir_datetime)
            if 'Portée(km)' in df_params.columns:
                df_params['Portée(km)'] = pd.to_numeric(df_params['Portée(km)'], errors='coerce').round(0).astype('Int64')

            df_events = pd.DataFrame(all_events)
            df_events = df_events.drop(columns=colonnes_a_supprimer_events, errors='ignore')
            df_events = df_events.rename(columns={
                'filename': 'MétaNommage',
                'Event ID': 'N° évenement',
                'refl loss': 'Réfléctance',
                'distance': 'Distance',
                'slope': 'Pente',
                'splice loss': 'Atténuation(dB)',
                'type': "Type d'évenements"
            }, errors='ignore')

            remplacement_types = {
                r'0F9999.*': 'Epissure',
                r'1E9999.*': 'Fin de fibre',
                r'1F9999.*': 'Connecteur',
                r'2E9999.*': 'Fin de fibre',
                r'0A9999LS.*': 'Epissure',
                r'1A9999LS.*': 'Connecteur',
                r'0O99992P.*': 'Epissure',
                r'1A9999OO.*': 'Connecteur',
                r'0A9999OO.*': 'Epissure',
                r'0O9999LS.*': 'Epissure'
            }
            if "Type d'évenements" in df_events.columns:
                df_events["Type d'évenements"] = df_events["Type d'évenements"].replace(remplacement_types, regex=True)
            if "Type de ROP" in df_events.columns:
                df_events = df_events.drop(columns=["Type de ROP"])
            if not df_events.empty:
                cols = ['Fichier', 'MétaNommage', 'N° évenement'] + [
                    col for col in df_events.columns if col not in ['Fichier', 'MétaNommage', 'N° évenement']
                ]
                df_events = df_events[cols]
            if "Type d'évenements" in df_events.columns and "Distance" in df_events.columns:
                last_fin_de_fibre = (
                    df_events[df_events["Type d'évenements"] == "Fin de fibre"]
                    .groupby('Fichier')['Distance']
                    .last()
                    .reset_index()
                    .rename(columns={'Distance': 'Distance Totale(km)_new'})
                )
                df_params = df_params.merge(last_fin_de_fibre, on='Fichier', how='left')
                df_params['Distance Totale(km)'] = df_params['Distance Totale(km)_new']
                df_params = df_params.drop(columns=['Distance Totale(km)_new'])

            df_hors_normes = df_events[
                (df_events["Type d'évenements"] == "Epissure") &
                (pd.to_numeric(df_events["Atténuation(dB)"], errors='coerce') >= 0.3)
            ].copy()
            df_hors_normes['Anomalie'] = (
                ((df_hors_normes["Type d'évenements"] == "Epissure") &
                (pd.to_numeric(df_hors_normes["Atténuation(dB)"], errors='coerce') >= 0.3))
                .map({True: "Epissure NOK", False: ""})
            )

            step += 1
            progress_bar.progress(step / total_steps)
            status_text.info("Contrôle Lambda/Indice de Réfraction...")
            df_hors_normes = controle_lambda_indice(df_params, df_hors_normes)
            step += 1
            progress_bar.progress(step / total_steps)
            status_text.info("Contrôle longueurs fibres (même boîte)...")
            df_hors_normes = controle_longueur_fibres(df_params, df_hors_normes, tolerance_m=15)
            step += 1
            progress_bar.progress(step / total_steps)
            status_text.info("Contrôle des autres paramètres...")
            df_hors_normes = controle_parametres(df_params, df_hors_normes, "1.4675", "30")
            step += 1
            progress_bar.progress(step / total_steps)
            status_text.info("Analyse temporelle des fichiers...")
            df_hors_normes = analyse_temps_mesures(df_params, df_hors_normes)
            step += 1
            progress_bar.progress(step / total_steps)
            status_text.info("Analyse des courbes en doublons...")
            df_hors_normes = analyser_doublons_courbes(df_params, df_hors_normes)
            step += 1
            progress_bar.progress(step / total_steps)
            status_text.info("Vérification du nommage des courbes...")
            df_hors_normes = analyser_nommage_courbes(df_params, df_hors_normes)
            step += 1
            progress_bar.progress(step / total_steps)
            status_text.info("Export du rapport Excel...")

            excel_output_path = os.path.join(temp_dir, 'rapport_otdr_final.xlsx')
            with pd.ExcelWriter(excel_output_path, engine='openpyxl') as writer:
                df_params.to_excel(writer, sheet_name='Parametres OTDR', index=False)
                df_events.to_excel(writer, sheet_name='Evenements', index=False)
                df_hors_normes.to_excel(writer, sheet_name='Hors Normes', index=False)
                wb = load_workbook(excel_output_path)
                for ws in wb.worksheets:
                    for column_cells in ws.columns:
                        length = max(len(str(cell.value)) if cell.value is not None else 0 for cell in column_cells)
                        ws.column_dimensions[column_cells[0].column_letter].width = length + 2
                wb.save(excel_output_path)
            step += 1
            progress_bar.progress(step / total_steps)
            status_text.success("Traitement terminé !")

            with open(excel_output_path, "rb") as f:
                st.download_button("Télécharger le rapport Excel", f, file_name="rapport_otdr_final.xlsx")
    except Exception as e:
        st.error(f"Erreur inattendue : {e}")

def main():
    st.title("Analyse OTDR - Version Streamlit")
    st.write("Déposez vos fichiers .sor pour lancer l'analyse.")
    sor_files = st.file_uploader("Sélectionner les fichiers .sor", accept_multiple_files=True, type="sor")
    if sor_files and st.button("Lancer l'analyse"):
        traitement_otdr("1.4675", "30", sor_files)

if __name__ == "__main__":
    main()

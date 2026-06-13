# ============================================================
# dag_pipeline_mobile_money.py
# Auteur      : Sylla Bazoumana
# Description : Pipeline ETL quotidien Mobile Money CI
# Schedule    : Tous les jours à 6h00
# ============================================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash   import BashOperator
from datetime import datetime, timedelta
import logging
import pandas as pd
import numpy as np

# ============================================================
# CONFIGURATION PAR DÉFAUT
# ============================================================
default_args = {
    'owner'             : 'Sylla_Bazoumana',
    'depends_on_past'   : False,
    'start_date'        : datetime(2024, 1, 1),
    'email'             : ['ton.email@gmail.com'],
    'email_on_failure'  : True,    # Alerte email si échec
    'email_on_retry'    : False,
    'retries'           : 3,       # 3 tentatives avant échec définitif
    'retry_delay'       : timedelta(minutes=5),
    'execution_timeout' : timedelta(hours=2),  # Tuer si dépasse 2h
}

# ============================================================
# TÂCHE 1 — EXTRACTION
# Lire le CSV brut et sauvegarder en Parquet
# Parquet = format colonne plus rapide que CSV pour l'analyse
# ============================================================
def task_extract(**context):
    log = logging.getLogger(__name__)
    log.info('EXTRACT -- Démarrage extraction CSV')

    # Charger le fichier CSV brut
    df = pd.read_csv('/data/transactions_mobile_money_100k.csv',
                     encoding='utf-8', low_memory=False)
    nb = len(df)
    log.info(f'EXTRACT -- {nb:,} lignes extraites')

    # Pousser le nombre de lignes vers XCom
    # XCom = mécanisme de communication entre tâches Airflow
    context['ti'].xcom_push(key='nb_lignes_brutes', value=nb)

    # Sauvegarder en Parquet pour la tâche suivante
    # Parquet est plus rapide à lire que CSV pour les grandes tables
    df.to_parquet('/data/raw/transactions_raw.parquet', index=False)

    log.info('EXTRACT -- Fichier Parquet sauvegardé')
    return nb

# ============================================================
# TÂCHE 2 — NETTOYAGE ET ENRICHISSEMENT
# Nettoyer les données et créer les colonnes calculées
# ============================================================
def task_clean(**context):
    log = logging.getLogger(__name__)
    log.info('CLEAN -- Démarrage nettoyage')

    # Lire le fichier brut sauvegardé par la tâche 1
    df = pd.read_parquet('/data/raw/transactions_raw.parquet')

    # --- Nettoyage ---
    df = df.replace('', np.nan)
    df['frais_fcfa']         = df['frais_fcfa'].fillna(0).astype(int)
    df['zone_beneficiaire']  = df['zone_beneficiaire'].fillna('Inconnu')
    df['id_agent']           = df['id_agent'].fillna('SANS_AGENT')
    df = df[df['montant_fcfa'] > 0].copy()   # Supprimer montants négatifs
    df['date_heure']         = pd.to_datetime(df['date_heure'])

    # --- Enrichissement ---
    df['heure']            = df['date_heure'].dt.hour
    df['mois']             = df['date_heure'].dt.month
    df['est_weekend']      = df['date_heure'].dt.dayofweek >= 5
    df['montant_net_fcfa'] = df['montant_fcfa'] - df['frais_fcfa']
    df['taux_frais_pct']   = (df['frais_fcfa'] / df['montant_fcfa'] * 100).round(2)
    df['inter_ville']      = (df['zone_expediteur'] != df['zone_beneficiaire']).astype(int)

    nb = len(df)
    log.info(f'CLEAN -- {nb:,} lignes propres, {df.shape[1]} colonnes')

    # Communiquer le résultat à la tâche validation
    context['ti'].xcom_push(key='nb_lignes_propres', value=nb)

    # Sauvegarder les données propres
    df.to_parquet('/data/clean/transactions_clean.parquet', index=False)

    return nb

# ============================================================
# TÂCHE 3 — CHARGEMENT SUPABASE
# Envoyer les données propres dans PostgreSQL
# ============================================================
def task_load(**context):
    import sqlalchemy, os
    log = logging.getLogger(__name__)

    # Lire l'URL depuis les variables d'environnement
    # Plus sécurisé que d'écrire le mot de passe dans le code
    SUPABASE_URL = os.environ.get('SUPABASE_URL', '')
    if not SUPABASE_URL:
        raise ValueError('SUPABASE_URL non définie — vérifier les variables Airflow')

    engine = sqlalchemy.create_engine(SUPABASE_URL)
    df     = pd.read_parquet('/data/clean/transactions_clean.parquet')

    # Chargement par lots de 5000 lignes
    chunks = [df[i:i+5000] for i in range(0, len(df), 5000)]

    with engine.connect() as conn:
        for chunk in chunks:
            chunk.to_sql(
                'transactions_mobile_money', conn,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=500
            )
        conn.commit()   # Valider toutes les insertions

    engine.dispose()
    log.info(f'LOAD -- {len(df):,} lignes chargées dans Supabase')

# ============================================================
# TÂCHE 4 — RAPPORT JSON AUTOMATIQUE
# Générer un rapport de qualité après chaque exécution
# ============================================================
def task_report(**context):
    import json
    from datetime import datetime as dt
    log = logging.getLogger(__name__)

    df   = pd.read_parquet('/data/clean/transactions_clean.parquet')
    df_s = df[df['statut'] == 'Succès']

    # Récupérer les métriques des tâches précédentes via XCom
    ti         = context['ti']
    nb_brutes  = ti.xcom_pull(task_ids='extraction',      key='nb_lignes_brutes')
    nb_propres = ti.xcom_pull(task_ids='transformation',  key='nb_lignes_propres')

    # Construire le rapport
    rapport = {
        'date_generation'    : dt.now().strftime('%Y-%m-%d %H:%M'),
        'nb_lignes_brutes'   : nb_brutes,
        'nb_lignes_propres'  : nb_propres,
        'nb_anomalies'       : nb_brutes - nb_propres,
        'volume_total_fcfa'  : int(df_s['montant_fcfa'].sum()),
        'taux_succes_pct'    : round(len(df_s) / len(df) * 100, 1),
        'montant_moyen_fcfa' : int(df_s['montant_fcfa'].mean()),
    }

    # Sauvegarder le rapport en JSON
    with open('/data/output/rapport_quotidien.json', 'w') as f:
        json.dump(rapport, f, ensure_ascii=False, indent=2)

    log.info(f"RAPPORT -- Généré : {rapport}")
    return rapport

# ============================================================
# DÉFINITION DU DAG ET ORDRE D'EXÉCUTION
# ============================================================
with DAG(
    dag_id            = 'pipeline_mobile_money_ci',
    default_args      = default_args,
    description       = 'Pipeline ETL Mobile Money CI — quotidien 6h',
    schedule_interval = '0 6 * * *',   # Chaque jour à 6h du matin
    catchup           = False,
    max_active_runs   = 1,             # Une seule exécution à la fois
    tags              = ['etl', 'mobile-money', 'cote-divoire'],
) as dag:

    t1 = PythonOperator(task_id='extraction',     python_callable=task_extract)
    t2 = PythonOperator(task_id='transformation', python_callable=task_clean)
    t3 = PythonOperator(task_id='chargement',     python_callable=task_load)
    t4 = PythonOperator(task_id='rapport',        python_callable=task_report)

    # Ordre d'exécution : t1 → t2 → t3 → t4
    t1 >> t2 >> t3 >> t4

# ============================================================
# DAG AIRFLOW — Pipeline Mobile Money Côte d'Ivoire
# Ce fichier automatise l'exécution quotidienne du pipeline
# ============================================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

# ============================================================
# CONFIGURATION PAR DÉFAUT DU DAG
# Ces paramètres s'appliquent à toutes les tâches
# ============================================================
default_args = {
    'owner'            : 'Sylla_Bazoumana',   # Propriétaire du pipeline
    'depends_on_past'  : False,                # Ne pas attendre l'exécution précédente
    'start_date'       : datetime(2024, 1, 1), # Date de début du pipeline
    'email'            : ['ton.email@gmail.com'],  # Email pour les alertes
    'email_on_failure' : True,                 # Envoyer un email si échec
    'email_on_retry'   : False,                # Pas d'email à chaque retry
    'retries'          : 2,                    # Réessayer 2 fois si erreur
    'retry_delay'      : timedelta(minutes=5), # Attendre 5 min entre chaque essai
}

# ============================================================
# TÂCHE 1 — EXTRACTION
# Charger le fichier CSV brut
# ============================================================
def tache_extraction(**context):
    print("📥 TÂCHE 1 : Extraction des données...")

    # Charger le dataset Mobile Money
    df = pd.read_csv('transactions_mobile_money_100k.csv')

    print(f"✅ {len(df):,} lignes extraites du fichier CSV")
    print(f"   Colonnes : {list(df.columns)}")

    # Passer les données à la tâche suivante via XCom
    # XCom = système de communication entre tâches Airflow
    context['ti'].xcom_push(key='nb_lignes_brut', value=len(df))

    return f"{len(df)} lignes extraites"

# ============================================================
# TÂCHE 2 — TRANSFORMATION
# Nettoyer et enrichir les données
# ============================================================
def tache_transformation(**context):
    print("⚙️ TÂCHE 2 : Transformation des données...")

    # Charger et nettoyer
    df = pd.read_csv('transactions_mobile_money_100k.csv')
    df['date_heure'] = pd.to_datetime(df['date_heure'])

    # Supprimer les montants négatifs
    df = df[df['montant_fcfa'] > 0]

    # Remplir les valeurs manquantes
    df['frais_fcfa']          = df['frais_fcfa'].fillna(df['frais_fcfa'].median())
    df['zone_beneficiaire']   = df['zone_beneficiaire'].fillna('Inconnu')
    df['id_agent']            = df['id_agent'].fillna('SANS_AGENT')

    # Enrichissement — nouvelles colonnes
    df['heure']            = df['date_heure'].dt.hour
    df['est_weekend']      = df['date_heure'].dt.dayofweek >= 5
    df['mois']             = df['date_heure'].dt.month
    df['montant_net_fcfa'] = df['montant_fcfa'] - df['frais_fcfa']
    df['taux_frais_pct']   = (df['frais_fcfa'] / df['montant_fcfa'] * 100).round(2)
    df['inter_ville']      = (df['zone_expediteur'] != df['zone_beneficiaire']).astype(int)

    print(f"✅ {len(df):,} lignes après transformation")
    print(f"   Colonnes enrichies : {df.shape[1]}")

    # Communiquer le résultat à la tâche suivante
    context['ti'].xcom_push(key='nb_lignes_clean', value=len(df))

    return f"{len(df)} lignes transformées"

# ============================================================
# TÂCHE 3 — CHARGEMENT
# Envoyer les données dans Supabase
# ============================================================
def tache_chargement(**context):
    print("📤 TÂCHE 3 : Chargement dans Supabase...")

    from sqlalchemy import create_engine
    import os

    # Récupérer l'URL depuis les variables d'environnement
    # Plus sécurisé que d'écrire le mot de passe dans le code
    SUPABASE_URL = os.environ.get('SUPABASE_URL', 'postgresql://...')

    engine = create_engine(SUPABASE_URL)

    # Recharger les données transformées
    df = pd.read_csv('transactions_mobile_money_100k.csv')
    df['date_heure'] = pd.to_datetime(df['date_heure'])
    df = df[df['montant_fcfa'] > 0]

    # Chargement par lots
    df.to_sql(
        'transactions_mobile_money',
        engine,
        if_exists='replace',
        index=False,
        chunksize=5000   # Envoyer 5000 lignes à la fois
    )

    engine.dispose()   # Fermer la connexion proprement

    print(f"✅ {len(df):,} lignes chargées dans Supabase")

    return f"{len(df)} lignes chargées"

# ============================================================
# TÂCHE 4 — VALIDATION
# Vérifier que le chargement s'est bien passé
# ============================================================
def tache_validation(**context):
    print("✅ TÂCHE 4 : Validation de la qualité...")

    # Récupérer les stats des tâches précédentes via XCom
    nb_brut  = context['ti'].xcom_pull(key='nb_lignes_brut',  task_ids='extraction')
    nb_clean = context['ti'].xcom_pull(key='nb_lignes_clean', task_ids='transformation')

    # Calculer le taux de données conservées
    if nb_brut and nb_clean:
        taux_conservation = round(nb_clean / nb_brut * 100, 2)
        print(f"   Lignes brutes    : {nb_brut:,}")
        print(f"   Lignes propres   : {nb_clean:,}")
        print(f"   Taux conservation: {taux_conservation}%")

        # Alerte si on perd trop de données
        if taux_conservation < 90:
            raise ValueError(
                f"⚠️ ALERTE : Seulement {taux_conservation}% des données conservées !"
            )

    print("✅ Validation réussie — Pipeline terminé avec succès !")
    return "Validation OK"

# ============================================================
# DÉFINITION DU DAG
# ============================================================
with DAG(
    dag_id          = 'pipeline_mobile_money_ci',   # Nom unique du DAG
    default_args    = default_args,
    description     = 'Pipeline ETL automatisé — Mobile Money Côte d\'Ivoire',
    schedule_interval = '0 6 * * *',   # Chaque jour à 6h du matin (cron)
    catchup         = False,           # Ne pas rattraper les exécutions passées
    tags            = ['mobile-money', 'etl', 'cote-divoire'],  # Tags pour filtrer
) as dag:

    # Définir les 4 tâches
    t1 = PythonOperator(
        task_id         = 'extraction',
        python_callable = tache_extraction,
        provide_context = True,   # Donner accès au contexte Airflow (XCom)
    )

    t2 = PythonOperator(
        task_id         = 'transformation',
        python_callable = tache_transformation,
        provide_context = True,
    )

    t3 = PythonOperator(
        task_id         = 'chargement',
        python_callable = tache_chargement,
        provide_context = True,
    )

    t4 = PythonOperator(
        task_id         = 'validation',
        python_callable = tache_validation,
        provide_context = True,
    )

    # Définir l'ordre d'exécution
    # t1 doit finir avant t2, t2 avant t3, t3 avant t4
    t1 >> t2 >> t3 >> t4

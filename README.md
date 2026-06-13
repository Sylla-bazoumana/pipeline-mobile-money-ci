# 🇨🇮 Pipeline Mobile Money — Côte d'Ivoire

## 📋 Description
Pipeline ETL complet d'analyse des flux Mobile Money en Côte d'Ivoire.
Analyse de 100 000 transactions sur 4 opérateurs (MTN, Orange, Moov, Wave).

## 🛠️ Technologies utilisées
![Python](https://img.shields.io/badge/Python-3.10-blue)
![Pandas](https://img.shields.io/badge/Pandas-2.0-green)
![Supabase](https://img.shields.io/badge/Supabase-PostgreSQL-orange)
![Airflow](https://img.shields.io/badge/Airflow-2.7-red)
![Docker](https://img.shields.io/badge/Docker-blue)

## 📁 Structure du projet
pipeline-mobile-money-ci/
├── dags/
│   └── pipeline_mobile_money.py    ← DAG Airflow (4 tâches)
├── sql/
│   └── requetes_analytiques.sql    ← 5 requêtes SQL analytiques
├── Dockerfile                      ← Conteneurisation Python
├──  README.md                       ← Documentation du projet
├── dashboard_mobile_money.png      ← Tableau de bord (5 graphiques)
├── docker-compose.yml              ← 3 services (Airflow, PostgreSQL, Notebook)
└──  requirements.txt                ← Dépendances versionnées


## 👤 Auteur
Sylla Bazoumana — Promotion 2024-2025
```
## ✅ Avancement
- [x] Environnement setup (GitHub, Supabase, Colab)
- [x] Dataset chargé et audité (100 000 lignes)
- [x] Nettoyage complet (99 800 lignes propres)
- [x] Enrichissement (20 colonnes)
- [x] Tests qualité (5/5 passés)
- [x] Chargement Supabase réussi
- [x] Schéma en étoile créé (4 dimensions + 1 table de faits)
- [x] faits_transactions chargée (99 800 lignes)
- [x] 5 requêtes SQL analytiques (dont CTE + RANK)
- [x] Tableau de bord Matplotlib (5 graphiques, dpi=150)
- [x] DAG Airflow (4 tâches, logging, format Parquet)
- [x] Dockerfile + docker-compose.yml (3 services)

## 📊 Résultats clés
- **Volume total traité** : ~18 milliards FCFA sur 6 mois
- **Opérateur leader** : Wave (25.20% de parts de marché)
- **Zone la plus active** : Korhogo (10.35% du volume)
- **Meilleur taux de succès** : Wave (90.30%)

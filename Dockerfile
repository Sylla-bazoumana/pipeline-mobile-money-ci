# ── IMAGE DE BASE ────────────────────────────────────────
# Python 3.10 slim = image légère (~130 Mo vs 900 Mo pour la complète)
FROM python:3.10-slim

# ── MÉTADONNÉES ───────────────────────────────────────────
LABEL maintainer="ton.email@gmail.com"
LABEL description="Pipeline ETL Mobile Money Côte d'Ivoire"
LABEL version="1.0.0"

# ── VARIABLES D'ENVIRONNEMENT ─────────────────────────────
ENV PYTHONDONTWRITEBYTECODE=1
# Ne pas créer les fichiers .pyc (image plus propre)
ENV PYTHONUNBUFFERED=1
# Les logs Python sont affichés immédiatement
ENV ENVIRONMENT=production

# ── RÉPERTOIRE DE TRAVAIL ─────────────────────────────────
# Tous les chemins COPY et CMD seront relatifs à /app
WORKDIR /app

# ── INSTALLATION SYSTÈME ──────────────────────────────────
RUN apt-get update && apt-get install -y \
    build-essential \
    # Compilateur C nécessaire pour certaines libs Python
    libpq-dev \
    # Headers PostgreSQL requis par psycopg2
    && rm -rf /var/lib/apt/lists/*
    # Nettoyer le cache apt pour réduire la taille de l'image

# ── COPIER D'ABORD requirements.txt ──────────────────────
# Docker cache chaque étape — si requirements.txt ne change pas,
# cette étape n'est pas ré-exécutée = build beaucoup plus rapide
COPY requirements.txt .

# ── INSTALLER LES DÉPENDANCES PYTHON ─────────────────────
RUN pip install --no-cache-dir --upgrade pip && \
    # Mettre à jour pip vers la dernière version
    pip install --no-cache-dir -r requirements.txt
    # --no-cache-dir = ne pas stocker le cache pip = image plus légère

# ── COPIER LE CODE DU PIPELINE ────────────────────────────
COPY dags/ ./dags/
# Copier tous les DAGs Airflow
COPY data/ ./data/
# Copier les données

# ── CRÉER LES DOSSIERS NÉCESSAIRES ───────────────────────
RUN mkdir -p /data/raw /data/clean /data/output /logs

# ── PORT EXPOSÉ ───────────────────────────────────────────
# Le serveur web Airflow utilise ce port
EXPOSE 8080

# ── COMMANDE PAR DÉFAUT ───────────────────────────────────
# Peut être surchargée au lancement du conteneur
CMD ["python", "dags/pipeline_mobile_money.py"]

# ================================================================
# Dockerfile - Image Python pour Producer, Train ML et Spark
# Crypto Real-Time Analytics - TP Big Data
# ================================================================
FROM python:3.11-slim-bookworm

# Métadonnées
LABEL maintainer="TP Big Data"
LABEL description="Crypto Real-Time Analytics Pipeline"

# Variables d'environnement
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

# Installer Java (nécessaire pour Spark) et dépendances système
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
        procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Répertoire de travail
WORKDIR /app

# Installer les dépendances Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier le code applicatif
COPY app/ .

# Créer les répertoires pour les modèles et checkpoints
RUN mkdir -p /app/models /app/checkpoints

# Commande par défaut (sera surchargée dans docker-compose)
CMD ["python", "--version"]

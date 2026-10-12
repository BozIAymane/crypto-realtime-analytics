# 🚀 Crypto Real-Time Analytics and Anomaly Detection

## TP Big Data — Kafka, Spark Structured Streaming et Machine Learning

> **Projet pédagogique** : Ce système collecte des données réelles du marché des cryptomonnaies, les transporte dans Kafka, les traite avec Spark Structured Streaming, détecte des anomalies grâce au Machine Learning (Isolation Forest) et visualise les résultats dans Grafana.

> ⚠️ Ce projet ne réalise **aucune transaction financière** et ne présente pas les résultats comme des prédictions financières garanties.

---

## 📋 Table des matières

1. [Architecture](#architecture)
2. [Technologies](#technologies)
3. [Prérequis](#prérequis)
4. [Installation rapide](#installation-rapide)
5. [Commandes essentielles](#commandes-essentielles)
6. [Vérification des services](#vérification-des-services)
7. [Accès au dashboard Grafana](#accès-au-dashboard-grafana)
8. [Exécution des tests](#exécution-des-tests)
9. [Arrêt et nettoyage](#arrêt-et-nettoyage)
10. [Structure du projet](#structure-du-projet)
11. [Description des composants](#description-des-composants)

---

## Architecture

```
API Binance (publique)
    │
    ▼
┌──────────────────┐
│ Producer Python  │  ← Collecte les bougies 1min toutes les 5s
└──────┬───────────┘
       │
       ▼
┌──────────────────┐
│  Kafka Broker    │  ← Topic "crypto-market"
└──────┬───────────┘
       │
       ▼
┌──────────────────┐     ┌───────────────────┐
│ Spark Structured │────▶│   PostgreSQL      │
│   Streaming      │     │  (crypto_events)  │
└──────────────────┘     └───────┬───────────┘
       │                         │
       │ Modèle ML               │
       │ (Isolation Forest)      ▼
       │                 ┌───────────────────┐
       │                 │     Grafana       │
       │                 │   Dashboard       │
       └─────────────────┘   (port 3000)    │
                         └───────────────────┘
```

**Note** : Le système collecte périodiquement des **snapshots de bougies d'une minute** depuis l'API Binance. Il ne s'agit pas d'un flux de transactions individuelles.

---

## Technologies

| Composant | Technologie | Version |
|-----------|-------------|---------|
| Langage | Python | 3.11 |
| Source de données | API Binance Spot (publique) | v3 |
| Message broker | Apache Kafka (KRaft) | 3.7 |
| Stream processing | Apache Spark Structured Streaming | 3.5.3 |
| Machine Learning | scikit-learn (Isolation Forest) | ≥ 1.4 |
| Base de données | PostgreSQL | 16 |
| Visualisation | Grafana | 11.1 |
| Conteneurisation | Docker + Docker Compose | ≥ 24.0 |
| Tests | Pytest | ≥ 8.0 |

---

## Prérequis

### Logiciels requis

```powershell
# Vérifier Docker
docker --version
docker compose version

# Vérifier que Docker Desktop est lancé
docker info
```

- **Windows 11** avec **Docker Desktop** (WSL2 activé)
- **~4 Go de RAM** disponible pour Docker
- **Connexion Internet** (pour l'API Binance et le téléchargement des images)

### Vérifier l'accès à l'API Binance

```powershell
# Test rapide
curl https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1m&limit=1
```

---

## Installation rapide

### 1. Cloner ou ouvrir le projet

```powershell
git clone https://github.com/BozIAymane/crypto-realtime-analytics.git
cd crypto-realtime-analytics
```

### 2. Configurer l'environnement

```powershell
# Copier le fichier d'environnement
copy .env.example .env
```

Les valeurs par défaut conviennent pour un TP local. Modifiez `.env` si nécessaire.

### 3. Construire les images Docker

```powershell
docker compose build
```

### 4. Démarrer tous les services

```powershell
docker compose up -d
```

### 5. Vérifier le démarrage

```powershell
# Voir l'état des conteneurs
docker compose ps

# Suivre les logs en temps réel
docker compose logs -f
```

---

## Commandes essentielles

### Démarrage complet

```powershell
docker compose up -d
```

### Suivre les logs d'un service

```powershell
# Logs du producteur
docker compose logs -f producer

# Logs de Spark
docker compose logs -f spark-stream

# Logs de l'entraînement ML
docker compose logs train-model

# Logs de Kafka
docker compose logs kafka
```

### Vérifier les messages Kafka

```powershell
# Lire les derniers messages du topic
docker exec crypto-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic crypto-market --from-beginning --max-messages 5
```

### Vérifier les données PostgreSQL

```powershell
# Se connecter à PostgreSQL
docker exec -it crypto-postgres psql -U crypto_user -d crypto_analytics

# Compter les événements
# SELECT COUNT(*) FROM crypto_events;

# Voir les derniers événements
# SELECT symbol, event_time, close_price, return_pct, is_anomaly FROM crypto_events ORDER BY event_time DESC LIMIT 10;

# Voir les anomalies
# SELECT * FROM crypto_events WHERE is_anomaly = TRUE ORDER BY event_time DESC LIMIT 10;

# Quitter
# \q
```

### Accès au dashboard Grafana

- **URL** : http://localhost:3000
- **Login** : `admin`
- **Mot de passe** : `admin123`

Le dashboard **"Crypto Real-Time Analytics - TP Big Data"** est automatiquement provisionné.

---

## Vérification des services

### Ordre de vérification recommandé

1. **Kafka** : Vérifier que le broker est healthy

```powershell
docker compose ps kafka
```

2. **Entraînement ML** : Doit se terminer avec succès

```powershell
docker compose logs train-model
# Chercher : "ENTRAÎNEMENT TERMINÉ AVEC SUCCÈS"
```

3. **Producteur** : Doit envoyer des événements

```powershell
docker compose logs -f producer
# Chercher : "✓ [BTCUSDT] close=..."
```

4. **Spark** : Doit traiter les batches

```powershell
docker compose logs -f spark-stream
# Chercher : "Batch X : Y événements"
# Chercher : "✓ Batch X : Y lignes écrites dans PostgreSQL"
```

5. **PostgreSQL** : Doit contenir des données

```powershell
docker exec crypto-postgres psql -U crypto_user -d crypto_analytics -c "SELECT COUNT(*) FROM crypto_events;"
```

6. **Grafana** : Accessible sur http://localhost:3000

---

## Exécution des tests

### Tests unitaires (depuis l'hôte Windows)

```powershell
# Option A : Exécution directe dans Docker (Recommandé, aucun Python requis sur l'hôte)
docker compose run --rm -v ./:/project -w /project -e PYTHONPATH=/project/app spark-stream pytest -v tests/

# Option B : Depuis l'hôte (si Python 3.11 est installé localement)
pip install -r requirements.txt pytest pytest-mock pyyaml
python -m pytest tests/ -v
```

---

## Arrêt et nettoyage

### Arrêter les services

```powershell
docker compose down
```

### Arrêter et supprimer les volumes (réinitialisation complète)

```powershell
docker compose down -v
```

### Supprimer les images construites

```powershell
docker compose down -v --rmi local
```

### Relancer après une réinitialisation

```powershell
docker compose up -d --build
```

---

## Structure du projet

```
TP2_crypto/
├── app/
│   ├── config.py           # Configuration centralisée
│   ├── producer.py         # Producteur Kafka (API Binance → Kafka)
│   ├── train_model.py      # Entraînement Isolation Forest
│   └── spark_stream.py     # Spark Structured Streaming
├── sql/
│   └── init.sql            # Schéma PostgreSQL
├── grafana/
│   ├── provisioning/
│   │   ├── datasources/
│   │   │   └── datasource.yml
│   │   └── dashboards/
│   │       └── dashboard.yml
│   └── dashboards/
│       └── crypto-dashboard.json
├── tests/
│   ├── test_features.py    # Tests calcul ML
│   ├── test_producer.py    # Tests producteur + API
│   ├── test_schema.py      # Tests schéma SQL
│   └── test_docker_config.py # Tests configs Docker/Grafana
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── .env                    # (créé depuis .env.example)
├── .gitignore
├── .dockerignore
├── README.md
└── COMPTE_RENDU_TP.md
```

---

## Description des composants

### 1. Producteur Kafka (`app/producer.py`)
- Interroge l'API Binance toutes les 5 secondes
- Récupère les bougies 1 minute pour BTCUSDT et ETHUSDT
- Valide et normalise les données
- Publie dans le topic Kafka `crypto-market`
- Gère les erreurs réseau et les interruptions

### 2. Entraînement ML (`app/train_model.py`)
- Télécharge ~3000 bougies historiques par symbole
- Calcule 3 features : `return_pct`, `range_pct`, `log_volume`
- Entraîne un modèle **Isolation Forest** (contamination=5%)
- Sauvegarde le modèle en Joblib dans un volume Docker partagé

### 3. Spark Structured Streaming (`app/spark_stream.py`)
- Lit le flux Kafka en temps réel
- Parse le JSON et valide les données
- Calcule `return_pct` et `range_pct` pour chaque snapshot
- Applique le modèle Isolation Forest pour détecter les anomalies
- Écrit les résultats enrichis dans PostgreSQL

### 4. PostgreSQL (`sql/init.sql`)
- Table `crypto_events` avec contrainte d'unicité
- Index optimisés pour les requêtes temporelles
- Idempotence via `(symbol, event_time)` unique

### 5. Dashboard Grafana
- 9 panels : prix BTC/ETH, variations, volumes, compteurs, anomalies
- Auto-refresh toutes les 10 secondes
- Provisionné automatiquement au démarrage

---

## Dépannage

### Le producteur ne se connecte pas à Kafka

```powershell
# Vérifier que Kafka est healthy
docker compose ps kafka
# Si le health check échoue, attendre 30-60 secondes
```

### Spark ne trouve pas le modèle ML

```powershell
# Vérifier que l'entraînement s'est terminé
docker compose logs train-model
# Si l'entraînement a échoué, le relancer
docker compose up train-model
```

### Pas de données dans Grafana

1. Vérifier que le producteur envoie des données (`docker compose logs producer`)
2. Vérifier que Spark écrit dans PostgreSQL (`docker compose logs spark-stream`)
3. Vérifier les données en base (`docker exec crypto-postgres psql ...`)
4. Rafraîchir le dashboard Grafana (Ctrl+R)
5. Ajuster la fenêtre temporelle (sélectionner "Last 15 minutes")

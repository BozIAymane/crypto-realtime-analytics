# Compte Rendu de TP — Big Data

## Crypto Real-Time Analytics and Anomaly Detection Using Kafka, Spark and Machine Learning

---

## Page de garde

| | |
|---|---|
| **Nom et Prénom** | *(À compléter)* |
| **Filière** | *(À compléter, ex : Ingénierie des Données / Big Data)* |
| **Module** | *(À compléter, ex : Big Data et Technologies Distribuées)* |
| **Établissement** | *(À compléter)* |
| **Professeur** | *(À compléter)* |
| **Date** | *(À compléter)* |
| **Année universitaire** | 2024-2025 |

---

## 1. Introduction et problématique

Le marché des cryptomonnaies se caractérise par une volatilité extrême et un volume d'échanges considérable, générant des flux de données massifs en temps réel. L'analyse de ces données représente un cas d'usage idéal pour les technologies Big Data, combinant ingestion de flux, traitement distribué et détection d'anomalies.

**Problématique** : Comment concevoir un pipeline Big Data capable de collecter, transporter, traiter et visualiser en temps réel les données du marché des cryptomonnaies, tout en détectant automatiquement les comportements anormaux grâce au Machine Learning ?

Ce TP propose une réponse concrète en implémentant une architecture complète utilisant Apache Kafka, Apache Spark Structured Streaming, scikit-learn (Isolation Forest), PostgreSQL et Grafana.

> ⚠️ **Avertissement** : Ce projet est strictement pédagogique. Il ne réalise aucune transaction financière et ne présente pas les résultats du modèle comme des prédictions financières garanties.

---

## 2. Objectifs du TP

1. **Comprendre** l'architecture d'un pipeline de traitement de données en temps réel.
2. **Implémenter** un producteur de données connecté à une API publique réelle (Binance).
3. **Utiliser** Apache Kafka comme système de messagerie distribué.
4. **Traiter** les flux de données avec Apache Spark Structured Streaming.
5. **Appliquer** un algorithme de Machine Learning (Isolation Forest) pour la détection d'anomalies.
6. **Stocker** les résultats dans une base de données relationnelle (PostgreSQL).
7. **Visualiser** les données en temps réel avec Grafana.
8. **Conteneuriser** l'ensemble du système avec Docker et Docker Compose.

---

## 3. Présentation des technologies

### 3.1 Apache Kafka

Apache Kafka est une plateforme de streaming distribuée, conçue pour le traitement de flux de données en temps réel. Dans notre projet, Kafka joue le rôle de **message broker** entre le producteur (qui collecte les données) et le consommateur (Spark). Nous utilisons le mode **KRaft** (sans Zookeeper) pour simplifier le déploiement.

### 3.2 Apache Spark Structured Streaming

Spark Structured Streaming est une extension de Spark SQL qui permet de traiter des flux de données en continu en utilisant la même API que le traitement par lots. Il offre un modèle de traitement **micro-batch** avec des garanties de tolérance aux pannes.

### 3.3 Scikit-learn et Isolation Forest

**Isolation Forest** est un algorithme de détection d'anomalies basé sur le principe d'isolation : les points anormaux sont plus faciles à séparer du reste des données. Il est particulièrement adapté à la détection d'anomalies dans des données multidimensionnelles.

Caractéristiques utilisées :
- **return_pct** : variation relative du prix `(close - open) / open`
- **range_pct** : amplitude relative `(high - low) / low`
- **log_volume** : logarithme naturel du volume d'échanges

### 3.4 PostgreSQL

Base de données relationnelle utilisée pour stocker les résultats enrichis du traitement Spark, avec des index optimisés pour les requêtes temporelles de Grafana.

### 3.5 Grafana

Outil de visualisation permettant de créer des tableaux de bord interactifs connectés directement à PostgreSQL. Le dashboard est provisionné automatiquement au démarrage.

### 3.6 Docker et Docker Compose

Docker permet de conteneuriser chaque composant du pipeline. Docker Compose orchestre le démarrage de tous les services dans le bon ordre, avec gestion des dépendances et des healthchecks.

---

## 4. Architecture générale

```
┌─────────────────────────────────────────────────────────────────┐
│                    ARCHITECTURE DU SYSTÈME                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│   API Binance (publique)                                         │
│       │ HTTPS (bougies 1min)                                     │
│       ▼                                                           │
│   ┌──────────────┐      ┌───────────────┐                        │
│   │  Producer    │─────▶│  Kafka Broker │                        │
│   │  (Python)    │ JSON │  (KRaft mode) │                        │
│   └──────────────┘      └───────┬───────┘                        │
│                                  │                                │
│   ┌──────────────┐              │                                │
│   │ Train Model  │              ▼                                │
│   │ (Isolation   │      ┌───────────────┐     ┌───────────┐     │
│   │  Forest)     │─────▶│ Spark Stream  │────▶│PostgreSQL │     │
│   └──────────────┘model │ (foreachBatch)│JDBC │           │     │
│                          └───────────────┘     └─────┬─────┘     │
│                                                       │           │
│                                                       ▼           │
│                                                ┌───────────┐     │
│                                                │  Grafana   │     │
│                                                │ Dashboard  │     │
│                                                └───────────┘     │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

### Flux de données

1. Le **producteur Python** interroge l'API Binance toutes les 5 secondes.
2. Les données (bougies 1 minute) sont sérialisées en **JSON** et publiées dans le topic Kafka `crypto-market`.
3. **Spark Structured Streaming** consomme le flux Kafka en micro-batches de 10 secondes.
4. Pour chaque batch, Spark calcule les indicateurs (`return_pct`, `range_pct`) et applique le modèle **Isolation Forest**.
5. Les résultats enrichis (avec le flag `is_anomaly`) sont écrits dans **PostgreSQL**.
6. **Grafana** interroge PostgreSQL et affiche les données en temps réel avec auto-refresh.

---

## 5. Description détaillée du pipeline

### 5.1 Collecte des données (Producer)

Le producteur se connecte à l'endpoint public Binance `/api/v3/klines` pour récupérer la dernière bougie d'une minute. Il ne nécessite aucune clé API.

**Données collectées** pour chaque symbole (BTCUSDT, ETHUSDT) :
- `symbol` : paire de trading
- `open_time` : timestamp d'ouverture de la bougie
- `open`, `high`, `low`, `close` : prix OHLC
- `volume` : volume en crypto de base
- `quote_volume` : volume en devise de cotation (USDT)
- `trade_count` : nombre de transactions
- `collected_at` : horodatage UTC de la collecte

**Mécanismes de robustesse** :
- Retry automatique pour la connexion Kafka (jusqu'à 30 tentatives)
- Timeout sur les requêtes HTTP (10 secondes)
- Gestion des erreurs : timeout, HTTP, connexion, parsing
- Arrêt gracieux sur SIGTERM/SIGINT

### 5.2 Transport (Kafka)

Le topic `crypto-market` reçoit les événements JSON. Kafka est configuré en mode KRaft (sans Zookeeper) avec :
- 2 partitions
- Rétention de 24 heures
- Auto-création de topics activée

### 5.3 Traitement (Spark Structured Streaming)

Spark consomme le flux Kafka et effectue les traitements suivants dans un `foreachBatch` :

1. **Parsing JSON** : désérialisation avec schéma strict
2. **Filtrage** : rejet des événements invalides (prix ≤ 0)
3. **Calcul des indicateurs** :
   - `return_pct = (close - open) / open`
   - `range_pct = (high - low) / low`
4. **Inférence ML** : application du modèle Isolation Forest chargé depuis un volume partagé
5. **Écriture JDBC** : insertion dans PostgreSQL

### 5.4 Détection d'anomalies (Machine Learning)

Le modèle Isolation Forest est entraîné **avant** le démarrage de Spark sur ~6000 bougies historiques (3000 par symbole).

**Paramètres du modèle** :
- `n_estimators = 100`
- `contamination = 0.05` (5% d'anomalies attendues)
- `random_state = 42` (reproductibilité)

Le modèle est sauvegardé en format Joblib et partagé via un volume Docker.

### 5.5 Stockage (PostgreSQL)

La table `crypto_events` stocke chaque événement enrichi. Une contrainte d'unicité sur `(symbol, event_time)` assure l'idempotence lors des reprises.

### 5.6 Visualisation (Grafana)

Le dashboard contient 9 panels :
1. Évolution du prix BTCUSDT
2. Évolution du prix ETHUSDT
3. Variation relative (return_pct)
4. Volume des snapshots
5. Nombre total d'événements traités
6. Nombre d'anomalies détectées
7. Compteur BTCUSDT
8. Compteur ETHUSDT
9. Tableau des dernières anomalies détectées

---

## 6. Modèle de données PostgreSQL

```sql
CREATE TABLE crypto_events (
    id              BIGSERIAL       PRIMARY KEY,
    symbol          VARCHAR(20)     NOT NULL,
    event_time      TIMESTAMP       NOT NULL,
    open_price      DOUBLE PRECISION NOT NULL,
    high_price      DOUBLE PRECISION NOT NULL,
    low_price       DOUBLE PRECISION NOT NULL,
    close_price     DOUBLE PRECISION NOT NULL,
    volume          DOUBLE PRECISION DEFAULT 0,
    quote_volume    DOUBLE PRECISION DEFAULT 0,
    trade_count     INTEGER          DEFAULT 0,
    return_pct      DOUBLE PRECISION DEFAULT 0,
    range_pct       DOUBLE PRECISION DEFAULT 0,
    is_anomaly      BOOLEAN          DEFAULT FALSE,
    inserted_at     TIMESTAMP        DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_symbol_event_time UNIQUE (symbol, event_time)
);
```

**Index** :
- `idx_event_time` : requêtes temporelles
- `idx_symbol` : filtrage par symbole
- `idx_symbol_time` : requêtes combinées (Grafana)
- `idx_anomaly` : recherche rapide des anomalies

---

## 7. Procédure d'installation et d'exécution

### Prérequis

- Windows 11 avec Docker Desktop et WSL2
- ~4 Go de RAM disponible pour Docker
- Connexion Internet

### Démarrage

```powershell
# 1. Ouvrir le projet
cd C:\Users\LENOVO\Desktop\EST_K\BD\TP2_crypto

# 2. Configurer l'environnement
copy .env.example .env

# 3. Construire les images
docker compose build

# 4. Démarrer tous les services
docker compose up -d

# 5. Vérifier l'état
docker compose ps

# 6. Suivre les logs
docker compose logs -f
```

### Vérification

```powershell
# Vérifier les données PostgreSQL
docker exec crypto-postgres psql -U crypto_user -d crypto_analytics -c "SELECT COUNT(*) FROM crypto_events;"

# Accéder à Grafana
# URL : http://localhost:3000
# Login : admin / admin123
```

---

## 8. Résultats attendus et interprétation

### Ce que vous devriez observer

Après 2 à 5 minutes de fonctionnement :

1. **Logs du producteur** : Messages réguliers avec les prix actuels de BTC et ETH
2. **Logs Spark** : Batches traités avec le nombre d'événements et les écritures PostgreSQL
3. **PostgreSQL** : Événements croissants dans la table `crypto_events`
4. **Grafana** : Graphiques des prix en temps réel, compteurs d'événements croissants
5. **Anomalies** : Quelques anomalies détectées (~5% des événements selon la contamination)

### Interprétation des anomalies

Le modèle Isolation Forest identifie comme anomalies les bougies dont les caractéristiques (`return_pct`, `range_pct`, `log_volume`) s'écartent significativement de la distribution historique. Une anomalie signale un comportement **statistiquement inhabituel**, pas nécessairement un problème.

> 📸 **Captures d'écran à ajouter ici** :
>
> - [ ] Capture du terminal avec `docker compose ps` montrant tous les services healthy
> - [ ] Capture des logs du producteur montrant les événements envoyés
> - [ ] Capture des logs Spark montrant les batches traités
> - [ ] Capture de la requête SQL montrant les données dans PostgreSQL
> - [ ] Capture du dashboard Grafana avec les graphiques de prix
> - [ ] Capture du tableau des anomalies dans Grafana

---

## 9. Limites du système

1. **Fréquence de collecte** : Le système collecte des snapshots de bougies d'une minute toutes les 5 secondes. La même bougie peut être collectée plusieurs fois pendant sa formation, mais grâce à la contrainte d'unicité, un seul enregistrement est conservé par bougie et par symbole.

2. **Latence** : Le traitement par micro-batches (10 secondes) introduit une latence entre l'événement réel et sa visualisation.

3. **Modèle ML statique** : L'Isolation Forest est entraîné une seule fois au démarrage. Il ne s'adapte pas aux évolutions du marché en temps réel.

4. **Ressources** : Le mode local Docker consomme davantage de ressources qu'un déploiement distribué optimisé.

5. **API Binance** : L'API publique sans authentification peut imposer des limites de requêtes. Le producteur respecte un intervalle de 5 secondes pour éviter le rate limiting.

6. **Échelle** : Ce pipeline est conçu pour un TP pédagogique avec 2 symboles. Un déploiement en production nécessiterait un dimensionnement adapté.

---

## 10. Conclusion

Ce TP a permis de concevoir et d'implémenter un pipeline Big Data complet pour l'analyse en temps réel des cryptomonnaies. Le système combine :

- **L'ingestion** de données réelles depuis l'API Binance
- **Le transport** via Apache Kafka en mode KRaft
- **Le traitement** distribué avec Spark Structured Streaming
- **La détection d'anomalies** par Machine Learning (Isolation Forest)
- **Le stockage** structuré dans PostgreSQL
- **La visualisation** interactive dans Grafana

L'ensemble est conteneurisé avec Docker Compose, permettant un déploiement reproductible et portable. Ce projet illustre concrètement les concepts fondamentaux du Big Data : ingestion de flux, traitement en temps réel, persistance et visualisation.

---

## 11. Questions techniques possibles du professeur

### Q1 : Pourquoi avoir choisi Kafka plutôt qu'un simple système de fichiers ?
**R** : Kafka offre un découplage entre le producteur et le consommateur, une persistance des messages, une tolérance aux pannes et la possibilité de rejouer les données. Un fichier ne permettrait pas le traitement en temps réel ni la mise à l'échelle.

### Q2 : Quelle est la différence entre Spark Streaming (DStreams) et Structured Streaming ?
**R** : Structured Streaming utilise l'API DataFrame/SQL, offrant un modèle de programmation plus simple et des optimisations automatiques (Catalyst optimizer). Les DStreams sont l'ancienne API, plus bas niveau et moins optimisée.

### Q3 : Pourquoi utiliser `foreachBatch` au lieu d'un sink direct ?
**R** : `foreachBatch` permet d'appliquer le modèle ML (scikit-learn, pas un modèle Spark MLlib) sur chaque micro-batch et d'écrire les résultats enrichis dans PostgreSQL via JDBC, ce qui n'est pas possible avec un sink Kafka ou fichier standard.

### Q4 : Comment fonctionne l'Isolation Forest ?
**R** : L'algorithme construit des arbres de décision aléatoires. Les points anormaux nécessitent moins de divisions pour être isolés, car ils sont rares et différents. Le score d'anomalie est inversement proportionnel à la profondeur moyenne d'isolation.

### Q5 : Pourquoi le modèle est-il entraîné une seule fois ?
**R** : Pour simplifier le pipeline dans le cadre du TP. En production, on pourrait implémenter un réentraînement périodique (ex : toutes les heures) ou utiliser un modèle en ligne adaptatif.

### Q6 : Comment gérez-vous l'idempotence des écritures ?
**R** : La contrainte `UNIQUE (symbol, event_time)` dans PostgreSQL empêche les doublons. Si Spark redémarre et re-traite des données, les insertions en double sont rejetées grâce au `ON CONFLICT` implicite.

### Q7 : Que signifie le mode KRaft de Kafka ?
**R** : KRaft (Kafka Raft) est le nouveau mode de consensus de Kafka qui remplace Zookeeper. Il simplifie l'architecture en éliminant un composant externe et améliore les performances.

### Q8 : Comment le modèle ML est-il partagé entre les conteneurs ?
**R** : Via un volume Docker nommé `ml-models`. Le conteneur `train-model` écrit le modèle Joblib dans ce volume, et le conteneur `spark-stream` le lit en lecture seule.

### Q9 : Pourquoi utiliser le logarithme du volume comme feature ?
**R** : Le volume a une distribution très asymétrique (log-normale). Le logarithme normalise cette distribution et rend la feature plus exploitable pour l'Isolation Forest.

### Q10 : Comment assurer la tolérance aux pannes du streaming ?
**R** : Spark utilise un répertoire de checkpoint qui enregistre la progression du streaming. En cas de redémarrage, Spark reprend la lecture Kafka là où il s'était arrêté.

---

## Annexes

### A. Tests unitaires

Le projet inclut 46 tests unitaires (Pytest) couvrant :
- Calcul des caractéristiques ML (12 tests)
- Validation des événements et comportement API (13 tests)
- Cohérence du schéma SQL (8 tests)
- Validation Docker et Grafana (13 tests)

```powershell
python -m pytest tests/ -v
```

### B. Fichiers du projet

Voir la section "Structure du projet" dans le README.md.

---

*Fin du compte rendu.*

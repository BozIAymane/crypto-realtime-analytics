"""
producer.py - Producteur Kafka pour les données de marché crypto.

Collecte périodiquement les bougies (klines) d'une minute depuis l'API Binance
pour BTCUSDT et ETHUSDT, puis publie chaque snapshot dans le topic Kafka
"crypto-market" au format JSON.

Note pédagogique : le système collecte des snapshots de bougies à intervalle
régulier. Il ne s'agit PAS d'un flux de transactions individuelles.
"""
import json
import logging
import signal
import sys
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

try:
    from kafka.errors import NoBrokersAvailable
except ImportError:
    NoBrokersAvailable = KafkaError

from config import (
    BINANCE_BASE_URL,
    FETCH_INTERVAL,
    KAFKA_BROKER,
    KAFKA_TOPIC,
    KLINE_INTERVAL,
    SYMBOLS,
)

# ── Logging ────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("producer")

# ── Arrêt propre ───────────────────────────────────────
running = True


def _shutdown(signum, frame):
    global running
    logger.info("Signal d'arrêt reçu (%s). Fermeture…", signum)
    running = False


signal.signal(signal.SIGINT, _shutdown)
signal.signal(signal.SIGTERM, _shutdown)


# ── Fonctions utilitaires ─────────────────────────────


def create_kafka_producer(broker: str, retries: int = 30, delay: float = 5.0) -> KafkaProducer:
    """Crée un producteur Kafka avec retry pour attendre le broker."""
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=broker,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
            )
            logger.info("Connecté au broker Kafka : %s", broker)
            return producer
        except NoBrokersAvailable:
            logger.warning(
                "Kafka indisponible (tentative %d/%d). Retry dans %.0fs…",
                attempt, retries, delay,
            )
            time.sleep(delay)
    logger.error("Impossible de se connecter à Kafka après %d tentatives.", retries)
    sys.exit(1)


def fetch_kline(symbol: str) -> dict | None:
    """
    Récupère la dernière bougie d'une minute pour le symbole donné.

    Retourne un dictionnaire normalisé ou None en cas d'erreur.
    """
    url = f"{BINANCE_BASE_URL}/api/v3/klines"
    params = {"symbol": symbol, "interval": KLINE_INTERVAL, "limit": 1}
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if not data or not isinstance(data, list) or len(data) == 0:
            logger.warning("Réponse vide pour %s", symbol)
            return None

        k = data[0]
        # Structure Binance kline :
        # [open_time, open, high, low, close, volume, close_time,
        #  quote_volume, trade_count, ...]
        event = {
            "symbol": symbol,
            "open_time": int(k[0]),
            "open": float(k[1]),
            "high": float(k[2]),
            "low": float(k[3]),
            "close": float(k[4]),
            "volume": float(k[5]),
            "close_time": int(k[6]),
            "quote_volume": float(k[7]),
            "trade_count": int(k[8]),
            "collected_at": datetime.now(timezone.utc).isoformat(),
        }
        return event

    except requests.exceptions.Timeout:
        logger.error("Timeout lors de la requête pour %s", symbol)
    except requests.exceptions.HTTPError as e:
        logger.error("Erreur HTTP pour %s : %s", symbol, e)
    except requests.exceptions.ConnectionError:
        logger.error("Erreur de connexion pour %s", symbol)
    except (ValueError, IndexError, KeyError) as e:
        logger.error("Erreur de parsing pour %s : %s", symbol, e)
    return None


def validate_event(event: dict) -> bool:
    """Vérifie qu'un événement contient des valeurs cohérentes."""
    required = ["symbol", "open", "high", "low", "close", "volume"]
    for field in required:
        if field not in event:
            return False
    # Prix et volume doivent être positifs
    if event["open"] <= 0 or event["close"] <= 0:
        return False
    if event["high"] < event["low"]:
        return False
    if event["volume"] < 0:
        return False
    return True


# ── Boucle principale ─────────────────────────────────


def main():
    logger.info("=== Démarrage du producteur Kafka ===")
    logger.info("Broker  : %s", KAFKA_BROKER)
    logger.info("Topic   : %s", KAFKA_TOPIC)
    logger.info("Symbols : %s", SYMBOLS)
    logger.info("Interval: %ds", FETCH_INTERVAL)

    producer = create_kafka_producer(KAFKA_BROKER)

    event_count = 0
    error_count = 0

    while running:
        for symbol in SYMBOLS:
            if not running:
                break
            event = fetch_kline(symbol)
            if event and validate_event(event):
                producer.send(KAFKA_TOPIC, value=event)
                event_count += 1
                logger.info(
                    "✓ [%s] close=%.2f vol=%.4f trades=%d  (total=%d)",
                    event["symbol"],
                    event["close"],
                    event["volume"],
                    event["trade_count"],
                    event_count,
                )
            else:
                error_count += 1
                logger.warning("✗ Événement ignoré pour %s (erreurs=%d)", symbol, error_count)

        producer.flush()

        # Attente entre les cycles de collecte
        for _ in range(int(FETCH_INTERVAL * 10)):
            if not running:
                break
            time.sleep(0.1)

    producer.close()
    logger.info("=== Producteur arrêté. Total=%d, Erreurs=%d ===", event_count, error_count)


if __name__ == "__main__":
    main()

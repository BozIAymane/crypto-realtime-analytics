"""
config.py - Configuration centralisée du projet Crypto Real-Time Analytics.

Charge les variables depuis l'environnement ou utilise des valeurs par défaut
adaptées à l'environnement Docker du TP.
"""
import os


# ── Kafka ──────────────────────────────────────────────
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crypto-market")

# ── PostgreSQL ─────────────────────────────────────────
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "crypto_analytics")
POSTGRES_USER = os.getenv("POSTGRES_USER", "crypto_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "crypto_pass_2024")

POSTGRES_JDBC_URL = (
    f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)
POSTGRES_PROPS = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}

# ── Binance API ────────────────────────────────────────
BINANCE_BASE_URL = os.getenv("BINANCE_BASE_URL", "https://api.binance.com")
SYMBOLS = os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT").split(",")
FETCH_INTERVAL = int(os.getenv("FETCH_INTERVAL_SECONDS", "5"))
KLINE_INTERVAL = "1m"  # bougies d'une minute

# ── Machine Learning ──────────────────────────────────
MODEL_DIR = os.getenv("MODEL_DIR", "/app/models")
MODEL_PATH = os.path.join(MODEL_DIR, "isolation_forest.joblib")

# ── Spark ──────────────────────────────────────────────
SPARK_CHECKPOINT_DIR = os.getenv("SPARK_CHECKPOINT_DIR", "/app/checkpoints")

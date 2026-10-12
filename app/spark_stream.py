"""
spark_stream.py - Traitement Spark Structured Streaming.

Lit les messages du topic Kafka "crypto-market", parse le JSON,
calcule les indicateurs techniques, applique le modèle Isolation Forest
pour détecter les anomalies, et écrit les résultats dans PostgreSQL.
"""
import logging
import os
import sys
import time

import joblib
import numpy as np
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from config import (
    KAFKA_BROKER,
    KAFKA_TOPIC,
    MODEL_PATH,
    SPARK_CHECKPOINT_DIR,
)

# ── Logging ────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("spark_stream")

# ── Schéma JSON des événements Kafka ──────────────────
EVENT_SCHEMA = StructType([
    StructField("symbol", StringType(), False),
    StructField("open_time", LongType(), False),
    StructField("open", DoubleType(), False),
    StructField("high", DoubleType(), False),
    StructField("low", DoubleType(), False),
    StructField("close", DoubleType(), False),
    StructField("volume", DoubleType(), False),
    StructField("close_time", LongType(), True),
    StructField("quote_volume", DoubleType(), True),
    StructField("trade_count", IntegerType(), True),
    StructField("collected_at", StringType(), True),
])


# ── Chargement du modèle ML ──────────────────────────


def wait_for_model(path: str, timeout: int = 300) -> object:
    """Attend que le fichier modèle soit disponible (écrit par train_model)."""
    logger.info("Attente du modèle ML : %s", path)
    start = time.time()
    while time.time() - start < timeout:
        if os.path.exists(path) and os.path.getsize(path) > 0:
            model = joblib.load(path)
            logger.info("✓ Modèle chargé depuis %s", path)
            return model
        time.sleep(5)
    logger.error("✗ Timeout : modèle introuvable après %ds", timeout)
    sys.exit(1)


# ── Fonction de traitement par micro-batch ────────────


def process_batch(batch_df: DataFrame, batch_id: int, model, jdbc_url: str, props: dict):
    """
    Traite un micro-batch Spark :
    1. Calcule return_pct et range_pct
    2. Applique le modèle Isolation Forest
    3. Écrit dans PostgreSQL
    """
    if batch_df.rdd.isEmpty():
        return

    count = batch_df.count()
    logger.info("── Batch %d : %d événements ──", batch_id, count)

    # Calcul des indicateurs
    enriched = batch_df \
        .withColumn("return_pct",
                     F.when(F.col("open") > 0,
                            (F.col("close") - F.col("open")) / F.col("open"))
                     .otherwise(0.0)) \
        .withColumn("range_pct",
                     F.when(F.col("low") > 0,
                            (F.col("high") - F.col("low")) / F.col("low"))
                     .otherwise(0.0)) \
        .withColumn("event_time",
                     (F.col("open_time") / 1000).cast(TimestampType())) \
        .withColumn("inserted_at", F.current_timestamp())

    # Collecter pour l'inférence ML (les micro-batches sont petits)
    rows = enriched.collect()

    results = []
    for row in rows:
        return_pct = float(row["return_pct"]) if row["return_pct"] is not None else 0.0
        range_pct = float(row["range_pct"]) if row["range_pct"] is not None else 0.0
        volume = float(row["volume"]) if row["volume"] is not None else 0.0

        # Calcul du log_volume pour le modèle
        log_vol = np.log(volume) if volume > 0 else 0.0

        # Prédiction Isolation Forest : -1 = anomalie, 1 = normal
        try:
            features = np.array([[return_pct, range_pct, log_vol]])
            prediction = model.predict(features)[0]
            is_anomaly = bool(prediction == -1)
        except Exception as e:
            logger.warning("Erreur ML pour %s : %s", row["symbol"], e)
            is_anomaly = False

        if is_anomaly:
            logger.info("🔴 ANOMALIE : %s close=%.2f return=%.4f%% range=%.4f%%",
                        row["symbol"], row["close"], return_pct * 100, range_pct * 100)

        results.append((
            row["symbol"],
            row["event_time"],
            float(row["open"]),
            float(row["high"]),
            float(row["low"]),
            float(row["close"]),
            volume,
            float(row["quote_volume"]) if row["quote_volume"] else 0.0,
            int(row["trade_count"]) if row["trade_count"] else 0,
            return_pct,
            range_pct,
            is_anomaly,
            row["inserted_at"],
        ))

    # Écriture dans PostgreSQL avec UPSERT (ON CONFLICT)
    try:
        import psycopg2

        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            dbname=os.getenv("POSTGRES_DB", "crypto_analytics"),
            user=os.getenv("POSTGRES_USER", "crypto_user"),
            password=os.getenv("POSTGRES_PASSWORD", "crypto_pass_2024"),
        )
        cur = conn.cursor()

        upsert_sql = """
            INSERT INTO crypto_events
                (symbol, event_time, open_price, high_price, low_price,
                 close_price, volume, quote_volume, trade_count,
                 return_pct, range_pct, is_anomaly, inserted_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, event_time) DO UPDATE SET
                close_price = EXCLUDED.close_price,
                volume = EXCLUDED.volume,
                quote_volume = EXCLUDED.quote_volume,
                trade_count = EXCLUDED.trade_count,
                return_pct = EXCLUDED.return_pct,
                range_pct = EXCLUDED.range_pct,
                is_anomaly = EXCLUDED.is_anomaly,
                inserted_at = EXCLUDED.inserted_at
        """

        written = 0
        for r in results:
            cur.execute(upsert_sql, r)
            written += 1

        conn.commit()
        cur.close()
        conn.close()
        logger.info("✓ Batch %d : %d lignes écrites dans PostgreSQL", batch_id, written)
    except Exception as e:
        logger.error("✗ Erreur d'écriture PostgreSQL (batch %d) : %s", batch_id, e)


# ── Main ──────────────────────────────────────────────


def main():
    logger.info("=" * 60)
    logger.info("  DÉMARRAGE SPARK STRUCTURED STREAMING")
    logger.info("=" * 60)

    # Charger le modèle ML
    model = wait_for_model(MODEL_PATH)

    # Créer la session Spark
    spark = SparkSession.builder \
        .appName("CryptoRealTimeAnalytics") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logger.info("✓ SparkSession créée")

    # Lire le flux Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    logger.info("✓ Flux Kafka connecté : %s @ %s", KAFKA_TOPIC, KAFKA_BROKER)

    # Parser le JSON
    parsed_df = kafka_df \
        .selectExpr("CAST(value AS STRING) as json_str") \
        .select(F.from_json(F.col("json_str"), EVENT_SCHEMA).alias("data")) \
        .select("data.*") \
        .filter(F.col("symbol").isNotNull()) \
        .filter(F.col("open") > 0) \
        .filter(F.col("close") > 0)

    # Démarrer le streaming avec foreachBatch
    query = parsed_df.writeStream \
        .foreachBatch(
            lambda df, bid: process_batch(df, bid, model, None, None)
        ) \
        .option("checkpointLocation", SPARK_CHECKPOINT_DIR) \
        .trigger(processingTime="10 seconds") \
        .start()

    logger.info("✓ Streaming démarré (trigger=10s)")
    logger.info("  En attente de données…")

    query.awaitTermination()


if __name__ == "__main__":
    main()

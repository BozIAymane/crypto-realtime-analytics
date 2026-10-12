"""
train_model.py - Entraînement du modèle Isolation Forest.

Télécharge les données historiques depuis l'API Binance, calcule les
caractéristiques (features) et entraîne un modèle de détection d'anomalies
avec scikit-learn IsolationForest.

Le modèle entraîné est sauvegardé au format Joblib dans un volume partagé
avec Spark pour permettre l'inférence en temps réel.
"""
import logging
import os
import sys
import time

import joblib
import numpy as np
import requests
from sklearn.ensemble import IsolationForest

from config import BINANCE_BASE_URL, MODEL_DIR, MODEL_PATH, SYMBOLS

# ── Logging ────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("train_model")


# ── Téléchargement des données historiques ────────────


def fetch_historical_klines(symbol: str, interval: str = "1m", limit: int = 1000) -> list[dict]:
    """
    Télécharge les klines historiques depuis l'API Binance.
    Retourne une liste de dictionnaires avec les champs OHLCV.
    """
    url = f"{BINANCE_BASE_URL}/api/v3/klines"
    all_klines = []
    end_time = None

    # On fait plusieurs appels pour récupérer assez de données
    batches = 3  # 3 x 1000 = 3000 bougies ≈ 50 heures
    for batch in range(batches):
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        if end_time:
            params["endTime"] = end_time - 1

        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if not data:
                break

            for k in data:
                kline = {
                    "symbol": symbol,
                    "open_time": int(k[0]),
                    "open": float(k[1]),
                    "high": float(k[2]),
                    "low": float(k[3]),
                    "close": float(k[4]),
                    "volume": float(k[5]),
                    "quote_volume": float(k[7]),
                    "trade_count": int(k[8]),
                }
                all_klines.append(kline)

            end_time = int(data[0][0])
            logger.info(
                "  Batch %d/%d pour %s : %d bougies récupérées",
                batch + 1, batches, symbol, len(data),
            )
            time.sleep(0.5)  # Respecter les limites de l'API

        except Exception as e:
            logger.error("Erreur lors du téléchargement pour %s (batch %d): %s", symbol, batch + 1, e)
            break

    return all_klines


# ── Calcul des caractéristiques ML ────────────────────


def compute_features(klines: list[dict]) -> np.ndarray | None:
    """
    Calcule les features pour l'Isolation Forest :
    - return_pct : variation relative (close - open) / open
    - range_pct  : amplitude relative (high - low) / low
    - log_volume : logarithme naturel du volume

    Nettoie les données invalides (prix nuls, NaN, Inf).
    """
    features = []
    skipped = 0

    for k in klines:
        open_p = k["open"]
        close_p = k["close"]
        high_p = k["high"]
        low_p = k["low"]
        volume = k["volume"]

        # Rejeter les prix nuls ou négatifs
        if open_p <= 0 or close_p <= 0 or high_p <= 0 or low_p <= 0:
            skipped += 1
            continue
        if volume <= 0:
            skipped += 1
            continue

        return_pct = (close_p - open_p) / open_p
        range_pct = (high_p - low_p) / low_p
        log_volume = np.log(volume)

        # Vérifier que les valeurs sont finies
        if not (np.isfinite(return_pct) and np.isfinite(range_pct) and np.isfinite(log_volume)):
            skipped += 1
            continue

        features.append([return_pct, range_pct, log_volume])

    if skipped > 0:
        logger.info("  Échantillons ignorés (données invalides) : %d", skipped)

    if len(features) < 10:
        logger.error("Pas assez de données valides pour l'entraînement (%d)", len(features))
        return None

    return np.array(features)


# ── Entraînement ──────────────────────────────────────


def train_isolation_forest(X: np.ndarray, contamination: float = 0.05) -> IsolationForest:
    """
    Entraîne un modèle Isolation Forest.

    Paramètres :
    - contamination : proportion estimée d'anomalies (5% par défaut)
    - n_estimators  : nombre d'arbres
    - random_state  : reproductibilité
    """
    model = IsolationForest(
        n_estimators=100,
        contamination=contamination,
        max_samples="auto",
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X)
    return model


# ── Main ──────────────────────────────────────────────


def main():
    logger.info("=" * 60)
    logger.info("  ENTRAÎNEMENT DU MODÈLE ISOLATION FOREST")
    logger.info("=" * 60)

    # 1. Télécharger les données historiques
    all_klines = []
    for symbol in SYMBOLS:
        logger.info("Téléchargement des données historiques pour %s…", symbol)
        klines = fetch_historical_klines(symbol, interval="1m", limit=1000)
        logger.info("  → %d bougies récupérées pour %s", len(klines), symbol)
        all_klines.extend(klines)

    logger.info("Total bougies téléchargées : %d", len(all_klines))

    if not all_klines:
        logger.error("Aucune donnée récupérée. Abandon de l'entraînement.")
        sys.exit(1)

    # 2. Calculer les features
    logger.info("Calcul des caractéristiques ML…")
    X = compute_features(all_klines)
    if X is None:
        logger.error("Impossible de calculer les features. Abandon.")
        sys.exit(1)

    logger.info("  → Échantillons valides   : %d", X.shape[0])
    logger.info("  → Caractéristiques       : %d (return_pct, range_pct, log_volume)", X.shape[1])
    logger.info("  → return_pct  : mean=%.6f, std=%.6f", X[:, 0].mean(), X[:, 0].std())
    logger.info("  → range_pct   : mean=%.6f, std=%.6f", X[:, 1].mean(), X[:, 1].std())
    logger.info("  → log_volume  : mean=%.4f, std=%.4f", X[:, 2].mean(), X[:, 2].std())

    # 3. Entraîner le modèle
    logger.info("Entraînement de l'Isolation Forest…")
    contamination = 0.05
    model = train_isolation_forest(X, contamination=contamination)

    # Évaluation rapide sur les données d'entraînement
    predictions = model.predict(X)
    n_anomalies = (predictions == -1).sum()
    logger.info("  → Anomalies détectées sur les données d'entraînement : %d / %d (%.1f%%)",
                n_anomalies, len(predictions), 100 * n_anomalies / len(predictions))

    # 4. Sauvegarder le modèle
    os.makedirs(MODEL_DIR, exist_ok=True)
    joblib.dump(model, MODEL_PATH)
    logger.info("  → Modèle sauvegardé : %s", MODEL_PATH)
    logger.info("  → Paramètres : n_estimators=%d, contamination=%.2f, random_state=42",
                model.n_estimators, contamination)

    logger.info("=" * 60)
    logger.info("  ENTRAÎNEMENT TERMINÉ AVEC SUCCÈS")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

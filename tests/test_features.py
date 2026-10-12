"""
test_features.py - Tests unitaires pour le calcul des caractéristiques ML.

Vérifie le calcul de return_pct, range_pct et log_volume,
ainsi que le rejet des données invalides.
"""
import sys
import os

import numpy as np
import pytest

# Ajouter le répertoire app/ au path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

from train_model import compute_features


class TestComputeFeatures:
    """Tests pour la fonction compute_features."""

    def test_valid_klines(self):
        """Vérifie le calcul correct des features avec des données valides."""
        klines = [
            {"open": 100.0, "high": 110.0, "low": 95.0, "close": 105.0, "volume": 1000.0},
            {"open": 200.0, "high": 220.0, "low": 190.0, "close": 210.0, "volume": 2000.0},
        ] * 10  # Besoin de ≥ 10 échantillons

        result = compute_features(klines)
        assert result is not None
        assert result.shape[0] == 20
        assert result.shape[1] == 3  # return_pct, range_pct, log_volume

    def test_return_pct_calculation(self):
        """Vérifie que return_pct = (close - open) / open."""
        klines = [
            {"open": 100.0, "high": 110.0, "low": 90.0, "close": 105.0, "volume": 500.0},
        ] * 15

        result = compute_features(klines)
        assert result is not None
        expected_return = (105.0 - 100.0) / 100.0  # 0.05
        np.testing.assert_almost_equal(result[0, 0], expected_return, decimal=6)

    def test_range_pct_calculation(self):
        """Vérifie que range_pct = (high - low) / low."""
        klines = [
            {"open": 100.0, "high": 120.0, "low": 80.0, "close": 110.0, "volume": 500.0},
        ] * 15

        result = compute_features(klines)
        assert result is not None
        expected_range = (120.0 - 80.0) / 80.0  # 0.5
        np.testing.assert_almost_equal(result[0, 1], expected_range, decimal=6)

    def test_log_volume_calculation(self):
        """Vérifie que log_volume = ln(volume)."""
        klines = [
            {"open": 100.0, "high": 110.0, "low": 90.0, "close": 105.0, "volume": 1000.0},
        ] * 15

        result = compute_features(klines)
        assert result is not None
        expected_log_vol = np.log(1000.0)
        np.testing.assert_almost_equal(result[0, 2], expected_log_vol, decimal=6)

    def test_zero_open_price_rejected(self):
        """Les prix d'ouverture à 0 doivent être rejetés."""
        klines = [
            {"open": 0.0, "high": 110.0, "low": 90.0, "close": 105.0, "volume": 1000.0},
        ] * 15

        result = compute_features(klines)
        assert result is None  # Tous rejetés → pas assez de données

    def test_zero_close_price_rejected(self):
        """Les prix de clôture à 0 doivent être rejetés."""
        klines = [
            {"open": 100.0, "high": 110.0, "low": 90.0, "close": 0.0, "volume": 1000.0},
        ] * 15

        result = compute_features(klines)
        assert result is None

    def test_negative_price_rejected(self):
        """Les prix négatifs doivent être rejetés."""
        klines = [
            {"open": -100.0, "high": 110.0, "low": 90.0, "close": 105.0, "volume": 1000.0},
        ] * 15

        result = compute_features(klines)
        assert result is None

    def test_zero_volume_rejected(self):
        """Le volume à 0 doit être rejeté."""
        klines = [
            {"open": 100.0, "high": 110.0, "low": 90.0, "close": 105.0, "volume": 0.0},
        ] * 15

        result = compute_features(klines)
        assert result is None

    def test_mixed_valid_invalid(self):
        """Un mélange de données valides et invalides ne conserve que les valides."""
        klines = [
            {"open": 100.0, "high": 110.0, "low": 90.0, "close": 105.0, "volume": 1000.0},
        ] * 12 + [
            {"open": 0.0, "high": 110.0, "low": 90.0, "close": 105.0, "volume": 1000.0},
        ] * 3

        result = compute_features(klines)
        assert result is not None
        assert result.shape[0] == 12  # Seuls les 12 valides

    def test_not_enough_valid_data(self):
        """Moins de 10 échantillons valides → retourne None."""
        klines = [
            {"open": 100.0, "high": 110.0, "low": 90.0, "close": 105.0, "volume": 1000.0},
        ] * 5  # Seulement 5

        result = compute_features(klines)
        assert result is None

    def test_empty_klines(self):
        """Liste vide → retourne None."""
        result = compute_features([])
        assert result is None

    def test_features_are_finite(self):
        """Toutes les features calculées doivent être des nombres finis."""
        klines = [
            {"open": 100.0 + i, "high": 110.0 + i, "low": 90.0 + i,
             "close": 105.0 + i, "volume": 1000.0 + i * 100}
            for i in range(20)
        ]

        result = compute_features(klines)
        assert result is not None
        assert np.all(np.isfinite(result))

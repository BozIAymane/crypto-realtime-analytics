"""
test_producer.py - Tests unitaires pour le producteur Kafka.

Vérifie la validation des événements, le format JSON et le comportement
en cas d'erreur API (avec mocks).
"""
import sys
import os
import json
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone

import pytest

# Ajouter le répertoire app/ au path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

from producer import validate_event, fetch_kline


class TestValidateEvent:
    """Tests pour la validation des événements."""

    def test_valid_event(self):
        """Un événement complet et valide doit être accepté."""
        event = {
            "symbol": "BTCUSDT",
            "open_time": 1700000000000,
            "open": 35000.0,
            "high": 35500.0,
            "low": 34800.0,
            "close": 35200.0,
            "volume": 150.5,
            "quote_volume": 5280000.0,
            "trade_count": 1200,
            "collected_at": "2024-01-01T12:00:00+00:00",
        }
        assert validate_event(event) is True

    def test_missing_symbol(self):
        """Un événement sans symbole est invalide."""
        event = {
            "open": 35000.0,
            "high": 35500.0,
            "low": 34800.0,
            "close": 35200.0,
            "volume": 150.5,
        }
        assert validate_event(event) is False

    def test_missing_open(self):
        """Un événement sans prix d'ouverture est invalide."""
        event = {
            "symbol": "BTCUSDT",
            "high": 35500.0,
            "low": 34800.0,
            "close": 35200.0,
            "volume": 150.5,
        }
        assert validate_event(event) is False

    def test_zero_open_price(self):
        """Un prix d'ouverture à 0 est invalide."""
        event = {
            "symbol": "BTCUSDT",
            "open": 0.0,
            "high": 35500.0,
            "low": 34800.0,
            "close": 35200.0,
            "volume": 150.5,
        }
        assert validate_event(event) is False

    def test_zero_close_price(self):
        """Un prix de clôture à 0 est invalide."""
        event = {
            "symbol": "BTCUSDT",
            "open": 35000.0,
            "high": 35500.0,
            "low": 34800.0,
            "close": 0.0,
            "volume": 150.5,
        }
        assert validate_event(event) is False

    def test_negative_volume(self):
        """Un volume négatif est invalide."""
        event = {
            "symbol": "BTCUSDT",
            "open": 35000.0,
            "high": 35500.0,
            "low": 34800.0,
            "close": 35200.0,
            "volume": -10.0,
        }
        assert validate_event(event) is False

    def test_high_less_than_low(self):
        """High < Low est invalide."""
        event = {
            "symbol": "BTCUSDT",
            "open": 35000.0,
            "high": 34000.0,
            "low": 35000.0,
            "close": 35200.0,
            "volume": 150.5,
        }
        assert validate_event(event) is False

    def test_event_json_serializable(self):
        """Un événement valide doit être sérialisable en JSON."""
        event = {
            "symbol": "ETHUSDT",
            "open_time": 1700000000000,
            "open": 2000.0,
            "high": 2050.0,
            "low": 1990.0,
            "close": 2030.0,
            "volume": 5000.0,
            "quote_volume": 10150000.0,
            "trade_count": 3500,
            "collected_at": datetime.now(timezone.utc).isoformat(),
        }
        # Ne doit pas lever d'exception
        json_str = json.dumps(event)
        parsed = json.loads(json_str)
        assert parsed["symbol"] == "ETHUSDT"
        assert isinstance(parsed["open"], float)


class TestFetchKline:
    """Tests pour la récupération des klines (avec mocks)."""

    @patch("producer.requests.get")
    def test_successful_fetch(self, mock_get):
        """Un appel API réussi retourne un événement valide."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            [
                1700000000000,  # open_time
                "35000.00",     # open
                "35500.00",     # high
                "34800.00",     # low
                "35200.00",     # close
                "150.50",       # volume
                1700000060000,  # close_time
                "5280000.00",   # quote_volume
                1200,           # trade_count
                "100.25",       # taker_buy_base
                "3520000.00",   # taker_buy_quote
                "0",            # ignore
            ]
        ]
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        result = fetch_kline("BTCUSDT")
        assert result is not None
        assert result["symbol"] == "BTCUSDT"
        assert result["open"] == 35000.0
        assert result["close"] == 35200.0
        assert result["volume"] == 150.5
        assert "collected_at" in result

    @patch("producer.requests.get")
    def test_api_timeout(self, mock_get):
        """Un timeout API retourne None."""
        import requests
        mock_get.side_effect = requests.exceptions.Timeout("Connection timed out")

        result = fetch_kline("BTCUSDT")
        assert result is None

    @patch("producer.requests.get")
    def test_api_connection_error(self, mock_get):
        """Une erreur de connexion retourne None."""
        import requests
        mock_get.side_effect = requests.exceptions.ConnectionError("DNS resolution failed")

        result = fetch_kline("BTCUSDT")
        assert result is None

    @patch("producer.requests.get")
    def test_api_http_error(self, mock_get):
        """Une erreur HTTP (ex: 429, 500) retourne None."""
        import requests
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("429 Too Many Requests")
        mock_get.return_value = mock_response

        result = fetch_kline("BTCUSDT")
        assert result is None

    @patch("producer.requests.get")
    def test_empty_api_response(self, mock_get):
        """Une réponse vide retourne None."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = []
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        result = fetch_kline("BTCUSDT")
        assert result is None

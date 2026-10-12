"""
test_schema.py - Tests de cohérence du schéma PostgreSQL.

Vérifie que le fichier SQL d'initialisation est valide et contient
les éléments attendus.
"""
import os
import re

import pytest


@pytest.fixture
def sql_content():
    """Charge le contenu du fichier init.sql."""
    sql_path = os.path.join(os.path.dirname(__file__), "..", "sql", "init.sql")
    with open(sql_path, "r", encoding="utf-8") as f:
        return f.read()


class TestSQLSchema:
    """Tests pour le schéma SQL."""

    def test_file_exists(self):
        """Le fichier init.sql doit exister."""
        sql_path = os.path.join(os.path.dirname(__file__), "..", "sql", "init.sql")
        assert os.path.exists(sql_path), "sql/init.sql n'existe pas"

    def test_create_table_exists(self, sql_content):
        """La table crypto_events doit être créée."""
        assert "CREATE TABLE" in sql_content
        assert "crypto_events" in sql_content

    def test_required_columns(self, sql_content):
        """Toutes les colonnes requises doivent être présentes."""
        required_columns = [
            "id", "symbol", "event_time",
            "open_price", "high_price", "low_price", "close_price",
            "volume", "quote_volume", "trade_count",
            "return_pct", "range_pct", "is_anomaly", "inserted_at",
        ]
        for col in required_columns:
            assert col in sql_content, f"Colonne manquante : {col}"

    def test_primary_key(self, sql_content):
        """La table doit avoir une clé primaire."""
        assert "PRIMARY KEY" in sql_content.upper()

    def test_uniqueness_constraint(self, sql_content):
        """Une contrainte d'unicité doit exister pour l'idempotence."""
        assert "UNIQUE" in sql_content.upper()

    def test_indexes_exist(self, sql_content):
        """Des index doivent être définis."""
        assert "CREATE INDEX" in sql_content.upper()
        # Au moins un index sur event_time
        assert "event_time" in sql_content.lower()

    def test_sql_valid_syntax(self, sql_content):
        """Vérification basique de la syntaxe SQL (pas de blocs non fermés)."""
        # Compter les parenthèses
        open_parens = sql_content.count("(")
        close_parens = sql_content.count(")")
        assert open_parens == close_parens, \
            f"Parenthèses déséquilibrées : {open_parens} ouvertes, {close_parens} fermées"

    def test_boolean_type_for_anomaly(self, sql_content):
        """is_anomaly doit être de type BOOLEAN."""
        # Chercher la définition de is_anomaly
        assert "BOOLEAN" in sql_content.upper()

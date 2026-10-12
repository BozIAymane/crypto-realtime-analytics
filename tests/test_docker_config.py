"""
test_docker_config.py - Tests de validation des configurations Docker et Grafana.

Vérifie la syntaxe du docker-compose.yml, du Dockerfile et du dashboard JSON.
"""
import json
import os

import pytest


PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..")


class TestDockerCompose:
    """Tests pour docker-compose.yml."""

    def test_file_exists(self):
        """docker-compose.yml doit exister."""
        path = os.path.join(PROJECT_ROOT, "docker-compose.yml")
        assert os.path.exists(path), "docker-compose.yml n'existe pas"

    def test_valid_yaml(self):
        """docker-compose.yml doit être un YAML valide."""
        try:
            import yaml
        except ImportError:
            pytest.skip("PyYAML non installé")

        path = os.path.join(PROJECT_ROOT, "docker-compose.yml")
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        assert "services" in data
        assert isinstance(data["services"], dict)

    def test_required_services(self):
        """Les services essentiels doivent être définis."""
        try:
            import yaml
        except ImportError:
            pytest.skip("PyYAML non installé")

        path = os.path.join(PROJECT_ROOT, "docker-compose.yml")
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        required = ["kafka", "postgres", "train-model", "producer", "spark-stream", "grafana"]
        for svc in required:
            assert svc in data["services"], f"Service manquant : {svc}"


class TestDockerfile:
    """Tests pour le Dockerfile."""

    def test_file_exists(self):
        """Dockerfile doit exister."""
        path = os.path.join(PROJECT_ROOT, "Dockerfile")
        assert os.path.exists(path), "Dockerfile n'existe pas"

    def test_python_base_image(self):
        """Le Dockerfile doit utiliser Python 3.11."""
        path = os.path.join(PROJECT_ROOT, "Dockerfile")
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        assert "python:3.11" in content, "L'image de base n'est pas Python 3.11"

    def test_java_installed(self):
        """Le Dockerfile doit installer Java."""
        path = os.path.join(PROJECT_ROOT, "Dockerfile")
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        assert "openjdk" in content, "Java n'est pas installé dans le Dockerfile"


class TestGrafanaDashboard:
    """Tests pour le dashboard Grafana."""

    def test_dashboard_file_exists(self):
        """Le fichier JSON du dashboard doit exister."""
        path = os.path.join(PROJECT_ROOT, "grafana", "dashboards", "crypto-dashboard.json")
        assert os.path.exists(path), "Dashboard Grafana n'existe pas"

    def test_valid_json(self):
        """Le dashboard doit être un JSON valide."""
        path = os.path.join(PROJECT_ROOT, "grafana", "dashboards", "crypto-dashboard.json")
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert "panels" in data
        assert "title" in data

    def test_dashboard_title(self):
        """Le dashboard doit avoir le bon titre."""
        path = os.path.join(PROJECT_ROOT, "grafana", "dashboards", "crypto-dashboard.json")
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert "Crypto Real-Time Analytics" in data["title"]

    def test_minimum_panels(self):
        """Le dashboard doit contenir au moins 7 panels."""
        path = os.path.join(PROJECT_ROOT, "grafana", "dashboards", "crypto-dashboard.json")
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert len(data["panels"]) >= 7, \
            f"Nombre de panels insuffisant : {len(data['panels'])}"

    def test_auto_refresh(self):
        """Le dashboard doit avoir un auto-refresh de 5 à 10 secondes."""
        path = os.path.join(PROJECT_ROOT, "grafana", "dashboards", "crypto-dashboard.json")
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert data.get("refresh") in ["5s", "10s"], \
            f"Auto-refresh invalide : {data.get('refresh')}"

    def test_datasource_provisioning_exists(self):
        """Le fichier de provisioning du datasource doit exister."""
        path = os.path.join(PROJECT_ROOT, "grafana", "provisioning", "datasources", "datasource.yml")
        assert os.path.exists(path)

    def test_dashboard_provisioning_exists(self):
        """Le fichier de provisioning du dashboard doit exister."""
        path = os.path.join(PROJECT_ROOT, "grafana", "provisioning", "dashboards", "dashboard.yml")
        assert os.path.exists(path)

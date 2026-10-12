-- =============================================================
-- init.sql - Initialisation de la base de données PostgreSQL
-- Crypto Real-Time Analytics - TP Big Data
-- =============================================================

-- Création de la table principale
CREATE TABLE IF NOT EXISTS crypto_events (
    id              BIGSERIAL       PRIMARY KEY,
    symbol          VARCHAR(20)     NOT NULL,
    event_time      TIMESTAMP       NOT NULL,
    open_price      DOUBLE PRECISION NOT NULL,
    high_price      DOUBLE PRECISION NOT NULL,
    low_price       DOUBLE PRECISION NOT NULL,
    close_price     DOUBLE PRECISION NOT NULL,
    volume          DOUBLE PRECISION NOT NULL DEFAULT 0,
    quote_volume    DOUBLE PRECISION DEFAULT 0,
    trade_count     INTEGER          DEFAULT 0,
    return_pct      DOUBLE PRECISION DEFAULT 0,
    range_pct       DOUBLE PRECISION DEFAULT 0,
    is_anomaly      BOOLEAN          DEFAULT FALSE,
    inserted_at     TIMESTAMP        DEFAULT CURRENT_TIMESTAMP,

    -- Contrainte d'unicité pour l'idempotence :
    -- même symbole + même bougie = pas de doublon
    CONSTRAINT uq_symbol_event_time UNIQUE (symbol, event_time)
);

-- Index pour les requêtes temporelles (dashboard Grafana)
CREATE INDEX IF NOT EXISTS idx_event_time ON crypto_events (event_time DESC);

-- Index pour le filtrage par symbole
CREATE INDEX IF NOT EXISTS idx_symbol ON crypto_events (symbol);

-- Index composite pour les requêtes Grafana typiques
CREATE INDEX IF NOT EXISTS idx_symbol_time ON crypto_events (symbol, event_time DESC);

-- Index pour les anomalies
CREATE INDEX IF NOT EXISTS idx_anomaly ON crypto_events (is_anomaly) WHERE is_anomaly = TRUE;

-- Message de confirmation
DO $$
BEGIN
    RAISE NOTICE 'Base de données initialisée avec succès.';
END $$;

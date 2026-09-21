-- TimescaleDB 初始化: 载荷谱与疲劳损伤两张超表
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS load_samples (
    time        TIMESTAMPTZ      NOT NULL,
    rope_id     TEXT             NOT NULL,
    load_kn     DOUBLE PRECISION NOT NULL,
    cycles      BIGINT           NOT NULL
);
SELECT create_hypertable('load_samples', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_load_samples_rope ON load_samples (rope_id, time DESC);

CREATE TABLE IF NOT EXISTS damage_snapshots (
    time              TIMESTAMPTZ      NOT NULL,
    rope_id           TEXT             NOT NULL,
    damage_fraction   DOUBLE PRECISION NOT NULL,
    total_cycles      BIGINT           NOT NULL,
    remaining_life_pct DOUBLE PRECISION NOT NULL,
    estimated_cycles  DOUBLE PRECISION NOT NULL
);
SELECT create_hypertable('damage_snapshots', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_damage_rope ON damage_snapshots (rope_id, time DESC);

-- 数据保留策略: 原始采样保留 90 天
SELECT add_retention_policy('load_samples', INTERVAL '90 days', if_not_exists => TRUE);
SELECT add_retention_policy('damage_snapshots', INTERVAL '90 days', if_not_exists => TRUE);

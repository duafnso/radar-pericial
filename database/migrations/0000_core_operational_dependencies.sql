-- Dependencias operacionais minimas para migracoes incrementais.

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS execucoes_coleta (
    id SERIAL PRIMARY KEY,
    fonte TEXT NOT NULL,
    tarefa TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'running',
    parametros JSONB,
    registros_coletados INT DEFAULT 0,
    registros_salvos INT DEFAULT 0,
    erro TEXT,
    iniciado_em TIMESTAMPTZ DEFAULT NOW(),
    finalizado_em TIMESTAMPTZ,
    duracao_segundos NUMERIC
);

CREATE INDEX IF NOT EXISTS idx_exec_coleta_fonte
    ON execucoes_coleta(fonte, iniciado_em DESC);

CREATE INDEX IF NOT EXISTS idx_exec_coleta_status
    ON execucoes_coleta(status, iniciado_em DESC);

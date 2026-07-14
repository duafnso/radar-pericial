-- Metricas detalhadas de coletas por fonte/classe.
-- Idempotente para bancos que ja receberam o schema via database/db.py.

CREATE TABLE IF NOT EXISTS metricas_coleta_classe (
    id SERIAL PRIMARY KEY,
    execucao_id INT REFERENCES execucoes_coleta(id) ON DELETE CASCADE,
    fonte TEXT NOT NULL,
    chave TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'success',
    registros_coletados INT DEFAULT 0,
    registros_salvos INT DEFAULT 0,
    descartados_sem_cnj INT DEFAULT 0,
    duplicados INT DEFAULT 0,
    erro TEXT,
    criado_em TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_metricas_coleta_execucao
    ON metricas_coleta_classe(execucao_id, chave);

CREATE INDEX IF NOT EXISTS idx_metricas_coleta_fonte
    ON metricas_coleta_classe(fonte, criado_em DESC);

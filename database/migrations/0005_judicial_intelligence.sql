-- Judicial data, administrative events and pericial intelligence.
-- Depends on processos from 0004_tracking_alerts.

CREATE TABLE IF NOT EXISTS movimentacoes (
    id SERIAL PRIMARY KEY,
    processo_id INT REFERENCES processos(id) ON DELETE CASCADE,
    data_movimentacao DATE,
    descricao TEXT,
    fonte TEXT,
    score_evento INT DEFAULT 0,
    criado_em TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_mov_proc ON movimentacoes(processo_id);

DELETE FROM movimentacoes a
USING movimentacoes b
WHERE a.id > b.id
  AND a.processo_id = b.processo_id
  AND COALESCE(a.data_movimentacao, DATE '0001-01-01') =
      COALESCE(b.data_movimentacao, DATE '0001-01-01')
  AND COALESCE(a.descricao, '') = COALESCE(b.descricao, '');

CREATE UNIQUE INDEX IF NOT EXISTS ux_mov_unique_business
    ON movimentacoes (
        processo_id,
        (COALESCE(data_movimentacao, DATE '0001-01-01')),
        md5(COALESCE(descricao, ''))
    );

CREATE TABLE IF NOT EXISTS publicacoes (
    id SERIAL PRIMARY KEY,
    processo_id INT REFERENCES processos(id) ON DELETE SET NULL,
    data_publicacao DATE,
    texto TEXT,
    tipo_publicacao TEXT,
    palavras_detectadas TEXT,
    orgao_origem TEXT,
    fonte TEXT,
    url TEXT,
    criado_em TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pub_proc ON publicacoes(processo_id);

CREATE TABLE IF NOT EXISTS eventos_administrativos (
    id SERIAL PRIMARY KEY,
    orgao TEXT,
    data_evento DATE,
    municipio TEXT,
    estado TEXT DEFAULT 'MT',
    descricao TEXT,
    categoria TEXT,
    score_evento INT DEFAULT 0,
    fonte TEXT,
    url TEXT,
    area_ha NUMERIC,
    criado_em TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ev_mun ON eventos_administrativos(municipio);

CREATE TABLE IF NOT EXISTS score_pericial (
    id SERIAL PRIMARY KEY,
    processo_id INT REFERENCES processos(id) ON DELETE CASCADE,
    score_total INT DEFAULT 0,
    score_classe INT DEFAULT 0,
    score_assunto INT DEFAULT 0,
    score_movimentacao INT DEFAULT 0,
    score_publicacao INT DEFAULT 0,
    score_administrativo INT DEFAULT 0,
    faixa_probabilidade TEXT DEFAULT 'frio',
    faixa_label TEXT DEFAULT 'Frio',
    tipo_pericia_sugerida TEXT,
    categorias_detectadas TEXT,
    urgencia TEXT DEFAULT 'baixa',
    calculado_em TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_score_proc ON score_pericial(processo_id);
CREATE INDEX IF NOT EXISTS idx_score_total ON score_pericial(score_total DESC);
CREATE INDEX IF NOT EXISTS idx_score_faixa ON score_pericial(faixa_probabilidade);

CREATE TABLE IF NOT EXISTS portarias_diario_oficial (
    id SERIAL PRIMARY KEY,
    titulo TEXT,
    resumo TEXT,
    data_publicacao TEXT,
    municipio TEXT,
    area_ha NUMERIC,
    fonte TEXT,
    orgao TEXT,
    url TEXT,
    categoria_agronomica TEXT,
    score_evento INT DEFAULT 0,
    faixa_probabilidade TEXT DEFAULT 'frio',
    coletado_em TIMESTAMPTZ DEFAULT NOW()
);

DELETE FROM portarias_diario_oficial a
USING portarias_diario_oficial b
WHERE a.id > b.id
  AND COALESCE(a.titulo, '') = COALESCE(b.titulo, '')
  AND COALESCE(a.data_publicacao, '') = COALESCE(b.data_publicacao, '')
  AND COALESCE(a.fonte, '') = COALESCE(b.fonte, '');

CREATE UNIQUE INDEX IF NOT EXISTS ux_portarias_unique_business
    ON portarias_diario_oficial (
        md5(COALESCE(titulo, '')),
        (COALESCE(data_publicacao, '')),
        (COALESCE(fonte, ''))
    );

CREATE TABLE IF NOT EXISTS data_lake_raw (
    id SERIAL PRIMARY KEY,
    fonte TEXT,
    tipo TEXT,
    payload JSONB,
    processado BOOLEAN DEFAULT FALSE,
    coletado_em TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_proc ON data_lake_raw(processado);

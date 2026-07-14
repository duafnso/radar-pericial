-- Usuarios, sessoes e auditoria administrativa.

CREATE TABLE IF NOT EXISTS usuarios (
    id SERIAL PRIMARY KEY,
    username TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'user',
    ativo BOOLEAN NOT NULL DEFAULT TRUE,
    regiao_foco TEXT,
    criado_em TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_usuarios_role ON usuarios(role);
CREATE INDEX IF NOT EXISTS idx_usuarios_ativo ON usuarios(ativo);

CREATE TABLE IF NOT EXISTS user_sessions (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
    token_hash TEXT UNIQUE NOT NULL,
    criado_em TIMESTAMPTZ DEFAULT NOW(),
    expira_em TIMESTAMPTZ NOT NULL,
    revogado_em TIMESTAMPTZ,
    ultimo_uso_em TIMESTAMPTZ,
    user_agent TEXT,
    client_ip TEXT
);

CREATE INDEX IF NOT EXISTS idx_sessions_user ON user_sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_sessions_exp ON user_sessions(expira_em);

CREATE TABLE IF NOT EXISTS auditoria_eventos (
    id SERIAL PRIMARY KEY,
    ator_user_id INT REFERENCES usuarios(id) ON DELETE SET NULL,
    ator_username TEXT,
    acao TEXT NOT NULL,
    entidade TEXT,
    entidade_id TEXT,
    detalhes JSONB,
    ip TEXT,
    user_agent TEXT,
    criado_em TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_auditoria_eventos_criado
    ON auditoria_eventos(criado_em DESC);
CREATE INDEX IF NOT EXISTS idx_auditoria_eventos_ator
    ON auditoria_eventos(ator_user_id, criado_em DESC);
CREATE INDEX IF NOT EXISTS idx_auditoria_eventos_acao
    ON auditoria_eventos(acao, criado_em DESC);

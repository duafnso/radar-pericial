-- Acompanhamento de processos e alertas de usuario.
-- Depende de usuarios e da tabela processos criada pelo schema legado.
-- Em bancos novos, aplique depois que processos existir ou mantenha via database/db.py.

CREATE TABLE IF NOT EXISTS processos (
    id SERIAL PRIMARY KEY,
    numero_cnj TEXT UNIQUE,
    tribunal TEXT,
    comarca TEXT,
    vara TEXT,
    classe_processual TEXT,
    assunto_principal TEXT,
    data_distribuicao DATE,
    fase_atual TEXT,
    origem TEXT,
    municipio TEXT,
    regiao_imea TEXT,
    ativo BOOLEAN DEFAULT TRUE,
    criado_em TIMESTAMPTZ DEFAULT NOW(),
    atualizado_em TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_proc_cnj ON processos(numero_cnj);
CREATE INDEX IF NOT EXISTS idx_proc_mun ON processos(municipio);
CREATE TABLE IF NOT EXISTS processos_acompanhados (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
    processo_id INT NOT NULL REFERENCES processos(id) ON DELETE CASCADE,
    ativo BOOLEAN NOT NULL DEFAULT TRUE,
    criado_em TIMESTAMPTZ DEFAULT NOW(),
    atualizado_em TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(user_id, processo_id)
);

CREATE INDEX IF NOT EXISTS idx_proc_acomp_user
    ON processos_acompanhados(user_id, ativo, criado_em DESC);

CREATE TABLE IF NOT EXISTS alertas_usuario (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
    processo_id INT REFERENCES processos(id) ON DELETE CASCADE,
    tipo TEXT NOT NULL DEFAULT 'processo',
    titulo TEXT NOT NULL,
    mensagem TEXT,
    lido BOOLEAN NOT NULL DEFAULT FALSE,
    criado_em TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alertas_usuario_user
    ON alertas_usuario(user_id, lido, criado_em DESC);


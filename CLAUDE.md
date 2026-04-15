# Radar Pericial — Contexto para Claude Code

## O que é este projeto
Sistema de inteligência judicial e geoespacial para **peritos agrônomos no Mato Grosso**.
Monitora processos judiciais (DataJud/CNJ), eventos administrativos (DOU, DNIT, SINFRA-MT)
e dados geoespaciais (SIGEF/INCRA, INPE PRODES/DETER) para identificar oportunidades de perícia.

**Stack:** FastAPI + Celery + PostgreSQL/PostGIS + Redis | Docker Compose

## Estrutura
```
api/main.py           — FastAPI: endpoints REST + serve HTML
alerts/scheduler.py   — Celery Beat: agendamento das coletas
collector/            — Coletores por fonte (judicial, admin, geoespacial)
etl/geospatial_etl.py — Pipeline ETL: limpeza, CRS, clip MT
intelligence/taxonomy.py — Motor de score pericial (0–100)
database/db.py        — Acesso ao banco (SQLAlchemy + PostGIS)
interface/templates/  — Frontend HTML (Leaflet + vanilla JS)
```

## Comandos úteis
```bash
docker compose up -d              # sobe todos os serviços
docker compose logs -f web        # acompanha a API
docker compose exec web python -c "from database.db import Database; Database()"  # testa schema
```

## Problemas conhecidos a corrigir (prioridade alta)

### 1. Autenticação quebrada — api/main.py
O token UUID gerado no `/api/login` nunca é validado nos outros endpoints.
- Implementar validação real: armazenar tokens na tabela `usuarios` ou usar JWT (python-jose)
- Proteger com dependency injection (`Depends(get_current_user)`) todos os endpoints sensíveis

### 2. Database() instanciado em toda requisição — api/main.py + database/db.py
A função `db()` cria um novo `Database()` a cada chamada, e o construtor executa `_init_schema` completo.
- Criar engine como singleton (module-level) fora da classe
- Chamar `_init_schema` apenas na inicialização da aplicação (lifespan do FastAPI)
- Usar pool de conexões via `create_engine(..., pool_size=5, max_overflow=10)`

### 3. Dados geoespaciais destruídos a cada coleta — alerts/scheduler.py + database/db.py
`save_all_layers` usa `if_exists="replace"` — apaga tudo a cada 12h.
- Mudar para `if_exists="append"` com deduplicação por `codigo_imovel` no SIGEF
- Ou manter tabelas de staging e fazer UPSERT

### 4. Hash de senha sem salt — database/db.py
`hashlib.sha256` puro é vulnerável a rainbow tables.
- Substituir por `passlib[bcrypt]` ou `argon2-cffi`

### 5. Portarias duplicadas — database/db.py
`save_portarias` faz append sem checar duplicatas.
- Adicionar deduplicação por `(titulo, data_publicacao, fonte)` antes do insert

### 6. `fetch_assentamentos` sem geometria — collector/multi_source_collector.py
Retorna DataFrame comum sem coluna `geometry` — falha no ETL geoespacial.
- Verificar se a API retorna campos `lat/lon` e construir geometria de ponto, ou tratar como tabela não-geoespacial no ETL

### 7. ETL usa municípios crus como referência — alerts/scheduler.py
`run_etl(raw, municipios=raw.get("municipios_mt"))` passa o GDF antes da limpeza.
- Limpar `municipios_mt` primeiro, depois passar para o ETL dos demais layers

### 8. Movimentações duplicadas — alerts/scheduler.py
`save_movimentacao` insere sem checar se a movimentação já existe para o processo.
- Adicionar constraint `UNIQUE(processo_id, data_movimentacao, descricao)` ou checar antes de inserir

## Convenções do projeto
- Python 3.11+, type hints onde possível
- Logs com `logger = logging.getLogger(__name__)` — não usar `print()`
- Variáveis de ambiente via `.env` + `python-dotenv`
- Todo SQL via SQLAlchemy `text()` com parâmetros nomeados (nunca f-string com input externo)
- Dados pessoais removidos antes de persistir (LGPD) — ver `COLUNAS_PESSOAIS` no ETL

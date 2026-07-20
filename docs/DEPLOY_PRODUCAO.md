# Deploy de Producao - Radar Pericial

Este guia descreve o caminho recomendado para executar o Radar Pericial em
producao mantendo Docker para a aplicacao, mas usando PostgreSQL/PostGIS e
Redis como servicos externos ou gerenciados.

## Arquivos relevantes

- `docker-compose.yml`: ambiente local de desenvolvimento com PostGIS e Redis.
- `docker-compose.prod.yml`: ambiente de producao com containers `web`,
  `worker` e `beat`.
- `.env.example`: modelo seguro de variaveis de ambiente.
- `.dockerignore`: impede envio de segredos, caches e artefatos para o build.
- `frontend/`: aplicacao React/Vite/TypeScript.
- `interface/templates/index.html` e `interface/static/assets/`: frontend
  compilado e servido pelo FastAPI.

## Build da aplicacao

O Dockerfile executa o build completo do frontend em um estagio separado com
Node.js:

1. instala dependencias com `npm ci`;
2. executa `npm run frontend:build`;
3. sincroniza o resultado para `interface/templates/index.html` e
   `interface/static/assets`;
4. copia apenas backend Python e assets compilados para a imagem final.

Com isso, uma maquina limpa nao precisa executar build manual antes do Docker.
Use:

```bash
docker compose up -d --build
```

Para desenvolvimento local do frontend:

```bash
npm run frontend:dev
```

Para gerar os assets que o FastAPI serve localmente:

```bash
npm run frontend:build
```

## Servicos em producao

O ambiente de producao deve executar tres processos separados:

| Servico | Funcao | Porta publica |
| --- | --- | --- |
| `web` | FastAPI, endpoints REST, healthcheck e frontend | Sim |
| `worker` | Celery worker para coletas, ETL, score e alertas | Nao |
| `beat` | Celery Beat para agendamento periodico | Nao |

O banco PostgreSQL/PostGIS e o Redis devem ser externos ao compose de producao,
preferencialmente gerenciados pela plataforma de hospedagem.

## Variaveis obrigatorias

Em producao, defina obrigatoriamente:

```env
APP_ENV=production
DATABASE_URL=postgresql://...
REDIS_URL=redis://...
SECRET_KEY=...
SESSION_TOKEN_PEPPER=...
CORS_ALLOW_ORIGINS=https://seu-dominio.com.br
```

Nao use `*` em `CORS_ALLOW_ORIGINS` em producao. A aplicacao falha no startup
se essa configuracao estiver insegura.

## Variaveis recomendadas

```env
ENABLE_API_DOCS=false
LOAD_DEMO_DATA=false
LOG_LEVEL=info
WEB_WORKERS=2
WORKER_CONCURRENCY=2
WORKER_TIME_LIMIT=3600
```

## Fontes de dados

Habilite apenas as fontes que serao operadas no ambiente inicial:

```env
DATAJUD_API_KEY=
ENABLE_SOURCE_DATAJUD=true
ENABLE_SOURCE_DJE=false
ENABLE_SOURCE_DOU=true
ENABLE_SOURCE_IOMAT=false
ENABLE_SOURCE_DNIT=true
ENABLE_SOURCE_SINFRA=false
ENABLE_SOURCE_SIGEF=false
ENABLE_SOURCE_PRODES=true
ENABLE_SOURCE_DETER=false
ENABLE_SOURCE_CAR=false
ALLOW_INSECURE_CAR_SSL=false
```

Fontes pesadas ou instaveis, como SIGEF, DETER e CAR, devem entrar somente
depois que o monitoramento de coletas estiver pronto.

Mantenha `ALLOW_INSECURE_CAR_SSL=false` em producao. Use `true` apenas em
homologacao controlada, se o endpoint publico do CAR apresentar problema de
certificado e a fonte estiver explicitamente habilitada.

## Comando de subida

Com as variaveis exportadas no ambiente ou definidas pela plataforma:

```bash
docker compose -f docker-compose.prod.yml up -d --build
```

Para acompanhar logs:

```bash
docker compose -f docker-compose.prod.yml logs -f web
docker compose -f docker-compose.prod.yml logs -f worker
docker compose -f docker-compose.prod.yml logs -f beat
```

## Healthchecks

Use:

- `/health` para liveness publico.
- `/health/ready` para readiness com banco, Redis e Celery.
- `/api/health` apenas para validar sessao autenticada.
- `/api/coletas/resumo` para validar a saude operacional das coletas.
- `/api/qualidade/processos` para validar a qualidade tecnica dos dados coletados.

O healthcheck do container `web` usa `/health`, porque `/api/health` exige
token.

## Smoke test autenticado

Depois do deploy, valide em ordem:

```bash
curl http://localhost:8000/health
curl http://localhost:8000/health/ready
curl http://localhost:8000/
```

Em seguida faca login com o usuario admin inicial e use o token Bearer para:

```bash
curl -H "Authorization: Bearer <token>" http://localhost:8000/api/me
curl -H "Authorization: Bearer <token>" http://localhost:8000/api/stats
curl -H "Authorization: Bearer <token>" "http://localhost:8000/api/processos?limit=5"
curl -H "Authorization: Bearer <token>" "http://localhost:8000/api/coletas/status?limit=5"
curl -H "Authorization: Bearer <token>" http://localhost:8000/api/coletas/resumo
curl -H "Authorization: Bearer <token>" http://localhost:8000/api/qualidade/processos
```

O endpoint `/api/coletas/resumo` deve retornar um resumo por fonte com ultimo
status, ultima execucao, registros salvos, falha resumida e indicador de tarefa
em execucao.

Tambem existe um smoke test automatizado:

```bash
RADAR_SMOKE_USER='admin' RADAR_SMOKE_PASSWORD='senha-admin' python tools/smoke_test.py --base-url http://localhost:8000
```

O usuario pode ser informado por `RADAR_SMOKE_USER` ou `RADAR_SMOKE_USERNAME`.

Ele valida health, readiness, HTML do frontend, asset estatico, login,
`/api/me`, `/api/stats`, `/api/processos`, `/api/coletas/status`,
`/api/coletas/resumo` e `/api/qualidade/processos`.


## Backup e restore

Antes de aplicar migracoes em homologacao ou producao, gere um backup:

```bash
python tools/backup_db.py --output-dir backups
```

Para testar restauracao em ambiente local ou homologacao:

```bash
python tools/restore_db.py backups/arquivo.dump
```

Detalhes completos ficam em `docs/BACKUP_RESTORE.md`.

## Checklist antes de publicar

Use tambem `docs/CHECKLIST_HOMOLOGACAO_PRODUCAO.md` como checklist operacional completo.


- `.env` e `.env.txt` removidos do Git.
- Chaves expostas rotacionadas.
- `SECRET_KEY` e `SESSION_TOKEN_PEPPER` fortes e diferentes.
- `CORS_ALLOW_ORIGINS` com dominio real.
- Banco PostGIS com backup automatico.
- Redis sem porta publica aberta.
- Retencao de logs definida na plataforma.
- Dominio e HTTPS configurados.
- `ENABLE_API_DOCS=false`, salvo em staging controlado.
- Primeiro usuario admin criado por `DEFAULT_ADMIN_PASSWORD` temporario.
- Senha admin trocada depois do primeiro login.
- Teste de coleta judicial executado com a chave DataJud real.
- Logs de `web`, `worker` e `beat` verificados.

## Mapa territorial

Leaflet e seu CSS sao compilados no bundle Vite local; o mapa nao depende de CDN em tempo de execucao.

### Endpoint agregado

`GET /api/processos/mapa/resumo` exige autenticacao e aceita os filtros do mapa, inclusive `limit_cidades`.
O endpoint agrega por municipio antes de limitar a lista de cidades.
A resposta inclui `total_processos`, `total_municipios`, `sem_localizacao` e `items`.
`limit_cidades` limita municipios retornados, nunca processos antes da agregacao.

Em uma validacao local de 2026-07-20, endpoint e PostGIS retornaram 702 processos localizados, 73 municipios e 27 processos sem localizacao.

### Tiles e atribuicao

Configure o provedor por variaveis de build:

```env
VITE_MAP_TILE_URL=https://tiles.seu-provedor.example/{z}/{x}/{y}.png
VITE_MAP_TILE_ATTRIBUTION=Texto ou HTML de atribuicao exigido pelo provedor
```

Em producao, configure as duas variaveis com um provedor comercial aprovado.
A atribuicao deve permanecer visivel no controle do mapa e e obrigatoria.
OpenStreetMap e apenas o fallback de desenvolvimento local; nao o use como provedor comercial sem revisao de termos, capacidade e politica de uso.

### Operacao do mapa

1. Depois do deploy, execute o smoke autenticado; ele inclui `/api/processos/mapa/resumo?limit_cidades=200`.
2. Compare os tres totais retornados pelo endpoint com uma consulta PostGIS atual, sem fixar os numeros historicos.
3. Revise os logs de `web` sem incluir tokens, senhas ou variaveis de ambiente em tickets ou relatorios.
4. Verifique que os assets compilados servem Leaflet pelo bundle local e nao por `unpkg`.

### Checklist visual de homologacao

Para concluir a homologacao, validar em navegador real:
- 1440x900;
- 1024x768;
- 390x844.

Em cada viewport, conferir:
- basemap visivel e marcadores compactos;
- contadores coerentes com o endpoint;
- selecao municipal, detalhes e acompanhamento;
- aviso de falha de tiles sem perda dos dados;
- ausencia de poligonos ou limites territoriais;
- ausencia de sobreposicao ou corte de textos e controles.

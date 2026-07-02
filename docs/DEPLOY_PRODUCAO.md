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

O healthcheck do container `web` usa `/health`, porque `/api/health` exige
token.

## Checklist antes de publicar

- `.env` e `.env.txt` removidos do Git.
- Chaves expostas rotacionadas.
- `SECRET_KEY` e `SESSION_TOKEN_PEPPER` fortes e diferentes.
- `CORS_ALLOW_ORIGINS` com dominio real.
- Banco PostGIS com backup automatico.
- Redis sem porta publica aberta.
- `ENABLE_API_DOCS=false`, salvo em staging controlado.
- Primeiro usuario admin criado por `DEFAULT_ADMIN_PASSWORD` temporario.
- Senha admin trocada depois do primeiro login.
- Logs de `web`, `worker` e `beat` verificados.

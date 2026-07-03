# Operacao das Coletas Automaticas

As coletas do Radar Pericial sao executadas automaticamente por Celery Beat e
Celery Worker. O usuario final nao precisa disparar coletas manualmente.

## Agenda atual

| Tarefa | Fila | Frequencia | Funcao |
| --- | --- | --- | --- |
| `task_geo` | `geo` | A cada 12 horas | Coleta e processa camadas geoespaciais. |
| `task_judicial` | `judicial` | Diario, 06:00 | Coleta processos judiciais e movimentacoes. |
| `task_admin` | `admin` | Diario, 07:00 | Coleta eventos administrativos e portarias. |
| `task_score` | `default` | Diario, 08:00 | Recalcula scores dos processos salvos. |

## Registro de execucao

Cada tarefa registra uma linha em `execucoes_coleta` com:

- `fonte`
- `tarefa`
- `status`
- `parametros`
- `registros_coletados`
- `registros_salvos`
- `erro`
- `iniciado_em`
- `finalizado_em`
- `duracao_segundos`

Statuses esperados:

- `running`: tarefa iniciada e ainda nao finalizada.
- `success`: tarefa finalizada sem excecao.
- `failed`: tarefa falhou e sera reprocessada pelo retry do Celery, quando aplicavel.

## Consulta via API

Historico detalhado protegido:

```http
GET /api/coletas/status?limit=50
Authorization: Bearer <token>
```

Resposta:

```json
{
  "total": 1,
  "items": [
    {
      "id": 10,
      "fonte": "judicial",
      "tarefa": "task_judicial",
      "status": "success",
      "parametros": {"dias_atras": 1},
      "registros_coletados": 120,
      "registros_salvos": 118,
      "erro": "",
      "iniciado_em": "2026-07-01 06:00:00-04",
      "finalizado_em": "2026-07-01 06:03:20-04",
      "duracao_segundos": 200
    }
  ]
}
```

Resumo operacional protegido:

```http
GET /api/coletas/resumo
Authorization: Bearer <token>
```

Resposta esperada:

```json
{
  "total": 1,
  "items": [
    {
      "fonte": "judicial",
      "ultimo_status": "success",
      "ultima_tarefa": "task_judicial",
      "ultima_execucao": "2026-07-01 06:00:00-04",
      "ultimo_fim": "2026-07-01 06:03:20-04",
      "ultimos_coletados": 120,
      "registros_salvos": 118,
      "erro": "",
      "em_execucao": false,
      "total_execucoes": 30,
      "total_coletados": 4000,
      "total_salvos": 3900,
      "duracao_media_segundos": 210,
      "ultima_falha": "",
      "mensagem_operacional": ""
    }
  ]
}
```

Mensagens operacionais amigaveis sao geradas para:

- DataJud `429` ou `Too Many Requests`;
- DataJud `401`, `APIKey` ausente/invalida ou `unauthorized`;
- timeout da fonte externa;
- coleta em andamento;
- coleta concluida sem novos registros.

## Disparo manual

Endpoint protegido para usuarios com `role='admin'`:

```http
POST /api/coletas/{tipo}/executar
Authorization: Bearer <token-admin>
```

Tipos aceitos:

- `geo`
- `judicial`
- `admin`
- `score`

O endpoint apenas enfileira a tarefa no Celery. A execucao real acontece no
worker e aparece posteriormente em `/api/coletas/status`.

## Uso no produto

O dashboard consome `/api/coletas/resumo` para mostrar a saude operacional das
fontes sem precisar calcular tudo no cliente.

A tela de operacao continua consumindo `/api/coletas/status` para exibir:

- ultima coleta por fonte;
- tempo de duracao;
- status;
- quantidade coletada;
- quantidade salva;
- erro resumido;
- botoes de disparo manual para `geo`, `judicial`, `admin` e `score`.

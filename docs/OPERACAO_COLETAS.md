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

Endpoint protegido:

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

## Proximo passo de produto

O painel administrativo deve consumir `/api/coletas/status` para exibir:

- ultima coleta por fonte;
- tempo de duracao;
- status;
- quantidade coletada;
- quantidade salva;
- erro resumido;
- botao futuro para "executar agora".


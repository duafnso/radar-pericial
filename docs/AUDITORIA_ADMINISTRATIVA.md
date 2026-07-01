# Auditoria Administrativa

## Objetivo

A auditoria registra eventos sensiveis do sistema para rastreabilidade operacional e suporte a uso comercial.

Ela nao substitui logs de infraestrutura, mas cria uma trilha consultavel dentro do banco para acoes feitas por usuarios autenticados e eventos criticos de acesso.

## Estrutura de dados

Tabela: `auditoria_eventos`

Campos principais:

- `ator_user_id`: usuario responsavel pela acao, quando conhecido.
- `ator_username`: username do ator.
- `acao`: nome tecnico do evento.
- `entidade`: tipo de recurso afetado.
- `entidade_id`: identificador do recurso afetado.
- `detalhes`: JSONB com metadados nao sensiveis.
- `ip`: IP de origem informado pela requisicao.
- `user_agent`: agente do navegador/cliente.
- `criado_em`: data e hora do evento.

Indices criados:

- `idx_auditoria_eventos_criado`
- `idx_auditoria_eventos_ator`
- `idx_auditoria_eventos_acao`

## Eventos registrados

- `login_success`: login realizado com sucesso.
- `login_failed`: tentativa de login recusada.
- `logout`: encerramento de sessao.
- `coleta_manual_enfileirada`: admin disparou coleta manual.
- `usuario_criado`: admin criou novo usuario.
- `usuario_role_atualizada`: admin alterou papel de usuario.
- `usuario_status_atualizado`: admin ativou ou desativou usuario.
- `usuario_senha_redefinida`: admin redefiniu senha de usuario.

## Endpoint

`GET /api/admin/auditoria?limit=100`

Requer usuario com `role = admin`.

Retorno:

```json
{
  "total": 1,
  "items": [
    {
      "id": 1,
      "ator_user_id": 1,
      "ator_username": "admin",
      "acao": "usuario_criado",
      "entidade": "usuario",
      "entidade_id": "2",
      "detalhes": {"username": "operador", "role": "operator"},
      "ip": "127.0.0.1",
      "criado_em": "2026-07-01 12:00:00+00"
    }
  ]
}
```

## Interface

A tela `Auditoria` lista os 100 eventos mais recentes e mostra:

- data;
- ator;
- acao;
- entidade;
- ID afetado;
- IP;
- detalhes.

## Cuidados de seguranca

- Senhas e tokens nao sao gravados em auditoria.
- Falha ao gravar auditoria nao bloqueia a operacao principal, mas gera warning no log.
- O endpoint de consulta e restrito a admin.

## Proximas melhorias

- Filtros por acao, ator e periodo.
- Retencao configuravel por ambiente.
- Exportacao CSV para auditoria externa.
- Integracao com log centralizado em producao.

# Usuarios e Permissoes

O controle de acesso inicial do Radar Pericial usa um campo `role` na tabela
`usuarios`.

## Papeis atuais

| Role | Uso |
| --- | --- |
| `admin` | Administracao completa, usuarios, auditoria, operacao e consultas. |
| `operator` | Operacao do sistema: coletas, peritos e consultas operacionais. |
| `user` | Uso comum: consulta de dados e calculo de score. |
| `viewer` | Perfil somente leitura para dados comuns. |

## Matriz de permissoes

| Permissao | admin | operator | user | viewer |
| --- | --- | --- | --- | --- |
| `read_data` | sim | sim | sim | sim |
| `read_operational` | sim | sim | nao | nao |
| `calculate_score` | sim | sim | sim | nao |
| `create_perito` | sim | sim | nao | nao |
| `run_collections` | sim | sim | nao | nao |
| `manage_users` | sim | nao | nao | nao |
| `view_audit` | sim | nao | nao | nao |

## Usuario admin inicial

Quando `DEFAULT_ADMIN_PASSWORD` esta definido no ambiente, o startup cria ou
atualiza o usuario `admin` com `role='admin'`.

Em producao:

1. Defina `DEFAULT_ADMIN_PASSWORD` apenas para o primeiro deploy ou bootstrap.
2. Faca login com `admin`.
3. Troque a senha assim que existir tela/endpoint proprio para isso.
4. Remova `DEFAULT_ADMIN_PASSWORD` do ambiente para evitar reset involuntario.

## Endpoints protegidos por permissao

| Endpoint | Requisito |
| --- | --- |
| `GET /api/admin/usuarios` | `manage_users` |
| `POST /api/admin/usuarios` | `manage_users` |
| `PATCH /api/admin/usuarios/{user_id}/role` | `manage_users` |
| `PATCH /api/admin/usuarios/{user_id}/ativo` | `manage_users` |
| `PATCH /api/admin/usuarios/{user_id}/senha` | `manage_users` |
| `GET /api/admin/auditoria` | `view_audit` |
| `GET /api/coletas/status` | `read_operational` |
| `GET /api/coletas/resumo` | `read_operational` |
| `POST /api/coletas/{tipo}/executar` | `run_collections` |
| `POST /api/peritos` | `create_perito` |
| `GET /api/me` | usuario autenticado |
| `PATCH /api/me/senha` | usuario autenticado |

## Tela administrativa

A interface web possui a tela `Usuarios`, acessivel pela sidebar principal. Ela
permite:

- listar usuarios;
- criar usuarios;
- alterar role;
- ativar/desativar conta;
- redefinir senha.

Todas as acoes usam endpoints protegidos por token. Gestao de usuarios e
auditoria continuam restritas ao `admin`; operacao de coletas pode ser feita
por `admin` ou `operator`.

## Comportamento da interface

Ao fazer login, a API retorna o token e os dados basicos do usuario autenticado.
Quando a pagina e reaberta com token salvo, a interface consulta `GET /api/me`.

A UI usa a matriz de permissoes para:

- mostrar o perfil atual na sidebar;
- ocultar menu `Operacao de Coletas` para quem nao possui `read_operational`;
- ocultar menu `Usuarios` para quem nao possui `manage_users`;
- ocultar menu `Auditoria` para quem nao possui `view_audit`;
- ocultar botoes de disparo manual para quem nao possui `run_collections`;
- ocultar cadastro de peritos para quem nao possui `create_perito`.

Essa camada e apenas ergonomia e reducao de erro operacional. A regra definitiva
continua no backend, por meio das dependencias de permissao dos endpoints.

## Troca de senha pelo proprio usuario

Endpoint:

```http
PATCH /api/me/senha
Authorization: Bearer <token>
Content-Type: application/json
```

```json
{
  "senha_atual": "senha-atual",
  "nova_senha": "nova-senha-forte"
}
```

Regras:

- exige usuario autenticado;
- exige senha atual correta;
- exige nova senha com ao menos 8 caracteres;
- nova senha deve ser diferente da senha atual;
- revoga outras sessoes do mesmo usuario;
- mantem a sessao atual ativa;
- registra auditoria `usuario_senha_alterada`.

## Criar usuario

```http
POST /api/admin/usuarios
Authorization: Bearer <token-admin>
Content-Type: application/json
```

```json
{
  "username": "operador",
  "password": "senha-forte-aqui",
  "role": "operator",
  "regiao_foco": "Medio-Norte"
}
```

Roles aceitos:

- `admin`
- `user`
- `viewer`
- `operator`

## Regras de seguranca

- Usuarios inativos nao conseguem fazer login.
- Sessoes de usuarios desativados sao revogadas.
- Redefinir senha revoga sessoes ativas do usuario.
- O admin autenticado nao pode desativar a propria conta.
- O admin autenticado nao pode remover o proprio papel `admin`.

## Proximos passos

- Adicionar filtros por role/status na tela de usuarios.
- Exibir mensagens de permissao mais especificas por tela.

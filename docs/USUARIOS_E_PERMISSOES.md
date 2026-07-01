# Usuarios e Permissoes

O controle de acesso inicial do Radar Pericial usa um campo `role` na tabela
`usuarios`.

## Papeis atuais

| Role | Uso |
| --- | --- |
| `admin` | Pode acessar endpoints administrativos, incluindo disparo manual de coletas. |
| `user` | Pode acessar as telas e APIs autenticadas comuns. |

## Usuario admin inicial

Quando `DEFAULT_ADMIN_PASSWORD` esta definido no ambiente, o startup cria ou
atualiza o usuario `admin` com `role='admin'`.

Em producao:

1. Defina `DEFAULT_ADMIN_PASSWORD` apenas para o primeiro deploy ou bootstrap.
2. Faca login com `admin`.
3. Troque a senha assim que existir tela/endpoint proprio para isso.
4. Remova `DEFAULT_ADMIN_PASSWORD` do ambiente para evitar reset involuntario.

## Endpoints administrativos atuais

| Endpoint | Requisito |
| --- | --- |
| `GET /api/admin/usuarios` | `role='admin'` |
| `POST /api/admin/usuarios` | `role='admin'` |
| `PATCH /api/admin/usuarios/{user_id}/role` | `role='admin'` |
| `PATCH /api/admin/usuarios/{user_id}/ativo` | `role='admin'` |
| `PATCH /api/admin/usuarios/{user_id}/senha` | `role='admin'` |
| `POST /api/coletas/{tipo}/executar` | `role='admin'` |

## Tela administrativa

A interface web possui a tela `Usuarios`, acessivel pela barra superior. Ela
permite:

- listar usuarios;
- criar usuarios;
- alterar role;
- ativar/desativar conta;
- redefinir senha.

Todas as acoes usam os endpoints administrativos e exigem token com
`role='admin'`.

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

- Criar endpoint de troca de senha pelo proprio usuario.
- Adicionar filtros por role/status na tela de usuarios.
- Revisar permissoes finas para `operator` e `viewer`.

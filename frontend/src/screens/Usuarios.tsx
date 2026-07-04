import React from "react";
import { DataTable } from "../components/DataTable";
import { Page } from "../components/Page";
import type { ApiClient } from "../types";
import { shortDate } from "../utils/format";

export function Usuarios({ api, hasPermission, notify }: {
  api: ApiClient;
  hasPermission: (permission?: string) => boolean;
  notify: (message: string) => void;
}) {
  const [items, setItems] = React.useState<any[]>([]);

  async function load() {
    if (!hasPermission("manage_users")) return;
    const data = await api.get<any>("/api/admin/usuarios");
    setItems(data?.items || []);
  }

  React.useEffect(() => { load(); }, []);

  async function changeRole(id: number, role: string) {
    const result = await api.patch<any>(`/api/admin/usuarios/${id}/role`, { role });
    notify(result?.status === "ok" ? "Role atualizada." : "Não foi possível atualizar role.");
    load();
  }

  return (
    <Page title="Usuários e Permissões" subtitle={`${items.length} usuários cadastrados`} action={<button onClick={load}>Atualizar</button>}>
      <DataTable headers={["ID", "Usuário", "Role", "Status", "Região", "Criado em"]}>
        {items.map((item) => (
          <tr key={item.id}>
            <td>{item.id}</td>
            <td>{item.username}</td>
            <td>
              <select value={item.role} onChange={(event) => changeRole(item.id, event.target.value)}>
                {["admin", "operator", "user", "viewer"].map((role) => <option key={role}>{role}</option>)}
              </select>
            </td>
            <td>{item.ativo ? "ativo" : "inativo"}</td>
            <td>{item.regiao_foco || "--"}</td>
            <td>{shortDate(item.criado_em)}</td>
          </tr>
        ))}
      </DataTable>
    </Page>
  );
}

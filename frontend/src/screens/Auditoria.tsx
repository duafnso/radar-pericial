import React from "react";
import { DataTable } from "../components/DataTable";
import { Page } from "../components/Page";
import type { ApiClient } from "../types";
import { shortDate } from "../utils/format";

export function Auditoria({ api }: { api: ApiClient }) {
  const [items, setItems] = React.useState<any[]>([]);

  async function load() {
    const data = await api.get<any>("/api/admin/auditoria?limit=100");
    setItems(data?.items || []);
  }

  React.useEffect(() => { load(); }, []);

  return (
    <Page title="Auditoria Administrativa" subtitle={`${items.length} eventos recentes`} action={<button onClick={load}>Atualizar</button>}>
      <DataTable headers={["Data", "Ator", "Ação", "Entidade", "IP"]}>
        {items.map((item) => (
          <tr key={item.id}>
            <td>{shortDate(item.criado_em)}</td>
            <td>{item.ator_username || "--"}</td>
            <td>{item.acao}</td>
            <td>{item.entidade || "--"}</td>
            <td>{item.ip || "--"}</td>
          </tr>
        ))}
      </DataTable>
    </Page>
  );
}

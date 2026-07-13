import React from "react";
import { Download, RefreshCw } from "lucide-react";
import { DataTable } from "../components/DataTable";
import { Page } from "../components/Page";
import type { ApiClient } from "../types";
import { downloadCsv } from "../utils/export";
import { shortDate } from "../utils/format";

export function Auditoria({ api }: { api: ApiClient }) {
  const [items, setItems] = React.useState<any[]>([]);
  const [filters, setFilters] = React.useState({
    acao: "",
    ator: "",
    entidade: "",
    dataInicio: "",
    dataFim: ""
  });

  async function load() {
    const params = new URLSearchParams({ limit: "200" });
    if (filters.acao) params.set("acao", filters.acao);
    if (filters.ator) params.set("ator", filters.ator);
    if (filters.entidade) params.set("entidade", filters.entidade);
    if (filters.dataInicio) params.set("data_inicio", filters.dataInicio);
    if (filters.dataFim) params.set("data_fim", filters.dataFim);
    const data = await api.get<any>(`/api/admin/auditoria?${params.toString()}`);
    setItems(data?.items || []);
  }

  React.useEffect(() => { load(); }, [filters]);

  function exportAudit() {
    downloadCsv("auditoria-radar-pericial.csv", items, [
      "criado_em",
      "ator_username",
      "acao",
      "entidade",
      "entidade_id",
      "ip"
    ]);
  }

  return (
    <Page
      title="Auditoria Administrativa"
      subtitle={`${items.length} eventos recentes`}
      action={
        <div className="button-row">
          <button onClick={exportAudit} disabled={!items.length}><Download size={14} /> CSV</button>
          <button onClick={load}><RefreshCw size={14} /> Atualizar</button>
        </div>
      }
    >
      <div className="filters">
        <input placeholder="Ação" value={filters.acao} onChange={(event) => setFilters({ ...filters, acao: event.target.value })} />
        <input placeholder="Ator" value={filters.ator} onChange={(event) => setFilters({ ...filters, ator: event.target.value })} />
        <input placeholder="Entidade" value={filters.entidade} onChange={(event) => setFilters({ ...filters, entidade: event.target.value })} />
        <label className="field-compact">
          <span>De</span>
          <input type="date" value={filters.dataInicio} onChange={(event) => setFilters({ ...filters, dataInicio: event.target.value })} />
        </label>
        <label className="field-compact">
          <span>Até</span>
          <input type="date" value={filters.dataFim} onChange={(event) => setFilters({ ...filters, dataFim: event.target.value })} />
        </label>
      </div>

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

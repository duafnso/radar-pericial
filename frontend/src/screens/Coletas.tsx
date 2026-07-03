import React from "react";
import { RefreshCw } from "lucide-react";
import { DataTable } from "../components/DataTable";
import { Page } from "../components/Page";
import type { ApiClient, Coleta } from "../types";
import { fmt, friendlyError, shortDate } from "../utils/format";

export function Coletas({ api, hasPermission, notify }: {
  api: ApiClient;
  hasPermission: (permission?: string) => boolean;
  notify: (message: string) => void;
}) {
  const [items, setItems] = React.useState<Coleta[]>([]);

  async function load() {
    const data = await api.get<any>("/api/coletas/status?limit=50");
    setItems(data?.items || []);
  }

  React.useEffect(() => { load(); }, []);

  async function run(tipo: string) {
    if (!hasPermission("run_collections")) {
      notify("Seu perfil não pode executar coletas.");
      return;
    }
    const data = await api.post<any>(`/api/coletas/${tipo}/executar`, {});
    if (data?.status === "queued") {
      notify(`Coleta ${tipo} enfileirada.`);
      window.setTimeout(load, 900);
    } else {
      notify("Não foi possível enfileirar a coleta.");
    }
  }

  return (
    <Page title="Operação de Coletas" subtitle={`${items.length} execuções recentes`} action={<button onClick={load}><RefreshCw size={14} /> Atualizar</button>}>
      <div className="card">
        <div className="card-title">Disparo manual</div>
        <div className="button-row">
          {["geo", "judicial", "admin", "score"].map((tipo) => <button key={tipo} className="secondary" onClick={() => run(tipo)}>Executar {tipo}</button>)}
        </div>
      </div>
      <DataTable headers={["Fonte", "Status", "Início", "Duração", "Progresso", "Fim", "Erro"]}>
        {items.map((item) => (
          <tr key={item.id}>
            <td>{item.fonte}</td>
            <td><Badge status={item.status} /></td>
            <td>{shortDate(item.iniciado_em)}</td>
            <td>{item.duracao_segundos ? `${fmt(item.duracao_segundos, 1)}s` : "--"}</td>
            <td>{fmt(item.registros_salvos)} salvos de {fmt(item.registros_coletados)} coletados</td>
            <td>{item.finalizado_em ? shortDate(item.finalizado_em) : "Em andamento"}</td>
            <td className="wrap">{friendlyError(item.erro)}</td>
          </tr>
        ))}
      </DataTable>
    </Page>
  );
}

function Badge({ status }: { status?: string }) {
  const cls = status === "success" ? "high" : status === "failed" ? "critical" : "medium";
  const label = status === "success" ? "finalizada" : status === "failed" ? "falhou" : "em execução";
  return <span className={`badge ${cls}`}>{label}</span>;
}

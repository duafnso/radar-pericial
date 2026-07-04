import React from "react";
import { RefreshCw } from "lucide-react";
import { DataTable } from "../components/DataTable";
import { Empty, ErrorState, LoadingState } from "../components/Empty";
import { Page } from "../components/Page";
import type { ApiClient, Coleta } from "../types";
import { fmt, friendlyError, shortDate } from "../utils/format";

export function Coletas({ api, hasPermission, notify }: {
  api: ApiClient;
  hasPermission: (permission?: string) => boolean;
  notify: (message: string) => void;
}) {
  const [items, setItems] = React.useState<Coleta[]>([]);
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState("");

  async function load() {
    setLoading(true);
    setError("");
    const data = await api.get<any>("/api/coletas/status?limit=50");
    if (!data) setError("Não foi possível carregar o histórico de coletas.");
    setItems(data?.items || []);
    setLoading(false);
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
      {error && <ErrorState text={error} retry={load} />}
      {loading ? <LoadingState text="Carregando histórico de coletas..." /> : items.length ? (
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
      ) : <Empty text="Nenhuma execução de coleta registrada." />}
    </Page>
  );
}

function Badge({ status }: { status?: string }) {
  const cls = status === "success" ? "high" : status === "failed" ? "critical" : "medium";
  const label = status === "success" ? "finalizada" : status === "failed" ? "falhou" : "em execução";
  return <span className={`badge ${cls}`}>{label}</span>;
}

import React from "react";
import { Download, RefreshCw } from "lucide-react";
import { DataTable } from "../components/DataTable";
import { Empty, ErrorState, LoadingState } from "../components/Empty";
import { Page } from "../components/Page";
import type { ApiClient, Coleta } from "../types";
import { downloadCsv } from "../utils/export";
import { fmt, friendlyError, shortDate } from "../utils/format";

export function Coletas({ api, hasPermission, notify }: {
  api: ApiClient;
  hasPermission: (permission?: string) => boolean;
  notify: (message: string) => void;
}) {
  const [items, setItems] = React.useState<Coleta[]>([]);
  const [metrics, setMetrics] = React.useState<any[]>([]);
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState("");

  async function load() {
    setLoading(true);
    setError("");
    const [history, metricRows] = await Promise.all([
      api.get<any>("/api/coletas/status?limit=80"),
      api.get<any>("/api/coletas/metricas?limit=120")
    ]);
    if (!history) setError("Não foi possível carregar o histórico de coletas.");
    setItems(history?.items || []);
    setMetrics(metricRows?.items || []);
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
      return;
    }
    notify("Não foi possível enfileirar a coleta. Verifique se já existe uma coleta em andamento.");
  }

  function exportHistory() {
    downloadCsv("historico-coletas-radar-pericial.csv", items, [
      "id",
      "fonte",
      "tarefa",
      "status",
      "registros_coletados",
      "registros_salvos",
      "erro",
      "iniciado_em",
      "finalizado_em",
      "duracao_segundos"
    ]);
  }

  function exportMetrics() {
    downloadCsv("metricas-coletas-radar-pericial.csv", metrics, [
      "execucao_id",
      "fonte",
      "chave",
      "status",
      "registros_coletados",
      "registros_salvos",
      "descartados_sem_cnj",
      "duplicados",
      "erro",
      "criado_em"
    ]);
  }

  return (
    <Page
      title="Operação de Coletas"
      subtitle={`${items.length} execuções recentes · ${metrics.length} métricas de diagnóstico`}
      action={
        <div className="button-row">
          <button onClick={exportHistory} disabled={!items.length}><Download size={14} /> Histórico CSV</button>
          <button onClick={exportMetrics} disabled={!metrics.length}><Download size={14} /> Métricas CSV</button>
          <button onClick={load}><RefreshCw size={14} /> Atualizar</button>
        </div>
      }
    >
      <div className="card">
        <div className="card-title">Disparo manual</div>
        <div className="button-row">
          {["geo", "judicial", "admin", "score"].map((tipo) => (
            <button key={tipo} className="secondary" onClick={() => run(tipo)}>Executar {tipo}</button>
          ))}
        </div>
      </div>

      {error && <ErrorState text={error} retry={load} />}
      {loading ? <LoadingState text="Carregando histórico de coletas..." /> : (
        <>
          {items.length ? (
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

          <section className="card">
            <div className="card-title">Diagnóstico por fonte e classe</div>
            {metrics.length ? (
              <DataTable headers={["Execução", "Fonte", "Classe/Fonte", "Status", "Coletados", "Salvos", "Sem CNJ", "Duplicados"]}>
                {metrics.map((item) => (
                  <tr key={item.id}>
                    <td>{item.execucao_id}</td>
                    <td>{item.fonte}</td>
                    <td>{item.chave}</td>
                    <td><Badge status={item.status} /></td>
                    <td>{fmt(item.registros_coletados)}</td>
                    <td>{fmt(item.registros_salvos)}</td>
                    <td>{fmt(item.descartados_sem_cnj)}</td>
                    <td>{fmt(item.duplicados)}</td>
                  </tr>
                ))}
              </DataTable>
            ) : <Empty text="Nenhuma métrica detalhada registrada ainda. Execute uma nova coleta judicial para alimentar este diagnóstico." />}
          </section>
        </>
      )}
    </Page>
  );
}

function Badge({ status }: { status?: string }) {
  const cls = status === "success" ? "high" : status === "failed" ? "critical" : "medium";
  const label = status === "success" ? "finalizada" : status === "failed" ? "falhou" : "em execução";
  return <span className={`badge ${cls}`}>{label}</span>;
}

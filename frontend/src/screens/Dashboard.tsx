import React from "react";
import { RefreshCw } from "lucide-react";
import { Empty, ErrorState, LoadingState } from "../components/Empty";
import { Metric, StatusBlock } from "../components/Metric";
import { Page } from "../components/Page";
import { ProcessCard } from "../components/ProcessCard";
import type { ApiClient, Coleta, Processo, Screen } from "../types";
import { fmt, shortDate } from "../utils/format";

export function Dashboard({ api, region, navigate, hasPermission }: {
  api: ApiClient;
  region: string;
  navigate: (screen: Screen) => void;
  hasPermission: (permission?: string) => boolean;
}) {
  const [stats, setStats] = React.useState<any>(null);
  const [processos, setProcessos] = React.useState<Processo[]>([]);
  const [coletasResumo, setColetasResumo] = React.useState<Coleta[]>([]);
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState("");

  async function load() {
    setLoading(true);
    setError("");
    const suffix = region ? `?regiao=${encodeURIComponent(region)}` : "";
    const qs = `?faixa=janela_quente&limit=4${region ? `&regiao=${encodeURIComponent(region)}` : ""}`;
    const [statsData, processosData, coletasData] = await Promise.all([
      api.get<any>(`/api/stats${suffix}`),
      api.get<any>(`/api/processos${qs}`),
      hasPermission("read_operational") ? api.get<any>("/api/coletas/resumo") : Promise.resolve(null)
    ]);
    if (!statsData || !processosData) setError("NÃ£o foi possÃ­vel carregar o painel agora.");
    setStats(statsData);
    setProcessos(processosData?.items || []);
    setColetasResumo(coletasData?.items || []);
    setLoading(false);
  }

  React.useEffect(() => { load(); }, [region]);

  const failed = coletasResumo.filter((item) => item.ultimo_status === "failed").length;
  const running = coletasResumo.filter((item) => item.em_execucao).length;
  const last = coletasResumo[0];

  return (
    <Page title="Painel de InteligÃªncia Pericial" subtitle={region ? `Foco regional: ${region}` : "Mato Grosso inteiro"} action={<button onClick={load}><RefreshCw size={14} /> Atualizar</button>}>
      <div className="metrics">
        <Metric label="Processos" value={loading ? "..." : fmt(stats?.total_processos)} />
        <Metric label="Janela quente" value={loading ? "..." : fmt(stats?.processos_quentes)} tone="critical" />
        <Metric label="ProvÃ¡vel perÃ­cia" value={loading ? "..." : fmt(stats?.processos_provaveis)} tone="high" />
        <Metric label="ImÃ³veis SIGEF" value={loading ? "..." : fmt(stats?.total_parcelas)} />
        <Metric label="Portarias D.O." value={loading ? "..." : fmt(stats?.total_portarias)} />
        <Metric label="Alertas" value={loading ? "..." : fmt(stats?.processos_quentes)} />
      </div>
      {error && <ErrorState text={error} retry={load} />}
      {hasPermission("read_operational") && (
        <div className="card">
          <div className="card-title">SaÃºde das coletas</div>
          <div className="health-grid">
            <StatusBlock label="OperaÃ§Ã£o" value={failed ? `${failed} falha(s)` : running ? `${running} em execuÃ§Ã£o` : "EstÃ¡vel"} tone={failed ? "critical" : running ? "medium" : "high"} />
            <StatusBlock label="Ãšltima coleta" value={last ? `${last.fonte} Â· ${shortDate(last.ultima_execucao)}` : "Sem registro"} />
            <StatusBlock label="DataJud" value={coletasResumo.find((item) => item.fonte === "judicial")?.mensagem_operacional || "Monitorado"} />
            <button className="secondary" onClick={() => navigate("coletas")}>Abrir operaÃ§Ã£o de coletas</button>
          </div>
        </div>
      )}
      <div className="two-col">
        <div>
          <div className="section-label">Ãšltimos processos relevantes</div>
          <div className="stack">
            {loading ? <LoadingState text="Carregando processos relevantes..." /> : processos.length ? processos.map((processo) => <ProcessCard key={processo.id || processo.numero_cnj} processo={processo} />) : <Empty text="Nenhum processo quente encontrado. Execute ou aguarde a próxima coleta judicial para atualizar oportunidades." />}
          </div>
        </div>
        <div className="card">
          <div className="card-title">PrÃ³ximas aÃ§Ãµes</div>
          <div className="action-list">
            <button onClick={() => navigate("processos")}>Abrir radar de processos</button>
            <button onClick={() => navigate("mapa")}>Ver territÃ³rio</button>
            {hasPermission("run_collections") && <button onClick={() => navigate("coletas")}>Executar coleta manual</button>}
          </div>
        </div>
      </div>
    </Page>
  );
}


import React from "react";
import { RefreshCw } from "lucide-react";
import { Empty, ErrorState, LoadingState } from "../components/Empty";
import { Metric, StatusBlock } from "../components/Metric";
import { Page } from "../components/Page";
import { ProcessCard } from "../components/ProcessCard";
import type { ApiClient, Coleta, Processo, Screen } from "../types";
import { fmt, shortDate } from "../utils/format";

type QualityProblem = {
  codigo: string;
  rotulo: string;
  total: number;
  severidade: "alta" | "media" | "baixa";
};

type QualitySummary = {
  total_processos: number;
  score_qualidade: number;
  total_problemas: number;
  problemas: QualityProblem[];
  recomendacoes: string[];
};

export function Dashboard({ api, region, navigate, hasPermission }: {
  api: ApiClient;
  region: string;
  navigate: (screen: Screen) => void;
  hasPermission: (permission?: string) => boolean;
}) {
  const [stats, setStats] = React.useState<any>(null);
  const [processos, setProcessos] = React.useState<Processo[]>([]);
  const [coletasResumo, setColetasResumo] = React.useState<Coleta[]>([]);
  const [quality, setQuality] = React.useState<QualitySummary | null>(null);
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState("");

  async function load() {
    setLoading(true);
    setError("");
    const suffix = region ? `?regiao=${encodeURIComponent(region)}` : "";
    const qs = `?faixa=janela_quente&limit=4${region ? `&regiao=${encodeURIComponent(region)}` : ""}`;
    const [statsData, processosData, coletasData, qualityData] = await Promise.all([
      api.get<any>(`/api/stats${suffix}`),
      api.get<any>(`/api/processos${qs}`),
      hasPermission("read_operational") ? api.get<any>("/api/coletas/resumo") : Promise.resolve(null),
      hasPermission("read_operational") ? api.get<QualitySummary>("/api/qualidade/processos") : Promise.resolve(null)
    ]);
    if (!statsData || !processosData) setError("Não foi possível carregar o painel agora.");
    setStats(statsData);
    setProcessos(processosData?.items || []);
    setColetasResumo(coletasData?.items || []);
    setQuality(qualityData);
    setLoading(false);
  }

  React.useEffect(() => { load(); }, [region]);

  const failed = coletasResumo.filter((item) => item.ultimo_status === "failed").length;
  const running = coletasResumo.filter((item) => item.em_execucao).length;
  const last = coletasResumo[0];
  const qualityProblems = quality?.problemas.filter((item) => item.total > 0).slice(0, 4) || [];
  const qualityTone = (quality?.score_qualidade || 0) >= 90 ? "high" : (quality?.score_qualidade || 0) >= 70 ? "medium" : "critical";

  return (
    <Page title="Painel de Inteligência Pericial" subtitle={region ? `Foco regional: ${region}` : "Mato Grosso inteiro"} action={<button onClick={load}><RefreshCw size={14} /> Atualizar</button>}>
      <div className="metrics">
        <Metric label="Processos" value={loading ? "..." : fmt(stats?.total_processos)} />
        <Metric label="Janela quente" value={loading ? "..." : fmt(stats?.processos_quentes)} tone="critical" />
        <Metric label="Provável perícia" value={loading ? "..." : fmt(stats?.processos_provaveis)} tone="high" />
        <Metric label="Imóveis SIGEF" value={loading ? "..." : fmt(stats?.total_parcelas)} />
        <Metric label="Portarias D.O." value={loading ? "..." : fmt(stats?.total_portarias)} />
        <Metric label="Alertas" value={loading ? "..." : fmt(stats?.processos_quentes)} />
      </div>
      {error && <ErrorState text={error} retry={load} />}
      {hasPermission("read_operational") && (
        <>
          <div className="card">
            <div className="card-title">Saúde das coletas</div>
            <div className="health-grid">
              <StatusBlock label="Operação" value={failed ? `${failed} falha(s)` : running ? `${running} em execução` : "Estável"} tone={failed ? "critical" : running ? "medium" : "high"} />
              <StatusBlock label="Última coleta" value={last ? `${last.fonte} · ${shortDate(last.ultima_execucao)}` : "Sem registro"} />
              <StatusBlock label="DataJud" value={coletasResumo.find((item) => item.fonte === "judicial")?.mensagem_operacional || "Monitorado"} />
              <button className="secondary" onClick={() => navigate("coletas")}>Abrir operação de coletas</button>
            </div>
          </div>
          <div className="card quality-card">
            <div className="quality-heading">
              <div>
                <div className="card-title">Qualidade dos dados</div>
                <p>Consistência dos processos coletados e preparados para análise.</p>
              </div>
              <div className={`quality-score ${qualityTone}`}>
                <strong>{loading ? "..." : quality?.score_qualidade ?? "--"}<small>/100</small></strong>
                <span>{quality ? `${fmt(quality.total_processos)} processos auditados` : "Auditoria indisponível"}</span>
              </div>
            </div>
            {qualityProblems.length ? (
              <div className="quality-problems">
                {qualityProblems.map((problem) => (
                  <div key={problem.codigo}>
                    <span>{problem.rotulo}</span>
                    <strong>{fmt(problem.total)}</strong>
                  </div>
                ))}
              </div>
            ) : (
              <div className="quality-ok">Nenhuma inconsistência relevante identificada.</div>
            )}
            {!!quality?.recomendacoes.length && (
              <p className="quality-guidance">{quality.recomendacoes[0]}</p>
            )}
          </div>
        </>
      )}
      <div className="two-col">
        <div>
          <div className="section-label">Últimos processos relevantes</div>
          <div className="stack">
            {loading ? <LoadingState text="Carregando processos relevantes..." /> : processos.length ? processos.map((processo) => <ProcessCard key={processo.id || processo.numero_cnj} processo={processo} />) : <Empty text="Nenhum processo quente encontrado. Execute ou aguarde a próxima coleta judicial para atualizar oportunidades." />}
          </div>
        </div>
        <div className="card">
          <div className="card-title">Próximas ações</div>
          <div className="action-list">
            <button onClick={() => navigate("processos")}>Abrir radar de processos</button>
            <button onClick={() => navigate("mapa")}>Ver território</button>
            {hasPermission("run_collections") && <button onClick={() => navigate("coletas")}>Executar coleta manual</button>}
          </div>
        </div>
      </div>
    </Page>
  );
}
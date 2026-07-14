import React from "react";
import { Bell, CheckCircle2, FileText, RefreshCw } from "lucide-react";
import { CardLine } from "../components/CardLine";
import { Empty, ErrorState, LoadingState } from "../components/Empty";
import { Page } from "../components/Page";
import type { ApiClient } from "../types";
import { fmt, shortDate } from "../utils/format";

export function Administrativo({ api }: { api: ApiClient }) {
  const [items, setItems] = React.useState<any[]>([]);
  const [days, setDays] = React.useState("365");
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState("");

  async function load() {
    setLoading(true);
    setError("");
    const data = await api.get<any>(`/api/eventos?limit=80&dias=${days}`);
    if (!data) setError("Não foi possível carregar eventos administrativos.");
    setItems(data?.items || []);
    setLoading(false);
  }

  React.useEffect(() => { load(); }, [days]);

  return (
    <Page
      title="Radar Administrativo"
      subtitle={`${fmt(items.length)} eventos administrativos no período selecionado`}
      action={<button onClick={load}><RefreshCw size={14} /> Atualizar</button>}
    >
      <div className="filters">
        <select value={days} onChange={(event) => setDays(event.target.value)}>
          <option value="30">Últimos 30 dias</option>
          <option value="90">Últimos 90 dias</option>
          <option value="365">Últimos 12 meses</option>
          <option value="1825">Últimos 5 anos</option>
        </select>
      </div>
      {error && <ErrorState text={error} retry={load} />}
      {loading ? <LoadingState text="Carregando eventos administrativos..." /> : (
        <div className="stack">
          {items.length ? items.map((item, index) => (
            <CardLine
              key={item.id || index}
              title={item.titulo || item.descricao || item.fonte || "Evento administrativo"}
              meta={`${item.fonte || "Fonte não informada"} · ${item.municipio || "Município pendente"} · ${shortDate(item.coletado_em || item.data_publicacao)}`}
            />
          )) : (
            <Empty text="Nenhum evento administrativo encontrado nesse período. Execute a coleta administrativa em Operação de Coletas para buscar novos dados." />
          )}
        </div>
      )}
    </Page>
  );
}

export function Alertas({ api }: { api: ApiClient }) {
  const [items, setItems] = React.useState<any[]>([]);
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState("");

  async function load() {
    setLoading(true);
    setError("");
    const data = await api.get<any>("/api/alertas?limit=80");
    if (!data) setError("Não foi possível carregar alertas.");
    setItems(data?.items || []);
    setLoading(false);
  }

  async function markRead(item: any) {
    if (!item.id || item.origem_alerta !== "processo_acompanhado") return;
    const result = await api.patch<any>(`/api/alertas/${item.id}/lido`, {});
    if (result?.status === "ok") {
      setItems((current) => current.map((row) => row.id === item.id ? { ...row, lido: true } : row));
    }
  }

  React.useEffect(() => { load(); }, []);

  const tracked = items.filter((item) => item.origem_alerta === "processo_acompanhado");
  const general = items.filter((item) => item.origem_alerta !== "processo_acompanhado");

  return (
    <Page title="Central de Alertas" subtitle={`${fmt(items.length)} alertas e oportunidades recentes`} action={<button onClick={load}><RefreshCw size={14} /> Atualizar</button>}>
      {error && <ErrorState text={error} retry={load} />}
      {loading ? <LoadingState text="Carregando alertas..." /> : (
        <div className="two-col">
          <section className="card">
            <div className="section-label"><Bell size={14} /> Processos acompanhados</div>
            <div className="stack">
              {tracked.length ? tracked.map((item) => (
                <div className={item.lido ? "alert-row read" : "alert-row"} key={item.id}>
                  <CardLine
                    title={item.titulo || "Alerta de processo"}
                    meta={`${item.numero_cnj || "CNJ pendente"} · ${item.municipio || item.comarca || "Local pendente"} · ${shortDate(item.criado_em)}`}
                  />
                  {!item.lido && (
                    <button className="secondary" onClick={() => markRead(item)}>
                      <CheckCircle2 size={14} /> Lido
                    </button>
                  )}
                </div>
              )) : <Empty text="Nenhum processo acompanhado gerou alerta ainda. Acompanhe processos no Radar de Processos para receber mudanças aqui." />}
            </div>
          </section>
          <section className="card">
            <div className="section-label"><FileText size={14} /> Oportunidades administrativas</div>
            <div className="stack">
              {general.length ? general.map((item, index) => (
                <CardLine
                  key={item.id || index}
                  title={item.titulo || item.orgao || "Alerta"}
                  meta={`${item.fonte || ""} · Score ${item.score_evento || item.score_total || 0} · ${item.municipio || ""}`}
                />
              )) : <Empty text="Nenhuma oportunidade administrativa quente encontrada. Execute a coleta administrativa para buscar novos eventos." />}
            </div>
          </section>
        </div>
      )}
    </Page>
  );
}



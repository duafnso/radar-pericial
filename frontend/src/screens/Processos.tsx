import React from "react";
import { BellPlus, RefreshCw, X } from "lucide-react";
import { Empty, ErrorState, LoadingState } from "../components/Empty";
import { Page } from "../components/Page";
import { ProcessCard } from "../components/ProcessCard";
import type { ApiClient, Processo, Screen } from "../types";
import { fmt, scoreLabel, shortDate } from "../utils/format";

type Filters = { faixa: string; regiao: string; municipio: string; classe: string };

export function Processos({
  api,
  region,
  navigate,
  notify
}: {
  api: ApiClient;
  region: string;
  navigate: (screen: Screen) => void;
  notify: (message: string) => void;
}) {
  const [items, setItems] = React.useState<Processo[]>([]);
  const [total, setTotal] = React.useState(0);
  const [filters, setFilters] = React.useState<Filters>({ faixa: "", regiao: "", municipio: "", classe: "" });
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState("");
  const [selected, setSelected] = React.useState<Processo | null>(null);

  async function load() {
    setLoading(true);
    setError("");
    const params = new URLSearchParams({ limit: "50", offset: "0" });
    const effectiveRegion = region || filters.regiao;
    if (filters.faixa) params.set("faixa", filters.faixa);
    if (effectiveRegion) params.set("regiao", effectiveRegion);
    if (filters.municipio) params.set("municipio", filters.municipio);
    if (filters.classe) params.set("classe", filters.classe);
    const data = await api.get<any>(`/api/processos?${params.toString()}`);
    if (!data) setError("Não foi possível carregar os processos.");
    setItems(data?.items || []);
    setTotal(data?.total || 0);
    setLoading(false);
  }

  async function follow(processo: Processo) {
    if (!processo.id) {
      notify("Processo sem identificador interno.");
      return;
    }
    const result = await api.post<any>(`/api/processos/${processo.id}/acompanhar`);
    if (result?.status === "ok") {
      notify("Processo adicionado à Central de Alertas.");
      navigate("alertas");
      return;
    }
    notify("Não foi possível acompanhar este processo.");
  }

  React.useEffect(() => { load(); }, [region, filters]);

  return (
    <Page title="Radar de Processos Judiciais" subtitle={`${fmt(total)} processos · dados da última coleta judicial`} action={<button onClick={load}><RefreshCw size={14} /> Atualizar</button>}>
      <FilterBar filters={filters} setFilters={setFilters} />
      {error && <ErrorState text={error} retry={load} />}
      {loading ? (
        <LoadingState text="Carregando processos..." />
      ) : items.length ? (
        <div className="stack">
          {items.map((processo) => (
            <ProcessCard
              key={processo.id || processo.numero_cnj}
              processo={processo}
              onOpen={setSelected}
              onFollow={follow}
            />
          ))}
        </div>
      ) : (
        <Empty text="Nenhum processo encontrado com estes filtros." />
      )}
      {selected && <ProcessModal processo={selected} close={() => setSelected(null)} follow={follow} />}
    </Page>
  );
}

function FilterBar({ filters, setFilters }: {
  filters: Filters;
  setFilters: React.Dispatch<React.SetStateAction<Filters>>;
}) {
  return (
    <div className="filters">
      <select value={filters.faixa} onChange={(event) => setFilters({ ...filters, faixa: event.target.value })}>
        <option value="">Todas as faixas</option>
        <option value="janela_quente">Janela quente</option>
        <option value="provavel">Provável perícia</option>
        <option value="observacao">Observação</option>
        <option value="frio">Frio</option>
      </select>
      <select value={filters.regiao} onChange={(event) => setFilters({ ...filters, regiao: event.target.value })}>
        <option value="">Todas as regiões</option>
        <option>Médio-Norte</option>
        <option>Norte</option>
        <option>Centro-Sul</option>
        <option>Oeste</option>
        <option>Leste</option>
        <option>Sudoeste</option>
      </select>
      <input placeholder="Município" value={filters.municipio} onChange={(event) => setFilters({ ...filters, municipio: event.target.value })} />
      <input placeholder="Classe processual" value={filters.classe} onChange={(event) => setFilters({ ...filters, classe: event.target.value })} />
    </div>
  );
}

function ProcessModal({ processo, close, follow }: {
  processo: Processo;
  close: () => void;
  follow: (processo: Processo) => void;
}) {
  const rows = [
    ["Número CNJ", processo.numero_cnj],
    ["Tribunal", processo.tribunal],
    ["Comarca", processo.comarca],
    ["Vara", processo.vara],
    ["Município", processo.municipio],
    ["Região IMEA", processo.regiao_imea],
    ["Classe", processo.classe_processual],
    ["Assunto", processo.assunto_principal],
    ["Fase atual", processo.fase_atual],
    ["Distribuição", shortDate(processo.data_distribuicao)],
    ["Tipo de perícia sugerida", processo.tipo_pericia_sugerida],
    ["Urgência", processo.urgencia],
    ["Faixa", scoreLabel(processo.faixa_probabilidade)]
  ];

  return (
    <div className="modal-backdrop" role="presentation" onClick={close}>
      <section className="modal-panel" role="dialog" aria-modal="true" onClick={(event) => event.stopPropagation()}>
        <header className="modal-header">
          <div>
            <strong>{processo.numero_cnj || "Processo sem CNJ"}</strong>
            <span>{processo.classe_processual || "Classe não informada"}</span>
          </div>
          <button className="secondary icon-button" onClick={close} aria-label="Fechar"><X size={16} /></button>
        </header>
        <div className="detail-grid">
          {rows.map(([label, value]) => (
            <div key={label}>
              <span>{label}</span>
              <strong>{value || "--"}</strong>
            </div>
          ))}
        </div>
        <div className="modal-score">
          <div>
            <span>Score pericial</span>
            <strong>{processo.score_total || 0}</strong>
          </div>
          <p>{processo.categorias_detectadas || "Sem categorias adicionais detectadas."}</p>
        </div>
        <footer className="modal-actions">
          <button className="secondary" onClick={close}>Fechar</button>
          <button className="primary" onClick={() => follow(processo)}><BellPlus size={14} /> Acompanhar processo</button>
        </footer>
      </section>
    </div>
  );
}

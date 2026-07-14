import React from "react";
import { BellPlus, Download, RefreshCw, X } from "lucide-react";
import { Empty, ErrorState, LoadingState } from "../components/Empty";
import { Page } from "../components/Page";
import { ProcessCard } from "../components/ProcessCard";
import type { ApiClient, Processo, Screen } from "../types";
import { downloadCsv } from "../utils/export";
import { fmt, scoreLabel, shortDate } from "../utils/format";

type Filters = {
  faixa: string;
  regiao: string;
  municipio: string;
  classe: string;
  dataInicio: string;
  dataFim: string;
};

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
  const [filters, setFilters] = React.useState<Filters>({
    faixa: "",
    regiao: "",
    municipio: "",
    classe: "",
    dataInicio: "2026-01-01",
    dataFim: ""
  });
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState("");
  const [selected, setSelected] = React.useState<Processo | null>(null);
  const [page, setPage] = React.useState(0);
  const pageSize = 50;

  function buildProcessParams(limit: number, offset: number) {
    const params = new URLSearchParams({ limit: String(limit), offset: String(offset) });
    const effectiveRegion = region || filters.regiao;
    if (filters.faixa) params.set("faixa", filters.faixa);
    if (effectiveRegion) params.set("regiao", effectiveRegion);
    if (filters.municipio) params.set("municipio", filters.municipio);
    if (filters.classe) params.set("classe", filters.classe);
    if (filters.dataInicio) params.set("data_inicio", filters.dataInicio);
    if (filters.dataFim) params.set("data_fim", filters.dataFim);
    return params;
  }

  async function load() {
    setLoading(true);
    setError("");
    const params = buildProcessParams(pageSize, page * pageSize);
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
      notify("Processo adicionado Ã  Central de Alertas.");
      navigate("alertas");
      return;
    }
    notify("NÃ£o foi possÃ­vel acompanhar este processo.");
  }

  React.useEffect(() => { setPage(0); }, [region, filters]);
  React.useEffect(() => { load(); }, [region, filters, page]);

  const csvColumns = [
    "numero_cnj",
    "classe_processual",
    "municipio",
    "comarca",
    "data_distribuicao",
    "fase_atual",
    "score_total",
    "faixa_probabilidade",
    "tipo_pericia_sugerida"
  ];

  function exportCurrentPage() {
    downloadCsv("radar-processos-pagina.csv", items, csvColumns);
  }

  async function exportAllFiltered() {
    const limit = 500;
    const all: Processo[] = [];
    let offset = 0;
    while (offset < Math.max(total, 1)) {
      const params = buildProcessParams(limit, offset);
      const data = await api.get<any>(`/api/processos?${params.toString()}`);
      const batch = data?.items || [];
      all.push(...batch);
      if (!batch.length || batch.length < limit) break;
      offset += limit;
    }
    downloadCsv("radar-processos-filtrados.csv", all, csvColumns);
    notify(`${fmt(all.length)} processos exportados.`);
  }

  return (
    <Page
      title="Radar de Processos Judiciais"
      subtitle={`${fmt(total)} processos Â· pÃ¡gina ${page + 1} Â· dados da Ãºltima coleta judicial`}
      action={
        <div className="button-row">
          <button onClick={exportCurrentPage} disabled={!items.length}><Download size={14} /> CSV página</button>
          <button onClick={exportAllFiltered} disabled={!total}><Download size={14} /> CSV filtros</button>
          <button onClick={load}><RefreshCw size={14} /> Atualizar</button>
        </div>
      }
    >
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
        <Empty text="Nenhum processo encontrado com estes filtros. Reduza os filtros ou execute uma nova coleta judicial em Operação de Coletas." />
      )}
      <Pagination
        page={page}
        pageSize={pageSize}
        total={total}
        setPage={setPage}
      />
      {selected && <ProcessModal processo={selected} close={() => setSelected(null)} follow={follow} />}
    </Page>
  );
}

function Pagination({ page, pageSize, total, setPage }: {
  page: number;
  pageSize: number;
  total: number;
  setPage: (page: number) => void;
}) {
  const maxPage = Math.max(0, Math.ceil(total / pageSize) - 1);
  return (
    <div className="pagination-row">
      <span>{fmt(page * pageSize + 1)}-{fmt(Math.min((page + 1) * pageSize, total))} de {fmt(total)}</span>
      <div className="button-row">
        <button className="secondary" disabled={page <= 0} onClick={() => setPage(page - 1)}>Anterior</button>
        <button className="secondary" disabled={page >= maxPage} onClick={() => setPage(page + 1)}>PrÃ³xima</button>
      </div>
    </div>
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
        <option value="provavel">ProvÃ¡vel perÃ­cia</option>
        <option value="observacao">ObservaÃ§Ã£o</option>
        <option value="frio">Frio</option>
      </select>
      <select value={filters.regiao} onChange={(event) => setFilters({ ...filters, regiao: event.target.value })}>
        <option value="">Todas as regiÃµes</option>
        <option>MÃ©dio-Norte</option>
        <option>Norte</option>
        <option>Centro-Sul</option>
        <option>Oeste</option>
        <option>Leste</option>
        <option>Sudoeste</option>
      </select>
      <input placeholder="MunicÃ­pio" value={filters.municipio} onChange={(event) => setFilters({ ...filters, municipio: event.target.value })} />
      <input placeholder="Classe processual" value={filters.classe} onChange={(event) => setFilters({ ...filters, classe: event.target.value })} />
      <label className="field-compact">
        <span>De</span>
        <input type="date" value={filters.dataInicio} onChange={(event) => setFilters({ ...filters, dataInicio: event.target.value })} />
      </label>
      <label className="field-compact">
        <span>AtÃ©</span>
        <input type="date" value={filters.dataFim} onChange={(event) => setFilters({ ...filters, dataFim: event.target.value })} />
      </label>
    </div>
  );
}

function ProcessModal({ processo, close, follow }: {
  processo: Processo;
  close: () => void;
  follow: (processo: Processo) => void;
}) {
  const rows = [
    ["NÃºmero CNJ", processo.numero_cnj],
    ["Tribunal", processo.tribunal],
    ["Comarca", processo.comarca],
    ["Vara", processo.vara],
    ["MunicÃ­pio", processo.municipio],
    ["RegiÃ£o IMEA", processo.regiao_imea],
    ["Classe", processo.classe_processual],
    ["Assunto", processo.assunto_principal],
    ["Fase atual", processo.fase_atual],
    ["DistribuiÃ§Ã£o", shortDate(processo.data_distribuicao)],
    ["Tipo de perÃ­cia sugerida", processo.tipo_pericia_sugerida],
    ["UrgÃªncia", processo.urgencia],
    ["Faixa", scoreLabel(processo.faixa_probabilidade)]
  ];

  return (
    <div className="modal-backdrop" role="presentation" onClick={close}>
      <section className="modal-panel" role="dialog" aria-modal="true" onClick={(event) => event.stopPropagation()}>
        <header className="modal-header">
          <div>
            <strong>{processo.numero_cnj || "Processo sem CNJ"}</strong>
            <span>{processo.classe_processual || "Classe nÃ£o informada"}</span>
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




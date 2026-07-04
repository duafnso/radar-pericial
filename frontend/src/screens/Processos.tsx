import React from "react";
import { RefreshCw } from "lucide-react";
import { Empty, ErrorState, LoadingState } from "../components/Empty";
import { Page } from "../components/Page";
import { ProcessCard } from "../components/ProcessCard";
import type { ApiClient, Processo } from "../types";
import { fmt } from "../utils/format";

type Filters = { faixa: string; regiao: string; municipio: string; classe: string };

export function Processos({ api, region }: { api: ApiClient; region: string }) {
  const [items, setItems] = React.useState<Processo[]>([]);
  const [total, setTotal] = React.useState(0);
  const [filters, setFilters] = React.useState<Filters>({ faixa: "", regiao: "", municipio: "", classe: "" });
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState("");

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

  React.useEffect(() => { load(); }, [region, filters]);

  return (
    <Page title="Radar de Processos Judiciais" subtitle={`${fmt(total)} processos · dados da última coleta judicial`} action={<button onClick={load}><RefreshCw size={14} /> Atualizar</button>}>
      <FilterBar filters={filters} setFilters={setFilters} />
      {error && <ErrorState text={error} retry={load} />}
      {loading ? <LoadingState text="Carregando processos..." /> : items.length ? <div className="stack">{items.map((processo) => <ProcessCard key={processo.id || processo.numero_cnj} processo={processo} />)}</div> : <Empty text="Nenhum processo encontrado com estes filtros." />}
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

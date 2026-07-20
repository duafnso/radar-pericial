import React from "react";
import L from "leaflet";
import "leaflet/dist/leaflet.css";
import {
  BellPlus,
  ChevronLeft,
  ChevronRight,
  Eye,
  FilterX,
  LocateFixed,
  MapPin,
  RefreshCw,
  TriangleAlert,
  X,
} from "lucide-react";
import { Empty, ErrorState } from "../components/Empty";
import { Page } from "../components/Page";
import { ProcessModal } from "../components/ProcessModal";
import {
  MAP_TONE_COLORS,
  buildMapSummaryParams,
  formatCivilDate,
  markerTone,
  parseProcessListResponse,
  resolveTileConfig,
} from "../map/model";
import type {
  ApiClient,
  MapCitySummary,
  MapFilters,
  MapProcess,
  MapSummaryResponse,
  Screen,
} from "../types";
import { fmt, scoreLabel } from "../utils/format";

const PAGE_SIZE = 10;
const DEFAULT_CENTER: L.LatLngExpression = [-13.8, -55.9];
const DEFAULT_FILTERS: MapFilters = {
  regiao: "",
  municipio: "",
  faixa: "",
  dataInicio: "2026-01-01",
  dataFim: "",
};
const EMPTY_SUMMARY: MapSummaryResponse = {
  total_processos: 0,
  total_municipios: 0,
  sem_localizacao: 0,
  items: [],
};
const TILE_CONFIG = resolveTileConfig(
  import.meta.env.VITE_MAP_TILE_URL,
  import.meta.env.VITE_MAP_TILE_ATTRIBUTION,
);

type MapScreenProps = {
  api: ApiClient;
  region: string;
  navigate: (screen: Screen) => void;
  notify: (message: string) => void;
};

function markerColor(tone: ReturnType<typeof markerTone>) {
  return MAP_TONE_COLORS[tone];
}

function hasFiniteCoordinates(city: MapCitySummary) {
  return Number.isFinite(city.lat) && Number.isFinite(city.lng);
}

export function MapScreen({ api, region, navigate, notify }: MapScreenProps) {
  const mapContainerRef = React.useRef<HTMLDivElement | null>(null);
  const mapInstanceRef = React.useRef<L.Map | null>(null);
  const markerLayerRef = React.useRef<L.LayerGroup | null>(null);
  const summaryRequestRef = React.useRef(0);
  const followInFlightRef = React.useRef(false);
  const [filters, setFilters] = React.useState<MapFilters>({ ...DEFAULT_FILTERS });
  const [appliedFilters, setAppliedFilters] = React.useState<MapFilters>({ ...DEFAULT_FILTERS });
  const [summary, setSummary] = React.useState<MapSummaryResponse>(EMPTY_SUMMARY);
  const [selectedCity, setSelectedCity] = React.useState<MapCitySummary | null>(null);
  const [processes, setProcesses] = React.useState<MapProcess[]>([]);
  const [processTotal, setProcessTotal] = React.useState(0);
  const [page, setPage] = React.useState(0);
  const [selectedProcess, setSelectedProcess] = React.useState<MapProcess | null>(null);
  const [followingId, setFollowingId] = React.useState<number | null>(null);
  const [loadingSummary, setLoadingSummary] = React.useState(true);
  const [loadingProcesses, setLoadingProcesses] = React.useState(false);
  const [summaryError, setSummaryError] = React.useState("");
  const [processError, setProcessError] = React.useState("");
  const [processRefresh, setProcessRefresh] = React.useState(0);
  const [tilesAvailable, setTilesAvailable] = React.useState(true);

  React.useEffect(() => {
    const container = mapContainerRef.current;
    if (!container || mapInstanceRef.current) return;

    const map = L.map(container, {
      center: DEFAULT_CENTER,
      zoom: 6,
      zoomControl: true,
      attributionControl: true,
    });
    const markerLayer = L.layerGroup().addTo(map);
    const tileLayer = L.tileLayer(TILE_CONFIG.url, {
      attribution: TILE_CONFIG.attribution,
      maxZoom: 18,
    });
    const handleTileError = () => setTilesAvailable(false);

    tileLayer.on("tileerror", handleTileError);
    tileLayer.addTo(map);
    mapInstanceRef.current = map;
    markerLayerRef.current = markerLayer;

    const resizeFrame = window.requestAnimationFrame(() => map.invalidateSize());

    return () => {
      window.cancelAnimationFrame(resizeFrame);
      tileLayer.off("tileerror", handleTileError);
      tileLayer.remove();
      markerLayer.clearLayers();
      markerLayer.remove();
      map.remove();
      markerLayerRef.current = null;
      mapInstanceRef.current = null;
    };
  }, []);

  const loadSummary = React.useCallback(async () => {
    const requestId = summaryRequestRef.current + 1;
    summaryRequestRef.current = requestId;
    setLoadingSummary(true);
    setSummaryError("");

    const params = buildMapSummaryParams(appliedFilters, region);
    const data = await api.get<MapSummaryResponse>(
      `/api/processos/mapa/resumo?${params.toString()}`,
    );
    if (requestId !== summaryRequestRef.current) return;

    if (!data) {
      setSummary(EMPTY_SUMMARY);
      setSelectedCity(null);
      setSummaryError("Não foi possível carregar o resumo territorial.");
      setLoadingSummary(false);
      return;
    }

    const nextSummary: MapSummaryResponse = {
      total_processos: data.total_processos,
      total_municipios: data.total_municipios,
      sem_localizacao: data.sem_localizacao,
      items: data.items,
    };
    setSummary(nextSummary);
    setSelectedCity((current) => {
      if (!current) return null;
      return nextSummary.items.find((city) => city.municipio === current.municipio) || null;
    });
    setLoadingSummary(false);
  }, [api, appliedFilters, region]);

  React.useEffect(() => {
    void loadSummary();
    return () => {
      summaryRequestRef.current += 1;
    };
  }, [loadSummary]);

  const selectCity = React.useCallback((city: MapCitySummary) => {
    setSelectedCity(city);
    setPage(0);
    if (hasFiniteCoordinates(city)) {
      const map = mapInstanceRef.current;
      const targetZoom = Math.max(map?.getZoom() || 6, 8);
      map?.setView([city.lat, city.lng], targetZoom, { animate: true });
    }
  }, []);

  React.useEffect(() => {
    const map = mapInstanceRef.current;
    const markerLayer = markerLayerRef.current;
    if (!map || !markerLayer) return;

    markerLayer.clearLayers();
    const clickBindings: Array<{
      layer: L.CircleMarker | L.Marker;
      activate: () => void;
    }> = [];
    const keyboardBindings: Array<{
      element: HTMLElement | null | undefined;
      handleMarkerKeyDown: (event: KeyboardEvent) => void;
    }> = [];

    summary.items.filter(hasFiniteCoordinates).forEach((city) => {
      const selected = city.municipio === selectedCity?.municipio;
      const coordinates: L.LatLngExpression = [city.lat, city.lng];
      const activate = () => selectCity(city);
      const marker = L.circleMarker(coordinates, {
        radius: 13,
        weight: selected ? 3 : 2,
        color: "#ffffff",
        fillColor: markerColor(markerTone(city.faixa_dominante)),
        fillOpacity: 1,
      });
      const faixaLabel = scoreLabel(city.faixa_dominante);
      const accessibleName = `${city.municipio}, ${fmt(city.total_processos)} processos, faixa ${faixaLabel}`;
      const tooltipNode = document.createElement("div");
      tooltipNode.textContent = `${city.municipio}: ${fmt(city.total_processos)} processos, maior score ${fmt(city.maior_score)}. Faixa ${faixaLabel}`;
      marker.bindTooltip(tooltipNode, {
        className: "map-city-tooltip",
        direction: "top",
        offset: L.point(0, -12),
      });
      marker.on("click", activate);
      marker.addTo(markerLayer);

      const countNode = document.createElement("span");
      countNode.className = city.total_processos >= 1000
        ? "map-city-count-value compact"
        : "map-city-count-value";
      countNode.textContent = fmt(city.total_processos);
      const countIcon = L.divIcon({
        className: `map-city-count${selected ? " is-selected" : ""}`,
        html: countNode,
        iconSize: [26, 26],
        iconAnchor: [13, 13],
      });
      const countMarker = L.marker(coordinates, {
        icon: countIcon,
        keyboard: false,
        title: accessibleName,
        zIndexOffset: selected ? 1000 : 0,
      });
      countMarker.on("click", activate);
      countMarker.addTo(markerLayer);
      const markerElement = countMarker.getElement();
      const handleMarkerKeyDown = (event: KeyboardEvent) => {
        if (event.key !== "Enter" && event.key !== " ") return;
        event.preventDefault();
        activate();
      };
      if (markerElement) {
        markerElement.setAttribute("role", "button");
        markerElement.setAttribute("tabindex", "0");
        markerElement.setAttribute("aria-label", accessibleName);
        markerElement.addEventListener("keydown", handleMarkerKeyDown);
      }
      clickBindings.push({ layer: marker, activate }, { layer: countMarker, activate });
      keyboardBindings.push({ element: markerElement, handleMarkerKeyDown });
    });

    return () => {
      clickBindings.forEach(({ layer, activate }) => layer.off("click", activate));
      keyboardBindings.forEach(({ element, handleMarkerKeyDown }) => {
        element?.removeEventListener("keydown", handleMarkerKeyDown);
      });
      markerLayer.clearLayers();
    };
  }, [selectCity, selectedCity?.municipio, summary.items]);

  React.useEffect(() => {
    const map = mapInstanceRef.current;
    if (!map) return;
    const coordinates = summary.items
      .filter(hasFiniteCoordinates)
      .map((city): L.LatLngTuple => [city.lat, city.lng]);

    if (!coordinates.length) {
      map.setView(DEFAULT_CENTER, 6, { animate: false });
      return;
    }

    const bounds = L.latLngBounds(coordinates);
    if (bounds.isValid()) {
      map.fitBounds(bounds, { padding: [34, 34], maxZoom: 8, animate: false });
    }
  }, [summary.items]);

  React.useEffect(() => {
    const city = selectedCity;
    if (!city) {
      setProcesses([]);
      setProcessTotal(0);
      setProcessError("");
      setLoadingProcesses(false);
      return;
    }
    const selectedMunicipio = city.municipio;

    let active = true;
    async function loadProcesses() {
      setLoadingProcesses(true);
      setProcessError("");
      const params = new URLSearchParams({
        limit: "10",
        offset: String(page * PAGE_SIZE),
      });
      params.set("municipio", selectedMunicipio);
      const effectiveRegion = region || appliedFilters.regiao;
      if (effectiveRegion) params.set("regiao", effectiveRegion);
      if (appliedFilters.faixa) params.set("faixa", appliedFilters.faixa);
      if (appliedFilters.dataInicio) params.set("data_inicio", appliedFilters.dataInicio);
      if (appliedFilters.dataFim) params.set("data_fim", appliedFilters.dataFim);

      const payload = await api.get<unknown>(
        `/api/processos?${params.toString()}`,
      );
      if (!active) return;
      const data = parseProcessListResponse(payload);
      if (!data) {
        setProcessError("Não foi possível carregar os processos deste município.");
        setLoadingProcesses(false);
        return;
      }

      const maxPage = Math.max(0, Math.ceil(data.total / PAGE_SIZE) - 1);
      if (page > maxPage) {
        setPage(maxPage);
        return;
      }
      if (page === 0 && data.total === 0) {
        setSelectedCity(null);
        setProcesses([]);
        setProcessTotal(0);
        setLoadingProcesses(false);
        return;
      }

      setProcesses(data.items);
      setProcessTotal(data.total);
      setLoadingProcesses(false);
    }

    void loadProcesses();
    return () => {
      active = false;
    };
  }, [api, appliedFilters, page, processRefresh, region, selectedCity]);

  async function followProcess(processo: MapProcess) {
    const id = Number(processo.id);
    if (!Number.isInteger(id) || id <= 0) {
      notify("Processo sem identificador interno.");
      return;
    }
    if (followInFlightRef.current) return;

    followInFlightRef.current = true;
    setFollowingId(id);
    try {
      const result = await api.post<{ status: string }>(
        `/api/processos/${id}/acompanhar`,
      );
      if (result?.status === "ok") {
        notify("Processo adicionado à Central de Alertas.");
        navigate("alertas");
        return;
      }
      notify("Não foi possível acompanhar este processo.");
    } finally {
      followInFlightRef.current = false;
      setFollowingId(null);
    }
  }

  function applyFilters(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setPage(0);
    setAppliedFilters({ ...filters });
  }

  function clearFilters() {
    setFilters({ ...DEFAULT_FILTERS });
    setAppliedFilters({ ...DEFAULT_FILTERS });
    setPage(0);
  }

  const locatedProcesses = summary.total_processos;
  const totalFound = summary.total_processos + summary.sem_localizacao;
  const pageCount = Math.max(1, Math.ceil(processTotal / PAGE_SIZE));

  return (
    <Page
      title="Mapa Territorial"
      subtitle={`${fmt(locatedProcesses)} processos georreferenciados em ${fmt(summary.total_municipios)} municípios`}
      action={
        <button onClick={() => void loadSummary()} disabled={loadingSummary}>
          <RefreshCw size={14} /> Atualizar
        </button>
      }
    >
      <MapFilterBar
        filters={filters}
        setFilters={setFilters}
        globalRegion={region}
        apply={applyFilters}
        clear={clearFilters}
      />

      <div className="map-summary-strip" aria-label="Resumo territorial">
        <div><span>Processos encontrados</span><strong>{fmt(totalFound)}</strong></div>
        <div><span>Com localização</span><strong>{fmt(locatedProcesses)}</strong></div>
        <div><span>Municípios</span><strong>{fmt(summary.total_municipios)}</strong></div>
        <div><span>Sem localização</span><strong>{fmt(summary.sem_localizacao)}</strong></div>
        <div><span>Recorte</span><strong>{region || appliedFilters.regiao || "Mato Grosso"}</strong></div>
      </div>

      {summaryError && <ErrorState text={summaryError} retry={() => void loadSummary()} />}

      <section className="map-shell" aria-label="Mapa de processos por município">
        <div className="map-workspace">
          <div className="map-canvas-frame">
            <div ref={mapContainerRef} className="leaflet-map" />

            {(loadingSummary || !tilesAvailable || TILE_CONFIG.warning) && (
              <div className="map-overlay-stack">
                {loadingSummary && (
                  <div className="map-status-overlay" role="status">
                    <span className="spinner" /> Carregando municípios...
                  </div>
                )}
                {!tilesAvailable && (
                  <div className="map-tile-warning" role="status">
                    <TriangleAlert size={15} />
                    Basemap indisponível. Os dados permanecem navegáveis.
                  </div>
                )}
                {TILE_CONFIG.warning && (
                  <div className="map-config-warning" role="status">
                    <TriangleAlert size={15} /> {TILE_CONFIG.warning}
                  </div>
                )}
              </div>
            )}
            {!loadingSummary && !summaryError && !summary.items.length && (
              <div className="map-empty-overlay">
                <MapPin size={20} />
                <strong>Nenhum município encontrado</strong>
                <span>Ajuste o recorte para voltar a exibir oportunidades.</span>
                <button className="secondary" onClick={clearFilters}>
                  <FilterX size={14} /> Limpar filtros
                </button>
              </div>
            )}

            <MapLegend />
          </div>

          <MunicipalPanel
            city={selectedCity}
            cities={summary.items}
            processes={processes}
            processTotal={processTotal}
            page={page}
            pageCount={pageCount}
            loading={loadingProcesses}
            error={processError}
            followingId={followingId}
            selectCity={selectCity}
            clearSelection={() => setSelectedCity(null)}
            retry={() => setProcessRefresh((current) => current + 1)}
            setPage={setPage}
            openProcess={setSelectedProcess}
            followProcess={followProcess}
          />
        </div>
      </section>

      {selectedProcess && (
        <ProcessModal
          processo={selectedProcess}
          close={() => setSelectedProcess(null)}
          follow={followProcess}
          followDisabled={followingId !== null}
        />
      )}
    </Page>
  );
}

function MapFilterBar({
  filters,
  setFilters,
  globalRegion,
  apply,
  clear,
}: {
  filters: MapFilters;
  setFilters: React.Dispatch<React.SetStateAction<MapFilters>>;
  globalRegion: string;
  apply: (event: React.FormEvent<HTMLFormElement>) => void;
  clear: () => void;
}) {
  return (
    <form className="map-filter-bar" onSubmit={apply}>
      <label className="field-compact">
        <span>Faixa</span>
        <select
          value={filters.faixa}
          onChange={(event) => setFilters((current) => ({ ...current, faixa: event.target.value }))}
        >
          <option value="">Todas as faixas</option>
          <option value="janela_quente">Janela quente</option>
          <option value="provavel">Provável perícia</option>
          <option value="observacao">Observação</option>
          <option value="frio">Frio</option>
        </select>
      </label>
      <label className="field-compact">
        <span>Região IMEA</span>
        <select
          value={globalRegion || filters.regiao}
          disabled={Boolean(globalRegion)}
          onChange={(event) => setFilters((current) => ({ ...current, regiao: event.target.value }))}
        >
          <option value="">Mato Grosso</option>
          <option>Médio-Norte</option>
          <option>Norte</option>
          <option>Centro-Sul</option>
          <option>Oeste</option>
          <option>Leste</option>
          <option>Sudoeste</option>
        </select>
      </label>
      <label className="field-compact">
        <span>Município</span>
        <input
          value={filters.municipio}
          placeholder="Buscar município"
          onChange={(event) => setFilters((current) => ({ ...current, municipio: event.target.value }))}
        />
      </label>
      <label className="field-compact">
        <span>De</span>
        <input
          type="date"
          value={filters.dataInicio}
          onChange={(event) => setFilters((current) => ({ ...current, dataInicio: event.target.value }))}
        />
      </label>
      <label className="field-compact">
        <span>Até</span>
        <input
          type="date"
          value={filters.dataFim}
          onChange={(event) => setFilters((current) => ({ ...current, dataFim: event.target.value }))}
        />
      </label>
      <div className="map-filter-actions">
        <button type="submit" className="primary"><LocateFixed size={14} /> Aplicar</button>
        <button type="button" className="secondary icon-button" onClick={clear} aria-label="Limpar filtros" title="Limpar filtros">
          <FilterX size={15} />
        </button>
      </div>
    </form>
  );
}

function MapLegend() {
  const entries = [
    ["critical", "Janela quente"],
    ["high", "Provável"],
    ["medium", "Observação"],
    ["low", "Frio"],
  ] as const;

  return (
    <div className="map-legend" aria-label="Legenda de oportunidade">
      {entries.map(([tone, label]) => (
        <span key={tone}><i className={tone} />{label}</span>
      ))}
    </div>
  );
}

function MunicipalPanel({
  city,
  cities,
  processes,
  processTotal,
  page,
  pageCount,
  loading,
  error,
  followingId,
  selectCity,
  clearSelection,
  retry,
  setPage,
  openProcess,
  followProcess,
}: {
  city: MapCitySummary | null;
  cities: MapCitySummary[];
  processes: MapProcess[];
  processTotal: number;
  page: number;
  pageCount: number;
  loading: boolean;
  error: string;
  followingId: number | null;
  selectCity: (city: MapCitySummary) => void;
  clearSelection: () => void;
  retry: () => void;
  setPage: React.Dispatch<React.SetStateAction<number>>;
  openProcess: (processo: MapProcess) => void;
  followProcess: (processo: MapProcess) => void;
}) {
  if (!city) {
    const leadingCities = [...cities]
      .sort((left, right) => right.total_processos - left.total_processos)
      .slice(0, 8);
    return (
      <aside className="map-side-panel">
        <div className="map-panel-header">
          <div>
            <span>Visão municipal</span>
            <strong>Selecione uma cidade</strong>
          </div>
        </div>
        <p className="map-panel-guidance">Use um marcador ou a lista para consultar os processos do município.</p>
        {leadingCities.length ? (
          <div className="map-city-list">
            {leadingCities.map((item) => (
              <button key={item.municipio} onClick={() => selectCity(item)}>
                <span><strong>{item.municipio}</strong><small>{item.regiao_imea || "Região não informada"}</small></span>
                <b>{fmt(item.total_processos)}</b>
              </button>
            ))}
          </div>
        ) : (
          <Empty text="Nenhum município disponível para seleção." />
        )}
      </aside>
    );
  }

  return (
    <aside className="map-side-panel" aria-label={`Processos de ${city.municipio}`}>
      <div className="map-panel-header">
        <div>
          <span>{city.regiao_imea || "Mato Grosso"}</span>
          <strong>{city.municipio}</strong>
        </div>
        <button className="secondary icon-button" onClick={clearSelection} aria-label="Fechar município" title="Fechar município">
          <X size={15} />
        </button>
      </div>
      <div className="map-city-stats">
        <div><span>Processos</span><strong>{fmt(city.total_processos)}</strong></div>
        <div><span>Maior score</span><strong>{fmt(city.maior_score)}</strong></div>
        <div><span>Quentes</span><strong>{fmt(city.processos_quentes)}</strong></div>
        <div><span>Prováveis</span><strong>{fmt(city.processos_provaveis)}</strong></div>
      </div>

      {error && <ErrorState text={error} retry={retry} />}
      {loading ? (
        <div className="map-panel-loading" role="status"><span className="spinner" /> Carregando processos...</div>
      ) : processes.length ? (
        <div className="map-process-list">
          {processes.map((processo) => {
            const id = Number(processo.id);
            const tone = markerTone(String(processo.faixa_probabilidade || ""));
            return (
              <article className="map-process-row" key={processo.id || processo.numero_cnj}>
                <div className="map-process-heading">
                  <strong>{processo.numero_cnj || "CNJ não informado"}</strong>
                  <span className={`map-score-chip ${tone}`}>{fmt(processo.score_total)}</span>
                </div>
                <span className="map-process-class">{processo.classe_processual || "Classe não informada"}</span>
                <div className="map-process-meta">
                  <span>{formatCivilDate(processo.data_distribuicao)}</span>
                  <span>{scoreLabel(processo.faixa_probabilidade)}</span>
                </div>
                <div className="map-process-actions">
                  <button className="secondary" onClick={() => openProcess(processo)}><Eye size={13} /> Detalhes</button>
                  <button className="secondary" disabled={followingId !== null} onClick={() => followProcess(processo)}><BellPlus size={13} /> Acompanhar</button>
                </div>
              </article>
            );
          })}
        </div>
      ) : (
        <Empty text="Nenhum processo disponível neste município." />
      )}

      <div className="map-panel-pagination">
        <span>{fmt(processTotal)} processos · página {page + 1} de {pageCount}</span>
        <div>
          <button className="secondary icon-button" disabled={page <= 0 || loading} onClick={() => setPage((current) => current - 1)} aria-label="Página anterior" title="Página anterior">
            <ChevronLeft size={15} />
          </button>
          <button className="secondary icon-button" disabled={page + 1 >= pageCount || loading} onClick={() => setPage((current) => current + 1)} aria-label="Próxima página" title="Próxima página">
            <ChevronRight size={15} />
          </button>
        </div>
      </div>
    </aside>
  );
}

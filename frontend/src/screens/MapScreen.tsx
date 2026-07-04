import React from "react";
import { MapPin, RefreshCw } from "lucide-react";
import { ErrorState, LoadingState } from "../components/Empty";
import { Page } from "../components/Page";
import type { ApiClient, Processo } from "../types";
import { fmt } from "../utils/format";

declare global {
  interface Window {
    L?: any;
  }
}

const CITY_COORDS: Record<string, [number, number]> = {
  "Cuiabá": [-15.601, -56.097],
  "Cuiaba": [-15.601, -56.097],
  "Várzea Grande": [-15.646, -56.132],
  "Rondonópolis": [-16.467, -54.637],
  "Sinop": [-11.860, -55.509],
  "Sorriso": [-12.542, -55.721],
  "Lucas do Rio Verde": [-13.070, -55.923],
  "Nova Mutum": [-13.837, -56.074],
  "Primavera do Leste": [-15.544, -54.281],
  "Tangará da Serra": [-14.622, -57.493],
  "Cáceres": [-16.076, -57.681],
  "Alta Floresta": [-9.875, -56.086],
  "Barra do Garças": [-15.890, -52.256],
  "Água Boa": [-14.051, -52.160],
  "Juína": [-11.423, -58.758]
};

export function MapScreen({ api, region }: { api: ApiClient; region: string }) {
  const mapRef = React.useRef<HTMLDivElement | null>(null);
  const mapInstance = React.useRef<any>(null);
  const markerLayer = React.useRef<any>(null);
  const [items, setItems] = React.useState<Processo[]>([]);
  const [status, setStatus] = React.useState("Carregando processos georreferenciados...");
  const [loading, setLoading] = React.useState(true);
  const [fallback, setFallback] = React.useState(false);
  const [error, setError] = React.useState("");

  function ensureLeaflet() {
    return new Promise<any>((resolve, reject) => {
      if (window.L) return resolve(window.L);
      if (!document.querySelector('link[data-leaflet="true"]')) {
        const link = document.createElement("link");
        link.rel = "stylesheet";
        link.href = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.css";
        link.dataset.leaflet = "true";
        document.head.appendChild(link);
      }
      const existing = document.querySelector('script[data-leaflet="true"]') as HTMLScriptElement | null;
      if (existing) {
        existing.addEventListener("load", () => resolve(window.L));
        existing.addEventListener("error", reject);
        return;
      }
      const script = document.createElement("script");
      script.src = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.js";
      script.dataset.leaflet = "true";
      script.onload = () => resolve(window.L);
      script.onerror = reject;
      document.body.appendChild(script);
    });
  }

  function coordsFor(processo: Processo): [number, number] | null {
    const lat = Number(processo.lat);
    const lng = Number(processo.lng);
    if (Number.isFinite(lat) && Number.isFinite(lng) && lat && lng) return [lat, lng];
    const city = String(processo.municipio || processo.comarca || "").trim();
    return CITY_COORDS[city] || null;
  }

  async function loadMap() {
    setLoading(true);
    setFallback(false);
    setError("");
    const params = new URLSearchParams({ limit: "120" });
    if (region) params.set("regiao", region);
    const data = await api.get<any>(`/api/processos/mapa?${params.toString()}`);
    if (!data) {
      setError("Não foi possível carregar os processos do mapa.");
      setLoading(false);
      return;
    }
    const nextItems = (data.items || []).filter((item: Processo) => coordsFor(item));
    setItems(nextItems);

    try {
      const L = await ensureLeaflet();
      if (!mapRef.current) return;
      if (!mapInstance.current) {
        mapInstance.current = L.map(mapRef.current, { center: [-13.8, -55.9], zoom: 6, zoomControl: true });
        L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
          attribution: "OpenStreetMap",
          maxZoom: 18
        }).addTo(mapInstance.current);
      }
      if (markerLayer.current) markerLayer.current.clearLayers();
      markerLayer.current = L.layerGroup().addTo(mapInstance.current);
      const pinIcon = L.divIcon({
        className: "process-pin",
        html: `
          <svg viewBox="0 0 24 24" aria-hidden="true">
            <path d="M12 2.75c-4.05 0-7.25 3.12-7.25 7.05 0 4.91 5.72 10.37 6.78 11.33a.7.7 0 0 0 .94 0c1.06-.96 6.78-6.42 6.78-11.33 0-3.93-3.2-7.05-7.25-7.05Z" />
            <circle cx="12" cy="9.8" r="2.55" />
          </svg>
        `,
        iconSize: [21, 27],
        iconAnchor: [10.5, 25],
        popupAnchor: [0, -24]
      });
      const bounds: any[] = [];
      nextItems.forEach((processo: Processo) => {
        const coords = coordsFor(processo);
        if (!coords) return;
        bounds.push(coords);
        L.marker(coords, { icon: pinIcon })
          .bindPopup(`
            <strong>${processo.numero_cnj || "Processo"}</strong><br/>
            ${processo.municipio || processo.comarca || "Município não informado"}<br/>
            Score ${processo.score_total || 0} · ${processo.classe_processual || ""}
          `)
          .addTo(markerLayer.current);
      });
      if (bounds.length) mapInstance.current.fitBounds(bounds, { padding: [34, 34], maxZoom: 8 });
      setStatus(`${fmt(nextItems.length)} processos com pin por município`);
      window.setTimeout(() => mapInstance.current?.invalidateSize(), 80);
    } catch {
      setFallback(true);
      setStatus("Mapa interativo indisponível. Pins exibidos em modo resumo.");
    } finally {
      setLoading(false);
    }
  }

  React.useEffect(() => { loadMap(); }, [api, region]);

  return (
    <Page title="Mapa Territorial" subtitle={region ? `Processos por cidade em ${region}` : "Processos com localização por município"} action={<button onClick={loadMap}><RefreshCw size={14} /> Atualizar</button>}>
      <div className="map-shell">
        <div className="map-toolbar">
          <div><MapPin size={16} /> {fmt(items.length)} pins de processos</div>
          <span>{status}</span>
        </div>
        {error && <ErrorState text={error} retry={loadMap} />}
        <div ref={mapRef} className={`leaflet-map ${fallback ? "hidden" : ""}`} />
        {loading && <LoadingState text="Carregando mapa territorial..." />}
        {!loading && fallback && !error && <FallbackPins items={items.slice(0, 12)} />}
      </div>
    </Page>
  );
}

function FallbackPins({ items }: { items: Processo[] }) {
  return (
    <div className="map-fallback">
      <div className="map-fallback-summary">
        <div><MapPin size={18} /> Pins por cidade</div>
        <strong>{fmt(items.length)} processos</strong>
      </div>
      <div className="map-fallback-grid">
        {items.length ? items.map((processo) => (
          <div className="map-fallback-item" key={processo.id || processo.numero_cnj}>
            <strong>{processo.municipio || processo.comarca || "Cidade não informada"}</strong>
            <span>{processo.numero_cnj || "CNJ pendente"}</span>
            <span>Score {processo.score_total || 0} · {processo.classe_processual || "Classe pendente"}</span>
          </div>
        )) : <span className="map-fallback-empty">Nenhum processo com município reconhecido para posicionar no mapa.</span>}
      </div>
    </div>
  );
}

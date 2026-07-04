import React from "react";
import { Layers, Map as MapIcon, RefreshCw } from "lucide-react";
import { ErrorState, LoadingState } from "../components/Empty";
import { Page } from "../components/Page";
import type { ApiClient } from "../types";
import { fmt } from "../utils/format";

declare global {
  interface Window {
    L?: any;
  }
}

type FeatureCollection = {
  type?: string;
  features?: Array<{ properties?: Record<string, any>; geometry?: any }>;
};

export function MapScreen({ api, region }: { api: ApiClient; region: string }) {
  const mapRef = React.useRef<HTMLDivElement | null>(null);
  const mapInstance = React.useRef<any>(null);
  const [municipios, setMunicipios] = React.useState<FeatureCollection | null>(null);
  const [prioritarias, setPrioritarias] = React.useState<FeatureCollection | null>(null);
  const [status, setStatus] = React.useState("Carregando camadas territoriais...");
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

  async function loadLayers() {
    setLoading(true);
    setFallback(false);
    setError("");
    setStatus("Carregando camadas territoriais...");

    const [municipiosData, prioritariasData] = await Promise.all([
      api.get<FeatureCollection>("/api/municipios/geojson"),
      api.get<FeatureCollection>("/api/parcelas/geojson?apenas_desapropriadas=true")
    ]);

    if (!municipiosData || !prioritariasData) {
      setError("Não foi possível carregar as camadas geoespaciais.");
      setMunicipios(municipiosData);
      setPrioritarias(prioritariasData);
      setLoading(false);
      return;
    }

    setMunicipios(municipiosData);
    setPrioritarias(prioritariasData);

    try {
      const L = await ensureLeaflet();
      if (!mapRef.current) return;
      if (!mapInstance.current) {
        mapInstance.current = L.map(mapRef.current, { center: [-12.5, -55.5], zoom: 6, zoomControl: true });
        L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
          attribution: "© OpenStreetMap",
          maxZoom: 18
        }).addTo(mapInstance.current);
      }
      mapInstance.current.eachLayer((layer: any) => {
        if (layer?.feature) mapInstance.current.removeLayer(layer);
      });
      if (municipiosData.features?.length) {
        L.geoJSON(municipiosData, {
          style: { color: "#7f8978", weight: 1, fillOpacity: 0.04 },
          onEachFeature: (feature: any, layer: any) => layer.bindTooltip(feature.properties?.nome || "Município", { sticky: true })
        }).addTo(mapInstance.current);
      }
      if (prioritariasData.features?.length) {
        const layer = L.geoJSON(prioritariasData, {
          style: { color: "#8a2424", weight: 1.5, fillColor: "#c0392b", fillOpacity: 0.42 },
          onEachFeature: (feature: any, layer: any) => {
            const props = feature.properties || {};
            layer.bindPopup(`<strong>${props.municipio || "Imóvel rural"}</strong><br/>${props.codigo_imovel || ""}<br/>${props.area_ha ? Number(props.area_ha).toLocaleString("pt-BR") + " ha" : ""}`);
          }
        }).addTo(mapInstance.current);
        try { mapInstance.current.fitBounds(layer.getBounds(), { padding: [18, 18] }); } catch {}
      }
      setStatus("Camadas geoespaciais carregadas");
    } catch {
      setFallback(true);
      setStatus("Mapa interativo indisponível. Resumo territorial carregado pela API.");
    } finally {
      setLoading(false);
    }
  }

  React.useEffect(() => { loadLayers(); }, [api]);

  const municipiosCount = municipios?.features?.length || 0;
  const prioritariasCount = prioritarias?.features?.length || 0;
  const samples = (prioritarias?.features || []).slice(0, 6).map((feature) => feature.properties || {});

  return (
    <Page title="Mapa Territorial" subtitle={region ? `Camadas territoriais em ${region}` : "Camadas geoespaciais de Mato Grosso"} action={<button onClick={loadLayers}><RefreshCw size={14} /> Atualizar</button>}>
      <div className="map-shell">
        <div className="map-toolbar">
          <div><Layers size={16} /> {fmt(prioritariasCount)} imóveis em camada prioritária</div>
          <span>{status}</span>
        </div>
        {error && <ErrorState text={error} retry={loadLayers} />}
        {loading && <LoadingState text="Carregando mapa territorial..." />}
        {!loading && !fallback && !error && <div ref={mapRef} className="leaflet-map" />}
        {!loading && fallback && !error && (
          <div className="map-fallback">
            <div className="map-fallback-summary">
              <div><MapIcon size={18} /> Modo resumo</div>
              <strong>{fmt(municipiosCount)} municípios</strong>
              <strong>{fmt(prioritariasCount)} imóveis prioritários</strong>
            </div>
            <div className="map-fallback-grid">
              {samples.length ? samples.map((props, index) => (
                <div className="map-fallback-item" key={`${props.codigo_imovel || props.municipio || index}`}>
                  <strong>{props.municipio || "Município não informado"}</strong>
                  <span>{props.codigo_imovel || "Código do imóvel pendente"}</span>
                  <span>{props.area_ha ? `${Number(props.area_ha).toLocaleString("pt-BR")} ha` : "Área não informada"}</span>
                </div>
              )) : <span className="map-fallback-empty">Nenhum imóvel prioritário retornado pela API.</span>}
            </div>
          </div>
        )}
      </div>
    </Page>
  );
}

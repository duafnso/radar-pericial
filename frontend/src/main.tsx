import React from "react";
import { createRoot } from "react-dom/client";
import { Layers, RefreshCw } from "lucide-react";
import { useApiClient } from "./api/client";
import { ROLE_PERMISSIONS, SCREEN_PERMISSIONS } from "./auth/permissions";
import { CardLine } from "./components/CardLine";
import { DataTable } from "./components/DataTable";
import { Empty } from "./components/Empty";
import { Page } from "./components/Page";
import { Sidebar } from "./layout/Sidebar";
import { Coletas } from "./screens/Coletas";
import { Dashboard } from "./screens/Dashboard";
import { Processos } from "./screens/Processos";
import { useLocalState } from "./hooks/useLocalState";
import type { ApiClient, ApiUser, Screen } from "./types";
import { fmt, shortDate } from "./utils/format";
import "./styles.css";

declare global {
  interface Window {
    L?: any;
  }
}

function App() {
  const [token, setToken] = useLocalState<string | null>("radar_token", null);
  const [user, setUser] = useLocalState<ApiUser | null>("radar_user", null);
  const [screen, setScreen] = React.useState<Screen>("dashboard");
  const [toast, setToast] = React.useState("");
  const [region, setRegion] = useLocalState<string>("radar_region", "");
  const api = useApiClient(token, setToken, setUser);

  const hasPermission = React.useCallback((permission?: string) => {
    if (!permission) return true;
    const role = user?.role || "viewer";
    return (ROLE_PERMISSIONS[role] || []).includes(permission);
  }, [user]);

  const notify = React.useCallback((message: string) => {
    setToast(message);
    window.setTimeout(() => setToast(""), 2800);
  }, []);

  React.useEffect(() => {
    if (!token) return;
    api.get<{ user: ApiUser }>("/api/me").then((data) => {
      if (data?.user) setUser(data.user);
    });
  }, [api, token, setUser]);

  const navigate = (next: Screen) => {
    const required = SCREEN_PERMISSIONS[next];
    if (required && !hasPermission(required)) {
      notify("Seu perfil não tem permissão para acessar esta área.");
      setScreen("dashboard");
      return;
    }
    setScreen(next);
  };

  if (!token || !user) {
    return <Login api={api} setToken={setToken} setUser={setUser} notify={notify} />;
  }

  return (
    <div className="app">
      <Sidebar
        screen={screen}
        navigate={navigate}
        user={user}
        region={region}
        setRegion={setRegion}
        hasPermission={hasPermission}
        logout={() => {
          setToken(null);
          setUser(null);
        }}
      />
      <main className="workspace">
        {screen === "dashboard" && <Dashboard api={api} region={region} navigate={navigate} hasPermission={hasPermission} />}
        {screen === "mapa" && <MapScreen api={api} region={region} />}
        {screen === "processos" && <Processos api={api} region={region} />}
        {screen === "administrativo" && <Administrativo api={api} />}
        {screen === "score" && <ScoreCalculator api={api} hasPermission={hasPermission} />}
        {screen === "peritos" && <Peritos api={api} />}
        {screen === "alertas" && <Alertas api={api} />}
        {screen === "coletas" && <Coletas api={api} hasPermission={hasPermission} notify={notify} />}
        {screen === "usuarios" && <Usuarios api={api} hasPermission={hasPermission} notify={notify} />}
        {screen === "auditoria" && <Auditoria api={api} />}
      </main>
      <div className={`toast ${toast ? "show" : ""}`}>{toast}</div>
    </div>
  );
}

function Login({ api, setToken, setUser, notify }: {
  api: ApiClient;
  setToken: (token: string | null) => void;
  setUser: (user: ApiUser | null) => void;
  notify: (message: string) => void;
}) {
  const [username, setUsername] = React.useState("");
  const [password, setPassword] = React.useState("");
  const [loading, setLoading] = React.useState(false);

  async function submit(event: React.FormEvent) {
    event.preventDefault();
    setLoading(true);
    const data = await api.post<{ status: string; token: string; user: ApiUser }>("/api/login", { username, password });
    setLoading(false);
    if (data?.status === "ok") {
      setToken(data.token);
      setUser(data.user);
    } else {
      notify("Credenciais inválidas ou acesso indisponível.");
    }
  }

  return (
    <div className="login-screen">
      <form className="login-panel" onSubmit={submit}>
        <div className="login-brand">Radar Pericial</div>
        <div className="login-subtitle">Inteligência judicial e territorial para perícia agronômica</div>
        <label>Usuário</label>
        <input value={username} onChange={(event) => setUsername(event.target.value)} autoComplete="username" required />
        <label>Senha</label>
        <input value={password} onChange={(event) => setPassword(event.target.value)} type="password" autoComplete="current-password" required />
        <button className="primary" disabled={loading}>{loading ? "Entrando..." : "Entrar"}</button>
      </form>
    </div>
  );
}

function ScoreCalculator({ api, hasPermission }: { api: ApiClient; hasPermission: (permission?: string) => boolean }) {
  const [classe, setClasse] = React.useState("");
  const [assunto, setAssunto] = React.useState("");
  const [texto, setTexto] = React.useState("");
  const [result, setResult] = React.useState<any>(null);

  async function calculate() {
    if (!hasPermission("calculate_score")) return;
    const data = await api.post<any>("/api/score/calcular", { classe_processual: classe, assunto, texto_livre: texto });
    setResult(data);
  }

  return (
    <Page title="Calculadora Pericial" subtitle="Simule a probabilidade de demanda pericial">
      <div className="form-grid">
        <input placeholder="Classe processual" value={classe} onChange={(event) => setClasse(event.target.value)} />
        <input placeholder="Assunto principal" value={assunto} onChange={(event) => setAssunto(event.target.value)} />
        <textarea placeholder="Texto livre, movimentações ou publicação" value={texto} onChange={(event) => setTexto(event.target.value)} />
        <button onClick={calculate}>Calcular score</button>
      </div>
      {result && (
        <div className="score-result">
          <strong>{result.score_total ?? result.score?.score_total ?? 0}</strong>
          <span>{result.faixa_label || result.score?.faixa_label || "Resultado calculado"}</span>
        </div>
      )}
    </Page>
  );
}

function SimpleListScreen({ title, subtitle, endpoint, render }: {
  title: string;
  subtitle: string;
  endpoint: { api: ApiClient; path: string };
  render: (item: any, index: number) => React.ReactNode;
}) {
  const [items, setItems] = React.useState<any[]>([]);

  async function load() {
    const data = await endpoint.api.get<any>(endpoint.path);
    setItems(data?.items || data || []);
  }

  React.useEffect(() => { load(); }, []);

  return (
    <Page title={title} subtitle={subtitle} action={<button onClick={load}><RefreshCw size={14} /> Atualizar</button>}>
      <div className="stack">
        {items.length ? items.map(render) : <Empty text="Nenhum registro encontrado." />}
      </div>
    </Page>
  );
}

function Administrativo({ api }: { api: ApiClient }) {
  return (
    <SimpleListScreen
      title="Radar Administrativo"
      subtitle="Eventos administrativos relevantes"
      endpoint={{ api, path: "/api/eventos?limit=50&dias=90" }}
      render={(item, index) => <CardLine key={index} title={item.titulo || item.descricao || item.fonte} meta={`${item.fonte || ""} · ${item.municipio || ""}`} />}
    />
  );
}

function Peritos({ api }: { api: ApiClient }) {
  return (
    <SimpleListScreen
      title="Corpo Pericial"
      subtitle="Profissionais cadastrados"
      endpoint={{ api, path: "/api/peritos" }}
      render={(item, index) => <CardLine key={index} title={item.nome || "Profissional"} meta={`${item.registro_profissional || ""} · ${item.regiao_imea || ""}`} />}
    />
  );
}

function Alertas({ api }: { api: ApiClient }) {
  return (
    <SimpleListScreen
      title="Central de Alertas"
      subtitle="Eventos e oportunidades recentes"
      endpoint={{ api, path: "/api/alertas?limit=40" }}
      render={(item, index) => <CardLine key={index} title={item.titulo || item.orgao || "Alerta"} meta={`${item.fonte || ""} · Score ${item.score_evento || item.score_total || 0}`} />}
    />
  );
}

function MapScreen({ api, region }: { api: ApiClient; region: string }) {
  const mapRef = React.useRef<HTMLDivElement | null>(null);
  const mapInstance = React.useRef<any>(null);
  const [count, setCount] = React.useState(0);
  const [status, setStatus] = React.useState("Carregando camadas territoriais...");

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

  React.useEffect(() => {
    let cancelled = false;
    async function loadMap() {
      try {
        const L = await ensureLeaflet();
        if (cancelled || !mapRef.current) return;
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
        const [municipios, prioritarias] = await Promise.all([
          api.get<any>("/api/municipios/geojson"),
          api.get<any>("/api/parcelas/geojson?apenas_desapropriadas=true")
        ]);
        if (cancelled) return;
        if (municipios?.features?.length) {
          L.geoJSON(municipios, {
            style: { color: "#7f8978", weight: 1, fillOpacity: 0.04 },
            onEachFeature: (feature: any, layer: any) => layer.bindTooltip(feature.properties?.nome || "Município", { sticky: true })
          }).addTo(mapInstance.current);
        }
        if (prioritarias?.features?.length) {
          const layer = L.geoJSON(prioritarias, {
            style: { color: "#8a2424", weight: 1.5, fillColor: "#c0392b", fillOpacity: 0.42 },
            onEachFeature: (feature: any, layer: any) => {
              const p = feature.properties || {};
              layer.bindPopup(`<strong>${p.municipio || "Imóvel rural"}</strong><br/>${p.codigo_imovel || ""}<br/>${p.area_ha ? Number(p.area_ha).toLocaleString("pt-BR") + " ha" : ""}`);
            }
          }).addTo(mapInstance.current);
          try { mapInstance.current.fitBounds(layer.getBounds(), { padding: [18, 18] }); } catch {}
        }
        setCount(prioritarias?.features?.length || 0);
        setStatus("Camadas geoespaciais carregadas");
      } catch {
        setStatus("Não foi possível carregar o mapa interativo. Verifique conexão com CDN do Leaflet.");
      }
    }
    loadMap();
    return () => { cancelled = true; };
  }, [api]);

  return (
    <Page title="Mapa Territorial" subtitle={region ? `Camadas territoriais em ${region}` : "Camadas geoespaciais de Mato Grosso"}>
      <div className="map-shell">
        <div className="map-toolbar">
          <div><Layers size={16} /> {fmt(count)} imóveis em camada prioritária</div>
          <span>{status}</span>
        </div>
        <div ref={mapRef} className="leaflet-map" />
      </div>
    </Page>
  );
}

function Usuarios({ api, hasPermission, notify }: {
  api: ApiClient;
  hasPermission: (permission?: string) => boolean;
  notify: (message: string) => void;
}) {
  const [items, setItems] = React.useState<any[]>([]);

  async function load() {
    if (!hasPermission("manage_users")) return;
    const data = await api.get<any>("/api/admin/usuarios");
    setItems(data?.items || []);
  }

  React.useEffect(() => { load(); }, []);

  async function changeRole(id: number, role: string) {
    const result = await api.patch<any>(`/api/admin/usuarios/${id}/role`, { role });
    notify(result?.status === "ok" ? "Role atualizada." : "Não foi possível atualizar role.");
    load();
  }

  return (
    <Page title="Usuários e Permissões" subtitle={`${items.length} usuários cadastrados`} action={<button onClick={load}>Atualizar</button>}>
      <DataTable headers={["ID", "Usuário", "Role", "Status", "Região", "Criado em"]}>
        {items.map((item) => (
          <tr key={item.id}>
            <td>{item.id}</td>
            <td>{item.username}</td>
            <td>
              <select value={item.role} onChange={(event) => changeRole(item.id, event.target.value)}>
                {["admin", "operator", "user", "viewer"].map((role) => <option key={role}>{role}</option>)}
              </select>
            </td>
            <td>{item.ativo ? "ativo" : "inativo"}</td>
            <td>{item.regiao_foco || "--"}</td>
            <td>{shortDate(item.criado_em)}</td>
          </tr>
        ))}
      </DataTable>
    </Page>
  );
}

function Auditoria({ api }: { api: ApiClient }) {
  const [items, setItems] = React.useState<any[]>([]);

  async function load() {
    const data = await api.get<any>("/api/admin/auditoria?limit=100");
    setItems(data?.items || []);
  }

  React.useEffect(() => { load(); }, []);

  return (
    <Page title="Auditoria Administrativa" subtitle={`${items.length} eventos recentes`} action={<button onClick={load}>Atualizar</button>}>
      <DataTable headers={["Data", "Ator", "Ação", "Entidade", "IP"]}>
        {items.map((item) => (
          <tr key={item.id}>
            <td>{shortDate(item.criado_em)}</td>
            <td>{item.ator_username || "--"}</td>
            <td>{item.acao}</td>
            <td>{item.entidade || "--"}</td>
            <td>{item.ip || "--"}</td>
          </tr>
        ))}
      </DataTable>
    </Page>
  );
}

createRoot(document.getElementById("root")!).render(<App />);

import React from "react";
import { createRoot } from "react-dom/client";
import {
  Activity,
  AlertTriangle,
  Bell,
  Calculator,
  Database,
  FileText,
  Gavel,
  Layers,
  LogOut,
  Map,
  RefreshCw,
  Shield,
  Users,
  UserRoundCog
} from "lucide-react";
import "./styles.css";

declare global {
  interface Window {
    L?: any;
  }
}

type Role = "admin" | "operator" | "user" | "viewer";
type Screen =
  | "dashboard"
  | "mapa"
  | "processos"
  | "administrativo"
  | "score"
  | "peritos"
  | "alertas"
  | "coletas"
  | "usuarios"
  | "auditoria";

type ApiUser = { id?: number; username: string; role: Role; regiao_foco?: string | null };
type Processo = Record<string, any>;
type Coleta = Record<string, any>;

const ROLE_PERMISSIONS: Record<Role, string[]> = {
  admin: ["read_data", "read_operational", "calculate_score", "create_perito", "run_collections", "manage_users", "view_audit"],
  operator: ["read_data", "read_operational", "calculate_score", "create_perito", "run_collections"],
  user: ["read_data", "calculate_score"],
  viewer: ["read_data"]
};

const SCREEN_PERMISSIONS: Partial<Record<Screen, string>> = {
  coletas: "read_operational",
  usuarios: "manage_users",
  auditoria: "view_audit"
};

const NAV = [
  { section: "Inteligência", items: [
    { id: "dashboard" as Screen, label: "Painel", icon: Activity },
    { id: "mapa" as Screen, label: "Mapa Territorial", icon: Map },
    { id: "processos" as Screen, label: "Radar de Processos", icon: Gavel },
    { id: "administrativo" as Screen, label: "Radar Administrativo", icon: FileText }
  ]},
  { section: "Ferramentas", items: [
    { id: "score" as Screen, label: "Calculadora Pericial", icon: Calculator },
    { id: "peritos" as Screen, label: "Corpo Pericial", icon: Users },
    { id: "alertas" as Screen, label: "Central de Alertas", icon: Bell }
  ]},
  { section: "Administração", items: [
    { id: "coletas" as Screen, label: "Operação de Coletas", icon: Database, permission: "read_operational" },
    { id: "usuarios" as Screen, label: "Usuários", icon: UserRoundCog, permission: "manage_users" },
    { id: "auditoria" as Screen, label: "Auditoria", icon: Shield, permission: "view_audit" }
  ]}
];

function fmt(value: any, digits = 0) {
  const n = Number(value || 0);
  return Number.isFinite(n) ? n.toLocaleString("pt-BR", { maximumFractionDigits: digits }) : "0";
}

function shortDate(value?: string | null) {
  if (!value) return "--";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return String(value).slice(0, 16);
  return d.toLocaleString("pt-BR", { day: "2-digit", month: "2-digit", year: "2-digit", hour: "2-digit", minute: "2-digit" });
}

function scoreClass(score: any) {
  const n = Number(score || 0);
  if (n >= 75) return "critical";
  if (n >= 50) return "high";
  if (n >= 25) return "medium";
  return "low";
}

function scoreLabel(faixa?: string) {
  const map: Record<string, string> = {
    janela_quente: "Janela quente",
    provavel: "Provável perícia",
    observacao: "Observação",
    frio: "Frio"
  };
  return map[faixa || ""] || faixa || "Sem faixa";
}

function useLocalState<T>(key: string, initial: T) {
  const [value, setValue] = React.useState<T>(() => {
    const raw = localStorage.getItem(key);
    if (!raw) return initial;
    try { return JSON.parse(raw) as T; } catch { return initial; }
  });
  React.useEffect(() => localStorage.setItem(key, JSON.stringify(value)), [key, value]);
  return [value, setValue] as const;
}

function App() {
  const [token, setToken] = useLocalState<string | null>("radar_token", null);
  const [user, setUser] = useLocalState<ApiUser | null>("radar_user", null);
  const [screen, setScreen] = React.useState<Screen>("dashboard");
  const [toast, setToast] = React.useState("");
  const [region, setRegion] = useLocalState<string>("radar_region", "");

  const hasPermission = React.useCallback((permission?: string) => {
    if (!permission) return true;
    const role = user?.role || "viewer";
    return (ROLE_PERMISSIONS[role] || []).includes(permission);
  }, [user]);

  const notify = React.useCallback((message: string) => {
    setToast(message);
    window.setTimeout(() => setToast(""), 2800);
  }, []);

  const api = React.useMemo(() => ({
    async request<T>(path: string, options: RequestInit = {}): Promise<T | null> {
      const headers: Record<string, string> = { ...(options.headers as Record<string, string> || {}) };
      if (token) headers.Authorization = `Bearer ${token}`;
      if (options.body && !headers["Content-Type"]) headers["Content-Type"] = "application/json";
      const response = await fetch(path, { ...options, headers });
      if (response.status === 401) {
        setToken(null);
        setUser(null);
        return null;
      }
      if (!response.ok) return null;
      return response.json();
    },
    get<T>(path: string) { return this.request<T>(path); },
    post<T>(path: string, body?: unknown) { return this.request<T>(path, { method: "POST", body: JSON.stringify(body || {}) }); },
    patch<T>(path: string, body?: unknown) { return this.request<T>(path, { method: "PATCH", body: JSON.stringify(body || {}) }); }
  }), [token, setToken, setUser]);

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
        logout={() => { setToken(null); setUser(null); }}
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

function Login({ api, setToken, setUser, notify }: any) {
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
        <input value={username} onChange={(e) => setUsername(e.target.value)} autoComplete="username" required />
        <label>Senha</label>
        <input value={password} onChange={(e) => setPassword(e.target.value)} type="password" autoComplete="current-password" required />
        <button className="primary" disabled={loading}>{loading ? "Entrando..." : "Entrar"}</button>
      </form>
    </div>
  );
}

function Sidebar({ screen, navigate, user, region, setRegion, hasPermission, logout }: any) {
  return (
    <aside className="sidebar-global">
      <div className="brand">
        <strong>Radar Pericial</strong>
        <span>Inteligência Fundiária</span>
      </div>
      <nav>
        {NAV.map((group) => (
          <div key={group.section} className="nav-group">
            <div className="nav-section">{group.section}</div>
            {group.items.filter((item: any) => hasPermission(item.permission)).map((item: any) => {
              const Icon = item.icon;
              return (
                <button key={item.id} className={screen === item.id ? "active" : ""} onClick={() => navigate(item.id)}>
                  <Icon size={16} /> <span>{item.label}</span>
                </button>
              );
            })}
          </div>
        ))}
      </nav>
      <div className="sidebar-footer">
        <select value={region} onChange={(e) => setRegion(e.target.value)}>
          <option value="">Mato Grosso inteiro</option>
          <option value="Médio-Norte">Médio-Norte</option>
          <option value="Norte">Norte</option>
          <option value="Centro-Sul">Centro-Sul</option>
          <option value="Oeste">Oeste</option>
          <option value="Leste">Leste</option>
          <option value="Sudoeste">Sudoeste</option>
        </select>
        <div className="status-line"><span className="dot" /> Perfil {user.role}</div>
        <div className="status-muted">{region ? `Foco: ${region}` : "Sistema ativo em MT"}</div>
        <button className="ghost" onClick={logout}><LogOut size={14} /> Sair</button>
      </div>
    </aside>
  );
}

function Page({ title, subtitle, action, children }: any) {
  return (
    <section className="page">
      <header className="page-header">
        <div>
          <h1>{title}</h1>
          <p>{subtitle}</p>
        </div>
        {action}
      </header>
      <div className="page-content">{children}</div>
    </section>
  );
}

function Dashboard({ api, region, navigate, hasPermission }: any) {
  const [stats, setStats] = React.useState<any>(null);
  const [processos, setProcessos] = React.useState<Processo[]>([]);
  const [coletas, setColetas] = React.useState<Coleta[]>([]);

  async function load() {
    const suffix = region ? `?regiao=${encodeURIComponent(region)}` : "";
    const qs = `?faixa=janela_quente&limit=4${region ? `&regiao=${encodeURIComponent(region)}` : ""}`;
    const [s, p, c] = await Promise.all([
      api.get<any>(`/api/stats${suffix}`),
      api.get<any>(`/api/processos${qs}`),
      hasPermission("read_operational") ? api.get<any>("/api/coletas/status?limit=8") : Promise.resolve(null)
    ]);
    setStats(s);
    setProcessos(p?.items || []);
    setColetas(c?.items || []);
  }

  React.useEffect(() => { load(); }, [region]);

  const failed = coletas.filter((c) => c.status === "failed").length;
  const running = coletas.filter((c) => c.status === "running").length;

  return (
    <Page title="Painel de Inteligência Pericial" subtitle={region ? `Foco regional: ${region}` : "Mato Grosso inteiro"} action={<button onClick={load}><RefreshCw size={14} /> Atualizar</button>}>
      <div className="metrics">
        <Metric label="Processos" value={fmt(stats?.total_processos)} />
        <Metric label="Janela quente" value={fmt(stats?.processos_quentes)} tone="critical" />
        <Metric label="Provável perícia" value={fmt(stats?.processos_provaveis)} tone="high" />
        <Metric label="Imóveis SIGEF" value={fmt(stats?.total_parcelas)} />
        <Metric label="Portarias D.O." value={fmt(stats?.total_portarias)} />
        <Metric label="Alertas" value={fmt(stats?.processos_quentes)} />
      </div>
      {hasPermission("read_operational") && (
        <div className="card">
          <div className="card-title">Saúde das coletas</div>
          <div className="health-grid">
            <StatusBlock label="Operação" value={failed ? `${failed} falha(s)` : running ? `${running} em execução` : "Estável"} tone={failed ? "critical" : running ? "medium" : "high"} />
            <StatusBlock label="Última coleta" value={coletas[0] ? `${coletas[0].fonte} · ${shortDate(coletas[0].iniciado_em)}` : "Sem registro"} />
            <StatusBlock label="DataJud" value="Pode demorar por limite da API externa" />
            <button className="secondary" onClick={() => navigate("coletas")}>Abrir operação de coletas</button>
          </div>
        </div>
      )}
      <div className="two-col">
        <div>
          <div className="section-label">Últimos processos relevantes</div>
          <div className="stack">
            {processos.length ? processos.map((p) => <ProcessCard key={p.id || p.numero_cnj} processo={p} />) : <Empty text="Nenhum processo quente encontrado." />}
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

function Metric({ label, value, tone }: any) {
  return <div className={`metric ${tone || ""}`}><span>{label}</span><strong>{value}</strong></div>;
}

function StatusBlock({ label, value, tone }: any) {
  return <div className={`status-block ${tone || ""}`}><span>{label}</span><strong>{value}</strong></div>;
}

function ProcessCard({ processo }: { processo: Processo }) {
  return (
    <article className={`process-card ${scoreClass(processo.score_total)}`}>
      <div>
        <strong>{processo.classe_processual || "Classe não informada"}</strong>
        <span>{processo.numero_cnj || "CNJ não informado"} · {processo.municipio || processo.comarca || "Município pendente"}</span>
        <p>{processo.assunto_principal || processo.fase_atual || "Sem resumo disponível"}</p>
      </div>
      <div className="score">
        <strong>{processo.score_total || 0}</strong>
        <span>{scoreLabel(processo.faixa_probabilidade)}</span>
      </div>
    </article>
  );
}

function Processos({ api, region }: any) {
  const [items, setItems] = React.useState<Processo[]>([]);
  const [total, setTotal] = React.useState(0);
  const [filters, setFilters] = React.useState({ faixa: "", regiao: "", municipio: "", classe: "" });

  async function load() {
    const params = new URLSearchParams({ limit: "50", offset: "0" });
    const effectiveRegion = region || filters.regiao;
    if (filters.faixa) params.set("faixa", filters.faixa);
    if (effectiveRegion) params.set("regiao", effectiveRegion);
    if (filters.municipio) params.set("municipio", filters.municipio);
    if (filters.classe) params.set("classe", filters.classe);
    const data = await api.get<any>(`/api/processos?${params.toString()}`);
    setItems(data?.items || []);
    setTotal(data?.total || 0);
  }

  React.useEffect(() => { load(); }, [region, filters]);

  return (
    <Page title="Radar de Processos Judiciais" subtitle={`${fmt(total)} processos · dados da última coleta judicial`} action={<button onClick={load}><RefreshCw size={14} /> Atualizar</button>}>
      <FilterBar filters={filters} setFilters={setFilters} />
      {items.length ? <div className="stack">{items.map((p) => <ProcessCard key={p.id || p.numero_cnj} processo={p} />)}</div> : <Empty text="Nenhum processo encontrado com estes filtros." />}
    </Page>
  );
}

function FilterBar({ filters, setFilters }: any) {
  return (
    <div className="filters">
      <select value={filters.faixa} onChange={(e) => setFilters({ ...filters, faixa: e.target.value })}>
        <option value="">Todas as faixas</option>
        <option value="janela_quente">Janela quente</option>
        <option value="provavel">Provável perícia</option>
        <option value="observacao">Observação</option>
        <option value="frio">Frio</option>
      </select>
      <select value={filters.regiao} onChange={(e) => setFilters({ ...filters, regiao: e.target.value })}>
        <option value="">Todas as regiões</option>
        <option>Médio-Norte</option><option>Norte</option><option>Centro-Sul</option><option>Oeste</option><option>Leste</option><option>Sudoeste</option>
      </select>
      <input placeholder="Município" value={filters.municipio} onChange={(e) => setFilters({ ...filters, municipio: e.target.value })} />
      <input placeholder="Classe processual" value={filters.classe} onChange={(e) => setFilters({ ...filters, classe: e.target.value })} />
    </div>
  );
}

function Coletas({ api, hasPermission, notify }: any) {
  const [items, setItems] = React.useState<Coleta[]>([]);
  async function load() {
    const data = await api.get<any>("/api/coletas/status?limit=50");
    setItems(data?.items || []);
  }
  React.useEffect(() => { load(); }, []);

  async function run(tipo: string) {
    if (!hasPermission("run_collections")) return notify("Seu perfil não pode executar coletas.");
    const data = await api.post<any>(`/api/coletas/${tipo}/executar`, {});
    if (data?.status === "queued") {
      notify(`Coleta ${tipo} enfileirada.`);
      window.setTimeout(load, 900);
    } else {
      notify("Não foi possível enfileirar a coleta.");
    }
  }

  return (
    <Page title="Operação de Coletas" subtitle={`${items.length} execuções recentes`} action={<button onClick={load}><RefreshCw size={14} /> Atualizar</button>}>
      <div className="card">
        <div className="card-title">Disparo manual</div>
        <div className="button-row">
          {["geo", "judicial", "admin", "score"].map((t) => <button key={t} className="secondary" onClick={() => run(t)}>Executar {t}</button>)}
        </div>
      </div>
      <DataTable headers={["Fonte", "Status", "Início", "Duração", "Progresso", "Fim", "Erro"]}>
        {items.map((item) => <tr key={item.id}>
          <td>{item.fonte}</td>
          <td><Badge status={item.status} /></td>
          <td>{shortDate(item.iniciado_em)}</td>
          <td>{item.duracao_segundos ? `${fmt(item.duracao_segundos, 1)}s` : "--"}</td>
          <td>{fmt(item.registros_salvos)} salvos de {fmt(item.registros_coletados)} coletados</td>
          <td>{item.finalizado_em ? shortDate(item.finalizado_em) : "Em andamento"}</td>
          <td className="wrap">{friendlyError(item.erro)}</td>
        </tr>)}
      </DataTable>
    </Page>
  );
}

function Badge({ status }: { status?: string }) {
  const cls = status === "success" ? "high" : status === "failed" ? "critical" : "medium";
  const label = status === "success" ? "finalizada" : status === "failed" ? "falhou" : "em execução";
  return <span className={`badge ${cls}`}>{label}</span>;
}

function friendlyError(error?: string) {
  if (!error) return "--";
  const lower = error.toLowerCase();
  if (lower.includes("429")) return "Limite da fonte externa. Aguarde antes de tentar novamente.";
  if (lower.includes("401") || lower.includes("apikey")) return "Chave da API inválida ou ausente.";
  if (lower.includes("timeout") || lower.includes("timed out")) return "A fonte externa demorou para responder.";
  return error.slice(0, 160);
}

function ScoreCalculator({ api, hasPermission }: any) {
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
        <input placeholder="Classe processual" value={classe} onChange={(e) => setClasse(e.target.value)} />
        <input placeholder="Assunto principal" value={assunto} onChange={(e) => setAssunto(e.target.value)} />
        <textarea placeholder="Texto livre, movimentações ou publicação" value={texto} onChange={(e) => setTexto(e.target.value)} />
        <button onClick={calculate}>Calcular score</button>
      </div>
      {result && <div className="score-result"><strong>{result.score_total ?? result.score?.score_total ?? 0}</strong><span>{result.faixa_label || result.score?.faixa_label || "Resultado calculado"}</span></div>}
    </Page>
  );
}

function SimpleListScreen({ title, subtitle, endpoint, render }: any) {
  const [items, setItems] = React.useState<any[]>([]);
  const [apiRef] = React.useState(() => endpoint.api);
  async function load() {
    const data = await apiRef.get<any>(endpoint.path);
    setItems(data?.items || data || []);
  }
  React.useEffect(() => { load(); }, []);
  return <Page title={title} subtitle={subtitle} action={<button onClick={load}><RefreshCw size={14} /> Atualizar</button>}><div className="stack">{items.length ? items.map(render) : <Empty text="Nenhum registro encontrado." />}</div></Page>;
}

function Administrativo({ api }: any) {
  return <SimpleListScreen title="Radar Administrativo" subtitle="Eventos administrativos relevantes" endpoint={{ api, path: "/api/eventos?limit=50&dias=90" }} render={(e: any, i: number) => <CardLine key={i} title={e.titulo || e.descricao || e.fonte} meta={`${e.fonte || ""} · ${e.municipio || ""}`} />} />;
}

function Peritos({ api }: any) {
  return <SimpleListScreen title="Corpo Pericial" subtitle="Profissionais cadastrados" endpoint={{ api, path: "/api/peritos" }} render={(p: any, i: number) => <CardLine key={i} title={p.nome || "Profissional"} meta={`${p.registro_profissional || ""} · ${p.regiao_imea || ""}`} />} />;
}

function Alertas({ api }: any) {
  return <SimpleListScreen title="Central de Alertas" subtitle="Eventos e oportunidades recentes" endpoint={{ api, path: "/api/alertas?limit=40" }} render={(a: any, i: number) => <CardLine key={i} title={a.titulo || a.orgao || "Alerta"} meta={`${a.fonte || ""} · Score ${a.score_evento || a.score_total || 0}`} />} />;
}

function MapScreen({ api, region }: any) {
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

function Usuarios({ api, hasPermission, notify }: any) {
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
        {items.map((u) => <tr key={u.id}>
          <td>{u.id}</td><td>{u.username}</td>
          <td><select value={u.role} onChange={(e) => changeRole(u.id, e.target.value)}>{["admin","operator","user","viewer"].map((r) => <option key={r}>{r}</option>)}</select></td>
          <td>{u.ativo ? "ativo" : "inativo"}</td><td>{u.regiao_foco || "--"}</td><td>{shortDate(u.criado_em)}</td>
        </tr>)}
      </DataTable>
    </Page>
  );
}

function Auditoria({ api }: any) {
  const [items, setItems] = React.useState<any[]>([]);
  async function load() {
    const data = await api.get<any>("/api/admin/auditoria?limit=100");
    setItems(data?.items || []);
  }
  React.useEffect(() => { load(); }, []);
  return (
    <Page title="Auditoria Administrativa" subtitle={`${items.length} eventos recentes`} action={<button onClick={load}>Atualizar</button>}>
      <DataTable headers={["Data", "Ator", "Ação", "Entidade", "IP"]}>
        {items.map((ev) => <tr key={ev.id}><td>{shortDate(ev.criado_em)}</td><td>{ev.ator_username || "--"}</td><td>{ev.acao}</td><td>{ev.entidade || "--"}</td><td>{ev.ip || "--"}</td></tr>)}
      </DataTable>
    </Page>
  );
}

function DataTable({ headers, children }: any) {
  return <div className="table-wrap"><table><thead><tr>{headers.map((h: string) => <th key={h}>{h}</th>)}</tr></thead><tbody>{children}</tbody></table></div>;
}

function CardLine({ title, meta }: any) {
  return <div className="card-line"><strong>{title || "Sem título"}</strong><span>{meta || "Sem metadados"}</span></div>;
}

function Empty({ text }: { text: string }) {
  return <div className="empty"><AlertTriangle size={20} /><span>{text}</span></div>;
}

createRoot(document.getElementById("root")!).render(<App />);

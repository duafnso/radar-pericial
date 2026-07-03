import { Activity, Bell, Calculator, Database, FileText, Gavel, LogOut, Map, Shield, Users, UserRoundCog } from "lucide-react";
import type { ApiUser, NavItem, Screen } from "../types";

const NAV: Array<{ section: string; items: NavItem[] }> = [
  {
    section: "Inteligência",
    items: [
      { id: "dashboard", label: "Painel", icon: Activity },
      { id: "mapa", label: "Mapa Territorial", icon: Map },
      { id: "processos", label: "Radar de Processos", icon: Gavel },
      { id: "administrativo", label: "Radar Administrativo", icon: FileText }
    ]
  },
  {
    section: "Ferramentas",
    items: [
      { id: "score", label: "Calculadora Pericial", icon: Calculator },
      { id: "peritos", label: "Corpo Pericial", icon: Users },
      { id: "alertas", label: "Central de Alertas", icon: Bell }
    ]
  },
  {
    section: "Administração",
    items: [
      { id: "coletas", label: "Operação de Coletas", icon: Database, permission: "read_operational" },
      { id: "usuarios", label: "Usuários", icon: UserRoundCog, permission: "manage_users" },
      { id: "auditoria", label: "Auditoria", icon: Shield, permission: "view_audit" }
    ]
  }
];

export function Sidebar({ screen, navigate, user, region, setRegion, hasPermission, logout }: {
  screen: Screen;
  navigate: (screen: Screen) => void;
  user: ApiUser;
  region: string;
  setRegion: (region: string) => void;
  hasPermission: (permission?: string) => boolean;
  logout: () => void;
}) {
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
            {group.items.filter((item) => hasPermission(item.permission)).map((item) => {
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
        <select value={region} onChange={(event) => setRegion(event.target.value)}>
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

import React from "react";
import { createRoot } from "react-dom/client";
import { useApiClient } from "./api/client";
import { ROLE_PERMISSIONS, SCREEN_PERMISSIONS } from "./auth/permissions";
import { Sidebar } from "./layout/Sidebar";
import { Administrativo, Alertas } from "./screens/AdminScreens";
import { Auditoria } from "./screens/Auditoria";
import { Coletas } from "./screens/Coletas";
import { Dashboard } from "./screens/Dashboard";
import { Login } from "./screens/Login";
import { MapScreen } from "./screens/MapScreen";
import { Processos } from "./screens/Processos";
import { ScoreCalculator } from "./screens/ScoreCalculator";
import { Usuarios } from "./screens/Usuarios";
import { useLocalState } from "./hooks/useLocalState";
import type { ApiUser, Screen } from "./types";
import "./styles.css";

type MapScreenProps = React.ComponentProps<typeof MapScreen> & {
  navigate: (screen: Screen) => void;
  notify: (message: string) => void;
};

const MapScreenWithFutureProps = MapScreen as React.ComponentType<MapScreenProps>;

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
        {screen === "mapa" && <MapScreenWithFutureProps api={api} region={region} navigate={navigate} notify={notify} />}
        {screen === "processos" && <Processos api={api} region={region} navigate={navigate} notify={notify} />}
        {screen === "administrativo" && <Administrativo api={api} />}
        {screen === "score" && <ScoreCalculator api={api} hasPermission={hasPermission} />}
        {screen === "alertas" && <Alertas api={api} />}
        {screen === "coletas" && <Coletas api={api} hasPermission={hasPermission} notify={notify} />}
        {screen === "usuarios" && <Usuarios api={api} hasPermission={hasPermission} notify={notify} />}
        {screen === "auditoria" && <Auditoria api={api} />}
      </main>
      <div className={`toast ${toast ? "show" : ""}`}>{toast}</div>
    </div>
  );
}

createRoot(document.getElementById("root")!).render(<App />);

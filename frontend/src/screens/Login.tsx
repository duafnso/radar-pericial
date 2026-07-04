import React from "react";
import type { ApiClient, ApiUser } from "../types";

export function Login({ api, setToken, setUser, notify }: {
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

import React from "react";
import logoUrl from "../assets/radar-pericial-logo.svg";
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
  const [error, setError] = React.useState("");

  async function submit(event: React.FormEvent) {
    event.preventDefault();
    setError("");
    setLoading(true);
    const data = await api.post<{ status: string; token: string; user: ApiUser }>("/api/login", { username, password });
    setLoading(false);
    if (data?.status === "ok") {
      setToken(data.token);
      setUser(data.user);
      return;
    }

    const requestError = api.getLastError?.();
    let message = requestError?.detail || "Credenciais inválidas ou acesso indisponível.";
    if (requestError?.status === 429) {
      const minutes = Math.max(1, Math.ceil((requestError.retryAfterSeconds || 300) / 60));
      message = `Muitas tentativas de login. Aguarde ${minutes} minutos e tente novamente.`;
    }
    setError(message);
    notify(message);
  }

  return (
    <div className="login-screen">
      <form className="login-panel" onSubmit={submit}>
        <div className="login-brand"><img src={logoUrl} alt="Radar Pericial" /></div>
        <div className="login-subtitle">Inteligência judicial e territorial para perícia agronômica</div>
        <label htmlFor="login-username">Usuário</label>
        <input id="login-username" value={username} onChange={(event) => { setUsername(event.target.value); setError(""); }} autoComplete="username" required />
        <label htmlFor="login-password">Senha</label>
        <input id="login-password" value={password} onChange={(event) => { setPassword(event.target.value); setError(""); }} type="password" autoComplete="current-password" required />
        {error && <div className="login-error" role="alert" aria-live="polite">{error}</div>}
        <button className="primary" disabled={loading}>{loading ? "Entrando..." : "Entrar"}</button>
      </form>
    </div>
  );
}

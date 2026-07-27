import React from "react";
import type { ApiClient, ApiRequestError, ApiUser } from "../types";

export function useApiClient(
  token: string | null,
  setToken: (token: string | null) => void,
  setUser: (user: ApiUser | null) => void
): ApiClient {
  return React.useMemo(() => {
    let lastError: ApiRequestError | null = null;
    const client: ApiClient = {
      async request<T>(path: string, options: RequestInit = {}): Promise<T | null> {
        lastError = null;
        const headers: Record<string, string> = { ...(options.headers as Record<string, string> || {}) };
        if (token) headers.Authorization = `Bearer ${token}`;
        if (options.body && !headers["Content-Type"]) headers["Content-Type"] = "application/json";

        try {
          const response = await fetch(path, { ...options, headers });
          if (!response.ok) {
            let detail = "Não foi possível concluir a solicitação.";
            try {
              const payload = await response.json() as { detail?: unknown };
              if (typeof payload.detail === "string" && payload.detail.trim()) detail = payload.detail;
            } catch {
              // Keep the safe fallback for non-JSON error responses.
            }
            const retryAfter = Number(response.headers.get("Retry-After"));
            lastError = {
              status: response.status,
              detail,
              retryAfterSeconds: Number.isFinite(retryAfter) && retryAfter > 0 ? retryAfter : undefined,
            };
          }
          if (response.status === 401) {
            setToken(null);
            setUser(null);
            return null;
          }
          if (!response.ok) return null;
          return response.json();
        } catch {
          lastError = { status: 0, detail: "Não foi possível conectar ao servidor." };
          return null;
        }
      },
      get<T>(path: string) {
        return this.request<T>(path);
      },
      post<T>(path: string, body?: unknown) {
        return this.request<T>(path, { method: "POST", body: JSON.stringify(body || {}) });
      },
      patch<T>(path: string, body?: unknown) {
        return this.request<T>(path, { method: "PATCH", body: JSON.stringify(body || {}) });
      },
      getLastError() {
        return lastError;
      },
    };
    return client;
  }, [token, setToken, setUser]);
}

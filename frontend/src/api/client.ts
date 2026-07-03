import React from "react";
import type { ApiClient, ApiUser } from "../types";

export function useApiClient(
  token: string | null,
  setToken: (token: string | null) => void,
  setUser: (user: ApiUser | null) => void
): ApiClient {
  return React.useMemo(() => ({
    async request<T>(path: string, options: RequestInit = {}): Promise<T | null> {
      const headers: Record<string, string> = { ...(options.headers as Record<string, string> || {}) };
      if (token) headers.Authorization = `Bearer ${token}`;
      if (options.body && !headers["Content-Type"]) headers["Content-Type"] = "application/json";

      try {
        const response = await fetch(path, { ...options, headers });
        if (response.status === 401) {
          setToken(null);
          setUser(null);
          return null;
        }
        if (!response.ok) return null;
        return response.json();
      } catch {
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
    }
  }), [token, setToken, setUser]);
}

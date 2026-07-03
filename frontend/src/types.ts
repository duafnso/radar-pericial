import type { LucideIcon } from "lucide-react";

export type Role = "admin" | "operator" | "user" | "viewer";

export type Screen =
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

export type ApiUser = {
  id?: number;
  username: string;
  role: Role;
  regiao_foco?: string | null;
};

export type Processo = Record<string, any>;
export type Coleta = Record<string, any>;

export type NavItem = {
  id: Screen;
  label: string;
  icon: LucideIcon;
  permission?: string;
};

export type ApiClient = {
  request<T>(path: string, options?: RequestInit): Promise<T | null>;
  get<T>(path: string): Promise<T | null>;
  post<T>(path: string, body?: unknown): Promise<T | null>;
  patch<T>(path: string, body?: unknown): Promise<T | null>;
};

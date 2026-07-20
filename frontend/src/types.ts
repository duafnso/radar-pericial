import type { LucideIcon } from "lucide-react";

export type Role = "admin" | "operator" | "user" | "viewer";

export type Screen =
  | "dashboard"
  | "mapa"
  | "processos"
  | "administrativo"
  | "score"
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

export type MapProcess = Record<string, unknown> & {
  id: number;
  numero_cnj: string;
  tribunal: string;
  comarca: string;
  vara: string;
  municipio: string;
  regiao_imea: string;
  classe_processual: string;
  assunto_principal: string;
  data_distribuicao: string;
  fase_atual: string;
  origem: string;
  score_total: number;
  faixa_probabilidade: string;
  faixa_label: string;
  tipo_pericia_sugerida: string;
  categorias_detectadas: string;
  urgencia: string;
};

export type MapProcessListResponse = {
  total: number;
  offset: number;
  limit: number;
  items: MapProcess[];
};
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
export type MapFilters = {
  regiao: string;
  municipio: string;
  faixa: string;
  dataInicio: string;
  dataFim: string;
};

export type MapCitySummary = {
  municipio: string;
  regiao_imea: string;
  lat: number;
  lng: number;
  total_processos: number;
  maior_score: number;
  processos_quentes: number;
  processos_provaveis: number;
  faixa_dominante: string;
  ultima_distribuicao: string;
};

export type MapSummaryResponse = {
  total_processos: number;
  total_municipios: number;
  sem_localizacao: number;
  items: MapCitySummary[];
};

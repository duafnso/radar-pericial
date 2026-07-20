import type { MapFilters, MapProcess, MapProcessListResponse } from "../types";

export const MAP_TONE_COLORS = {
  critical: "#12492a",
  high: "#17613a",
  medium: "#256b45",
  low: "#3d6449",
} as const;

const OSM_TILE_URL = "https://tile.openstreetmap.org/{z}/{x}/{y}.png";
const OSM_TILE_ATTRIBUTION =
  '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors';

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function isNonNegativeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isInteger(value) && value >= 0;
}

function readScore(value: unknown): number | null {
  if (value === "") return 0;
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function readOptionalText(record: Record<string, unknown>, field: string): string {
  const value = record[field];
  return typeof value === "string" ? value : "";
}

function parseMapProcess(value: unknown): MapProcess | null {
  if (!isRecord(value)) return null;

  const {
    id,
    numero_cnj: numeroCnj,
    classe_processual: classeProcessual,
    data_distribuicao: dataDistribuicao,
    faixa_probabilidade: faixaProbabilidade,
  } = value;
  if (!isNonNegativeInteger(id) || id <= 0) return null;
  if (
    typeof numeroCnj !== "string" ||
    typeof classeProcessual !== "string" ||
    typeof dataDistribuicao !== "string" ||
    typeof faixaProbabilidade !== "string"
  ) return null;

  const score = readScore(value.score_total);
  if (score === null) return null;

  return {
    ...value,
    id,
    numero_cnj: numeroCnj,
    tribunal: readOptionalText(value, "tribunal"),
    comarca: readOptionalText(value, "comarca"),
    vara: readOptionalText(value, "vara"),
    municipio: readOptionalText(value, "municipio"),
    regiao_imea: readOptionalText(value, "regiao_imea"),
    classe_processual: classeProcessual,
    assunto_principal: readOptionalText(value, "assunto_principal"),
    data_distribuicao: dataDistribuicao,
    fase_atual: readOptionalText(value, "fase_atual"),
    origem: readOptionalText(value, "origem"),
    score_total: score,
    faixa_probabilidade: faixaProbabilidade,
    faixa_label: readOptionalText(value, "faixa_label"),
    tipo_pericia_sugerida: readOptionalText(value, "tipo_pericia_sugerida"),
    categorias_detectadas: readOptionalText(value, "categorias_detectadas"),
    urgencia: readOptionalText(value, "urgencia"),
  };
}

export function parseProcessListResponse(value: unknown): MapProcessListResponse | null {
  if (!isRecord(value)) return null;
  const record = value;
  if (!isNonNegativeInteger(record.total) || !isNonNegativeInteger(record.offset)) return null;
  if (!isNonNegativeInteger(record.limit) || record.limit <= 0) return null;
  if (!Array.isArray(record.items)) return null;

  const items: MapProcess[] = [];
  for (const item of record.items) {
    const parsed = parseMapProcess(item);
    if (!parsed) return null;
    items.push(parsed);
  }

  return {
    total: record.total,
    offset: record.offset,
    limit: record.limit,
    items,
  };
}

export function formatCivilDate(value: unknown): string {
  if (typeof value !== "string") return "--";
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(value);
  if (!match) return "--";

  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const leapYear = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  const daysInMonth = [31, leapYear ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  if (month < 1 || month > 12 || day < 1 || day > daysInMonth[month - 1]) return "--";

  return `${match[3]}/${match[2]}/${match[1]}`;
}

export function resolveTileConfig(urlValue?: string, attributionValue?: string) {
  const url = urlValue?.trim() || "";
  const attribution = attributionValue?.trim() || "";
  if (url && attribution) return { url, attribution, warning: "" };
  if (url || attribution) {
    return {
      url: OSM_TILE_URL,
      attribution: OSM_TILE_ATTRIBUTION,
      warning: "Configuração de tiles incompleta; usando OpenStreetMap no modo de desenvolvimento.",
    };
  }
  return { url: OSM_TILE_URL, attribution: OSM_TILE_ATTRIBUTION, warning: "" };
}

export function buildMapSummaryParams(
  filters: MapFilters,
  globalRegion: string,
): URLSearchParams {
  const params = new URLSearchParams({ limit_cidades: "200" });
  const region = globalRegion || filters.regiao;

  if (region) params.set("regiao", region);
  if (filters.municipio.trim()) params.set("municipio", filters.municipio.trim());
  if (filters.faixa) params.set("faixa", filters.faixa);
  if (filters.dataInicio) params.set("data_inicio", filters.dataInicio);
  if (filters.dataFim) params.set("data_fim", filters.dataFim);

  return params;
}

export function markerTone(faixa: string): "critical" | "high" | "medium" | "low" {
  if (faixa === "janela_quente") return "critical";
  if (faixa === "provavel") return "high";
  if (faixa === "observacao") return "medium";
  return "low";
}

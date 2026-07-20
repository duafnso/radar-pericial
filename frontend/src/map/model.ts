import type { MapFilters } from "../types";

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

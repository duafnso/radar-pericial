import { describe, expect, it } from "vitest";
import {
  MAP_TONE_COLORS,
  formatCivilDate,
  parseMapSummaryResponse,
  parseProcessListResponse,
  resolveTileConfig,
} from "../src/map/model";

const validProcess = {
  id: 42,
  numero_cnj: "0000042-00.2026.8.11.0001",
  classe_processual: "Usucapião",
  data_distribuicao: "2026-07-01",
  score_total: 62,
  faixa_probabilidade: "provavel",
  tribunal: "TJMT",
  comarca: "Cuiabá",
  municipio: "Cuiabá",
};

function contrastRatio(foreground: string, background: string) {
  const luminance = (hex: string) => {
    const channels = hex.slice(1).match(/.{2}/g)?.map((part) => Number.parseInt(part, 16) / 255) || [];
    const linear = channels.map((channel) => channel <= 0.03928
      ? channel / 12.92
      : ((channel + 0.055) / 1.055) ** 2.4);
    return 0.2126 * linear[0] + 0.7152 * linear[1] + 0.0722 * linear[2];
  };
  const first = luminance(foreground);
  const second = luminance(background);
  return (Math.max(first, second) + 0.05) / (Math.min(first, second) + 0.05);
}

describe("map process response parsing", () => {
  it("accepts and normalizes a complete paginated response", () => {
    const result = parseProcessListResponse({
      total: 1,
      offset: 0,
      limit: 10,
      items: [validProcess],
    });

    expect(result).not.toBeNull();
    expect(result?.items[0]).toMatchObject({
      id: 42,
      numero_cnj: validProcess.numero_cnj,
      score_total: 62,
    });
  });

  it.each([
    { total: 0, items: [] },
    { offset: 0, limit: 10, items: [] },
    { total: 1, offset: 0, limit: 10, items: [{ id: "broken" }] },
  ])("rejects an invalid paginated payload", (payload) => {
    expect(parseProcessListResponse(payload)).toBeNull();
  });
});

const validMapSummary = { total_processos: 2, total_municipios: 1, sem_localizacao: 0, items: [{ municipio: "Cuiaba", regiao_imea: "Centro-Sul", lat: -15.6, lng: -56.1, total_processos: 2, maior_score: 82, processos_quentes: 1, processos_provaveis: 1, faixa_dominante: "janela_quente", ultima_distribuicao: "2026-07-01" }] };

describe("map summary response parsing", () => {
  it("accepts an empty, structurally valid map summary", () => {
    expect(parseMapSummaryResponse({ ...validMapSummary, items: [] })).toEqual({ ...validMapSummary, items: [] });
  });

  it.each([
    null,
    { ...validMapSummary, items: null },
    { ...validMapSummary, total_processos: "2" },
    { ...validMapSummary, items: [{ ...validMapSummary.items[0], lat: Number.NaN }] },
    { ...validMapSummary, items: [{ ...validMapSummary.items[0], total_processos: -1 }] },
    { ...validMapSummary, items: [{ ...validMapSummary.items[0], municipio: "" }] },
  ])("rejects malformed aggregate payloads", (payload) => {
    expect(parseMapSummaryResponse(payload)).toBeNull();
  });
});

describe("civil date formatting", () => {
  it("preserves YYYY-MM-DD without UTC conversion or time", () => {
    expect(formatCivilDate("2026-07-01")).toBe("01/07/2026");
  });

  it("rejects impossible or timestamp values", () => {
    expect(formatCivilDate("2026-02-30")).toBe("--");
    expect(formatCivilDate("2026-07-01T00:00:00Z")).toBe("--");
  });
});

describe("tile provider configuration", () => {
  it("uses a custom provider only when URL and attribution are paired", () => {
    expect(resolveTileConfig("https://tiles.example/{z}/{x}/{y}.png", "Example Maps")).toEqual({
      url: "https://tiles.example/{z}/{x}/{y}.png",
      attribution: "Example Maps",
      warning: "",
    });
  });

  it("falls back honestly when a custom URL has no attribution", () => {
    const result = resolveTileConfig("https://tiles.example/{z}/{x}/{y}.png", "");

    expect(result.url).toBe("https://tile.openstreetmap.org/{z}/{x}/{y}.png");
    expect(result.attribution).toContain("OpenStreetMap");
    expect(result.warning).toMatch(/incompleta/i);
  });
});

describe("map tone contrast", () => {
  it.each(["critical", "high", "medium", "low"] as const)("keeps white count text readable for %s", (tone) => {
    const color = MAP_TONE_COLORS?.[tone];
    expect(color).toMatch(/^#[0-9a-f]{6}$/i);
    expect(contrastRatio("#ffffff", color)).toBeGreaterThanOrEqual(4.5);
  });
});

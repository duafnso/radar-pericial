export function fmt(value: any, digits = 0) {
  const n = Number(value || 0);
  return Number.isFinite(n)
    ? n.toLocaleString("pt-BR", { maximumFractionDigits: digits })
    : "0";
}

export function shortDate(value?: string | null) {
  if (!value) return "--";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return String(value).slice(0, 16);
  return d.toLocaleString("pt-BR", {
    day: "2-digit",
    month: "2-digit",
    year: "2-digit",
    hour: "2-digit",
    minute: "2-digit"
  });
}

export function scoreClass(score: any) {
  const n = Number(score || 0);
  if (n >= 75) return "critical";
  if (n >= 50) return "high";
  if (n >= 25) return "medium";
  return "low";
}

export function scoreLabel(faixa?: string) {
  const map: Record<string, string> = {
    janela_quente: "Janela quente",
    provavel: "Provável perícia",
    observacao: "Observação",
    frio: "Frio"
  };
  return map[faixa || ""] || faixa || "Sem faixa";
}

export function friendlyError(error?: string) {
  if (!error) return "--";
  const lower = error.toLowerCase();
  if (lower.includes("429")) return "Limite da fonte externa. Aguarde antes de tentar novamente.";
  if (lower.includes("401") || lower.includes("apikey")) return "Chave da API inválida ou ausente.";
  if (lower.includes("timeout") || lower.includes("timed out")) return "A fonte externa demorou para responder.";
  return error.slice(0, 160);
}

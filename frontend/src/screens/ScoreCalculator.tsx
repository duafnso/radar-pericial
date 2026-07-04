import React from "react";
import { Page } from "../components/Page";
import type { ApiClient } from "../types";

export function ScoreCalculator({ api, hasPermission }: {
  api: ApiClient;
  hasPermission: (permission?: string) => boolean;
}) {
  const [classe, setClasse] = React.useState("");
  const [assunto, setAssunto] = React.useState("");
  const [texto, setTexto] = React.useState("");
  const [result, setResult] = React.useState<any>(null);

  async function calculate() {
    if (!hasPermission("calculate_score")) return;
    const data = await api.post<any>("/api/score/calcular", { classe_processual: classe, assunto, texto_livre: texto });
    setResult(data);
  }

  return (
    <Page title="Calculadora Pericial" subtitle="Simule a probabilidade de demanda pericial">
      <div className="form-grid">
        <input placeholder="Classe processual" value={classe} onChange={(event) => setClasse(event.target.value)} />
        <input placeholder="Assunto principal" value={assunto} onChange={(event) => setAssunto(event.target.value)} />
        <textarea placeholder="Texto livre, movimentações ou publicação" value={texto} onChange={(event) => setTexto(event.target.value)} />
        <button onClick={calculate}>Calcular score</button>
      </div>
      {result && (
        <div className="score-result">
          <strong>{result.score_total ?? result.score?.score_total ?? 0}</strong>
          <span>{result.faixa_label || result.score?.faixa_label || "Resultado calculado"}</span>
        </div>
      )}
    </Page>
  );
}

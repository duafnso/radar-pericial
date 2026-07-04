import React from "react";
import { Calculator, RotateCcw } from "lucide-react";
import { Page } from "../components/Page";
import type { ApiClient } from "../types";

const CLASSES = [
  "Desapropriação",
  "Servidão administrativa",
  "Ação possessória",
  "Usucapião",
  "Dano ambiental",
  "Ação indenizatória",
  "Inventário"
];

const ASSUNTOS = [
  "Avaliação de imóvel rural",
  "Benfeitorias",
  "Produtividade agrícola",
  "Georreferenciamento",
  "Dano ambiental rural",
  "Regularização fundiária",
  "Servidão"
];

const MOVS = [
  "Nomeação de perito",
  "Apresentação de quesitos",
  "Despacho saneador",
  "Fixação de honorários",
  "Apresentação de laudo",
  "Manifestação de assistente técnico"
];

const EVENTOS = [
  "Decreto de utilidade pública",
  "Portaria de desapropriação",
  "Faixa de servidão",
  "Projeto de duplicação",
  "Regularização fundiária",
  "Licença ambiental"
];

export function ScoreCalculator({ api, hasPermission }: {
  api: ApiClient;
  hasPermission: (permission?: string) => boolean;
}) {
  const [classe, setClasse] = React.useState(CLASSES[0]);
  const [assunto, setAssunto] = React.useState(ASSUNTOS[0]);
  const [movimentacoes, setMovimentacoes] = React.useState<string[]>([]);
  const [eventos, setEventos] = React.useState<string[]>([]);
  const [texto, setTexto] = React.useState("");
  const [result, setResult] = React.useState<any>(null);
  const [loading, setLoading] = React.useState(false);

  async function calculate() {
    if (!hasPermission("calculate_score")) return;
    setLoading(true);
    const data = await api.post<any>("/api/score/calcular", {
      classe_processual: classe,
      assunto,
      movimentacoes,
      eventos_admin: eventos,
      texto_livre: texto
    });
    setResult(data);
    setLoading(false);
  }

  function reset() {
    setClasse(CLASSES[0]);
    setAssunto(ASSUNTOS[0]);
    setMovimentacoes([]);
    setEventos([]);
    setTexto("");
    setResult(null);
  }

  return (
    <Page title="Calculadora Pericial" subtitle="Simule a chance de oportunidade pericial antes de acompanhar um processo">
      <div className="calculator-layout">
        <section className="card">
          <div className="section-label">Cenário do processo</div>
          <div className="form-grid">
            <label>
              Classe processual
              <select value={classe} onChange={(event) => setClasse(event.target.value)}>
                {CLASSES.map((item) => <option key={item}>{item}</option>)}
              </select>
            </label>
            <label>
              Assunto principal
              <select value={assunto} onChange={(event) => setAssunto(event.target.value)}>
                {ASSUNTOS.map((item) => <option key={item}>{item}</option>)}
              </select>
            </label>
            <label>
              Texto livre, resumo, movimentação ou publicação
              <textarea value={texto} onChange={(event) => setTexto(event.target.value)} placeholder="Ex.: despacho saneador determinou perícia agronômica para avaliação de imóvel rural..." />
            </label>
          </div>
        </section>

        <section className="card">
          <div className="section-label">Sinais detectados</div>
          <CheckGroup title="Movimentações processuais" values={MOVS} selected={movimentacoes} setSelected={setMovimentacoes} />
          <CheckGroup title="Eventos administrativos próximos" values={EVENTOS} selected={eventos} setSelected={setEventos} />
          <div className="button-row">
            <button className="primary" onClick={calculate} disabled={loading}><Calculator size={14} /> {loading ? "Calculando..." : "Calcular score"}</button>
            <button className="secondary" onClick={reset}><RotateCcw size={14} /> Limpar</button>
          </div>
        </section>

        <section className="score-panel">
          {result ? <ScoreResult result={result} /> : (
            <div className="empty score-empty">Preencha o cenário e calcule para ver score, urgência e tipo de perícia sugerida.</div>
          )}
        </section>
      </div>
    </Page>
  );
}

function CheckGroup({ title, values, selected, setSelected }: {
  title: string;
  values: string[];
  selected: string[];
  setSelected: (items: string[]) => void;
}) {
  function toggle(value: string) {
    setSelected(selected.includes(value) ? selected.filter((item) => item !== value) : [...selected, value]);
  }

  return (
    <div className="check-group">
      <strong>{title}</strong>
      <div>
        {values.map((value) => (
          <label key={value} className="check-pill">
            <input type="checkbox" checked={selected.includes(value)} onChange={() => toggle(value)} />
            <span>{value}</span>
          </label>
        ))}
      </div>
    </div>
  );
}

function ScoreResult({ result }: { result: any }) {
  const parts = [
    ["Classe", result.score_classe],
    ["Assunto", result.score_assunto],
    ["Movimentação", result.score_movimentacao],
    ["Publicação/texto", result.score_publicacao],
    ["Administrativo", result.score_administrativo]
  ];

  return (
    <div className="score-result-rich">
      <div className="score-hero">
        <span>Score pericial</span>
        <strong>{result.score_total ?? 0}</strong>
        <em>{result.faixa_label || "Resultado calculado"}</em>
      </div>
      <div className="score-insight">
        <strong>{result.tipo_pericia_sugerida || "Tipo de perícia ainda indefinido"}</strong>
        <span>Urgência: {result.urgencia || "baixa"}</span>
        <p>{result.categorias_detectadas || "Nenhuma categoria específica detectada no texto livre."}</p>
      </div>
      <div className="score-breakdown">
        {parts.map(([label, value]) => (
          <div key={label}>
            <span>{label}</span>
            <strong>{value || 0}</strong>
          </div>
        ))}
      </div>
    </div>
  );
}

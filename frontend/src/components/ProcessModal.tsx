import { BellPlus, X } from "lucide-react";
import type { Processo } from "../types";
import { scoreLabel, shortDate } from "../utils/format";

export function ProcessModal({ processo, close, follow }: {
  processo: Processo;
  close: () => void;
  follow: (processo: Processo) => void;
}) {
  const rows = [
    ["Número CNJ", processo.numero_cnj],
    ["Tribunal", processo.tribunal],
    ["Comarca", processo.comarca],
    ["Vara", processo.vara],
    ["Município", processo.municipio],
    ["Região IMEA", processo.regiao_imea],
    ["Classe", processo.classe_processual],
    ["Assunto", processo.assunto_principal],
    ["Fase atual", processo.fase_atual],
    ["Distribuição", shortDate(processo.data_distribuicao)],
    ["Tipo de perícia sugerida", processo.tipo_pericia_sugerida],
    ["Urgência", processo.urgencia],
    ["Faixa", scoreLabel(processo.faixa_probabilidade)]
  ];

  return (
    <div className="modal-backdrop" role="presentation" onClick={close}>
      <section className="modal-panel" role="dialog" aria-modal="true" aria-labelledby="process-modal-title" onClick={(event) => event.stopPropagation()}>
        <header className="modal-header">
          <div>
            <strong id="process-modal-title">{processo.numero_cnj || "Processo sem CNJ"}</strong>
            <span>{processo.classe_processual || "Classe não informada"}</span>
          </div>
          <button className="secondary icon-button" onClick={close} aria-label="Fechar"><X size={16} /></button>
        </header>
        <div className="detail-grid">
          {rows.map(([label, value]) => (
            <div key={label}>
              <span>{label}</span>
              <strong>{value || "--"}</strong>
            </div>
          ))}
        </div>
        <div className="modal-score">
          <div>
            <span>Score pericial</span>
            <strong>{processo.score_total || 0}</strong>
          </div>
          <p>{processo.categorias_detectadas || "Sem categorias adicionais detectadas."}</p>
        </div>
        <footer className="modal-actions">
          <button className="secondary" onClick={close}>Fechar</button>
          <button className="primary" onClick={() => follow(processo)}><BellPlus size={14} /> Acompanhar processo</button>
        </footer>
      </section>
    </div>
  );
}

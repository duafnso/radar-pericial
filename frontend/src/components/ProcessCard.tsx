import { BellPlus, ExternalLink } from "lucide-react";
import type { Processo } from "../types";
import { scoreClass, scoreLabel } from "../utils/format";

export function ProcessCard({
  processo,
  onOpen,
  onFollow
}: {
  processo: Processo;
  onOpen?: (processo: Processo) => void;
  onFollow?: (processo: Processo) => void;
}) {
  return (
    <article className={`process-card ${scoreClass(processo.score_total)}`} onClick={() => onOpen?.(processo)}>
      <div className="process-main">
        <strong>{processo.classe_processual || "Classe não informada"}</strong>
        <span>
          {processo.numero_cnj || "CNJ não informado"} · {processo.municipio || processo.comarca || "Município pendente"}
        </span>
        <p>{processo.assunto_principal || processo.fase_atual || "Sem resumo disponível"}</p>
        <div className="process-actions" onClick={(event) => event.stopPropagation()}>
          <button className="secondary" onClick={() => onOpen?.(processo)}><ExternalLink size={14} /> Detalhes</button>
          {onFollow && <button className="primary" onClick={() => onFollow(processo)}><BellPlus size={14} /> Acompanhar processo</button>}
        </div>
      </div>
      <div className="score">
        <strong>{processo.score_total || 0}</strong>
        <span>{scoreLabel(processo.faixa_probabilidade)}</span>
      </div>
    </article>
  );
}

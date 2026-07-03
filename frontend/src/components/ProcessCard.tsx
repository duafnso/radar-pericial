import type { Processo } from "../types";
import { scoreClass, scoreLabel } from "../utils/format";

export function ProcessCard({ processo }: { processo: Processo }) {
  return (
    <article className={`process-card ${scoreClass(processo.score_total)}`}>
      <div>
        <strong>{processo.classe_processual || "Classe não informada"}</strong>
        <span>
          {processo.numero_cnj || "CNJ não informado"} · {processo.municipio || processo.comarca || "Município pendente"}
        </span>
        <p>{processo.assunto_principal || processo.fase_atual || "Sem resumo disponível"}</p>
      </div>
      <div className="score">
        <strong>{processo.score_total || 0}</strong>
        <span>{scoreLabel(processo.faixa_probabilidade)}</span>
      </div>
    </article>
  );
}

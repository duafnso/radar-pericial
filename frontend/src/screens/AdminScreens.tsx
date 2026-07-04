import { CardLine } from "../components/CardLine";
import type { ApiClient } from "../types";
import { SimpleListScreen } from "./SimpleListScreen";

export function Administrativo({ api }: { api: ApiClient }) {
  return (
    <SimpleListScreen
      title="Radar Administrativo"
      subtitle="Eventos administrativos relevantes"
      endpoint={{ api, path: "/api/eventos?limit=50&dias=90" }}
      render={(item, index) => <CardLine key={index} title={item.titulo || item.descricao || item.fonte} meta={`${item.fonte || ""} · ${item.municipio || ""}`} />}
    />
  );
}

export function Peritos({ api }: { api: ApiClient }) {
  return (
    <SimpleListScreen
      title="Corpo Pericial"
      subtitle="Profissionais cadastrados"
      endpoint={{ api, path: "/api/peritos" }}
      render={(item, index) => <CardLine key={index} title={item.nome || "Profissional"} meta={`${item.registro_profissional || ""} · ${item.regiao_imea || ""}`} />}
    />
  );
}

export function Alertas({ api }: { api: ApiClient }) {
  return (
    <SimpleListScreen
      title="Central de Alertas"
      subtitle="Eventos e oportunidades recentes"
      endpoint={{ api, path: "/api/alertas?limit=40" }}
      render={(item, index) => <CardLine key={index} title={item.titulo || item.orgao || "Alerta"} meta={`${item.fonte || ""} · Score ${item.score_evento || item.score_total || 0}`} />}
    />
  );
}

export function CardLine({ title, meta }: { title?: string; meta?: string }) {
  return (
    <div className="card-line">
      <strong>{title || "Sem título"}</strong>
      <span>{meta || "Sem metadados"}</span>
    </div>
  );
}

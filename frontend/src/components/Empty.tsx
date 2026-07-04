import { AlertTriangle } from "lucide-react";

export function Empty({ text }: { text: string }) {
  return (
    <div className="empty">
      <AlertTriangle size={20} />
      <span>{text}</span>
    </div>
  );
}

export function LoadingState({ text = "Carregando dados..." }: { text?: string }) {
  return (
    <div className="empty loading-state">
      <span className="spinner" />
      <span>{text}</span>
    </div>
  );
}

export function ErrorState({ text, retry }: { text: string; retry?: () => void }) {
  return (
    <div className="empty error-state">
      <AlertTriangle size={20} />
      <span>{text}</span>
      {retry && <button className="secondary" onClick={retry}>Tentar novamente</button>}
    </div>
  );
}

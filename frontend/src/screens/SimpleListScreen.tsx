import React from "react";
import { RefreshCw } from "lucide-react";
import { Empty } from "../components/Empty";
import { Page } from "../components/Page";
import type { ApiClient } from "../types";

export function SimpleListScreen({ title, subtitle, endpoint, render }: {
  title: string;
  subtitle: string;
  endpoint: { api: ApiClient; path: string };
  render: (item: any, index: number) => React.ReactNode;
}) {
  const [items, setItems] = React.useState<any[]>([]);

  async function load() {
    const data = await endpoint.api.get<any>(endpoint.path);
    setItems(data?.items || data || []);
  }

  React.useEffect(() => { load(); }, []);

  return (
    <Page title={title} subtitle={subtitle} action={<button onClick={load}><RefreshCw size={14} /> Atualizar</button>}>
      <div className="stack">
        {items.length ? items.map(render) : <Empty text="Nenhum registro encontrado." />}
      </div>
    </Page>
  );
}


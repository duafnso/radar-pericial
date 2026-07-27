import React from "react";
import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ApiClient, MapSummaryResponse, Processo, Screen } from "../src/types";
import "../src/styles.css";

const leafletMock = vi.hoisted(() => ({
  maps: [] as Array<{
    setView: ReturnType<typeof vi.fn>;
    fitBounds: ReturnType<typeof vi.fn>;
    getZoom: ReturnType<typeof vi.fn>;
    invalidateSize: ReturnType<typeof vi.fn>;
    remove: ReturnType<typeof vi.fn>;
  }>,
  tileLayers: [] as Array<{ emit: (type: string) => void; offTypes: string[] }>,
  circleMarkers: [] as Array<{ tooltip: HTMLElement | null }>,
}));

vi.mock("leaflet", () => {
  type Handler = () => void;

  class Evented {
    handlers = new Map<string, Set<Handler>>();
    offTypes: string[] = [];

    on(type: string, handler: Handler) {
      const handlers = this.handlers.get(type) || new Set<Handler>();
      handlers.add(handler);
      this.handlers.set(type, handlers);
      return this;
    }

    off(type: string, handler: Handler) {
      this.offTypes.push(type);
      this.handlers.get(type)?.delete(handler);
      return this;
    }

    emit(type: string) {
      this.handlers.get(type)?.forEach((handler) => handler());
    }
  }

  class LayerGroup {
    layers: Array<{ detach?: () => void }> = [];

    addTo() {
      return this;
    }

    addLayer(layer: { detach?: () => void }) {
      this.layers.push(layer);
    }

    clearLayers() {
      this.layers.forEach((layer) => layer.detach?.());
      this.layers = [];
      return this;
    }

    remove() {
      this.clearLayers();
    }
  }

  class TileLayer extends Evented {
    constructor() {
      super();
      leafletMock.tileLayers.push(this);
    }

    addTo() {
      return this;
    }

    remove() {}
  }

  class CircleMarker extends Evented {
    tooltip: HTMLElement | null = null;

    constructor() {
      super();
      leafletMock.circleMarkers.push(this);
    }

    bindTooltip(node: HTMLElement) {
      this.tooltip = node;
      return this;
    }

    addTo(group: LayerGroup) {
      group.addLayer(this);
      return this;
    }
  }

  class Marker extends Evented {
    element: HTMLDivElement | null = null;
    options: Record<string, unknown>;

    constructor(options: Record<string, unknown>) {
      super();
      this.options = options;
    }

    addTo(group: LayerGroup) {
      const icon = this.options.icon as { options?: { className?: string; html?: HTMLElement } } | undefined;
      const element = document.createElement("div");
      element.className = icon?.options?.className || "";
      if (this.options.keyboard) {
        element.tabIndex = 0;
        element.setAttribute("role", "button");
      }
      if (typeof this.options.title === "string") element.title = this.options.title;
      if (icon?.options?.html) element.append(icon.options.html);
      element.addEventListener("click", () => this.emit("click"));
      document.body.append(element);
      this.element = element;
      group.addLayer(this);
      return this;
    }

    getElement() {
      return this.element;
    }

    detach = () => {
      this.element?.remove();
    };
  }

  const leaflet = {
    map: () => {
      const map = {
        setView: vi.fn(),
        fitBounds: vi.fn(),
        getZoom: vi.fn(() => 6),
        invalidateSize: vi.fn(),
        remove: vi.fn(),
      };
      leafletMock.maps.push(map);
      return map;
    },
    layerGroup: () => new LayerGroup(),
    tileLayer: () => new TileLayer(),
    circleMarker: () => new CircleMarker(),
    marker: (_coordinates: unknown, options: Record<string, unknown>) => new Marker(options),
    divIcon: (options: Record<string, unknown>) => ({ options }),
    point: (x: number, y: number) => ({ x, y }),
    latLngBounds: () => ({ isValid: () => true }),
  };

  return { default: leaflet };
});

import { MapScreen } from "../src/screens/MapScreen";

const summary: MapSummaryResponse = {
  total_processos: 2,
  total_municipios: 1,
  sem_localizacao: 0,
  items: [{
    municipio: "Cuiabá",
    regiao_imea: "Centro-Sul",
    lat: -15.6,
    lng: -56.1,
    total_processos: 2,
    maior_score: 82,
    processos_quentes: 1,
    processos_provaveis: 1,
    faixa_dominante: "janela_quente",
    ultima_distribuicao: "2026-07-01",
  }],
};

function processItem(id: number, cnj = `00000${id}-00.2026.8.11.0001`) {
  return {
    id,
    numero_cnj: cnj,
    tribunal: "TJMT",
    comarca: "Cuiabá",
    vara: "1ª Vara",
    municipio: "Cuiabá",
    regiao_imea: "Centro-Sul",
    classe_processual: "Usucapião",
    assunto_principal: "Imóvel rural",
    data_distribuicao: "2026-07-01",
    fase_atual: "Conhecimento",
    origem: "DataJud",
    score_total: 82,
    faixa_probabilidade: "janela_quente",
    faixa_label: "Janela quente",
    tipo_pericia_sugerida: "Avaliação agronômica",
    categorias_detectadas: "fundiário",
    urgencia: "alta",
  };
}

function createApi(processPayload: unknown | ((path: string) => unknown)) {
  const get = vi.fn(async (path: string) => {
    if (path.startsWith("/api/processos/mapa/resumo")) return summary;
    return typeof processPayload === "function" ? processPayload(path) : processPayload;
  });
  return {
    request: vi.fn(),
    get,
    post: vi.fn(async () => ({ status: "ok" })),
    patch: vi.fn(),
  } as unknown as ApiClient;
}

function renderMap(api: ApiClient, overrides?: {
  navigate?: (screen: Screen) => void;
  notify?: (message: string) => void;
}) {
  return render(
    <MapScreen
      api={api}
      region=""
      navigate={overrides?.navigate || vi.fn()}
      notify={overrides?.notify || vi.fn()}
    />,
  );
}

async function selectCuiaba() {
  await userEvent.click(await screen.findByRole("button", { name: /Cuiabá.*Centro-Sul.*2/i }));
}

beforeEach(() => {
  leafletMock.maps.length = 0;
  leafletMock.tileLayers.length = 0;
  leafletMock.circleMarkers.length = 0;
});

describe("municipal process panel", () => {
  it("keeps the selected city and exposes retry for an invalid paginated payload", async () => {
    const api = createApi({ total: 0, items: [] });
    renderMap(api);

    await selectCuiaba();

    expect(await screen.findByText("Não foi possível carregar os processos deste município.")).toBeInTheDocument();
    expect(screen.getByLabelText("Processos de Cuiabá")).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Tentar novamente" }));
    await waitFor(() => expect(api.get).toHaveBeenCalledTimes(3));
  });

  it("renders a civil distribution date without timezone or time", async () => {
    const api = createApi({ total: 1, offset: 0, limit: 10, items: [processItem(1)] });
    renderMap(api);

    await selectCuiaba();

    expect(await screen.findByText("01/07/2026")).toBeInTheDocument();
    expect(screen.queryByText(/21:00|00:00/)).not.toBeInTheDocument();
  });

  it("clears stale rows and total when a later page response is invalid", async () => {
    const api = createApi((path) => path.includes("offset=10")
      ? { total: 0, items: [] }
      : { total: 11, offset: 0, limit: 10, items: [processItem(1, "STALE-PROCESS")] });
    renderMap(api);

    await selectCuiaba();
    expect(await screen.findByText("STALE-PROCESS")).toBeInTheDocument();
    expect(screen.getByText(/11 processos/)).toBeInTheDocument();

    await userEvent.click(screen.getByRole("button", { name: "Próxima página" }));

    expect(await screen.findByText("Não foi possível carregar os processos deste município.")).toBeInTheDocument();
    expect(screen.queryByText("STALE-PROCESS")).not.toBeInTheDocument();
    expect(screen.queryByText(/11 processos/)).not.toBeInTheDocument();
    expect(screen.getByText(/0 processos/)).toBeInTheDocument();
    expect(screen.getByText(/p.gina 1 de 1/)).toBeInTheDocument();
  });

  it("selects, centers and requests the next ten-process page", async () => {
    const api = createApi((path) => path.includes("offset=10")
      ? { total: 11, offset: 10, limit: 10, items: [processItem(11, "PAGE-2")] }
      : { total: 11, offset: 0, limit: 10, items: [processItem(1, "PAGE-1")] });
    renderMap(api);

    await selectCuiaba();
    expect(await screen.findByText("PAGE-1")).toBeInTheDocument();
    expect(leafletMock.maps[0].setView).toHaveBeenCalledWith([-15.6, -56.1], 8, { animate: true });

    await userEvent.click(screen.getByRole("button", { name: "Próxima página" }));

    expect(await screen.findByText("PAGE-2")).toBeInTheDocument();
    expect(api.get).toHaveBeenCalledWith(expect.stringContaining("limit=10&offset=10"));
    expect(api.get).toHaveBeenCalledWith(expect.stringContaining("municipio=Cuiab%C3%A1"));
    expect(api.get).toHaveBeenCalledWith(expect.stringContaining("municipio_exato=true"));
  });
});

describe("summary request ordering", () => {
  it("discards an older summary response resolved after the current request", async () => {
    let resolveOlder: (value: MapSummaryResponse) => void = () => undefined;
    let resolveCurrent: (value: MapSummaryResponse) => void = () => undefined;
    const older = new Promise<MapSummaryResponse>((resolve) => {
      resolveOlder = resolve;
    });
    const current = new Promise<MapSummaryResponse>((resolve) => {
      resolveCurrent = resolve;
    });
    const currentSummary: MapSummaryResponse = {
      ...summary,
      items: [{
        ...summary.items[0],
        municipio: "Sinop",
        regiao_imea: "Norte",
      }],
    };
    const api = createApi({ total: 0, offset: 0, limit: 10, items: [] });
    api.get = vi.fn((path: string) => {
      if (!path.startsWith("/api/processos/mapa/resumo")) {
        return Promise.resolve({ total: 0, offset: 0, limit: 10, items: [] });
      }
      return path.includes("regiao=Norte") ? current : older;
    });
    const view = renderMap(api);

    view.rerender(
      <MapScreen api={api} region="Norte" navigate={vi.fn()} notify={vi.fn()} />,
    );
    resolveCurrent(currentSummary);
    expect(await screen.findByRole("button", { name: /Sinop.*Norte.*2/i })).toBeInTheDocument();

    resolveOlder(summary);
    await act(async () => {
      await older;
    });

    expect(screen.getByRole("button", { name: /Sinop.*Norte.*2/i })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Cuiabá.*Centro-Sul.*2/i })).not.toBeInTheDocument();
  });
});

describe("municipal process request ordering", () => {
  it("discards an older municipal response resolved after the selected city", async () => {
    let resolveCuiaba: (value: unknown) => void = () => undefined;
    let resolveSinop: (value: unknown) => void = () => undefined;
    const cuiaba = new Promise<unknown>((resolve) => {
      resolveCuiaba = resolve;
    });
    const sinop = new Promise<unknown>((resolve) => {
      resolveSinop = resolve;
    });
    const twoCities: MapSummaryResponse = {
      ...summary,
      total_processos: 3,
      total_municipios: 2,
      items: [
        summary.items[0],
        {
          ...summary.items[0],
          municipio: "Sinop",
          regiao_imea: "Norte",
          lat: -11.86,
          lng: -55.51,
          total_processos: 1,
        },
      ],
    };
    const api = createApi({ total: 0, offset: 0, limit: 10, items: [] });
    api.get = vi.fn((path: string) => {
      if (path.startsWith("/api/processos/mapa/resumo")) return Promise.resolve(twoCities);
      if (path.includes("municipio=Sinop")) return sinop;
      return cuiaba;
    });
    renderMap(api);

    await userEvent.click(await screen.findByRole("button", { name: /Cuiab.*Centro-Sul.*2/i }));
    await userEvent.click(await screen.findByRole("button", { name: /Sinop.*1/i }));

    resolveSinop({ total: 1, offset: 0, limit: 10, items: [processItem(2, "SINOP-CURRENT")] });
    expect(await screen.findByText("SINOP-CURRENT")).toBeInTheDocument();

    resolveCuiaba({ total: 1, offset: 0, limit: 10, items: [processItem(1, "CUIABA-STALE")] });
    await act(async () => {
      await cuiaba;
    });

    expect(screen.getByText("SINOP-CURRENT")).toBeInTheDocument();
    expect(screen.queryByText("CUIABA-STALE")).not.toBeInTheDocument();
  });
});

describe("municipal marker accessibility", () => {

  it.each(["Enter", " "])("activates with %s and removes its keyboard handler during cleanup", async (key) => {
    const api = createApi({ total: 1, offset: 0, limit: 10, items: [processItem(1)] });
    const view = renderMap(api);
    const marker = await screen.findByRole("button", {
      name: "Cuiabá, 2 processos, faixa Janela quente",
    });

    expect(leafletMock.circleMarkers[0].tooltip?.textContent).toContain("Faixa Janela quente");
    const callsBeforeActivation = leafletMock.maps[0].setView.mock.calls.length;
    fireEvent.keyDown(marker, { key });
    await waitFor(() => expect(leafletMock.maps[0].setView).toHaveBeenCalledTimes(callsBeforeActivation + 1));

    view.unmount();
    fireEvent.keyDown(marker, { key });
    expect(leafletMock.maps[0].setView).toHaveBeenCalledTimes(callsBeforeActivation + 1);
  });
});

describe("follow process guard", () => {
  it("allows only one in-flight POST and disables list and modal follow actions", async () => {
    let resolvePost: (value: { status: string }) => void = () => undefined;
    const postResult = new Promise<{ status: string }>((resolve) => {
      resolvePost = resolve;
    });
    const api = createApi({ total: 1, offset: 0, limit: 10, items: [processItem(1)] });
    api.post = vi.fn(() => postResult);
    const navigate = vi.fn();
    const notify = vi.fn();
    renderMap(api, { navigate, notify });
    await selectCuiaba();
    await screen.findByText(processItem(1).numero_cnj);
    await userEvent.click(screen.getByRole("button", { name: "Detalhes" }));
    const modalButton = await screen.findByRole("button", { name: "Acompanhar processo" });

    act(() => {
      modalButton.click();
      modalButton.click();
    });

    expect(api.post).toHaveBeenCalledTimes(1);
    expect(screen.getByRole("button", { name: "Acompanhar" })).toBeDisabled();
    expect(modalButton).toBeDisabled();

    resolvePost({ status: "ok" });
    await waitFor(() => expect(navigate).toHaveBeenCalledWith("alertas"));
    expect(notify).toHaveBeenCalledTimes(1);
  });
});

describe("map failure states and cleanup", () => {
  it("stacks loading and tile warnings while keeping municipal data visible", async () => {
    let resolveSummary: (value: MapSummaryResponse) => void = () => undefined;
    const summaryResult = new Promise<MapSummaryResponse>((resolve) => {
      resolveSummary = resolve;
    });
    const api = createApi({ total: 1, offset: 0, limit: 10, items: [processItem(1)] });
    api.get = vi.fn((path: string) => path.startsWith("/api/processos/mapa/resumo")
      ? summaryResult
      : Promise.resolve({ total: 1, offset: 0, limit: 10, items: [processItem(1)] }));
    renderMap(api);

    act(() => leafletMock.tileLayers[0].emit("tileerror"));

    const stack = document.querySelector(".map-overlay-stack");
    expect(stack).not.toBeNull();
    expect(within(stack as HTMLElement).getByText("Carregando municípios...")).toBeInTheDocument();
    expect(within(stack as HTMLElement).getByText(/Basemap indisponível/)).toBeInTheDocument();

    resolveSummary(summary);
    expect(await screen.findByRole("button", { name: /Cuiabá.*Centro-Sul.*2/i })).toBeInTheDocument();
  });

  it("removes tile listeners and the map on unmount", async () => {
    const api = createApi({ total: 1, offset: 0, limit: 10, items: [processItem(1)] });
    const view = renderMap(api);
    await screen.findByRole("button", { name: /Cuiabá.*Centro-Sul.*2/i });
    const map = leafletMock.maps[0];
    const tileLayer = leafletMock.tileLayers[0];

    view.unmount();

    expect(tileLayer.offTypes).toContain("tileerror");
    expect(map.remove).toHaveBeenCalledTimes(1);
  });
});


describe("summary payload failures", () => {
  it("clears stale municipal state and exposes retry when the aggregate payload is invalid", async () => {
    let summaryCalls = 0;
    const api = createApi({ total: 1, offset: 0, limit: 10, items: [processItem(1, "STALE-PROCESS")] });
    api.get = vi.fn((path: string) => {
      if (!path.startsWith("/api/processos/mapa/resumo")) {
        return Promise.resolve({ total: 1, offset: 0, limit: 10, items: [processItem(1, "STALE-PROCESS")] });
      }
      summaryCalls += 1;
      return Promise.resolve(summaryCalls === 1 ? summary : { ...summary, items: null });
    });
    renderMap(api);

    await selectCuiaba();
    expect(await screen.findByText("STALE-PROCESS")).toBeInTheDocument();

    await userEvent.click(screen.getByRole("button", { name: /Atualizar/i }));

    expect(await screen.findByText("N\u00e3o foi poss\u00edvel carregar o resumo territorial.")).toBeInTheDocument();
    expect(screen.queryByText("STALE-PROCESS")).not.toBeInTheDocument();
    expect(screen.queryByLabelText("Processos de Cuiab\u00e1")).not.toBeInTheDocument();

    await userEvent.click(screen.getByRole("button", { name: "Tentar novamente" }));
    await waitFor(() => expect(summaryCalls).toBe(3));
  });
});

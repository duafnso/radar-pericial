import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import test from "node:test";

const modelUrl = new URL("../src/map/model.ts", import.meta.url);

test("map model module exists", () => {
  assert.equal(existsSync(modelUrl), true);
});

if (existsSync(modelUrl)) {
  const { buildMapSummaryParams, markerTone } = await import(modelUrl.href);

  test("builds map query from local and global filters", () => {
    const params = buildMapSummaryParams({
      regiao: "",
      municipio: "Cuiab\u00e1",
      faixa: "janela_quente",
      dataInicio: "2026-01-01",
      dataFim: "",
    }, "Centro-Sul");

    assert.equal(params.get("regiao"), "Centro-Sul");
    assert.equal(params.get("municipio"), "Cuiab\u00e1");
    assert.equal(params.get("faixa"), "janela_quente");
    assert.equal(params.get("data_inicio"), "2026-01-01");
    assert.equal(params.get("data_fim"), null);
    assert.equal(params.get("limit_cidades"), "200");
  });

  test("maps probability bands to stable marker tones", () => {
    assert.equal(markerTone("janela_quente"), "critical");
    assert.equal(markerTone("provavel"), "high");
    assert.equal(markerTone("observacao"), "medium");
    assert.equal(markerTone("frio"), "low");
  });
}

from collector.judicial_collector import _normaliza_datajud


def test_normalizes_apostrophe_in_datajud_municipality():
    process = _normaliza_datajud(
        {
            "numeroProcesso": "10000000020268110001",
            "municipio": "Mirassol D´Oeste",
        }
    )

    assert process["municipio"] == "Mirassol D'Oeste"
    assert process["regiao_imea"] == "Oeste"


def test_extracts_municipality_when_comarca_omits_de():
    process = _normaliza_datajud(
        {
            "numeroProcesso": "10000000020268110002",
            "orgaoJulgador": {
                "nome": "Quarta Vara Cível - Comarca Lucas do Rio Verde - SDCR"
            },
        }
    )

    assert process["municipio"] == "Lucas do Rio Verde"
    assert process["regiao_imea"] == "Médio-Norte"

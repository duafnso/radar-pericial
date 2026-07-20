from intelligence.taxonomy import calcular_score


def test_score_explanation_lists_contributing_signals():
    score = calcular_score(
        classe_processual="Desapropriação",
        assunto="Avaliação de imóvel rural",
    )

    result = score.to_dict()

    assert result["explicacao_score"]
    assert "Classe processual" in result["explicacao_score"]


def test_score_explanation_identifies_baseline_when_inputs_are_empty():
    result = calcular_score().to_dict()

    assert result["explicacao_score"] == (
        "Pontuação-base da classe: 5 pontos; "
        "Pontuação-base do assunto: 5 pontos."
    )

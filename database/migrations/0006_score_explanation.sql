-- Persist an auditable explanation for each calculated score.

ALTER TABLE score_pericial
    ADD COLUMN IF NOT EXISTS explicacao_score TEXT;

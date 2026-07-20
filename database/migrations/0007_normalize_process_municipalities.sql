-- Normalize municipality names observed in the DataJud backfill.

UPDATE processos
SET municipio = 'Mirassol d''Oeste',
    regiao_imea = 'Oeste',
    atualizado_em = NOW()
WHERE municipio IN (
    'Mirassol D´Oeste',
    'Mirassol D`Oeste',
    'Mirassol D’Oeste',
    'Mirassol D‘Oeste'
);

UPDATE processos
SET municipio = 'Lucas do Rio Verde',
    regiao_imea = 'Médio-Norte',
    atualizado_em = NOW()
WHERE (municipio IS NULL OR btrim(municipio) = '')
  AND comarca ILIKE '%Comarca Lucas do Rio Verde%';

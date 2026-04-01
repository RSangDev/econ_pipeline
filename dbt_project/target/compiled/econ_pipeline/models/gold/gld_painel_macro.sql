

-- Painel macro: SELIC, IPCA, câmbio e PIB por ano
-- Pivota os indicadores em colunas para facilitar análise comparativa

WITH base AS (
    SELECT
        ano,
        indicador,
        ROUND(AVG(valor), 4) AS valor_medio,
        ROUND(MIN(valor), 4) AS valor_min,
        ROUND(MAX(valor), 4) AS valor_max,
        COUNT(*)              AS n_observacoes
    FROM "econ"."silver"."indicadores"
    WHERE ano >= 2015 AND valor IS NOT NULL
    GROUP BY ano, indicador
)

SELECT
    ano,
    MAX(CASE WHEN indicador = 'selic_mensal'   THEN valor_medio END) AS selic_media,
    MAX(CASE WHEN indicador = 'ipca_mensal'    THEN valor_medio END) AS ipca_media,
    MAX(CASE WHEN indicador = 'cambio_usd_brl' THEN valor_medio END) AS cambio_medio,
    MAX(CASE WHEN indicador = 'pib_variacao_pct' THEN valor_medio END) AS pib_variacao_media,
    MAX(CASE WHEN indicador = 'desemprego_pct' THEN valor_medio END) AS desemprego_medio,
    MAX(CASE WHEN indicador = 'renda_media_pc' THEN valor_medio END) AS renda_media_pc
FROM base
GROUP BY ano
ORDER BY ano
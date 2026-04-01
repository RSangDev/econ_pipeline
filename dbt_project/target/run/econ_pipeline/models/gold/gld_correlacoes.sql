
  
    
    

    create  table
      "econ"."main"."gld_correlacoes__dbt_tmp"
  
    as (
      

-- Correlação entre indicadores por ano
-- Responde: quando o câmbio sobe, o IPCA sobe também?

WITH mensal AS (
    SELECT
        ano, mes,
        MAX(CASE WHEN indicador = 'selic_mensal'    THEN valor END) AS selic,
        MAX(CASE WHEN indicador = 'ipca_mensal'     THEN valor END) AS ipca,
        MAX(CASE WHEN indicador = 'cambio_usd_brl'  THEN valor END) AS cambio,
        MAX(CASE WHEN indicador = 'desemprego_pct'  THEN valor END) AS desemprego
    FROM "econ"."silver"."indicadores"
    WHERE mes IS NOT NULL AND valor IS NOT NULL
    GROUP BY ano, mes
),

com_lags AS (
    SELECT
        ano, mes, selic, ipca, cambio, desemprego,
        LAG(ipca,   1) OVER (ORDER BY ano, mes) AS ipca_lag1,
        LAG(cambio, 1) OVER (ORDER BY ano, mes) AS cambio_lag1,
        LAG(selic,  1) OVER (ORDER BY ano, mes) AS selic_lag1
    FROM mensal
)

SELECT
    ano,
    ROUND(AVG(selic),      2) AS selic_media,
    ROUND(AVG(ipca),       2) AS ipca_media,
    ROUND(AVG(cambio),     2) AS cambio_medio,
    ROUND(AVG(desemprego), 2) AS desemprego_medio,
    -- Diferença cambio vs IPCA no mesmo mês
    ROUND(AVG(cambio - ipca_lag1), 2) AS cambio_vs_ipca_defasado
FROM com_lags
GROUP BY ano
ORDER BY ano
    );
  
  

  
    
    

    create  table
      "econ"."main"."gld_serie_mensal__dbt_tmp"
  
    as (
      

-- Série mensal de todos os indicadores com variação YoY
-- Usada pelos gráficos de linha do dashboard

SELECT
    indicador,
    categoria,
    fonte,
    ano,
    mes,
    periodo,
    ROUND(valor, 4)        AS valor,
    unidade,
    ROUND(variacao_yoy, 2) AS variacao_yoy,
    CASE
        WHEN variacao_yoy > 0  THEN 'alta'
        WHEN variacao_yoy < 0  THEN 'queda'
        WHEN variacao_yoy = 0  THEN 'estavel'
        ELSE 'sem_dado'
    END                    AS tendencia

FROM "econ"."silver"."indicadores"
WHERE mes IS NOT NULL
  AND valor IS NOT NULL
ORDER BY indicador, ano, mes
    );
  
  
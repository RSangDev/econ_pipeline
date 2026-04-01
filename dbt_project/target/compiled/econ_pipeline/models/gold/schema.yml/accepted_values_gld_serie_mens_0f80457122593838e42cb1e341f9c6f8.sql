
    
    

with all_values as (

    select
        tendencia as value_field,
        count(*) as n_records

    from "econ"."main"."gld_serie_mensal"
    group by tendencia

)

select *
from all_values
where value_field not in (
    'alta','queda','estavel','sem_dado'
)




    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

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



  
  
      
    ) dbt_internal_test
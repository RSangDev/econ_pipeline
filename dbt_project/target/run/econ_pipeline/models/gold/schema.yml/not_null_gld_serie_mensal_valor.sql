
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select valor
from "econ"."main"."gld_serie_mensal"
where valor is null



  
  
      
    ) dbt_internal_test
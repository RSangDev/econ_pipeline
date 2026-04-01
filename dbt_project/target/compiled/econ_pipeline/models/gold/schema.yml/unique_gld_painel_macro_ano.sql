
    
    

select
    ano as unique_field,
    count(*) as n_records

from "econ"."main"."gld_painel_macro"
where ano is not null
group by ano
having count(*) > 1



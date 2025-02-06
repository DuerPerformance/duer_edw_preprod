{{ config(
    materialized='table'
) }}
with cte as (select * from {{source('FIVETRAN_DB','V33_CR')}})
select * from cte
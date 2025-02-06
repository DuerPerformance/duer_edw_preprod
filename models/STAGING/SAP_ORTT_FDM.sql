{{ config(
    materialized='table'
) }}
with cte as (select * from {{source('FIVETRAN_DB','ORTT')}}),
cte1 as (select * from {{source('FIVETRAN_DB','ORTT_USA')}})
select *,'SAP' as Source,'CA' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML from cte
union
select *,'SAP' as Source,'US' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML from cte1

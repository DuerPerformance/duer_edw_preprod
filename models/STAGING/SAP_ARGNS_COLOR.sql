{{ config(
    materialized='table'
) }}
WITH cte as (select * from {{source('FIVETRAN_DB','ARGNS_COLOR')}})
select *,'SAP' as Source,'CA' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML from cte
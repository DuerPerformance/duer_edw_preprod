{{ config(
    materialized='table'
) }}
with ca as (select *,'SAP' as Source,'CA' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML
FROM {{source('FIVETRAN_DB','CA_JDT1')}}),
us as (select *,'SAP' as Source,'US' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML
FROM {{source('FIVETRAN_DB','US_JDT1')}})
select * from ca union all select * from us
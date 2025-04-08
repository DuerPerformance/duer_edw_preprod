{{ config(
    materialized='table'
) }}




with cte01 as (select DATE, SAP_LOCATION_ID, BUDGET_NAME, NET_SALES, 'CA' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML
FROM {{source('BUDGET','BUDGET_CGY_2026')}}),

cte02 as (select DATE, SAP_LOCATION_ID, BUDGET_NAME, NET_SALES, 'CA' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML
FROM {{source('BUDGET','BUDGET_VAN_2026')}}),

cte03 as (select DATE, SAP_LOCATION_ID, BUDGET_NAME, NET_SALES, 'CA' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML
FROM {{source('BUDGET','BUDGET_SQ_1_2026')}}),

cte04 as (select DATE, SAP_LOCATION_ID, BUDGET_NAME, NET_SALES, 'CA' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML
FROM {{source('BUDGET','BUDGET_OTT_2026')}}),

cte05 as (select DATE, SAP_LOCATION_ID, BUDGET_NAME, NET_SALES, 'CA' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML
FROM {{source('BUDGET','BUDGET_TO_2026')}}),

cte06 as (select DATE, SAP_LOCATION_ID, BUDGET_NAME, NET_SALES, 'CA' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML
FROM {{source('BUDGET','BUDGET_ED_2026')}}),

cte07 as (select DATE, SAP_LOCATION_ID, BUDGET_NAME, NET_SALES, 'US' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML
FROM {{source('BUDGET','BUDGET_LA_2026')}}),

cte08 as (select DATE, SAP_LOCATION_ID, BUDGET_NAME, NET_SALES, 'US' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML
FROM {{source('BUDGET','BUDGET_DEN_2026')}}),

cte09 as (select DATE, SAP_LOCATION_ID, BUDGET_NAME, NET_SALES, 'CA' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML
FROM {{source('BUDGET','BUDGET_ECOMM_CA_2026')}}),

cte010 as (select DATE, SAP_LOCATION_ID, BUDGET_NAME, NET_SALES, 'US' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML
FROM {{source('BUDGET','BUDGET_ECOMM_US_2026')}}),






cte1 as (select DATE, SAP_LOCATION_ID, BUDGET_NAME, NET_SALES, 'CA' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML
FROM {{source('BUDGET','BUDGET_CGY')}}),

cte2 as (select DATE, SAP_LOCATION_ID, BUDGET_NAME, NET_SALES, 'CA' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML
FROM {{source('BUDGET','BUDGET_VAN')}}),

cte3 as (select DATE, SAP_LOCATION_ID, BUDGET_NAME, NET_SALES, 'CA' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML
FROM {{source('BUDGET','BUDGET_SQ_1')}}),

cte4 as (select DATE, SAP_LOCATION_ID, BUDGET_NAME, NET_SALES, 'CA' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML
FROM {{source('BUDGET','BUDGET_OTT')}}),

cte5 as (select DATE, SAP_LOCATION_ID, BUDGET_NAME, NET_SALES, 'CA' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML
FROM {{source('BUDGET','BUDGET_TO')}}),

cte6 as (select DATE, SAP_LOCATION_ID, BUDGET_NAME, NET_SALES, 'CA' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML
FROM {{source('BUDGET','BUDGET_ED')}}),

cte7 as (select DATE, SAP_LOCATION_ID, BUDGET_NAME, NET_SALES, 'US' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML
FROM {{source('BUDGET','BUDGET_LA')}}),

cte8 as (select DATE, SAP_LOCATION_ID, BUDGET_NAME, NET_SALES, 'US' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML
FROM {{source('BUDGET','BUDGET_DEN')}}),

cte9 as (select DATE, SAP_LOCATION_ID, BUDGET_NAME, NET_SALES, 'CA' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML
FROM {{source('BUDGET','BUDGET_ECOMM_CA')}}),

cte10 as (select DATE, SAP_LOCATION_ID, BUDGET_NAME, NET_SALES, 'US' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML
FROM {{source('BUDGET','BUDGET_ECOMM_US')}}),

cte101 as (select DATE, SAP_LOCATION_ID, BUDGET_NAME, NET_SALES, 'CA' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML
FROM {{source('BUDGET','BUDGET_RC_07')}}),


cte11 as (select DATE, SAP_LOCATION_ID, BUDGET_NAME, NET_SALES, 'CA' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML
FROM {{source('BUDGET','BUDGET_CGY_2024')}}),

cte12 as (select DATE, SAP_LOCATION_ID, BUDGET_NAME, NET_SALES, 'CA' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML
FROM {{source('BUDGET','BUDGET_VAN_2024')}}),

cte13 as (select DATE, SAP_LOCATION_ID, BUDGET_NAME, NET_SALES, 'CA' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML
FROM {{source('BUDGET','BUDGET_SQ_1_2024')}}),

cte14 as (select DATE, SAP_LOCATION_ID, BUDGET_NAME, NET_SALES, 'CA' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML
FROM {{source('BUDGET','BUDGET_TO_2024')}}),

cte15 as (select DATE, SAP_LOCATION_ID, BUDGET_NAME, NET_SALES, 'US' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML
FROM {{source('BUDGET','BUDGET_LA_2024')}}),

cte16 as (select DATE, SAP_LOCATION_ID, BUDGET_NAME, NET_SALES, 'US' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML
FROM {{source('BUDGET','BUDGET_DEN_2024')}}),

cte17 as (select DATE, SAP_LOCATION_ID, BUDGET_NAME, NET_SALES, 'CA' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML
FROM {{source('BUDGET','BUDGET_ECOMM_CA_2024')}}),

cte18 as (select DATE, SAP_LOCATION_ID, BUDGET_NAME, NET_SALES, 'US' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML
FROM {{source('BUDGET','BUDGET_ECOMM_US_2024')}}),


cte19 as (select DATE, SAP_LOCATION_ID, BUDGET_NAME, NET_SALES, 'CA' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML
FROM {{source('BUDGET','BUDGET_CGY_2023')}}),

cte20 as (select DATE, SAP_LOCATION_ID, BUDGET_NAME, NET_SALES, 'CA' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML
FROM {{source('BUDGET','BUDGET_VAN_2023')}}),

cte21 as (select DATE, SAP_LOCATION_ID, BUDGET_NAME, NET_SALES, 'US' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML
FROM {{source('BUDGET','BUDGET_LA_2023')}}),

cte22 as (select DATE, SAP_LOCATION_ID, BUDGET_NAME, NET_SALES, 'US' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML
FROM {{source('BUDGET','BUDGET_DEN_2023')}}),

cte23 as (select DATE, SAP_LOCATION_ID, BUDGET_NAME, NET_SALES, 'CA' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML
FROM {{source('BUDGET','BUDGET_ECOMM_CA_2023')}}),

cte24 as (select DATE, SAP_LOCATION_ID, BUDGET_NAME, NET_SALES, 'US' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML
FROM {{source('BUDGET','BUDGET_ECOMM_US_2023')}})



select * from cte01 union all select * from cte02 union all select * from cte03 union all
select * from cte04 union all select * from cte05 union all select * from cte06 union all
select * from cte07 union all select * from cte08 union all select * from cte09 union all select * from cte010 union all



select * from cte1 union all select * from cte2 union all select * from cte3 union all
select * from cte4 union all select * from cte5 union all select * from cte6 union all
select * from cte7 union all select * from cte8 union all select * from cte9 union all select * from cte10 union all select * from cte101 union all

select * from cte11 union all select * from cte12 union all select * from cte13 union all
select * from cte14 union all select * from cte15 union all select * from cte16 union all
select * from cte17 union all select * from cte18 

union all select * from cte19 union all select * from cte20 union all

select * from cte21 union all select * from cte22 union all select * from cte23 union all
select * from cte24
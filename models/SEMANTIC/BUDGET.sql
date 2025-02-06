{{ config(
    materialized='table'
) }}
with cte1 as (select a.*, b.LOCATION_NAME, b.CHANNEL, c.* exclude DATE from {{ref('BUDGET_STG')}} a left join {{ref('DIM_LOCATION')}} b on
a.SAP_LOCATION_ID = b.LOCATION_ID left join {{ref('DIM_CALENDAR')}} c on a.DATE = c.DATE and c.CALENDAR_TYPE = 'Retail Calendar')

select * from cte1
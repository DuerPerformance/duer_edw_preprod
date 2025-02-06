{{ config(materialized='table')}}

WITH cte as
(
select *,'Shopify' as Source,'CA' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL AS UpdateDate, 'IO' AS DML 
from  {{ source('SHOPIFY_CA_SRC', 'CA_CUSTOMER') }}
UNION
select *,'Shopify' as Source,'US' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL AS UpdateDate, 'IO' AS DML 
from  {{ source('SHOPIFY_US_SRC', 'US_CUSTOMER') }}
)

SELECT * FROM cte
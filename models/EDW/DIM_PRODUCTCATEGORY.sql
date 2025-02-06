{{ config(
    materialized='table',
    schema='EDW',
    unique_key='PRODUCTCATEGORY_ID'
) }}


WITH joined 
as
 (

    select Distinct
    A."itmsgrpcod"   AS ProductCategory_ID,
    A."itmsgrpnam"   AS ProductCategoryName,
    A."locked"       AS GroupLocked,
    A."createdate"   AS GroupCreateDate,
    A.SOURCE,
    current_timestamp() AS InsertDate,
    NULL AS UpdateDate,
    'IO' AS DML,
    ROW_NUMBER() OVER (PARTITION BY ProductCategory_ID ORDER BY A."createdate") AS row_num

FROM {{ ref('SAP_OITB') }}  A

)


select 
 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS PRODUCTCATEGORY_KEY,
 ProductCategory_ID,
 ProductCategoryName,
 GroupLocked,
 GroupCreateDate,
 SOURCE,
 InsertDate,
 UpdateDate,
 DML,
   
from joined A
WHERE row_num = 1





 

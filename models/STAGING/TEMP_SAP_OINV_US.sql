WITH cte as
(
select *,'SAP' as Source,'US' as Source_Region,cast(current_timestamp() as TIMESTAMP_NTZ) as InsertDate, NULL AS UpdateDate, 'IO' AS DML 
from  {{ source('FIVETRAN_DB', 'US_OINV') }}

)

SELECT * FROM cte


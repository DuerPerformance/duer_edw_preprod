WITH cte as
(
select *,'SAP' as Source,'CA' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as InsertDate, NULL AS UpdateDate, 'IO' AS DML 
from  {{ source('FIVETRAN_DB', 'CA_OINV') }}

)

SELECT * FROM cte


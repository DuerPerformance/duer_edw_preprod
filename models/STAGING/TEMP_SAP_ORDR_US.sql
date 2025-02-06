with cte as (select * from {{source('FIVETRAN_DB','US_ORDR')}})
select *,'SAP' as Source,'US' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML
,'PPAUS' as SUBSIDIARY_ID
from cte
with cte as (select * from {{source('FIVETRAN_DB','US_OCRD')}})
select *,'SAP' as Source,'US' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML
from cte
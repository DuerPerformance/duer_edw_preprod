WITH temp as (
SELECT distinct 
CASE WHEN ordr."SOURCE_REGION"= 'CA'THEN CONCAT('CA_', ordr."cardcode")
ELSE CONCAT('USA_', ordr."cardcode") END as Location_ID,
 ordr."cardname" as Location_Name,
CASE
WHEN ordr."cardcode" LIKE 'SCAE%' THEN 'ECOM CA'
WHEN ordr."cardcode" LIKE 'SUSE%' THEN 'ECOM USA'
WHEN ordr."cardcode" LIKE 'SCAR%' THEN 'RETAIL CA'
WHEN ordr."cardcode" LIKE 'SUSR%' THEN 'RETAIL USA'
WHEN ordr."cardcode" LIKE 'C%' THEN 'WHOLESALE'
ELSE 'OTHER'
END Channel,
ordr."SOURCE_REGION" as SOURCE_REGION,
cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date,
NULL AS Update_Date,
'IO' as DML


FROM {{ ref('SAP_ORDR') }} ordr




)
SELECT 
row_number() over(order by a.location_id) as LOCATION_KEY,
a.location_id,
a.location_name,
a.channel,
a.Source_Region,
a.Insert_Date,
a.Update_Date,
a.DML
from temp a
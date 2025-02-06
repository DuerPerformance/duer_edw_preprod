
WITH calctable as 
(
    select distinct
        CONCAT(
            "doccur", '_', 
            TO_CHAR("docrate"), '_', 
            REPLACE(TO_CHAR("docdate", 'YYYYMMDD'), '-', '')
        ) as Currency_ID,
        "doccur" as Currency_Code,
        "docrate" as Currency_Rate,
        "docdate" as Currency_Date,
        CASE 
            WHEN "doccur" = 'USD' THEN 'US Dollar' 
            WHEN "doccur" = 'CAD' THEN 'Canadian Dollar' 
            WHEN "doccur" = 'EUR' THEN 'Euro' 
            ELSE 'Other' 
        END as Currency_Name,
        SOURCE,
        SOURCE_REGION,
        current_timestamp() as InsertDate,
        NULL AS UpdateDate,
        'IO' as DML
    from {{ ref('SAP_ORDR') }}
    /*{% if is_incremental() %}
        where "docdate" not exists (select  from {{ this }})
    {% endif %}*/
)
SELECT 
    row_number() over (order by 1) AS Currency_Key,
    a.Currency_ID,
    a.Currency_Code,
    a.Currency_Rate,
    a.Currency_Date,
    a.Currency_Name,
    a.source,
    a.source_region,
    a.InsertDate,
    a.UpdateDate,
    a.DML
from calctable a
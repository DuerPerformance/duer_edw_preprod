with cte as (select a.*, b."u_v33costctr", b."u_v33_ter",b."country", c."cityb",c."citys",c."stateb",c."states",d."name" as order_type,
e."slpname"
from {{ref('SAP_ORDR')}} a 
left join {{ref('SAP_OCRD')}} b on a."cardcode" = b."cardcode" and a.SOURCE_REGION = b.SOURCE_REGION
left join {{ref('SAP_RDR12')}} c on a."docentry" = c."docentry" and a.SOURCE_REGION = c.SOURCE_REGION
left join {{ref('SAP_V33_SO_TYPE')}} d on a."u_v33_ordertype" = d."code"
left join {{ref('SAP_OSLP')}} e on b."slpcode" = e."slpcode" and b.SOURCE_REGION = e.SOURCE_REGION),

cte2 as ( select a."docentry", a.SOURCE_REGION,

    CASE
WHEN a."docstatus" = 'O' AND SUM(b."delivrdqty") = 0 THEN 'OPEN'
WHEN a."docstatus" = 'O' AND SUM(b."delivrdqty") > 0  AND SUM(b."delivrdqty") < SUM(b."quantity") THEN 'PARTIALLY SHIPPED, OPEN'
WHEN a."docstatus" = 'C' AND SUM(b."delivrdqty") = 0 THEN 'CANCELLED'
WHEN a."docstatus" = 'C' AND SUM(b."delivrdqty") = SUM(b."quantity") THEN 'FULLY SHIPPED'
WHEN a."docstatus" = 'C' AND SUM(b."delivrdqty") > 0 AND SUM(b."delivrdqty") < SUM(b."quantity") THEN 'PARTIALLY SHIPPED, CANCELLED'
ELSE 'UNKNOWN'
END order_status,

FROM {{ref('SAP_ORDR')}} a left join {{ref('SAP_RDR1')}} b on a."docentry" = b."docentry" and a.SOURCE_REGION = b.source_region group by a."docentry", a.SOURCE_REGION, a."docstatus")


select distinct
    row_number() over (order by 1) AS Order_Key,
    a."docentry" as DocEntry,
    --"docnum" as DocNumber,
    a.SOURCE_REGION || '_' || "docnum" as Docnumber,
    "numatcard" as OrderNumber,
    CASE WHEN "docstatus" = 'R' Then 'Y' ELSE 'N' END as IsRejected,
    CASE When "numatcard" like 'EXC%' then 'Y' else 'N' END as ExchangeFlag,
    "canceled" as IsCanceled,
    CASE WHEN "u_v33s_ic_docsource" = 'InterComp' then 'Y' ELSE 'N' END as intercompany_flag, 
    "docstatus" as OrderStatus,
    b.order_status,
    "u_dd_season" as Wholesale_season,

    "u_v33p_discountcode" as discount_code,
    c."name" as order_reason_canceled,
    coalesce(order_type, 'Unknown') as order_type,
    "cityb" as Billing_city,
    "stateb" as Billing_province,
    "citys" as shipping_city,
    "states" as shipping_province,


    "cardcode" as loc_code,
    "cardname" as loc_name,
    "u_v33costctr" as WS,
    "u_v33_ter" as WS_loc,
    "country" as Wholesale_country,
    "slpname" as Wholesale_representative,

    SOURCE,
    a.SOURCE_REGION,
    cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date,
    NULL AS UpdateDate,
    'IO' as DML
FROM cte a inner join cte2 b on a."docentry" = b."docentry" and a.SOURCE_REGION = b.SOURCE_REGION
left join {{ref('SAP_V33_CR')}} c on a."u_v33_co" = c."code"
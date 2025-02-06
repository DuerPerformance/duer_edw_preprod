with cte as (select a.*, b."u_v33costctr", b."u_v33_ter",b."country", c."cityb",c."citys",c."stateb",c."states" from {{ref('SAP_ORDR')}} a left join {{ref('SAP_OCRD')}} b on a."cardcode" = b."cardcode" and a.SOURCE_REGION = b.SOURCE_REGION
left join {{ref('SAP_RDR12')}} c on a."docentry" = c."docentry" and a.SOURCE_REGION = c.SOURCE_REGION)
select distinct
    row_number() over (order by 1) AS Order_Key,
    "docentry" as DocEntry,
    "docnum" as DocNumber,
    "numatcard" as OrderNumber,
    CASE WHEN "docstatus" = 'R' Then 'Y' ELSE 'N' END as IsRejected,
    "canceled" as IsCanceled,
    "docstatus" as OrderStatus,

    "u_v33p_discountcode" as discount_code,
    "u_v33_co" as order_reason_canceled,
    "u_argns_ordertype" as order_type,
    "cityb" as Billing_city,
    "stateb" as Billing_province,
    "citys" as shipping_city,
    "states" as shipping_province,


    "cardcode" as loc_code,
    "cardname" as loc_name,
    "u_v33costctr" as WS,
    "u_v33_ter" as WS_loc,
    "country" as Wholesale_country,

    SOURCE,
    SOURCE_REGION,
    cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date,
    NULL AS UpdateDate,
    'IO' as DML
FROM cte
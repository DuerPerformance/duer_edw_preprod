select 
a."docentry" as inv_docentry,
a."linenum" as inv_LINENUM,
e.date_key_2 as invoice_date_key,
a."price" as inv_Price,
a."quantity" as inv_Quantity,
a."delivrdqty" as inv_delivered_quantity,
a."openqty" as inv_open_quantity,
a."pricebefdi" as inv_Pricebefdi,
a."linetotal" as inv_Linetotal,
a."vatsum" as inv_vatsum,
a."gtotal" as inv_Gross_Total,
a."discprcnt" as inv_Product_discount_Percentage,
a."itemcode" as inv_item_code,

a."baseentry" as inv_baseentry,
a."baseline" as inv_baseline,


a.Source, a.Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML
from {{ref('SAP_INV1')}} a
LEFT JOIN {{ref('DIM_CALENDAR')}} e on cast(a."docdate" as DATE) = e.DATE AND e.calendar_type = 'Retail Calendar'

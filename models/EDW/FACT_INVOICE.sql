select "docentry" as inv_docentry,
"docnum" as inv_docnum,
"discsum" as inv_OrderDiscount,
"discprcnt" as inv_Order_discount_percentage,
"docdate" as inv_date,
Source, Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML
from {{ref('SAP_OINV')}}
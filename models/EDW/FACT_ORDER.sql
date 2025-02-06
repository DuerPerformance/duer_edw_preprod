with location as (select source_region, "docentry", CASE WHEN SOURCE_REGION= 'CA'THEN CONCAT('CA_', "cardcode")
ELSE CONCAT('USA_',"cardcode") END as Location_ID, 
"cardname" as Location_name
from {{ref('SAP_ORDR')}})
select 
a.Order_key,
c.location_key,
c1.date_KEY_2 as orderdatekey,
c2.date_KEY_2 as deliverydatekey,
c3.date_KEY_2 as canceldatekey,
c4.date_KEY_2 as reqshipdatekey,
crd.date_key_2 as crossdockdatekey,
--pd.date_key_2 as packdatekey,
cr.currency_key,
s.SUBSIDIARY_KEY,
CASE WHEN bb."docstatus" = 'R' Then 1 ELSE 0 END as IsRejected,
bb. "doccur" as Currency,
bb."doctotal"-bb."vatsum"+bb."discsum" as OrderPrice,
bb."doctotal" as OrderNetPrice,
bb."discsum" as OrderDiscount,
bb."discprcnt" as Order_discount_percentage,
bb."vatsum" as OrderTax,
bb."expansum" as OrderExpenditure,
bb. "docstatus" as OrderStatus,
bb.Source, bb.Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL as Update_Date, 'IO' as DML


from {{ref('SAP_ORDR')}} bb
left join location l on l."docentry" = bb."docentry" and bb.source_region = l.source_region
left join {{ref('DIM_ORDER')}} a on bb."docentry" = a.docentry and bb.source_region = a.source_region
left join {{ref('DIM_LOCATION')}} c on c.location_id = l.location_id AND c.Location_name = l.Location_name
left join {{ref('DIM_CUSTOMER')}} cu on bb."cardcode" = cu.customer_id and bb."cardname" = cu.customer_name and
bb.source_region = cu.source_region
left join {{ref('DIM_CALENDAR')}} c1 on bb."docdate" = c1.DATE AND c1.CALENDAR_TYPE = 'Retail Calendar'
left join {{ref('DIM_CALENDAR')}} c2 on bb."docduedate" = c2.DATE AND c2.CALENDAR_TYPE = 'Retail Calendar'
left join {{ref('DIM_CALENDAR')}} c3 on bb."canceldate" = c3.DATE AND c3.CALENDAR_TYPE = 'Retail Calendar'
left join {{ref('DIM_CALENDAR')}} c4 on bb."reqdate" = c4.DATE AND c4.CALENDAR_TYPE = 'Retail Calendar'
left join {{ref('DIM_CALENDAR')}} crd on bb."u_dd_xd_movedate" = crd.DATE AND crd.CALENDAR_TYPE = 'Retail Calendar'
left join {{ref('DIM_CURRENCY')}} cr on CONCAT(bb."doccur", '_', TO_CHAR(bb."docrate"), '_',
 REPLACE(TO_CHAR(bb."docdate", 'YYYYMMDD'), '-', '')) = cr.currency_id
 left join EDW_PREPROD.EDW.DIM_SUBSIDIARY s on bb.SUBSIDIARY_ID = s.SUBSIDIARY_ID

 --left join {{ref('SAP_ODLN')}} odln on bb."docnum" = odln."u_v33_sonum" and bb.SOURCE_REGION = odln.SOURCE_REGION
 --left join {{ref('DIM_CALENDAR')}} pd on odln."docdate" = pd.DATE AND pd.CALENDAR_TYPE = 'Retail Calendar'
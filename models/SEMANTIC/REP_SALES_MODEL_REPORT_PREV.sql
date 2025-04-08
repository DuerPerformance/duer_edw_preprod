with cte as (select DATE as budget_date, BUDGET_NAME, NET_SALES as BUDGET, SOURCE_REGION as BUDGET_REGION, LOCATION_NAME as BUDGET_LOCATION
from {{ref('BUDGET')}}),
cte2 as (

select
c.calendar_type,c.year_name,c.quarter_name,c.month_name,c.week_name,c.day_name,c.date,
((a.DELIVERED_QUANTITY*a.pricebefdi)*(1-a.PRODUCT_DISCOUNT_PERCENTAGE)) as Actual_Revenue_Delivered
,((a.DELIVERED_QUANTITY*a.pricebefdi)) as Actual_Revenue_Gross
,((a.DELIVERED_QUANTITY*a.pricebefdi)*(1-a.PRODUCT_DISCOUNT_PERCENTAGE)) as Actual_Revenue_Net
,((a.pricebefdi*a.quantity)-(a.price*a.quantity)) as Actual_Sales_Discount
,(a.pricebefdi*a.quantity) as Actual_Sales_Gross
,((a.price*a.quantity)) as Actual_Sales_Net
,(A.REFUND) AS Actual_Sales_Returns,
a.price * a.open_quantity * b.rate  as actual_sales_open,
a.DELIVERED_QUANTITY as ACTUAL_SALES_DELIVERED_QUANTITY,
a.QUANTITY as ACTUAL_SALES_GROSS_QUANTITY,

((a.pricebefdi*a.quantity - a.price*a.quantity) / nullif(a.pricebefdi*a.quantity,0)) * 100 as percentage_discount,
(a.REFUND/nullif(a.pricebefdi*a.quantity,0)) * 100 as percentage_returns,

a.quantity - a.refund_quantity as ACTUAL_SALES_NET_QUANTITY,
a.PRICE,
a.QUANTITY,
a.DELIVERED_QUANTITY,
a.OPEN_QUANTITY,
a.PRICEBEFDI,
a.LINETOTAL,
a.VATSUM,
a.GROSS_TOTAL,
a.PRICE_DELIVERED,
a.PRODUCT_DISCOUNT,
a.PRODUCT_DISCOUNT_PERCENTAGE,
a.VAT_PERCENTAGE,
a.REFUND,
a.REFUND_TAX,
a.REFUND_QUANTITY,
a.ISGIFTCARD as Shopify_Flag_Gift_Card,
case when a.ISPROMOITEM = 'Y' OR b.ISPROMOITEM_ORDER = 'Y' THEN 'Y' ELSE 'N' END as Shopify_Flag_Promo_Item,
a.LINE_STATUS,
a.ORDER_LINE_REASON_CANCELED,
a.SOURCE_REGION, a.DOCENTRY, a.DOCNUMBER as SAP_ORDER_ID, a.ORDERNUMBER,a.line_num,MAX(line_num) over(partition by b.ordernumber) as max_line_num, a.ISREJECTED, a.ISCANCELED as Order_Line_Flag_Canceled, a.ORDERSTATUS as Order_Hearder_status, a.order_status, a.DISCOUNT_CODE, a.ORDER_REASON_CANCELED, a.ORDER_TYPE, a.BILLING_CITY, a.BILLING_PROVINCE, a.SHIPPING_CITY, a.SHIPPING_PROVINCE, a.LOC_CODE, a.LOC_NAME, a.WS, a.WS_LOC, a.WHOLESALE_COUNTRY, a.SOURCE_REGION_ORDER, a.DATE as ORDER_CREATE_DATE, a.SHIPPING_DATE , a.DAY_NAME AS DAYNAME2, a.Product_Key,

a.invoice_docentry, a.invoice_targetentry, a.invoice_linenum,

 --a.PRODUCT_ID as ITEM_CODE, a.PRODUCTNAME as ITEM_NAME, a.PRODUCTDESCRIPTION, a.PRODUCTTYPE, a.BRAND, a.GENDER, a.DIVISION, a.FABRIC, a.FIT, a.MODEL_CODE, a.MODEL_NAME, a.MODEL_DESCRIPTION, a.SIZE_VARIANT, a.STYLE, a.STYLE_DESCRIPTION, a.STYLE_NAME, a.STYLE_NAME as STYLE_CODE, a.CHANNEL_ASSORTMENT, a.FIRST_SEASON, a.NEXT_SEASON, a.SEASON, a.SEASONAL, a.PRODUCT_LIFECYCLE, a.COLOR, a.REPLENISHMENT_TYPE, a.ACTIVE, a.DESCRIPTION, a.ITEM_PURCHASE, a.ITEM_SALES, a.MANUFACTURER_ID_NUMBER, a.SEGMENTATION, a.SIZE_DESCRIPTION, a.SIZE, a.SIZE_DESC, a.SIZE_VALUE, a.SIZE_DTC_INSEAM_RANGE, a.INSEAM_VALUE, a.SIZE_DTC_INSEAM_MAX, a.SIZE_DTC_INSEAM_MIN, a.SIZE_DTC_RANGE, a.SIZE_DTC_MAX, a.SIZE_DTC_MIN, a.REPLEN_STATUS, a.SIZE_WHOLESALE_INSEAM_RANGE, a.SIZE_WHOLESALE_RANGE, a.ACTIVE_DTC, a.SIZE_TYPE, a.SKU_YEAR, a.UPC, a.ITEM_INVENTORY, a.COLOR_CODE, a.DIM_HEIGHT, a.DIM_LENGTH, a.DIM_VOLUME, a.DIM_WEIGHT, a.DIM_WIDTH, a.LAST_PRICE, a.ITEM_GROUP, a.WAIST_SIZE_RANGE,cast(a.CREATEDATE as DATE) as PRODUCTCREATEDATE, a.LATEST_FLAG, a.ROW_NUM, 
a.WAREHOUSE_ID, a.WAREHOUSE_NAME, a.WH_COUNTRY, a.WH_STATE, a.WH_CITY, a.WH_STREET, a.WH_POSTAL, a.ISACTIVE
,b.REQSHIP_DATE as REQUESTED_SHIP_DATE
,a.SHIPPING_DATE as Actual_Ship_date
,b.CROSSDOCK_DATE as Cross_dock_date
,b.SUBSIDIARY_ID,
b.S_NAME,
b.S_CURRENCY,
b.PARENT_ID,
b.SUBSIDIARYL0,
b.SUBSIDIARYL1,
b.SUBSIDIARYL2,
b.SUBSIDIARYL3,
b.ExchangeFlag as Shopify_Flag_Exchange,
b.intercompany_flag,
b.Wholesale_representative,
b.Wholesale_season,
b.customer_type,
b.ISREJECTED as ISREJECTED_ORDER ,b.CURRENCY,b.ORDERPRICE,b.ORDERNETPRICE,b.ORDERDISCOUNT,b.Order_discount_percentage,b.ORDERTAX,b.ORDEREXPENDITURE,b.ORDERSTATUS AS ORDERSTATUS_ORDER ,b.SOURCE_REGION AS source_region_2,
--b.DOCENTRY AS DOCENTRY_ORDR ,b.DOCNUMBER AS DOCNUMBER_ORDR,b.ORDERNUMBER AS ORDERNUMBER_ORDER,
b.ISCANCELED AS Order_Flag_Canceled ,b.LOCATION_ID,b.LOCATION_NAME,b.CHANNEL as channel_region,b.CURRENCY_ID,b.CURRENCY_CODE,b.CURRENCY_RATE, cast(b.CURRENCY_DATE as DATE) as CURRENCY_DATE,b.CURRENCY_NAME,b.RATE,b.currency_rate_2,
case when b.channel like 'ECOM%' then 'Ecommerce' when b.channel like 'RETAIL%' then 'Retail' when b.channel like 'WHOLESALE' then 'Wholesale' ELSE 'Other' end as channel,
case when b.channel like 'ECOM%' then 'DTC' when b.channel like 'RETAIL%' then 'DTC' when b.channel like 'WHOLESALE' then 'Wholesale' else 'Other' end as Line_of_business
,cte.BUDGET
FROM {{ref('REP_ORDERDETAIL')}} a
INNER JOIN {{ref('REP_ORDER')}} b ON a.DOCENTRY = b.DOCENTRY AND a.SOURCE_REGION = b.SOURCE_REGION
INNER JOIN {{ref('DIM_CALENDAR')}} c On c.Date = a.Date and c.CALENDAR_TYPE = 'Retail Calendar'
left join cte on a.DATE = cte.budget_date and a.LOC_NAME= cte.BUDGET_LOCATION )

select a.*,
b.* exclude (SOURCE_REGION, inv_item_code),
b.source_region || '_' || b.inv_docnum as inv_num,
--null as customer_type,
from cte2 a left join {{ref('REP_INVOICE')}} b on a.invoice_targetentry = b.inv_docentry
and a.invoice_docentry = b.inv_baseentry and a.invoice_linenum = b.inv_baseline and a.source_region = b.source_region  




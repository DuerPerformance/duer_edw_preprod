WITH selected_date AS (
    SELECT *
    FROM {{ref('DIM_CALENDAR')}}
    WHERE --DATE = '2024-09-19' AND
        CALENDAR_TYPE = 'Retail Calendar'
),
prev_year_date AS (
    SELECT b.date_key_2, b. Date as cur_date, a.Date as prev_date 
    FROM {{ref('DIM_CALENDAR')}} a
    INNER JOIN selected_date b ON a.DATE_OF_YEAR = b.DATE_OF_YEAR -- Explicitly referencing `a.date` and `b.date`
    WHERE  a.YEAR_SERIAL = b.YEAR_SERIAL - 1 and
         a.CALENDAR_TYPE = 'Retail Calendar' 
)
--select * from prev_year_date where cur_date = '2024-09-19'
,
cte1 AS (
    SELECT s.date_key_2,b.channel,
           b.location_name,
           a.DATE AS order_date, -- Aliased the column to avoid ambiguity
           SUM((a.price * a.quantity) * currency_rate) AS NET_SALES,
           SUM(A.REFUND  * currency_rate) AS RETURNS,
           SUM((A.PRICEBEFDI * A.QUANTITY - A.PRICE * A.QUANTITY) * currency_rate) AS DISCOUNT,
           SUM(A.PRICEBEFDI * A.QUANTITY * currency_rate) AS TEMP
    FROM {{ref('REP_ORDERDETAIL')}} a
    INNER JOIN {{ref('REP_ORDER')}} b ON a.DOCENTRY = b.DOCENTRY
        AND a.SOURCE_REGION = b.SOURCE_REGION
        INNER JOIN prev_year_date s On a.Date = s.cur_date
    WHERE b.channel IN ('ECOM CA', 'ECOM USA')
        AND b.ordernumber NOT LIKE 'EXC%'
        AND isgiftcard <> 'Y'
        AND a.product_discount_percentage <> 100
    GROUP BY b.channel, b.location_name, a.DATE, s.date_key_2
)
--select * from cte1 where order_date = '2024-09-01'
,
cte2 AS (
    SELECT b.channel,
           b.location_name,
           a.DATE AS order_date, -- Aliased the column to avoid ambiguity
           p.cur_date,
           SUM((a.price * a.quantity) * currency_rate) AS NET_SALES
    FROM {{ref('REP_ORDERDETAIL')}} a
    INNER JOIN {{ref('REP_ORDER')}} b ON a.DOCENTRY = b.DOCENTRY
        AND a.SOURCE_REGION = b.SOURCE_REGION
        INNER JOIN prev_year_date p ON a.Date = P.prev_date
    WHERE b.channel IN ('ECOM CA', 'ECOM USA')
        AND b.ordernumber NOT LIKE 'EXC%'
        AND isgiftcard <> 'Y'
        AND a.product_discount_percentage <> 100
    GROUP BY b.channel, b.location_name, a.DATE,p.cur_date
)
--select * from cte2 where order_date = '2023-09-21'
,
cte3 AS (
    SELECT a.channel,
           a.location_name,
           a.order_date,
           a.NET_SALES,
           a.RETURNS,
           a.DISCOUNT,
           a.TEMP,
           b.net_sales as prev_year,
           a.date_key_2,
COALESCE(ROUND(((a.Net_sales - NULLIF(b.net_sales,0)) / NULLIF(b.net_sales,0)) * 100),-100) as "YOY %"
    FROM cte1 a
    INNER JOIN cte2 b ON a.channel = b.channel AND a.location_name = b.location_name AND a.order_date = b.cur_date
)
--select * from cte3 where order_date = '2024-09-01' 
,
cte4 AS ( select channel, location_name, order_date, date_key_2, Sum(NET_SALES) as NET_SALES,
Sum(RETURNS) as RETURNS, Sum(DISCOUNT) as DISCOUNT , Sum(TEMP) as TEMP
from
    (SELECT s.date_key_2, b.channel,
           b.location_name,
           b.ordernumber,
           a.DATE AS order_date, -- Aliased the column to avoid ambiguity
           SUM(a.price * a.quantity * rate) - ORDERDISCOUNT * rate AS NET_SALES,
            SUM(A.REFUND  * rate) AS RETURNS,
           SUM((A.PRICEBEFDI * A.QUANTITY - A.PRICE * A.QUANTITY) * rate) + orderdiscount*rate 
           AS DISCOUNT,
           SUM(A.PRICEBEFDI * A.QUANTITY * rate) AS TEMP
    FROM {{ref('REP_ORDERDETAIL')}} a
    INNER JOIN {{ref('REP_ORDER')}} b ON a.DOCENTRY = b.DOCENTRY
        AND a.SOURCE_REGION = b.SOURCE_REGION
        INNER JOIN prev_year_date s On a.Date = s.cur_date
    WHERE b.channel IN ('RETAIL CA', 'RETAIL USA')
        AND b.ordernumber NOT LIKE 'EXC%'
        --AND isgiftcard <> 'Y'
        AND a.product_discount_percentage <> 100
    GROUP BY b.channel, b.location_name, b.ordernumber, a.DATE, ORDERDISCOUNT, rate, s.date_key_2
    having (SUM(a.price * a.quantity * rate) - ORDERDISCOUNT * rate) > 0 AND count(*) <> count(case when isgiftcard = 'Y' then 1 end))
    GROUP BY channel, location_name, order_date, date_key_2
)
,
cte5 AS ( select channel, location_name, order_date, ROUND(Sum(NET_SALES)) as NET_SALES,cur_date from
    (SELECT b.channel,
           b.location_name,
           b.ordernumber,
           a.DATE AS order_date, -- Aliased the column to avoid ambiguity
           SUM(a.price * a.quantity * rate) - ORDERDISCOUNT * rate AS NET_SALES,
            s.cur_date
    FROM {{ref('REP_ORDERDETAIL')}} a
    INNER JOIN {{ref('REP_ORDER')}} b ON a.DOCENTRY = b.DOCENTRY
        AND a.SOURCE_REGION = b.SOURCE_REGION
        INNER JOIN prev_year_date s On a.Date = s.prev_date
    WHERE b.channel IN ('RETAIL CA', 'RETAIL USA')
        AND b.ordernumber NOT LIKE 'EXC%'
        --AND isgiftcard <> 'Y'
        AND a.product_discount_percentage <> 100
    GROUP BY b.channel, b.location_name, b.ordernumber, a.DATE, ORDERDISCOUNT, rate,cur_date
    having (SUM(a.price * a.quantity * rate) - ORDERDISCOUNT * rate) > 0 AND count(*) <> count(case when isgiftcard = 'Y' then 1 end))
    GROUP BY channel, location_name, order_date,cur_date
),
cte6 AS (
    SELECT a.channel,
           a.location_name,
           a.order_date,
           a.NET_SALES,
           a.RETURNS,
           a.DISCOUNT,
           a.TEMP,
           b.net_sales as prev_year,
           a.date_key_2,
COALESCE(ROUND(((a.Net_sales - NULLIF(b.net_sales,0)) / NULLIF(b.net_sales,0)) * 100),-100) as "YOY %"
    FROM cte4 a
    left JOIN cte5 b ON a.channel = b.channel AND a.location_name = b.location_name AND a.order_date = b.cur_date
),
--select * from cte6 where order_date = '2024-09-01'

cte7 as (select * from cte3 union all select * from cte6),
cte8 as (select location_name, DATE, SUM(NET_SALES) as db from {{ref('BUDGET')}} where DATE IN (select order_date from cte7) group by location_name, DATE)
--select * from cte8 where order_date = '2024-09-01'


--select * from cte7 where order_date = '2024-09-01'

select case when a.CHANNEL like '%ECOM%' then 'Ecommerce' Else 'Retail' END as Channel1,
case when a.CHANNEL like '% CA%' then 'CA' ELSE 'US' end as channel2,
case when a.location_name like '%CA%Online%' then 'CA - Online'
when a.location_name like '%US%Online%' then 'US - Online'
when a.location_name like '%CA%Calgary%' then 'CA - Calgary'
when a.location_name like '%CA%Gastown%' then 'CA - Vancouver'
when a.location_name like '%CA%Ottawa%' then 'CA - Ottawa'
when a.location_name like '%CA%Queens%' then 'CA - Queens West'
when a.location_name like '%CA%Squ%' then 'CA - Square One'
when a.location_name like '%US%Denver%' then 'US - Denver'
when a.location_name like '%US%LA%' then 'US - LA' END as channel3,
ROUND(a.Net_sales) as "Actual Sales (Net) $",a."YOY %" as "YOY %", b.db as "Budget (Net $)", ROUND((a.Net_sales/coalesce(NULLIF(b.db,0),-100))*100) as "% Budget",
ROUND(a.RETURNS) as "Returns", 
ROUND((a.RETURNS/a.TEMP)*100) as "% Returns",
ROUND(a.DISCOUNT) as "Discount",
ROUND((a.DISCOUNT/a.TEMP)*100) as "% Discount",
ROUND(a.TEMP) as Temp,
a.prev_year,
a.order_date,
a.date_key_2
from cte7 a inner join
cte8 b on a.location_name = b.location_name and a.order_date = b.date
--where a.order_date = '2024-09-20'
order by channel1, channel2, channel3
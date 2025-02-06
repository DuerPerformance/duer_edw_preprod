with cur_date as ( select * from {{ref('DIM_CALENDAR')}} where DATE = 
(select cast(max("docdate") as DATE) from  {{ref('SAP_RDR1')}} where SOURCE_REGION = 'US')
and CALENDAR_TYPE = 'Retail Calendar'),


prev_year_date AS (
    SELECT a. Date 
    FROM {{ref('DIM_CALENDAR')}} a
    INNER JOIN cur_date b ON a.DATE_OF_YEAR = b.DATE_OF_YEAR -- Explicitly referencing `a.date` and `b.date`
    WHERE  a.YEAR_SERIAL = b.YEAR_SERIAL - 1 and
         a.CALENDAR_TYPE = 'Retail Calendar' 
),
weekly as (select a.date from {{ref('DIM_CALENDAR')}} a inner join cur_date b on 
a.year_serial = b.year_serial and a.quarter_serial = b.quarter_serial and a.month_serial = b.month_serial
and a.week_serial = b.week_serial and a.CALENDAR_TYPE = 'Retail Calendar' and a.date <= b.date
)
,
cte1 as (select
b.channel, b.location_name, sum(Price * Quantity * currency_rate) as Net_Sales,
    c.net_sales as daily_budget    
    FROM {{ref('REP_ORDERDETAIL')}} a
    INNER JOIN {{ref('REP_ORDER')}} b ON a.DOCENTRY = b.DOCENTRY
        AND a.SOURCE_REGION = b.SOURCE_REGION
        INNER JOIN cur_date s On a.Date = s.date
left JOIN {{ref('BUDGET')}} c on b.channel = c.channel AND b.location_name = c.location_name AND
a.date = c.DATE
    WHERE b.channel IN ('ECOM CA', 'ECOM USA')
        AND b.ordernumber NOT LIKE 'EXC%'
        AND isgiftcard <> 'Y'
        AND a.product_discount_percentage <> 100
    GROUP BY b.channel, b.location_name,c.net_sales),
    
cte2 AS (select t.channel, t.location_NAME, SUM(t.NET_SALES) as NET_SALES, 
    c.net_sales as daily_budget
from
    (SELECT b.channel,
           b.location_name,
           b.ordernumber,
           a.DATE,
           SUM(a.price * a.quantity*RATE) - orderdiscount*RATE  AS NET_SALES,
    FROM {{ref('REP_ORDERDETAIL')}} a
    INNER JOIN {{ref('REP_ORDER')}} b ON a.DOCENTRY = b.DOCENTRY
        AND a.SOURCE_REGION = b.SOURCE_REGION
       inner join cur_date s On a.Date = s.date
    WHERE b.channel in ('RETAIL CA', 'RETAIL USA') and b.location_name NOT LIKE '%Edmonton%'
        AND b.ordernumber NOT LIKE 'EXC%'
        AND isgiftcard <> 'Y'
        AND a.product_discount_percentage <> 100
    GROUP BY b.channel, b.location_name,b.ordernumber, a.DATE , ORDERDISCOUNT, RATE) t
    left JOIN {{ref('BUDGET')}} c on t.channel = c.channel AND t.location_name = c.location_name AND
t.date = c.DATE
    group by t.channel, t.location_NAME, c.net_sales
),

cte3temp as (select
b.channel, b.location_name, sum(Price * Quantity * currency_rate) as Net_Sales   
    FROM {{ref('REP_ORDERDETAIL')}} a
    INNER JOIN {{ref('REP_ORDER')}} b ON a.DOCENTRY = b.DOCENTRY
        AND a.SOURCE_REGION = b.SOURCE_REGION
        INNER JOIN prev_year_date s On a.Date = s.date
    WHERE b.channel IN ('ECOM CA', 'ECOM USA')
        AND b.ordernumber NOT LIKE 'EXC%'
        AND isgiftcard <> 'Y'
        AND a.product_discount_percentage <> 100
    GROUP BY b.channel, b.location_name),

cte4temp AS (select t.channel, t.location_NAME, SUM(t.NET_SALES) as NET_SALES from
    (SELECT b.channel,
           b.location_name,
           b.ordernumber,
           a.DATE,
           SUM(a.price * a.quantity*RATE) - orderdiscount*RATE  AS NET_SALES,
    FROM {{ref('REP_ORDERDETAIL')}} a
    INNER JOIN {{ref('REP_ORDER')}} b ON a.DOCENTRY = b.DOCENTRY
        AND a.SOURCE_REGION = b.SOURCE_REGION
       inner join prev_year_date s On a.Date = s.date
    WHERE b.channel in ('RETAIL CA', 'RETAIL USA') and b.location_name NOT LIKE '%Edmonton%'
        AND b.ordernumber NOT LIKE 'EXC%'
        AND isgiftcard <> 'Y'
        AND a.product_discount_percentage <> 100
    GROUP BY b.channel, b.location_name,b.ordernumber, a.DATE , ORDERDISCOUNT, RATE) t
    group by t.channel, t.location_NAME
),
cte3 as ( select * from cte1 union all select * from cte2),
ctetemp as (select * from cte3temp union all select * from cte4temp),

yoy AS (
    SELECT a.channel,
           a.location_name,
           a.Net_sales,
           a.daily_budget,
           CONCAT(ROUND((a.net_sales/a.daily_budget)*100),'%') as budget_daily_percentage,           
    CONCAT(COALESCE(ROUND(((a.Net_sales - NULLIF(b.net_sales,0)) / NULLIF(b.net_sales,0)) * 100),-100),'%') as "YOY %",
            b.net_sales as net_sales_prev,
    --CONCAT(COALESCE(ROUND(((a.net_sales - b.net_sales) / NULLIF(b.net_sales,0)) * 100),-100), '%') AS "YOY %"
    FROM cte3 a
    left JOIN ctetemp b ON a.channel = b.channel AND a.location_name = b.location_name
),
cte10 as ( select
channel, location_name , sum(Net_sales) as Net_sales_weekly, sum(daily_budget) as budget_weekly from( select
b.channel, b.location_name, sum(Price * Quantity * currency_rate) as Net_Sales,
    c.net_sales as daily_budget    
    FROM {{ref('REP_ORDERDETAIL')}} a
    INNER JOIN {{ref('REP_ORDER')}} b ON a.DOCENTRY = b.DOCENTRY
        AND a.SOURCE_REGION = b.SOURCE_REGION
        INNER JOIN weekly s On a.Date = s.date
left JOIN {{ref('BUDGET')}} c on b.channel = c.channel AND b.location_name = c.location_name AND
a.date = c.DATE
    WHERE b.channel IN ('ECOM CA', 'ECOM USA')
        AND b.ordernumber NOT LIKE 'EXC%'
        AND isgiftcard <> 'Y'
        AND a.product_discount_percentage <> 100
    GROUP BY b.channel, b.location_name,c.net_sales) group by channel, location_name),

cte20 AS ( select channel, location_name, sum(Net_sales) as Net_sales_weekly, sum(daily_budget) as budget_weekly
from (select t.channel, t.location_NAME, SUM(t.NET_SALES) as NET_SALES, 
    c.net_sales as daily_budget
from
    (SELECT b.channel,
           b.location_name,
           b.ordernumber,
           a.DATE,
           SUM(a.price * a.quantity*RATE) - orderdiscount*RATE  AS NET_SALES,
    FROM {{ref('REP_ORDERDETAIL')}} a
    INNER JOIN {{ref('REP_ORDER')}} b ON a.DOCENTRY = b.DOCENTRY
        AND a.SOURCE_REGION = b.SOURCE_REGION
       inner join weekly s On a.Date = s.date
    WHERE b.channel in ('RETAIL CA', 'RETAIL USA') and b.location_name NOT LIKE '%Edmonton%'
        AND b.ordernumber NOT LIKE 'EXC%'
        AND isgiftcard <> 'Y'
        AND a.product_discount_percentage <> 100
    GROUP BY b.channel, b.location_name,b.ordernumber, a.DATE , ORDERDISCOUNT, RATE) t
    left JOIN {{ref('BUDGET')}} c on t.channel = c.channel AND t.location_name = c.location_name AND
t.date = c.DATE
    group by t.channel, t.location_NAME, c.net_sales) group by channel, location_NAME
)
,
cte30 as (select * from cte10 union all select * from cte20),
cte40 as (select location_name, SUM(NET_SALES) as db from {{ref('BUDGET')}} where DATE IN (select date from weekly) group by location_name)

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
ROUND(a.Net_sales) as "Actual Sales Daily (Net) $", a.daily_budget as "Budget Daily", a.budget_daily_percentage as "Budget Daily %", a."YOY %" as "YOY Growth %", ROUND(a.net_sales_prev) as prev, ROUND(b.net_sales_weekly) as "Actual Sales WTD (Net) $", bb.db as "Budget WTD ($)",
CONCAT(ROUND((b.net_sales_weekly/bb.db)*100),'%') as "Budget WTD %"
from yoy a inner join
cte30 b on a.channel = b.channel and a.location_name = b.location_name
inner join cte40 bb on a.location_name = bb.location_name
order by channel1, channel2, channel3


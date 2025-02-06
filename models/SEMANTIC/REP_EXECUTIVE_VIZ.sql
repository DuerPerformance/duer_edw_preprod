WITH selected_date AS (
    SELECT *
    FROM {{ref('DIM_CALENDAR')}}
    WHERE --DATE = '2024-09-19' AND
        CALENDAR_TYPE = 'Retail Calendar'
),
prev_year_date AS (
    SELECT b.date_key_2 , b. Date as cur_date, a.Date as prev_date 
    FROM {{ref('DIM_CALENDAR')}} a
    INNER JOIN selected_date b ON a.DATE_OF_YEAR = b.DATE_OF_YEAR -- Explicitly referencing `a.date` and `b.date`
    WHERE  a.YEAR_SERIAL = b.YEAR_SERIAL - 1 and
         a.CALENDAR_TYPE = 'Retail Calendar' 
)

,
cte4 AS ( select channel, location_name, order_date,date_key_2, Sum(NET_SALES) as NET_SALES, SUM(NET_REVENUE)-SUM(RETURNS) as NET_REVENUE, SUM(NET_REVENUE) as Delivered_revenue, SUM(OPEN_SALES) as OPEN_SALES,
Sum(RETURNS) as RETURNS, Sum(DISCOUNT) as DISCOUNT , Sum(gross_sales) as gross_sales
from
    (
    SELECT s.date_key_2, b.channel,
           b.location_name,
           b.ordernumber,
           a.DATE AS order_date, -- Aliased the column to avoid ambiguity
           SUM(a.price * a.quantity * currency_rate_2) - ORDERDISCOUNT * currency_rate_2 AS NET_SALES,

           SUM(a.price * a.delivered_quantity ) * (1-(order_discount_percentage/100)) * currency_rate_2 as NET_REVENUE,

            SUM(a.price * a.open_quantity) * (1-(order_discount_percentage/100)) * currency_rate_2 as OPEN_SALES,
           
            SUM(A.REFUND  * currency_rate_2) AS RETURNS,
           SUM((A.PRICEBEFDI * A.QUANTITY - A.PRICE * A.QUANTITY) * currency_rate_2) + orderdiscount*currency_rate_2 
           AS DISCOUNT,
           SUM(A.PRICEBEFDI * A.QUANTITY * currency_rate_2) AS Gross_Sales
    FROM {{ref('REP_ORDERDETAIL')}} a
    INNER JOIN {{ref('REP_ORDER')}} b ON a.DOCENTRY = b.DOCENTRY
        AND a.SOURCE_REGION = b.SOURCE_REGION
        INNER JOIN prev_year_date s On a.Date = s.cur_date
    WHERE b.channel IN ('RETAIL CA', 'RETAIL USA','ECOM CA', 'ECOM USA')
        AND b.ordernumber NOT LIKE 'EXC%'
        --AND isgiftcard <> 'Y'
        AND a.product_discount_percentage <> 100
    GROUP BY b.channel, b.location_name, b.ordernumber, a.DATE, ORDERDISCOUNT, currency_rate_2, s.date_key_2, order_discount_percentage
    having (SUM(a.price * a.quantity * currency_rate_2) - ORDERDISCOUNT * currency_rate_2) > 0 AND count(*) <> count(case when isgiftcard = 'Y' then 1 end)
    )
    GROUP BY channel, location_name, order_date, date_key_2
)

,
cte5 AS ( select channel, location_name, order_date, ROUND(Sum(NET_SALES)) as NET_SALES,cur_date, SUM(NET_REVENUE)-SUM(RETURNS) as NET_REVENUE, SUM(NET_REVENUE)as Delivered_revenue,
Sum(gross_sales) as gross_sales
 from
    (SELECT b.channel,
           b.location_name,
           b.ordernumber,
           a.DATE AS order_date, -- Aliased the column to avoid ambiguity

           SUM(a.price * a.delivered_quantity ) * (1-(order_discount_percentage/100)) * currency_rate_2 as NET_REVENUE,

           SUM(A.REFUND  * currency_rate_2) AS RETURNS,
           SUM(a.price * a.quantity * currency_rate_2) - ORDERDISCOUNT * currency_rate_2 AS NET_SALES,
            s.cur_date,
            SUM(A.PRICEBEFDI * A.QUANTITY * currency_rate_2) AS Gross_Sales
    FROM {{ref('REP_ORDERDETAIL')}} a
    INNER JOIN {{ref('REP_ORDER')}} b ON a.DOCENTRY = b.DOCENTRY
        AND a.SOURCE_REGION = b.SOURCE_REGION
        INNER JOIN prev_year_date s On a.Date = s.prev_date
    WHERE b.channel IN ('RETAIL CA', 'RETAIL USA', 'ECOM CA', 'ECOM USA')
        AND b.ordernumber NOT LIKE 'EXC%'
        --AND isgiftcard <> 'Y'
        AND a.product_discount_percentage <> 100
    GROUP BY b.channel, b.location_name, b.ordernumber, a.DATE, ORDERDISCOUNT, currency_rate_2,cur_date,order_discount_percentage
    having SUM(a.price * a.quantity * currency_rate_2) - ORDERDISCOUNT * currency_rate_2 > 0 AND count(*) <> count(case when isgiftcard = 'Y' then 1 end)
    )
    GROUP BY channel, location_name, order_date,cur_date
)

,
cte6 AS (
    SELECT a.channel,
           a.location_name,
           a.order_date,
           a.NET_SALES,
           a.NET_REVENUE,
           a.RETURNS,
           a.DISCOUNT,
           a.Gross_sales,
           a.open_sales,
           a.delivered_revenue,
           --a.net_revenue,
           b.net_sales as prev_year,
           b.NET_REVENUE as prev_net_revenue,
           b.Delivered_revenue as prev_delivered_revenue,
           b.gross_sales as prev_gross_sales,
           a.date_key_2,
CONCAT(COALESCE(ROUND(((a.Net_sales - NULLIF(b.net_sales,0)) / NULLIF(b.net_sales,0)) * 100),-100),'%') as "YOY %"
    FROM cte4 a
    left JOIN cte5 b ON a.channel = b.channel AND a.location_name = b.location_name AND a.order_date = b.cur_date
),


cte7 as (select * from cte6),
cte8 as (select location_name, DATE, SUM(NET_SALES) as db from {{ref('BUDGET')}} where DATE IN (select order_date from cte7) group by location_name, DATE),




cte44 AS ( select date_key_2, WS, WS_LOC, loc_name, order_date, Sum(NET_SALES) as NET_SALES, SUM(NET_REVENUE) as NET_REVENUE, SUM(NET_REVENUE) as Delivered_revenue, SUM(OPEN_SALES) as open_sales,
 Sum(DISCOUNT) as DISCOUNT, SUM(gross_sales) as gross_sales
from
    (SELECT s.date_key_2, a.WS, a.ws_loc, a.loc_name,
           b.ordernumber,
           s.cur_date AS order_date, -- Aliased the column to avoid ambiguity
           SUM(a.price * a.quantity * currency_rate_2) - ORDERDISCOUNT * currency_rate_2 AS NET_SALES,

           SUM(a.price * a.delivered_quantity ) * (1-(order_discount_percentage/100)) * currency_rate_2 as NET_REVENUE,

           SUM(a.price * a.open_quantity) * (1-(order_discount_percentage/100)) * currency_rate_2 as OPEN_SALES,
           
           SUM((A.PRICEBEFDI * A.QUANTITY - A.PRICE * A.QUANTITY) * currency_rate_2) + orderdiscount*currency_rate_2 
           AS DISCOUNT,
           SUM(A.PRICEBEFDI * A.QUANTITY * currency_rate_2) AS gross_sales
    FROM {{ref('REP_ORDERDETAIL')}} a
    INNER JOIN {{ref('REP_ORDER')}} b ON a.DOCENTRY = b.DOCENTRY
        AND a.SOURCE_REGION = b.SOURCE_REGION
        INNER JOIN prev_year_date s On a.Date = s.cur_date
    WHERE a.WS IN ('WSCAD', 'WSEU', 'WSUS') and WS_LOC IS NOT NULL
        --AND b.ordernumber NOT LIKE 'EXC%'
        --AND isgiftcard <> 'Y'
        --AND a.product_discount_percentage <> 100
    GROUP BY a.WS, a.WS_loc, b.ordernumber, s.cur_date, ORDERDISCOUNT, currency_rate_2, a.loc_name, s.date_key_2,order_discount_percentage)
    GROUP BY WS, WS_loc, loc_name, order_date, date_key_2     having sum(Net_sales) > 0)


,
cte55 AS ( select date_key_2, WS, WS_LOC, loc_name, order_date, Sum(NET_SALES) as NET_SALES, SUM(NET_REVENUE) as NET_REVENUE, SUM(NET_REVENUE) as Delivered_revenue, sum(gross_sales) as gross_sales from
    (SELECT s.date_key_2, a.WS, a.ws_loc, a.loc_name,
           b.ordernumber,
           --a.DATE AS order_date, -- Aliased the column to avoid ambiguity
           SUM(a.price * a.quantity * currency_rate_2) - ORDERDISCOUNT * currency_rate_2 AS NET_SALES,
           SUM(a.price * a.delivered_quantity ) * (1-(order_discount_percentage/100)) * currency_rate_2 as NET_REVENUE,
            s.cur_date as order_date,
            SUM(A.PRICEBEFDI * A.QUANTITY * currency_rate_2) AS gross_sales
    FROM {{ref('REP_ORDERDETAIL')}} a
    INNER JOIN {{ref('REP_ORDER')}} b ON a.DOCENTRY = b.DOCENTRY
        AND a.SOURCE_REGION = b.SOURCE_REGION
        INNER JOIN prev_year_date s On a.Date = s.prev_date
    WHERE a.WS IN ('WSCAD', 'WSEU', 'WSUS') and WS_LOC IS NOT NULL
        --AND b.ordernumber NOT LIKE 'EXC%'
        --AND isgiftcard <> 'Y'
        --AND a.product_discount_percentage <> 100
    GROUP BY a.WS, a.WS_loc, b.ordernumber, order_date, ORDERDISCOUNT, currency_rate_2,s.date_key_2, a.loc_name, order_discount_percentage)
    GROUP BY WS, WS_loc,  loc_name , order_date, date_key_2    having sum(Net_sales) > 0),



    cte66 as (SELECT 'Wholesale' as channel0,
            case when a.WS = 'WSCAD' then 'Canada' when a.WS = 'WSUS' then 'United States' else 'Europe' End as channel1,
           a.WS_loc as channel2,
           a.loc_name as channel3,
           a.order_date,
           ROUND(a.NET_SALES) as "Actual Sales (Net) $",
           ROUND(a.DISCOUNT) as "Discount",
           ROUND((a.DISCOUNT/a.gross_sales)*100) as "% Discount",
           ROUND(a.gross_sales) as gross_sales,
           
           ROUND(a.net_revenue) as net_revenue,
           ROUND(a.delivered_revenue) as delivered_revenue,
           ROUND(a.open_sales) as open_sales,
           ROUND(b.net_sales) as prev_year,
           ROUND(b.net_revenue) as prev_net_revenue,
           ROUND(b.delivered_revenue) as prev_delivered_revenue,
           ROUND(b.gross_sales) as prev_gross_sales,
           a.date_key_2,
COALESCE(ROUND(((a.Net_sales - NULLIF(b.net_sales,0)) / NULLIF(b.net_sales,0)) * 100),-100) as "YOY %"
    FROM cte44 a
    left JOIN cte55 b ON a.WS = b.WS AND a.WS_loc = b.WS_loc AND a.order_date = b.order_date
    and a.loc_name = b.loc_name



        union
    
    select 'Wholesale' as channel0, 
    case when b.WS = 'WSCAD' then 'Canada' when a.WS = 'WSUS' then 'United States' else 'Europe' End as channel1,
    b.WS_loc as channel2,
    b.loc_name as channel3,
    b.order_date,
    0 as "Actual Sales (Net) $",
    0 as "Discount",
    0 as "% Discount",
    0 as gross_sales,
    0 as net_revenue,
    0 as delivered_revenue,
    0 as open_sales,
    ROUND(b.net_sales) as prev_year,
    ROUND(b.net_revenue) as prev_net_revenue,
    ROUND(b.delivered_revenue) as prev_delivered_revenue,
    ROUND(b.gross_sales) as prev_gross_sales,
    b.date_key_2,
    0 as "YOY %"
    FROM cte44 a
    right join cte55 b ON a.order_date = b.order_date and a.WS = b.WS AND a.WS_loc = b.WS_loc
    and a.loc_name = b.loc_name where a.WS is NULL and a.WS_loc is NULL and a.loc_name is NULL ),




    cte77 as (select CASE WHEN channel2 IS NOT NULL then channel1 else 'n/a' END as channel1,
    CASE WHEN channel2 IS NOT NULL then channel2 else 'Unknown' END as channel2, * exclude (channel1, channel2) from cte66
    --where order_date = '2024-09-19'
    ),






cte9 as (select case when a.CHANNEL like '%ECOM%' then 'Ecommerce' Else 'Retail' END as Channel1,
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
ROUND(a.Net_sales) as "Actual Sales (Net) $",a."YOY %" as "YOY %", b.db as "Budget (Net $)", CONCAT(ROUND((a.Net_sales/coalesce(NULLIF(b.db,0),-100))*100),'%') as "% Budget",
ROUND(a.RETURNS) as "Returns", 
CONCAT(ROUND((a.RETURNS/a.gross_sales)*100),'%') as "% Returns",
ROUND(a.DISCOUNT) as "Discount",
CONCAT(ROUND((a.DISCOUNT/a.gross_sales)*100),'%') as "% Discount",
ROUND(a.gross_sales) as gross_sales,
ROUND(a.NET_REVENUE) as NET_REVENUE,
ROUND(a.open_sales) as open_sales,
ROUND(a.delivered_revenue) as delivered_revenue,
ROUND(a.prev_year) as prev_year,
ROUND(a.prev_net_revenue) as prev_net_revenue,
ROUND(a.prev_delivered_revenue) as prev_delivered_revenue,
ROUND(a.prev_gross_sales) as prev_gross_sales,
a.order_date,
a.date_key_2
from cte7 a inner join
cte8 b on a.location_name = b.location_name and a.order_date = b.date
--where a.order_date = '2024-09-19'
order by channel1, channel2, channel3)

--select * from cte77

select channel1 as channel0,
    case when channel2 = 'CA' Then 'Canada' Else 'United States' end as channel1,
    channel2,
    channel3,
    order_date,
    gross_sales,
    "Actual Sales (Net) $" as Actual_Sales,
    open_sales,
    --"YOY %",
    "Budget (Net $)" as Budget,
    --"% Budget",
    "Returns" as Returns,
    --"% Returns",
    "Discount" as Discount,
    --"% Discount",
    net_revenue,
    delivered_revenue,
    prev_gross_sales,
    prev_year as prev_net_sales,
    prev_net_revenue,
    prev_delivered_revenue,
    date_key_2
    from cte9

    union all

    select channel0,
    channel1,
    channel2,
    channel3,
    order_date,
    gross_sales,
    "Actual Sales (Net) $" as Actual_Sales,
    open_sales,
    --"YOY %",
    null  as Budget,
    --null as "% Budget",
    null  as Returns,
    --null as "% Returns",
    "Discount" as Discount,
    --"% Discount",
    net_revenue,
    delivered_revenue,
    prev_gross_sales,
    prev_year as prev_net_sales,
    prev_net_revenue,
    prev_delivered_revenue,
    date_key_2
    from cte77
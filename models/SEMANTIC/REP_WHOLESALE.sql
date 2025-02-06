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
--select * from prev_year_date where cur_date = '2024-09-19'
,
cte4 AS ( select date_key_2, WS, WS_LOC, loc_name, order_date, Sum(NET_SALES) as NET_SALES,
 Sum(DISCOUNT) as DISCOUNT, SUM(TEMP) as TEMP
from
    (SELECT s.date_key_2, a.WS, a.ws_loc, a.loc_name,
           b.ordernumber,
           s.cur_date AS order_date, -- Aliased the column to avoid ambiguity
           SUM(a.price * a.quantity * rate) - ORDERDISCOUNT * rate AS NET_SALES,
           SUM((A.PRICEBEFDI * A.QUANTITY - A.PRICE * A.QUANTITY) * rate) + orderdiscount*rate 
           AS DISCOUNT,
           SUM(A.PRICEBEFDI * A.QUANTITY * rate) AS TEMP
    FROM {{ref('REP_ORDERDETAIL')}} a
    INNER JOIN {{ref('REP_ORDER')}} b ON a.DOCENTRY = b.DOCENTRY
        AND a.SOURCE_REGION = b.SOURCE_REGION
        INNER JOIN prev_year_date s On a.Date = s.cur_date
    WHERE a.WS IN ('WSCAD', 'WSEU', 'WSUS') and WS_LOC IS NOT NULL
        --AND b.ordernumber NOT LIKE 'EXC%'
        --AND isgiftcard <> 'Y'
        --AND a.product_discount_percentage <> 100
    GROUP BY a.WS, a.WS_loc, b.ordernumber, s.cur_date, ORDERDISCOUNT, rate, a.loc_name, s.date_key_2)
    GROUP BY WS, WS_loc, loc_name, order_date, date_key_2     having sum(Net_sales) > 0)


,
cte5 AS ( select date_key_2, WS, WS_LOC, loc_name, order_date, Sum(NET_SALES) as NET_SALES from
    (SELECT s.date_key_2, a.WS, a.ws_loc, a.loc_name,
           b.ordernumber,
           --a.DATE AS order_date, -- Aliased the column to avoid ambiguity
           SUM(a.price * a.quantity * rate) - ORDERDISCOUNT * rate AS NET_SALES,
            s.cur_date as order_date
    FROM {{ref('REP_ORDERDETAIL')}} a
    INNER JOIN {{ref('REP_ORDER')}} b ON a.DOCENTRY = b.DOCENTRY
        AND a.SOURCE_REGION = b.SOURCE_REGION
        INNER JOIN prev_year_date s On a.Date = s.prev_date
    WHERE a.WS IN ('WSCAD', 'WSEU', 'WSUS') and WS_LOC IS NOT NULL
        --AND b.ordernumber NOT LIKE 'EXC%'
        --AND isgiftcard <> 'Y'
        --AND a.product_discount_percentage <> 100
    GROUP BY a.WS, a.WS_loc, b.ordernumber, order_date, ORDERDISCOUNT, rate,s.date_key_2, a.loc_name)
    GROUP BY WS, WS_loc,  loc_name , order_date, date_key_2    having sum(Net_sales) > 0),



    cte6 as (SELECT 'Wholesale' as channel0,
            case when a.WS = 'WSCAD' then 'Canada' when a.WS = 'WSUS' then 'United States' else 'Europe' End as channel1,
           a.WS_loc as channel2,
           a.loc_name as channel3,
           a.order_date,
           ROUND(a.NET_SALES) as "Actual Sales (Net) $",
           ROUND(a.DISCOUNT) as "Discount",
           ROUND((a.DISCOUNT/a.TEMP)*100) as "% Discount",
           ROUND(a.TEMP) as temp,
           ROUND(b.net_sales) as prev_year,
           a.date_key_2,
COALESCE(ROUND(((a.Net_sales - NULLIF(b.net_sales,0)) / NULLIF(b.net_sales,0)) * 100),-100) as "YOY %"
    FROM cte4 a
    left JOIN cte5 b ON a.WS = b.WS AND a.WS_loc = b.WS_loc AND a.order_date = b.order_date
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
    0 as temp,
    ROUND(b.net_sales) as prev_year,
    b.date_key_2,
    0 as "YOY %"
    FROM cte4 a
    right join cte5 b ON a.order_date = b.order_date and a.WS = b.WS AND a.WS_loc = b.WS_loc
    and a.loc_name = b.loc_name where a.WS is NULL and a.WS_loc is NULL and a.loc_name is NULL )




    select CASE WHEN channel2 IS NOT NULL then channel1 else 'n/a' END as channel1,
    CASE WHEN channel2 IS NOT NULL then channel2 else 'Unknown' END as channel2, * exclude (channel1, channel2) from cte6
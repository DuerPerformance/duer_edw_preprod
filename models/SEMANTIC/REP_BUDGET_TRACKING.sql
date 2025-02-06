with cal as (select DATE_KEY_2, DATE, YEAR_SERIAL from {{ref('DIM_CALENDAR')}} WHERE CALENDAR_TYPE = 'Retail Calendar' AND YEAR_SERIAL IN ('2024','2025') ORDER BY DATE),

cte2 AS ( SELECT date_key_2, date, Sum(NET_SALES) as RETAIL_SALES
from
    (SELECT s.date_key_2, b.channel,
           b.location_name,
           b.ordernumber,
           a.DATE AS date, -- Aliased the column to avoid ambiguity
           SUM(a.price * a.quantity * rate) - ORDERDISCOUNT * rate AS NET_SALES,
    FROM {{ref('REP_ORDERDETAIL')}} a
    INNER JOIN {{ref('REP_ORDER')}} b ON a.DOCENTRY = b.DOCENTRY
        AND a.SOURCE_REGION = b.SOURCE_REGION
        INNER JOIN CAL s On a.Date = s.date
    WHERE b.channel IN ('RETAIL CA', 'RETAIL USA','ECOM CA', 'ECOM USA')
        AND b.ordernumber NOT LIKE 'EXC%'
        AND isgiftcard <> 'Y'
        AND a.product_discount_percentage <> 100
    GROUP BY b.channel, b.location_name, b.ordernumber, a.DATE, ORDERDISCOUNT, rate, s.date_key_2)
    GROUP BY date, date_key_2
),

cte3 as (select a.DATE,a.year_serial, SUM(Net_sales) as budget from {{ref('BUDGET')}} a inner join cal c on a.DATE = c.date group by a.DATE, a.year_serial order by a.DATE),

cte4 as (select a.DATE,c.YEAR_SERIAL, a.RETAIL_SALES as Net_sales, c.budget from cte2 a inner join cte3 c on a.DATE = c.DATE order by a.DATE)

select date, year_serial, round(sum(NET_SALES) OVER(PARTITION by Year_SErial order by date)) as Commulative_Sales,round(sum(budget) OVER(PARTITION by Year_SErial order by date)) as Commulative_budget from cte4 



WITH BUDGET AS (SELECT *,
LEFT(PERIOD,4) as Fiscal_Year,
RIGHT(PERIOD,2) as Fiscal_Month,
CASE WHEN SUBSIDIARY = 'PPACANADA' THEN 'CA'
ELSE 'US'
END AS SOURCE_REGION 
FROM {{ref('Budget_FY24_FDM')}})
SELECT cc.date as transaction_post_date,a.*,b."segment_0",b."acctcode",b."acctname" from 
BUDGET a 
INNER JOIN {{ref('SAP_OACT')}} b
on a.account_num::TEXT = b."segment_0"
and a.SOURCE_REGION = b.source_region
INNER JOIN {{ref('DIM_CALENDAR')}} cc
on a.period = cc.month_serial
where cc.calendar_type = 'DUER FISCAL CALENDAR'
with 
l0 as
(
select *, 
UPPER
(case
when  "acctname" ='Revenue' then   'Revenues'
when  "acctname" ='Other Revenue and Expenses' then 'Other Revenues and Expenses'
when  "acctname" ='CGS' then 'COST OF SALES'
ELSE "acctname" end) as new_acctname  
 
 from {{ ref('SAP_OACT') }}  where "levels" = 1
),

l1 as 
(
select *,UPPER("acctname")  as new_acctname  
from {{ ref('SAP_OACT') }}  where "levels" = 2
),

l2 as 
(
select * ,
UPPER(
case 
when  "acctname" ='ACCOUNTS RECEIVABLE' then   'ACCOUNTS RECEIVABLES'
when  "acctname" ='LOANS ETC..' then   'Loans' 
when  "acctname" ='OTHER COGS & VARIANCES' then   'OTHER COGS and VARIANCES' 
when  "acctname" ='Contractor Expense' then   'CONTRACTOR EXPENSES' 
else "acctname"
end
) as new_acctname  
from {{ ref('SAP_OACT') }}  where "levels" = 3

),
l3 as 
(
select *,UPPER("acctname")  as new_acctname 
from {{ ref('SAP_OACT') }}  where "levels" = 4
),
l4 as 
(
select *,UPPER("acctname")  as new_acctname 
from {{ ref('SAP_OACT') }}  where "levels" = 5
),

Joining AS
(

select  DISTINCT
l3.source_region as source_region,


concat(concat(l0."acctcode",concat('(',l0.new_acctname)),')') as AccountL0,
concat(concat(l1."acctcode",concat('(',l1.new_acctname)),')') AS AccountL1,
l1."acctcode" AS L1AccountCode,
l1.new_acctname as L1AccountName,

concat(concat(l2."acctcode",concat('(',l2.new_acctname)),')') AS AccountL2,
concat(concat(l3."acctcode",concat('(',l3.new_acctname)),')') AS AccountL3,
concat(concat(l4."acctcode",concat('(',l4.new_acctname)),')') AS AccountL4,

l3."acctcode" as AccountID,
l3.new_acctname as AccountName,
concat(concat(l3."acctcode",concat('(',l3.new_acctname)),')')  as AccountingDescription,
CASE WHEN l3."segment_0" < '40000' THEN 'Balance'
WHEN l3."segment_0" >= '40000' THEN 'Income'
END AS Statement,
l3."currtotal" as currtotal,
l0.new_acctname as ReportingGroup,
l2.new_acctname as ReportingSubgroup

from  l0 
left join  l1 
on (l0."acctcode"=l1."fathernum") 
left join  l2 
on (l1."acctcode"=l2."fathernum") 
left join  l3
on (l2."acctcode"=l3."fathernum") 
left join  l4
on (l3."acctcode"=l4."fathernum") 
-- where l3."acctcode"='_SYS00000000317'
),

joined as
(
select  DISTINCT
AccountL0 ,
AccountL1 ,
AccountL2,
AccountL3,
AccountL4,


coalesce(AccountID ,L1AccountCode) as AccountID ,
coalesce(AccountName,L1AccountName ) as AccountName ,
coalesce(AccountingDescription,AccountL1) as AccountingDescription,

currtotal,
Statement,
ReportingGroup,
ReportingSubgroup,
Source_Region

from Joining
-- ORDER BY AccountL0,AccountL1,AccountL2,AccountL3,AccountL4
)

select 

    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS ACCOUNT_KEY,
    A.*,
    current_timestamp() AS InsertDate,
    NULL AS UpdateDate,
    'IO' AS DML

from joined A
WHERE AccountID IS NOT NULL


 

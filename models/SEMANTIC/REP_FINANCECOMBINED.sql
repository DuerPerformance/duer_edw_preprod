
WITH first as (      
SELECT DISTINCT a.*, b.amount as total_budget
FROM {{ref('REP_FINANCE')}} a 
LEFT JOIN {{ref('FDM_BUDGET')}} b
on a.accountid = b."acctcode" 
and a.costcenter_id = b.cost_center  
and a.source_region = b.source_region 
and  a.transaction_post_date = b.transaction_post_date )
, 
SECOND AS (
SELECT DISTINCT a.*,b.transaction_id,b.costcenter_id as current_cost_center
FROM {{ref('FDM_BUDGET')}} a
LEFT  JOIN {{ref('REP_FINANCE')}} b 
on a."acctcode" = b.accountid
and a.cost_center = b.costcenter_id 
and a.source_region =b.source_region
and a.transaction_post_date = b.transaction_post_date

),THIRD AS (
SELECT DISTINCT
 a.transaction_id
, a.transactiontype_id
//, a.total
, a.transaction_create_date 
, a.transaction_post_date 
, a.transaction_due_date 
, a.transaction_tax_date  
, a.exchange_rate
, a.LINE_NUMBER
, a.MEMO
, a.local_deb
, a.local_cred
, a.fcdeb
, a.fccred
, a.SYSTEM_DEB
, a.SYSTEM_CRED
, a.Currency_ID
, a.contract
, a.VAT_RATE 
, a.VAT_AMOUNT 
, a.GROSS_VALUE 
, a.TOTAL_VAT 
, a.post_status
, a.sales_delivery_document_date
, a.days_difference
, a.post_date_mismatch
, a.ACCOUNTL0
, a.accountl1
, a.accountl2
, a.accountl3
, a.accountl4
, a.accountid 
, a.account_name
, a.accounting_description
, a.statement
, a.currtotal
, a.reporting_group
, a.reporting_subgroup
, a.costcenter_id
, a.costcenter_name
, a.costcenter_type
, a.subsidiary_id
, a.SYSTEM_CRED - a.SYSTEM_DEB as act_ledger_amount
, a.total_budget
, a.source_region 
, a.Amount_Consolidated
, a.Amount_Subsidiary
, a.Amount_Transaction
FROM FIRST a 

UNION 


SELECT DISTINCT 
 NULL as transaction_id
, NULL as transactiontype_id
//, 0 as total
, NULL as transaction_create_date 
, b.transaction_post_date 
, NULL as transaction_due_date 
, NULL as transaction_tax_date  
, NULL as exchange_rate
, NULL as LINE_NUMBER
, NULL as MEMO
, 0 as local_deb
, 0 as local_cred
, 0 as fcdeb
, 0 as fccred
, 0 as SYSTEM_DEB
, 0 as SYSTEM_CRED
, NULL as Currency_ID
, NULL as contract
, NULL as VAT_RATE 
, 0 as VAT_AMOUNT 
, 0 as GROSS_VALUE 
, 0 as TOTAL_VAT 
, NULL as post_status
, NULL as sales_delivery_document_date
, 0 as days_difference
, NULL as post_date_mismatch
, NULL as ACCOUNTL0
, NULL as accountl1
, NULL as accountl2
, NULL as accountl3
, NULL as accountl4
, b."acctcode" as accountid 
, b."acctname" as account_name
, NULL as accounting_description
, NULL as statement
, 0 as currtotal
, NULL as reporting_group
, NULL as reporting_subgroup
, b."COST_CENTER" as costcenter_id
, NULL as costcenter_name
, NULL as costcenter_type
, CASE WHEN b.source_region ='CA' THEN 'PPACA'
  ELSE 'PPAUS'
  END AS subsidiary_id
, 0 as  act_ledger_amount
, b.amount as total_budget
, b.source_region 
, 0 as Amount_Consolidated
, 0 as Amount_Subsidiary
, 0 as Amount_Transaction

FROM second b
where b.transaction_id is null)
SELECT * FROM THIRD
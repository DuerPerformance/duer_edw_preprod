
WITH TEMP AS (

SELECT 
 a.transaction_id
, a.transactiontype_id
, a.total
, a.transaction_create_date 
, a.transaction_post_date 
, a.transaction_due_date 
, a.transaction_tax_date  
, b.LINE_NUMBER
, b.MEMO
, b.local_deb
,b.local_cred
, b.fcdeb
, b.fccred
,b.fcurrency as Currency_ID
,b.system_cred
,b.system_deb
, b.contract
, b.VAT_RATE 
, b.VAT_AMOUNT 
, b.GROSS_VALUE 
, b.TOTAL_VAT 
, b.post_status
, b.sales_delivery_document_date
, b.days_difference
, b.post_date_mismatch
, b.ACCOUNTL0
, b.accountl1
, b.accountl2
, b.accountl3
, b.accountl4
, b.accountid 
, b.account_name
, b.accounting_description
, b.statement
, b.currtotal
, b.reporting_group
, b.reporting_subgroup
, b.costcenter_id
, b.costcenter_name
, b.costcenter_type
, b.subsidiary_id
, b.system_cred - b.system_deb as act_ledger_amount
, b.source_region 
, CASE WHEN SYSTEM_CRED <> '0.000000' THEN  SYSTEM_CRED 
       WHEN SYSTEM_DEB <> '0.000000' THEN SYSTEM_DEB 
       ELSE '0.000000'
       END AS Amount_Consolidated
, CASE WHEN LOCAL_CRED <> '0.000000' THEN  LOCAL_CRED 
       WHEN LOCAL_DEB <> '0.000000' THEN LOCAL_DEB
       ELSE '0.000000'
       END AS Amount_Subsidiary

, CASE WHEN fccred <> '0.000000' THEN  fccred 
       WHEN fcdeb <> '0.000000' THEN fcdeb
       ELSE '0.000000'
       END AS Amount_Transaction
,c."rate" as exchange_rate
  
//SELECT COUNT(1)       
from {{ ref('REP_TRANSACTIONHEADER') }}  a
left join {{ ref('REP_TRANSACTIONLINE') }} b 
on a.transaction_id = b.transaction_id
LEFT JOIN {{ ref('SAP_ORTT_FDM') }} c
On a.transaction_post_date = c."ratedate"
 and b.fcurrency = c."currency"
 and b.source_region = c.source_region

)

SELECT * FROM TEMP
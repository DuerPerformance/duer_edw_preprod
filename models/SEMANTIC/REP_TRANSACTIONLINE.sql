
WITH TEMP AS (SELECT 
 b.transaction_id
, a.exchange_rate
, a.LINE_NUMBER
, a.MEMO
, a.local_deb
, a.local_cred
, a.fcdeb
, a.fccred
, a.fcurrency
, a.system_cred
,a.system_deb
,a.contract
, a.VAT_RATE 
, a.VAT_AMOUNT 
, a.GROSS_VALUE 
, a.TOTAL_VAT 

, b.transactiontype_id
, b.poststatus as post_status
, b.SALESDELIVERYDOCUMENT_DATE as sales_delivery_document_date
, b.days_difference
, b.postdatemismatch as post_date_mismatch
,acc.ACCOUNTL0
,acc.accountl1
,acc.accountl2
, acc.accountl3
, acc.accountl4
, acc.accountid 
, acc.accountname as account_name
, acc.accountingdescription as accounting_description
, acc.statement
,acc.currtotal
, acc.reportinggroup as reporting_group
, acc.reportingsubgroup as reporting_subgroup
,cc.costcenter_id
,cc.costcenter_name
,cc.costcenter_type
,sub.subsidiary_id
, b.source_region
-- FROM EDW_PREPROD.EDW.FACT_TRANSACTIONLIN a --7794348
FROM {{ref('FACT_TRANSACTIONLINE')}} a 
LEFT JOIN 
    {{ref('DIM_TRANSACTION')}} b ON a.transaction_key = b.transaction_key

LEFT JOIN 
    {{ref('DIM_ACCOUNT')}} acc on a.account_key=acc.account_key
LEFT JOIN 
   {{ref('DIM_COSTCENTER')}} cc on a.costcenter_key = cc.costcenter_key
INNER JOIN 
   {{ref('DIM_SUBSIDIARY')}} sub on a.subsidiary_key = sub.subsidiary_key

   
   )

SELECT 
 transaction_id
, exchange_rate
, LINE_NUMBER
, MEMO
, local_deb
, local_cred
, fcdeb 
, fccred
,system_Cred
, system_deb
, fcurrency
, contract
, VAT_RATE 
, VAT_AMOUNT 
, GROSS_VALUE 
, TOTAL_VAT 
, transactiontype_id
, post_status
, sales_delivery_document_date
, days_difference
, post_date_mismatch
, ACCOUNTL0
, accountl1
, accountl2
, accountl3
, accountl4
, accountid 
, account_name
, accounting_description
, statement
, currtotal
, reporting_group
, reporting_subgroup
, costcenter_id
, costcenter_name
, costcenter_type
, subsidiary_id
, source_region
FROM TEMP

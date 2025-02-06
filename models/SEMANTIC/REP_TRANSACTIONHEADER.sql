
WITH TEMP AS (SELECT 
    b.transaction_id,
    b.transactiontype_id,
    a.total,
    b.memo,
    b.poststatus as post_status,
    b.SALESDELIVERYDOCUMENT_DATE as sales_delivery_document_date,
    b.days_difference,
    b.postdatemismatch as post_date_mismatch,
    create_cl.date as transaction_create_date,
    post_cl.date AS transaction_post_date,
    due_cl.date AS transaction_due_date,
    tax_cl.date AS transaction_tax_date,
    b.source_region
FROM 
    {{ ref('FACT_TRANSACTIONHEADER') }} a
LEFT JOIN 
    {{ ref('DIM_TRANSACTION') }} b ON a.transaction_key = b.transaction_key
LEFT JOIN 
    {{ref('DIM_CALENDAR')}} create_cl ON a.transaction_create_date_key = create_cl.date_key_2
LEFT JOIN 
    {{ref('DIM_CALENDAR')}} post_cl ON a.transaction_post_date_key = post_cl.date_key_2
LEFT JOIN 
    {{ref('DIM_CALENDAR')}} due_cl ON a.transaction_due_date_key = due_cl.date_key_2
LEFT JOIN 
    {{ref('DIM_CALENDAR')}} tax_cl ON a.transaction_tax_date_key = tax_cl.date_key_2)

SELECT 
transaction_id
, transactiontype_id
, total
, memo 
, post_status
, sales_delivery_document_date
, days_difference 
, post_date_mismatch 
, transaction_create_date 
, transaction_post_date 
, transaction_due_date 
, transaction_tax_date 
, source_region 

FROM TEMP
{{ config(
    unique_key='Transaction_ID'
) }}

WITH transaction_table AS (
    SELECT * 
    FROM {{ ref('SAP_OJDT') }}
),

odln_table AS (
    SELECT *
    FROM {{ ref('SAP_ODLN') }}
),


source_data AS (
        Select  
            CASE WHEN a.SOURCE_REGION = 'CA'THEN CONCAT('CA_', a."transid")
            ELSE CONCAT('US_', a."transid")
            END as Transaction_ID,
            a."createdate" as TransactionCreateDate_Key,
            a."taxdate" as TransactionTaxDate_Key,
            a."duedate" as TransactionDueDate_Key,
            a."memo" as Memo,
            a."transcode" as TRANSACTIONTYPE_ID,
            a."btfstatus" as PostStatus,
            a."refdate" as PostDate,
            b."createdate" as SalesDeliveryDocument_Date,
            DATEDIFF(day, SalesDeliveryDocument_Date, TransactionCreateDate_Key) as Days_Difference,
            case when Days_Difference = '0' then 'No_Differnece_Found'
            else 'Differnece_Found' end as PostDateMismatch,
            a.SOURCE,a.SOURCE_REGION, current_timestamp() AS InsertDate, NULL AS UpdateDate, 'IO' AS DML,
            ROW_NUMBER() OVER (PARTITION BY Transaction_ID ORDER BY a."createdate") AS row_num
        FROM transaction_table a
        left join odln_table b 
        on TO_CHAR(a."ref1") = TO_CHAR(b."docnum")
    
)

SELECT
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS Transaction_KEY,
    Transaction_ID, Memo, TRANSACTIONTYPE_ID,
    --TransactionCreateDate_Key,TransactionTaxDate_Key, TransactionDueDate_Key,
    PostStatus, PostDate, SalesDeliveryDocument_Date, Days_Difference, PostDateMismatch,
    SOURCE,SOURCE_REGION,InsertDate,UpdateDate,DML
FROM source_data
WHERE row_num = 1
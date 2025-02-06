WITH temp AS (
SELECT *, ROW_NUMBER() OVER(PARTITION BY "prccode" ORDER BY LEN("prccode"),LEN("prcname"),LEN("cctypecode") DESC ) AS RN
FROM {{ ref('SAP_OPRC') }}
)
SELECT
row_number() over(order by temp."prccode") as CostCenter_KEY
,"prccode" as CostCenter_ID
,"prcname" as CostCenter_Name 
,"cctypecode" as CostCenter_Type 
,"SOURCE_REGION"
,"INSERT_DATE"
,"UPDATE_DATE"
,"DML"
FROM temp WHERE RN= 1
WITH source_data AS (
    SELECT distinct
P.PRODUCT_KEY ,PD.DATE_KEY as InventoryPurchaseDate_Key, 'null' as InventoryExpireDate_Key,SUM(I."onhand") AS Quantity
,W.WAREHOUSE_KEY ,P.ProductCategory_Key as ProductCategory_Key,UD.DATE_KEY as InventoryUpdateDate_Key,'null' AS InventoryValue,
I.source as SOURCE,I.source_region as SOURCE_REGION, current_timestamp() AS INSERTDATE,'IO' AS DML,'null' AS UPDATEDATE
FROM {{ref('SAP_OITM')}} I 
INNER JOIN {{ref('DIM_PRODUCT')}} P ON I."itemcode" = P.PRODUCT_ID
INNER JOIN {{ref('DIM_WAREHOUSE')}} W ON I."dfltwh" = W.WAREHOUSE_ID
INNER JOIN {{ref('DIM_CALENDAR')}} PD ON cast(I."lastpurdat" as DATE) = PD.DATE
INNER JOIN {{ref('DIM_CALENDAR')}} UD ON cast(I."updatedate" as DATE) = UD.DATE
GROUP BY P.PRODUCT_KEY ,PD.DATE_KEY,W.WAREHOUSE_KEY,P.ProductCategory_Key,UD.DATE_KEY,I.source,I.source_region
)
SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS INVENTORY_KEY,* FROM source_data
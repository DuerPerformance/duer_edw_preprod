WITH Source_Data AS (

    SELECT DISTINCT 
    
        W."whscode"               AS Warehouse_ID,
        W."whsname"               AS Warehouse_Name,
        --LCO.CountryKey            AS Country_Key,
        --LS.StateKey               AS State_Key,
        --LCI.CityKey               AS City_Key,
        W."street"                AS Street,
        W."zipcode"               AS Warehouse_ZipCode,
        DECODE(
            W."inactive", 
            'N', 'Y', 
            'Y', 'N', 
            NULL, NULL
        ) AS IsActive,
        W.SOURCE as SOURCE,
        W.SOURCE_REGION as SOURCE_REGION,
		w."u_dd_whsparent" as ParentWarehouse_Key,
        W."u_v33p_shopifylocationid" as shopifylocationid,
        W."street" as WH_Street,
        W."zipcode" as WH_postal,
        W."country" as WH_country,
        W."state" as WH_State,
        W."city" as WH_City

    FROM {{ref('SAP_OWHS')}}  W


    --INNER JOIN EDW_PREPROD.edw.lkp_country LCO
        --ON W."country" = LCO.Country

    --INNER JOIN EDW_PREPROD.edw.lkp_state LS
        --ON W."state" = LS.State
        
   -- INNER JOIN EDW_PREPROD.edw.lkp_city LCI
        --ON W."city" = LCI.City


)

, Dim_WareHouse AS (


    SELECT
        row_number() over(order by Warehouse_ID ) as Warehouse_Key,
		
        Warehouse_ID,
        Warehouse_Name,
        --Country_Key,
        --State_Key,
        --City_Key,
        WH_Street,
        WH_postal,
        WH_City,
        WH_State,
        WH_country,
        --Street,
        --Warehouse_ZipCode,
        IsActive,
        ParentWarehouse_Key,
        SOURCE,
        SOURCE_REGION,
        CURRENT_TIMESTAMP() AS InsertDate,
        NULL AS UpdateDate,
        'IO' AS DML
		
    FROM Source_Data
)

SELECT * FROM Dim_WareHouse

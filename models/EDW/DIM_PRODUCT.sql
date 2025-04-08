WITH items as (
    
    select *  FROM {{ ref('SAP_OITM') }} 

), model as (

    select * FROM {{ ref('SAP_ARGNS_MODEL') }} 

), modelgrp as (

    select * FROM {{ ref('SAP_ARGNS_MODELGRP') }} 

), div as (

    select * FROM {{ ref('SAP_ARGNS_DIV') }} 

), prodline as (

    select * FROM {{ ref('SAP_ARGNS_PRODLINE') }} 

), color as (

   select * FROM {{ ref('SAP_ARGNS_COLOR') }} 
    
), fabric as (

select * FROM {{ ref('SAP_ARGNS_COLLECTION') }}

), itemgroups as(

select * FROM {{ ref('SAP_OITB') }} 

), scale as(

select *  FROM {{ ref('SAP_ARGNS_SCALE') }} 

),

brand_detail as(

select *  FROM {{ ref('SAP_ARGNS_BRAND') }} 

),

joined as (

select 

items."itemcode" as Product_ID, --(itemcode)
items."itemname" as ProductName,--(itemname)
items."u_v33_ed" AS ProductDESCRIPTION,
items."itemtype" as ProductType,
brand_detail."name" AS Brand, --(u_brand)
div."name" as Gender,
prodline."name" as Division,
fabric."u_desc" AS Fabric,
modelgrp."name" as Fit,
model."u_modcode" as Model_Code,
model."u_modcode" as Model_Name,
--concat(concat(concat(model."u_modcode",'('),model."u_moddesc"),')' ) as Model_Description,
model."u_moddesc" as Model_Description,
items."u_argns_var" as Size_Variant,
concat(model."u_moddesc", '-', color."u_coldesc") as Style,
Concat(Concat(Concat(concat(model."u_modcode", '-', color."u_colcode"),' ('),concat(model."u_modcode", '-', color."u_colcode")),')')  as Style_Description,

concat(model."u_modcode", '-', color."u_colcode") as Style_Name,
items."u_argns_ca" as Channel_Assortment,
items."u_argns_fs" as First_Season,
items."u_argns_ns" as Next_Season,
items."u_argns_season" as Season,

case 
    	when regexp_instr(Season, 'Fall', 1, 1, 0, 'i') and regexp_instr(First_Season, 'Spring', 1, 1, 0, 'i') then 'ALL SEASON'
        when regexp_instr(Season, 'Spring', 1, 1, 0, 'i') and regexp_instr(First_Season, 'Fall', 1, 1, 0, 'i') then 'ALL SEASON'
        when regexp_instr(Season, 'Fall', 1, 1, 0, 'i') and regexp_instr(Next_Season, 'Fall', 1, 1, 0, 'i') then 'SEASONAL'
        when regexp_instr(Season, 'Spring', 1, 1, 0, 'i') and regexp_instr(Next_Season, 'Spring', 1, 1, 0, 'i') then 'SEASONAL'
        when Next_Season is null then 'SEASONAL'
        else 'ALL SEASON'
	end as SEASONAL,

    items."u_argns_pl" as Product_Lifecycle,

color."u_coldesc" as Color,

product_cat.ProductCategory_key AS ProductCategory_key,--not in sales model
items."u_argns_rt" AS Replenishment_Type,
items."u_argns_act" AS ACTIVE,
items."u_v33_ed" AS DESCRIPTION,
items."prchseitem" AS ITEM_PURCHASE,
items."sellitem" AS ITEM_SALES,
items."u_v33_mid" AS MANUFACTURER_ID_NUMBER,
items."u_argns_appgrp" AS SEGMENTATION,
items."u_argns_size" AS SIZE_DESCRIPTION,
items."u_argns_size" as Size,

case
        when SIZE = '80' then 'XXS'
        when SIZE = '81' then 'XS'
        when SIZE = '82' then 'S'
        when SIZE = '83' then 'M'
        when SIZE = '84' then 'L'
        when SIZE = '85' then 'XL'
        when SIZE = '86' then 'XXL'
        when SIZE = '99' then 'XX'
        else to_char(SIZE)
    end as SIZE_DESC,
	
    case SIZE_DESC
	when 'XXS' then 80
    when 'XS' then 81
    when 'S' then 82
    when 'M' then 83
    when 'L' then 84
    when 'XL' then 85
    when 'XXL' then 86
    when 'One' then 99
    when 'XX' then 99
    else 0  --SIZE_DESC
end as SIZE_VALUE,

items."u_argns_dtc_in" AS SIZE_DTC_INSEAM_RANGE,

    CASE 
    	WHEN items."u_argns_var"='XX' then 99
        WHEN items."u_argns_var" is NULL then 0
        ELSE  0  --items."u_argns_var" 
    END AS INSEAM_VALUE,

    case  SIZE_DTC_INSEAM_RANGE
    	when 'XX' then 99
        else right( SIZE_DTC_INSEAM_RANGE, 2)
    end as SIZE_DTC_INSEAM_MAX,
    case  SIZE_DTC_INSEAM_RANGE
    	when 'XX' then 99
        else left( SIZE_DTC_INSEAM_RANGE, 2)
    end as SIZE_DTC_INSEAM_MIN,


items."u_argns_dtc_sr" AS SIZE_DTC_RANGE,

SPLIT_PART(items."u_argns_dtc_sr", '-', 2) AS SIZE_DTC_MAX,
SPLIT_PART(items."u_argns_dtc_sr", '-', 1) AS SIZE_DTC_MIN,


case REPLENISHMENT_TYPE
    	when 'NOS - Never Out of Stock' then 'NOS'
        when 'Core' then 'COR'
        when 'SMU - Special Make Up' then 'SMU'
        when 'Cancelled - DO NOT USE' then 'Cancelled'
        when 'Cancelled - Not Adopted' then 'Cancelled'
        when 'Seasonal Energy' then
            concat('EG',substr(SEASON,3, 2),substr(SEASON, 6, 1)) --season_now for season
        when 'Seasonal' then
        	case 
            	when NEXT_SEASON is null then
                    concat(
                    case 
                    	when substr(SEASON, 6, 1) = 'F' then 'FW'
                    	else 'SS'
                    end, substr(SEASON, 3,2))
                when (substr(SEASON, 6, 1) <> substr(NEXT_SEASON, 6, 1)) then
                    concat(
                    case 
                    	when substr(SEASON, 6, 1) = 'F' then 'FW'
                    	else 'SS'
                    end, 
                    substr(SEASON, 3,2), 
                    '-',
                    case 
                    	when substr(NEXT_SEASON, 6, 1) = 'F' then 'FW'
                    	else 'SS'
                    end,
                    substr(NEXT_SEASON, 3,2)
                    )
                 when (substr(SEASON, 6, 1) = substr(NEXT_SEASON, 6, 1)) then
        			concat(
                    case 
                    	when substr(SEASON, 6, 1) = 'F' then 'FW'
                    	else 'SS'
                    end, substr(SEASON, 3,2))
            end
        when 'Discontinued' then
        	concat(
                    case 
                    	when substr(SEASON, 6, 1) = 'F' then 'FW'
                    	else 'SS'
                    end, substr(SEASON, 3,2), ' Discont')
        else 'N/A'
    end as REPLEN_STATUS,

items."u_argns_ws_in" AS SIZE_WHOLESALE_INSEAM_RANGE,
items."u_argns_ws_sr" AS SIZE_WHOLESALE_RANGE,

CASE 
        WHEN SIZE_DTC_RANGE IS NULL THEN 0 
        ELSE 
            CASE 
                WHEN SIZE_VALUE < TRY_TO_NUMBER(SIZE_DTC_MIN)
                  OR SIZE_VALUE > TRY_TO_NUMBER(SIZE_DTC_MAX)
                  OR INSEAM_VALUE < SIZE_DTC_INSEAM_MIN
                  OR INSEAM_VALUE >  SIZE_DTC_INSEAM_MAX
            
                THEN 0
                ELSE 1
            END
    END AS ACTIVE_DTC,

case when GENDER = 'Mens' then
    case when (SIZE_VALUE < 30 or SIZE_VALUE > 36) and SIZE_VALUE<= 40 then 'FRIN'
         when SIZE_VALUE = 80 or SIZE_VALUE = 81 or SIZE_VALUE > 84 then 'FRIN'
         else 'CORE'
    end
    else -- t0.""GENDER"" = 'women'
    case when (SIZE_VALUE< 25 or SIZE_VALUE > 32) and SIZE_VALUE <= 40 then 'FRIN'
         when SIZE_VALUE = 80 or SIZE_VALUE > 84 then 'FRIN'
        else 'CORE'
    end
end as SIZE_TYPE,


items."u_argns_year" AS SKU_YEAR,
items."codebars" AS UPC,
items."invntitem" AS ITEM_INVENTORY,

color."u_colcode" as Color_Code,

items."sheight1" as DIM_HEIGHT,
items."slength1" as DIM_LENGTH,
items."svolume" as DIM_VOLUME,
items."sweight1" as DIM_WEIGHT,
items."swidth1" as DIM_WIDTH,
items."lstevlpric" as LAST_PRICE,
itemgroups."itmsgrpnam" as ITEM_GROUP,
scale."u_scldesc" as WAIST_SIZE_RANGE,
items."createdate" as createdate,
ROW_NUMBER() OVER (PARTITION BY  items."itemcode" order by IFNULL(items."lstevldate",'1999-12-31') desc) LATEST_FLAG

        
    from items
    left join model
        on items."u_argns_mod" = model."u_modcode"
    left join modelgrp
        on model."u_modgrp" = modelgrp."code"
    left join div
        on model."u_division" = div."code"
    left join prodline
        on model."u_linecode" = prodline."code"
    left join color
        on items."u_argns_col" = color."u_colcode"
    left join fabric
        on fabric."u_collcode" = items."u_argns_coll"
    left join itemgroups 
        on itemgroups."itmsgrpcod" = items."itmsgrpcod"
    left join scale
        on scale."docentry"= items."docentry"
    left join brand_detail
        on items."u_argns_brand"= brand_detail."code"
    left join  {{ ref('DIM_PRODUCTCATEGORY') }} product_cat
        on   items."itmsgrpcod"=product_cat.ProductCategory_ID
        
    WHERE  items."itemcode"   is not null
),

surrogate_gen AS
(
      select A.*,
      ROW_NUMBER() OVER (PARTITION BY Product_ID ORDER BY createdate) AS row_num
      from joined A

)


select 

    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS PRODUCT_KEY,
    A.*,
    current_timestamp() AS InsertDate,
    NULL AS UpdateDate,
    'IO' AS DML

from surrogate_gen A

 where --A.Product_ID in ('MPHS1722-BLK-36-28')
 LATEST_FLAG =1  and row_num = 1
 ORDER BY PRODUCT_KEY ASC


 

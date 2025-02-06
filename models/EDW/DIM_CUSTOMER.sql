
WITH Customer_Data AS (

    SELECT 
	
        crd."cardcode" AS CUSTOMER_ID,
        crd."cardname" AS CUSTOMER_NAME, 
        crd."cardtype" AS CUSTOMER_TYPE,
        crd."groupcode" AS GROUP_CODE,
        crd."e_mail" AS CUSTOMER_EMAIL, 
        crd."phone1" AS CUSTOMER_CONTACT_NUMBER1,
        crd."phone2" AS CUSTOMER_CONTACT_NUMBER2,
        crd."u_region" AS REGION, 
        crd."u_v33_ter" AS TERRITORY,
        crd."shiptodef" AS CUSTOMER_DEFAULT_SHIPTO,
        crd."cntctprsn" AS CUSTOMER_CONTACT_PERSON, 
        crd."currency" AS CUSTOMER_CURRENCY,
	    crd."createdate" AS CUSTOMER_CREATED_DATE,
        crd."updatedate" AS CUSTOMER_UPDATED_DATE,
		MAX(crd1."address") AS ADDRESS,
        MAX(crd1."address2") AS ADDRESS2,
        MAX(crd1."address3") AS ADDRESS3,
        MAX(crd1."street") AS STREET,
        MAX(crd1."zipcode") AS ZIPCODE,
        MAX(crd1."city") AS CITY,
        MAX(crd1."county") AS COUNTY,
        MAX(crd1."country") AS COUNTRY,
        MAX(crd1."state") AS STATE,
        MAX(crd1."building") AS LOCATION,
        MAX(crd1."addrtype") AS ADDRESS_TYPE,
        MAX(crd1."gsttype") AS GST_TYPE,
        MAX(CASE WHEN ocrg."u_v33_grouptype" LIKE '%Wholesale%' THEN 'Wholesale' ELSE NULL END) AS ISWHOLESALE,
        MAX(CASE WHEN ocrg."u_v33_grouptype" LIKE '%Retail%' THEN 'Retail' ELSE NULL END) AS ISRETAILER,
        MAX(CASE WHEN ocrg."u_v33_grouptype" LIKE '%Ecomm%' THEN 'Ecommerce' ELSE NULL END) AS ISECOMMERCE,
        MAX(CASE WHEN ocrg."u_v33_grouptype" LIKE '%Others%' OR ocrg."groupname" LIKE '%Others%' THEN 'Others' ELSE NULL END) AS ISOTHER,
		cal.date_key AS InsertDate_Key,
        cal1.date_key AS UpdateDate_Key,
		crd.source_region AS SOURCE_REGION,
        crd.source AS SOURCE

    FROM {{ref('SAP_OCRD')}} crd
	
    LEFT JOIN {{ref('SAP_CRD1')}} crd1 
        ON crd."cardcode" = crd1."cardcode"
		
    LEFT JOIN {{ref('SAP_OCRG')}} ocrg 
        ON crd."groupcode" = ocrg."groupcode"
	
	LEFT JOIN {{ref('DIM_CALENDAR')}} cal
        ON CAST(crd."createdate" AS DATE) = cal.date
		
    LEFT JOIN {{ref('DIM_CALENDAR')}} cal1
        ON CAST(crd."updatedate" AS DATE) = cal1.date

   

    GROUP BY 
        crd."cardcode", crd."cardname", crd."cardtype", crd."groupcode", 
        crd."e_mail", crd."phone1", crd."phone2", crd."u_region", crd."u_v33_ter",
        crd."shiptodef", crd."cntctprsn", crd."currency", cal.date_key,cal1.date_key,
        crd.source,crd.source_region,crd."createdate",crd."updatedate"
),

Final_Data AS (

SELECT 

ROW_NUMBER() OVER (ORDER BY Customer_ID) as Customer_Key,
*,
current_timestamp() as InsertDate,
Null as UpdateDate,
'IO' AS DML,
 ROW_NUMBER() OVER (partition by Customer_ID order by Address) AS row_num
	
FROM Customer_Data

ORDER BY Customer_Key,Customer_ID, Address


)


SELECT * FROM Final_Data
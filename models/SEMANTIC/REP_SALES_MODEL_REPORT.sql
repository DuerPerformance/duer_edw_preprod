select * EXCLUDE (currency_rate_2, channel), currency_rate_2, 'consolidated' as consolidation_type, ORDERDISCOUNT/(max_line_num+1) as ORDERDISCOUNT_2,

--WHERE NOT (CHANNEL LIKE 'Other' AND LOCATION_NAME LIKE 'Pimlico%')

    CASE 
        WHEN channel = 'Other' AND intercompany_flag = 'Y' THEN 'Intercompany'
        
        WHEN channel = 'Other' AND intercompany_flag = 'N' AND loc_name LIKE '%Pimlico%' AND loc_name LIKE '%RFD%' THEN 'RFD'
        
        WHEN channel = 'Other' AND intercompany_flag = 'N' AND loc_name LIKE '%Pimlico Reserve Inventory%' THEN 'Reserve Inventory'
        
        WHEN channel = 'Other' AND intercompany_flag = 'N' AND loc_name LIKE '%US Bookings Hold%' THEN 'US Bookings Hold'
        
        WHEN channel = 'Other' AND intercompany_flag = 'N' AND loc_name IN ('Pimlico Canada', 'Pimlico US', 'Pimlico US (US Dollar)') THEN 'Other'
    
        ELSE channel
    END AS channel,



    CASE 
        WHEN channel = 'Ecommerce' AND loc_name LIKE 'CA%' THEN 'CA'
        
        WHEN channel = 'Retail' AND loc_name LIKE 'CA%' THEN 'CA'
        
        WHEN channel = 'Ecommerce' AND loc_name LIKE 'US%' THEN 'US'
        
        WHEN channel = 'Retail' AND loc_name LIKE 'US%' THEN 'US'
        
        WHEN line_of_business = 'Other' THEN 'CA'
        
        WHEN channel = 'Wholesale' AND WS IS NULL THEN 'n/a'
        
        WHEN channel = 'Wholesale' AND WS LIKE '%WSCAD%' THEN 'CA'
        
        WHEN channel = 'Wholesale' AND WS LIKE '%WSUS%' THEN 'US'
        
        WHEN channel = 'Wholesale' AND WS LIKE '%WSEU%' THEN 'EU'
        

    END AS country, LOC_NAME as LOCATION



from {{ref('REP_SALES_MODEL_REPORT_PREV')}}

union all
select * EXCLUDE (currency_rate_2, channel), 1 as currency_rate_2, 'transaction' as consolidation_type, ORDERDISCOUNT/(max_line_num+1) as ORDERDISCOUNT_2,



   CASE 
        WHEN channel = 'Other' AND intercompany_flag = 'Y' THEN 'Intercompany'
        
        WHEN channel = 'Other' AND intercompany_flag = 'N' AND loc_name LIKE '%Pimlico%' AND loc_name LIKE '%RFD%' THEN 'RFD'
        
        WHEN channel = 'Other' AND intercompany_flag = 'N' AND loc_name LIKE '%Pimlico Reserve Inventory%' THEN 'Reserve Inventory'
        
        WHEN channel = 'Other' AND intercompany_flag = 'N' AND loc_name LIKE '%US Bookings Hold%' THEN 'US Bookings Hold'
        
        WHEN channel = 'Other' AND intercompany_flag = 'N' AND loc_name IN ('Pimlico Canada', 'Pimlico US', 'Pimlico US (US Dollar)') THEN 'Other'
    
        ELSE channel
    END AS channel,



    CASE 
        WHEN channel = 'Ecommerce' AND loc_name LIKE 'CA%' THEN 'CA'
        
        WHEN channel = 'Retail' AND loc_name LIKE 'CA%' THEN 'CA'
        
        WHEN channel = 'Ecommerce' AND loc_name LIKE 'US%' THEN 'US'
        
        WHEN channel = 'Retail' AND loc_name LIKE 'US%' THEN 'US'
        
        WHEN line_of_business = 'Other' THEN 'CA'
        
        WHEN channel = 'Wholesale' AND WS IS NULL THEN 'n/a'
        
        WHEN channel = 'Wholesale' AND WS LIKE '%WSCAD%' THEN 'CA'
        
        WHEN channel = 'Wholesale' AND WS LIKE '%WSUS%' THEN 'US'
        
        WHEN channel = 'Wholesale' AND WS LIKE '%WSEU%' THEN 'EU'
        

    END AS country, LOC_NAME as LOCATION



from {{ref('REP_SALES_MODEL_REPORT_PREV')}}
--WHERE NOT (CHANNEL LIKE 'Other' AND LOCATION_NAME LIKE 'Pimlico%')
union all
select * EXCLUDE (currency_rate_2, channel), currency_rate as currency_rate_2, 'subsidiary' as consolidation_type, ORDERDISCOUNT/(max_line_num+1) as ORDERDISCOUNT_2,


   CASE 
        WHEN channel = 'Other' AND intercompany_flag = 'Y' THEN 'Intercompany'
        
        WHEN channel = 'Other' AND intercompany_flag = 'N' AND loc_name LIKE '%Pimlico%' AND loc_name LIKE '%RFD%' THEN 'RFD'
        
        WHEN channel = 'Other' AND intercompany_flag = 'N' AND loc_name LIKE '%Pimlico Reserve Inventory%' THEN 'Reserve Inventory'
        
        WHEN channel = 'Other' AND intercompany_flag = 'N' AND loc_name LIKE '%US Bookings Hold%' THEN 'US Bookings Hold'
        
        WHEN channel = 'Other' AND intercompany_flag = 'N' AND loc_name IN ('Pimlico Canada', 'Pimlico US', 'Pimlico US (US Dollar)') THEN 'Other'
    
        ELSE channel
    END AS channel,



    CASE 
        WHEN channel = 'Ecommerce' AND loc_name LIKE 'CA%' THEN 'CA'
        
        WHEN channel = 'Retail' AND loc_name LIKE 'CA%' THEN 'CA'
        
        WHEN channel = 'Ecommerce' AND loc_name LIKE 'US%' THEN 'US'
        
        WHEN channel = 'Retail' AND loc_name LIKE 'US%' THEN 'US'
        
        WHEN line_of_business = 'Other' THEN 'CA'
        
        WHEN channel = 'Wholesale' AND WS IS NULL THEN 'n/a'
        
        WHEN channel = 'Wholesale' AND WS LIKE '%WSCAD%' THEN 'CA'
        
        WHEN channel = 'Wholesale' AND WS LIKE '%WSUS%' THEN 'US'
        
        WHEN channel = 'Wholesale' AND WS LIKE '%WSEU%' THEN 'EU'
        

    END AS country, LOC_NAME as LOCATION



from {{ref('REP_SALES_MODEL_REPORT_PREV')}}
--WHERE NOT (CHANNEL LIKE 'Other' AND LOCATION_NAME LIKE 'Pimlico%')
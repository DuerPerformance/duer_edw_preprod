With CalProfitCode As (
    Select
        *,
        Case When SOURCE_REGION = 'CA' Then Concat('CA_',"transid")
             When SOURCE_REGION = 'US' Then Concat('US_',"transid")
             End Transaction_ID,
        CASE 
            When "profitcode" Is Not Null And "profitcode" != '' Then "profitcode"
            WHEN "linememo" ILIKE '%SCAE01%' THEN 'EcommCAD'
            WHEN "linememo" ILIKE '%SUSE01%' THEN 'EcommUS'
            WHEN "linememo" ILIKE '%SCAR01%' THEN 'RetailVN'
            WHEN "linememo" ILIKE '%SCAR02%' THEN 'RetailTO'
            WHEN "linememo" ILIKE '%SCAR03%' THEN 'RetailCG'
            WHEN "linememo" ILIKE '%SCAR04%' THEN 'RetTO2'
            WHEN "linememo" ILIKE '%SCAR05%' THEN 'RetailOT'
            WHEN "linememo" ILIKE '%SCAR06%' THEN 'RetailED'
            WHEN "linememo" ILIKE '%SUSR01%' THEN 'RetailDV'
            WHEN "linememo" ILIKE '%SUSR02%' THEN 'RetailLA'
            WHEN "linememo" ILIKE '%PICCUS01%' THEN 'Intercompany'
            ELSE RIGHT("linememo", charindex('-', REVERSE("linememo")) - 2)
            END CalProfitCode,
        CASE
            When SOURCE_REGION = 'CA' Then 'PPACA'
            When SOURCE_REGION = 'US' Then 'PPAUS'
            End SUBSIDIARY_ID,
    From {{ref('SAP_JDT1')}}
)
, Final As
(
Select 
    t.Transaction_Key,
    a.Account_Key,
    c.COSTCENTER_KEY,
    s.SUBSIDIARY_KEY,
    1 As Exchange_Rate,
    j."line_id" Line_Number,
    j."linememo" Memo,
    j."contraact" Contract,
  
    j."debit" as Local_Deb,
    j."credit" as Local_Cred,
    j."fcdebit" as fcdeb,
    j."fccredit" as fccred, 
    j."sysdeb" System_Deb,
    j."syscred" System_Cred,
    j."vatrate" Vat_Rate,
    j."vatamount" Vat_Amount,
    j."grossvalue" Gross_Value,
    j."fccurrency" as fccurrency
    ,
    Case 
        When j.SOURCE_REGION = 'CA' Then j."totalvat"
        When j.SOURCE_REGION = 'US' Then j."systvat"
    End Total_Vat
    ,j.SOURCE_REGION
From CalProfitCode j 
Inner Join {{ref('DIM_TRANSACTION')}} t On t.Transaction_Id = j.Transaction_Id 
Inner Join {{ref('DIM_ACCOUNT')}} a On a.accountid = j."account"
And a.SOURCE_REGION = j.SOURCE_REGION
Inner Join {{ref('DIM_SUBSIDIARY')}} s On s.SUBSIDIARY_ID = j.SUBSIDIARY_ID
Left Join {{ref('DIM_COSTCENTER')}} c On c.COSTCENTER_ID = j.CalProfitCode
)
Select 
    Transaction_Key,
    Account_Key,
    COSTCENTER_KEY,
    SUBSIDIARY_KEY,
    Exchange_Rate,
    Line_Number,
    Memo,
    Local_Deb,
    Local_Cred,
    fcdeb,
    fccred,
    CASE WHEN fccurrency IS NULL
              THEN CASE WHEN source_region = 'US' THEN 'USD'
              ELSE 'CAD'
              END 
        ELSE fccurrency
        END as fcurrency
    ,Contract,
    System_Deb,
    System_Cred,
    Vat_Rate,
    Vat_Amount,
    Gross_Value, 
    Total_Vat
From Final
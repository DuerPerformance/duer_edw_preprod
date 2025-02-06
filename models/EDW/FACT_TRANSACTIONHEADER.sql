With FisCal As
(
    Select DATE_KEY_2,DATE,CALENDAR_TYPE
    From {{ref('DIM_CALENDAR')}}
    Where CALENDAR_TYPE = 'DUER FISCAL CALENDAR'
)
,Final As
(
    Select
        dt.TRANSACTION_KEY,
        s.SUBSIDIARY_KEY,
        f1.DATE_KEY_2 As TRANSACTION_CREATE_DATE_KEY,
        f2.DATE_KEY_2 As TRANSACTION_POST_DATE_KEY,
        f3.DATE_KEY_2 As TRANSACTION_TAX_DATE_KEY,
        f4.DATE_KEY_2 As TRANSACTION_DUE_DATE_KEY,
        Case 
            When o.SOURCE_REGION = 'CA' Then o."loctotal"
            When o.SOURCE_REGION = 'US' Then o."systotal"
        End Total,
        o."systotal" As System_Total
    From {{ref('SAP_OJDT')}} o 
    Inner Join {{ref('DIM_TRANSACTION')}} dt 
    On dt.TRANSACTION_ID = Case When o.SOURCE_REGION = 'CA' Then Concat('CA_',o."transid")
            When o.SOURCE_REGION = 'US' Then Concat('US_',o."transid")
            End 
    Inner Join {{ref('DIM_SUBSIDIARY')}} s 
    On s.SUBSIDIARY_ID = CASE When o.SOURCE_REGION = 'CA' Then 'PPACA'
            When o.SOURCE_REGION = 'US' Then 'PPAUS'
            End
    Inner Join FisCal f1 On f1.DATE = o."createdate" 
    Inner Join FisCal f2 On f2.DATE = o."refdate" 
    Inner Join FisCal f3 On f3.DATE = o."taxdate" 
    Inner Join FisCal f4 On f4.DATE = o."duedate" 
)
Select 
    TRANSACTION_KEY,
    SUBSIDIARY_KEY,
    TRANSACTION_CREATE_DATE_KEY,
    TRANSACTION_POST_DATE_KEY,
    TRANSACTION_TAX_DATE_KEY,
    TRANSACTION_DUE_DATE_KEY,
    Total,
    System_Total
From Final
select b.inv_OrderDiscount,
b.inv_Order_discount_percentage,
b.inv_date,
b.inv_docnum,

a.inv_docentry,
a.inv_LINENUM,
a.invoice_date_key,
a.inv_Price,
a.inv_Quantity,
a.inv_delivered_quantity,
a.inv_open_quantity,
a.inv_Pricebefdi,
a.inv_Linetotal,
a.inv_vatsum,
a.inv_Gross_Total,
a.inv_Product_discount_Percentage,
a.inv_item_code,

a.inv_baseentry,
a.inv_baseline,
a.source_region

from {{ref('FACT_INVOICE')}} b left join {{ref('FACT_INVOICEDETAIL')}} a on a.inv_docentry = b.inv_docentry and a.source_region = b.source_region
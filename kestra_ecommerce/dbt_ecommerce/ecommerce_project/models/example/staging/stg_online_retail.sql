with source as (

    select * 
    from `kestra-zoomcamp-v2.ecommerce.online_retail`

),

cleaned as (

    select
        InvoiceNo as invoice_no,
        StockCode as stock_code,
        Description as description,
        Quantity as quantity,
        InvoiceDate as invoice_date,
        UnitPrice as unit_price,
        CustomerID as customer_id,
        Country as country,

        Quantity * UnitPrice as revenue

    from source
    where Quantity > 0

)

select * from cleaned
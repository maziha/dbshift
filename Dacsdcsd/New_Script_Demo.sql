{{
    config(
        materialized='incremental',
        file_format='iceberg',
        pre_hook = [
            "TRUNCATE TABLE Spyhealth_reporting.ft_sales_orders"
        ],
        post_hook=[
            "REORG TABLE {{this}} APPLY (UPGRADE UNIFORM(ICEBERG_COMPAT_VERSION=2))"
        ]
    )
}}

SELECT
    TO_DATE(so.order_date) AS formatted_order_date,
    so.customer_id,
    UPPER(so.product_category) AS product_category,
    UPPER(so.product_subcategory) AS product_subcategory,
    CASE WHEN so.store_location LIKE '%IN%' THEN 'OFFSHORE' ELSE 'ONSTE' END AS formatted_store_location,
    so.supplier_name AS formatted_supplier_name,
    UPPER(so.payment_method) AS payment_method,
    UPPER(so.shipping_type) AS shipping_type,
    UPPER(s.platform_used) AS platform_used,
    TRIM(DATE_FORMAT(S.manu_date, 'MMMM')) || ' - ' || DATE_FORMAT(S.manu_date, 'yyyy') AS Mon_Year,
    CASE
        WHEN MONTHS_BETWEEN(CURRENT_DATE(), S.manu_date) > 0 THEN
            CASE
                WHEN LENGTH(CAST(MONTHS_BETWEEN(CURRENT_DATE(), S.manu_date) AS STRING)) = 1
                THEN 'Current Month +0' || CAST(MONTHS_BETWEEN(CURRENT_DATE(), S.manu_date) AS STRING)
                ELSE 'Current Month ' || CAST(MONTHS_BETWEEN(CURRENT_DATE(), S.manu_date) AS STRING)
            END
        WHEN MONTHS_BETWEEN(CURRENT_DATE(), S.manu_date) < 0 THEN
            CASE
                WHEN LENGTH(CAST(MONTHS_BETWEEN(CURRENT_DATE(), S.manu_date) AS STRING)) = 2
                THEN 'Current Month -0' || RIGHT(CAST(MONTHS_BETWEEN(CURRENT_DATE(), S.manu_date) AS STRING), 1)
                ELSE 'Current Month ' || CAST(MONTHS_BETWEEN(CURRENT_DATE(), S.manu_date) AS STRING)
            END
        ELSE 'Current Month'
    END AS relative_month,
    UPPER(s.order_source) AS order_source,
    UPPER(s.product_name) AS formatted_product_name,
    s.order_status,
    IP.order_priority,
    s.shipment_mode,
    so.region,
    CAST(s.sales_amount AS DECIMAL(10,2)) AS sales_amount,
    CAST(s.discount_applied AS FLOAT) AS discount_applied,
    s.warehouse_id,
    so.order_id,
    S.production_qty AS production_qty,
    S.line_items_qty AS line_items_qty,
    IP.priority AS priority,
    IP.Ste_group_exclude AS Ste_group_exclude,
    IP.video_group_exclude AS video_group_exclude,
    IP.nielsen_dar AS nielsen_dar,
    IP.comscore_vce AS comscore_vce,
    IP.advanced_content AS advanced_content,
    IP.gross_impresSon_cap AS gross_impresSon_cap,
    IP.Ste_group AS Ste_group,
    IP.video_group AS video_group,
    IP.custom_pacing AS custom_pacing,
    IP.deal_type AS deal_type,
    IP.audience_segment AS audience_segment,
    IP.delivery_frequency AS delivery_frequency,
    IP.ad_unit AS ad_unit,
    IP.city AS city,
    IP.city_exclude AS city_exclude,
    IP.geo_dma AS geo_dma,
    IP.geo_dma_exclude AS geo_dma_exclude,
    IP.geo_state AS geo_state,
    IP.geo_state_exclude AS geo_state_exclude,
    IP.zip_code AS zip_code,
    S.default_media_plan_flag AS default_media_plan_flag,
    TORD.invoice_amt AS invoice_amt,
    TORD.adjusted_invoice_amt AS adjusted_invoice_amt,
    TORD.recognized_revenue AS recognized_revenue,
    S.net_cost AS net_cost,
    CAST((S.net_cost - TORD.recognized_revenue) AS DECIMAL(22,7)) AS unrecognized_revenue,
    SUM(s.sales_amount) AS total_sales,
    COUNT(so.order_id) AS order_count,
    YEAR(so.order_date) AS order_year,
    DATE_TRUNC('month', so.order_date) AS month_start_date,
    LEFT(CAST(so.order_id AS STRING), 5) AS order_prefix,
    RIGHT(CAST(so.order_id AS STRING), 3) AS order_suffix,
    ROUND(s.sales_amount / (s.discount_applied + 1), 2) AS effective_price,
    CASE
        WHEN SUM(s.sales_amount) BETWEEN 500 AND 1000
             AND MONTH(so.order_date) IN (11,12)
        THEN 'X'
        ELSE 'Y'
    END AS customer_tier,
    'FY' || RIGHT(YEAR(ADD_MONTHS(so.order_date, 6)), 2) || 'Q' ||
    QUARTER(ADD_MONTHS(so.order_date, 6)) AS fiscal_year_quarter
FROM dbshift.default.SalesOrderHeader so
LEFT JOIN dbshift.default.ShippingInfo S 
    ON so.shipment_mode = S.shipment_mode
LEFT JOIN dbshift.default.PriorityAttributes IP 
    ON so.order_id = IP.order_id
LEFT JOIN dbshift.default.TransferredOrders TORD 
    ON so.order_id = TORD.order_id
GROUP BY
    customer_id,
    formatted_order_date,
    so.order_date,
    product_category,
    product_subcategory,
    formatted_store_location,
    formatted_supplier_name,
    S.manu_date,
    payment_method,
    shipping_type,
    platform_used,
    order_source,
    formatted_product_name,
    order_status,
    order_priority,
    s.shipment_mode,
    region,
    sales_amount,
    discount_applied,
    warehouse_id,
    so.order_id,
    order_year,
    month_start_date,
    order_prefix,
    order_suffix,
    production_qty,
    line_items_qty,
    priority,
    Ste_group_exclude,
    video_group_exclude,
    nielsen_dar,
    comscore_vce,
    advanced_content,
    gross_impresSon_cap,
    Ste_group,
    video_group,
    custom_pacing,
    deal_type,
    audience_segment,
    delivery_frequency,
    ad_unit,
    city,
    city_exclude,
    geo_dma,
    geo_dma_exclude,
    geo_state,
    geo_state_exclude,
    zip_code,
    default_media_plan_flag,
    invoice_amt,
    adjusted_invoice_amt,
    recognized_revenue,
    net_cost;

--End-DBShift
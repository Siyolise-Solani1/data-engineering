WITH base AS (
    
    SELECT 
        o.order_id, c.name as customer_name, o.order_date, s.status as order_status,
        s.shipped_at, s.delivered_at, DATEDIFF('hour',s.shipped_at,s.delivered_at) as delivery_hours
    FROM {{ ref('stg_orders') }} o
    JOIN {{ ref('stg_customers') }} c ON o.customer_id = c.customer_id
    JOIN {{ ref('stg_shipments') }} s ON o.order_id = s.order_id

)

SELECT *,
    CASE WHEN order_status <> 'delivered' and delivery_hours > 48 THEN 'delayed' ELSE order_status END AS final_status
FROM base
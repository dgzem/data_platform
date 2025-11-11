SELECT
    customer_id,
    COUNT(*) AS total_transactions,
    SUM(quantity) AS total_quantity,
    SUM(price * quantity) AS total_revenue,
    AVG(price * quantity) AS average_order_value,
    MIN(transaction_date) AS first_purchase_date,
    MAX(transaction_date) AS last_purchase_date,
    NOW() AS load_datetime
from {{ ref('tb_dim_customers_transactions') }}
group by customer_id
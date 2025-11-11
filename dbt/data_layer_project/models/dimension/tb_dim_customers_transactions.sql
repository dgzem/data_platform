    SELECT
        CASE WHEN transaction_id ~ '^\d+(\.\d+)?$' THEN transaction_id::numeric::integer ELSE NULL END as transaction_id,
        CASE WHEN customer_id ~ '^\d+(\.\d+)?$' THEN customer_id::numeric::integer ELSE NULL END as customer_id,
        CASE WHEN transaction_date ~ '^\d{4}-\d{2}-\d{2}$' THEN transaction_date::date ELSE NULL END as transaction_date,
        CASE WHEN product_id ~ '^\d+(\.\d+)?$' THEN product_id::numeric::integer ELSE NULL END as product_id,
        CASE WHEN product_name IS NOT NULL AND product_name <> 'NaN' THEN product_name::TEXT ELSE NULL END as product_name,
        CASE WHEN quantity ~ '^\d+(\.\d+)?$' THEN quantity::numeric::integer ELSE NULL END as quantity,
        CASE WHEN price ~ '^\d+(\.\d+)?$' THEN price::numeric(10,2) ELSE NULL END as price,
        CASE WHEN tax ~ '^\d+(\.\d+)?$' THEN tax::numeric(10,2) ELSE NULL END as tax,
        NOW() AS load_datetime
    FROM {{ source('pipeline', 'tb_raw_customer_transactions') }}

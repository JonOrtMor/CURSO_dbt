{{ config(materialized='view') }}

WITH src_orders AS (
    SELECT * FROM {{ source('postgres_public', 'POSTGRES_ORDERS') }}
),

renamed_casted AS (
    SELECT
        order_id
        , shipping_service
        , CAST(shipping_cost AS FLOAT)          AS shipping_cost
        , address_id
        , CAST(created_at AS TIMESTAMP_TZ)      AS created_at
        , promo_id
        , CAST(estimated_delivery_at AS TIMESTAMP_TZ) AS estimated_delivery_at
        , CAST(order_cost AS FLOAT)             AS order_cost
        , user_id
        , CAST(order_total AS FLOAT)            AS order_total
        , CAST(delivered_at AS TIMESTAMP_TZ)    AS delivered_at
        , tracking_id
        , status
        , _fivetran_deleted                     AS is_deleted
        , _fivetran_synced                      AS date_load
    FROM src_orders
    WHERE _fivetran_deleted IS NULL OR _fivetran_deleted = FALSE
)

SELECT * FROM renamed_casted

{{ config(materialized='view') }}

WITH src_orderitems AS (
    SELECT * FROM {{ source('postgres_public', 'POSTGRES_ORDERITEMS') }}
),

renamed_casted AS (
    SELECT
        order_id
        , product_id
        , quantity
        , _fivetran_deleted          AS is_deleted
        , _fivetran_synced           AS date_load
    FROM src_orderitems
    WHERE _fivetran_deleted IS NULL OR _fivetran_deleted = FALSE
)

SELECT * FROM renamed_casted

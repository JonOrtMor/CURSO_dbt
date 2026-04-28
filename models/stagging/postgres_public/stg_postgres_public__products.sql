{{ config(materialized='view') }}

WITH src_products AS (
    SELECT * FROM {{ source('postgres_public', 'POSTGRES_PRODUCTS') }}
),

renamed_casted AS (
    SELECT
        product_id
        , CAST(price AS FLOAT)      AS price
        , name
        , CAST(inventory AS NUMBER) AS inventory
        , _fivetran_deleted         AS is_deleted
        , _fivetran_synced          AS date_load
    FROM src_products
    WHERE _fivetran_deleted IS NULL OR _fivetran_deleted = FALSE
)

SELECT * FROM renamed_casted

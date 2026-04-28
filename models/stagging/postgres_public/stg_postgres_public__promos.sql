{{ config(materialized='view') }}

WITH src_promos AS (
    SELECT * FROM {{ source('postgres_public', 'POSTGRES_PROMOS') }}
),

renamed_casted AS (
    SELECT
        promo_id
        , CAST(discount AS NUMBER)  AS discount
        , status
        , _fivetran_deleted         AS is_deleted
        , _fivetran_synced          AS date_load
    FROM src_promos
    WHERE _fivetran_deleted IS NULL OR _fivetran_deleted = FALSE
)

SELECT * FROM renamed_casted

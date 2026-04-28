{{ config(materialized='view') }}

WITH src_addresses AS (
    SELECT * FROM {{ source('postgres_public', 'POSTGRES_ADDRESSES') }}
),

renamed_casted AS (
    SELECT
        address_id
        , zipcode
        , country
        , address
        , state
        , _fivetran_deleted          AS is_deleted
        , _fivetran_synced           AS date_load
    FROM src_addresses
    WHERE _fivetran_deleted IS NULL OR _fivetran_deleted = FALSE
)

SELECT * FROM renamed_casted

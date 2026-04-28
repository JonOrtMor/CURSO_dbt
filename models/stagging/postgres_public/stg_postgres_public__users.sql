{{ config(materialized='view') }}

WITH src_users AS (
    SELECT * FROM {{ source('postgres_public', 'POSTGRES_USER') }}
),

renamed_casted AS (
    SELECT
        user_id
        , CAST(updated_at AS TIMESTAMP_TZ)  AS updated_at
        , address_id
        , last_name
        , CAST(created_at AS TIMESTAMP_TZ)  AS created_at
        , phone_number
        , CAST(total_orders AS NUMBER)       AS total_orders
        , first_name
        , email
        , _fivetran_deleted                 AS is_deleted
        , _fivetran_synced                  AS date_load
    FROM src_users
    WHERE _fivetran_deleted IS NULL OR _fivetran_deleted = FALSE
)

SELECT * FROM renamed_casted

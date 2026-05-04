{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='_row'
) }}

WITH stg_budget_products AS (
    SELECT * 
    FROM {{ source('google_sheets', 'budget') }}
),

renamed_casted AS (
    SELECT
          _row
        , month
        , quantity
        , _fivetran_synced
    FROM stg_budget_products
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY _row ORDER BY _fivetran_synced DESC) AS rn
    FROM renamed_casted
)

SELECT _row, month, quantity, _fivetran_synced
FROM deduped
WHERE rn = 1

{% if is_incremental() %}
    AND _fivetran_synced > (SELECT MAX(_fivetran_synced) FROM {{ this }})
{% endif %}
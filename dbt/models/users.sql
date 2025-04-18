{{ config(
    materialized='incremental',
    alias='users',
    unique_key='user_id'
) }}

SELECT
  user_id,
  inserted_at,
  raw_api_data->>'email' AS email,
  raw_api_data->'name'->>'first' AS first_name,
  raw_api_data->'name'->>'last' AS last_name,
  raw_api_data->>'nat' AS nationality
FROM users_raw.users_raw  -- ✅ schema.table

{% if is_incremental() %}
WHERE inserted_at > (SELECT MAX(inserted_at) FROM {{ this }})
{% endif %}
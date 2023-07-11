{{ config(
    indexes = [{'columns':['_airbyte_emitted_at', 'userid'],'type':'btree'}],
    unique_key = 'userid',
    schema = "public",
    tags = [ "top-level" ],
    incremental_strategy='delete+insert',
    materialized='incremental'
) }}
-- Final base SQL model
-- depends_on: {{ ref('dbo_users_ab3') }}
select
    email,
    ab_synced,
    userid,
    firstname,
    createdt,
    lastname,
    ab_deleted,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_dbo_users_hashid
from {{ ref('dbo_users_ab3') }}
-- dbo_users from {{ source('public', '_airbyte_raw_dbo_users') }}
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}


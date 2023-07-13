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
    a.email,
    a.ab_synced,
    a.userid,
    a.firstname,
    a.createdt,
    a.lastname,
    a.ab_deleted,
    a._airbyte_ab_id,
    a._airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    a._airbyte_dbo_users_hashid
from {{ ref('dbo_users_ab3') }} a
left join {{this}} b on  a.userid = b.userid
-- dbo_users from {{ source('public', '_airbyte_raw_dbo_users') }}
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}


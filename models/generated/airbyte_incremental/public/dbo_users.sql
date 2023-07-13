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
    case when a.ab_deleted = true then b.email else a.email end as email,
    case when a.ab_deleted = true then b.ab_synced else a.ab_synced end as ab_synced,
    case when a.ab_deleted = true then b.userid else a.userid end as userid,
    case when a.ab_deleted = true then b.firstname else a.firstname end as firstname,
    case when a.ab_deleted = true then b.createdt else a.createdt end as createdt,
    case when a.ab_deleted = true then b.lastname else a.lastname end as lastname,
    a.ab_deleted,
    a._airbyte_ab_id,
    a._airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    a._airbyte_dbo_users_hashid
from {{ ref('dbo_users_ab3') }} a
left join {{this}} b on  a.userid = b.userid
-- dbo_users from {{ source('public', '_airbyte_raw_dbo_users') }}
where 1 = 1
and coalesce(
    cast({{ 'a._airbyte_emitted_at' }} as {{ type_timestamp_with_timezone() }}) > (select max(cast(_airbyte_emitted_at as {{ type_timestamp_with_timezone() }})) from {{ this }}),
    true)

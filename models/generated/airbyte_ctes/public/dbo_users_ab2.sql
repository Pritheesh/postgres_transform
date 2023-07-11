{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_public",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('dbo_users_ab1') }}
select
    cast(email as {{ dbt_utils.type_string() }}) as email,
    cast(ab_synced as {{ dbt_utils.type_string() }}) as ab_synced,
    cast(userid as {{ dbt_utils.type_bigint() }}) as userid,
    cast(firstname as {{ dbt_utils.type_string() }}) as firstname,
    cast(createdt as {{ dbt_utils.type_string() }}) as createdt,
    cast(lastname as {{ dbt_utils.type_string() }}) as lastname,
    {{ cast_to_boolean('ab_deleted') }} as ab_deleted,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('dbo_users_ab1') }}
-- dbo_users
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}


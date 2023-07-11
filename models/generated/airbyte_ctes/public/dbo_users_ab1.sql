{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_public",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('public', '_airbyte_raw_dbo_users') }}
select
    {{ json_extract_scalar('_airbyte_data', ['Email'], ['Email']) }} as email,
    {{ json_extract_scalar('_airbyte_data', ['ab_synced'], ['ab_synced']) }} as ab_synced,
    {{ json_extract_scalar('_airbyte_data', ['UserID'], ['UserID']) }} as userid,
    {{ json_extract_scalar('_airbyte_data', ['FirstName'], ['FirstName']) }} as firstname,
    {{ json_extract_scalar('_airbyte_data', ['CreateDt'], ['CreateDt']) }} as createdt,
    {{ json_extract_scalar('_airbyte_data', ['LastName'], ['LastName']) }} as lastname,
    {{ json_extract_scalar('_airbyte_data', ['ab_deleted'], ['ab_deleted']) }} as ab_deleted,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('public', '_airbyte_raw_dbo_users') }} as table_alias
-- dbo_users
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}


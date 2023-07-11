{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_public",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('dbo_users_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'email',
        'ab_synced',
        'userid',
        'firstname',
        'createdt',
        'lastname',
        boolean_to_string('ab_deleted'),
    ]) }} as _airbyte_dbo_users_hashid,
    tmp.*
from {{ ref('dbo_users_ab2') }} tmp
-- dbo_users
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}


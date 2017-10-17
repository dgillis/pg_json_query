create or replace view _pg_json_query._default_row_types_with_typeinfo as (
  select
    t.oid::regtype::text as type_name,
    t.oid as type_oid,
    t.*
  from pg_type t join pg_namespace n on t.typnamespace = n.oid
  where
    n.nspname = 'pg_catalog' and -- built-ins
    typcategory not in ('P', 'X') and -- neither psuedo nor unknown
    typname::text !~* '^pg_.*' and -- non-internal
    typisdefined -- is defined
);
create or replace view _pg_json_query._default_row_types as (
  select type_name, type_oid
  from _pg_json_query._default_row_types_with_typeinfo
);

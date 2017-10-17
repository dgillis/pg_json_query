
-- Constant JSONB object describing the "core" (i.e., base operators from
-- which all others are derived) operators. The keys are the operator names
-- aligning with the API operators. The value associated with each key is an
-- object describing the operator. It contains the following properties:
--    * op: the postgresql operator.
--    * is_symmetric: true if this operator acts on two values of the same type
--          or false otherwise.
--    * lhs_type (required only if is_symmetric is false): the type of the LHS
--          argument for a non-symmetric operator.
create or replace function _pg_json_query._core_ops()
returns jsonb language sql immutable as $$ select '{
  "eq": {
    "op": "=",
    "is_symmetric": true
  },
  "ne": {
    "op": "<>",
    "is_symmetric": true
  },
  "gt": {
    "op": ">",
    "is_symmetric": true
  },
  "lt": {
    "op": "<",
    "is_symmetric": true
  },
  "ge": {
    "op": ">=",
    "is_symmetric": true
  },
  "le": {
    "op": "<=",
    "is_symmetric": true
  },
  "contains": {
    "op": "@>",
    "is_symmetric": true
  },
  "contained": {
    "op": "<@",
    "is_symmetric": true
  },
  "exists": {
    "op": "?",
    "is_symmetric": false,
    "lhs_type": "text"
  },
  "existsany": {
    "op": "?|",
    "is_symmetric": false,
    "lhs_type": "text[]"
  },
  "existsall": {
    "op": "?&",
    "is_symmetric": false,
    "lhs_type": "text[]"
  }
}'::jsonb;
$$;



-- A view of the core types for which we should provide ops/casts for
-- by default. This is comprised of all built-in non-array/non-psuedo/
-- non-unknown types, which are defined and which are not internal. The
-- rows consist of a textual representation of the type (suitable to
-- substitute into dynamic SQL) and the types OID.
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
-- Compact view consisting only of type name and oid.
create or replace view _pg_json_query._default_row_types as (
  select type_name, type_oid
  from _pg_json_query._default_row_types_with_typeinfo
);


create or replace function _pg_json_query._core_op_info(type_oid oid)
returns table(op_name text, op text, lhs_oid oid, rhs_oid oid, op_exists boolean)
language sql stable as $$
  select *, exists(
    select 1
    from pg_operator o
    -- Does there exist a boolean-valued operator matching our op and types?
    where o.oprname = _.op and
          o.oprleft = _.lhs_oid and
          o.oprright = _.rhs_oid and
          o.oprresult = 'boolean'::regtype::oid
  ) as op_exists
  from (
    select
      op_name,
      info->>'op' as op,
      type_oid as lhs_oid,
      case
        when (info->>'is_symmetric')::boolean then
          type_oid
        else
          -- If not symmetric, lhs_type must be included.
          (info->>'lhs_type')::regtype::oid
      end as rhs_oid
    from jsonb_each(_pg_json_query._core_ops()) _(op_name, info)
  ) _;
$$;


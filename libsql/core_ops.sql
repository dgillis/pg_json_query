

create or replace function _pg_json_query._base_ops()
returns jsonb language sql stable as $$ select '{
  "=": "eq",
  "<>": "ne",
  "<": "lt",
  ">": "gt",
  ">=": "ge",
  "<=": "le",
  "@>": "contains",
  "<@": "contained",
  "?": "exists",
  "?|": "exists_any",
  "?&": "exists_all"
}'::jsonb;
$$;


create or replace view _pg_json_query._base_op_info_view as (
  with
    ops as (
      select
        o.oprname::text as op,
        o.oprleft as arg_type,
        o.oprright as right_arg_type,
        p.provolatile::text as volatility
      from pg_operator o
        join pg_proc p on o.oprcode = p.oid
        join pg_type t on o.oprleft = t.oid
      where
        o.oprkind = 'b' and
        -- Exclude operators on psuedotypes and unknown types.
        t.typcategory not in ('P', 'X') and
        (select _pg_json_query._base_ops()) ? o.oprname::text
    ),
    grouped as (
      select op, arg_type, right_arg_type, string_agg(volatility, '') as volatilities
      from ops
      group by op, arg_type, right_arg_type
    ),
    results as (
      select
        (_pg_json_query._base_ops())->>op as op_name,
        op,
        arg_type,
        arg_type::regtype::text as arg_type_name,
        right_arg_type,
        right_arg_type::regtype::text as right_arg_type_name,
        case
          when strpos(volatilities, 'v') > 0 then
            'volatile'
          when strpos(volatilities, 's') > 0 then
            'stable'
          else
            'immutable'
        end::text as volatility
      from grouped
    )
  select *
  from results
  where volatility != 'volatile'
);


create or replace function _pg_json_query._base_op_func_name(op_name text)
returns text language sql stable as $$
  select '_op__' || op_name;
$$;


create or replace function _pg_json_query._base_col_op_func_name(op_name text)
returns text language sql stable as $$
  select '_col_op__' || op_name;
$$;


create or replace function _pg_json_query._base_op_func_src(
  op_name text,
  left_arg_type_name text,
  right_arg_type_name text,
  volatility text,
  op text
) returns text language sql stable as $$
  select concat(
    'create or replace function ',
    '_pg_json_query.', _pg_json_query._base_op_func_name(op_name),
    '(x ', left_arg_type_name, ', ', 'y ', right_arg_type_name, ') ', E'\n',
    'returns boolean language sql ', volatility, ' as $function$ ', E'\n',
    '  select x ', op, ' y;', E'\n',
    '$function$;'
  )::text;
$$;


create or replace function _pg_json_query._base_col_op_func_src(
  op_name text
) returns text language sql stable as $$
  select concat(
    'create or replace function ',
    '_pg_json_query.', _pg_json_query._base_col_op_func_name(op_name),
    '(x anyelement, y jsonb, _coltype anyelement)', E'\n',
    'returns boolean language sql stable as $function$ ', E'\n',
    '  select _pg_json_query.', _pg_json_query._base_op_func_name(op_name),
    '(x, _pg_json_query._cast_column_value(_coltype, y));'
    '$function$;'
  )::text;
$$;


create or replace function _pg_json_query._base_op_dne_func_src(
  op_name text,
  op text
) returns text language sql stable as $$
  select concat(
    'create or replace function ',
    '_pg_json_query.', _pg_json_query._base_op_func_name(op_name),
    '(x anyelement, y anyelement) ', E'\n',
    'returns boolean language sql stable as $function$', E'\n',
    '  select _pg_json_query._base_op_does_not_exist(',
       quote_literal(op), ', ', quote_literal(op_name), ', x);', E'\n',
    '$function$;'
  )::text;
$$;


create or replace function _pg_json_query._base_op_func_src(
  r _pg_json_query._base_op_info_view
) returns text language sql stable as $$
  select _pg_json_query._base_op_func_src(
    (r).op_name::text, (r).arg_type_name::text, (r).right_arg_type_name::text,
    (r).volatility, (r).op::text
  );
$$;


create or replace function _pg_json_query._base_op_does_not_exist(
  op text, op_name text, arg anyelement
)
returns boolean language plpgsql stable as $$
begin
  raise exception 'json_query operator ''%'' is not defined for % type', op_name, pg_typeof(arg);
  return false;
end;
$$;


-- This function inspects pg_operator and using the operators it finds,
-- constructs inlinable functions corresponding to each. A possible source
-- of bugs will be if operators for a type are added AFTER this function is
-- called (since it will have not made functions for those operators). This
-- can be fixed by calling this function again (it's safe to call multiple
-- times since it uses CREATE OR REPLACE). So, a public version of this
-- function should be added to the API for this purpose.
create or replace function _pg_json_query._make_base_op_funcs()
returns void language plpgsql as $$
declare
  op text;
  op_name text;
  r _pg_json_query._base_op_info_view%rowtype;
  create_fn_sql text;
begin
  for op, op_name in (select *
                      from jsonb_each_text(_pg_json_query._base_ops())) loop
    create_fn_sql := _pg_json_query._base_op_dne_func_src(op_name, op);
    execute create_fn_sql;
    
    create_fn_sql := _pg_json_query._base_col_op_func_src(op_name);
    execute create_fn_sql;
  end loop;
  
  for r in (select * from _pg_json_query._base_op_info_view) loop
    create_fn_sql := _pg_json_query._base_op_func_src(r);
    execute create_fn_sql;
  end loop;
end;
$$;



select _pg_json_query._make_base_op_funcs();





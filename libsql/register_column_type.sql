


-- Fallback implementation used for non-existent operators (we need such
-- an implementation so that the static-type checker doesn't complain).
create or replace function _pg_json_query._op_does_not_exist(
  op_name text,
  op text,
  lhs_type_name text,
  rhs_type_name text
) returns boolean language plpgsql stable as $$
begin
  raise notice 'yeee';
  raise exception 'json_query operator ''%'' (%) is not defined for (%, %)',
    op_name, op, lhs_type_name, rhs_type_name;
  return false;
end;
$$;


create or replace function _pg_json_query._op_func_name(op_name text)
returns text language sql immutable as $$ select '_op__' || op_name; $$;

create or replace function _pg_json_query._col_op_func_name(op_name text)
returns text language sql immutable as $$ select '_col_op__' || op_name; $$;


create or replace function _pg_json_query._op_func_get_create_src(
  op_name text,
  op text,
  lhs_oid oid,
  rhs_oid oid,
  op_exists boolean
) returns text language plpgsql stable as $$
declare
  func_name text;
  lhs_type_name text;
  rhs_type_name text;
  func_expr_src text;
  func_src text;
begin
  func_name := _pg_json_query._op_func_name(op_name);
  lhs_type_name := lhs_oid::regtype::text;
  rhs_type_name := rhs_oid::regtype::text;
  
  func_expr_src := case
    when op_exists then
      concat('x ', op, ' y')
    else
      format(
        '_pg_json_query._op_does_not_exist(%L, %L, %L, %L)',
        op_name, op, lhs_type_name, rhs_type_name)
  end;

  func_src := concat(
    'create or replace function _pg_json_query.',
    func_name, '(x ', lhs_type_name, ', y ', rhs_type_name, ')', E'\n',
    'returns boolean language sql stable as $function$', E'\n',
    '  select ', func_expr_src, ';', E'\n',
    '$function$;'
  );
  
  return func_src;
end;
$$;


create or replace function _pg_json_query._op_func_get_drop_src(
  op_name text,
  lhs_oid oid,
  rhs_oid oid,
  cascade_ boolean default false
) returns text language sql stable as $$
  select concat(
    'drop function if exists _pg_json_query.',
    _pg_json_query._op_func_name(op_name), '(',
    lhs_oid::regtype::text, ', ', rhs_oid::regtype::text, ')',
    (case when cascade_ then ' cascade;' else ';' end)
  );
$$;


create or replace function _pg_json_query._col_op_func_get_create_src(
  op_name text
) returns text language sql stable as $$
  select concat(
    'create or replace function ',
    '_pg_json_query.', _pg_json_query._col_op_func_name(op_name),
    '(x anyelement, y jsonb, _coltype anyelement)', E'\n',
    'returns boolean language sql stable as $function$ ', E'\n',
    '  select _pg_json_query.', _pg_json_query._op_func_name(op_name),
    '(x, _pg_json_query._cast_column_value(_coltype, y));'
    '$function$;'
  )::text;
$$;


create or replace function _pg_json_query._col_op_func_get_drop_src(
  op_name text,
  cascade_ boolean default false
) returns text language sql stable as $$
  select concat(
    'drop function if exists _pg_json_query.',
    _pg_json_query._col_op_func_name(op_name),
    '(anyelement, json, anyelement)',
    (case when cascade_ then ' cascade;' else ';' end)
  );
$$;


create or replace function _pg_json_query._op_func_create(
  op_name text,
  op text,
  lhs_oid oid,
  rhs_oid oid,
  op_exists boolean
) returns void language plpgsql volatile as $$
declare
  src text;
begin
  src := _pg_json_query._op_func_get_create_src(
    op_name, op, lhs_oid, rhs_oid, op_exists);
  execute src;
end;
$$;


create or replace function _pg_json_query._op_func_drop(
  op_name text,
  lhs_oid oid,
  rhs_oid oid,
  cascade_ boolean default false  
) returns void language plpgsql volatile as $$
declare
  func_name text;
  src text;
begin
  src := _pg_json_query._op_func_get_drop_src(
    op_name, lhs_oid, rhs_oid, cascade_);
  execute src;
end;
$$;


create or replace function _pg_json_query._col_op_func_create(op_name text)
returns void language plpgsql volatile as $$
declare
  src text;
begin
  src := _pg_json_query._col_op_func_get_create_src(op_name);
  execute src;
end;
$$;


create or replace function _pg_json_query._col_op_func_drop(
  op_name text,
  _cascade boolean default false
)
returns void language plpgsql volatile as $$
declare
  src text;
begin
  src := _pg_json_query._col_op_func_get_drop_src(op_name, _cascade);
  execute src;
end;
$$;


create or replace function _pg_json_query._register_col_type(type_oid oid)
returns void language plpgsql volatile as $$
declare
  op_name text;
  op text;
  lhs_oid oid;
  rhs_oid oid;
  op_exists boolean;
begin
  for op_name, op, lhs_oid, rhs_oid, op_exists in (
      select * from _pg_json_query._core_op_info(type_oid)) loop
    perform _pg_json_query._op_func_create(
      op_name, op, lhs_oid, rhs_oid, op_exists);
  end loop;
end;
$$;


create or replace function _pg_json_query._unregister_col_type(
  type_oid oid,
  cascade_ boolean default false
) returns void language plpgsql volatile as $$
declare
  op_name text;
  op text;
  lhs_oid oid;
  rhs_oid oid;
  op_exists boolean;
begin
  for op_name, op, lhs_oid, rhs_oid, op_exists in (
      select * from _pg_json_query._core_op_info(type_oid)) loop
    perform _pg_json_query._op_func_drop(
      op_name, op, lhs_oid, rhs_oid, op_exists, cascade_);
  end loop;
end;
$$;


-- Create the _col_op__<op> functions for all of core operators.
do $$
declare
  op_name text;
begin
  for op_name in (select *
                  from jsonb_object_keys(_pg_json_query._core_ops())) loop
    perform _pg_json_query._col_op_func_create(op_name);
  end loop;
end;
$$;


-- NOTE: This function should be invoked for any custom types to be used
-- with json_query.
create or replace function jq_register_column_type(type_name text)
returns boolean language plpgsql volatile as $$
begin
  perform _pg_json_query._register_col_type(type_name::regtype::oid);
  return true;
end;
$$;


create or replace function jq_unregister_column_type(
  type_name text,
  cascade_ boolean default false
) returns boolean language plpgsql volatile as $$
begin
  perform _pg_json_query._unregister_col_type(
    type_name::regtype::oid, cascade_);
  return true;
end;
$$;


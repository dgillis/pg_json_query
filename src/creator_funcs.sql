

create or replace function json_query._validate_attr_name(attrname text)
returns text language plpgsql immutable as $$
begin
  if not (attrname ~ '^[a-zA-Z0-9]+(_?[a-zA-Z0-9]+)*$') then
    raise exception 'Invalid column name for json_query: %', attrname;
  end if;
  return attrname;
end;
$$;


-- Returns a set of records listing the column names data datatypes belonging
-- to the specified table. The column names will not be quoted however columns
-- that require "double quoting" are not allowed by _validate_column_name() and
-- so any columns received from this function should be presumed safe to use as
-- an identifier.
create or replace function json_query._get_type_attrs(full_type_name text)
returns table(attrname text, datatype text)
language sql stable as $$
  with
    type_oid as (select full_type_name::regtype::oid as type_oid),
    type_relid as (
      select typrelid as type_relid
      from pg_type
      where oid = (select type_oid from type_oid)
    )
  select
    json_query._validate_attr_name(attname::text) as attrname,
    atttypid::regtype::text as datatype
  from pg_attribute
  where attrelid = (select type_relid from type_relid) and
        attnum > 0 and
        not attisdropped;
$$;


create or replace function json_query._filter_attr_not_exists_handler(
  full_type_name text,
  attr_name text
) returns boolean
language plpgsql immutable as $$
begin
  raise exception '% has no column "%"', full_type_name, attr_name;
  return false;
end;
$$;

create or replace function json_query._colval_attr_not_exists_handler(
  valtype anyelement,
  full_type_name text,
  attr_name text
) returns anyelement
language plpgsql immutable as $$
begin
  raise exception '% has no column "%"', full_type_name, attr_name;
  return case when false then valtype else null end;
end;
$$;


create or replace function json_query._attr_not_exists_handler(
  ret_type anyelement,
  full_type_name text,
  attr_name text
) returns anyelement language sql immutable as $$
  select null;
$$;


-- Create the function definition for the _filter_row_column_impl()
-- function for the specified type.
create or replace function json_query._get_filter_row_column_impl_defn(
  full_type_name text
) returns text language plpgsql stable as $$
declare
  when_exprs text;
  col_not_exists_expr text;
begin
  when_exprs := concat_ws('', variadic (
    select array_agg(format(
      E'    when %s then json_query._apply_filter(row_.%s, _)\n',
      quote_literal(attrname), quote_ident(attrname)))
    from json_query._get_type_attrs(full_type_name)
  ));
  
  col_not_exists_expr := format(
    'json_query._filter_attr_not_exists_handler(%s, fld)',
    quote_literal(full_type_name)
  );
  
  return concat(
     'create or replace function ',
     'json_query._filter_row_column_impl(',
         E'fld text, row_ ', full_type_name, E', _ anyelement)\n',
      E'returns boolean language sql immutable as $f$\n',
      E'  select case fld\n',
      when_exprs,
      '    else ', col_not_exists_expr, E'\n',
      E'  end;\n',
      E'$f$;\n'
  );
end;
$$;


-- Create the function definition for the _col_value_impl() function for the
-- specified table and output_typ. Note that for the _type passed, there must
-- exist a corresponding implementation of _col_value_cast_defn(expr, _type).
create or replace function json_query._get_col_value_impl_defn(
  full_type_name text,
  to_type_name text
) returns text language plpgsql stable as $$
declare
  attr_exprs text[];
  attr_not_exists_expr text;
begin
  with
    exprs as (
      select
        quote_literal(t.attrname) as attrname_lit,
        'row_.' || quote_ident(t.attrname) as expr,
        t.datatype as from_type_name
      from  json_query._get_type_attrs(full_type_name) t
    ),
    casted_exprs as (
      select
        attrname_lit,
        case
          when from_type_name = to_type_name then
            expr
          else
            format('json_query._cast(%s, null::%s)', expr, to_type_name)
        end as casted_expr
      from exprs
    )
  select array_agg(
    concat('    when ', attrname_lit, ' then ', casted_expr, E'\n')
  ) into attr_exprs
  from casted_exprs;
  
  attr_not_exists_expr := format(
    'json_query._colval_attr_not_exists_handler(null::%s, %s, fld)',
    to_type_name, quote_literal(full_type_name)
  );
  
  return concat(
     'create or replace function _col_val_impl(valtyp ',
       to_type_name, ', row_ ', full_type_name, ', fld text)', E'\n',
     'returns ', to_type_name, ' language sql stable as $f$', E'\n',
     '  select case fld ', E'\n', concat_ws(' ', variadic attr_exprs),
     ' else ', attr_not_exists_expr, E'\n', ' end;', E'\n',
     '$f$;', E'\n'
  );
end;
$$;



create or replace function json_query.register_type(full_type_name text)
returns boolean language plpgsql volatile as $$
declare
  stmt text;
begin
  full_type_name := full_type_name::regtype::text; -- ensure type exists.

  -- Create or replace the filter_row_column_impl() function for the type.
  stmt := json_query._get_filter_row_column_impl_defn(full_type_name);
  execute stmt;
  
  -- Create or replace the filter_row_column_impl() functions for the type.
  stmt := json_query._get_col_value_impl_defn(full_type_name, 'text');
  execute stmt;

  stmt := json_query._get_col_value_impl_defn(full_type_name, 'jsonb');
  execute stmt;

  stmt := json_query._get_col_value_impl_defn(full_type_name, 'json');
  execute stmt;

  return true;
end;
$$;



create or replace function json_query.unregister_type(full_type_name text)
returns boolean language plpgsql volatile as $$
declare
  stmt text;
begin
  full_type_name := full_type_name::regtype::text; -- ensure type exists.

  -- Drop the filter_row_column_impl() function for the type.
  execute format(
    'drop function if exists json_query._filter_row_column_impl(text, %s, anyelement)',
    full_type_name);

  execute format(
    'drop function if exists json_query._col_value_impl(text, %s, text)',
    full_type_name);

  execute format(
    'drop function if exists json_query._col_value_impl(jsonb, %s, text)',
    full_type_name);

  execute format(
    'drop function if exists json_query._col_value_impl(json, %s, text)',
    full_type_name);
  
  return true;
end;
$$;

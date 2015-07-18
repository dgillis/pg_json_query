


create type json_query._field_type as (
  field_arr text[],
  field_path_is_text boolean,
  field_arr_len int
);

create type json_query._op_type as enum(
  'eq',
  'ne',
  'lt',
  'gt',
  'ge', 'gte', 
  'le', 'lte',
  'in',
  'notin',
  'startswith',
  'istartswith',
  'like',
  'ilike',
  'exists',
  'notexists'
);

create type json_query._field_op_type as (
  field_arr text [],
  field_path_is_text boolean,
  field_arr_len int,
  op json_query._op_type
);

create type json_query._filt_type as (
  field_arr text [],
  field_path_is_text boolean,
  field_arr_len int,
  op json_query._op_type,
  value jsonb
);



-- Type initializer functions

/*
json_query._field_type(path_arr, is_text)

json_query._field_type(path_expr) - determines "is_text" based on whether
  "->>" or "->" delimiters are used.
*/
create or replace function json_query._field_type(field_arr text[], is_text boolean,
                                                  field_arr_len int default null)
returns json_query._field_type language sql immutable as $$
  select row(field_arr, is_text,
    case
      when field_arr_len is null
        then array_length(field_arr, 1)
      else
        field_arr_len
      end
  )::json_query._field_type;
$$;

create or replace function json_query._field_type(field_expr text)
returns json_query._field_type language sql immutable as $$
  select case
    when strpos(field_expr, '->') = 0 then
      json_query._field_type(array[field_expr], false)
    when strpos(field_expr, '->>') > 0 then
      json_query._field_type(regexp_split_to_array(field_expr, '->>?'), true, null)
    else
      json_query._field_type(regexp_split_to_array(field_expr, '->'), false, null)
    end;
$$;


/*
json_query._field_op_type(field, op)

json_query._field_op_type(path_expr, op) - Constructs the field and determines
  "is_text" based on "->"/"->>".
*/
create or replace function json_query._field_op_type(field json_query._field_type,
                                                     op json_query._op_type)
returns json_query._field_op_type language sql immutable as $$
  select row(field.field_arr, field.field_path_is_text, field.field_arr_len, op)::json_query._field_op_type;
$$;

create or replace function json_query._field_op_type(field_expr text, op json_query._op_type)
returns json_query._field_op_type language sql immutable as $$
  select json_query._field_op_type(json_query._field_type(field_expr), op);
$$;

create or replace function json_query._field_op_type(field_op_expr text)
returns json_query._field_op_type language plpgsql immutable as $$
declare
  parts text[];
  parts_len int;
begin
  parts := regexp_split_to_array(field_op_expr, '__');
  parts_len := array_length(parts, 1);
  
  if parts_len = 1 then
    return json_query._field_op_type(field_op_expr, 'eq');
  elsif parts_len = 2 then
    return json_query._field_op_type(parts[1], parts[2]::json_query._op_type);
  else
    return json_query._field_op_type(
      ws_concat('__', variadic parts[1:parts_len]),
      parts[parts_len]
    );
  end if;
end;
$$;



/*
json_query._filt_type(field_op, value)

json_query._filt_type(path_expr, op, value)

json_query._filt_type(field_op_expr, value)
*/
create or replace function json_query._filt_type(field_op json_query._field_op_type, value jsonb)
returns json_query._filt_type language sql immutable as $$
  select row(field_op.field_arr, field_op.field_path_is_text, field_op.field_arr_len,
             field_op.op, value)::json_query._filt_type;
$$;

create or replace function json_query._filt_type(field text, op json_query._op_type, value jsonb)
returns json_query._filt_type language sql immutable as $$
  select json_query._filt_type(json_query._field_op_type(field, op), value);
$$;

create or replace function json_query._filt_type(field_op_expr text, value jsonb)
returns json_query._filt_type language sql immutable as $$
  select json_query._filt_type(json_query._field_op_type(field_op_expr), value);
$$;



-- Methods for field-like types.
create or replace function json_query._get_column(fld anyelement)
returns text language sql immutable as $$ select (fld.field_arr)[1]; $$;

create or replace function json_query._get_path_length(fld anyelement)
returns int language sql immutable as $$ select fld.field_arr_len - 1; $$;

create or replace function json_query._get_field_column(fld anyelement)
returns text language sql immutable as $$
  select fld.field_arr[1];
$$;

create or replace function json_query._get_path_array(fld anyelement)
returns text[] language sql immutable as $$
  select case
    when fld.field_arr_len > 1 then
      (fld.field_arr)[2:fld.field_arr_len]
    else
      null
    end;
$$;

-- Returns NULL if there is no path array, a JSONB string of the first element
-- if the path array has length = 1 otherwise a JSONB array containing the path
-- elements as JSONB strings.
create or replace function json_query._get_path_json(fld anyelement)
returns jsonb language sql immutable as $$
  select case fld.field_arr_len
    when 1 then null::jsonb
    when 2 then to_json((fld.field_arr)[2])::jsonb
    else to_json((fld.field_arr)[2:fld.field_arr_len])::jsonb
    end;
$$;




create or replace function json_query._filt_to_json(filt json_query._filt_type)
returns jsonb language sql immutable as $$
   select json_build_object(
      'field', json_query._get_column(filt),
      'op', filt.op,
      'value', filt.value,
      'path', json_query._get_path_json(filt),
      'path_is_text', filt.field_path_is_text
    )::jsonb;
$$;

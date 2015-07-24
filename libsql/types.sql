  
  
create type _pg_json_query._field_type as (
  column_ text,
  path_arr text[],
  path_is_text boolean,
  path_arr_len int
);


create or replace function _pg_json_query._validate_op(
  op text,
  _ops jsonb default '{
    "eq": true,
    "ne": true,
    "lt": true,
    "gt": true,
    "ge": true, "gte": true,
    "le": true, "lte": true,
    "in": true,
    "notin": true,
    "startswith": true,
    "istartswith": true,
    "endswith": true,
    "iendswith": true,
    "like": true,
    "ilike": true,
    "exists": true,
    "notexists": true,
    "contains": true,
    "notcontains": true,
    "contained": true,
    "notcontained": true
  }')
returns text language plpgsql immutable as $$
begin
  if not (_ops ? op) then
    raise exception 'Invalid json_query op: %', op;
  end if;
  return op;
end;
$$;


create type _pg_json_query._field_op_type as (
  column_ text,
  path_arr text [],
  path_is_text boolean,
  path_arr_len int,
  op text
);

create type _pg_json_query._filt_type as (
  column_ text,
  path_arr text [],
  path_is_text boolean,
  path_arr_len int,
  op text,
  value jsonb
);



-- Type initializer functions

/*
_pg_json_query._field_type(path_arr, is_text)

_pg_json_query._field_type(path_expr) - determines "is_text" based on whether
  "->>" or "->" delimiters are used.
*/
create function _pg_json_query._field_type(column_ text,
                                           path_arr text[],
                                           path_is_text boolean,
                                           path_arr_len int default null)
returns _pg_json_query._field_type language sql immutable as $$
  select row(column_, path_arr, path_is_text,
    case
      when path_arr_len is null
        then
          case
            when path_arr is null then 0
            else array_length(path_arr, 1)
          end
      else
        path_arr_len
      end
  )::_pg_json_query._field_type;
$$;


create function _pg_json_query._unsplit_field_expr_arr(arr text[], path_is_text boolean, arrlen int)
returns _pg_json_query._field_type language sql immutable as $$
  select _pg_json_query._field_type(
    arr[1],
    case when arrlen > 1 then arr[2:arrlen] else null end,
    path_is_text,
    arrlen - 1
  );
$$;

create function _pg_json_query._unsplit_field_expr_arr(arr text[], path_is_text boolean)
returns _pg_json_query._field_type language sql immutable as $$
  select _pg_json_query._unsplit_field_expr_arr(arr, path_is_text, array_length(arr, 1));
$$;


create function _pg_json_query._field_type(field_expr text)
returns _pg_json_query._field_type language sql immutable as $$
  select case
    when strpos(field_expr, '->') = 0 then
      _pg_json_query._field_type(field_expr, null, false, 0)
    when strpos(field_expr, '->>') > 0 then
      _pg_json_query._unsplit_field_expr_arr(regexp_split_to_array(field_expr, '->>?'), true)
    else
      _pg_json_query._unsplit_field_expr_arr(regexp_split_to_array(field_expr, '->>?'), false)
    end;
$$;


/*
_pg_json_query._field_op_type(field, op)

_pg_json_query._field_op_type(path_expr, op) - Constructs the field and determines
  "is_text" based on "->"/"->>".
*/
create function _pg_json_query._field_op_type(field _pg_json_query._field_type,
                                              op text)
returns _pg_json_query._field_op_type language sql immutable as $$
  select row(
    field.column_,
    field.path_arr,
    field.path_is_text,
    field.path_arr_len,
    _pg_json_query._validate_op(op)
  )::_pg_json_query._field_op_type;
$$;

create function _pg_json_query._field_op_type(field_expr text, op text)
returns _pg_json_query._field_op_type language sql immutable as $$
  select _pg_json_query._field_op_type(_pg_json_query._field_type(field_expr), op);
$$;

create function _pg_json_query._field_op_type(field_op_expr text)
returns _pg_json_query._field_op_type language plpgsql immutable as $$
declare
  parts text[];
  parts_len int;
begin
  parts := regexp_split_to_array(field_op_expr, '__');
  parts_len := array_length(parts, 1);
  
  if parts_len = 1 then
    return _pg_json_query._field_op_type(field_op_expr, 'eq');
  elsif parts_len = 2 then
    return _pg_json_query._field_op_type(parts[1], parts[2]);
  else
    return _pg_json_query._field_op_type(
      ws_concat('__', variadic parts[1:parts_len]),
      parts[parts_len]
    );
  end if;
end;
$$;



/*
_pg_json_query._filt_type(field_op, value)

_pg_json_query._filt_type(path_expr, op, value)

_pg_json_query._filt_type(field_op_expr, value)
*/
create function _pg_json_query._filt_type(field_op _pg_json_query._field_op_type, value jsonb)
returns _pg_json_query._filt_type language sql immutable as $$
  select row(field_op.column_, field_op.path_arr, field_op.path_is_text,
             field_op.path_arr_len, field_op.op, value)::_pg_json_query._filt_type;
$$;

create function _pg_json_query._filt_type(field text, op text, value jsonb)
returns _pg_json_query._filt_type language sql immutable as $$
  select _pg_json_query._filt_type(
    _pg_json_query._field_op_type(field, op),
    value
  );
$$;


create function _pg_json_query._filt_type(field_op_expr text, value jsonb)
returns _pg_json_query._filt_type language sql immutable as $$
  select _pg_json_query._filt_type(_pg_json_query._field_op_type(field_op_expr), value);
$$;



-- Methods for field-like types.
create function _pg_json_query._get_column(fld anyelement)
returns text language sql immutable as $$ select (fld.field_arr)[1]; $$;

create function _pg_json_query._get_path_length(fld anyelement)
returns int language sql immutable as $$ select fld.field_arr_len - 1; $$;

create function _pg_json_query._get_field_column(fld anyelement)
returns text language sql immutable as $$
  select fld.field_arr[1];
$$;

-- Returns NULL if there is no path array, a JSONB string of the first element
-- if the path array has length = 1 otherwise a JSONB array containing the path
-- elements as JSONB strings.
create function _pg_json_query._get_path_json(fld anyelement)
returns jsonb language sql immutable as $$
  select case fld.path_arr_len
    when 0 then null::jsonb
    when 1 then to_json((fld.path_arr)[1])::jsonb
    else to_json((fld.path_arr)[1:fld.path_arr_len])::jsonb
    end;
$$;




create function _pg_json_query._filt_to_json(filt _pg_json_query._filt_type)
returns jsonb language sql immutable as $$
   select json_build_object(
      'field', filt.column_,
      'op', filt.op,
      'value', filt.value,
      'path', _pg_json_query._get_path_json(filt),
      'path_is_text', filt.path_is_text
    )::jsonb;
$$;



create function _pg_json_query._field_extract_from_column(fld anyelement, col jsonb)
returns jsonb language sql immutable as $$
  select _pg_json_query._column_extract_path(col, fld.path_arr);
$$;


create function _pg_json_query._field_extract_from_column(fld anyelement, col json)
returns json language sql immutable as $$
  select _pg_json_query._column_extract_path(col, fld.path_arr);
$$;


create function _pg_json_query._field_extract_text_from_column(fld anyelement, col jsonb)
returns text language sql immutable as $$
  select _pg_json_query._column_extract_path_text(col, fld.path_arr);
$$;


create function _pg_json_query._field_extract_text_from_column(fld anyelement, col json)
returns text language sql immutable as $$
  select _pg_json_query._column_extract_path_text(col, fld.path_arr);
$$;



create function _pg_json_query._field_extract_from_column(fld_expr text, col jsonb)
returns jsonb language sql immutable as $$
  select _pg_json_query._field_extract_from_column(_pg_json_query._field_type(fld_expr), col);
$$;

create function _pg_json_query._field_extract_from_column(fld_expr text, col json)
returns json language sql immutable as $$
  select _pg_json_query._field_extract_from_column(_pg_json_query._field_type(fld_expr), col);
$$;

create function _pg_json_query._field_extract_text_from_column(fld_expr text, col jsonb)
returns text language sql immutable as $$
  select _pg_json_query._field_extract_text_from_column(_pg_json_query._field_type(fld_expr), col);
$$;

create function _pg_json_query._field_extract_text_from_column(fld_expr text, col json)
returns text language sql immutable as $$
  select _pg_json_query._field_extract_text_from_column(_pg_json_query._field_type(fld_expr), col);
$$;

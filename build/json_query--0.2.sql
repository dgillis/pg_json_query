
create schema _pg_json_query;

create function _pg_json_query._jsonb_arr_to_text_arr(arr jsonb)
returns text[] language sql immutable
as $$
  select case jsonb_array_length(arr)
    when 0 then array[]::text[]
    when 1 then array[arr->>0]
    when 2 then array[arr->>0, arr->>1]
    when 3 then array[arr->>0, arr->>1, arr->>2]
    when 4 then array[arr->>0, arr->>1, arr->>2, arr->>3]
    when 5 then array[arr->>0, arr->>1, arr->>2, arr->>3, arr->>4]
    when 6 then array[arr->>0, arr->>1, arr->>2, arr->>3, arr->>4, arr->>5]
    when 7 then array[arr->>0, arr->>1, arr->>2, arr->>3, arr->>4, arr->>5, arr->>6]
    else
      (select array_agg(e) from jsonb_array_elements_text(arr) e)
  end::text[];
$$;


-- Concating elements like this is faster then the built in json_build_array.
create function _pg_json_query._build_array(variadic elems jsonb[] default array[]::jsonb[])
returns jsonb
language sql immutable
as $$
  select ('[' || concat_ws(', ', variadic elems) || ']')::jsonb;
$$;



create function _pg_json_query._json_string_to_text(s jsonb)
returns text
language sql immutable
as $$
  select ('[' || s || ']')::jsonb->>0;
$$;



-- Concat the two JSONB arrays to form a new one. If either element
-- is a non-array, it will be treated as a single element array of
-- that element.
create function _pg_json_query._jsonb_array_concat(e1 jsonb, e2 jsonb)
returns jsonb
language sql immutable strict
as $$
  select case
    when jsonb_typeof(e1) = 'array' then
      case jsonb_typeof(e2)
        when 'array' then
          -- Both are arrays.
          case
            when e1 = '[]' then e2
            when e2 = '[]' then e1
            else
              -- Both non-empty arrays.
              (left(e1::text, -1) || ', ' || right(e2::text, -1))::jsonb
            end
          -- concat the two arrays
        else
          -- e1 is an array, e2 is not.
          case
            when e1 = '[]' then ('[' || e2 || ']')::jsonb
            else
              -- e1 non-empty.
              (left(e1::text, -1) || ', ' || e2 || ']')::jsonb
            end
        end
    when jsonb_typeof(e2) = 'array' then
      -- e2 is an array, e1 is not.
      case
        when e2 = '[]' then ('[' || e1 || ']')::jsonb
        else
          -- e2 non-empty.
          ('[' || e1 || ', ' || right(e2::text, -1))::jsonb
        end
    else
      -- Neither are arrays
      ('[' || e1 || ', ' || e2 || ']')::jsonb
    end;
$$;



create function _pg_json_query._to_text(val jsonb)
returns text
language sql immutable
as $$
  select case
    when jsonb_typeof(val) = 'string' then
      _pg_json_query._json_string_to_text(val)
    else
      val::text
  end;
$$;


create function _pg_json_query._force_text(val anyelement,
                                                 -- The following defaults are just to cache
                                                 -- the type identifiers and shouldn't be
                                                 -- regarded as params.
                                                 json_rtyp regtype default 'json'::regtype,
                                                 jsonb_rtyp regtype default 'jsonb'::regtype)
returns text
language sql immutable
as $$
  select case pg_typeof(val)
    when jsonb_rtyp then
      _pg_json_query._json_string_to_text(val::text::jsonb)
    when json_rtyp then
      _pg_json_query._json_string_to_text(val::text::jsonb)
    else
      val::text
    end;
$$;


-- Test whether the string appears to be a valid JSON array or string.
-- This is intended to be fast rather than exact and will return true
-- for some string where s:json would actually raise an exception.
create function _pg_json_query._looks_like_json_string_or_array(s text)
returns boolean
language sql immutable as $$
  select length(s) > 1 and case left(s, 1)
    when '[' then
      right(s, 1) = ']'
    when '"' then
      right(s, 1) = '"'
    else
      false
    end;
$$;




create function _pg_json_query._col_in_jsonb_arr(
  col anyelement,
  arr jsonb,
  _coltype anyelement default null
)
returns boolean
language sql immutable
as $$
  -- NOTE: We only allow arrays of up to length 20 for the "in"
  -- operator. This could be enlarged but it must be a preset
  -- limit otherwise inlining won't work.
  select case jsonb_array_length(arr)
    when 0 then
      false
    when 1 then
      col = _pg_json_query._cast_column_value(_coltype, arr->0)
    when 2 then
      col = _pg_json_query._cast_column_value(_coltype, arr->0) or
      col = _pg_json_query._cast_column_value(_coltype, arr->1)
    when 3 then
      col = _pg_json_query._cast_column_value(_coltype, arr->0) or
      col = _pg_json_query._cast_column_value(_coltype, arr->1) or
      col = _pg_json_query._cast_column_value(_coltype, arr->2)
    when 4 then
      col = _pg_json_query._cast_column_value(_coltype, arr->0) or
      col = _pg_json_query._cast_column_value(_coltype, arr->1) or
      col = _pg_json_query._cast_column_value(_coltype, arr->2) or
      col = _pg_json_query._cast_column_value(_coltype, arr->3)
    when 5 then
      col = _pg_json_query._cast_column_value(_coltype, arr->0) or
      col = _pg_json_query._cast_column_value(_coltype, arr->1) or
      col = _pg_json_query._cast_column_value(_coltype, arr->2) or
      col = _pg_json_query._cast_column_value(_coltype, arr->3) or
      col = _pg_json_query._cast_column_value(_coltype, arr->4)
    when 6 then
      col = _pg_json_query._cast_column_value(_coltype, arr->0) or
      col = _pg_json_query._cast_column_value(_coltype, arr->1) or
      col = _pg_json_query._cast_column_value(_coltype, arr->2) or
      col = _pg_json_query._cast_column_value(_coltype, arr->3) or
      col = _pg_json_query._cast_column_value(_coltype, arr->4) or
      col = _pg_json_query._cast_column_value(_coltype, arr->5)
    else
      col = _pg_json_query._cast_column_value(_coltype, arr->0) or
      col = _pg_json_query._cast_column_value(_coltype, arr->1) or
      col = _pg_json_query._cast_column_value(_coltype, arr->2) or
      col = _pg_json_query._cast_column_value(_coltype, arr->3) or
      col = _pg_json_query._cast_column_value(_coltype, arr->4) or
      col = _pg_json_query._cast_column_value(_coltype, arr->5) or
      col = _pg_json_query._cast_column_value(_coltype, arr->6) or
      col = _pg_json_query._cast_column_value(_coltype, arr->7) or
      col = _pg_json_query._cast_column_value(_coltype, arr->8) or
      col = _pg_json_query._cast_column_value(_coltype, arr->9) or
      col = _pg_json_query._cast_column_value(_coltype, arr->10) or
      col = _pg_json_query._cast_column_value(_coltype, arr->11) or
      col = _pg_json_query._cast_column_value(_coltype, arr->12) or
      col = _pg_json_query._cast_column_value(_coltype, arr->13) or
      col = _pg_json_query._cast_column_value(_coltype, arr->14) or
      col = _pg_json_query._cast_column_value(_coltype, arr->15) or
      col = _pg_json_query._cast_column_value(_coltype, arr->16) or
      col = _pg_json_query._cast_column_value(_coltype, arr->17) or
      col = _pg_json_query._cast_column_value(_coltype, arr->18) or
      col = _pg_json_query._cast_column_value(_coltype, arr->19)
    end;
$$;


create function _pg_json_query._col_in_jsonb(
  col anyelement,
  arr_or_obj jsonb
)
returns boolean
language sql immutable
as $$
  select case jsonb_typeof(arr_or_obj)
    when 'object' then
      arr_or_obj ? _pg_json_query._force_text(col)
    when 'array' then
      _pg_json_query._col_in_jsonb_arr(col, arr_or_obj)
    else
      false
    end;
$$;



-- Helper methods for like/startswith/ilike.
create function _pg_json_query._like_helper(col text, pattern text)
returns boolean language sql immutable as $$
  select col like pattern;
$$;

create function _pg_json_query._like_helper(col jsonb, pattern text)
returns boolean language sql immutable as $$
  select _pg_json_query._json_string_to_text(col) like pattern;
$$;

create function _pg_json_query._like_helper(col json, pattern text)
returns boolean language sql immutable as $$
  select _pg_json_query._json_string_to_text(col::jsonb) like pattern;
$$;

create function _pg_json_query._ilike_helper(col text, pattern text)
returns boolean language sql immutable as $$
  select col ilike pattern;
$$;

create function _pg_json_query._ilike_helper(col jsonb, pattern text)
returns boolean language sql immutable as $$
  select _pg_json_query._json_string_to_text(col) ilike pattern;
$$;

create function _pg_json_query._ilike_helper(col json, pattern text)
returns boolean language sql immutable as $$
  select _pg_json_query._json_string_to_text(col::jsonb) ilike pattern;
$$;



-- Fallback implementations _like_helper/_ilike_helper for non string/json
-- types.
create function _pg_json_query._like_helper(col anyelement, pattern text)
returns boolean language sql immutable as $$
  select col::text like pattern;
$$;

create function _pg_json_query._ilike_helper(col anyelement, pattern text)
returns boolean language sql immutable as $$
  select col::text ilike pattern;
$$;


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


create function _pg_json_query._to_numeric(val jsonb)
returns numeric
language sql immutable
as $$
  select case jsonb_typeof(val)
    when 'string' then _pg_json_query._json_string_to_text(val)::numeric
    else val::text::numeric
  end;
$$;

create function _pg_json_query._to_bool(val jsonb)
returns boolean
language sql immutable
as $$
  select case val
    when 'true' then true
    when 'false' then false
    else _pg_json_query._to_text(val)::boolean
  end;
$$;

create function _pg_json_query._to_date(val jsonb)
returns date
language sql immutable
as $$
  select _pg_json_query._to_text(val)::date;
$$;

create function _pg_json_query._to_timestamp(val jsonb)
returns timestamp
language sql immutable
as $$
  select _pg_json_query._to_text(val)::timestamp;
$$;

create function _pg_json_query._to_timestamptz(val jsonb)
returns timestamptz
language sql immutable
as $$
  select _pg_json_query._to_text(val)::timestamptz;
$$;


create function _pg_json_query._cast_column_value(col jsonb, val jsonb)
returns jsonb
language sql immutable
as $$
  select val;
$$;


create function _pg_json_query._cast_column_value(col integer, val jsonb)
returns integer
language sql immutable
as $f$
  select _pg_json_query._to_numeric(val)::integer;
$f$;

create function _pg_json_query._cast_column_value(col bigint, val jsonb)
returns bigint
language sql immutable
as $f$
  select _pg_json_query._to_numeric(val)::bigint;
$f$;

create function _pg_json_query._cast_column_value(col numeric, val jsonb)
returns numeric
language sql immutable
as $f$
  select _pg_json_query._to_numeric(val);
$f$;

create function _pg_json_query._cast_column_value(col date, val jsonb)
returns date
language sql immutable
as $f$
  select _pg_json_query._to_date(val);
$f$;

create function _pg_json_query._cast_column_value(col timestamp, val jsonb)
returns timestamp
language sql immutable
as $f$
  select _pg_json_query._to_timestamp(val);
$f$;

create function _pg_json_query._cast_column_value(col timestamptz, val jsonb)
returns timestamptz
language sql immutable
as $f$
  select _pg_json_query._to_timestamptz(val);
$f$;

create function _pg_json_query._cast_column_value(col boolean, val jsonb)
returns boolean
language sql immutable
as $f$
  select _pg_json_query._to_bool(val);
$f$;

create function _pg_json_query._cast_column_value(col text, val jsonb)
returns text
language sql immutable
as $f$
  select _pg_json_query._to_text(val);
$f$;

create function _pg_json_query._cast_column_value(col float8, val jsonb)
returns float8
language sql immutable
as $f$
  select _pg_json_query._to_numeric(val)::float8;
$f$;



create function _pg_json_query._column_extract_path(col jsonb, path_ text)
returns jsonb language sql immutable as $$ select col->path_; $$;

create function _pg_json_query._column_extract_path_text(col jsonb, path_ text)
returns text language sql immutable as $$ select col->>path_; $$;

create function _pg_json_query._column_extract_path(col jsonb, path_ text[])
returns jsonb language sql immutable as $$ select col#>path_; $$;

create function _pg_json_query._column_extract_path_text(col jsonb, path_ text[])
returns text language sql immutable as $$ select col#>>path_; $$;

create function _pg_json_query._column_extract_path(col jsonb, path_ jsonb)
returns jsonb language sql immutable as $$
  select case jsonb_typeof(path_)
    when 'array' then
      _pg_json_query._column_extract_path(col, _pg_json_query._jsonb_arr_to_text_arr(path_))
    else
      _pg_json_query._column_extract_path(col, _pg_json_query._json_string_to_text(path_))
    end;
$$;

create function _pg_json_query._column_extract_path_text(col jsonb, path_ jsonb)
returns text language sql immutable as $$
  select case jsonb_typeof(path_)
    when 'array' then
      _pg_json_query._column_extract_path_text(col, _pg_json_query._jsonb_arr_to_text_arr(path_))
    else
      _pg_json_query._column_extract_path_text(col, _pg_json_query._json_string_to_text(path_))
    end;
$$;

create function _pg_json_query._column_extract_path(col json, path_ anyelement)
returns json language sql immutable as $$
  select _pg_json_query._column_extract_path(col::jsonb, path_)::json;
$$;

create function _pg_json_query._column_extract_path_text(col json, path_ anyelement)
returns text language sql immutable as $$
  select _pg_json_query._column_extract_path(col::jsonb, path_)::text;
$$;

-- Casting anyelement to its own type just returns the original element.
create function _pg_json_query._cast(x anyelement, to_type anyelement)
returns anyelement language sql immutable as $$ select x; $$;


/* to text */

-- * -> text
create function _pg_json_query._cast(x anyelement, to_type text)
returns text language sql immutable as $$;
  select x::text;
$$;

-- text -> text (override * -> text)
create function _pg_json_query._cast(x text, to_type text)
returns text language sql immutable as $$;
  select x;
$$;



/* to json/jsonb */

-- * -> json
create function _pg_json_query._cast(x anyelement, to_type json)
returns json language sql immutable as $$;
  select to_json(x);
$$;

-- * -> jsonb
create function _pg_json_query._cast(x anyelement, to_type jsonb)
returns jsonb language sql immutable as $$;
  select to_json(x)::jsonb;
$$;

-- json -> json (overide anyelement to json).
create function _pg_json_query._cast(x json, to_type json)
returns json language sql immutable as $$ select x; $$;

-- jsonb -> jsonb (overide anyelement to jsonb).
create function _pg_json_query._cast(x jsonb, to_type jsonb)
returns jsonb language sql immutable as $$ select x; $$;

-- jsonb -> json
create function _pg_json_query._cast(x jsonb, to_type json)
returns json language sql immutable as $$ select x::json; $$;

-- json -> jsonb
create function _pg_json_query._cast(x json, to_type jsonb)
returns jsonb language sql immutable as $$ select x::jsonb; $$;

-- eq
create function _pg_json_query._eq(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select case when filt->'value' = 'null' then
    col is null
  else
    col = _pg_json_query._cast_column_value(_coltyp, filt->'value')
  end;
$$;


-- ne
create function _pg_json_query._ne(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select case when filt->'value' = 'null' then
    col is not null
  else
    col != _pg_json_query._cast_column_value(_coltyp, filt->'value')
  end;
$$;


-- gt
create function _pg_json_query._gt(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select col > _pg_json_query._cast_column_value(_coltyp, filt->'value');
$$;


-- lt
create function _pg_json_query._lt(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select col < _pg_json_query._cast_column_value(_coltyp, filt->'value');
$$;


-- ge
create function _pg_json_query._ge(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select col >= _pg_json_query._cast_column_value(_coltyp, filt->'value');
$$;


-- le
create function _pg_json_query._le(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select col <= _pg_json_query._cast_column_value(_coltyp, filt->'value');
$$;


-- in (jsonb)
create function _pg_json_query._in(col jsonb, filt jsonb, _coltyp jsonb default null)
returns boolean language sql immutable as $$
  select _pg_json_query._col_in_jsonb(col, filt->'value');
$$;

-- in (anyelement)
create function _pg_json_query._in(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select _pg_json_query._col_in_jsonb(to_json(col)::jsonb, filt->'value');
$$;


-- notin
create function _pg_json_query._notin(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select not _pg_json_query._in(col, filt, _coltyp);
$$;


-- like
create function _pg_json_query._like(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$ select _pg_json_query._like_helper(col, filt->>'value'); $$;


-- ilike
create function _pg_json_query._ilike(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$ select _pg_json_query._ilike_helper(col, filt->>'value'); $$;


-- startswith
create function _pg_json_query._startswith(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select _pg_json_query._like_helper(col, (filt->>'value') || '%');
$$;


-- istartswith
create function _pg_json_query._istartswith(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select _pg_json_query._ilike_helper(col, (filt->>'value') || '%');
$$;



-- endswith
create function _pg_json_query._endswith(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select _pg_json_query._like_helper(col, '%' || (filt->>'value'));
$$;


-- iendswith
create function _pg_json_query._iendswith(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select _pg_json_query._ilike_helper(col, '%' || (filt->>'value'));
$$;



-- exists
create function _pg_json_query._exists(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select col ? (filt->>'value');
$$;


-- notexists.
create function _pg_json_query._notexists(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select not _pg_json_query._exists(col, filt, _coltyp);
$$;



-- contains (i.e., "@>" gin operator)

-- Fallback implementation: attempt to cast the textual representation of the value to whatever
-- the column's type is.
create function _pg_json_query._contains(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select col @> _pg_json_query._cast(filt->>'value', _coltyp);
$$;

-- jsonb implementation.
create function _pg_json_query._contains(col jsonb, filt jsonb, _coltyp jsonb default null)
returns boolean language sql immutable as $$ select col @> (filt->'value'); $$;

-- json implementation
create function _pg_json_query._contains(col json, filt jsonb, _coltyp json default null)
returns boolean language sql immutable as $$ select col::jsonb @> (filt->'value'); $$;


-- contained ("<@"). Functions analoguous to the contains functions.
create function _pg_json_query._contained(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select col <@ _pg_json_query._cast(filt->>'value', _coltyp);
$$;

-- jsonb implementation.
create function _pg_json_query._contained(col jsonb, filt jsonb, _coltyp jsonb default null)
returns boolean language sql immutable as $$ select col <@ (filt->'value') $$;

-- json implementation
create function _pg_json_query._contained(col json, filt jsonb, _coltyp json default null)
returns boolean language sql immutable as $$ select col::jsonb <@ (filt->'value') $$;


-- not contains
create function _pg_json_query._notcontains(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select not _pg_json_query._contains(col, filt, _coltyp);
$$;


-- not contained
create function _pg_json_query._notcontained(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select not _pg_json_query._contained(col, filt, _coltyp);
$$;




create function _pg_json_query._apply_op(op text, col anyelement, filt jsonb)
returns boolean language sql immutable as $$
  select case op
    when 'eq' then _pg_json_query._eq(col, filt)
    when 'ne' then _pg_json_query._ne(col, filt)
    when 'gt' then _pg_json_query._gt(col, filt)
    when 'lt' then _pg_json_query._lt(col, filt)
    when 'ge' then _pg_json_query._ge(col, filt)
    when 'le' then _pg_json_query._le(col, filt)
    when 'in' then _pg_json_query._in(col, filt)
    when 'notin' then _pg_json_query._notin(col, filt)
    when 'like' then _pg_json_query._like(col, filt)
    when 'ilike' then _pg_json_query._ilike(col, filt)
    when 'startswith' then _pg_json_query._startswith(col, filt)
    when 'istartswith' then _pg_json_query._istartswith(col, filt)
    when 'endswith' then _pg_json_query._endswith(col, filt)
    when 'iendswith' then _pg_json_query._iendswith(col, filt)
    when 'exists' then _pg_json_query._exists(col, filt)
    when 'notexists' then _pg_json_query._notexists(col, filt)
    when 'contains' then _pg_json_query._contains(col, filt)
    when 'notcontains' then _pg_json_query._notcontains(col, filt)
    when 'contained' then _pg_json_query._contained(col, filt)
    when 'notcontained' then _pg_json_query._notcontained(col, filt)
    -- Aliases
    when 'gte' then _pg_json_query._ge(col, filt)
    when 'lte' then _pg_json_query._le(col, filt)
    else null
    end;
$$;

-- General form for _apply_filter(). Type-specific implementations omit the
-- third argument from their call signature so that they can make a call to
-- this method after doing any type-specific preprocessing (e.g., see the json
-- implementation).
create function _pg_json_query._apply_filter(
  col anyelement,
  filt jsonb,
  _coltyp anyelement default null
) returns boolean language sql immutable
as $$ select _pg_json_query._apply_op(filt->>'op', col, filt); $$;


-- JSONB implementation of _apply_filter(). Calls the general form after
-- evaluating any path included in the filter.
create function _pg_json_query._apply_filter(col jsonb, filt jsonb)
returns boolean
language sql immutable
as $$
  select case
    when filt->'path' = 'null' then
      _pg_json_query._apply_filter(col, filt, null::jsonb)
    when filt->'path_is_text' = 'true' then
      _pg_json_query._apply_filter(
        _pg_json_query._column_extract_path_text(col, filt->'path'),
        filt,
        null
      )
    else
      _pg_json_query._apply_filter(
        _pg_json_query._column_extract_path(col, filt->'path'),
        filt,
        null
      )
    end;
$$;


-- JSON implementation of _apply_filter(). Similar to the JSONB one.
create function _pg_json_query._apply_filter(col json, filt jsonb)
returns boolean
language sql immutable as $$
  select _pg_json_query._apply_filter(col::jsonb, filt);
$$;


-- Call _filter_row_column_impl() if filt is non-null, otherwise return true.
-- Note that this functions depends on the existence of an implementation of
-- _filter_row_column_impl() for the given row type.
create function _pg_json_query._filter_row_column(
  row_ anyelement,
  filt jsonb
) returns boolean language sql immutable as $$
  select
    case
      when filt is null then
        true
      else
        _pg_json_query._filter_row_column_impl(filt->>'field', row_, filt)
    end;
$$;


-- Returns true if every filter object in the JSONB-array of filters is true
-- or false otherwise. Any elements beyond the maximum allowed (currently, 12)
-- will be ignored.
create function _pg_json_query._filter_row_impl(
  row_ anyelement,
  filts jsonb
) returns boolean
language sql immutable
as $$
  select
    _pg_json_query._filter_row_column(row_, filts->0) and
    _pg_json_query._filter_row_column(row_, filts->1) and
    _pg_json_query._filter_row_column(row_, filts->2) and
    _pg_json_query._filter_row_column(row_, filts->3) and
    _pg_json_query._filter_row_column(row_, filts->4) and
    _pg_json_query._filter_row_column(row_, filts->5) and
    _pg_json_query._filter_row_column(row_, filts->6) and
    _pg_json_query._filter_row_column(row_, filts->7) and
    _pg_json_query._filter_row_column(row_, filts->8) and
    _pg_json_query._filter_row_column(row_, filts->9) and
    _pg_json_query._filter_row_column(row_, filts->10) and
    _pg_json_query._filter_row_column(row_, filts->11);
$$;


-- Convert one of the filter objects from the user input format into a JSONB
-- array of _filter_type-like JSONB objects.
create function _pg_json_query._parse_filter_obj_to_json(obj jsonb)
returns jsonb language plpgsql immutable as $$
declare
  and_arr jsonb;
  dj_arr jsonb;
  expl_filt jsonb;
  arr jsonb;
begin
  if obj ? '$and' then
    select json_agg(o)::jsonb into and_arr
    from (
      select jsonb_array_elements(
        _pg_json_query._parse_filter_obj_to_json(o)
      ) as o
      from jsonb_array_elements(obj->'$and') o
    ) sq;

    arr := and_arr;
  end if;

  select json_agg(
    _pg_json_query._filt_to_json(_pg_json_query._filt_type(key, value))
  ) into dj_arr
  from jsonb_each(obj)
  where left(key, 1) != '$';

  if dj_arr is not null then
     arr := case
       when arr is null then
         dj_arr
       else
         _pg_json_query._jsonb_array_concat(arr, dj_arr)
       end;
  end if;

  if obj ? '$field' or obj ? '$op' or obj ? '$value' then
    expl_filt := _pg_json_query._filt_to_json(_pg_json_query._filt_type(
      obj->>'$field',
      obj->>'$op',
      obj->'$value'
    ));

    arr := case
      when arr is null then
        _pg_json_query._build_array(expl_filt)
      else
        _pg_json_query._jsonb_array_concat(arr, expl_filt)
      end;
  end if;

  return coalesce(arr, '[]');
end;
$$;


-- Wrapper around parse_parse_filter_obj_to_json().
create function _pg_json_query._parse_filter(typ jsonb, obj jsonb)
returns jsonb language sql immutable as $$
  select _pg_json_query._parse_filter_obj_to_json(obj);
$$;


-- Filter function available to all tables which implement
-- _filter_row_column_impl() for their row-type.
create function jq_filter(row_ anyelement, filter_obj jsonb)
returns boolean
language sql immutable
as $$
  select _pg_json_query._filter_row_impl(
    row_,
    _pg_json_query._parse_filter_obj_to_json(filter_obj)
  );
$$;


create function jq_and_filters(f1 jsonb, f2 jsonb)
returns jsonb language sql immutable as $$
  select
    case
      when f1 is null or f1 = '{}' then
        coalesce(f2, '{}')
      when f2 is null or f2 = '{}' then
        f1
      when f1 = f2 then
        f1
      else
        concat('{"$and": [', f1, ', ', f2, ']}')::jsonb
    end;
$$;
/*
_col_value(valtyp, row, fld)

Return either text/jsonb (depending on valtyp) value representing the row's
value for the specified field.

**** Requires implementation to be used with specific row types.
*/



-- _jq_extract_helper(row<anyelement>, fld<fldexpr|fldtype>, typ<*>)
create function _pg_json_query._jq_val_helper(
  row_ anyelement,
  fld _pg_json_query._field_type,
  typ jsonb
) returns jsonb language sql immutable as $$
  select case fld.path_arr_len
    when 0 then
      _pg_json_query._jq_col_val_impl(row_, fld.column_, typ)
    else
      _pg_json_query._field_extract_from_column(
        fld,
        _pg_json_query._jq_col_val_impl(row_, fld.column_, typ)
      )
    end;
$$;

create function _pg_json_query._jq_val_helper(
  row_ anyelement,
  fld _pg_json_query._field_type,
  typ json
) returns json language sql immutable as $$
  select case fld.path_arr_len
    when 0 then
      _pg_json_query._jq_col_val_impl(row_, fld.column_, typ)
    else
      _pg_json_query._field_extract_from_column(
        fld,
        _pg_json_query._jq_col_val_impl(row_, fld.column_, typ)
      )
    end;
$$;

create function _pg_json_query._jq_val_helper(
  row_ anyelement,
  fld _pg_json_query._field_type,
  typ text
) returns text language sql immutable as $$
  select case fld.path_arr_len
    when 0 then
      _pg_json_query._jq_col_val_impl(row_, fld.column_, typ)
    else
      _pg_json_query._field_extract_text_from_column(
        fld,
        _pg_json_query._jq_col_val_impl(row_, fld.column_, null::jsonb)
      )
    end;
$$;

create function _pg_json_query._jq_val_helper(
  row_ anyelement,
  fldexpr text,
  typ jsonb
) returns jsonb language sql immutable as $$
  select _pg_json_query._jq_val_helper(
    row_, _pg_json_query._field_type(fldexpr), typ
  );
$$;

create function _pg_json_query._jq_val_helper(
  row_ anyelement,
  fldexpr text,
  typ json
) returns json language sql immutable as $$
  select _pg_json_query._jq_val_helper(
    row_, _pg_json_query._field_type(fldexpr), typ
  );
$$;

create function _pg_json_query._jq_val_helper(
  row_ anyelement,
  fldexpr text,
  typ text
) returns text language sql immutable as $$
  select _pg_json_query._jq_val_helper(
    row_, _pg_json_query._field_type(fldexpr), typ
  );
$$;



-- text version.
-- To use with a row type, implement json_col_base_value_impl(text, <rowtype>, text)
-- that returns a textual representation of the specified column.
--create function _pg_json_query._col_value(valtyp text, row_ anyelement, fld text)
create function jq_val(row_ anyelement, colname text, typ text)
returns text language sql immutable as $$
  select _pg_json_query._jq_val_helper(row_, colname, typ);
$$;


create function jq_val(row_ anyelement, colname text, typ jsonb)
returns jsonb language sql immutable as $$
  select _pg_json_query._jq_val_helper(row_, colname, typ);
$$;


create function jq_val(row_ anyelement, colname text, typ json)
returns json language sql immutable as $$
  select _pg_json_query._jq_val_helper(row_, colname, typ);
$$;


-- Helper for jq_val(row_, jsonb_array) when the arrays are long.
create function _pg_json_query._jq_val_jsonb_arr(row_ anyelement, arr jsonb)
returns jsonb
language sql immutable
as $$
  select coalesce(json_agg(jq_val(row_, el, null::json)
                           order by idx)::jsonb, '[]')
  from jsonb_array_elements_text(arr) with ordinality o(el, idx);
$$;


create function jq_val(row_ anyelement, colexpr jsonb, typ jsonb)
returns jsonb
language sql immutable
as $$
  select case jsonb_typeof(colexpr)
    when 'array' then
      case jsonb_array_length(colexpr)
        when 0 then
          '[]'
        when 1 then
           _pg_json_query._build_array(
             coalesce(
               _pg_json_query._jq_val_helper(row_, colexpr->>0, typ), 'null'))
        when 2 then
           _pg_json_query._build_array(
             coalesce(
               _pg_json_query._jq_val_helper(row_, colexpr->>0, typ), 'null'),
             coalesce(
               _pg_json_query._jq_val_helper(row_, colexpr->>1, typ), 'null'))
        when 3 then
           _pg_json_query._build_array(
             coalesce(
               _pg_json_query._jq_val_helper(row_, colexpr->>0, typ), 'null'),
             coalesce(
               _pg_json_query._jq_val_helper(row_, colexpr->>1, typ), 'null'),
             coalesce(
               _pg_json_query._jq_val_helper(row_, colexpr->>2, typ), 'null'))
        when 4 then
           _pg_json_query._build_array(
             coalesce(
               _pg_json_query._jq_val_helper(row_, colexpr->>0, typ), 'null'),
             coalesce(
               _pg_json_query._jq_val_helper(row_, colexpr->>1, typ), 'null'),
             coalesce(
               _pg_json_query._jq_val_helper(row_, colexpr->>2, typ), 'null'),
             coalesce(
               _pg_json_query._jq_val_helper(row_, colexpr->>3, typ), 'null'))
        when 5 then
           _pg_json_query._build_array(
             coalesce(
               _pg_json_query._jq_val_helper(row_, colexpr->>0, typ), 'null'),
             coalesce(
               _pg_json_query._jq_val_helper(row_, colexpr->>1, typ), 'null'),
             coalesce(
               _pg_json_query._jq_val_helper(row_, colexpr->>2, typ), 'null'),
             coalesce(
               _pg_json_query._jq_val_helper(row_, colexpr->>3, typ), 'null'),
             coalesce(
               _pg_json_query._jq_val_helper(row_, colexpr->>4, typ), 'null'))
        else
          _pg_json_query._jq_val_jsonb_arr(row_, colexpr)
        end
    else
      _pg_json_query._jq_val_helper(
        row_,
        _pg_json_query._json_string_to_text(colexpr),
        typ
      )
    end;
$$;


-- If type is omitted, default to JSONB.
create function jq_val(row_ anyelement, colname text)
returns jsonb language sql immutable as $$
  select _pg_json_query._jq_val_helper(row_, colname, null::jsonb);
$$;

-- If type is omitted, default to JSONB.
create function jq_val(row_ anyelement, colexpr jsonb)
returns jsonb language sql immutable as $$
  select jq_val(row_, colexpr, null::jsonb);
$$;

create function jq_val_text(row_ anyelement, colname text)
returns text language sql immutable as $$
  select _pg_json_query._jq_val_helper(row_, colname, null::text);
$$;



create function jq_val_text_array(row_ anyelement, arr text[])
returns text[]
language sql immutable
as $$
  select coalesce(array_agg(jq_val_text(row_, el) order by idx), '{}')::text[]
  from unnest(arr) with ordinality o(el, idx);
$$;


create function jq_val(row_ anyelement, arr text[], typ jsonb)
returns jsonb language sql immutable as $$
  select jq_val(row_, to_json(arr)::jsonb, typ);
$$;


create function jq_val(row_ anyelement, arr text[], typ text)
returns text[] language sql immutable as $$
  select jq_val_text_array(row_, arr);
$$;


create function jq_concat_val_args(e1 jsonb, e2 jsonb)
returns jsonb
language sql immutable
as $$
  select case
    when e1 is null then coalesce(e2, '[]')
    when e2 is null then coalesce(e1, '[]')
    else
      -- Both non-null.
      _pg_json_query._jsonb_array_concat(e1, e2)
    end;
$$;


create function _pg_json_query._validate_attr_name(attrname text)
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
create function _pg_json_query._get_type_attrs(full_type_name text)
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
    _pg_json_query._validate_attr_name(attname::text) as attrname,
    atttypid::regtype::text as datatype
  from pg_attribute
  where attrelid = (select type_relid from type_relid) and
        attnum > 0 and
        not attisdropped;
$$;


create function _pg_json_query._filter_attr_not_exists_handler(
  full_type_name text,
  attr_name text
) returns boolean
language plpgsql immutable as $$
begin
  raise exception '% has no column "%"', full_type_name, attr_name;
  return false;
end;
$$;

create function _pg_json_query._colval_attr_not_exists_handler(
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


create function _pg_json_query._attr_not_exists_handler(
  ret_type anyelement,
  full_type_name text,
  attr_name text
) returns anyelement language sql immutable as $$
  select null;
$$;


-- Create the function definition for the _filter_row_column_impl()
-- function for the specified type.
create function _pg_json_query._get_filter_row_column_impl_defn(
  full_type_name text
) returns text language plpgsql stable as $$
declare
  when_exprs text;
  col_not_exists_expr text;
begin
  when_exprs := concat_ws('', variadic (
    select array_agg(format(
      E'    when %s then _pg_json_query._apply_filter(row_.%s, _)\n',
      quote_literal(attrname), quote_ident(attrname)))
    from _pg_json_query._get_type_attrs(full_type_name)
  ));

  col_not_exists_expr := format(
    '_pg_json_query._filter_attr_not_exists_handler(%s, fld)',
    quote_literal(full_type_name)
  );

  return concat(
     'create or replace function ',
     '_pg_json_query._filter_row_column_impl(',
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
create function _pg_json_query._get_col_value_impl_defn(
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
      from  _pg_json_query._get_type_attrs(full_type_name) t
    ),
    casted_exprs as (
      select
        attrname_lit,
        case
          when from_type_name = to_type_name then
            expr
          else
            format('_pg_json_query._cast(%s, null::%s)', expr, to_type_name)
        end as casted_expr
      from exprs
    )
  select array_agg(
    concat('    when ', attrname_lit, ' then ', casted_expr, E'\n')
  ) into attr_exprs
  from casted_exprs;

  attr_not_exists_expr := format(
    '_pg_json_query._colval_attr_not_exists_handler(null::%s, %s, fld)',
    to_type_name, quote_literal(full_type_name)
  );

  return concat(
     'create or replace function _pg_json_query._jq_col_val_impl(',
         'row_ ', full_type_name, ','
         'fld text, ',
         'valtyp ', to_type_name,
      ')', E'\n',
     'returns ', to_type_name, ' language sql stable as $f$', E'\n',
     '  select case fld ', E'\n', concat_ws(' ', variadic attr_exprs),
     ' else ', attr_not_exists_expr, E'\n', ' end;', E'\n',
     '$f$;', E'\n'
  );
end;
$$;



create function jq_register_type(full_type_name text)
returns boolean language plpgsql volatile as $$
declare
  stmt text;
begin
  full_type_name := full_type_name::regtype::text; -- ensure type exists.

  -- Create or replace the filter_row_column_impl() function for the type.
  stmt := _pg_json_query._get_filter_row_column_impl_defn(full_type_name);
  execute stmt;

  -- Create or replace the filter_row_column_impl() functions for the type.
  stmt := _pg_json_query._get_col_value_impl_defn(full_type_name, 'text');
  execute stmt;

  stmt := _pg_json_query._get_col_value_impl_defn(full_type_name, 'jsonb');
  execute stmt;

  stmt := _pg_json_query._get_col_value_impl_defn(full_type_name, 'json');
  execute stmt;

  return true;
end;
$$;



create function jq_unregister_type(full_type_name text)
returns boolean language plpgsql volatile as $$
declare
  stmt text;
begin
  full_type_name := full_type_name::regtype::text; -- ensure type exists.

  -- Drop the filter_row_column_impl() function for the type.
  execute format(
    'drop function if exists _pg_json_query._filter_row_column_impl(text, %s, anyelement)',
    full_type_name);

  execute format(
    'drop function if exists _pg_json_query._jq_col_val_impl(%s, text, text)',
    full_type_name);

  execute format(
    'drop function if exists _pg_json_query._jq_col_val_impl(%s, text, jsonb)',
    full_type_name);

  execute format(
    'drop function if exists _pg_json_query._jq_col_val_impl(%s, text, json)',
    full_type_name);

  return true;
end;
$$;

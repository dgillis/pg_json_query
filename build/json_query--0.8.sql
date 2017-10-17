
create schema _pg_json_query;
grant usage on schema _pg_json_query to public;

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
as $$ select s#>>'{}'; $$;


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
            when e1 = '[]' then ('[' || e2::text || ']')::jsonb
            else
              -- e1 non-empty.
              (left(e1::text, -1) || ', ' || e2::text || ']')::jsonb
            end
        end
    when jsonb_typeof(e2) = 'array' then
      -- e2 is an array, e1 is not.
      case
        when e2 = '[]' then ('[' || e1::text || ']')::jsonb
        else
          -- e2 non-empty.
          ('[' || e1::text || ', ' || right(e2::text, -1))::jsonb
        end
    else
      -- Neither are arrays
      ('[' || e1::text || ', ' || e2::text || ']')::jsonb
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
language sql stable
as $$
  select case jsonb_typeof(val)
    when 'string' then _pg_json_query._json_string_to_text(val)::numeric
    else val::text::numeric
  end;
$$;

create function _pg_json_query._to_bool(val jsonb)
returns boolean
language sql stable
as $$
  select case val
    when 'true' then true
    when 'false' then false
    else _pg_json_query._to_text(val)::boolean
  end;
$$;

create function _pg_json_query._to_date(val jsonb)
returns date
language sql stable
as $$
  select _pg_json_query._to_text(val)::date;
$$;

create function _pg_json_query._to_timestamp(val jsonb)
returns timestamp
language sql stable
as $$
  select _pg_json_query._to_text(val)::timestamp;
$$;

create function _pg_json_query._to_timestamptz(val jsonb)
returns timestamptz
language sql stable
as $$
  select _pg_json_query._to_text(val)::timestamptz;
$$;


create function _pg_json_query._cast_column_value(col jsonb, val jsonb)
returns jsonb
language sql stable
as $$
  select val;
$$;


create function _pg_json_query._cast_column_value(col integer, val jsonb)
returns integer
language sql stable
as $f$
  select _pg_json_query._to_numeric(val)::integer;
$f$;

create function _pg_json_query._cast_column_value(col bigint, val jsonb)
returns bigint
language sql stable
as $f$
  select _pg_json_query._to_numeric(val)::bigint;
$f$;

create function _pg_json_query._cast_column_value(col numeric, val jsonb)
returns numeric
language sql stable
as $f$
  select _pg_json_query._to_numeric(val);
$f$;

create function _pg_json_query._cast_column_value(col date, val jsonb)
returns date
language sql stable
as $f$
  select _pg_json_query._to_date(val);
$f$;

create function _pg_json_query._cast_column_value(col timestamp, val jsonb)
returns timestamp
language sql stable
as $f$
  select _pg_json_query._to_timestamp(val);
$f$;

create function _pg_json_query._cast_column_value(col timestamptz, val jsonb)
returns timestamptz
language sql stable
as $f$
  select _pg_json_query._to_timestamptz(val);
$f$;

create function _pg_json_query._cast_column_value(col boolean, val jsonb)
returns boolean
language sql stable
as $f$
  select _pg_json_query._to_bool(val);
$f$;

create function _pg_json_query._cast_column_value(col text, val jsonb)
returns text
language sql stable
as $f$
  select _pg_json_query._to_text(val);
$f$;

create function _pg_json_query._cast_column_value(col float8, val jsonb)
returns float8
language sql stable
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
create function _pg_json_query._cast(x text, to_type text)
returns text language sql immutable as $$;
  select x;
$$;

create function _pg_json_query._cast(x anyelement, to_type text)
returns text language sql stable as $$;
  select x::text;
$$;


/* to json/jsonb */

-- * -> json
create function _pg_json_query._cast(x anyelement, to_type json)
returns json language sql stable as $$;
  select to_json(x);
$$;

-- * -> jsonb
create function _pg_json_query._cast(x anyelement, to_type jsonb)
returns jsonb language sql stable as $$;
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



/* Generic casts from arbitrary textual representations to arbitrary types. */
create function _pg_json_query._cast(x text, to_type int)
returns int language sql immutable as $$ select x::int; $$;

create function _pg_json_query._cast(x text, to_type bigint)
returns bigint language sql immutable as $$ select x::bigint; $$;

create function _pg_json_query._cast(x text, to_type boolean)
returns boolean language sql immutable as $$ select x::boolean; $$;

create function _pg_json_query._cast(x text, to_type json)
returns json language sql stable as $$; select to_json(x); $$;

create function _pg_json_query._cast(x text, to_type jsonb)
returns jsonb language sql stable as $$; select to_json(x)::jsonb; $$;

create function _pg_json_query._cast(x text, to_type anyelement)
returns anyelement language plpgsql stable as $$
begin
  raise exception 'json_query has no cast from text to %', pg_typeof(to_type);
  return to_type;
end;
$$;

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
create or replace view _pg_json_query._default_row_types as (
  select
    t.oid::regtype::text as type_name,
    t.oid as type_oid
  from pg_type t join pg_namespace n on t.typnamespace = n.oid
  where
    n.nspname = 'pg_catalog' and -- built-ins
    typcategory not in ('P', 'x') and -- neither psuedo nor unknown
    typname::text !~* '^pg_.*' and -- non-internal
    typisdefined -- is defined
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

create function _pg_json_query._col_in_jsonb_arr(
  col anyelement,
  arr jsonb,
  _coltyp anyelement default null
)
returns boolean
language sql stable
as $$
  -- NOTE: We only allow arrays of up to length 20 for the "in"
  -- operator. This could be enlarged but it must be a preset
  -- limit otherwise inlining won't work.
  select case jsonb_array_length(arr)
    when 0 then
      false
    when 1 then
      _pg_json_query._col_op__eq(col, arr->0, _coltyp)
    when 2 then
      _pg_json_query._col_op__eq(col, arr->0, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->1, _coltyp)
    when 3 then
      _pg_json_query._col_op__eq(col, arr->0, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->1, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->2, _coltyp)
    when 4 then
      _pg_json_query._col_op__eq(col, arr->0, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->1, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->2, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->3, _coltyp)
    when 5 then
      _pg_json_query._col_op__eq(col, arr->0, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->1, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->2, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->3, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->4, _coltyp)
    when 6 then
      _pg_json_query._col_op__eq(col, arr->0, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->1, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->2, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->3, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->4, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->5, _coltyp)
    else
      _pg_json_query._col_op__eq(col, arr->0, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->1, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->2, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->3, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->4, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->5, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->6, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->7, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->8, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->9, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->10, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->11, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->12, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->13, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->14, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->15, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->16, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->17, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->18, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->19, _coltyp)
    end;
$$;



create function _pg_json_query._col_in_jsonb(
  col anyelement,
  arr_or_obj jsonb
)
returns boolean
language sql stable
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

-- eq
create function _pg_json_query._apply_pred__eq(col anyelement, filt jsonb,
                                               _coltyp anyelement default null)
returns boolean language sql stable as $$
  select case when filt->'value' = 'null' then
    col is null
  else
    _pg_json_query._col_op__eq(col, filt->'value', _coltyp)
  end;
$$;


-- ne
create function _pg_json_query._apply_pred__ne(col anyelement, filt jsonb,
                                               _coltyp anyelement default null)
returns boolean language sql stable as $$
  select case when filt->'value' = 'null' then
    col is not null
  else
    _pg_json_query._col_op__ne(col, filt->'value', _coltyp)
  end;
$$;


-- gt
create function _pg_json_query._apply_pred__gt(col anyelement, filt jsonb,
                                               _coltyp anyelement default null)
returns boolean language sql stable as $$
  select _pg_json_query._col_op__gt(col, filt->'value', _coltyp);
$$;


-- lt
create function _pg_json_query._apply_pred__lt(col anyelement, filt jsonb,
                                               _coltyp anyelement default null)
returns boolean language sql stable as $$
  select _pg_json_query._col_op__lt(col, filt->'value', _coltyp);
$$;


-- ge
create function _pg_json_query._apply_pred__ge(col anyelement, filt jsonb,
                                               _coltyp anyelement default null)
returns boolean language sql stable as $$
  select _pg_json_query._col_op__ge(col, filt->'value', _coltyp);
$$;


-- le
create function _pg_json_query._apply_pred__le(col anyelement, filt jsonb,
                                               _coltyp anyelement default null)
returns boolean language sql stable as $$
  select _pg_json_query._col_op__le(col, filt->'value', _coltyp);
$$;


-- in (jsonb)
create function _pg_json_query._apply_pred__in(col jsonb, filt jsonb,
                                               _coltyp jsonb default null)
returns boolean language sql stable as $$
  select _pg_json_query._col_in_jsonb(col, filt->'value');
$$;

-- in (anyelement)
create function _pg_json_query._apply_pred__in(col anyelement, filt jsonb,
                                               _coltyp anyelement default null)
returns boolean language sql stable as $$
  select _pg_json_query._col_in_jsonb(to_json(col)::jsonb, filt->'value');
$$;


-- notin
create function _pg_json_query._apply_pred__notin(col anyelement, filt jsonb,
                                                  _coltyp anyelement default null)
returns boolean language sql stable as $$
  select not _pg_json_query._apply_pred__in(col, filt, _coltyp);
$$;


-- like
create function _pg_json_query._apply_pred__like(col anyelement, filt jsonb,
                                                 _coltyp anyelement default null)
returns boolean language sql stable as $$ select _pg_json_query._like_helper(col, filt->>'value'); $$;


-- ilike
create function _pg_json_query._apply_pred__ilike(col anyelement, filt jsonb,
_coltyp anyelement default null)
returns boolean language sql stable as $$ select _pg_json_query._ilike_helper(col, filt->>'value'); $$;


-- startswith
create function _pg_json_query._apply_pred__startswith(col anyelement, filt jsonb,
                                                       _coltyp anyelement default null)
returns boolean language sql stable as $$
  select _pg_json_query._like_helper(col, (filt->>'value') || '%');
$$;


-- istartswith
create function _pg_json_query._apply_pred__istartswith(col anyelement, filt jsonb,
                                                        _coltyp anyelement default null)
returns boolean language sql stable as $$
  select _pg_json_query._ilike_helper(col, (filt->>'value') || '%');
$$;



-- endswith
create function _pg_json_query._apply_pred__endswith(col anyelement, filt jsonb,
                                                     _coltyp anyelement default null)
returns boolean language sql stable as $$
  select _pg_json_query._like_helper(col, '%' || (filt->>'value'));
$$;


-- iendswith
create function _pg_json_query._apply_pred__iendswith(col anyelement, filt jsonb,
                                                      _coltyp anyelement default null)
returns boolean language sql stable as $$
  select _pg_json_query._ilike_helper(col, '%' || (filt->>'value'));
$$;



-- exists
create function _pg_json_query._apply_pred__exists(col anyelement, filt jsonb,
                                                   _coltyp anyelement default null)
returns boolean language sql stable as $$
  select _pg_json_query._op__exists(col, filt->>'value');
$$;


-- notexists.
create function _pg_json_query._apply_pred__notexists(col anyelement, filt jsonb,
                                                      _coltyp anyelement default null)
returns boolean language sql stable as $$
  select not _pg_json_query._apply_pred__exists(col, filt, _coltyp);
$$;



-- contains (i.e., "@>" gin operator)

-- Fallback implementation: attempt to cast the textual representation of the value to whatever
-- the column's type is.
create function _pg_json_query._apply_pred__contains(col anyelement, filt jsonb,
                                                     _coltyp anyelement default null)
returns boolean language sql stable as $$
  select _pg_json_query._col_op__contains(col, filt->'value', _coltyp);
$$;

-- -- jsonb implementation.
-- create function _pg_json_query._apply_pred__contains(col jsonb, filt jsonb,
--                                                      _coltyp jsonb default null)
-- returns boolean language sql stable as $$ select col @> (filt->'value'); $$;

-- -- json implementation
-- create function _pg_json_query._apply_pred__contains(col json, filt jsonb,
--                                                      _coltyp json default null)
-- returns boolean language sql stable as $$ select col::jsonb @> (filt->'value'); $$;


-- contained ("<@"). Functions analoguous to the contains functions.
create function _pg_json_query._apply_pred__contained(col anyelement, filt jsonb,
                                                      _coltyp anyelement default null)
returns boolean language sql stable as $$
  select _pg_json_query._col_op__contained(col, filt->'value', _coltyp);
$$;

-- -- jsonb implementation.
-- create function _pg_json_query._apply_pred__contained(col jsonb, filt jsonb,
--                                                       _coltyp jsonb default null)
-- returns boolean language sql stable as $$ select col <@ (filt->'value') $$;

-- -- json implementation
-- create function _pg_json_query._apply_pred__contained(col json, filt jsonb,
--                                                       _coltyp json default null)
-- returns boolean language sql stable as $$ select col::jsonb <@ (filt->'value') $$;


-- not contains
create function _pg_json_query._apply_pred__notcontains(col anyelement, filt jsonb,
                                                        _coltyp anyelement default null)
returns boolean language sql stable as $$
  select not _pg_json_query._apply_pred__contains(col, filt, _coltyp);
$$;


-- not contained
create function _pg_json_query._apply_pred__notcontained(col anyelement, filt jsonb,
                                                         _coltyp anyelement default null)
returns boolean language sql stable as $$
  select not _pg_json_query._apply_pred__contained(col, filt, _coltyp);
$$;




create function _pg_json_query._apply_op(op text, col anyelement, filt jsonb)
returns boolean language sql stable as $$
  select case op
    when 'eq' then _pg_json_query._apply_pred__eq(col, filt)
    when 'ne' then _pg_json_query._apply_pred__ne(col, filt)
    when 'gt' then _pg_json_query._apply_pred__gt(col, filt)
    when 'lt' then _pg_json_query._apply_pred__lt(col, filt)
    when 'ge' then _pg_json_query._apply_pred__ge(col, filt)
    when 'le' then _pg_json_query._apply_pred__le(col, filt)
    when 'in' then _pg_json_query._apply_pred__in(col, filt)
    when 'notin' then _pg_json_query._apply_pred__notin(col, filt)
    when 'like' then _pg_json_query._apply_pred__like(col, filt)
    when 'ilike' then _pg_json_query._apply_pred__ilike(col, filt)
    when 'startswith' then _pg_json_query._apply_pred__startswith(col, filt)
    when 'istartswith' then _pg_json_query._apply_pred__istartswith(col, filt)
    when 'endswith' then _pg_json_query._apply_pred__endswith(col, filt)
    when 'iendswith' then _pg_json_query._apply_pred__iendswith(col, filt)
    when 'exists' then _pg_json_query._apply_pred__exists(col, filt)
    when 'notexists' then _pg_json_query._apply_pred__notexists(col, filt)
    when 'contains' then _pg_json_query._apply_pred__contains(col, filt)
    when 'notcontains' then _pg_json_query._apply_pred__notcontains(col, filt)
    when 'contained' then _pg_json_query._apply_pred__contained(col, filt)
    when 'notcontained' then _pg_json_query._apply_pred__notcontained(col, filt)
    -- Aliases
    when 'gte' then _pg_json_query._apply_pred__ge(col, filt)
    when 'lte' then _pg_json_query._apply_pred__le(col, filt)
    else null
    end;
$$;

-- General form for _apply_filter(). Type-specific implementations omit the
-- third argument from their call signature so that they can make a call to
-- this method after doing any type-specific preprocessing (e.g., see the json
-- implementation).
create or replace function _pg_json_query._apply_filter(
  col anyelement,
  filt jsonb,
  _coltyp anyelement default null
) returns boolean language sql stable
as $$ select _pg_json_query._apply_op(filt->>'op', col, filt); $$;


-- JSONB implementation of _apply_filter(). Calls the general form after
-- evaluating any path included in the filter.
create or replace function _pg_json_query._apply_filter(col jsonb, filt jsonb)
returns boolean
language sql stable
as $$
  select case
    when (filt->>'path') is null then
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
create or replace function _pg_json_query._apply_filter(col json, filt jsonb)
returns boolean
language sql stable as $$
  select _pg_json_query._apply_filter(col::jsonb, filt);
$$;


-- Call _filter_row_column_impl() if filt is non-null, otherwise return true.
-- Note that this functions depends on the existence of an implementation of
-- _filter_row_column_impl() for the given row type.
create or replace function _pg_json_query._filter_row_column(
  row_ anyelement,
  filt jsonb
)
returns boolean
language sql
stable
cost 1000000
as $$
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
create or replace function _pg_json_query._filter_row_impl(
  row_ anyelement,
  filts jsonb
)
returns boolean
language sql
stable
cost 1000000
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


-- The jq_filter() is having some performance issues stemming from the
-- string processing of the filter object preventing inlining.  This
-- function takes a JSONB array of filter objects already in the internal
-- form, with "field", "op", "value" being required and "path", "path_is_text"
-- being optional.
create or replace function jq_filter_raw(
  row_ anyelement,
  filts jsonb
)
returns boolean
language sql
stable
cost 1000000
as $$
   select _pg_json_query._filter_row_impl(row_, filts);
$$;


-- Convert one of the filter objects from the user input format into a JSONB
-- array of _filter_type-like JSONB objects.
--
-- NOTE: It appears that in many cases this function prevents inlining.
--   It looks like the implementation makes no difference when PLPGSQL is used
--   since even changing the function to a constant "return XXXX" prevents
--   inlining. Try and see if an SQL implementation can get over this.
create or replace function _pg_json_query._parse_filter_obj_to_json(obj jsonb)
returns jsonb
language plpgsql
immutable
as $$
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
create or replace function _pg_json_query._parse_filter(typ jsonb, obj jsonb)
returns jsonb
language sql
stable
cost 1000000
as $$
  select _pg_json_query._parse_filter_obj_to_json(obj);
$$;


-- Filter function available to all tables which implement
-- _filter_row_column_impl() for their row-type.
create or replace function jq_filter(row_ anyelement, filter_obj jsonb)
returns boolean
language sql
stable
cost 1000000 -- same as cost of _filter_row_impl
as $$
  select _pg_json_query._filter_row_impl(
    row_,
    _pg_json_query._parse_filter_obj_to_json(filter_obj)
  );
$$;


create or replace function jq_and_filters(f1 jsonb, f2 jsonb)
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
create or replace function _pg_json_query._jq_val_helper(
  row_ anyelement,
  fld _pg_json_query._field_type,
  typ jsonb
)
returns jsonb
language sql
stable
cost 1000000
as $$
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

create or replace function _pg_json_query._jq_val_helper(
  row_ anyelement,
  fld _pg_json_query._field_type,
  typ json
)
returns json
language sql
stable
cost 1000000
as $$
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

create or replace function _pg_json_query._jq_val_helper(
  row_ anyelement,
  fld _pg_json_query._field_type,
  typ text
)
returns text
language sql
stable
cost 1000000
as $$
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

create or replace function _pg_json_query._jq_val_helper(
  row_ anyelement,
  fldexpr text,
  typ jsonb
)
returns jsonb
language sql
stable
cost 100000
as $$
  select _pg_json_query._jq_val_helper(
    row_, _pg_json_query._field_type(fldexpr), typ
  );
$$;

create or replace function _pg_json_query._jq_val_helper(
  row_ anyelement,
  fldexpr text,
  typ json
)
returns json
language sql
stable
cost 1000000
as $$
  select _pg_json_query._jq_val_helper(
    row_, _pg_json_query._field_type(fldexpr), typ
  );
$$;

create or replace function _pg_json_query._jq_val_helper(
  row_ anyelement,
  fldexpr text,
  typ text
)
returns text
language sql
stable
cost 1000000
as $$
  select _pg_json_query._jq_val_helper(
    row_, _pg_json_query._field_type(fldexpr), typ
  );
$$;



-- text version.
-- To use with a row type, implement json_col_base_value_impl(text, <rowtype>, text)
-- that returns a textual representation of the specified column.
--create or replace function _pg_json_query._col_value(valtyp text, row_ anyelement, fld text)
create or replace function jq_val(row_ anyelement, colname text, typ text)
returns text
language sql
stable
cost 1000000
as $$
  select _pg_json_query._jq_val_helper(row_, colname, typ);
$$;


create or replace function jq_val(row_ anyelement, colname text, typ jsonb)
returns jsonb language sql stable as $$
  select _pg_json_query._jq_val_helper(row_, colname, typ);
$$;


create or replace function jq_val(row_ anyelement, colname text, typ json)
returns json
language sql
stable
cost 1000000
as $$
  select _pg_json_query._jq_val_helper(row_, colname, typ);
$$;


-- Helper for jq_val(row_, jsonb_array) when the arrays are long.
create or replace function _pg_json_query._jq_val_jsonb_arr(row_ anyelement, arr jsonb)
returns jsonb
language sql
stable
cost 1000000
as $$
  select coalesce(json_agg(jq_val(row_, el, null::json)
                           order by idx)::jsonb, '[]')
  from jsonb_array_elements_text(arr) with ordinality o(el, idx);
$$;


create or replace function jq_val(row_ anyelement, colexpr jsonb, typ jsonb)
returns jsonb
language plpgsql
stable
cost 1000000
as $$
declare
  exprtyp text;
  arrlen int;
begin
  if colexpr = '[]' then
    return '[]';
  end if;

  exprtyp := jsonb_typeof(colexpr);

  if jsonb_typeof(colexpr) != 'array' then
    return _pg_json_query._jq_val_helper(
      row_,
      _pg_json_query._json_string_to_text(colexpr),
      typ
    );
  end if;

  arrlen := jsonb_array_length(colexpr);

  if arrlen = 1 then
     return _pg_json_query._build_array(
       coalesce(
         _pg_json_query._jq_val_helper(row_, colexpr->>0, typ), 'null'));
  elsif arrlen = 2 then
     return _pg_json_query._build_array(
       coalesce(
         _pg_json_query._jq_val_helper(row_, colexpr->>0, typ), 'null'),
       coalesce(
         _pg_json_query._jq_val_helper(row_, colexpr->>1, typ), 'null'));
  elsif arrlen = 3 then
     return _pg_json_query._build_array(
       coalesce(
         _pg_json_query._jq_val_helper(row_, colexpr->>0, typ), 'null'),
       coalesce(
         _pg_json_query._jq_val_helper(row_, colexpr->>1, typ), 'null'),
       coalesce(
         _pg_json_query._jq_val_helper(row_, colexpr->>2, typ), 'null'));
  elsif arrlen = 4 then
     return _pg_json_query._build_array(
       coalesce(
         _pg_json_query._jq_val_helper(row_, colexpr->>0, typ), 'null'),
       coalesce(
         _pg_json_query._jq_val_helper(row_, colexpr->>1, typ), 'null'),
       coalesce(
         _pg_json_query._jq_val_helper(row_, colexpr->>2, typ), 'null'),
       coalesce(
         _pg_json_query._jq_val_helper(row_, colexpr->>3, typ), 'null'));
  elsif arrlen = 5 then
     return _pg_json_query._build_array(
       coalesce(
         _pg_json_query._jq_val_helper(row_, colexpr->>0, typ), 'null'),
       coalesce(
         _pg_json_query._jq_val_helper(row_, colexpr->>1, typ), 'null'),
       coalesce(
         _pg_json_query._jq_val_helper(row_, colexpr->>2, typ), 'null'),
       coalesce(
         _pg_json_query._jq_val_helper(row_, colexpr->>3, typ), 'null'),
       coalesce(
         _pg_json_query._jq_val_helper(row_, colexpr->>4, typ), 'null'));
  else
    return _pg_json_query._jq_val_jsonb_arr(row_, colexpr);
  end if;
end;
$$;


-- If type is omitted, default to JSONB.
create or replace function jq_val(row_ anyelement, colname text)
returns jsonb
language sql
stable
cost 1000000
as $$
  select _pg_json_query._jq_val_helper(row_, colname, null::jsonb);
$$;

-- If type is omitted, default to JSONB.
create or replace function jq_val(row_ anyelement, colexpr jsonb)
returns jsonb
language sql
stable
cost 1000000
as $$
  select jq_val(row_, colexpr, null::jsonb);
$$;

create or replace function jq_val_text(row_ anyelement, colname text)
returns text
language sql
stable
cost 1000000
as $$
  select _pg_json_query._jq_val_helper(row_, colname, null::text);
$$;



create or replace function jq_val_text_array(row_ anyelement, arr text[])
returns text[]
language sql
stable
cost 1000000
as $$
  select coalesce(array_agg(jq_val_text(row_, el) order by idx), '{}')::text[]
  from unnest(arr) with ordinality o(el, idx);
$$;


create or replace function jq_val(row_ anyelement, arr text[], typ jsonb)
returns jsonb
language sql
stable
cost 1000000
as $$
  select jq_val(row_, to_json(arr)::jsonb, typ);
$$;


create or replace function jq_val(row_ anyelement, arr text[], typ text)
returns text[]
language sql
stable
cost 1000000
as $$
  select jq_val_text_array(row_, arr);
$$;


create or replace function jq_concat_val_args(e1 jsonb, e2 jsonb)
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
  -- valid attribute names cannot end in an underscore and cannot contain
  -- consecutive underscores.
  if not (attrname ~ '^(_?[a-zA-Z0-9]+)+$') then
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
language plpgsql stable as $$
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
language plpgsql stable as $$
begin
  raise exception '% has no column "%"', full_type_name, attr_name;
  return case when false then valtype else null end;
end;
$$;


create function _pg_json_query._attr_not_exists_handler(
  ret_type anyelement,
  full_type_name text,
  attr_name text
) returns anyelement language sql stable as $$
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
      E'returns boolean language sql stable as $f$\n',
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



create function jq_register_row_type(full_type_name text)
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



create function jq_unregister_row_type(full_type_name text)
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



-- Fallback implementation used for non-existent operators (we need such
-- an implementation so that the static-type checker doesn't complain).
create or replace function _pg_json_query._op_does_not_exist(
  op_name text,
  op text,
  lhs_type_name text,
  rhs_type_name text
) returns boolean language plpgsql stable as $$
begin
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


do $$
declare
  type_name text;
  type_oid oid;
begin
  for type_name, type_oid in (select *
                              from _pg_json_query._default_row_types) loop
    perform _pg_json_query._register_col_type(type_oid);
  end loop;
end;
$$;

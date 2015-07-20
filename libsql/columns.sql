

create function json_query._to_numeric(val jsonb)
returns numeric
language sql immutable
as $$
  select case jsonb_typeof(val)
    when 'string' then json_query._json_string_to_text(val)::numeric
    else val::text::numeric
  end;
$$;

create function json_query._to_bool(val jsonb)
returns boolean
language sql immutable
as $$
  select case val
    when 'true' then true
    when 'false' then false
    else json_query._to_text(val)::boolean
  end;
$$;

create function json_query._to_date(val jsonb)
returns date
language sql immutable
as $$
  select json_query._to_text(val)::date;
$$;

create function json_query._to_timestamp(val jsonb)
returns timestamp
language sql immutable
as $$
  select json_query._to_text(val)::timestamp;
$$;

create function json_query._to_timestamptz(val jsonb)
returns timestamptz
language sql immutable
as $$
  select json_query._to_text(val)::timestamptz;
$$;


create function json_query._cast_column_value(col jsonb, val jsonb)
returns jsonb
language sql immutable
as $$
  select val;
$$;


create function json_query._cast_column_value(col integer, val jsonb)
returns integer
language sql immutable
as $f$
  select json_query._to_numeric(val)::integer;
$f$;

create function json_query._cast_column_value(col bigint, val jsonb)
returns bigint
language sql immutable
as $f$
  select json_query._to_numeric(val)::bigint;
$f$;

create function json_query._cast_column_value(col numeric, val jsonb)
returns numeric
language sql immutable
as $f$
  select json_query._to_numeric(val);
$f$;

create function json_query._cast_column_value(col date, val jsonb)
returns date
language sql immutable
as $f$
  select json_query._to_date(val);
$f$;

create function json_query._cast_column_value(col timestamp, val jsonb)
returns timestamp
language sql immutable
as $f$
  select json_query._to_timestamp(val);
$f$;

create function json_query._cast_column_value(col timestamptz, val jsonb)
returns timestamptz
language sql immutable
as $f$
  select json_query._to_timestamptz(val);
$f$;

create function json_query._cast_column_value(col boolean, val jsonb)
returns boolean
language sql immutable
as $f$
  select json_query._to_bool(val);
$f$;

create function json_query._cast_column_value(col text, val jsonb)
returns text
language sql immutable
as $f$
  select json_query._to_text(val);
$f$;

create function json_query._cast_column_value(col float8, val jsonb)
returns float8
language sql immutable
as $f$
  select json_query._to_numeric(val)::float8;
$f$;



create function json_query._col_in_jsonb_arr(
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
      col = json_query._cast_column_value(_coltype, arr->0)
    when 2 then
      col = json_query._cast_column_value(_coltype, arr->0) or
      col = json_query._cast_column_value(_coltype, arr->1)
    when 3 then
      col = json_query._cast_column_value(_coltype, arr->0) or
      col = json_query._cast_column_value(_coltype, arr->1) or
      col = json_query._cast_column_value(_coltype, arr->2)
    when 4 then
      col = json_query._cast_column_value(_coltype, arr->0) or
      col = json_query._cast_column_value(_coltype, arr->1) or
      col = json_query._cast_column_value(_coltype, arr->2) or
      col = json_query._cast_column_value(_coltype, arr->3)
    when 5 then
      col = json_query._cast_column_value(_coltype, arr->0) or
      col = json_query._cast_column_value(_coltype, arr->1) or
      col = json_query._cast_column_value(_coltype, arr->2) or
      col = json_query._cast_column_value(_coltype, arr->3) or
      col = json_query._cast_column_value(_coltype, arr->4)
    when 6 then
      col = json_query._cast_column_value(_coltype, arr->0) or
      col = json_query._cast_column_value(_coltype, arr->1) or
      col = json_query._cast_column_value(_coltype, arr->2) or
      col = json_query._cast_column_value(_coltype, arr->3) or
      col = json_query._cast_column_value(_coltype, arr->4) or
      col = json_query._cast_column_value(_coltype, arr->5)
    else
      col = json_query._cast_column_value(_coltype, arr->0) or
      col = json_query._cast_column_value(_coltype, arr->1) or
      col = json_query._cast_column_value(_coltype, arr->2) or
      col = json_query._cast_column_value(_coltype, arr->3) or
      col = json_query._cast_column_value(_coltype, arr->4) or
      col = json_query._cast_column_value(_coltype, arr->5) or
      col = json_query._cast_column_value(_coltype, arr->6) or
      col = json_query._cast_column_value(_coltype, arr->7) or
      col = json_query._cast_column_value(_coltype, arr->8) or
      col = json_query._cast_column_value(_coltype, arr->9) or
      col = json_query._cast_column_value(_coltype, arr->10) or
      col = json_query._cast_column_value(_coltype, arr->11) or
      col = json_query._cast_column_value(_coltype, arr->12) or
      col = json_query._cast_column_value(_coltype, arr->13) or
      col = json_query._cast_column_value(_coltype, arr->14) or
      col = json_query._cast_column_value(_coltype, arr->15) or
      col = json_query._cast_column_value(_coltype, arr->16) or
      col = json_query._cast_column_value(_coltype, arr->17) or
      col = json_query._cast_column_value(_coltype, arr->18) or
      col = json_query._cast_column_value(_coltype, arr->19)
    end;
$$;


create function json_query._col_in_jsonb(
  col anyelement,
  arr_or_obj jsonb
)
returns boolean
language sql immutable
as $$
  select case jsonb_typeof(arr_or_obj)
    when 'object' then
      arr_or_obj ? json_query._force_text(col)
    when 'array' then
      json_query._col_in_jsonb_arr(col, arr_or_obj)
    else
      false
    end;
$$;



create function json_query._column_extract_path(col jsonb, path_ text)
returns jsonb language sql immutable as $$ select col->path_; $$;

create function json_query._column_extract_path_text(col jsonb, path_ text)
returns text language sql immutable as $$ select col->>path_; $$;

create function json_query._column_extract_path(col jsonb, path_ text[])
returns jsonb language sql immutable as $$ select col#>path_; $$;

create function json_query._column_extract_path_text(col jsonb, path_ text[])
returns text language sql immutable as $$ select col#>>path_; $$;

create function json_query._column_extract_path(col jsonb, path_ jsonb)
returns jsonb language sql immutable as $$
  select case jsonb_typeof(path_)
    when 'array' then
      json_query._column_extract_path(col, json_query._jsonb_arr_to_text_arr(path_))
    else
      json_query._column_extract_path(col, json_query._json_string_to_text(path_))
    end;
$$;

create function json_query._column_extract_path_text(col jsonb, path_ jsonb)
returns text language sql immutable as $$
  select case jsonb_typeof(path_)
    when 'array' then
      json_query._column_extract_path_text(col, json_query._jsonb_arr_to_text_arr(path_))
    else
      json_query._column_extract_path_text(col, json_query._json_string_to_text(path_))
    end;
$$;

create function json_query._column_extract_path(col json, path_ anyelement)
returns json language sql immutable as $$
  select json_query._column_extract_path(col::jsonb, path_)::json;
$$;

create function json_query._column_extract_path_text(col json, path_ anyelement)
returns text language sql immutable as $$
  select json_query._column_extract_path(col::jsonb, path_)::text;
$$;

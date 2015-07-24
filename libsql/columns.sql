

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

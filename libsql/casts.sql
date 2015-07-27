
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

create function _pg_json_query._cast(x text, to_type text)
returns text language sql immutable as $$;
  select x;
$$;

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

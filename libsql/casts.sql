
-- Casting anyelement to its own type just returns the original element.
create function json_query._cast(x anyelement, to_type anyelement)
returns anyelement language sql immutable as $$ select x; $$;


/* to text */

-- * -> text
create function json_query._cast(x anyelement, to_type text)
returns text language sql immutable as $$;
  select x::text;
$$;

-- text -> text (override * -> text)
create function json_query._cast(x text, to_type text)
returns text language sql immutable as $$;
  select x;
$$;



/* to json/jsonb */

-- * -> json
create function json_query._cast(x anyelement, to_type json)
returns json language sql immutable as $$;
  select to_json(x);
$$;

-- * -> jsonb
create function json_query._cast(x anyelement, to_type jsonb)
returns jsonb language sql immutable as $$;
  select to_json(x)::jsonb;
$$;

-- json -> json (overide anyelement to json).
create function json_query._cast(x json, to_type json)
returns json language sql immutable as $$ select x; $$;

-- jsonb -> jsonb (overide anyelement to jsonb).
create function json_query._cast(x jsonb, to_type jsonb)
returns jsonb language sql immutable as $$ select x; $$;

-- jsonb -> json
create function json_query._cast(x jsonb, to_type json)
returns json language sql immutable as $$ select x::json; $$;

-- json -> jsonb
create function json_query._cast(x json, to_type jsonb)
returns jsonb language sql immutable as $$ select x::jsonb; $$;

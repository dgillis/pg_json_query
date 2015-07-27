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

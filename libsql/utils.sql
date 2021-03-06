
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





create or replace function json_query._jsonb_arr_to_text_arr(arr jsonb)
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
create or replace function json_query._build_array(variadic elems jsonb[] default array[]::jsonb[])
returns jsonb
language sql immutable
as $$
  select ('[' || concat_ws(', ', variadic elems) || ']')::jsonb;
$$;



create function json_query._json_string_to_text(s jsonb)
returns text
language sql immutable
as $$
  select ('[' || s || ']')::jsonb->>0;
$$;



-- Concat the two JSONB arrays to form a new one. If either element
-- is a non-array, it will be treated as a single element array of
-- that element.
create or replace function json_query._jsonb_array_concat(e1 jsonb, e2 jsonb)
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



create or replace function json_query._to_text(val jsonb)
returns text
language sql immutable
as $$
  select case
    when jsonb_typeof(val) = 'string' then
      json_query._json_string_to_text(val)
    else
      val::text
  end;
$$;


create or replace function json_query._force_text(val anyelement,
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
      json_query._json_string_to_text(val::text::jsonb)
    when json_rtyp then
      json_query._json_string_to_text(val::text::jsonb)
    else
      val::text
    end;
$$;


-- Test whether the string appears to be a valid JSON array or string.
-- This is intended to be fast rather than exact and will return true
-- for some string where s:json would actually raise an exception.
create or replace function json_query._looks_like_json_string_or_array(s text)
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




create or replace function json_query._col_in_jsonb_arr(
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


create or replace function json_query._col_in_jsonb(
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



-- Helper methods for like/startswith/ilike.
create or replace function json_query._like_helper(col text, pattern text)
returns boolean language sql immutable as $$
  select col like pattern;
$$;

create or replace function json_query._like_helper(col jsonb, pattern text)
returns boolean language sql immutable as $$
  select json_query._json_string_to_text(col) like pattern;
$$;

create or replace function json_query._like_helper(col json, pattern text)
returns boolean language sql immutable as $$
  select json_query._json_string_to_text(col::jsonb) like pattern;
$$;

create or replace function json_query._ilike_helper(col text, pattern text)
returns boolean language sql immutable as $$
  select col ilike pattern;
$$;

create or replace function json_query._ilike_helper(col jsonb, pattern text)
returns boolean language sql immutable as $$
  select json_query._json_string_to_text(col) ilike pattern;
$$;

create or replace function json_query._ilike_helper(col json, pattern text)
returns boolean language sql immutable as $$
  select json_query._json_string_to_text(col::jsonb) ilike pattern;
$$;

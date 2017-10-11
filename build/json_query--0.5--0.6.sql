create or replace function _pg_json_query._jsonb_array_concat(e1 jsonb, e2 jsonb)
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

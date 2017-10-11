
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

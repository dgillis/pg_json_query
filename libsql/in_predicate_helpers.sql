
create function _pg_json_query._col_in_jsonb_arr(
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
      col = _pg_json_query._cast_column_value(_coltype, arr->0)
    when 2 then
      col = _pg_json_query._cast_column_value(_coltype, arr->0) or
      col = _pg_json_query._cast_column_value(_coltype, arr->1)
    when 3 then
      col = _pg_json_query._cast_column_value(_coltype, arr->0) or
      col = _pg_json_query._cast_column_value(_coltype, arr->1) or
      col = _pg_json_query._cast_column_value(_coltype, arr->2)
    when 4 then
      col = _pg_json_query._cast_column_value(_coltype, arr->0) or
      col = _pg_json_query._cast_column_value(_coltype, arr->1) or
      col = _pg_json_query._cast_column_value(_coltype, arr->2) or
      col = _pg_json_query._cast_column_value(_coltype, arr->3)
    when 5 then
      col = _pg_json_query._cast_column_value(_coltype, arr->0) or
      col = _pg_json_query._cast_column_value(_coltype, arr->1) or
      col = _pg_json_query._cast_column_value(_coltype, arr->2) or
      col = _pg_json_query._cast_column_value(_coltype, arr->3) or
      col = _pg_json_query._cast_column_value(_coltype, arr->4)
    when 6 then
      col = _pg_json_query._cast_column_value(_coltype, arr->0) or
      col = _pg_json_query._cast_column_value(_coltype, arr->1) or
      col = _pg_json_query._cast_column_value(_coltype, arr->2) or
      col = _pg_json_query._cast_column_value(_coltype, arr->3) or
      col = _pg_json_query._cast_column_value(_coltype, arr->4) or
      col = _pg_json_query._cast_column_value(_coltype, arr->5)
    else
      col = _pg_json_query._cast_column_value(_coltype, arr->0) or
      col = _pg_json_query._cast_column_value(_coltype, arr->1) or
      col = _pg_json_query._cast_column_value(_coltype, arr->2) or
      col = _pg_json_query._cast_column_value(_coltype, arr->3) or
      col = _pg_json_query._cast_column_value(_coltype, arr->4) or
      col = _pg_json_query._cast_column_value(_coltype, arr->5) or
      col = _pg_json_query._cast_column_value(_coltype, arr->6) or
      col = _pg_json_query._cast_column_value(_coltype, arr->7) or
      col = _pg_json_query._cast_column_value(_coltype, arr->8) or
      col = _pg_json_query._cast_column_value(_coltype, arr->9) or
      col = _pg_json_query._cast_column_value(_coltype, arr->10) or
      col = _pg_json_query._cast_column_value(_coltype, arr->11) or
      col = _pg_json_query._cast_column_value(_coltype, arr->12) or
      col = _pg_json_query._cast_column_value(_coltype, arr->13) or
      col = _pg_json_query._cast_column_value(_coltype, arr->14) or
      col = _pg_json_query._cast_column_value(_coltype, arr->15) or
      col = _pg_json_query._cast_column_value(_coltype, arr->16) or
      col = _pg_json_query._cast_column_value(_coltype, arr->17) or
      col = _pg_json_query._cast_column_value(_coltype, arr->18) or
      col = _pg_json_query._cast_column_value(_coltype, arr->19)
    end;
$$;



create function _pg_json_query._col_in_jsonb(
  col anyelement,
  arr_or_obj jsonb
)
returns boolean
language sql immutable
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

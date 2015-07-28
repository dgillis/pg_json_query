
create function _pg_json_query._col_in_jsonb_arr(
  col anyelement,
  arr jsonb,
  _coltyp anyelement default null
)
returns boolean
language sql stable
as $$
  -- NOTE: We only allow arrays of up to length 20 for the "in"
  -- operator. This could be enlarged but it must be a preset
  -- limit otherwise inlining won't work.
  select case jsonb_array_length(arr)
    when 0 then
      false
    when 1 then
      _pg_json_query._col_op__eq(col, arr->0, _coltyp)
    when 2 then
      _pg_json_query._col_op__eq(col, arr->0, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->1, _coltyp)
    when 3 then
      _pg_json_query._col_op__eq(col, arr->0, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->1, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->2, _coltyp)
    when 4 then
      _pg_json_query._col_op__eq(col, arr->0, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->1, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->2, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->3, _coltyp)
    when 5 then
      _pg_json_query._col_op__eq(col, arr->0, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->1, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->2, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->3, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->4, _coltyp)
    when 6 then
      _pg_json_query._col_op__eq(col, arr->0, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->1, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->2, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->3, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->4, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->5, _coltyp)
    else
      _pg_json_query._col_op__eq(col, arr->0, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->1, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->2, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->3, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->4, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->5, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->6, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->7, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->8, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->9, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->10, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->11, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->12, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->13, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->14, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->15, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->16, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->17, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->18, _coltyp) or
      _pg_json_query._col_op__eq(col, arr->19, _coltyp)
    end;
$$;



create function _pg_json_query._col_in_jsonb(
  col anyelement,
  arr_or_obj jsonb
)
returns boolean
language sql stable
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

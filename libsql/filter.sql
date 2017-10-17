
-- General form for _apply_filter(). Type-specific implementations omit the
-- third argument from their call signature so that they can make a call to
-- this method after doing any type-specific preprocessing (e.g., see the json
-- implementation).
create or replace function _pg_json_query._apply_filter(
  col anyelement,
  filt jsonb,
  _coltyp anyelement default null
) returns boolean language sql stable
as $$ select _pg_json_query._apply_op(filt->>'op', col, filt); $$;


-- JSONB implementation of _apply_filter(). Calls the general form after
-- evaluating any path included in the filter.
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


-- JSON implementation of _apply_filter(). Similar to the JSONB one.
create or replace function _pg_json_query._apply_filter(col json, filt jsonb)
returns boolean
language sql stable as $$
  select _pg_json_query._apply_filter(col::jsonb, filt);
$$;


-- Call _filter_row_column_impl() if filt is non-null, otherwise return true.
-- Note that this functions depends on the existence of an implementation of
-- _filter_row_column_impl() for the given row type.
create or replace function _pg_json_query._filter_row_column(
  row_ anyelement,
  filt jsonb
)
returns boolean
language sql
stable
cost 1000000
as $$
  select
    case
      when filt is null then
        true
      else
        _pg_json_query._filter_row_column_impl(filt->>'field', row_, filt)
    end;
$$;


-- Returns true if every filter object in the JSONB-array of filters is true
-- or false otherwise. Any elements beyond the maximum allowed (currently, 12)
-- will be ignored.
create or replace function _pg_json_query._filter_row_impl(
  row_ anyelement,
  filts jsonb
)
returns boolean
language sql
stable
cost 1000000
as $$
  select
    _pg_json_query._filter_row_column(row_, filts->0) and
    _pg_json_query._filter_row_column(row_, filts->1) and
    _pg_json_query._filter_row_column(row_, filts->2) and
    _pg_json_query._filter_row_column(row_, filts->3) and
    _pg_json_query._filter_row_column(row_, filts->4) and
    _pg_json_query._filter_row_column(row_, filts->5) and
    _pg_json_query._filter_row_column(row_, filts->6) and
    _pg_json_query._filter_row_column(row_, filts->7) and
    _pg_json_query._filter_row_column(row_, filts->8) and
    _pg_json_query._filter_row_column(row_, filts->9) and
    _pg_json_query._filter_row_column(row_, filts->10) and
    _pg_json_query._filter_row_column(row_, filts->11);
$$;


-- The jq_filter() is having some performance issues stemming from the
-- string processing of the filter object preventing inlining.  This
-- function takes a JSONB array of filter objects already in the internal
-- form, with "field", "op", "value" being required and "path", "path_is_text"
-- being optional.
create or replace function jq_filter_raw(
  row_ anyelement,
  filts jsonb
)
returns boolean
language sql
stable
cost 1000000
as $$
   select _pg_json_query._filter_row_impl(row_, filts);
$$;


-- Convert one of the filter objects from the user input format into a JSONB
-- array of _filter_type-like JSONB objects.
--
-- NOTE: It appears that in many cases this function prevents inlining.
--   It looks like the implementation makes no difference when PLPGSQL is used
--   since even changing the function to a constant "return XXXX" prevents
--   inlining. Try and see if an SQL implementation can get over this.
create or replace function _pg_json_query._parse_filter_obj_to_json(obj jsonb)
returns jsonb
language plpgsql
immutable
as $$
declare
  and_arr jsonb;
  dj_arr jsonb;
  expl_filt jsonb;
  arr jsonb;
begin
  if obj ? '$and' then
    select json_agg(o)::jsonb into and_arr
    from (
      select jsonb_array_elements(
        _pg_json_query._parse_filter_obj_to_json(o)
      ) as o
      from jsonb_array_elements(obj->'$and') o
    ) sq;

    arr := and_arr;
  end if;

  select json_agg(
    _pg_json_query._filt_to_json(_pg_json_query._filt_type(key, value))
  ) into dj_arr
  from jsonb_each(obj)
  where left(key, 1) != '$';

  if dj_arr is not null then
     arr := case
       when arr is null then
         dj_arr
       else
         _pg_json_query._jsonb_array_concat(arr, dj_arr)
       end;
  end if;

  if obj ? '$field' or obj ? '$op' or obj ? '$value' then
    expl_filt := _pg_json_query._filt_to_json(_pg_json_query._filt_type(
      obj->>'$field',
      obj->>'$op',
      obj->'$value'
    ));

    arr := case
      when arr is null then
        _pg_json_query._build_array(expl_filt)
      else
        _pg_json_query._jsonb_array_concat(arr, expl_filt)
      end;
  end if;

  return coalesce(arr, '[]');
end;
$$;


-- Wrapper around parse_parse_filter_obj_to_json().
create or replace function _pg_json_query._parse_filter(typ jsonb, obj jsonb)
returns jsonb
language sql
stable
cost 1000000
as $$
  select _pg_json_query._parse_filter_obj_to_json(obj);
$$;


-- Filter function available to all tables which implement
-- _filter_row_column_impl() for their row-type.
create or replace function jq_filter(row_ anyelement, filter_obj jsonb)
returns boolean
language sql
stable
cost 1000000 -- same as cost of _filter_row_impl
as $$
  select _pg_json_query._filter_row_impl(
    row_,
    _pg_json_query._parse_filter_obj_to_json(filter_obj)
  );
$$;


create or replace function jq_and_filters(f1 jsonb, f2 jsonb)
returns jsonb language sql immutable as $$
  select
    case
      when f1 is null or f1 = '{}' then
        coalesce(f2, '{}')
      when f2 is null or f2 = '{}' then
        f1
      when f1 = f2 then
        f1
      else
        concat('{"$and": [', f1, ', ', f2, ']}')::jsonb
    end;
$$;



create or replace function json_query._apply_filter(col anyelement, filt jsonb,
                                                    _coltyp anyelement default null)
returns boolean
language sql immutable
as $$ select json_query._apply_op(filt->>'op', col, filt); $$;



-- Filter expressions applied to JSONB columns may also optionally specify
-- a "path" that should either be a JSONB array or a string representing the
-- path to a nested value to use as the basis for the filter.
create or replace function json_query._apply_filter(col jsonb, filt jsonb)
returns boolean
language sql immutable
as $$
  select case
    when filt->'path' = 'null' then
      json_query._apply_filter(col, filt, null::jsonb)
    when filt->'path_is_text' = 'true' then
      json_query._apply_filter(
        json_query._column_extract_path_text(col, filt->'path'),
        filt,
        null
      )
    else
      json_query._apply_filter(
        json_query._column_extract_path(col, filt->'path'),
        filt,
        null
      )
    end;
$$;




/*
The following functions are partial implementations for additional functionality
defined on a per-table basis. 
*/

create or replace function json_query._filter_row_column_router(row_ anyelement, filt jsonb)
returns boolean language sql immutable as $$
  select case
    when filt is null then
      true
    else 
      json_query._filter_row_column_router_impl(filt->>'field', row_, filt)
    end;
$$;



create or replace function json_query._filter_row_impl(
  row_ anyelement,
  filts jsonb
)
returns boolean
language sql immutable
as $$
  select
    json_query._filter_row_column_router(row_, filts->0) and
    json_query._filter_row_column_router(row_, filts->1) and
    json_query._filter_row_column_router(row_, filts->2) and
    json_query._filter_row_column_router(row_, filts->3) and
    json_query._filter_row_column_router(row_, filts->4) and
    json_query._filter_row_column_router(row_, filts->5) and
    json_query._filter_row_column_router(row_, filts->6) and
    json_query._filter_row_column_router(row_, filts->7) and
    json_query._filter_row_column_router(row_, filts->8) and
    json_query._filter_row_column_router(row_, filts->9) and
    json_query._filter_row_column_router(row_, filts->10) and
    json_query._filter_row_column_router(row_, filts->11);
$$;


create or replace function json_query._parse_filter_obj_to_json(obj jsonb)
returns jsonb language plpgsql immutable as $$
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
        json_query._parse_filter_obj_to_json(o)
      ) as o
      from jsonb_array_elements(obj->'$and') o
    ) sq;

    arr := and_arr;
  end if;
  
  select json_agg(
    json_query._filt_to_json(json_query._filt_type(key, value))
  ) into dj_arr
  from jsonb_each(obj)
  where left(key, 1) != '$';
  
  if dj_arr is not null then
     arr := case
       when arr is null then
         dj_arr
       else
         json_query._jsonb_array_concat(arr, dj_arr)
       end;
  end if;
  
  if obj ? '$field' or obj ? '$op' or obj ? '$value' then
    expl_filt := json_query._filt_type(
      obj->>'$field',
      obj->>'$op',
      obj->'$value'
    );
    
    arr := case
      when arr is null then
        json_query._build_array(expl_filt)
      else
        json_query._jsonb_array_concat(arr, expl_filt)
      end;
  end if;
  
  return coalesce(arr, '[]');
end;
$$;



create or replace function json_query._parse_filter(typ jsonb, obj jsonb)
returns jsonb language sql immutable as $$
  select json_query._parse_filter_obj_to_json(obj);
$$;


create or replace function json_query.filter(row_ anyelement, filter_obj jsonb)
returns boolean
language sql immutable
as $$
  select json_query._filter_row_impl(
    row_,
    json_query._parse_filter_obj_to_json(filter_obj)
  );
$$;

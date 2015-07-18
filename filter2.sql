
create or replace function json_query._apply_filter(
  col anyelement,
  filt json_query._filt_type,
  -- Since all of the _cast_column_value(col, val) functions only need
  -- the type info of the column, by always passing null as the column
  -- you can make it easier to Postgres to optimize.            
  _coltype anyelement default null
)
returns boolean
language sql immutable
as $$
  select
    case filt.op
      when 'eq' then
        case 
          when filt.value = 'null' then
            col is null
          else
            col = json_query._cast_column_value(_coltype, filt.value)
          end
      when 'ne' then
        case
          when filt.value = 'null' then
            col is not null
          else
            col != json_query._cast_column_value(_coltype, filt.value)
          end
      when 'gt' then col >  json_query._cast_column_value(_coltype, filt.value)
      when 'lt' then col <  json_query._cast_column_value(_coltype, filt.value)
      when 'ge' then col >= json_query._cast_column_value(_coltype, filt.value)
      when 'le' then col <= json_query._cast_column_value(_coltype, filt.value)
      /*
      when 'in' then json_query._col_in_jsonb(col, filt.value)
      when 'notin' then not json_query._col_in_jsonb(col, filt.value)
      when 'like' then
        json_query._force_text(col) like filt->>'value'
      when 'ilike' then
        json_query._force_text(col) ilike filt->>'value'
      when 'startswith' then
        json_query._force_text(col) like (filt->>'value') || '%'
      when 'istartswith' then
        json_query._force_text(col) ilike (filt->>'value') || '%'
*/
/*
      when 'exists' then
        -- Presumably col is JSON
        col ? json_query._json_string_to_text(filt.value)
      when 'notexists' then
        not (col ? json_query._json_string_to_text(filt.value))
*/
      else
        false
      end
$$;



create or replace function json_query._apply_filter(col jsonb, filt json_query._filt_type)
returns boolean
language sql immutable
as $$
  select case filt.field_arr_len
    when 1 then
      json_query._apply_filter(col, filt, null::jsonb)
    when 2 then
      json_query._apply_filter_to_col_path(
        col, filt.field_arr[2], filt.field_path_is_text, filt
      )
    else
      json_query._apply_filter_to_col_path(
        col, filt.field_arr[2:(filt.field_arr_len)],
        filt.field_path_is_text, filt
      )
    end;
$$;





create or replace function json_query._filter_row_column_router(row_ anyelement, filt json_query._filt_type)
returns boolean language sql immutable as $$
  select case
    when filt is null then
      true
    else
      json_query._filter_row_column_router_impl(json_query._get_column(filt), row_, filt)
    end;
$$;


create or replace function json_query._filter_row_impl(
  row_ anyelement,
  filts json_query._filt_type[]
)
returns boolean
language sql immutable
as $$
  select
    json_query._filter_row_column_router(row_, filts[1]) and
    json_query._filter_row_column_router(row_, filts[2]) /*and
    json_query._filter_row_column_router_impl(row_, filts->2) and
    json_query._filter_row_column_router_impl(row_, filts->3) and
    json_query._filter_row_column_router_impl(row_, filts->4) and
    json_query._filter_row_column_router_impl(row_, filts->5) and
    json_query._filter_row_column_router_impl(row_, filts->6) and
    json_query._filter_row_column_router_impl(row_, filts->7) and
    json_query._filter_row_column_router_impl(row_, filts->8) and
    json_query._filter_row_column_router_impl(row_, filts->9) and
    json_query._filter_row_column_router_impl(row_, filts->10) and
    json_query._filter_row_column_router_impl(row_, filts->11)*/;
$$;


create or replace function json_query._parse_filter_obj_to_filts_array(obj jsonb)
returns json_query._filt_type[] language plpgsql immutable as $$
declare
  and_arr json_query._filt_type[];
  dj_arr json_query._filt_type[];
  expl_filt json_query._filt_type;
  arr json_query._filt_type[];
begin
  if obj ? '$and' then
    select array_agg(o)::jsonb into and_arr
    from (
      select jsonb_array_elements(
        json_query._clean_filter_obj(o)
      ) as o
      from jsonb_array_elements(obj->'$and') o
    ) sq;

    arr := and_arr;
  end if;
  
  select array_agg(json_query._filt_type(key, value)) into dj_arr
  from jsonb_each(obj)
  where left(key, 1) != '$';
  
  if dj_arr is not null then
     arr := case
       when arr is null then
         dj_arr
       else
         arr || dj_arr
       end;
  end if;
  
  if obj ? '$field' or obj ? '$op' or obj ? '$value' then
    expl_filt := json_query._filt_type(
      obj->>'$field',
      (obj->>'$op')::json_query._op_type,
      obj->'$value'
    );

    arr := case
      when arr is null then
        array[expl_filt]
      else
        arr || expl_filt
      end;
  end if;
  
  return coalesce(arr, '{}');
end;
$$;



create or replace function json_query.filter2(row_ anyelement, filter_obj jsonb)
returns boolean
language sql immutable
as $$
  select json_query._filter_row_impl(
    row_,
    json_query._parse_filter_obj_to_filts_array(filter_obj)
  );
$$;

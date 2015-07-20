/*
_col_value(valtyp, row, fld)

Return either text/jsonb (depending on valtyp) value representing the row's
value for the specified field.

**** Requires implementation to be used with specific row types.
*/

-- text version.
-- To use with a row type, implement json_col_base_value_impl(text, <rowtype>, text)
-- that returns a textual representation of the specified column.
create or replace function json_query._col_value(valtyp text, row_ anyelement, fld text)
returns text
language sql immutable
as $$
  select json_query._col_value_impl(valtyp, row_, fld)::text;
$$;

-- jsonb version.
-- To use with a row type, implement json_col_value_impl(jsonb, <rowtype>, text)
-- that returns a JSON representation of the specified column.
create or replace function json_query._col_value(valtyp jsonb, row_ anyelement, fld text)
returns jsonb
language sql immutable
as $$
  select json_query._col_value_impl(valtyp, row_, fld)::jsonb;
$$;


create or replace function json_query._column_value_for_type_fldref(
  valtyp jsonb,
  row_ anyelement,
  fld json_query._field_type
)
returns jsonb
language sql immutable
as $$
  select case fld.path_arr_len
    when 0 then
      json_query._col_value(valtyp, row_, fld.column_)
    else
      json_query._field_extract_from_column(
        fld,
        json_query._col_value(valtyp, row_, fld.column_)
      )
    end;
$$;


create or replace function json_query._column_value_for_type_fldref(
  valtyp text,
  row_ anyelement,
  fld json_query._field_type
)
returns text
language sql immutable
as $$
  select case fld.path_arr_len
    when 0 then
      json_query._col_value(valtyp, row_, fld.column_)
    else
      json_query._field_extract_text_from_column(
        fld,
        json_query._col_value(null::jsonb, row_, fld.column_)
      )
    end;
$$;



create or replace function json_query._column_value_for_type(valtyp text, row_ anyelement, fldref text)
returns text
language sql immutable
as $$
  select json_query._column_value_for_type_fldref(
    valtyp, row_, json_query._field_type(fldref)
  );
$$;


create or replace function json_query._column_value_for_type(valtyp jsonb, row_ anyelement, fldref text)
returns jsonb
language sql immutable
as $$
  select json_query._column_value_for_type_fldref(
    valtyp, row_, json_query._field_type(fldref)
  );
$$;


create or replace function json_query.column_value(row_ anyelement, fldref text)
returns jsonb
language sql immutable
as $$
  select json_query._column_value_for_type(null::jsonb, row_, fldref);
$$;


create or replace function json_query.column_value_text(row_ anyelement, fldref text)
returns text
language sql immutable
as $$
  select json_query._column_value_for_type(null::text, row_, fldref);
$$;



/*
column_expr(row, fldexpr)
*/
create or replace function json_query.column_expr(row_ anyelement, fldexpr text)
returns jsonb
language sql immutable
as $$
  select json_query.column_value(row_, fldexpr);
$$;

create or replace function json_query.column_expr(row_ anyelement, fldexpr_arr text[])
returns jsonb
language sql immutable
as $$
  select coalesce(json_agg(json_query.column_value(row_, el) order by idx),
                  '[]')::jsonb
  from unnest(fldexpr_arr) with ordinality o(el, idx);
$$;


create or replace function json_query._column_expr_jsonb_arr(row_ anyelement, arr jsonb)
returns jsonb
language sql immutable
as $$
  select coalesce(json_agg(json_query.column_value(row_, el) order by idx)::jsonb, '[]')
  from jsonb_array_elements_text(arr) with ordinality o(el, idx);
$$;


create or replace function json_query.column_expr(row_ anyelement, fldexpr jsonb)
returns jsonb
language sql immutable
as $$
  select case jsonb_typeof(fldexpr)
    when 'array' then
      case jsonb_array_length(fldexpr)
        when 0 then
          '[]'
        when 1 then
           json_query._build_array(json_query._col_value_impl(null::jsonb, row_, fldexpr->>0))
        when 2 then
           json_query._build_array(
             json_query._col_value_impl(null::jsonb, row_, fldexpr->>0),
             json_query._col_value_impl(null::jsonb, row_, fldexpr->>1)
           )::jsonb
        when 3 then
           json_query._build_array(
             json_query._col_value_impl(null::jsonb, row_, fldexpr->>0),
             json_query._col_value_impl(null::jsonb, row_, fldexpr->>1),
             json_query._col_value_impl(null::jsonb, row_, fldexpr->>2)
           )
        when 4 then
           json_query._build_array(
             json_query._col_value_impl(null::jsonb, row_, fldexpr->>0),
             json_query._col_value_impl(null::jsonb, row_, fldexpr->>1),
             json_query._col_value_impl(null::jsonb, row_, fldexpr->>2),
             json_query._col_value_impl(null::jsonb, row_, fldexpr->>3)
           )
        when 5 then
           json_query._build_array(
             json_query._col_value_impl(null::jsonb, row_, fldexpr->>0),
             json_query._col_value_impl(null::jsonb, row_, fldexpr->>1),
             json_query._col_value_impl(null::jsonb, row_, fldexpr->>2),
             json_query._col_value_impl(null::jsonb, row_, fldexpr->>3),
             json_query._col_value_impl(null::jsonb, row_, fldexpr->>4)
           )
        else
          json_query._column_expr_jsonb_arr(row_, fldexpr)
        end
    else
      json_query.column_value(row_, json_query._json_string_to_text(fldexpr))
    end;
$$;




/*
column_expr_text_array(row, fldexpr_array)
*/
create or replace function json_query.column_expr_text_array(row_ anyelement, fldexpr_arr text[])
returns text[]
language sql immutable
as $$
  select coalesce(array_agg(json_query.column_value_text(row_, el) order by idx), '{}')::text[]
  from unnest(fldexpr_arr) with ordinality o(el, idx);
$$;


/*
column_expr_text(row, fldexpr)
*/
create or replace function json_query.column_expr_text(row_ anyelement, fldexpr text)
returns text
language sql immutable
as $$
  select json_query.column_value_text(row_, fldexpr);
$$;

create or replace function json_query.column_expr_text(row_ anyelement, fldexpr_arr text[])
returns text
language sql immutable
as $$
  select json_query.column_expr_text_array(row_, fldexpr_arr)::text;
$$;

create or replace function json_query.column_expr_text(row_ anyelement, fldexpr jsonb)
returns text
language sql immutable
as $$
  select case jsonb_typeof(fldexpr)
    when 'array' then
      json_query.column_expr_text_array(
        row_,
        json_query._jsonb_arr_to_text_arr(fldexpr)
      )::text
    else
      json_query.column_value_text(
        row_,
        json_query._json_string_to_text(fldexpr)
      )
    end;
$$;


create or replace function json_query.concat_column_exprs(e1 jsonb, e2 jsonb)
returns jsonb
language sql immutable
as $$
  select case
    when e1 is null then coalesce(e2, '[]')
    when e2 is null then coalesce(e1, '[]')
    else
      -- Both non-null.
      json_query._jsonb_array_concat(e1, e2)
    end;
$$;

/*
_col_value(valtyp, row, fld)

Return either text/jsonb (depending on valtyp) value representing the row's
value for the specified field.

**** Requires implementation to be used with specific row types.
*/



-- _jq_extract_helper(row<anyelement>, fld<fldexpr|fldtype>, typ<*>)
create or replace function _pg_json_query._jq_val_helper(
  row_ anyelement,
  fld _pg_json_query._field_type,
  typ jsonb
)
returns jsonb
language sql
stable
cost 1000000
as $$
  select case fld.path_arr_len
    when 0 then
      _pg_json_query._jq_col_val_impl(row_, fld.column_, typ)
    else
      _pg_json_query._field_extract_from_column(
        fld,
        _pg_json_query._jq_col_val_impl(row_, fld.column_, typ)
      )
    end;
$$;

create or replace function _pg_json_query._jq_val_helper(
  row_ anyelement,
  fld _pg_json_query._field_type,
  typ json
)
returns json
language sql
stable
cost 1000000
as $$
  select case fld.path_arr_len
    when 0 then
      _pg_json_query._jq_col_val_impl(row_, fld.column_, typ)
    else
      _pg_json_query._field_extract_from_column(
        fld,
        _pg_json_query._jq_col_val_impl(row_, fld.column_, typ)
      )
    end;
$$;

create or replace function _pg_json_query._jq_val_helper(
  row_ anyelement,
  fld _pg_json_query._field_type,
  typ text
)
returns text
language sql
stable
cost 1000000
as $$
  select case fld.path_arr_len
    when 0 then
      _pg_json_query._jq_col_val_impl(row_, fld.column_, typ)
    else
      _pg_json_query._field_extract_text_from_column(
        fld,
        _pg_json_query._jq_col_val_impl(row_, fld.column_, null::jsonb)
      )
    end;
$$;

create or replace function _pg_json_query._jq_val_helper(
  row_ anyelement,
  fldexpr text,
  typ jsonb
)
returns jsonb
language sql
stable
cost 100000
as $$
  select _pg_json_query._jq_val_helper(
    row_, _pg_json_query._field_type(fldexpr), typ
  );
$$;

create or replace function _pg_json_query._jq_val_helper(
  row_ anyelement,
  fldexpr text,
  typ json
)
returns json
language sql
stable
cost 1000000
as $$
  select _pg_json_query._jq_val_helper(
    row_, _pg_json_query._field_type(fldexpr), typ
  );
$$;

create or replace function _pg_json_query._jq_val_helper(
  row_ anyelement,
  fldexpr text,
  typ text
)
returns text
language sql
stable
cost 1000000
as $$
  select _pg_json_query._jq_val_helper(
    row_, _pg_json_query._field_type(fldexpr), typ
  );
$$;



-- text version.
-- To use with a row type, implement json_col_base_value_impl(text, <rowtype>, text)
-- that returns a textual representation of the specified column.
--create or replace function _pg_json_query._col_value(valtyp text, row_ anyelement, fld text)
create or replace function jq_val(row_ anyelement, colname text, typ text)
returns text
language sql
stable
cost 1000000
as $$
  select _pg_json_query._jq_val_helper(row_, colname, typ);
$$;


create or replace function jq_val(row_ anyelement, colname text, typ jsonb)
returns jsonb language sql stable as $$
  select _pg_json_query._jq_val_helper(row_, colname, typ);
$$;


create or replace function jq_val(row_ anyelement, colname text, typ json)
returns json
language sql
stable
cost 1000000
as $$
  select _pg_json_query._jq_val_helper(row_, colname, typ);
$$;


-- Helper for jq_val(row_, jsonb_array) when the arrays are long.
create or replace function _pg_json_query._jq_val_jsonb_arr(row_ anyelement, arr jsonb)
returns jsonb
language sql
stable
cost 1000000
as $$
  select coalesce(json_agg(jq_val(row_, el, null::json)
                           order by idx)::jsonb, '[]')
  from jsonb_array_elements_text(arr) with ordinality o(el, idx);
$$;


create or replace function jq_val(row_ anyelement, colexpr jsonb, typ jsonb)
returns jsonb
language plpgsql
stable
cost 1000000
as $$
declare
  exprtyp text;
  arrlen int;
begin
  if colexpr = '[]' then
    return '[]';
  end if;

  exprtyp := jsonb_typeof(colexpr);

  if jsonb_typeof(colexpr) != 'array' then
    return _pg_json_query._jq_val_helper(
      row_,
      _pg_json_query._json_string_to_text(colexpr),
      typ
    );
  end if;

  arrlen := jsonb_array_length(colexpr);

  if arrlen = 1 then
     return _pg_json_query._build_array(
       coalesce(
         _pg_json_query._jq_val_helper(row_, colexpr->>0, typ), 'null'));
  elsif arrlen = 2 then
     return _pg_json_query._build_array(
       coalesce(
         _pg_json_query._jq_val_helper(row_, colexpr->>0, typ), 'null'),
       coalesce(
         _pg_json_query._jq_val_helper(row_, colexpr->>1, typ), 'null'));
  elsif arrlen = 3 then
     return _pg_json_query._build_array(
       coalesce(
         _pg_json_query._jq_val_helper(row_, colexpr->>0, typ), 'null'),
       coalesce(
         _pg_json_query._jq_val_helper(row_, colexpr->>1, typ), 'null'),
       coalesce(
         _pg_json_query._jq_val_helper(row_, colexpr->>2, typ), 'null'));
  elsif arrlen = 4 then
     return _pg_json_query._build_array(
       coalesce(
         _pg_json_query._jq_val_helper(row_, colexpr->>0, typ), 'null'),
       coalesce(
         _pg_json_query._jq_val_helper(row_, colexpr->>1, typ), 'null'),
       coalesce(
         _pg_json_query._jq_val_helper(row_, colexpr->>2, typ), 'null'),
       coalesce(
         _pg_json_query._jq_val_helper(row_, colexpr->>3, typ), 'null'));
  elsif arrlen = 5 then
     return _pg_json_query._build_array(
       coalesce(
         _pg_json_query._jq_val_helper(row_, colexpr->>0, typ), 'null'),
       coalesce(
         _pg_json_query._jq_val_helper(row_, colexpr->>1, typ), 'null'),
       coalesce(
         _pg_json_query._jq_val_helper(row_, colexpr->>2, typ), 'null'),
       coalesce(
         _pg_json_query._jq_val_helper(row_, colexpr->>3, typ), 'null'),
       coalesce(
         _pg_json_query._jq_val_helper(row_, colexpr->>4, typ), 'null'));
  else
    return _pg_json_query._jq_val_jsonb_arr(row_, colexpr);
  end if;
end;
$$;


-- If type is omitted, default to JSONB.
create or replace function jq_val(row_ anyelement, colname text)
returns jsonb
language sql
stable
cost 1000000
as $$
  select _pg_json_query._jq_val_helper(row_, colname, null::jsonb);
$$;

-- If type is omitted, default to JSONB.
create or replace function jq_val(row_ anyelement, colexpr jsonb)
returns jsonb
language sql
stable
cost 1000000
as $$
  select jq_val(row_, colexpr, null::jsonb);
$$;

create or replace function jq_val_text(row_ anyelement, colname text)
returns text
language sql
stable
cost 1000000
as $$
  select _pg_json_query._jq_val_helper(row_, colname, null::text);
$$;



create or replace function jq_val_text_array(row_ anyelement, arr text[])
returns text[]
language sql
stable
cost 1000000
as $$
  select coalesce(array_agg(jq_val_text(row_, el) order by idx), '{}')::text[]
  from unnest(arr) with ordinality o(el, idx);
$$;


create or replace function jq_val(row_ anyelement, arr text[], typ jsonb)
returns jsonb
language sql
stable
cost 1000000
as $$
  select jq_val(row_, to_json(arr)::jsonb, typ);
$$;


create or replace function jq_val(row_ anyelement, arr text[], typ text)
returns text[]
language sql
stable
cost 1000000
as $$
  select jq_val_text_array(row_, arr);
$$;


create or replace function jq_concat_val_args(e1 jsonb, e2 jsonb)
returns jsonb
language sql immutable
as $$
  select case
    when e1 is null then coalesce(e2, '[]')
    when e2 is null then coalesce(e1, '[]')
    else
      -- Both non-null.
      _pg_json_query._jsonb_array_concat(e1, e2)
    end;
$$;


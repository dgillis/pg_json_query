
-- eq
create or replace function json_query._eq(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select case when filt->'value' = 'null' then
    col is null
  else
    col = json_query._cast_column_value(_coltyp, filt->'value')
  end;
$$;


-- ne
create or replace function json_query._ne(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select case when filt->'value' = 'null' then
    col is not null
  else
    col != json_query._cast_column_value(_coltyp, filt->'value')
  end;
$$;


-- gt
create or replace function json_query._gt(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select col > json_query._cast_column_value(_coltyp, filt->'value');
$$;


-- lt
create or replace function json_query._lt(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select col < json_query._cast_column_value(_coltyp, filt->'value');
$$;


-- ge
create or replace function json_query._ge(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select col >= json_query._cast_column_value(_coltyp, filt->'value');
$$;


-- le
create or replace function json_query._le(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select col <= json_query._cast_column_value(_coltyp, filt->'value');
$$;


-- in (jsonb)
create or replace function json_query._in(col jsonb, filt jsonb, _coltyp jsonb default null)
returns boolean language sql immutable as $$
  select json_query._col_in_jsonb(col, filt->'value');
$$;

-- in (anyelement)
create or replace function json_query._in(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select json_query._col_in_jsonb(to_json(col)::jsonb, filt->'value');
$$;


-- notin
create or replace function json_query._notin(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select not json_query._in(col, filt, _coltyp);
$$;
 

-- like
create or replace function json_query._like(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$ select json_query._like_helper(col, filt->>'value'); $$;


-- ilike
create or replace function json_query._ilike(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$ select json_query._ilike_helper(col, filt->>'value'); $$;


-- startswith
create or replace function json_query._startswith(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select json_query._like_helper(col, (filt->>'value') || '%');
$$;


-- istartswith
create or replace function json_query._istartswith(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select json_query._ilike_helper(col, (filt->>'value') || '%');
$$;



-- endswith
create or replace function json_query._endswith(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select json_query._like_helper(col, '%' || (filt->>'value'));
$$;


-- iendswith
create or replace function json_query._iendswith(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select json_query._ilike_helper(col, '%' || (filt->>'value'));
$$;



-- exists
create or replace function json_query._exists(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select col ? filt->>'value';
$$;


-- notexists.
create or replace function json_query._notexists(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select not json_query._exists(col, filt, _coltyp);
$$;



create or replace function json_query._apply_op(op text, col anyelement, filt jsonb)
returns boolean language sql immutable as $$
  select case op
    when 'eq' then json_query._eq(col, filt)
    when 'ne' then json_query._ne(col, filt)
    when 'gt' then json_query._gt(col, filt)
    when 'lt' then json_query._lt(col, filt)
    when 'ge' then json_query._ge(col, filt)
    when 'le' then json_query._le(col, filt)
    when 'in' then json_query._in(col, filt)
    when 'notin' then json_query._notin(col, filt)
    when 'like' then json_query._like(col, filt)
    when 'ilike' then json_query._ilike(col, filt)
    when 'startswith' then json_query._startswith(col, filt)
    when 'istartswith' then json_query._istartswith(col, filt)
    when 'endswith' then json_query._endswith(col, filt)
    when 'iendswith' then json_query._iendswith(col, filt)
    when 'exists' then json_query._exists(col, filt)
    when 'notexists' then json_query._notexists(col, filt)
    -- Aliases
    when 'gte' then json_query._ge(col, filt)
    when 'lte' then json_query._le(col, filt)
    else null
    end;
$$;

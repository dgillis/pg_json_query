
-- eq
create function _pg_json_query._eq(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select case when filt->'value' = 'null' then
    col is null
  else
    col = _pg_json_query._cast_column_value(_coltyp, filt->'value')
  end;
$$;


-- ne
create function _pg_json_query._ne(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select case when filt->'value' = 'null' then
    col is not null
  else
    col != _pg_json_query._cast_column_value(_coltyp, filt->'value')
  end;
$$;


-- gt
create function _pg_json_query._gt(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select col > _pg_json_query._cast_column_value(_coltyp, filt->'value');
$$;


-- lt
create function _pg_json_query._lt(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select col < _pg_json_query._cast_column_value(_coltyp, filt->'value');
$$;


-- ge
create function _pg_json_query._ge(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select col >= _pg_json_query._cast_column_value(_coltyp, filt->'value');
$$;


-- le
create function _pg_json_query._le(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select col <= _pg_json_query._cast_column_value(_coltyp, filt->'value');
$$;


-- in (jsonb)
create function _pg_json_query._in(col jsonb, filt jsonb, _coltyp jsonb default null)
returns boolean language sql immutable as $$
  select _pg_json_query._col_in_jsonb(col, filt->'value');
$$;

-- in (anyelement)
create function _pg_json_query._in(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select _pg_json_query._col_in_jsonb(to_json(col)::jsonb, filt->'value');
$$;


-- notin
create function _pg_json_query._notin(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select not _pg_json_query._in(col, filt, _coltyp);
$$;
 

-- like
create function _pg_json_query._like(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$ select _pg_json_query._like_helper(col, filt->>'value'); $$;


-- ilike
create function _pg_json_query._ilike(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$ select _pg_json_query._ilike_helper(col, filt->>'value'); $$;


-- startswith
create function _pg_json_query._startswith(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select _pg_json_query._like_helper(col, (filt->>'value') || '%');
$$;


-- istartswith
create function _pg_json_query._istartswith(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select _pg_json_query._ilike_helper(col, (filt->>'value') || '%');
$$;



-- endswith
create function _pg_json_query._endswith(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select _pg_json_query._like_helper(col, '%' || (filt->>'value'));
$$;


-- iendswith
create function _pg_json_query._iendswith(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select _pg_json_query._ilike_helper(col, '%' || (filt->>'value'));
$$;



-- exists
create function _pg_json_query._exists(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select col ? (filt->>'value');
$$;


-- notexists.
create function _pg_json_query._notexists(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select not _pg_json_query._exists(col, filt, _coltyp);
$$;



create function _pg_json_query._apply_op(op text, col anyelement, filt jsonb)
returns boolean language sql immutable as $$
  select case op
    when 'eq' then _pg_json_query._eq(col, filt)
    when 'ne' then _pg_json_query._ne(col, filt)
    when 'gt' then _pg_json_query._gt(col, filt)
    when 'lt' then _pg_json_query._lt(col, filt)
    when 'ge' then _pg_json_query._ge(col, filt)
    when 'le' then _pg_json_query._le(col, filt)
    when 'in' then _pg_json_query._in(col, filt)
    when 'notin' then _pg_json_query._notin(col, filt)
    when 'like' then _pg_json_query._like(col, filt)
    when 'ilike' then _pg_json_query._ilike(col, filt)
    when 'startswith' then _pg_json_query._startswith(col, filt)
    when 'istartswith' then _pg_json_query._istartswith(col, filt)
    when 'endswith' then _pg_json_query._endswith(col, filt)
    when 'iendswith' then _pg_json_query._iendswith(col, filt)
    when 'exists' then _pg_json_query._exists(col, filt)
    when 'notexists' then _pg_json_query._notexists(col, filt)
    -- Aliases
    when 'gte' then _pg_json_query._ge(col, filt)
    when 'lte' then _pg_json_query._le(col, filt)
    else null
    end;
$$;

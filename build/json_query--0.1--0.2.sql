-- contains (i.e., "@>" gin operator)

-- Fallback implementation: attempt to cast the textual representation of the value to whatever
-- the column's type is.
create function _pg_json_query._contains(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select col @> _pg_json_query._cast(filt->>'value', _coltyp)
$$;

-- jsonb implementation.
create function _pg_json_query._contains(col jsonb, filt jsonb, _coltyp jsonb default null)
returns boolean language sql immutable as $$ select col @> filt->'value' $$;

-- json implementation
create function _pg_json_query._contains(col json, filt jsonb, _coltyp json default null)
returns boolean language sql immutable as $$ select col::jsonb @> filt->'value' $$;


-- contained ("<@"). Functions analoguous to the contains functions.
create function _pg_json_query._contained(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select col <@ _pg_json_query._cast(filt->>'value', _coltyp)
$$;

-- jsonb implementation.
create function _pg_json_query._contained(col jsonb, filt jsonb, _coltyp jsonb default null)
returns boolean language sql immutable as $$ select col <@ filt->'value' $$;

-- json implementation
create function _pg_json_query._contained(col json, filt jsonb, _coltyp json default null)
returns boolean language sql immutable as $$ select col::jsonb <@ filt->'value' $$;


-- not contains
create function _pg_json_query._notcontains(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select not _pg_json_query._contains(col, filt, _coltyp);
$$;


-- not contained
create function _pg_json_query._notcontained(col anyelement, filt jsonb, _coltyp anyelement default null)
returns boolean language sql immutable as $$
  select not _pg_json_query._contains(col, filt, _coltyp);
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
    when 'contains' then _pg_json_query._contains(col, filt)
    when 'notcontains' then _pg_json_query._notcontains(col, filt)
    when 'contained' then _pg_json_query._contained(col, filt)
    when 'notcontained' then _pg_json_query._notcontained(col, filt)
    -- Aliases
    when 'gte' then _pg_json_query._ge(col, filt)
    when 'lte' then _pg_json_query._le(col, filt)
    else null
    end;
$$;

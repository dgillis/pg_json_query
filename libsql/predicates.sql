
-- eq
create function _pg_json_query._apply_pred__eq(col anyelement, filt jsonb,
                                               _coltyp anyelement default null)
returns boolean language sql stable as $$
  select case when filt->'value' = 'null' then
    col is null
  else
    col = _pg_json_query._cast_column_value(_coltyp, filt->'value')
  end;
$$;


-- ne
create function _pg_json_query._apply_pred__ne(col anyelement, filt jsonb,
                                               _coltyp anyelement default null)
returns boolean language sql stable as $$
  select case when filt->'value' = 'null' then
    col is not null
  else
    col != _pg_json_query._cast_column_value(_coltyp, filt->'value')
  end;
$$;


-- gt
create function _pg_json_query._apply_pred__gt(col anyelement, filt jsonb,
                                               _coltyp anyelement default null)
returns boolean language sql stable as $$
  select col > _pg_json_query._cast_column_value(_coltyp, filt->'value');
$$;


-- lt
create function _pg_json_query._apply_pred__lt(col anyelement, filt jsonb,
                                               _coltyp anyelement default null)
returns boolean language sql stable as $$
  select col < _pg_json_query._cast_column_value(_coltyp, filt->'value');
$$;


-- ge
create function _pg_json_query._apply_pred__ge(col anyelement, filt jsonb,
                                               _coltyp anyelement default null)
returns boolean language sql stable as $$
  select col >= _pg_json_query._cast_column_value(_coltyp, filt->'value');
$$;


-- le
create function _pg_json_query._apply_pred__le(col anyelement, filt jsonb,
                                               _coltyp anyelement default null)
returns boolean language sql stable as $$
  select col <= _pg_json_query._cast_column_value(_coltyp, filt->'value');
$$;


-- in (jsonb)
create function _pg_json_query._apply_pred__in(col jsonb, filt jsonb,
                                               _coltyp jsonb default null)
returns boolean language sql stable as $$
  select _pg_json_query._col_in_jsonb(col, filt->'value');
$$;

-- in (anyelement)
create function _pg_json_query._apply_pred__in(col anyelement, filt jsonb,
                                               _coltyp anyelement default null)
returns boolean language sql stable as $$
  select _pg_json_query._col_in_jsonb(to_json(col)::jsonb, filt->'value');
$$;


-- notin
create function _pg_json_query._apply_pred__notin(col anyelement, filt jsonb,
                                                  _coltyp anyelement default null)
returns boolean language sql stable as $$
  select not _pg_json_query._in(col, filt, _coltyp);
$$;
 

-- like
create function _pg_json_query._apply_pred__like(col anyelement, filt jsonb,
                                                 _coltyp anyelement default null)
returns boolean language sql stable as $$ select _pg_json_query._like_helper(col, filt->>'value'); $$;


-- ilike
create function _pg_json_query._apply_pred__ilike(col anyelement, filt jsonb,
_coltyp anyelement default null)
returns boolean language sql stable as $$ select _pg_json_query._ilike_helper(col, filt->>'value'); $$;


-- startswith
create function _pg_json_query._apply_pred__startswith(col anyelement, filt jsonb,
                                                       _coltyp anyelement default null)
returns boolean language sql stable as $$
  select _pg_json_query._like_helper(col, (filt->>'value') || '%');
$$;


-- istartswith
create function _pg_json_query._apply_pred__istartswith(col anyelement, filt jsonb,
                                                        _coltyp anyelement default null)
returns boolean language sql stable as $$
  select _pg_json_query._ilike_helper(col, (filt->>'value') || '%');
$$;



-- endswith
create function _pg_json_query._apply_pred__endswith(col anyelement, filt jsonb,
                                                     _coltyp anyelement default null)
returns boolean language sql stable as $$
  select _pg_json_query._like_helper(col, '%' || (filt->>'value'));
$$;


-- iendswith
create function _pg_json_query._apply_pred__iendswith(col anyelement, filt jsonb,
                                                      _coltyp anyelement default null)
returns boolean language sql stable as $$
  select _pg_json_query._ilike_helper(col, '%' || (filt->>'value'));
$$;



-- exists
create function _pg_json_query._apply_pred__exists(col anyelement, filt jsonb,
                                                   _coltyp anyelement default null)
returns boolean language sql stable as $$
  select col ? (filt->>'value');
$$;


-- notexists.
create function _pg_json_query._apply_pred__notexists(col anyelement, filt jsonb,
                                                      _coltyp anyelement default null)
returns boolean language sql stable as $$
  select not _pg_json_query._exists(col, filt, _coltyp);
$$;



-- contains (i.e., "@>" gin operator)

-- Fallback implementation: attempt to cast the textual representation of the value to whatever
-- the column's type is.
create function _pg_json_query._apply_pred__contains(col anyelement, filt jsonb,
                                                     _coltyp anyelement default null)
returns boolean language sql stable as $$
  select col @> _pg_json_query._cast(filt->>'value', _coltyp);
$$;

-- jsonb implementation.
create function _pg_json_query._apply_pred__contains(col jsonb, filt jsonb,
                                                     _coltyp jsonb default null)
returns boolean language sql stable as $$ select col @> (filt->'value'); $$;

-- json implementation
create function _pg_json_query._apply_pred__contains(col json, filt jsonb,
                                                     _coltyp json default null)
returns boolean language sql stable as $$ select col::jsonb @> (filt->'value'); $$;


-- contained ("<@"). Functions analoguous to the contains functions.
create function _pg_json_query._apply_pred__contained(col anyelement, filt jsonb,
                                                      _coltyp anyelement default null)
returns boolean language sql stable as $$
  select col <@ _pg_json_query._cast(filt->>'value', _coltyp);
$$;

-- jsonb implementation.
create function _pg_json_query._apply_pred__contained(col jsonb, filt jsonb,
                                                      _coltyp jsonb default null)
returns boolean language sql stable as $$ select col <@ (filt->'value') $$;

-- json implementation
create function _pg_json_query._apply_pred__contained(col json, filt jsonb,
                                                      _coltyp json default null)
returns boolean language sql stable as $$ select col::jsonb <@ (filt->'value') $$;


-- not contains
create function _pg_json_query._apply_pred__notcontains(col anyelement, filt jsonb,
                                                        _coltyp anyelement default null)
returns boolean language sql stable as $$
  select not _pg_json_query._contains(col, filt, _coltyp);
$$;


-- not contained
create function _pg_json_query._apply_pred__notcontained(col anyelement, filt jsonb,
                                                         _coltyp anyelement default null)
returns boolean language sql stable as $$
  select not _pg_json_query._contained(col, filt, _coltyp);
$$;




create function _pg_json_query._apply_op(op text, col anyelement, filt jsonb)
returns boolean language sql stable as $$
  select case op
    when 'eq' then _pg_json_query._apply_pred__eq(col, filt)
    when 'ne' then _pg_json_query._apply_pred__ne(col, filt)
    when 'gt' then _pg_json_query._apply_pred__gt(col, filt)
    when 'lt' then _pg_json_query._apply_pred__lt(col, filt)
    when 'ge' then _pg_json_query._apply_pred__ge(col, filt)
    when 'le' then _pg_json_query._apply_pred__le(col, filt)
    when 'in' then _pg_json_query._apply_pred__in(col, filt)
    when 'notin' then _pg_json_query._apply_pred__notin(col, filt)
    when 'like' then _pg_json_query._apply_pred__like(col, filt)
    when 'ilike' then _pg_json_query._apply_pred__ilike(col, filt)
    when 'startswith' then _pg_json_query._apply_pred__startswith(col, filt)
    when 'istartswith' then _pg_json_query._apply_pred__istartswith(col, filt)
    when 'endswith' then _pg_json_query._apply_pred__endswith(col, filt)
    when 'iendswith' then _pg_json_query._apply_pred__iendswith(col, filt)
    when 'exists' then _pg_json_query._apply_pred__exists(col, filt)
    when 'notexists' then _pg_json_query._apply_pred__notexists(col, filt)
    when 'contains' then _pg_json_query._apply_pred__contains(col, filt)
    when 'notcontains' then _pg_json_query._apply_pred__notcontains(col, filt)
    when 'contained' then _pg_json_query._apply_pred__contained(col, filt)
    when 'notcontained' then _pg_json_query._apply_pred__notcontained(col, filt)
    -- Aliases
    when 'gte' then _pg_json_query._apply_pred__ge(col, filt)
    when 'lte' then _pg_json_query._apply_pred__le(col, filt)
    else null
    end;
$$;

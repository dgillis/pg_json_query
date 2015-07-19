

drop schema if exists json_query cascade;
drop table if exists test_table;


\ir install.sql;


create table test_table(
  id serial primary key,
  obj jsonb,
  x int,
  y int
);

insert into test_table (obj, x, y)
  select
    json_build_object('idx', n, 'rnd', random(), 'is_even', n % 2 = 0)::jsonb,
    (10000 * random())::int,
    (10000 * random())::int
  from generate_series(0, 100000) n;

create index on test_table (x, (obj->'idx'));



create or replace function json_query._filter_row_column_router_impl(field text, tt test_table, filt anyelement)
returns boolean language sql immutable as $$
  select case field
    when 'id' then
      json_query._apply_filter(tt.id, filt)
    when 'x' then
      json_query._apply_filter(tt.x, filt)
    when 'y' then
      json_query._apply_filter(tt.y, filt)
    when 'obj' then
      json_query._apply_filter(tt.obj, filt)
    else
      false
    end;
$$;


create or replace function json_query._col_value_impl(valtype text, tt test_table, fld text)
returns text language sql immutable as $$
  select case fld
    when 'id' then tt.id::text
    when 'x' then tt.x::text
    when 'y' then tt.y::text
    when 'obj' then tt.obj::text
    else null
  end;
$$;


create or replace function json_query._col_value_impl(valtype jsonb, tt test_table, fld text)
returns jsonb language sql immutable as $$
  select case fld
    when 'id' then to_json(tt.id)::jsonb
    when 'x' then to_json(tt.x)::jsonb
    when 'y' then to_json(tt.y)::jsonb
    when 'obj' then tt.obj
    else null
  end;
$$;


-- Regular query:
--
-- explain
-- select *
-- from test_table
-- where x = 3
-- order by obj->>'idx'
-- limit 10;
-- #=>
--                                            QUERY PLAN                                            
-- -------------------------------------------------------------------------------------------------
--  Limit  (cost=42.29..42.32 rows=10 width=82)
--    ->  Sort  (cost=42.29..42.32 rows=10 width=82)
--          Sort Key: ((obj ->> 'idx'::text))
--          ->  Bitmap Heap Scan on test_table  (cost=4.50..42.13 rows=10 width=82)
--                Recheck Cond: (x = 3)
--                ->  Bitmap Index Scan on test_table_x_expr_idx  (cost=0.00..4.49 rows=10 width=0)
--                      Index Cond: (x = 3)
-- (7 rows)

-- Time: 0.562 ms



-- Using json_query.filter - same query plan.
--
-- explain
-- select *
-- from test_table
-- where json_query.filter(test_table, '{"x": 3}')
-- order by obj->>'idx'
-- limit 10;
-- #=> 
--                                            QUERY PLAN                                            
-- -------------------------------------------------------------------------------------------------
--  Limit  (cost=42.29..42.32 rows=10 width=82)
--    ->  Sort  (cost=42.29..42.32 rows=10 width=82)
--          Sort Key: ((obj ->> 'idx'::text))
--          ->  Bitmap Heap Scan on test_table  (cost=4.50..42.13 rows=10 width=82)
--                Recheck Cond: (x = 3)
--                ->  Bitmap Index Scan on test_table_x_expr_idx  (cost=0.00..4.49 rows=10 width=0)
--                      Index Cond: (x = 3)
-- (7 rows)

-- Time: 2.185 ms

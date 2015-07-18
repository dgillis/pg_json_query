

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




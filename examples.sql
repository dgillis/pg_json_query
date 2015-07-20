
BEGIN;

CREATE TABLE customer (
  id INT PRIMARY KEY,
  name TEXT,
  country TEXT DEFAULT 'US',
  info JSONB DEFAULT '{}'
);


CREATE TABLE purchase (
  id SERIAL PRIMARY KEY,
  customer_id INT,
  purchase_date DATE,
  last_payment_date DATE,
  amount NUMERIC,
  outstanding_amount NUMERIC,
  written_off BOOLEAN DEFAULT FALSE,
  
  FOREIGN KEY (customer_id) REFERENCES customer (id)
);


-- Returns the total of all outstanding customer balances for purchases
-- since the given date.
CREATE FUNCTION total_outstanding_since(since_date DATE)
RETURNS NUMERIC AS $$
  SELECT SUM(outstanding_amount)
  FROM purchase
  WHERE NOT written_off AND purchase_date >= since_date;
$$ LANGUAGE SQL STABLE;


CREATE FUNCTION total_outstanding(filters JSONB DEFAULT '{}')
RETURNS NUMERIC AS $$
  SELECT SUM(outstanding_amount)
  FROM purchase p
  WHERE NOT written_off AND json_query.FILTER(p, filters);
$$ LANGUAGE SQL STABLE;


CREATE OR REPLACE FUNCTION json_query._filter_row_column_impl(
  colname TEXT,
  row_ customer,
  _ ANYELEMENT
) RETURNS BOOLEAN LANGUAGE SQL IMMUTABLE AS $$
  SELECT CASE colname
    WHEN 'id' THEN
      json_query._apply_filter(row_.id, _)
    WHEN 'name' THEN
      json_query._apply_filter(row_.name, _)
    WHEN 'info' THEN
      json_query._apply_filter(row_.info, _)
    END;
$$;


CREATE OR REPLACE FUNCTION json_query._filter_row_column_impl(
  colname TEXT,
  row_ purchase,
  _ ANYELEMENT
) RETURNS BOOLEAN LANGUAGE SQL IMMUTABLE AS $$
  SELECT CASE colname
    WHEN 'customer_id' THEN
      json_query._apply_filter(row_.customer_id, _)
    WHEN 'purchase_date' THEN
      json_query._apply_filter(row_.purchase_date, _)
    WHEN 'last_payment_date' THEN
      json_query._apply_filter(row_.last_payment_date, _)
    WHEN 'amount' THEN
      json_query._apply_filter(row_.amount, _)
    WHEN 'outstanding' THEN
      json_query._apply_filter(row_.outstanding, _)
    WHEN 'written_off' THEN
      json_query._apply_filter(row_.written_off, _)
    END;
$$;



INSERT INTO customer (id, name)
VALUES (1, 'Amy'), (2, 'Bob'), (3, 'Charlie');


INSERT INTO purchase (customer_id,
                      purchase_date, last_payment_date,
                      amount, outstanding_amount)
VALUES
  (1, '2010-08-15', '2010-08-15', 120,   0),
  (1, '2011-09-30', '2011-09-30', 350,  10),
  (1, '2011-12-11',         NULL, 200, 200), 
  (1, '2012-04-11', '2015-07-20', 200,   5),
  (1, '2012-07-16', '2012-07-16',  75,  15),
  (1, '2013-01-01', '2013-01-01', 100,   0),
  (1, '2014-02-15', '2015-03-01', 500, 200),
  (1, '2015-04-15',         NULL, 150, 150),

  (2, '2000-05-02', '2000-06-15', 120,   0),
  (2, '2001-09-03', '2001-10-20', 350,  10),
  (2, '2005-01-04', '2005-02-12', 800,  40),
  (2, '2005-02-02', '2005-02-02', 700,   0),
  (2, '2005-03-18',         NULL, 500, 500),  
  (2, '2005-06-20', '2005-07-05', 250, 200),
  (2, '2005-06-15', '2005-10-01', 850,  50),
  (2, '2010-12-05', '2010-12-23', 100,  20),
  (2, '2012-04-11', '2015-07-20', 200,   5),
  (2, '2012-07-16', '2012-07-16',  75,  15),
  (2, '2013-01-01', '2013-01-01', 100,   0),
  (2, '2014-02-15', '2015-03-28', 150,  50),
  (2, '2015-04-15',         NULL, 150, 150),
  
  (3, '2015-01-01', '2015-01-15',  10,   0),
  (3, '2015-01-01', '2015-01-15', 100,  40),
  (3, '2015-02-02', '2015-02-02', 700,   0),
  (3, '2015-06-20', '2015-07-05', 250,   0),
  (3, '2015-02-15', '2015-03-01', 500, 400);


-- Bulk customer data
WITH max_id AS (SELECT MAX(id) AS max_id FROM customer)
  INSERT INTO customer (id, name, country, info)
    SELECT
      -- id
      max_id + idx,
      
      -- name
      'cust-' || idx as name,
      
      -- region
      CASE idx % 5
        WHEN 0 THEN 'US'
        WHEN 1 THEN 'UK'
        WHEN 2 THEN 'DE'
        WHEN 3 THEN 'CA'
        WHEN 4 THEN 'FR'
      END,

      -- info
      JSON_BUILD_OBJECT(
        'idx', idx,
        'score', (RANDOM() * 1000)::int
      )::JSONB
    FROM
      GENERATE_SERIES(1, 100000) idx,
      max_id;


CREATE INDEX ON customer (country, (info->'score'), id) WHERE info ? 'score';





/*
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



create or replace function json_query._filter_row_column_impl(field text, tt test_table, filt anyelement)
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
*/

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

--                                    QUERY PLAN                                    
-- ---------------------------------------------------------------------------------
--  Index Scan using customer_pkey on customer c  (cost=0.29..8.31 rows=1 width=68)
--    Index Cond: (id = 1000)
-- (2 rows)

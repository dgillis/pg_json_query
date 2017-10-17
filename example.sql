
CREATE TABLE json_query_example (
  id SERIAL PRIMARY KEY,
  text_col TEXT,
  tstamp_col TIMESTAMP,
  jsonb_col JSONB,
  int_col INT,
  float_col FLOAT
);

INSERT INTO json_query_example (text_col, tstamp_col, jsonb_col, int_col, float_col)
  VALUES
  ('ab', '2017-01-01', '{"x": 1, "y": {"z": "yz"}}', -1, 3.14),
  ('xxx', '1900-05-01', '[1, [2, 2], 3]',  0, NULL),
  (NULL, '2000-05-01', NULL,  3, 0.01),
  ('aa', '1990-03-30', '100',  2, -20421.43243);

-- Register the table's row type so it can be used with jq_filter().
SELECT jq_register_row_type('json_query_example');

----------------------------------------------------------------------------
-- Django-style filters                                                   --
----------------------------------------------------------------------------
SELECT *
FROM json_query_example t
WHERE
  -- text_col = 'aa'
  jq_filter(t, '{"text_col": "aa"}')

  -- text_col IN ('aa', 'xxx')
  -- jq_filter(t, '{"text_col__in": ["aa", "xxx"]}')

  -- text_col IS NOT NULL AND int_col > 0
  -- jq_filter(t, '{"text_col__ne": null, "int_col__ge": 0}')

  -- jsonb_col->'x' = '1'
  -- jq_filter(t, '{"jsonb_col->>x": 1}')
ORDER BY id;

----------------------------------------------------------------------------
-- Array "raw" filters                                                    --
----------------------------------------------------------------------------
SELECT *
FROM json_query_example t
WHERE
  -- text_col = 'aa'
  jq_filter_raw(t, '[{"field": "text_col", "op": "eq", "value": "aa"}]')

  -- text_col IN ('aa', 'xxx')
  -- jq_filter_raw(t, '[{"field": "text_col", "op": "in", "value": ["aa", "xxx"]}]')

  -- text_col IS NOT NULL AND int_col > 0
  -- jq_filter_raw(t, '[{"field": "text_col", "op": "ne", "value": null}, {"field": "int_col", "op": "ge", "value": 0}]')

  -- jsonb_col->'x' = '1'
  -- jq_filter_raw(t, '[{"field": "jsonb_col", "path": "x", "op": "eq", "value": 1}]')
ORDER BY id;

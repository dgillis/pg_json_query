# json_query


Generate a boolean expression based on a JSONB object using Django-style filter syntax so that
```SQL
SELECT *
FROM tbl t
WHERE json_query.filter(t, '{"<column>__<op>": <value>}')
```
selects the same rows as
```SQL
SELECT *
FROM tbl t
WHERE t.<column> <op> <value>
```

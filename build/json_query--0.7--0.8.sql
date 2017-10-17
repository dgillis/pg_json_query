-- Change from table to immutable.
alter function _pg_json_query._parse_filter_obj_to_json(obj jsonb) immutable;

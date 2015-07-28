
do $$
declare
  type_name text;
  type_oid oid;
begin
  for type_name, type_oid in (select *
                              from _pg_json_query._default_row_types) loop
    perform _pg_json_query._register_col_type(type_oid);
  end loop;
end;
$$;

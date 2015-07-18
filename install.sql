\set ON_ERROR_STOP on;

set check_function_bodies = true;

begin;

create schema json_query;


\ir utils.sql;
\ir types.sql;
\ir columns.sql;
\ir filter.sql;
\ir filter2.sql;








commit;

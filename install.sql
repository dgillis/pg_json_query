/*
Public interface:

* json_query.filter(rowtype, filter_obj) -> boolean

  where filter object is of one of the following forms:
    (1) {"field__op": "value"}
    (2) {"$field": F, "$op": O, "$value": V, "$path": P}
    (3) {"$and": [f1, f2, ..., fN]} where fi are also filter objects.
  In the (1) syntax, a path for nested JSON value can be specified '->'
  within the field portion of the string. Thus {"column->x->y__gt": 1}
  is equivalent to "column->'x'->'y' > 1". The (optional) "$path" parameter
  in (2) can also be used to specify the path to a nested JSON object. The
  "$path" value can either be a single string (depth of 1) or an array of
  strings to specify more deeply nested paths.


* json_query.column_value(row rowtype, field text) -> jsonb
    
  Returns a JSONB representation of the specfied field. The field may either
  be a column name or a column name followed by a "path suffix". The path
  suffix should be one or more strings prefixed with "->" immediately after
  the column name (e.g., "column_x->path->to->value"). The path suffix
  describes a JSON lookup to perform on the column's value (and so this
  should only be used on JSON columns). No way of escaping "->" substrings
  is provided so this can only be used on column-names/JSON keys which do
  not contain "->" substrings.


* json_query.column_value_text(row rowtype, field text) -> text

  Returns a textual representation of the specified field. When paths ("->")
  are specified in the field expression, the JSONB representation of the base
  value will be used and then "->>" on the terminal path element so that the
  result is text.


* json_query.column_expr(row rowtype, expr text|text[]|jsonb) -> jsonb

  Returns a JSONB string or array corresponding to the given "field
  expression". When field expression is text, this is the same as
  json_query.column_value(). When field expression is a text array, this
  returns a JSONB array with each entry corresponding to the column value
  for the respective array members. If expr is JSONB, then either the text
  or text[] variety is used depending on whether expr is either a JSONB
  string or a JSONB array.


* json_query.column_expr_text(row rowtype, expr text|text[]|jsonb) -> text
 
  Similar to json_query.column_expr() but the result will always be text.
  In cases where column_expr() would have returned as JSONB array,
  column_expr_text() will return the textual representation of a text[]
  array (e.g., array['a', 'b', 'c']::text #=> '{a, b, c}').


* json_query.column_expr_text_array(row rowtype, expr text[]) -> text[]
  
  Similar to json_query.column_expr_text() when invoked with a text[] array 
  except in this case an actual text[] array rather than text will be returned.
*/



\set ON_ERROR_STOP on;

set check_function_bodies = true;

begin;

create schema json_query;


\ir utils.sql;
\ir types.sql;
\ir columns.sql;
\ir ops.sql;
\ir casts.sql;
\ir filter.sql;
\ir column_expr.sql;
\ir creator_funcs.sql;


commit;
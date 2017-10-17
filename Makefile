EXTENSION = json_query

DATA = build/json_query--*.sql
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)

CURR_VERSION = 0.9
CURR_BUNDLE = build/json_query--${CURR_VERSION}.sql

libsqldir = libsql

data_includes =	${libsqldir}/init_schema.sql \
		${libsqldir}/utils.sql \
		${libsqldir}/types.sql \
		${libsqldir}/columns.sql \
		${libsqldir}/casts.sql \
		${libsqldir}/db_info.sql \
		${libsqldir}/like_predicate_helpers.sql \
		${libsqldir}/in_predicate_helpers.sql \
		${libsqldir}/predicates.sql \
		${libsqldir}/filter.sql \
		${libsqldir}/column_expr.sql \
		${libsqldir}/register_row_type.sql \
		${libsqldir}/register_column_type.sql \
		${libsqldir}/register_defaults.sql

bundle: ${data_includes}
	cat $^ > ${CURR_BUNDLE}

include $(PGXS)

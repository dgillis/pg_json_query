EXTENSION = json_query

DATA = build/json_query--*.sql
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)

CURR_VERSION = 0.3
CURR_BUNDLE = build/json_query--${CURR_VERSION}.sql

libsqldir = libsql

data_includes =	${libsqldir}/init_schema.sql \
		${libsqldir}/utils.sql \
		${libsqldir}/types.sql \
		${libsqldir}/columns.sql \
		${libsqldir}/casts.sql \
		${libsqldir}/core_ops.sql \
		${libsqldir}/predicates.sql \
		${libsqldir}/filter.sql \
		${libsqldir}/column_expr.sql \
		${libsqldir}/creator_funcs.sql

bundle: ${data_includes}
	cat $^ > ${CURR_BUNDLE}

include $(PGXS)

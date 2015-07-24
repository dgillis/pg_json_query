EXTENSION = json_query

DATA = build/json_query--0.1.sql
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)

libsqldir = libsql

data_includes =	${libsqldir}/init_schema.sql \
		${libsqldir}/utils.sql \
		${libsqldir}/types.sql \
		${libsqldir}/columns.sql \
		${libsqldir}/ops.sql \
		${libsqldir}/casts.sql \
		${libsqldir}/filter.sql \
		${libsqldir}/column_expr.sql \
		${libsqldir}/creator_funcs.sql

$(DATA): ${data_includes}
	cat $^ > ${DATA}

clean:
	rm -f ${DATA}

include $(PGXS)

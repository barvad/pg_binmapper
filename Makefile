MODULES = pg_binmapper
EXTENSION = pg_binmapper
DATA = pg_binmapper--1.0.sql
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

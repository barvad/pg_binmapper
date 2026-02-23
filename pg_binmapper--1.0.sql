CREATE OR REPLACE FUNCTION bin_parse(target_table regclass, payload bytea)
RETURNS record
AS 'MODULE_PATHNAME', 'parse_binary_payload'
LANGUAGE C STRICT;

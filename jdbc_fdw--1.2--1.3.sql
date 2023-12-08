/* contrib/jdbc_fdw/jdbc_fdw--1.2--1.3.sql */

CREATE FUNCTION jdbc_exec (text, text, VARIADIC "any")
RETURNS setof record
AS 'MODULE_PATHNAME','jdbc_exec'
LANGUAGE C STRICT PARALLEL RESTRICTED;
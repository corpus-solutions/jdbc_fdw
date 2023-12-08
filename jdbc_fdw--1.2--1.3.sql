/* contrib/jdbc_fdw/jdbc_fdw--1.2--1.3.sql */

DROP FUNCTION  jdbc_exec (text, text);

CREATE FUNCTION jdbc_exec (text, text, VARIADIC "any")
RETURNS setof record
AS 'MODULE_PATHNAME','jdbc_exec'
LANGUAGE C STRICT PARALLEL RESTRICTED;
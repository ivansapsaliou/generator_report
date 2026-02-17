CREATE OR REPLACE FUNCTION public.report_validate_identifier(
	p_identifier text)
    RETURNS boolean
    LANGUAGE plpgsql
    COST 100
    IMMUTABLE PARALLEL UNSAFE
AS $BODY$
BEGIN
    RETURN p_identifier ~ '^[a-zA-Z_][a-zA-Z0-9_]{0,63}$';
END;
$BODY$;

ALTER FUNCTION public.report_validate_identifier(p_identifier text)
    OWNER TO rul_developer;

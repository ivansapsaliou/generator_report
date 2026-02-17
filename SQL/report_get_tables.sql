CREATE OR REPLACE FUNCTION public.report_get_tables(
	p_schema text DEFAULT 'public'::text)
    RETURNS TABLE(table_name text, table_comment text) 
    LANGUAGE plpgsql
    COST 100
    VOLATILE SECURITY DEFINER PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
BEGIN
    RETURN QUERY
    SELECT 
        t.table_name::TEXT,
        obj_description(format('%I.%I', t.table_schema, t.table_name)::regclass)::TEXT
    FROM information_schema.tables t
    WHERE t.table_schema = p_schema 
      AND t.table_type = 'BASE TABLE'
    ORDER BY t.table_name;
END;
$BODY$;

ALTER FUNCTION public.report_get_tables(p_schema text DEFAULT 'public'::text)
    OWNER TO rul_developer;

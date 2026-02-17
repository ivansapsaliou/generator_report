CREATE OR REPLACE FUNCTION public.report_get_columns(
	p_table text,
	p_schema text DEFAULT 'public'::text)
    RETURNS TABLE(column_name text, data_type text, is_nullable boolean, column_default text, column_comment text) 
    LANGUAGE plpgsql
    COST 100
    VOLATILE SECURITY DEFINER PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
BEGIN
    RETURN QUERY
    SELECT 
        c.column_name::TEXT,
        c.data_type::TEXT,
        (c.is_nullable = 'YES') AS is_nullable,
        c.column_default::TEXT,
        col_description(format('%I.%I', p_schema, p_table)::regclass::oid, c.ordinal_position)::TEXT
    FROM information_schema.columns c
    WHERE c.table_schema = p_schema 
      AND c.table_name = p_table
    ORDER BY c.ordinal_position;
END;
$BODY$;

ALTER FUNCTION public.report_get_columns(p_table text, p_schema text DEFAULT 'public'::text)
    OWNER TO rul_developer;

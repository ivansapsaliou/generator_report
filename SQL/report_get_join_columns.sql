CREATE OR REPLACE FUNCTION public.report_get_join_columns(
	p_table1 text,
	p_table2 text,
	p_schema text DEFAULT 'public'::text)
    RETURNS TABLE(column_name1 text, column_name2 text, data_type text, is_fk boolean, fk_direction text, confidence numeric) 
    LANGUAGE plpgsql
    COST 100
    VOLATILE SECURITY DEFINER PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
BEGIN
    RETURN QUERY
    WITH fk_relations AS (
        -- Прямые FK из таблицы 1 в таблицу 2
        SELECT DISTINCT
            kcu.column_name AS col1,
            ccu.column_name AS col2,
            'table1->table2' AS direction
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu 
            ON tc.constraint_name = kcu.constraint_name
        JOIN information_schema.constraint_column_usage ccu 
            ON tc.constraint_name = ccu.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_schema = p_schema
          AND tc.table_name = p_table1
          AND ccu.table_schema = p_schema
          AND ccu.table_name = p_table2
        
        UNION ALL
        
        -- Обратные FK из таблицы 2 в таблицу 1
        SELECT DISTINCT
            ccu.column_name AS col1,
            kcu.column_name AS col2,
            'table2->table1' AS direction
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu 
            ON tc.constraint_name = kcu.constraint_name
        JOIN information_schema.constraint_column_usage ccu 
            ON tc.constraint_name = ccu.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_schema = p_schema
          AND tc.table_name = p_table2
          AND ccu.table_schema = p_schema
          AND ccu.table_name = p_table1
    )
    SELECT DISTINCT
        c1.column_name::TEXT,
        c2.column_name::TEXT,
        c1.data_type::TEXT,
        (fk.col1 IS NOT NULL) AS is_fk,
        fk.direction::TEXT AS fk_direction,
        CASE 
            WHEN fk.col1 IS NOT NULL THEN 1.0
            WHEN c1.column_name = c2.column_name THEN 0.8
            WHEN c1.column_name ILIKE '%' || c2.column_name || '%' 
                 OR c2.column_name ILIKE '%' || c1.column_name || '%' 
            THEN 0.6
            ELSE 0.3
        END AS confidence
    FROM information_schema.columns c1
    JOIN information_schema.columns c2 
        ON c1.data_type = c2.data_type
        AND (
            c1.column_name = c2.column_name
            OR c1.column_name ILIKE '%id' AND c2.column_name ILIKE '%id'
            OR c1.column_name ILIKE '%_id' AND c2.column_name ILIKE '%_id'
            OR c1.column_name ILIKE '%uuid' AND c2.column_name ILIKE '%uuid'
        )
    LEFT JOIN fk_relations fk 
        ON fk.col1 = c1.column_name AND fk.col2 = c2.column_name
    WHERE c1.table_schema = p_schema
      AND c1.table_name = p_table1
      AND c2.table_schema = p_schema
      AND c2.table_name = p_table2
    ORDER BY confidence DESC;
END;
$BODY$;

ALTER FUNCTION public.report_get_join_columns(p_table1 text, p_table2 text, p_schema text DEFAULT 'public'::text)
    OWNER TO rul_developer;

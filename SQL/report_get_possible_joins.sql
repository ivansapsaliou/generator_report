CREATE OR REPLACE FUNCTION public.report_get_possible_joins(
    p_table_name text,
    p_schema_name text DEFAULT 'public',
    p_current_tables text[] DEFAULT NULL,
  	p_include_semantic_matches boolean = true
)
RETURNS TABLE (
    target_table text,
    target_schema text,
    join_type text,
    source_column text,
    target_column text,
    constraint_name text,
    match_confidence numeric,
    join_suggestion text
)
LANGUAGE sql
STABLE
AS
$$
WITH current_tables AS (
    SELECT c.oid, c.relname, n.nspname
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = ANY(COALESCE(p_current_tables, ARRAY[p_table_name]))
      AND n.nspname = p_schema_name
)
SELECT DISTINCT
    CASE
        WHEN src.relname = ANY(COALESCE(p_current_tables, ARRAY[p_table_name]))
            THEN tgt.relname
        ELSE src.relname
    END AS target_table,

    CASE
        WHEN src.relname = ANY(COALESCE(p_current_tables, ARRAY[p_table_name]))
            THEN tgt_nsp.nspname
        ELSE src_nsp.nspname
    END AS target_schema,

    CASE
        WHEN src.relname = ANY(COALESCE(p_current_tables, ARRAY[p_table_name]))
            THEN 'FOREIGN_KEY'
        ELSE 'REVERSE_FK'
    END AS join_type,

    CASE
        WHEN src.relname = ANY(COALESCE(p_current_tables, ARRAY[p_table_name]))
            THEN src_col.attname
        ELSE tgt_col.attname
    END AS source_column,

    CASE
        WHEN src.relname = ANY(COALESCE(p_current_tables, ARRAY[p_table_name]))
            THEN tgt_col.attname
        ELSE src_col.attname
    END AS target_column,

    con.conname,
    1.0,

    CASE
        WHEN src.relname = ANY(COALESCE(p_current_tables, ARRAY[p_table_name]))
        THEN format('%I.%I = %I.%I',
            src.relname, src_col.attname,
            tgt.relname, tgt_col.attname
        )
        ELSE format('%I.%I = %I.%I',
            tgt.relname, tgt_col.attname,
            src.relname, src_col.attname
        )
    END AS join_suggestion
FROM pg_constraint con
JOIN pg_class src ON src.oid = con.conrelid
JOIN pg_namespace src_nsp ON src_nsp.oid = src.relnamespace
JOIN pg_class tgt ON tgt.oid = con.confrelid
JOIN pg_namespace tgt_nsp ON tgt_nsp.oid = tgt.relnamespace
JOIN pg_attribute src_col ON src_col.attrelid = src.oid AND src_col.attnum = ANY(con.conkey)
JOIN pg_attribute tgt_col ON tgt_col.attrelid = tgt.oid AND tgt_col.attnum = ANY(con.confkey)
WHERE con.contype = 'f'
  AND (
        (
            src.relname = ANY(COALESCE(p_current_tables, ARRAY[p_table_name]))
            AND tgt.relname <> ALL(COALESCE(p_current_tables, ARRAY[p_table_name]))
        )
        OR
        (
            tgt.relname = ANY(COALESCE(p_current_tables, ARRAY[p_table_name]))
            AND src.relname <> ALL(COALESCE(p_current_tables, ARRAY[p_table_name]))
        )
      )
  AND src_nsp.nspname = p_schema_name
  AND tgt_nsp.nspname = p_schema_name;

$$;
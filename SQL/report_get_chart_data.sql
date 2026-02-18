-- Функция для получения данных графиков
-- Поддерживает: bar, line, pie, doughnut, area
CREATE OR REPLACE FUNCTION public.report_get_chart_data(
    p_main_table text,
    p_schema text DEFAULT 'public'::text,
    p_joins report_join[] DEFAULT NULL::report_join[],
    p_x_axis text,                    -- Колонка для оси X (категории)
    p_y_axis text DEFAULT NULL::text, -- Колонка для оси Y (значения)
    p_aggregate_function text DEFAULT 'COUNT'::text,  -- SUM, COUNT, AVG, MIN, MAX
    p_conditions report_condition[] DEFAULT NULL::report_condition[],
    p_limit integer DEFAULT 50
)
RETURNS TABLE(label text, value numeric, tooltip text)
LANGUAGE plpgsql
COST 100
VOLATILE SECURITY DEFINER PARALLEL UNSAFE
ROWS 1000

AS $BODY$
DECLARE
    v_sql TEXT := '';
    v_select TEXT := '';
    v_from TEXT := '';
    v_join TEXT := '';
    v_where TEXT := '';
    v_group_by TEXT := '';
    v_join_item report_join;
    v_cond report_condition;
    v_main_alias TEXT;
    v_source_alias TEXT;
    v_target_alias TEXT;
    v_first BOOLEAN := TRUE;
    v_agg_expr TEXT;
BEGIN
    -- Валидация таблицы
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = p_schema AND table_name = p_main_table
    ) THEN
        RAISE EXCEPTION 'Таблица %.% не существует', p_schema, p_main_table;
    END IF;

    v_main_alias := p_main_table;

    -- Определяем выражение агрегации
    IF upper(p_aggregate_function) = 'COUNT' AND (p_y_axis IS NULL OR p_y_axis = '' OR p_y_axis = '1') THEN
        v_agg_expr := 'COUNT(*)::NUMERIC';
    ELSIF p_y_axis IS NULL OR p_y_axis = '' OR p_y_axis = '1' THEN
        v_agg_expr := 'COUNT(*)::NUMERIC';
    ELSIF p_y_axis ~ '\.' THEN
        v_agg_expr := format('%s(%I.%I)::NUMERIC', upper(p_aggregate_function), 
            split_part(p_y_axis, '.', 1), split_part(p_y_axis, '.', 2));
    ELSE
        v_agg_expr := format('%s(%I.%I)::NUMERIC', upper(p_aggregate_function), v_main_alias, p_y_axis);
    END IF;

    -- SELECT - агрегатная функция
    v_select := v_agg_expr || ' AS value';

    -- Добавляем категорию (ось X)
    IF p_x_axis ~ '\.' THEN
        v_select := v_select || ', ' || format('%I.%I::TEXT AS label', 
            split_part(p_x_axis, '.', 1), split_part(p_x_axis, '.', 2));
    ELSE
        v_select := v_select || ', ' || format('%I.%I::TEXT AS label', 
            v_main_alias, p_x_axis);
    END IF;

    -- FROM
    v_from := format('%I.%I AS %I', p_schema, p_main_table, v_main_alias);

    -- JOIN
    IF p_joins IS NOT NULL AND array_length(COALESCE(p_joins, ARRAY[]::report_join[]), 1) > 0 THEN
        FOREACH v_join_item IN ARRAY p_joins LOOP
            IF v_join_item.source_table = p_main_table THEN
                v_source_alias := v_main_alias;
            ELSE
                SELECT alias INTO v_source_alias
                FROM unnest(p_joins) AS j
                WHERE j.table_name = v_join_item.source_table
                LIMIT 1;
                IF v_source_alias IS NULL THEN
                    v_source_alias := v_join_item.source_table;
                END IF;
            END IF;
            
            v_target_alias := COALESCE(NULLIF(v_join_item.alias, ''), v_join_item.table_name);
            
            v_join := v_join || format(' %s JOIN %I.%I AS %I ON %I.%I = %I.%I',
                upper(v_join_item.join_type),
                p_schema, v_join_item.table_name,
                v_target_alias,
                v_source_alias,
                v_join_item.left_column,
                v_target_alias,
                v_join_item.right_column
            );
        END LOOP;
    END IF;

    -- WHERE
    IF p_conditions IS NOT NULL AND array_length(COALESCE(p_conditions, ARRAY[]::report_condition[]), 1) > 0 THEN
        v_first := TRUE;
        v_where := 'WHERE ';
        FOREACH v_cond IN ARRAY p_conditions LOOP
            IF NOT v_first AND v_cond.logic_operator IS NOT NULL THEN
                v_where := v_where || ' ' || upper(v_cond.logic_operator) || ' ';
            END IF;
            
            IF v_cond.column_name ~ '\.' THEN
                IF upper(v_cond.operator) = 'IN' THEN
                    v_where := v_where || format('%I.%I IN (%s)', 
                        split_part(v_cond.column_name, '.', 1), 
                        split_part(v_cond.column_name, '.', 2), 
                        v_cond.value);
                ELSIF upper(v_cond.operator) = 'LIKE' THEN
                    v_where := v_where || format('%I.%I LIKE %L', 
                        split_part(v_cond.column_name, '.', 1), 
                        split_part(v_cond.column_name, '.', 2), 
                        v_cond.value);
                ELSE
                    v_where := v_where || format('%I.%I %s %L', 
                        split_part(v_cond.column_name, '.', 1), 
                        split_part(v_cond.column_name, '.', 2), 
                        upper(v_cond.operator), 
                        v_cond.value);
                END IF;
            ELSE
                IF upper(v_cond.operator) = 'IN' THEN
                    v_where := v_where || format('%I IN (%s)', v_cond.column_name, v_cond.value);
                ELSIF upper(v_cond.operator) = 'LIKE' THEN
                    v_where := v_where || format('%I LIKE %L', v_cond.column_name, v_cond.value);
                ELSE
                    v_where := v_where || format('%I %s %L', v_cond.column_name, upper(v_cond.operator), v_cond.value);
                END IF;
            END IF;
            
            v_first := FALSE;
        END LOOP;
    END IF;

    -- GROUP BY
    IF p_x_axis ~ '\.' THEN
        v_group_by := 'GROUP BY ' || format('%I.%I', split_part(p_x_axis, '.', 1), split_part(p_x_axis, '.', 2));
    ELSE
        v_group_by := 'GROUP BY ' || format('%I.%I', v_main_alias, p_x_axis);
    END IF;

    -- ORDER BY value DESC
    v_group_by := v_group_by || ' ORDER BY value DESC';
    
    -- LIMIT
    IF p_limit IS NOT NULL AND p_limit > 0 THEN
        v_group_by := v_group_by || ' LIMIT ' || p_limit;
    END IF;

    -- Сборка SQL
    v_sql := format('SELECT label::TEXT AS label, value::NUMERIC AS value, label::TEXT AS tooltip FROM (SELECT %s FROM %s %s %s %s) AS chart_data',
        v_select, v_from, v_join, v_where, v_group_by);

    RAISE NOTICE 'Chart SQL: %', v_sql;

    RETURN QUERY EXECUTE v_sql;
END;
$BODY$;

ALTER FUNCTION public.report_get_chart_data(p_main_table text, p_schema text, p_joins report_join[], p_x_axis text, p_y_axis text, p_aggregate_function text, p_conditions report_condition[], p_limit integer)
    OWNER TO rul_developer;

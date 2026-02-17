CREATE OR REPLACE FUNCTION public.report_generate(
	p_main_table text,
	p_schema text DEFAULT 'public'::text,
	p_joins report_join[] DEFAULT NULL::report_join[],
	p_columns text[] DEFAULT NULL::text[],
	p_conditions report_condition[] DEFAULT NULL::report_condition[],
	p_aggregates report_aggregate[] DEFAULT NULL::report_aggregate[],
	p_group_by text[] DEFAULT NULL::text[],
	p_sort report_sort[] DEFAULT NULL::report_sort[],
	p_limit integer DEFAULT NULL::integer,
	p_offset integer DEFAULT NULL::integer)
    RETURNS SETOF record 
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
    v_order_by TEXT := '';
    v_limit_offset TEXT := '';
    v_col TEXT;
    v_join_item report_join;
    v_cond report_condition;
    v_sort_item report_sort;
    v_agg report_aggregate;
    v_first BOOLEAN := TRUE;
    v_main_alias TEXT;
    v_source_alias TEXT;
    v_target_alias TEXT;
BEGIN
    -- Защита от инъекций: валидация имен
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = p_schema AND table_name = p_main_table
    ) THEN
        RAISE EXCEPTION 'Таблица %.% не существует', p_schema, p_main_table;
    END IF;

    v_main_alias := p_main_table;

    -- SELECT часть
    IF p_aggregates IS NOT NULL AND array_length(p_aggregates, 1) > 0 THEN
        v_first := TRUE;
        v_select := '';

        -- 1. Сначала добавляем обычные колонки (Dimensions)
        IF p_columns IS NOT NULL THEN
            FOREACH v_col IN ARRAY p_columns LOOP
                IF NOT v_first THEN v_select := v_select || ', '; END IF;
                
                IF v_col ~ '\.' THEN
                    v_select := v_select || format('%I.%I::TEXT', split_part(v_col, '.', 1), split_part(v_col, '.', 2));
                ELSE
                    v_select := v_select || format('%I.%I::TEXT', v_main_alias, v_col);
                END IF;
                
                v_first := FALSE;
            END LOOP;
        END IF;
        
        -- 2. Затем добавляем агрегаты (Measures)
        FOR v_agg IN SELECT * FROM unnest(p_aggregates) LOOP
            IF NOT v_first THEN v_select := v_select || ', '; END IF;
            
            -- Проверяем, есть ли точка в имени колонки (формат table.column)
            IF v_agg.column_name ~ '\.' THEN
                v_select := v_select || format('%s(%I.%I)::TEXT AS %s', 
                    upper(v_agg.function_name), 
                    split_part(v_agg.column_name, '.', 1),
                    split_part(v_agg.column_name, '.', 2),
                    quote_ident(coalesce(v_agg.alias, v_agg.function_name || '_' || replace(v_agg.column_name, '.', '_')))
                );
            ELSE
                v_select := v_select || format('%s(%I.%I)::TEXT AS %s', 
                    upper(v_agg.function_name), 
                    v_main_alias, 
                    v_agg.column_name,
                    quote_ident(coalesce(v_agg.alias, v_agg.function_name || '_' || v_agg.column_name))
                );
            END IF;
            v_first := FALSE;
        END LOOP;
    ELSIF p_columns IS NOT NULL AND array_length(p_columns, 1) > 0 THEN
 -- Исправление для обычного SELECT: разбиваем table.column
        SELECT array_to_string(array_agg(
            CASE 
                WHEN col ~ '\.' THEN format('%I.%I::TEXT', split_part(col, '.', 1), split_part(col, '.', 2))
                ELSE format('%I.%I::TEXT', v_main_alias, col) -- ИСПРАВЛЕНО
            END
        ), ', ') INTO v_select
        FROM unnest(p_columns) AS col;
    ELSE
        v_select := '*';
    END IF;

    -- FROM часть
    v_from := format('%I.%I AS %I', p_schema, p_main_table, v_main_alias);

    -- JOIN часть
    IF p_joins IS NOT NULL THEN
        FOREACH v_join_item IN ARRAY p_joins LOOP
            -- Валидация таблицы для джоина
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = p_schema AND table_name = v_join_item.table_name
            ) THEN
                RAISE EXCEPTION 'Таблица для JOIN %.% не существует', p_schema, v_join_item.table_name;
            END IF;
            
            -- Определяем алиас таблицы-источника
            -- Если source_table - это основная таблица, используем её алиас
            IF v_join_item.source_table = p_main_table THEN
                v_source_alias := v_main_alias;
            ELSE
                -- Иначе ищем алиас таблицы-источника среди уже добавленных джойнов
                SELECT alias INTO v_source_alias
                FROM unnest(p_joins) AS j
                WHERE j.table_name = v_join_item.source_table
                LIMIT 1;

                -- Если почему-то не нашли (защита), используем имя таблицы как есть
                IF v_source_alias IS NULL OR v_source_alias = '' THEN
                    v_source_alias := v_join_item.source_table;
                END IF;
            END IF;

            -- Определяем алиас для целевой таблицы (справа от знака =)
            v_target_alias := COALESCE(NULLIF(v_join_item.alias, ''), v_join_item.table_name);

            -- Формируем джоин, используя найденный алиас источника
            v_join := v_join || format(' %s JOIN %I.%I AS %I ON %I.%I = %I.%I',
                upper(v_join_item.join_type),
                p_schema, v_join_item.table_name,
                v_target_alias,                         -- Алиас целевой таблицы
                v_source_alias,                         -- Алиас источника (ИСПРАВЛЕНО)
                v_join_item.left_column,                -- Колонка источника
                v_target_alias,                         -- Алиас целевой таблицы
                v_join_item.right_column                -- Колонка цели
            );
        END LOOP;
    END IF;

    -- WHERE часть
    IF p_conditions IS NOT NULL THEN
        v_first := TRUE;
        v_where := 'WHERE ';
        FOREACH v_cond IN ARRAY p_conditions LOOP
            IF NOT v_first AND v_cond.logic_operator IS NOT NULL THEN
                v_where := v_where || ' ' || upper(v_cond.logic_operator) || ' ';
            END IF;
            
            -- Разбиваем table.column, если точка есть
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
                -- Обработка без точки (обычная колонка)
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

    -- GROUP BY часть
    IF p_group_by IS NOT NULL AND array_length(p_group_by, 1) > 0 THEN
        SELECT array_to_string(array_agg(
            CASE 
                WHEN col ~ '\.' THEN format('%I.%I', split_part(col, '.', 1), split_part(col, '.', 2))
                ELSE p_main_table || '.' || quote_ident(col)
            END
        ), ', ') INTO v_group_by 
        FROM unnest(p_group_by) AS col;
        v_group_by := 'GROUP BY ' || v_group_by;
    END IF;

    -- ORDER BY часть
    IF p_sort IS NOT NULL AND array_length(p_sort, 1) > 0 THEN
        v_first := TRUE;
        v_order_by := 'ORDER BY ';
        FOREACH v_sort_item IN ARRAY p_sort LOOP
            -- Разбиваем table.column
            IF v_sort_item.column_name ~ '\.' THEN
                IF NOT v_first THEN v_order_by := v_order_by || ', '; END IF;
                v_order_by := v_order_by || format('%I.%I %s', 
                    split_part(v_sort_item.column_name, '.', 1), 
                    split_part(v_sort_item.column_name, '.', 2), 
                    upper(v_sort_item.direction));
                v_first := FALSE;
            ELSE
                IF NOT v_first THEN v_order_by := v_order_by || ', '; END IF;
                v_order_by := v_order_by || format('%I %s', v_sort_item.column_name, upper(v_sort_item.direction));
                v_first := FALSE;
            END IF;
        END LOOP;
    END IF;

    -- LIMIT/OFFSET
    IF p_limit IS NOT NULL THEN
        v_limit_offset := format('LIMIT %s', p_limit);
        IF p_offset IS NOT NULL THEN
            v_limit_offset := v_limit_offset || format(' OFFSET %s', p_offset);
        END IF;
    END IF;

    -- Сборка финального запроса
    v_sql := format('SELECT %s FROM %s %s %s %s %s %s',
        v_select,
        v_from,
        v_join,
        v_where,
        v_group_by,
        v_order_by,
        v_limit_offset
    );

    RAISE NOTICE 'Сгенерированный SQL: %', v_sql;
    
    -- Выполнение динамического SQL
    RETURN QUERY EXECUTE v_sql;
END;
$BODY$;

ALTER FUNCTION public.report_generate(p_main_table text, p_schema text DEFAULT 'public'::text, p_joins report_join[] DEFAULT NULL::report_join[], p_columns text[] DEFAULT NULL::text[], p_conditions report_condition[] DEFAULT NULL::report_condition[], p_aggregates report_aggregate[] DEFAULT NULL::report_aggregate[], p_group_by text[] DEFAULT NULL::text[], p_sort report_sort[] DEFAULT NULL::report_sort[], p_limit integer DEFAULT NULL::integer, p_offset integer DEFAULT NULL::integer)
    OWNER TO rul_developer;

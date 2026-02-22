"""
Модуль для получения структуры базы данных (дамп без данных).
Используется для экспорта схемы БД в SQL или JSON формате.
"""

from db_adapter import DatabaseAdapter


class SchemaDumper:
    """Класс для получения структуры базы данных"""
    
    @staticmethod
    def get_schema(conn, db_type):
        """Получить структуру БД"""
        if db_type == DatabaseAdapter.POSTGRES:
            return SchemaDumper._get_postgres_schema(conn)
        elif db_type == DatabaseAdapter.ORACLE:
            return SchemaDumper._get_oracle_schema(conn)
        return {'error': 'Unsupported database type'}
    
    @staticmethod
    def _get_postgres_schema(conn):
        """Получить структуру PostgreSQL (все схемы)"""
        schema = {'schemas': {}, 'all_schemas': []}
        cur = conn.cursor()
        try:
            # Получаем список всех схем (кроме системных)
            cur.execute("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                ORDER BY schema_name
            """)
            schemas = [row[0] for row in cur.fetchall()]
            schema['all_schemas'] = schemas
            
            for schema_name in schemas:
                schema['schemas'][schema_name] = {
                    'tables': [],
                    'views': [],
                    'sequences': [],
                    'functions': [],
                    'procedures': [],
                    'triggers': []
                }
                
                # Таблицы
                cur.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = %s AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                """, (schema_name,))
                tables = [row[0] for row in cur.fetchall()]
                
                for table in tables:
                    # Колонки
                    cur.execute("""
                        SELECT column_name, data_type, character_maximum_length, 
                               numeric_precision, numeric_scale, is_nullable, column_default
                        FROM information_schema.columns
                        WHERE table_schema = %s AND table_name = %s
                        ORDER BY ordinal_position
                    """, (schema_name, table))
                    columns = []
                    for row in cur.fetchall():
                        columns.append({
                            'name': row[0],
                            'type': row[1],
                            'length': row[2],
                            'precision': row[3],
                            'scale': row[4],
                            'nullable': row[5] == 'YES',
                            'default': row[6]
                        })
                    
                    # Ограничения
                    cur.execute("""
                        SELECT tc.constraint_name, tc.constraint_type, kcu.column_name
                        FROM information_schema.table_constraints tc
                        LEFT JOIN information_schema.key_column_usage kcu 
                            ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
                        WHERE tc.table_schema = %s AND tc.table_name = %s
                    """, (schema_name, table))
                    constraints = []
                    for row in cur.fetchall():
                        constraints.append({'name': row[0], 'type': row[1], 'column': row[2]})
                    
                    # Индексы
                    cur.execute("""
                        SELECT indexname, indexdef
                        FROM pg_indexes
                        WHERE schemaname = %s AND tablename = %s
                    """, (schema_name, table))
                    indexes = []
                    for row in cur.fetchall():
                        indexes.append({'name': row[0], 'definition': row[1]})
                    
                    schema['schemas'][schema_name]['tables'].append({
                        'name': table,
                        'schema': schema_name,
                        'columns': columns,
                        'constraints': constraints,
                        'indexes': indexes
                    })
                
                # Представления
                cur.execute("""
                    SELECT table_name, view_definition
                    FROM information_schema.views
                    WHERE table_schema = %s
                    ORDER BY table_name
                """, (schema_name,))
                for row in cur.fetchall():
                    schema['schemas'][schema_name]['views'].append({
                        'name': row[0], 
                        'definition': row[1],
                        'schema': schema_name
                    })
                
                # Последовательности
                cur.execute("""
                    SELECT sequence_name, start_value, minimum_value, maximum_value, increment
                    FROM information_schema.sequences
                    WHERE sequence_schema = %s
                    ORDER BY sequence_name
                """, (schema_name,))
                for row in cur.fetchall():
                    schema['schemas'][schema_name]['sequences'].append({
                        'name': row[0], 'start': row[1], 'min': row[2],
                        'max': row[3], 'increment': row[4],
                        'schema': schema_name
                    })
                
                # Функции
                cur.execute("""
                    SELECT routine_name, routine_type, data_type, pg_get_functiondef(p.oid) as definition
                    FROM information_schema.routines r
                    JOIN pg_proc p ON p.proname = r.routine_name
                    JOIN pg_namespace n ON n.oid = p.pronamespace
                    WHERE n.nspname = %s AND r.routine_type = 'FUNCTION'
                    ORDER BY routine_name
                """, (schema_name,))
                for row in cur.fetchall():
                    schema['schemas'][schema_name]['functions'].append({
                        'name': row[0], 
                        'type': row[1], 
                        'return_type': row[2],
                        'definition': row[3],
                        'schema': schema_name
                    })
                
                # Процедуры
                cur.execute("""
                    SELECT routine_name, routine_type, pg_get_functiondef(p.oid) as definition
                    FROM information_schema.routines r
                    JOIN pg_proc p ON p.proname = r.routine_name
                    JOIN pg_namespace n ON n.oid = p.pronamespace
                    WHERE n.nspname = %s AND r.routine_type = 'PROCEDURE'
                    ORDER BY routine_name
                """, (schema_name,))
                for row in cur.fetchall():
                    schema['schemas'][schema_name]['procedures'].append({
                        'name': row[0], 
                        'type': row[1], 
                        'definition': row[2],
                        'schema': schema_name
                    })
                
                # Триггеры
                cur.execute("""
                    SELECT trigger_name, event_object_table, action_timing, action_condition, action_statement
                    FROM information_schema.triggers
                    WHERE trigger_schema = %s
                    ORDER BY trigger_name
                """, (schema_name,))
                for row in cur.fetchall():
                    schema['schemas'][schema_name]['triggers'].append({
                        'name': row[0],
                        'table': row[1],
                        'timing': row[2],
                        'condition': row[3],
                        'statement': row[4],
                        'schema': schema_name
                    })
                
        except Exception as e:
            print(f"[PG Schema] Error: {e}")
            schema['error'] = str(e)
        finally:
            cur.close()
        
        return schema
    
    @staticmethod
    def _get_oracle_schema(conn):
        """Получить структуру Oracle"""
        schema = {'tables': [], 'views': [], 'sequences': [], 'procedures': [], 'triggers': []}
        cur = conn.cursor()
        
        try:
            # Таблицы
            cur.execute("SELECT table_name, tablespace_name, num_rows, blocks FROM user_tables ORDER BY table_name")
            tables = cur.fetchall()
            
            for table_row in tables:
                table = table_row[0]
                
                # Колонки
                cur.execute("""
                    SELECT column_name, data_type, data_length, data_precision, 
                           data_scale, nullable, column_id, data_default
                    FROM user_tab_columns
                    WHERE table_name = %s ORDER BY column_id
                """, (table,))
                columns = []
                for row in cur.fetchall():
                    columns.append({
                        'name': row[0], 'type': row[1], 'length': row[2],
                        'precision': row[3], 'scale': row[4],
                        'nullable': row[5] == 'Y', 'position': row[6], 'default': row[7]
                    })
                
                # Ограничения
                cur.execute("SELECT constraint_name, constraint_type, status FROM user_constraints WHERE table_name = %s", (table,))
                constraints = []
                for row in cur.fetchall():
                    constraints.append({'name': row[0], 'type': row[1], 'status': row[2]})
                
                # Индексы
                cur.execute("SELECT index_name, uniqueness, status FROM user_indexes WHERE table_name = %s", (table,))
                indexes = []
                for row in cur.fetchall():
                    indexes.append({'name': row[0], 'unique': row[1], 'status': row[2]})
                
                schema['tables'].append({
                    'name': table, 'tablespace': table_row[1],
                    'rows': table_row[2], 'blocks': table_row[3],
                    'columns': columns, 'constraints': constraints, 'indexes': indexes
                })
            
            # Представления
            cur.execute("SELECT view_name, text FROM user_views ORDER BY view_name")
            for row in cur.fetchall():
                schema['views'].append({'name': row[0], 'definition': row[1]})
            
            # Последовательности
            cur.execute("SELECT sequence_name, min_value, max_value, increment_by FROM user_sequences ORDER BY sequence_name")
            for row in cur.fetchall():
                schema['sequences'].append({'name': row[0], 'min': row[1], 'max': row[2], 'increment': row[3]})
            
            # Процедуры и функции
            cur.execute("SELECT object_name, object_type, status FROM user_objects WHERE object_type IN ('PROCEDURE', 'FUNCTION') ORDER BY object_type, object_name")
            for row in cur.fetchall():
                schema['procedures'].append({'name': row[0], 'type': row[1], 'status': row[2]})
            
            # Триггеры
            cur.execute("SELECT trigger_name, table_name, triggering_event, status FROM user_triggers ORDER BY table_name, trigger_name")
            for row in cur.fetchall():
                schema['triggers'].append({'name': row[0], 'table': row[1], 'event': row[2], 'status': row[3]})
                
        except Exception as e:
            print(f"[Oracle Schema] Error: {e}")
            schema['error'] = str(e)
        finally:
            cur.close()
        
        return schema
    
    @staticmethod
    def generate_sql(schema, db_type):
        """Генерирует SQL DDL из структуры"""
        sql_lines = []
        
        if db_type == DatabaseAdapter.POSTGRES:
            # Проверяем новую структуру с schemas
            if 'schemas' in schema:
                schemas = schema.get('schemas', {})
                for schema_name, schema_data in schemas.items():
                    sql_lines.append(f"-- ============================================")
                    sql_lines.append(f"-- Schema: {schema_name}")
                    sql_lines.append(f"-- ============================================")
                    
                    # Таблицы
                    for table in schema_data.get('tables', []):
                        sql_lines.append(f"CREATE TABLE {schema_name}.{table['name']} (")
                        col_defs = []
                        for col in table['columns']:
                            col_str = f"    {col['name']} {col['type']}"
                            if col['length']: col_str += f"({col['length']})"
                            if col['precision'] and col['scale']: col_str += f"({col['precision']},{col['scale']})"
                            if not col['nullable']: col_str += " NOT NULL"
                            if col['default']: col_str += f" DEFAULT {col['default']}"
                            col_defs.append(col_str)
                        pks = [c['column'] for c in table['constraints'] if c['type'] == 'PRIMARY KEY']
                        if pks: col_defs.append(f"    PRIMARY KEY ({', '.join(pks)})")
                        sql_lines.append(',\n'.join(col_defs))
                        sql_lines.append(");\n")
                    
                    # Индексы
                    for table in schema_data.get('tables', []):
                        for idx in table.get('indexes', []):
                            if 'PRIMARY' not in idx['definition'].upper():
                                sql_lines.append(f"{idx['definition']};\n")
                    
                    # Представления
                    for view in schema_data.get('views', []):
                        definition = view.get('definition') or ''
                        sql_lines.append(f"CREATE OR REPLACE VIEW {schema_name}.{view['name']} AS {definition};\n")
                    
                    # Последовательности
                    for seq in schema_data.get('sequences', []):
                        sql_lines.append(f"CREATE SEQUENCE {schema_name}.{seq['name']} START {seq['start']} INCREMENT {seq['increment']};\n")
                    
                    # Функции
                    for func in schema_data.get('functions', []):
                        definition = func.get('definition') or ''
                        if definition:
                            sql_lines.append(f"{definition}\n")
                        else:
                            return_type = func.get('return_type', '')
                            sql_lines.append(f"CREATE OR REPLACE FUNCTION {schema_name}.{func['name']}() RETURNS {return_type} AS $$-- TODO: add function body$$ LANGUAGE plpgsql;\n")
                    
                    # Процедуры
                    for proc in schema_data.get('procedures', []):
                        definition = proc.get('definition') or ''
                        if definition:
                            sql_lines.append(f"{definition}\n")
                        else:
                            sql_lines.append(f"CREATE OR REPLACE PROCEDURE {schema_name}.{proc['name']}() AS $$-- TODO: add procedure body$$ LANGUAGE plpgsql;\n")
                    
                    # Триггеры
                    for trig in schema_data.get('triggers', []):
                        table_name = trig.get('table', '')
                        timing = trig.get('timing', '')
                        statement = trig.get('statement', '')
                        if statement:
                            sql_lines.append(f"CREATE OR REPLACE TRIGGER {trig['name']}")
                            sql_lines.append(f"    {timing} ON {schema_name}.{table_name}")
                            sql_lines.append(f"    {statement};\n")
            else:
                # Старая структура без схем
                for table in schema.get('tables', []):
                    sql_lines.append(f"CREATE TABLE {table['name']} (")
                    col_defs = []
                    for col in table['columns']:
                        col_str = f"    {col['name']} {col['type']}"
                        if col['length']: col_str += f"({col['length']})"
                        if col['precision'] and col['scale']: col_str += f"({col['precision']},{col['scale']})"
                        if not col['nullable']: col_str += " NOT NULL"
                        if col['default']: col_str += f" DEFAULT {col['default']}"
                        col_defs.append(col_str)
                    pks = [c['column'] for c in table['constraints'] if c['type'] == 'PRIMARY KEY']
                    if pks: col_defs.append(f"    PRIMARY KEY ({', '.join(pks)})")
                    sql_lines.append(',\n'.join(col_defs))
                    sql_lines.append(");\n")
                
                for table in schema.get('tables', []):
                    for idx in table.get('indexes', []):
                        if 'PRIMARY' not in idx['definition'].upper():
                            sql_lines.append(f"{idx['definition']};\n")
                
                for view in schema.get('views', []):
                    sql_lines.append(f"CREATE VIEW {view['name']} AS {view.get('definition', '')};\n")
                
                for seq in schema.get('sequences', []):
                    sql_lines.append(f"CREATE SEQUENCE {seq['name']} START {seq['start']} INCREMENT {seq['increment']};\n")
        
        elif db_type == DatabaseAdapter.ORACLE:
            # Таблицы
            for table in schema.get('tables', []):
                sql_lines.append(f"CREATE TABLE {table['name']} (")
                col_defs = []
                for col in table['columns']:
                    col_str = f"    {col['name']} {col['type']}"
                    if col['length']: col_str += f"({col['length']})"
                    if col['precision'] and col['scale']: col_str += f"({col['precision']},{col['scale']})"
                    if not col['nullable']: col_str += " NOT NULL"
                    if col['default']: col_str += f" DEFAULT {col['default']}"
                    col_defs.append(col_str)
                pks = [c['column'] for c in table['constraints'] if c['type'] == 'P']
                if pks: col_defs.append(f"    PRIMARY KEY ({', '.join(pks)})")
                sql_lines.append(',\n'.join(col_defs))
                sql_lines.append(f") TABLESPACE {table.get('tablespace', 'USERS')};\n")
            
            # Представления
            for view in schema.get('views', []):
                sql_lines.append(f"CREATE OR REPLACE VIEW {view['name']} AS {view['definition']};\n")
            
            # Последовательности
            for seq in schema.get('sequences', []):
                sql_lines.append(f"CREATE SEQUENCE {seq['name']} MINVALUE {seq['min']} MAXVALUE {seq['max']} INCREMENT BY {seq['increment']};\n")
            
            # Триггеры
            for trig in schema.get('triggers', []):
                sql_lines.append(f"-- Trigger: {trig['name']} on {trig['table']} ({trig['event']})\n")
        
        return '\n'.join(sql_lines)

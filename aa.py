# !/usr/bin/env python3
import os
import psycopg2
import sys

# Параметры подключения — настройте под вашу БД
DB_PARAMS = {
    'host': '10.100.102.90',
    'port': 5432,
    'database': 'rul_jkh',  # Замените
    'user': 'rul_developer',  # Замените
    'password': '1234567890!@#$%^&*()'  # Замените или используйте .pgpass
}


def get_tables(conn, schema='public'):
    """Получить список таблиц в указанной схеме"""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT tablename 
            FROM pg_tables 
            WHERE schemaname = %s 
              AND tablename NOT LIKE 'pg\\_%%'
              AND tablename NOT LIKE 'sql\\_%%'
            ORDER BY tablename;
        """, (schema,))
        return [row[0] for row in cur.fetchall()]


def get_table_comment(conn, schema, table_name):
    """Получить комментарий к таблице"""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT obj_description(oid, 'pg_class') 
            FROM pg_class 
            WHERE oid = %s::regclass 
              AND obj_description(oid, 'pg_class') IS NOT NULL;
        """, (f'{schema}.{table_name}',))
        result = cur.fetchone()
        return result[0] if result else None


def get_column_comments(conn, schema, table_name):
    """Получить комментарии ко всем столбцам таблицы"""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
                a.attname AS column_name,
                col_description(a.attrelid, a.attnum) AS comment
            FROM pg_catalog.pg_attribute a
            WHERE a.attrelid = %s::regclass
              AND a.attnum > 0 
              AND NOT a.attisdropped
              AND col_description(a.attrelid, a.attnum) IS NOT NULL
            ORDER BY a.attnum;
        """, (f'{schema}.{table_name}',))
        return {row[0]: row[1] for row in cur.fetchall()}


def get_columns_ddl(conn, schema, table_name):
    """Получить определение столбцов таблицы"""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
                a.attname AS column_name,
                pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
                CASE 
                    WHEN a.atthasdef THEN pg_catalog.pg_get_expr(d.adbin, d.adrelid)
                    ELSE NULL
                END AS default_value,
                a.attnotnull AS not_null
            FROM pg_catalog.pg_attribute a
            LEFT JOIN pg_catalog.pg_attrdef d ON (a.attrelid = d.adrelid AND a.attnum = d.adnum)
            WHERE a.attrelid = %s::regclass
              AND a.attnum > 0 
              AND NOT a.attisdropped
            ORDER BY a.attnum;
        """, (f'{schema}.{table_name}',))
        return cur.fetchall()


def get_constraints_ddl(conn, schema, table_name):
    """Получить определение ограничений таблицы (PK, FK, UNIQUE, CHECK)"""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
                conname AS constraint_name,
                contype AS constraint_type,
                pg_get_constraintdef(oid) AS constraint_def
            FROM pg_catalog.pg_constraint
            WHERE conrelid = %s::regclass
              AND contype IN ('p', 'f', 'u', 'c')
            ORDER BY 
                CASE contype 
                    WHEN 'p' THEN 1  -- PRIMARY KEY первым
                    WHEN 'f' THEN 3  -- FOREIGN KEY последним
                    ELSE 2           -- остальные посередине
                END,
                conname;
        """, (f'{schema}.{table_name}',))
        return cur.fetchall()


def escape_sql_string(s):
    """Экранировать строку для использования в SQL (замена одинарных кавычек)"""
    return s.replace("'", "''")


def generate_table_ddl(conn, schema, table_name):
    """Сгенерировать полный DDL для одной таблицы с комментариями"""
    # Столбцы
    columns = get_columns_ddl(conn, schema, table_name)
    if not columns:
        return None

    lines = [f'CREATE TABLE {schema}.{table_name} (']

    # Определения столбцов
    col_defs = []
    for col_name, data_type, default_val, not_null in columns:
        col_def = f'    {col_name} {data_type}'
        if default_val:
            col_def += f' DEFAULT {default_val}'
        if not_null:
            col_def += ' NOT NULL'
        col_defs.append(col_def)

    # Добавляем столбцы с запятыми
    for i, col_def in enumerate(col_defs):
        if i < len(col_defs) - 1:
            lines.append(col_def + ',')
        else:
            lines.append(col_def)

    # Ограничения
    constraints = get_constraints_ddl(conn, schema, table_name)
    if constraints:
        lines.append('    ,')
        for i, (con_name, con_type, con_def) in enumerate(constraints):
            lines.append(f'    CONSTRAINT {con_name} {con_def}' + (',' if i < len(constraints) - 1 else ''))

    lines.append(');')

    # Комментарий к таблице
    table_comment = get_table_comment(conn, schema, table_name)
    if table_comment:
        lines.append('')
        escaped_comment = escape_sql_string(table_comment)
        lines.append(f"COMMENT ON TABLE {schema}.{table_name} IS '{escaped_comment}';")

    # Комментарии к столбцам
    column_comments = get_column_comments(conn, schema, table_name)
    if column_comments:
        if table_comment:
            lines.append('')
        for col_name, comment in sorted(column_comments.items()):
            escaped_comment = escape_sql_string(comment)
            lines.append(f"COMMENT ON COLUMN {schema}.{table_name}.{col_name} IS '{escaped_comment}';")

    return '\n'.join(lines)


def main():
    schema = 'public'
    output_dir = 'tables'

    # Создаём папку
    os.makedirs(output_dir, exist_ok=True)
    print(f"📁 Папка '{output_dir}' создана")

    # Подключаемся к БД
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        print(f"🔌 Подключено к БД: {DB_PARAMS['database']}@{DB_PARAMS['host']}:{DB_PARAMS['port']}")
    except Exception as e:
        print(f"❌ Ошибка подключения: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        tables = get_tables(conn, schema)
        print(f"📋 Найдено таблиц в схеме '{schema}': {len(tables)}\n")

        if not tables:
            print("⚠ Нет таблиц для экспорта")
            return

        # Генерируем общий файл
        all_ddl_lines = []
        successful = 0

        for table in tables:
            try:
                ddl = generate_table_ddl(conn, schema, table)
                if not ddl:
                    print(f"⚠ Пропущена пустая таблица: {table}")
                    continue

                # Добавляем разделитель между таблицами для наглядности
                if successful > 0:
                    all_ddl_lines.append('')
                    all_ddl_lines.append('-- ' + '=' * 70)
                    all_ddl_lines.append('')

                all_ddl_lines.append(f'-- Таблица: {schema}.{table}')
                all_ddl_lines.append(ddl)

                print(f"✓ {table}")
                successful += 1

            except Exception as e:
                print(f"⚠ Ошибка при обработке {table}: {e}", file=sys.stderr)

        # Записываем в один файл
        output_file = os.path.join(output_dir, 'all_tables.sql')
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(all_ddl_lines).rstrip() + '\n')

        print(f"\n✅ Успешно экспортировано: {successful}/{len(tables)} таблиц")
        print(f"📁 Общий файл сохранён: ./{output_dir}/all_tables.sql")

    finally:
        conn.close()


if __name__ == '__main__':
    main()
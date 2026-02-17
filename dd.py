#!/usr/bin/env python3
import os
import psycopg2
import sys
from datetime import datetime

# Параметры подключения — настройте под вашу БД
DB_PARAMS = {
    'host': '10.100.102.90',
    'port': 5432,
    'database': 'rul_jkh',  # Замените
    'user': 'rul_developer',  # Замените
    'password': '1234567890!@#$%^&*()'  # Замените или используйте .pgpass
}



def get_functions_and_procedures(conn, schema='public'):
    """Получить список функций и процедур из указанной схемы"""
    with conn.cursor() as cur:
        # Проверяем версию PostgreSQL для совместимости с prokind
        cur.execute("SELECT current_setting('server_version_num')::integer >= 110000")
        supports_prokind = cur.fetchone()[0]

        if supports_prokind:
            cur.execute("""
                SELECT 
                    p.proname AS name,
                    CASE 
                        WHEN p.prokind = 'p' THEN 'PROCEDURE'
                        ELSE 'FUNCTION'
                    END AS type,
                    p.oid AS oid,
                    pg_get_function_identity_arguments(p.oid) AS args_signature
                FROM pg_proc p
                JOIN pg_namespace n ON p.pronamespace = n.oid
                WHERE n.nspname = %s
                  AND p.prokind IN ('f', 'p')
                ORDER BY 
                    CASE WHEN p.prokind = 'p' THEN 1 ELSE 2 END,
                    p.proname,
                    pg_get_function_identity_arguments(p.oid);
            """, (schema,))
        else:
            cur.execute("""
                SELECT 
                    p.proname AS name,
                    'FUNCTION' AS type,
                    p.oid AS oid,
                    pg_get_function_identity_arguments(p.oid) AS args_signature
                FROM pg_proc p
                JOIN pg_namespace n ON p.pronamespace = n.oid
                WHERE n.nspname = %s
                ORDER BY p.proname, pg_get_function_identity_arguments(p.oid);
            """, (schema,))

        return cur.fetchall()


def get_function_definition(conn, oid):
    """Получить полное определение функции/процедуры через pg_get_functiondef"""
    with conn.cursor() as cur:
        cur.execute("SELECT pg_get_functiondef(%s)", (oid,))
        result = cur.fetchone()
        return result[0] if result else None


def get_function_comment(conn, oid):
    """Получить комментарий к функции/процедуре"""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT obj_description(%s, 'pg_proc')
            WHERE obj_description(%s, 'pg_proc') IS NOT NULL;
        """, (oid, oid))
        result = cur.fetchone()
        return result[0] if result else None


def escape_sql_string(s):
    """Экранировать строку для использования в SQL"""
    return s.replace("'", "''")


def clean_function_body(definition):
    """
    Удаляет ВСЕ пустые строки внутри определения функции/процедуры,
    сохраняя только содержательные строки с исходными отступами
    """
    # Разбиваем на строки, фильтруем пустые, сохраняем отступы
    lines = [line.rstrip() for line in definition.splitlines() if line.strip() != '']

    # Восстанавливаем структуру: добавляем пустую строку только перед AS $...$
    # и после завершения блока (но не внутри тела)
    cleaned_lines = []
    in_function_body = False

    for line in lines:
        if line.strip().startswith('AS $'):
            in_function_body = True
            cleaned_lines.append(line)
        elif in_function_body and line.strip().endswith('$;'):
            in_function_body = False
            cleaned_lines.append(line)
        elif in_function_body:
            # Сохраняем только непустые строки внутри тела функции
            cleaned_lines.append(line)
        else:
            cleaned_lines.append(line)

    return '\n'.join(cleaned_lines)


def main():
    schema = 'public'
    output_dir = 'tables'

    # Создаём папку
    os.makedirs(output_dir, exist_ok=True)
    print(f"📁 Папка '{output_dir}' готова")

    # Подключаемся к БД
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        print(f"🔌 Подключено к БД: {DB_PARAMS['database']}@{DB_PARAMS['host']}:{DB_PARAMS['port']}")
    except Exception as e:
        print(f"❌ Ошибка подключения: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        objects = get_functions_and_procedures(conn, schema)
        print(f"📋 Найдено объектов в схеме '{schema}': {len(objects)} (функции и процедуры)\n")

        if not objects:
            print("⚠ Нет функций или процедур для экспорта")
            output_file = os.path.join(output_dir, 'all_functions.sql')
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write('-- Нет функций или процедур в схеме public\n')
            print(f"📁 Пустой файл сохранён: ./{output_dir}/all_functions.sql")
            return

        # Формируем содержимое файла
        lines = []

        # Шапка файла
        lines.append(f'-- Экспорт функций и процедур схемы {schema}')
        lines.append(f'-- База данных: {DB_PARAMS["database"]}')
        lines.append(f'-- Дата экспорта: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        lines.append('')

        successful = 0
        for name, obj_type, oid, args_signature in objects:
            try:
                # Получаем определение
                definition = get_function_definition(conn, oid)
                if not definition:
                    print(f"⚠ Пропущен объект без определения: {name}")
                    continue

                # Очищаем от лишних пустых строк
                cleaned_definition = clean_function_body(definition)

                # Получаем комментарий
                comment = get_function_comment(conn, oid)

                # Добавляем разделитель между объектами (только перед вторым+)
                if successful > 0:
                    lines.append('')
                    lines.append('-- ' + '=' * 70)
                    lines.append('')

                # Заголовок объекта
                lines.append(f'-- {obj_type}: {schema}.{name}({args_signature})')

                # Комментарий-описание (если есть)
                if comment:
                    lines.append(f'-- Описание: {comment}')
                    lines.append('')  # Одна пустая строка перед определением

                # Само определение БЕЗ лишних пустых строк
                lines.append(cleaned_definition)

                # Команда COMMENT ON (если есть комментарий)
                if comment:
                    lines.append('')  # Одна пустая строка после определения
                    escaped_comment = escape_sql_string(comment)
                    if obj_type == 'PROCEDURE':
                        lines.append(f"COMMENT ON PROCEDURE {schema}.{name}({args_signature}) IS '{escaped_comment}';")
                    else:
                        lines.append(f"COMMENT ON FUNCTION {schema}.{name}({args_signature}) IS '{escaped_comment}';")

                print(f"✓ {obj_type.lower()} {name}({args_signature})")
                successful += 1

            except Exception as e:
                print(f"⚠ Ошибка при обработке {obj_type.lower()} {name}: {e}", file=sys.stderr)

        # Финальный перенос строки
        if lines and lines[-1] != '':
            lines.append('')

        # Записываем в файл
        output_file = os.path.join(output_dir, 'all_functions.sql')
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines))

        print(f"\n✅ Успешно экспортировано: {successful}/{len(objects)} объектов")
        print(f"📁 Файл сохранён: ./{output_dir}/all_functions.sql")

    finally:
        conn.close()


if __name__ == '__main__':
    main()
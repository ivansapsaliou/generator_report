from flask import Flask, render_template, jsonify, request, flash, redirect, url_for
import psycopg2
import psycopg2.extras
import json
from config import Config
import re
import psycopg2.extensions
from psycopg2.extensions import AsIs
from collections import defaultdict


app = Flask(__name__)
app.config.from_object(Config)
app.secret_key = app.config['SECRET_KEY']


def get_db_connection():
    """Создает подключение к PostgreSQL"""
    conn = psycopg2.connect(
        host=app.config['DB_HOST'],
        port=app.config['DB_PORT'],
        database=app.config['DB_NAME'],
        user=app.config['DB_USER'],
        password=app.config['DB_PASSWORD'],
        client_encoding='UTF8'
    )

    #conn.set_client_encoding('UTF8')

    return conn


def escape_composite_value(val):
    """Экранирует значение для composite-массива: заменяет " на "", удаляет переносы"""
    if val is None:
        return ''
    s = str(val).replace('"', '""')
    # Удаляем переносы, чтобы не ломать структуру
    s = s.replace('\n', ' ').replace('\r', ' ')
    return s


def build_composite_array(items, field_count, type_name):
    """
    Строит строку вида '{(v1,v2,...),(v3,v4,...)}\'::type[]
    items: список кортежей или словарей
    field_count: сколько полей в composite-типе
    """
    if not items:
        return 'NULL'

    elements = []
    for item in items:
        if isinstance(item, dict):
            # Берём значения по порядку: предполагаем, что ключи соответствуют полям типа
            # Но лучше — передавать как tuple или list
            print(">>> Полученные данные 222222:", item)

            vals = [escape_composite_value(item.get(f'f{i + 1}', '')) for i in range(field_count)]
        elif isinstance(item, (tuple, list)):
            print(">>> Полученные данные 222222:", item[1])
            vals = [escape_composite_value(v) for v in item[:field_count]]
        else:
            raise ValueError("Unsupported item type")

        # Формируем элемент: (val1,val2,...)
        element = "'ROW('" + "','".join(vals) + "')::" + type_name + "'"
        elements.append(element)

    array_str = '[' + ','.join(elements) + ']'

    print(">>> Полученные данные 222222:", array_str)

    return f"{array_str}"


def make_composite_array(items, composite_type):
    """
    Создаёт объект для передачи в psycopg2 как массив составного типа.
    items: список кортежей (val1, val2, ...)
    composite_type: имя типа, напр. 'report_sort' (больше не используется внутри, но оставлен для совместимости)
    """
    if not items:
        return None
    # Возвращаем список кортежей. psycopg2 сам преобразует это в ARRAY[ROW(...)].
    return items

def escape_composite_value(val):
    """Экранирует значение для composite-массива: заменяет " на "", удаляет переносы"""
    if val is None:
        return ''
    s = str(val).replace('"', '""')
    # Удаляем переносы, чтобы не ломать структуру
    s = s.replace('\n', ' ').replace('\r', ' ')
    return s

def safe_literal(s):
    """Безопасное обертывание строкового литерала (значения)"""
    return psycopg2.extensions.adapt(s).getquoted().decode('utf-8')

def safe_ident(s):
    """
    Безопасное обертывание идентификатора (таблица, колонка, алиас).
    Идентификаторы в SQL оборачиваются в двойные кавычки.
    """
    if s is None:
        return ''
    # Экранируем двойные кавычки внутри имени (например "Мой "Алиас"" -> "Мой ""Алиас""")
    escaped = str(s).replace('"', '""')
    # Оборачиваем в двойные кавычки для поддержки пробелов, кириллицы и спецсимволов
    return f'"{escaped}"'

@app.route('/')
def index():
    """Главная страница - выбор таблицы"""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Получаем список таблиц
        cur.execute("""
            SELECT * FROM report_get_tables('public')
            ORDER BY table_name
        """)
        tables = cur.fetchall()

        cur.close()
        conn.close()

        return render_template('index.html', tables=tables)

    except Exception as e:
        flash(f'Ошибка при загрузке таблиц: {str(e)}', 'danger')
        return render_template('index.html', tables=[])


# Добавьте этот декоратор перед функцией get_tables
@app.route('/api/get-tables', methods=['GET'])
@app.route('/api/tables')
def get_tables():
    """API: Получить список таблиц"""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        cur.execute("""
            SELECT * FROM report_get_tables('public')
            ORDER BY table_name
        """)
        tables = cur.fetchall()

        result = [{'table_name': row['table_name'], 'table_comment': row['table_comment']}
                  for row in tables]

        cur.close()
        conn.close()

        return jsonify({'success': True, 'data': result})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/table/<table_name>/columns')
@app.route('/api/get-table-columns', methods=['GET'])
def get_table_columns(table_name):
    """API: Получить колонки таблицы (одна таблица)"""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Вызываем с массивом из одного элемента
        cur.execute("""
            SELECT * FROM report_get_columns(%s, 'public')
            ORDER BY column_name
        """, ([table_name],))
        columns = cur.fetchall()

        result = [{
            'column_name': row['column_name'],
            'data_type': row['data_type'],
            'is_nullable': row['is_nullable'],
            'column_default': row['column_default'],
            'column_comment': row['column_comment']
        } for row in columns]

        cur.close()
        conn.close()

        return jsonify({'success': True, 'data': result})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/tables/columns', methods=['POST'])
def get_tables_columns():
    """API: Получить колонки для массива таблиц (например, для джойнов)"""
    try:
        tables = request.json.get('tables', [])
        if not tables or not isinstance(tables, list):
            return jsonify({'success': False, 'error': 'Не переданы имена таблиц'}), 400

        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Вызываем с массивом таблиц
        cur.execute("""
            SELECT * FROM report_get_columns(%s, 'public')
            ORDER BY table_name, column_name
        """, (tables,))
        columns = cur.fetchall()

        # Группируем по имени таблицы: {table1: [...], table2: [...]}
        result = defaultdict(list)
        for row in columns:
            result[row['table_name']].append({
                'column_name': row['column_name'],
                'data_type': row['data_type'],
                'is_nullable': row['is_nullable'],
                'column_default': row['column_default'],
                'column_comment': row['column_comment']
            })

        cur.close()
        conn.close()

        return jsonify({'success': True, 'data': dict(result)})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/table/<table_name>/possible-joins')
@app.route('/api/get-possible-joins', methods=['GET'])
def get_possible_joins(table_name):
    """API: Получить возможные джойны для таблицы"""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Получаем прямые джойны
        cur.execute("""
            SELECT * FROM report_get_possible_joins(%s, 'public', %s, true)
            ORDER BY match_confidence DESC, target_table
        """, (table_name,[table_name]))
        joins = cur.fetchall()

        print(joins)
        # Группируем по таблицам
        result = {}
        for join in joins:
            target_table = join['target_table']
            if target_table not in result:
                result[target_table] = {
                    'table_name': target_table,
                    'schema': join['target_schema'],
                    'possible_joins': []
                }

            result[target_table]['possible_joins'].append({
                'join_type': join['join_type'],
                'source_column': join['source_column'],
                'target_column': join['target_column'],
                'constraint_name': join['constraint_name'],
                'match_confidence': float(join['match_confidence']),
                'join_suggestion': join['join_suggestion']
            })

        cur.close()
        conn.close()

        return jsonify({'success': True, 'data': list(result.values())})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/generate-report', methods=['POST'])
def generate_report():
    """API: Сгенерировать отчет"""
    try:
        data = request.json
        print(">>> Полученные данные:", json.dumps(data, indent=2, ensure_ascii=False))

        main_table = data.get('main_table')
        columns = data.get('columns', [])
        joins = data.get('joins', [])
        conditions = data.get('conditions', [])
        aggregates = data.get('aggregates', [])
        group_by = data.get('group_by', [])
        sort = data.get('sort', [])
        limit = data.get('limit', 100)
        offset = data.get('offset', 0)

        # --- Подготовка данных для PostgreSQL ---
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Формируем строки для ARRAY[...] — безопасно через quote_ident/quote_literal
        def safe_ident(s):
            return psycopg2.extensions.adapt(s).getquoted().decode('utf-8').strip("'")

        def safe_literal(s):
            return psycopg2.extensions.adapt(s).getquoted().decode('utf-8')

        # JOINs
        joins_param = None
        if joins:
            join_tuples = [
                (
                    j.get('source_table'),      # 1. source_table (Добавлено)
                    j['table_name'],            # 2. table_name
                    j.get('alias', j['table_name']), # 3. alias
                    j['join_type'],             # 4. join_type
                    j['left_column'],           # 5. left_column
                    j['right_column'],          # 6. right_column
                    j.get('confidence', 0.0)    # 7. confidence (Добавлено с дефолтом)
                )
                for j in joins
            ]
            joins_param = make_composite_array(join_tuples, 'report_join')


        # Conditions
        conditions_param = None
        if conditions:
            cond_tuples = [
                (c['column_name'], c['operator'], c['value'], c.get('logic_operator', 'AND'))
                for c in conditions
            ]
            conditions_param = make_composite_array(cond_tuples, 'report_condition')

        # Aggregates
        aggregates_param = None
        if aggregates:
            agg_tuples = [
                (a['function_name'], a['column_name'], a.get('alias', ''))
                for a in aggregates
            ]
            aggregates_param = make_composite_array(agg_tuples, 'report_aggregate')

        # Sort
        sort_param = None
        if sort:
            # Превращаем в список кортежей: [(col, dir), ...]
            sort_tuples = []
            for s in sort:
                col = s.get('column_name', '').strip()
                dir_ = s.get('direction', 'ASC').upper().strip()
                if col:
                    sort_tuples.append((col, dir_))

            if sort_tuples:
                sort_param = make_composite_array(sort_tuples, 'report_sort')

        # --- Формируем SQL с параметрами только для значений, НЕ для структуры ---
        # Колонки для AS result(...) — генерируем как строку (без %s!)
        result_cols = []
        for col in columns:
            clean_col = re.sub(r'[^\w]', '_', col)  # заменяем недопустимые символы на _
            result_cols.append(f'"{clean_col}" TEXT')

        for agg in aggregates:
            alias = agg.get('alias') or f"{agg['function_name'].upper()}_{agg['column_name']}"
            clean_alias = re.sub(r'[^\w]', '_', alias)
            result_cols.append(f'"{clean_alias}" TEXT')

        result_cols_str = ', '.join(result_cols) if result_cols else '"data" TEXT'

        # Теперь строим SQL-запрос как строку с %s-местозаполнителями
        sql_template = """
            SELECT * FROM report_generate(
                %s,                    -- p_main_table
                'public',              -- p_schema
                %s::report_join[],     -- p_joins (ЯВНОЕ ПРИВЕДЕНИЕ ТИПА)
                %s,                    -- p_columns
                %s::report_condition[],-- p_conditions (ЯВНОЕ ПРИВЕДЕНИЕ ТИПА)
                %s::report_aggregate[],-- p_aggregates (ЯВНОЕ ПРИВЕДЕНИЕ ТИПА)
                %s,                    -- p_group_by
                %s::report_sort[],     -- p_sort (ЯВНОЕ ПРИВЕДЕНИЕ ТИПА)
                %s,                    -- p_limit
                %s                     -- p_offset
            ) AS result({result_cols})
        """.format(result_cols=result_cols_str)

        # Подготавливаем параметры
        params = [
            main_table,
            joins_param,      # Теперь это просто list of tuples
            columns if columns else None,
            conditions_param, # Теперь это просто list of tuples
            aggregates_param, # Теперь это просто list of tuples
            group_by if group_by else None,
            sort_param,       # Теперь это просто list of tuples
            limit,
            offset
        ]


        # Выполняем
        cur.execute(sql_template, params)
        rows = cur.fetchall()

        column_names = [desc[0] for desc in cur.description]
        result_data = {
            'columns': column_names,
            'rows': [dict(row) for row in rows],
            'total': len(rows)
        }

        cur.close()
        conn.close()

        return jsonify({'success': True, 'data': result_data})

    except Exception as e:
        import traceback
        return jsonify({
            'success': False,
            'error': str(e),
            'details': traceback.format_exc()
        }), 500


@app.route('/report-builder')
def report_builder():
    """Страница конструктора отчетов"""
    template_id = request.args.get('template_id')
    table = request.args.get('table')

    # Если передан ID шаблона, но нет таблицы, пытаемся загрузить таблицу из конфигурации шаблона
    if template_id and not table:
        try:
            conn = get_db_connection()
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute("SELECT config FROM report_templates WHERE id = %s", (template_id,))
            template = cur.fetchone()
            cur.close()
            conn.close()

            if template:
                config_data = template['config']
                if isinstance(config_data, str):
                    config_data = json.loads(config_data)
                
                if config_data and 'main_table' in config_data:
                    table = config_data['main_table']
                    # ИСПРАВЛЕНИЕ: Вместо редиректа сразу рендерим шаблон.
                    # Это сохраняет template_id в URL браузера, и JavaScript сможет его прочитать.
                    return render_template('report.html', selected_table=table)
        except Exception as e:
            print(f"Error loading template config for route: {e}")

    # Если таблица так и не была определена, возвращаем на главную
    if not table:
        return redirect(url_for('index'))

    return render_template('report.html', selected_table=table)


@app.route('/api/columns-batch')
def get_columns_batch():
    tables = request.args.get('tables')
    if not tables:
        return jsonify({'success': False, 'error': 'No tables provided'})

    tables = tables.split(',')

    result = {}

    for table in tables:
        result[table] = get_columns_for_table(table)  # ваша существующая логика

    return jsonify({
        'success': True,
        'data': result
    })

@app.route('/api/save-report', methods=['POST'])
def save_report():
    """API: Сохранить шаблон отчета"""
    try:
        data = request.json
        name = data.get('name')
        config = data.get('config')

        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO report_templates (name, config, created_by)
            VALUES (%s, %s, %s)
            RETURNING id
        """, (name, json.dumps(config), 'current_user'))  # TODO: Получить текущего пользователя

        report_id = cur.fetchone()[0]
        conn.commit()

        cur.close()
        conn.close()

        return jsonify({'success': True, 'report_id': report_id})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/templates')
def get_templates():
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("""
            SELECT id, name, created_at, created_by 
            FROM report_templates 
            ORDER BY created_at DESC
        """)
        templates = cur.fetchall()
        result = [{
            'id': t['id'],
            'name': t['name'],
            'created_at': t['created_at'].isoformat() if t['created_at'] else None,
            'created_by': t['created_by']
        } for t in templates]
        cur.close()
        conn.close()
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/templates/<int:template_id>', methods=['PUT'])
def update_template(template_id: int):
    """API: Обновить/переименовать шаблон отчета"""
    try:
        data = request.json or {}
        name = data.get('name')
        config = data.get('config')

        if name is not None:
            name = str(name).strip()
            if not name:
                return jsonify({'success': False, 'error': 'Название шаблона не может быть пустым'}), 400

        if name is None and config is None:
            return jsonify({'success': False, 'error': 'Нет данных для обновления (name/config)'}), 400

        conn = get_db_connection()
        cur = conn.cursor()

        # Проверяем существование
        cur.execute("SELECT 1 FROM report_templates WHERE id = %s", (template_id,))
        if cur.fetchone() is None:
            cur.close()
            conn.close()
            return jsonify({'success': False, 'error': 'Шаблон не найден'}), 404

        config_json = None
        if config is not None:
            config_json = json.dumps(config, ensure_ascii=False)

        cur.execute(
            """
            UPDATE report_templates
            SET
                name = COALESCE(%s, name),
                config = COALESCE(%s::jsonb, config)
            WHERE id = %s
            """,
            (name, config_json, template_id)
        )
        conn.commit()

        cur.close()
        conn.close()

        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/templates/<int:template_id>', methods=['DELETE'])
def delete_template(template_id: int):
    """API: Удалить шаблон отчета"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("DELETE FROM report_templates WHERE id = %s", (template_id,))
        deleted = cur.rowcount
        conn.commit()

        cur.close()
        conn.close()

        if deleted == 0:
            return jsonify({'success': False, 'error': 'Шаблон не найден'}), 404

        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/templates/<int:template_id>/duplicate', methods=['POST'])
def duplicate_template(template_id: int):
    """API: Дублировать шаблон отчета"""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        cur.execute("SELECT name, config FROM report_templates WHERE id = %s", (template_id,))
        tpl = cur.fetchone()
        if not tpl:
            cur.close()
            conn.close()
            return jsonify({'success': False, 'error': 'Шаблон не найден'}), 404

        base_name = (tpl['name'] or 'Шаблон').strip()
        new_name = f"{base_name} (копия)"

        config_data = tpl['config']
        if isinstance(config_data, str):
            config_data = json.loads(config_data)

        cur2 = conn.cursor()
        cur2.execute(
            """
            INSERT INTO report_templates (name, config, created_by)
            VALUES (%s, %s::jsonb, %s)
            RETURNING id
            """,
            (new_name, json.dumps(config_data, ensure_ascii=False), 'current_user')
        )
        new_id = cur2.fetchone()[0]
        conn.commit()

        cur2.close()
        cur.close()
        conn.close()

        return jsonify({'success': True, 'report_id': new_id})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/load-report/<int:report_id>', methods=['GET'])
def load_report(report_id):
    """API: Загрузить шаблон отчета"""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        cur.execute("""
            SELECT * FROM report_templates WHERE id = %s
        """, (report_id,))

        report = cur.fetchone()

        if not report:
            return jsonify({'success': False, 'error': 'Отчет не найден'}), 404

        # ИСПРАВЛЕНИЕ: Проверяем тип config и парсим, если это строка
        config_data = report['config']
        if isinstance(config_data, str):
            config_data = json.loads(config_data)

        result = {
            'id': report['id'],
            'name': report['name'],
            'config': config_data, # Передаем уже распарсенный объект
            'created_at': report['created_at'].isoformat() if report['created_at'] else None
        }

        cur.close()
        conn.close()

        return jsonify({'success': True, 'data': result})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
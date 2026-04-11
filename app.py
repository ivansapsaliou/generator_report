from flask import Flask, render_template, jsonify, request, flash, redirect, url_for, Response, session, flash
import psycopg2
import psycopg2.extras
import json
from config import Config
import re
import psycopg2.extensions
from psycopg2.extensions import AsIs
from collections import defaultdict
import base64
import os

from datetime import datetime,timedelta
import io
import csv
import threading
import time
from functools import wraps
from auth_manager import AuthManager

from pg_sessions import PostgreSQLSessionManager
from monitoring_db import MonitoringDB



# Импорт модулей
from db_connection import (
    get_ssh_tunnels,
    get_ssh_tunnels_lock,
    is_paramiko_available,
    create_ssh_tunnel,
    close_ssh_tunnel,
    close_all_ssh_tunnels,
    is_tunnel_active,
    get_tunnel_info,
    get_existing_ssh_tunnel,
    get_db_connection as _get_db_connection_original,
    get_db_connection_by_profile
)
from mail_utils import send_email, send_test_email

from db_adapter import DatabaseAdapter  # ✅ НОВЫЙ ИМПОРТ
from db_schema_dumper import SchemaDumper

def get_db_connection(config=None, ssh_settings=None):
    """
    Переопределённая функция подключения к БД.
    Сначала проверяет активный профиль в сессии, потом использует конфиг.
    """
    try:
        # Проверяем активный профиль в сессии
        profile_id = session.get('active_profile_id')
        
        if profile_id:
            print(f"[DB] Using profile {profile_id} from session")
            return get_db_connection_by_profile(profile_id)
    except Exception as e:
        print(f"[DB] Error using profile from session: {e}")
        # Падаем на конфиг по умолчанию
    
    # Используем оригинальное подключение по конфигу
    return _get_db_connection_original(config, ssh_settings)


# Для обратной совместимости
PARAMIKO_AVAILABLE = is_paramiko_available()
_ssh_tunnels = {}
_ssh_tunnels_lock = threading.Lock()

app = Flask(__name__)
app.config.from_object(Config)
app.secret_key = app.config['SECRET_KEY']

# 🔐 Конфигурация сессии
app.config['SESSION_TYPE'] = 'filesystem'
app.config['SESSION_PERMANENT'] = True
app.config['PERMANENT_SESSION_LIFETIME'] = 86400 * 30  # 30 дней
app.config['SESSION_COOKIE_HTTPONLY'] = True  # Защита от XSS
app.config['SESSION_COOKIE_SECURE'] = False  # True если используется HTTPS
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'  # Защита от CSRF

AuthManager.init_db()

from flask_session import Session
session_obj = Session()
session_obj.init_app(app)

# Инициализация менеджера сессий
pg_session_manager = PostgreSQLSessionManager(app.config)

# Монитор сессий был удален, осталась только кнопка перехода в сетке

# ─────────────────────────────────────────────
# SSH TUNNEL (используется модуль db_connection)
# ─────────────────────────────────────────────


# ─────────────────────────────────────────────
# DB CONNECTION (используется модуль db_connection)
# ─────────────────────────────────────────────

def login_required(f):
    """Декоратор для проверки авторизации"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        auth_token = session.get('auth_token')
        
        if not auth_token:
            return redirect(url_for('login'))
        
        # Проверяем валидность токена
        result = AuthManager.verify_session(auth_token)
        
        if not result['success']:
            # Сессия истекла или неверна
            session.clear()
            return redirect(url_for('login'))
        
        # Добавляем user_id в контекст
        request.user_id = result['user_id']
        request.user = AuthManager.get_user_by_id(result['user_id'])
        
        return f(*args, **kwargs)
    
    return decorated_function


def redirect_if_logged_in(f):
    """Редирект если уже авторизован"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        auth_token = session.get('auth_token')
        
        if auth_token:
            # Проверяем валидность
            result = AuthManager.verify_session(auth_token)
            if result['success']:
                return redirect(url_for('dashboard'))
        
        return f(*args, **kwargs)
    
    return decorated_function


# Функция get_db_connection теперь импортируется из db_connection
# Для обратной совместимости создаем обертку
def get_db_connection_wrapper():
    """Обратная совместимость - использует app.config"""
    return get_db_connection(app.config)

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def escape_composite_value(val):
    if val is None:
        return ''
    s = str(val).replace('"', '""')
    s = s.replace('\n', ' ').replace('\r', ' ')
    return s


def make_composite_array(items, composite_type):
    if not items:
        return None
    return items


def safe_literal(s):
    return psycopg2.extensions.adapt(s).getquoted().decode('utf-8')


def safe_ident(s):
    if s is None:
        return ''
    escaped = str(s).replace('"', '""')
    return f'"{escaped}"'


def ensure_scheduled_table(conn):
    """Создать таблицу report_scheduled если не существует"""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS report_scheduled (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            template_id INTEGER,
            config JSONB NOT NULL DEFAULT '{}',
            schedule_type VARCHAR(50) NOT NULL DEFAULT 'manual',
            schedule_cron VARCHAR(100),
            schedule_time TIME,
            schedule_day INTEGER,
            export_format VARCHAR(20) DEFAULT 'xlsx',
            export_email VARCHAR(255),
            recipients VARCHAR(1000),
            export_path VARCHAR(500),
            is_active BOOLEAN DEFAULT TRUE,
            last_run_at TIMESTAMP,
            last_status VARCHAR(50),
            last_error TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_by VARCHAR(100) DEFAULT 'system'
        )
    """)
    conn.commit()
    cur.close()


# Регистрируем очистку при завершении приложения
import atexit
atexit.register(close_all_ssh_tunnels)





# Сессия для отслеживания активного профиля
active_profile_id = None

# Добавьте прямо ПЕРЕД @app.route('/') (перед функцией index()):

# Добавьте прямо ПЕРЕД @app.route('/') (перед функцией index()):

from db_profiles import DatabaseProfileManager
from flask import session

# ═════════════════════════════════════════════════════════════════════════════
# ROUTES - АВТОРИЗАЦИЯ
# ═════════════════════════════════════════════════════════════════════════════

@app.route('/login', methods=['GET', 'POST'])
@redirect_if_logged_in
def login():
    """Страница входа"""
    if request.method == 'POST':
        data = request.json or request.form
        username = data.get('username', '').strip()
        password = data.get('password', '')
        
        # Аутентификация
        result = AuthManager.authenticate_user(username, password)
        
        if not result['success']:
            if request.is_json:
                return jsonify({'success': False, 'error': result['message']}), 401
            else:
                return render_template('login.html', error=result['message']), 401
        
        # Создаём сессию
        user_id = result['user_id']
        session_result = AuthManager.create_session(
            user_id,
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        if session_result['success']:
            session['auth_token'] = session_result['token']
            session['user_id'] = user_id
            session['username'] = result['username']
            session.permanent = True
            
            if request.is_json:
                return jsonify({'success': True, 'redirect': url_for('dashboard')})
            else:
                return redirect(url_for('dashboard'))
        else:
            if request.is_json:
                return jsonify({'success': False, 'error': 'Ошибка создания сессии'}), 500
            else:
                return render_template('login.html', error='Ошибка создания сессии'), 500
    
    return render_template('login.html')


@app.route('/register', methods=['GET', 'POST'])
@redirect_if_logged_in
def register():
    """Страница регистрации"""
    if request.method == 'POST':
        data = request.json or request.form
        username = data.get('username', '').strip()
        email = data.get('email', '').strip()
        password = data.get('password', '')
        confirm_password = data.get('confirm_password', '')
        
        # Проверяем совпадение паролей
        if password != confirm_password:
            if request.is_json:
                return jsonify({'success': False, 'error': 'Пароли не совпадают'}), 400
            else:
                return render_template('register.html', error='Пароли не совпадают'), 400
        
        # Регистрируем пользователя
        result = AuthManager.register_user(username, email, password)
        
        if not result['success']:
            if request.is_json:
                return jsonify({'success': False, 'error': result['message']}), 400
            else:
                return render_template('register.html', error=result['message']), 400
        
        # После успешной регистрации - сразу входим
        auth_result = AuthManager.authenticate_user(username, password)
        
        if auth_result['success']:
            session_result = AuthManager.create_session(
                auth_result['user_id'],
                ip_address=request.remote_addr,
                user_agent=request.headers.get('User-Agent')
            )
            
            if session_result['success']:
                session['auth_token'] = session_result['token']
                session['user_id'] = auth_result['user_id']
                session['username'] = auth_result['username']
                session.permanent = True
                
                if request.is_json:
                    return jsonify({'success': True, 'redirect': url_for('dashboard')})
                else:
                    return redirect(url_for('dashboard'))
        
        if request.is_json:
            return jsonify({'success': True, 'message': 'Регистрация успешна. Пожалуйста, войдите.'})
        else:
            return redirect(url_for('login'))
    
    return render_template('register.html')


@app.route('/logout', methods=['POST', 'GET'])
@login_required
def logout():
    """Выход из системы"""
    auth_token = session.get('auth_token')
    
    if auth_token:
        AuthManager.delete_session(auth_token)
    
    session.clear()
    
    if request.method == 'POST' and request.is_json:
        return jsonify({'success': True})
    else:
        return redirect(url_for('login'))


@app.route('/api/auth/profile', methods=['GET'])
@login_required
def get_profile():
    """Получить профиль текущего пользователя"""
    return jsonify({
        'success': True,
        'user': {
            'id': request.user['id'],
            'username': request.user['username'],
            'email': request.user['email'],
            'created_at': request.user['created_at'],
            'last_login': request.user['last_login']
        }
    })


@app.route('/api/auth/change-password', methods=['POST'])
@login_required
def change_password_api():
    """Изменить пароль"""
    data = request.json or {}
    
    old_password = data.get('old_password', '')
    new_password = data.get('new_password', '')
    confirm_password = data.get('confirm_password', '')
    
    if new_password != confirm_password:
        return jsonify({'success': False, 'error': 'Новые пароли не совпадают'}), 400
    
    result = AuthManager.change_password(request.user_id, old_password, new_password)
    
    if result['success']:
        return jsonify({'success': True, 'message': result['message']})
    else:
        return jsonify({'success': False, 'error': result['message']}), 400


# ═════════════════════════════════════════════════════════════════════════════
# ROUTES - ОСНОВНЫЕ СТРАНИЦЫ
# ═════════════════════════════════════════════════════════════════════════════

@app.route('/')
@login_required
def index():
    """Главная страница - требует авторизацию"""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("SELECT * FROM report_get_tables('public') ORDER BY table_name")
        tables = cur.fetchall()
        cur.close()
        conn.close()
        return render_template('index.html', tables=tables, user=request.user)
    except Exception as e:
        flash(f'Ошибка при загрузке таблиц: {str(e)}', 'danger')
        return render_template('index.html', tables=[], user=request.user)


@app.route('/dashboard')
@login_required
def dashboard():
    """Dashboard - требует авторизацию"""
    return render_template('dashboard.html', user=request.user)


#@app.route('/report-builder')
#@login_required
#def report_builder():
#    """Report Builder - требует авторизацию"""
#    # Ваш код для report builder
#    return render_template('report_builder.html', user=request.user)


# ═════════════════════════════════════════════════════════════════════════════
# ERROR HANDLERS
# ═════════════════════════════════════════════════════════════════════════════

@app.errorhandler(404)
def not_found(error):
    """Обработка 404 ошибок"""
    return render_template('404.html'), 404


@app.errorhandler(500)
def server_error(error):
    """Обработка 500 ошибок"""
    return render_template('500.html'), 500


# ═════════════════════════════════════════════════════════════════════════════
# DATABASE PROFILES API
# ═════════════════════════════════════════════════════════════════════════════

@app.route('/api/db-profiles', methods=['GET'])
def get_db_profiles():
    """Получить все сохранённые подключения"""
    profiles = DatabaseProfileManager.get_all_profiles()
    # Не отправляем пароли на клиент
    for p in profiles:
        if 'password' in p:
            p['password'] = '****'
    return jsonify({'profiles': profiles})


@app.route('/api/db-profiles/<int:profile_id>', methods=['GET'])
def get_db_profile(profile_id):
    """Получить конкретное подключение"""
    profile = DatabaseProfileManager.get_profile(profile_id)
    if not profile:
        return jsonify({'error': 'Profile not found'}), 404
    return jsonify({'profile': profile})


@app.route('/api/db-profiles', methods=['POST'])
def save_db_profile():
    """Сохранить новое или обновить существующее подключение"""
    try:
        data = request.json
        print(f"[API] Saving profile: {data.get('name', 'unnamed')}")
        
        # Валидация обязательных полей
        required_fields = ['name', 'host', 'port', 'database', 'user', 'password']
        for field in required_fields:
            if field not in data or not data[field]:
                return jsonify({
                    'success': False, 
                    'error': f'Обязательное поле отсутствует: {field}'
                }), 400
        
        # Устанавливаем db_type по умолчанию если не указан
        if 'db_type' not in data:
            data['db_type'] = 'postgresql'
        
        profile = DatabaseProfileManager.save_profile(data)
        print(f"[API] Profile saved with ID: {profile['id']}")
        
        return jsonify({'success': True, 'profile': profile})
        
    except Exception as e:
        print(f"[API] Error saving profile: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/db-profiles/<int:profile_id>', methods=['DELETE'])
def delete_db_profile(profile_id):
    """Удалить подключение"""
    try:
        DatabaseProfileManager.delete_profile(profile_id)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/db-profiles/<int:profile_id>/schema', methods=['GET'])
def get_db_profile_schema(profile_id):
    """
    Получить структуру базы данных (дамп схемы без данных).
    Поддерживает PostgreSQL и Oracle.
    
    Query params:
        format: 'json' (по умолчанию) или 'sql'
    """
    try:
        # Получаем параметры
        format_type = request.args.get('format', 'json')
        
        # Подключаемся к БД
        conn = get_db_connection_by_profile(profile_id)
        if not conn:
            return jsonify({'success': False, 'error': 'Не удалось подключиться к БД'}), 400
        
        # Получаем тип БД
        from db_profiles import DatabaseProfileManager
        profile = DatabaseProfileManager.get_profile(profile_id)
        db_type = profile.get('db_type', 'postgresql')
        
        # Определяем тип для SchemaDumper
        if db_type == 'oracle':
            schema_db_type = DatabaseAdapter.ORACLE
        else:
            schema_db_type = DatabaseAdapter.POSTGRES
        
        # Получаем структуру
        schema = SchemaDumper.get_schema(conn, schema_db_type)
        
        # Закрываем подключение
        conn.close()
        
        if 'error' in schema:
            return jsonify({'success': False, 'error': schema['error']}), 500
        
        # Форматируем ответ - всегда возвращаем SQL
        import io
        sql = SchemaDumper.generate_sql(schema, schema_db_type)
        # Возвращаем как файл для скачивания
        filename = f"schema_{profile.get('name', 'db')}_{db_type}.sql"
        return Response(
            io.BytesIO(sql.encode('utf-8')),
            mimetype='text/plain; charset=utf-8',
            headers={'Content-Disposition': f'attachment; filename={filename}'}
        )
            
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/db-profiles/test', methods=['POST'])
def test_db_profile_connection():
    """Тестирование подключения к БД (PostgreSQL или Oracle) - без SSH"""
    try:
        data = request.json
        
        db_type = data.get('db_type', 'postgresql')
        host = data['host']
        port = data['port']
        database = data['database']
        user = data['user']
        password = data['password']
        
        print(f"[TEST] Testing {db_type} connection to {host}:{port}/{database}")
        
        success, message, version = DatabaseAdapter.test_connection(
            db_type, host, port, database, user, password
        )
        
        if success:
            return jsonify({
                'success': True,
                'message': message,
                'version': version
            })
        else:
            return jsonify({
                'success': False,
                'error': message
            }), 400
            
    except Exception as e:
        print(f"[TEST] Error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': f'Ошибка тестирования: {str(e)}'
        }), 500


@app.route('/api/db-profiles/test-ssh', methods=['POST'])
def test_db_profile_connection_ssh():
    """Тестирование подключения через SSH туннель"""
    try:
        data = request.json
        
        if not data.get('ssh_enabled'):
            return jsonify({'success': False, 'error': 'SSH не включён'}), 400
        
        db_type = data.get('db_type', 'postgresql')
        
        print(f"[TEST-SSH] Creating SSH tunnel for {db_type} connection")
        print(f"[TEST-SSH] SSH: {data['ssh_user']}@{data['ssh_host']}:{data.get('ssh_port', 22)}")
        print(f"[TEST-SSH] Remote DB: {data.get('remote_db_host', 'localhost')}:{data.get('remote_db_port', 5432)}")
        
        # Создаём SSH туннель
        local_host, local_port = create_ssh_tunnel(
            ssh_host=data['ssh_host'],
            ssh_port=data.get('ssh_port', 22),
            ssh_user=data['ssh_user'],
            ssh_password=data.get('ssh_password'),
            ssh_key_path=data.get('ssh_key_path'),
            remote_db_host=data.get('remote_db_host', 'localhost'),
            remote_db_port=data.get('remote_db_port', 1521 if db_type == 'oracle' else 5432)
        )
        
        print(f"[TEST-SSH] Tunnel created: {local_host}:{local_port}")
        
        # Тестируем подключение через туннель
        success, message, version = DatabaseAdapter.test_connection(
            db_type, local_host, local_port, 
            data['database'], data['user'], data['password']
        )
        
        if success:
            return jsonify({
                'success': True,
                'message': f'✓ SSH туннель и подключение к {db_type} работают',
                'version': version
            })
        else:
            return jsonify({
                'success': False,
                'error': f'SSH туннель создан, но подключение к БД не удалось: {message}'
            }), 400
            
    except Exception as e:
        print(f"[TEST-SSH] Error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': f'Ошибка SSH туннеля или подключения: {str(e)}'
        }), 500


@app.route('/api/db-profiles/select', methods=['POST'])
def select_db_profile():
    """Выбрать активное подключение"""
    data = request.json
    profile_id = data.get('profile_id')
    
    profile = DatabaseProfileManager.get_profile(profile_id)
    if not profile:
        return jsonify({'error': 'Profile not found'}), 404
    
    session['active_profile_id'] = profile_id
    session.permanent = True
    
    return jsonify({'success': True})

# ────────────────────────────────────────────────
# ТЕПЕРЬ идёт существующий @app.route('/') который вы заменяете на новый выше
# Добавить перед другими маршрутами

# Монитор сессий был удален, осталась только кнопка перехода в сетке

# ─────────────────────────────────────────────
# MAIN ROUTES
# ─────────────────────────────────────────────

#@app.route('/')
#def index():
#    try:
#        conn = get_db_connection()
#        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
#        cur.execute("SELECT * FROM report_get_tables('public') ORDER BY table_name")
#        tables = cur.fetchall()
#        cur.close()
#        conn.close()
#        return render_template('index.html', tables=tables)
#    except Exception as e:
#        flash(f'Ошибка при загрузке таблиц: {str(e)}', 'danger')
#        return render_template('index.html', tables=[])


@app.route('/api/get-tables', methods=['GET'])
@app.route('/api/tables')
@login_required
def get_tables():
    try:
        schema = request.args.get('schema')
        all_schemas = request.args.get('all') in ('1', 'true', 'yes')

        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Не завязываемся на кастомную функцию report_get_tables(),
        # чтобы дерево объектов работало и на новых БД/через SSH-профили.
        base_sql = """
            SELECT
                n.nspname AS table_schema,
                c.relname AS table_name,
                COALESCE(obj_description(c.oid, 'pg_class'), '') AS table_comment
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind IN ('r','p')  -- table, partitioned table
              AND n.nspname NOT IN ('pg_catalog','information_schema')
              AND n.nspname NOT LIKE 'pg_toast%%'
              AND n.nspname NOT LIKE 'pg_temp%%'
        """
        params = []
        if not all_schemas:
            schema = schema or 'public'
            base_sql += " AND n.nspname = %s"
            params.append(schema)
        base_sql += " ORDER BY n.nspname, c.relname"

        cur.execute(base_sql, params)
        tables = cur.fetchall()
        result = [{
            'schema': row['table_schema'],
            'table_name': row['table_name'],
            'table_comment': row['table_comment']
        } for row in tables]

        cur.close()
        conn.close()
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/sql/schemas', methods=['GET'])
@login_required
def sql_schemas():
    """Список пользовательских схем для дерева объектов."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT nspname
            FROM pg_namespace
            WHERE nspname NOT IN ('pg_catalog','information_schema')
              AND nspname NOT LIKE 'pg_toast%%'
              AND nspname NOT LIKE 'pg_temp%%'
            ORDER BY nspname
        """)
        items = [r[0] for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify({'success': True, 'data': items})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 200


@app.route('/api/table/<table_name>/columns')
def get_table_columns(table_name):
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("SELECT * FROM report_get_columns(ARRAY[%s], 'public') ORDER BY column_name", (table_name,))
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
    try:
        tables = request.json.get('tables', [])
        if not tables or not isinstance(tables, list):
            return jsonify({'success': False, 'error': 'Не переданы имена таблиц'}), 400

        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        result = defaultdict(list)
        
        # Правильно: вызываем функцию для каждой таблицы отдельно
        for table_name in tables:
            try:
                cur.execute(
                    "SELECT * FROM report_get_columns(ARRAY[%s], 'public') ORDER BY column_name", 
                    (table_name,)
                )
                columns = cur.fetchall()
                
                for row in columns:
                    result[table_name].append({
                        'column_name': row['column_name'],
                        'data_type': row['data_type'],
                        'is_nullable': row['is_nullable'],
                        'column_default': row['column_default'],
                        'column_comment': row['column_comment']
                    })
            except Exception as e:
                print(f"Error getting columns for table {table_name}: {e}")
                result[table_name] = []
        
        cur.close()
        conn.close()
        return jsonify({'success': True, 'data': dict(result)})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/table/<table_name>/possible-joins')
def get_possible_joins(table_name):
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("""
            SELECT * FROM report_get_possible_joins(%s, 'public', %s, true)
            ORDER BY match_confidence DESC, target_table
        """, (table_name, [table_name]))
        joins = cur.fetchall()

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
    try:
        data = request.json
        main_table = data.get('main_table')
        columns = data.get('columns', [])
        joins = data.get('joins', [])
        conditions = data.get('conditions', [])
        aggregates = data.get('aggregates', [])
        group_by = data.get('group_by', [])
        sort = data.get('sort', [])
        limit = data.get('limit', 100)
        offset = data.get('offset', 0)

        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        joins_param = None
        if joins:
            join_tuples = [(j.get('source_table'), j['table_name'], j.get('alias', j['table_name']),
                           j['join_type'], j['left_column'], j['right_column'], j.get('confidence', 0.0))
                          for j in joins]
            joins_param = make_composite_array(join_tuples, 'report_join')

        conditions_param = None
        if conditions:
            cond_tuples = [(c['column_name'], c['operator'], c['value'], c.get('logic_operator', 'AND'))
                          for c in conditions]
            conditions_param = make_composite_array(cond_tuples, 'report_condition')

        aggregates_param = None
        if aggregates:
            agg_tuples = [(a['function_name'], a['column_name'], a.get('alias', '')) for a in aggregates]
            aggregates_param = make_composite_array(agg_tuples, 'report_aggregate')

        sort_param = None
        if sort:
            sort_tuples = [(s.get('column_name', '').strip(), s.get('direction', 'ASC').upper().strip())
                          for s in sort if s.get('column_name', '').strip()]
            if sort_tuples:
                sort_param = make_composite_array(sort_tuples, 'report_sort')

        result_cols = []
        for col in columns:
            clean_col = re.sub(r'[^\w]', '_', col)
            result_cols.append(f'"{clean_col}" TEXT')
        for agg in aggregates:
            alias = agg.get('alias') or f"{agg['function_name'].upper()}_{agg['column_name']}"
            clean_alias = re.sub(r'[^\w]', '_', alias)
            result_cols.append(f'"{clean_alias}" TEXT')
        result_cols_str = ', '.join(result_cols) if result_cols else '"data" TEXT'

        sql_template = """
            SELECT * FROM report_generate(
                %s, 'public',
                %s::report_join[],
                %s,
                %s::report_condition[],
                %s::report_aggregate[],
                %s,
                %s::report_sort[],
                %s, %s
            ) AS result({result_cols})
        """.format(result_cols=result_cols_str)

        params = [main_table, joins_param, columns if columns else None,
                  conditions_param, aggregates_param, group_by if group_by else None,
                  sort_param, limit, offset]

        cur.execute(sql_template, params)
        rows = cur.fetchall()
        column_names = [desc[0] for desc in cur.description]
        result_data = {'columns': column_names, 'rows': [dict(row) for row in rows], 'total': len(rows)}

        cur.close()
        conn.close()
        return jsonify({'success': True, 'data': result_data})
    except Exception as e:
        import traceback
        return jsonify({'success': False, 'error': str(e), 'details': traceback.format_exc()}), 500


@app.route('/report-builder')
@login_required
def report_builder():
    template_id = request.args.get('template_id')
    table = request.args.get('table')

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
                    return render_template('report.html', selected_table=table)
        except Exception as e:
            print(f"Error loading template config: {e}")

    if not table:
        return redirect(url_for('index'))

    return render_template('report.html', selected_table=table)


@app.route('/api/save-report', methods=['POST'])
def save_report():
    try:
        data = request.json
        name = data.get('name')
        config = data.get('config')

        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO report_templates (name, config, created_by)
            VALUES (%s, %s, %s) RETURNING id
        """, (name, json.dumps(config), 'current_user'))
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
        cur.execute("SELECT id, name, created_at, created_by FROM report_templates ORDER BY created_at DESC")
        templates = cur.fetchall()
        result = [{'id': t['id'], 'name': t['name'],
                   'created_at': t['created_at'].isoformat() if t['created_at'] else None,
                   'created_by': t['created_by']} for t in templates]
        cur.close()
        conn.close()
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/templates/<int:template_id>', methods=['PUT'])
def update_template(template_id):
    try:
        data = request.json
        name = data.get('name')
        config = data.get('config')
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("UPDATE report_templates SET name=%s, config=%s WHERE id=%s",
                    (name, json.dumps(config), template_id))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/load-report/<int:report_id>', methods=['GET'])
def load_report(report_id):
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("SELECT * FROM report_templates WHERE id = %s", (report_id,))
        report = cur.fetchone()
        if not report:
            return jsonify({'success': False, 'error': 'Отчет не найден'}), 404
        config_data = report['config']
        if isinstance(config_data, str):
            config_data = json.loads(config_data)
        result = {'id': report['id'], 'name': report['name'], 'config': config_data,
                  'created_at': report['created_at'].isoformat() if report['created_at'] else None}
        cur.close()
        conn.close()
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ─────────────────────────────────────────────
# SESSION MANAGEMENT
# ─────────────────────────────────────────────


@app.route('/api/sessions', methods=['GET'])
def api_get_sessions():
    """API: Получить список активных сессий"""
    try:
        exclude_current = request.args.get('exclude_current', 'true').lower() == 'true'
        sessions = pg_session_manager.get_active_sessions(exclude_current=exclude_current)
        return jsonify({
            'success': True,
            'data': sessions,
            'count': len(sessions)
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/sessions/stats', methods=['GET'])
def api_get_sessions_stats():
    """API: Получить статистику сессий"""
    try:
        stats = pg_session_manager.get_sessions_stats()
        return jsonify({
            'success': True,
            'data': stats
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/sessions/<int:pid>/terminate', methods=['POST'])
def api_terminate_session(pid):
    """API: Завершить сессию по PID"""
    try:
        force = False
        if request.json:
            force = request.json.get('force', False)
        
        result = pg_session_manager.terminate_session(pid, force=force)
        return jsonify({
            'success': result.get('success', False),
            'data': result
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/sessions/idle', methods=['GET'])
def api_get_idle_sessions():
    """API: Получить неактивные сессии"""
    try:
        timeout = request.args.get('timeout', 300, type=int)
        sessions = pg_session_manager.get_idle_sessions(timeout_seconds=timeout)
        return jsonify({
            'success': True,
            'data': sessions,
            'count': len(sessions)
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/sessions/long-queries', methods=['GET'])
def api_get_long_queries():
    """API: Получить долго выполняющиеся запросы"""
    try:
        timeout = request.args.get('timeout', 3600, type=int)
        sessions = pg_session_manager.get_long_running_queries(timeout_seconds=timeout)
        return jsonify({
            'success': True,
            'data': sessions,
            'count': len(sessions)
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


# ─────────────────────────────────────────────
# SCHEDULED REPORTS — ИСПРАВЛЕННЫЕ ЭНДПОИНТЫ
# ─────────────────────────────────────────────

@app.route('/scheduled')
def scheduled_reports():
    return render_template('scheduled.html')


@app.route('/api/scheduled', methods=['GET'])
def get_scheduled_reports():
    try:
        conn = get_db_connection()
        ensure_scheduled_table(conn)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("""
            SELECT sr.*, rt.name as template_name
            FROM report_scheduled sr
            LEFT JOIN report_templates rt ON sr.template_id = rt.id
            ORDER BY sr.id DESC
        """)
        reports = cur.fetchall()
        result = []
        for r in reports:
            result.append({
                'id': r['id'],
                'name': r['name'],
                'template_id': r['template_id'],
                'template_name': r['template_name'],
                'schedule_type': r['schedule_type'],
                'schedule_time': str(r['schedule_time']) if r['schedule_time'] else None,
                'schedule_day': r['schedule_day'],
                'export_format': r['export_format'],
                'recipients': r['recipients'],
                'last_status': r['last_status'],
                'last_run_at': r['last_run_at'].isoformat() if r['last_run_at'] else None,
                'created_at': r['created_at'].isoformat() if r['created_at'] else None
            })
        cur.close()
        conn.close()
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        import traceback
        return jsonify({'success': False, 'error': str(e), 'details': traceback.format_exc()}), 500


@app.route('/api/scheduled', methods=['POST'])
def create_scheduled_report():
    try:
        data = request.json
        if not data:
            return jsonify({'success': False, 'error': 'Нет данных'}), 400
        if not data.get('name'):
            return jsonify({'success': False, 'error': 'Название обязательно'}), 400
        if not data.get('template_id'):
            return jsonify({'success': False, 'error': 'Шаблон обязателен'}), 400

        conn = get_db_connection()
        ensure_scheduled_table(conn)
        cur = conn.cursor()

        schedule_time = data.get('schedule_time') or None
        schedule_day = data.get('schedule_day') or None

        cur.execute("""
            INSERT INTO report_scheduled
                (name, template_id, config, schedule_type, schedule_time, schedule_day,
                 export_format, export_email, recipients, is_active, created_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE, 'user')
            RETURNING id
        """, (
            data['name'],
            int(data['template_id']),
            json.dumps(data.get('config', {})),
            data.get('schedule_type', 'manual'),
            schedule_time,
            int(schedule_day) if schedule_day else None,
            data.get('export_format', 'xlsx'),
            data.get('recipients', ''),
            data.get('recipients', '')
        ))
        report_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'success': True, 'id': report_id})
    except Exception as e:
        import traceback
        return jsonify({'success': False, 'error': str(e), 'details': traceback.format_exc()}), 500


@app.route('/api/scheduled/<int:report_id>', methods=['GET'])
def get_scheduled_report(report_id):
    try:
        conn = get_db_connection()
        ensure_scheduled_table(conn)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("SELECT * FROM report_scheduled WHERE id = %s", (report_id,))
        report = cur.fetchone()
        if not report:
            return jsonify({'success': False, 'error': 'Отчёт не найден'}), 404
        result = {
            'id': report['id'], 'name': report['name'],
            'template_id': report['template_id'],
            'config': report['config'],
            'schedule_type': report['schedule_type'],
            'schedule_time': str(report['schedule_time']) if report['schedule_time'] else None,
            'schedule_day': report['schedule_day'],
            'export_format': report['export_format'],
            'recipients': report['recipients'],
            'last_status': report['last_status'],
            'last_run_at': report['last_run_at'].isoformat() if report['last_run_at'] else None
        }
        cur.close()
        conn.close()
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/scheduled/<int:report_id>', methods=['PUT'])
def update_scheduled_report(report_id):
    try:
        data = request.json
        if not data:
            return jsonify({'success': False, 'error': 'Нет данных'}), 400

        conn = get_db_connection()
        ensure_scheduled_table(conn)
        cur = conn.cursor()

        schedule_time = data.get('schedule_time') or None
        schedule_day = data.get('schedule_day') or None

        cur.execute("""
            UPDATE report_scheduled SET
                name=%s, template_id=%s, config=%s, schedule_type=%s,
                schedule_time=%s, schedule_day=%s, export_format=%s,
                export_email=%s, recipients=%s, updated_at=NOW()
            WHERE id=%s
        """, (
            data['name'],
            int(data['template_id']),
            json.dumps(data.get('config', {})),
            data.get('schedule_type', 'manual'),
            schedule_time,
            int(schedule_day) if schedule_day else None,
            data.get('export_format', 'xlsx'),
            data.get('recipients', ''),
            data.get('recipients', ''),
            report_id
        ))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        import traceback
        return jsonify({'success': False, 'error': str(e), 'details': traceback.format_exc()}), 500


@app.route('/api/scheduled/<int:report_id>', methods=['DELETE'])
def delete_scheduled_report(report_id):
    try:
        conn = get_db_connection()
        ensure_scheduled_table(conn)
        cur = conn.cursor()
        cur.execute("DELETE FROM report_scheduled WHERE id = %s", (report_id,))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/scheduled/<int:report_id>/run', methods=['POST'])
def run_scheduled_report(report_id):
    try:
        conn = get_db_connection()
        ensure_scheduled_table(conn)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("SELECT * FROM report_scheduled WHERE id = %s", (report_id,))
        report = cur.fetchone()
        if not report:
            return jsonify({'success': False, 'error': 'Отчёт не найден'}), 404

        config = report['config']
        if isinstance(config, str):
            config = json.loads(config)

        main_table = config.get('main_table')
        columns = config.get('columns', [])
        joins = config.get('joins', [])
        conditions = config.get('conditions', [])
        aggregates = config.get('aggregates', [])
        group_by = config.get('group_by', [])
        sort = config.get('sort', [])
        limit = 1000

        joins_param = None
        if joins:
            join_tuples = [(j.get('source_table'), j['table_name'], j.get('alias', j['table_name']),
                           j['join_type'], j['left_column'], j['right_column'], j.get('confidence', 0.0))
                          for j in joins]
            joins_param = make_composite_array(join_tuples, 'report_join')

        conditions_param = None
        if conditions:
            cond_tuples = [(c['column_name'], c['operator'], c['value'], c.get('logic_operator', 'AND'))
                          for c in conditions]
            conditions_param = make_composite_array(cond_tuples, 'report_condition')

        aggregates_param = None
        if aggregates:
            agg_tuples = [(a['function_name'], a['column_name'], a.get('alias', '')) for a in aggregates]
            aggregates_param = make_composite_array(agg_tuples, 'report_aggregate')

        sort_param = None
        if sort:
            sort_tuples = [(s.get('column_name', '').strip(), s.get('direction', 'ASC').upper())
                          for s in sort if s.get('column_name', '').strip()]
            if sort_tuples:
                sort_param = make_composite_array(sort_tuples, 'report_sort')

        result_cols = []
        for col in columns:
            clean_col = re.sub(r'[^\w]', '_', col)
            result_cols.append(f'"{clean_col}" TEXT')
        for agg in aggregates:
            alias = agg.get('alias') or f"{agg['function_name'].upper()}_{agg['column_name']}"
            clean_alias = re.sub(r'[^\w]', '_', alias)
            result_cols.append(f'"{clean_alias}" TEXT')
        result_cols_str = ', '.join(result_cols) if result_cols else '"data" TEXT'

        sql_template = """
            SELECT * FROM report_generate(
                %s, 'public',
                %s::report_join[],
                %s,
                %s::report_condition[],
                %s::report_aggregate[],
                %s,
                %s::report_sort[],
                %s, 0
            ) AS result({result_cols})
        """.format(result_cols=result_cols_str)

        params = [main_table, joins_param, columns if columns else None,
                  conditions_param, aggregates_param, group_by if group_by else None,
                  sort_param, limit]

        cur.execute(sql_template, params)
        rows = cur.fetchall()
        column_names = [desc[0] for desc in cur.description]

        cur.execute("UPDATE report_scheduled SET last_status='success', last_run_at=NOW() WHERE id=%s", (report_id,))
        conn.commit()

        export_format = report['export_format'] or 'xlsx'
        recipients = report['recipients'] or report['export_email'] or ''
        sent_count = 0

        if recipients:
            data_rows = [dict(row) for row in rows]
            filename = f"report_{report_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            file_content = None

            if export_format == 'xlsx':
                try:
                    import xlsxwriter
                    output = io.BytesIO()
                    workbook = xlsxwriter.Workbook(output, {'in_memory': True})
                    worksheet = workbook.add_worksheet()
                    for col, name in enumerate(column_names):
                        worksheet.write(0, col, name)
                    for row_idx, row in enumerate(data_rows):
                        for col_idx, col_name in enumerate(column_names):
                            worksheet.write(row_idx + 1, col_idx, str(row.get(col_name, '')))
                    workbook.close()
                    file_content = output.getvalue()
                    filename += '.xlsx'
                    mime_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                except ImportError:
                    export_format = 'csv'

            if export_format == 'csv':
                output = io.StringIO()
                writer = csv.DictWriter(output, fieldnames=column_names)
                writer.writeheader()
                writer.writerows(data_rows)
                file_content = output.getvalue().encode('utf-8')
                filename += '.csv'
                mime_type = 'text/csv'

            elif export_format == 'json':
                file_content = json.dumps({'columns': column_names, 'data': data_rows},
                                         ensure_ascii=False, indent=2).encode('utf-8')
                filename += '.json'
                mime_type = 'application/json'

            mail_settings = get_mail_settings()
            if mail_settings and mail_settings.get('smtp_host') and file_content:
                sent_count = send_email(
                    mail_settings, recipients.split(','),
                    f"Отчёт: {report['name']}",
                    f"Автоматически сгенерированный отчёт.\nНазвание: {report['name']}\nДата: {datetime.now().strftime('%d.%m.%Y %H:%M')}\nСтрок: {len(rows)}",
                    file_content, filename, mime_type
                )

        cur.close()
        conn.close()
        return jsonify({'success': True, 'data': {'total': len(rows), 'sent_count': sent_count}})

    except Exception as e:
        import traceback
        try:
            conn2 = get_db_connection()
            cur2 = conn2.cursor()
            cur2.execute("UPDATE report_scheduled SET last_status='failed', last_error=%s WHERE id=%s",
                        (str(e), report_id))
            conn2.commit()
            cur2.close()
            conn2.close()
        except Exception:
            pass
        return jsonify({'success': False, 'error': str(e), 'details': traceback.format_exc()}), 500


# ─────────────────────────────────────────────
# MONITORING
# ─────────────────────────────────────────────

@app.route('/monitoring')
def monitoring():
    """Страница мониторинга - адапт��вная под тип БД"""
    try:
        # Определяем активный профиль
        profile_id = session.get('active_profile_id')
        db_type = 'postgresql'  # По умолчанию
        
        if profile_id:
            from db_profiles import DatabaseProfileManager
            profile = DatabaseProfileManager.get_profile(profile_id)
            if profile:
                db_type = profile.get('db_type', 'postgresql')
        
        # Возвращаем соответствующий шаблон
        if db_type == 'oracle':
            return render_template('monitoring_oracle.html')
        else:
            return render_template('monitoring.html')
            
    except Exception as e:
        print(f"[Monitoring] Error determining DB type: {e}")
        # Fallback на PostgreSQL
        return render_template('monitoring.html')


def _get_system_stats_via_db(cur):
    """
    Получить системную статистику через pg_stat_* без psutil.
    Работает если есть доступ к pg_stat_bgwriter, pg_stat_activity и т.д.
    """
    sys_stats = {}
    try:
        # Попытка получить системные данные через /proc (Linux), если Flask запущен локально
        import os, platform
        sys_stats['os'] = platform.system() + ' ' + platform.release()

        # /proc/loadavg — средняя нагрузка (только Linux)
        if os.path.exists('/proc/loadavg'):
            with open('/proc/loadavg') as f:
                la = f.read().split()
            sys_stats['load_avg_1m']  = float(la[0])
            sys_stats['load_avg_5m']  = float(la[1])
            sys_stats['load_avg_15m'] = float(la[2])

        # /proc/meminfo
        if os.path.exists('/proc/meminfo'):
            mem = {}
            with open('/proc/meminfo') as f:
                for line in f:
                    parts = line.split()
                    if len(parts) >= 2:
                        mem[parts[0].rstrip(':')] = int(parts[1])  # kB
            total_kb  = mem.get('MemTotal', 0)
            avail_kb  = mem.get('MemAvailable', 0)
            used_kb   = total_kb - avail_kb
            sys_stats['memory_total_gb']   = round(total_kb / (1024**2), 2)
            sys_stats['memory_used_gb']    = round(used_kb  / (1024**2), 2)
            sys_stats['memory_avail_gb']   = round(avail_kb / (1024**2), 2)
            sys_stats['memory_percent']    = round(used_kb / total_kb * 100, 1) if total_kb else 0

        # /proc/stat — CPU (разница между двумя снимками невозможна без sleep, берём cumulative)
        if os.path.exists('/proc/stat'):
            with open('/proc/stat') as f:
                cpu_line = f.readline().split()
            # user, nice, system, idle, iowait, irq, softirq
            vals = [int(x) for x in cpu_line[1:8]]
            idle  = vals[3] + (vals[4] if len(vals) > 4 else 0)
            total = sum(vals)
            busy  = total - idle
            sys_stats['cpu_percent'] = round(busy / total * 100, 1) if total else 0

        # /proc/diskstats → суммируем только sda/vda/nvme
        # Проще через statvfs для корневого раздела
        st = os.statvfs('/')
        total_b = st.f_blocks * st.f_frsize
        free_b  = st.f_bfree  * st.f_frsize
        used_b  = total_b - free_b
        sys_stats['disk_total_gb'] = round(total_b / (1024**3), 2)
        sys_stats['disk_used_gb']  = round(used_b  / (1024**3), 2)
        sys_stats['disk_free_gb']  = round(free_b  / (1024**3), 2)
        sys_stats['disk_percent']  = round(used_b / total_b * 100, 1) if total_b else 0

        # /proc/net/dev — сеть
        if os.path.exists('/proc/net/dev'):
            net_rx = net_tx = 0
            with open('/proc/net/dev') as f:
                for line in f:
                    if ':' in line:
                        iface, data = line.split(':', 1)
                        iface = iface.strip()
                        if iface == 'lo':
                            continue
                        cols = data.split()
                        if len(cols) >= 9:
                            net_rx += int(cols[0])   # bytes received
                            net_tx += int(cols[8])   # bytes sent
            sys_stats['net_recv_mb'] = round(net_rx / (1024**2), 2)
            sys_stats['net_sent_mb'] = round(net_tx / (1024**2), 2)

    except Exception as e:
        sys_stats['proc_error'] = str(e)

    return sys_stats


@app.route('/api/monitoring/stats', methods=['GET'])
def get_monitoring_stats():
    """
    API endpoint для получения статистики мониторинга.
    ✅ Читает из SQLite (собрано фоновым скриптом)
    ⚡ Быстрый ответ, не блокирует UI
    """
    try:
        # Определяем активный профиль
        profile_id = session.get('active_profile_id')
        
        if not profile_id:
            return jsonify({
                'success': False,
                'error': 'No active database profile'
            }), 400
        
        # Получаем статистику из SQLite
        stats = MonitoringDB.get_stats(profile_id)
        
        if not stats:
            return jsonify({
                'success': False,
                'error': 'Monitoring data not available. Please wait for collector to gather metrics.',
                'data': {}
            }), 202  # 202 = Accepted (данные будут позже)
        
        return jsonify({
            'success': True,
            'data': stats,
            'timestamp': datetime.now().isoformat()
        })
    
    except Exception as e:
        print(f"[Monitoring API] Error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


# Добавить после /api/monitoring/stats:

@app.route('/api/monitoring/status', methods=['GET'])
def get_monitoring_status():
    """
    Получить статус мониторинга для всех профилей.
    Показывает когда последний раз была сборка, ошибки и т.д.
    """
    try:
        profiles = MonitoringDB.get_all_profiles()
        
        status = {
            'profiles': [],
            'total': len(profiles),
            'active': sum(1 for p in profiles if p['enabled'])
        }
        
        for profile in profiles:
            status['profiles'].append({
                'profile_id': profile['profile_id'],
                'profile_name': profile['profile_name'],
                'db_type': profile['db_type'],
                'enabled': profile['enabled'],
                'last_collection': profile['last_collection'],
                'last_error': profile['last_error'],
                'status': 'healthy' if not profile['last_error'] else 'error'
            })
        
        return jsonify({
            'success': True,
            'data': status
        })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/monitoring/refresh', methods=['POST'])
def trigger_monitoring_refresh():
    """
    Принудительно запустить сборку метрик (опционально).
    Вызывает collector.py --once в фоне.
    """
    try:
        import subprocess
        import threading
        
        def run_collector():
            subprocess.run(
                [sys.executable, 'monitoring_collector.py', '--once'],
                cwd=os.path.dirname(__file__)
            )
        
        # Запускаем в фоне
        thread = threading.Thread(target=run_collector, daemon=True)
        thread.start()
        
        return jsonify({
            'success': True,
            'message': 'Monitoring refresh triggered'
        })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# ─────────────────────────────────────────────
# SETTINGS
# ─────────────────────────────────────────────

@app.route('/settings')
def settings():
    return render_template('settings.html', config=app.config)


def get_mail_settings():
    try:
        conn = get_db_connection()
        ensure_app_settings_table(conn)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("SELECT * FROM app_settings WHERE setting_key = 'mail_config'")
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row and row['setting_value']:
            try:
                return json.loads(row['setting_value'])
            except json.JSONDecodeError as e:
                print(f"Error parsing mail settings JSON: {e}")
                return None
        return None
    except Exception as e:
        print(f"Error getting mail settings: {e}")
        return None


def save_mail_settings(settings):
    try:
        conn = get_db_connection()
        ensure_app_settings_table(conn)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO app_settings (setting_key, setting_value, updated_at)
            VALUES ('mail_config', %s, NOW())
            ON CONFLICT (setting_key) DO UPDATE SET setting_value = EXCLUDED.setting_value, updated_at = NOW()
        """, (json.dumps(settings),))
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Error saving mail settings: {e}")
        return False


def send_email(mail_settings, recipients, subject, body, attachment=None, filename=None, mime_type=None, is_html=False):
    """
    Отправка email с поддержкой простого текста, HTML и вложений.
    
    Args:
        mail_settings: dict с ключами: smtp_host, smtp_port, smtp_user, smtp_password, from_name
        recipients: list адресов получателей
        subject: тема письма
        body: текст письма
        attachment: путь к файлу в��ожения (опционально)
        filename: имя файла для вложения (опционально)
        mime_type: MIME тип вложения (опционально)
        is_html: bool - если True, отправляет как HTML (по умолчанию plain text)
    
    Returns:
        int: количество успешно отправленных писем, 0 если ошибка
    """
    try:
        # ✅ Валидация параметров
        if not mail_settings:
            raise ValueError("Mail settings not configured")
        
        required_fields = ['smtp_host', 'smtp_port', 'smtp_user', 'smtp_password']
        for field in required_fields:
            if field not in mail_settings:
                raise ValueError(f"Missing required field: {field}")
        
        if not recipients or not isinstance(recipients, list):
            raise ValueError("Recipients must be a non-empty list")
        
        # ✅ Создаем сообщение
        msg = MIMEMultipart('mixed')
        msg['Subject'] = subject
        msg['From'] = f"{mail_settings.get('from_name', 'Report Builder')} <{mail_settings['smtp_user']}>"
        msg['To'] = ', '.join(r.strip() for r in recipients)
        
        # ✅ Добавляем тело письма (HTML или plain text)
        if is_html:
            body_part = MIMEText(body, 'html', _charset='utf-8')
        else:
            body_part = MIMEText(body, 'plain', _charset='utf-8')
        msg.attach(body_part)
        
        # ✅ Добавляем вложение если есть
        if attachment:
            if isinstance(attachment, str):
                # Если attachment - путь к файлу
                if os.path.isfile(attachment):
                    with open(attachment, 'rb') as f:
                        part = MIMEApplication(
                            f.read(),
                            Name=filename or basename(attachment)
                        )
                    part['Content-Disposition'] = f'attachment; filename="{filename or basename(attachment)}"'
                    msg.attach(part)
            elif isinstance(attachment, bytes):
                # Если attachment - бинарные данные
                part = MIMEApplication(
                    attachment,
                    Name=filename or 'attachment'
                )
                part['Content-Disposition'] = f'attachment; filename="{filename or "attachment"}"'
                msg.attach(part)
        
        # ✅ Отправка письма
        print(f"[SMTP] Connecting to {mail_settings['smtp_host']}:{mail_settings['smtp_port']}")
        
        server = smtplib.SMTP(mail_settings['smtp_host'], int(mail_settings['smtp_port']), timeout=10)
        server.set_debuglevel(1)  # Для отладки
        
        # EHLO
        server.ehlo()
        print("[SMTP] EHLO sent")
        
        # STARTTLS если требуется
        if mail_settings.get('smtp_tls', True):
            print("[SMTP] Starting TLS...")
            server.starttls()
            server.ehlo()  # Повторяем EHLO после STARTTLS
        
        # ✅ АУТЕНТИФИКАЦИЯ
        print(f"[SMTP] Authenticating as {mail_settings['smtp_user']}")
        username = mail_settings['smtp_user']
        password = mail_settings['smtp_password']
        
        # Пробуем стандартный login() - работает для большинства серверов
        try:
            server.login(username, password)
            print("[SMTP] Authentication successful (standard method)")
        except smtplib.SMTPAuthenticationError:
            # Если стандартный не сработал, пробуем AUTH LOGIN вручную
            print("[SMTP] Standard AUTH failed, trying AUTH LOGIN...")
            try:
                import base64
                
                # Отправляем AUTH LOGIN команду
                code, response = server.docmd("AUTH LOGIN")
                if code != 334:
                    raise smtplib.SMTPAuthenticationError(code, response)
                
                # Отправляем base64-кодированный username
                username_b64 = base64.b64encode(username.encode()).decode()
                code, response = server.docmd(username_b64)
                if code != 334:
                    raise smtplib.SMTPAuthenticationError(code, response)
                
                # Отправляем base64-кодированный password
                password_b64 = base64.b64encode(password.encode()).decode()
                code, response = server.docmd(password_b64)
                if code not in (235, 250):
                    raise smtplib.SMTPAuthenticationError(code, response)
                
                print("[SMTP] Authentication successful (AUTH LOGIN method)")
            except Exception as e:
                server.quit()
                raise smtplib.SMTPAuthenticationError(str(e))
        
        # ✅ Отправляем письмо
        print(f"[SMTP] Sending email to {len(recipients)} recipient(s)")
        server.sendmail(mail_settings['smtp_user'], [r.strip() for r in recipients], msg.as_string())
        server.quit()
        
        print(f"[SMTP] Email sent successfully to {recipients}")
        return len(recipients)
        
    except smtplib.SMTPAuthenticationError as e:
        print(f"[SMTP] ❌ Authentication Error: {e}")
        print("[SMTP] Проверьте:")
        print("  1. Правильность логина и пароля в настройках")
        print("  2. Статус учетной записи (не должна быть заблокирована)")
        print("  3. IP-адрес сервера приложения (не должен быть заблокирован)")
        return 0
    except smtplib.SMTPException as e:
        print(f"[SMTP] ❌ SMTP Error: {e}")
        import traceback
        traceback.print_exc()
        return 0
    except Exception as e:
        print(f"[SMTP] ❌ Error sending email: {e}")
        import traceback
        traceback.print_exc()
        return 0


@app.route('/api/settings/mail', methods=['GET'])
def get_mail_settings_api():
    settings = get_mail_settings()
    if settings:
        settings.pop('smtp_password', None)
    return jsonify({'success': True, 'data': settings})


@app.route('/api/settings/mail', methods=['POST'])
def save_mail_settings_api():
    data = request.json
    if save_mail_settings(data):
        return jsonify({'success': True})
    return jsonify({'success': False, 'error': 'Ошибка сохранения'}), 500


@app.route('/api/settings/mail/test', methods=['POST'])
def test_mail_settings():
    data = request.json or {}
    test_email = data.get('test_email')
    settings = get_mail_settings()
    
    if not settings:
        return jsonify({'success': False, 'error': 'Настройки почты не настроены'}), 400
    
    recipients = [test_email] if test_email else [settings.get('smtp_user')]
    
    try:
        sent = send_email(
            settings, 
            recipients, 
            "Тестовое письмо от Report Builder",
            "Тестовое письмо. Если вы его получили, настройки корректны."
        )
        if sent > 0:
            return jsonify({'success': True, 'message': f'Письмо успешно отправлено на {recipients}'})
        else:
            return jsonify({
                'success': False, 
                'error': 'Не удалось отправить письмо. Проверьте логи для деталей.'
            }), 500
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ═════════════════════════════════════════════════════════════════════════════
# ER DIAGRAM ROUTE - Добавить в app.py после существующих маршрутов
# ═════════════════════════════════════════════════════════════════════════════

@app.route('/erd')
@login_required
def erd_diagram():
    """
    Страница Entity Relationship Diagram
    Интерактивная диаграмма связей таблиц
    """
    return render_template('erd_diagram.html', user=request.user)


@app.route('/api/erd/schema-objects', methods=['GET'])
@login_required
def get_erd_schema_objects():
    """
    Получить список всех объектов схемы (таблицы, представления и т.д.)
    для загрузки в диаграмму
    
    Query параметры:
        schema: имя схемы (по умолчанию 'public')
        object_types: типы объектов для фильтрации ('TABLE,VIEW,MATERIALIZED VIEW')
    
    Returns:
        JSON со списком объектов, сгруппированных по типам
    """
    try:
        schema = request.args.get('schema', 'public')
        object_types = request.args.get('object_types', 'TABLE').split(',')
        
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        # Список таблиц
        if 'TABLE' in object_types:
            cur.execute("""
                SELECT 
                    c.relname AS name,
                    'TABLE' AS type,
                    pg_size_pretty(pg_total_relation_size(c.oid)) AS size,
                    (SELECT count(*) FROM pg_attribute WHERE attrelid = c.oid) AS columns
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s AND c.relkind IN ('r', 'p')
                ORDER BY c.relname
            """, (schema,))
            tables = cur.fetchall()
        else:
            tables = []
        
        # Список представлений
        if 'VIEW' in object_types:
            cur.execute("""
                SELECT 
                    table_name AS name,
                    'VIEW' AS type,
                    'N/A' AS size,
                    (SELECT count(*) FROM information_schema.columns 
                     WHERE table_name = v.table_name AND table_schema = v.table_schema) AS columns
                FROM information_schema.views v
                WHERE table_schema = %s
                ORDER BY table_name
            """, (schema,))
            views = cur.fetchall()
        else:
            views = []
        
        # Группируем результаты
        result = {
            'objects': {
                'tables': [dict(row) for row in tables],
                'views': [dict(row) for row in views],
            },
            'schema': schema,
            'total': len(tables) + len(views),
        }
        
        cur.close()
        conn.close()
        
        return jsonify({'success': True, 'data': result})
        
    except Exception as e:
        print(f"[ERD] Error loading schema objects: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/erd/table-info', methods=['GET'])
@login_required
def get_erd_table_info():
    """
    Получить детальную информацию о таблице для диаграммы
    
    Query параметры:
        table: имя таблицы (обязательный)
        schema: имя схемы (по умолчанию 'public')
    
    Returns:
        JSON с информацией о таблице, колонках и связях
    """
    try:
        table_name = request.args.get('table', '').strip()
        schema = request.args.get('schema', 'public')
        
        if not table_name:
            return jsonify({'success': False, 'error': 'table parameter required'}), 400
        
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        # Получаем информацию о таблице
        cur.execute("""
            SELECT 
                c.relname AS table_name,
                n.nspname AS schema,
                pg_size_pretty(pg_total_relation_size(c.oid)) AS size,
                (SELECT count(*) FROM pg_stat_user_tables WHERE relname = %s) AS row_count
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s
        """, (table_name, schema, table_name))
        
        table_info = cur.fetchone()
        
        if not table_info:
            return jsonify({'success': False, 'error': 'Table not found'}), 404
        
        # Получаем информацию о колонках
        cur.execute("""
            SELECT
                a.attname AS name,
                pg_catalog.format_type(a.atttypid, a.atttypmod) AS type,
                a.attnotnull AS not_null,
                obj_description(a.attrelid, 'pg_class') AS comment
            FROM pg_attribute a
            JOIN pg_class c ON a.attrelid = c.oid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s AND a.attnum > 0
            ORDER BY a.attnum
        """, (schema, table_name))
        
        columns = [dict(row) for row in cur.fetchall()]
        
        # Primary Keys
        cur.execute("""
            SELECT DISTINCT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_schema = %s 
              AND tc.table_name = %s 
              AND tc.constraint_type = 'PRIMARY KEY'
        """, (schema, table_name))
        
        pk_columns = {row[0] for row in cur.fetchall()}
        
        # Foreign Keys
        cur.execute("""
            SELECT 
                kcu.column_name,
                ccu.table_name AS referenced_table,
                ccu.column_name AS referenced_column,
                tc.constraint_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage ccu 
                ON ccu.constraint_name = tc.constraint_name
            WHERE tc.table_schema = %s 
              AND tc.table_name = %s 
              AND tc.constraint_type = 'FOREIGN KEY'
        """, (schema, table_name))
        
        fk_columns = {}
        for row in cur.fetchall():
            fk_columns[row[0]] = {
                'referenced_table': row[1],
                'referenced_column': row[2],
                'constraint_name': row[3],
            }
        
        # Enriching columns with PK/FK info
        for col in columns:
            col['is_pk'] = col['name'] in pk_columns
            col['is_fk'] = col['name'] in fk_columns
            if col['is_fk']:
                col['fk_info'] = fk_columns[col['name']]
        
        cur.close()
        conn.close()
        
        result = dict(table_info)
        result['columns'] = columns
        
        return jsonify({'success': True, 'data': result})
        
    except Exception as e:
        print(f"[ERD] Error getting table info: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/erd/related-tables', methods=['GET'])
@login_required
def get_erd_related_tables():
    """
    Получить список связанных таблиц (Foreign Keys в обе стороны)
    
    Query параметры:
        table: имя таблицы (обязательный)
        schema: имя схемы (по умолчанию 'public')
        depth: глубина поиска (по умолчанию 1)
    
    Returns:
        JSON со списком связанных таблиц
    """
    try:
        table_name = request.args.get('table', '').strip()
        schema = request.args.get('schema', 'public')
        depth = int(request.args.get('depth', 1))
        
        if not table_name:
            return jsonify({'success': False, 'error': 'table parameter required'}), 400
        
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        related_tables = set()
        relationships = []
        
        # Рекурсивный поиск связанных таблиц
        def find_related(tbl_name, current_depth=1):
            if current_depth > depth:
                return
            
            # Исходящие связи (Foreign Keys)
            cur.execute("""
                SELECT 
                    tc.table_name,
                    kcu.column_name,
                    ccu.table_name AS referenced_table,
                    ccu.column_name AS referenced_column,
                    tc.constraint_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name
                JOIN information_schema.constraint_column_usage ccu 
                    ON ccu.constraint_name = tc.constraint_name
                WHERE tc.table_schema = %s 
                  AND tc.table_name = %s 
                  AND tc.constraint_type = 'FOREIGN KEY'
            """, (schema, tbl_name))
            
            for row in cur.fetchall():
                ref_table = row['referenced_table']
                if ref_table not in related_tables:
                    related_tables.add(ref_table)
                    relationships.append({
                        'from': tbl_name,
                        'to': ref_table,
                        'from_col': row['column_name'],
                        'to_col': row['referenced_column'],
                        'type': 'many-to-one',
                        'constraint': row['constraint_name'],
                    })
                    if current_depth < depth:
                        find_related(ref_table, current_depth + 1)
            
            # Входящие связи (обратные Foreign Keys)
            cur.execute("""
                SELECT 
                    tc.table_name,
                    kcu.column_name,
                    ccu.table_name AS referenced_table,
                    ccu.column_name AS referenced_column,
                    tc.constraint_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name
                JOIN information_schema.constraint_column_usage ccu 
                    ON ccu.constraint_name = tc.constraint_name
                WHERE tc.table_schema = %s 
                  AND ccu.table_name = %s 
                  AND tc.constraint_type = 'FOREIGN KEY'
            """, (schema, tbl_name))
            
            for row in cur.fetchall():
                child_table = row['table_name']
                if child_table not in related_tables:
                    related_tables.add(child_table)
                    relationships.append({
                        'from': ref_table,
                        'to': child_table,
                        'from_col': row['referenced_column'],
                        'to_col': row['column_name'],
                        'type': 'one-to-many',
                        'constraint': row['constraint_name'],
                    })
                    if current_depth < depth:
                        find_related(child_table, current_depth + 1)
        
        find_related(table_name)
        
        cur.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'data': {
                'root_table': table_name,
                'related_tables': list(related_tables),
                'relationships': relationships,
            }
        })
        
    except Exception as e:
        print(f"[ERD] Error getting related tables: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/erd/export', methods=['POST'])
@login_required
def export_erd():
    """
    Экспортировать диаграмму в различные форматы
    
    POST данные:
        format: 'png' | 'svg' | 'json'
        data: JSON данные диаграммы
        filename: имя файла для сохранения
    
    Returns:
        Файл в указанном формате или JSON ошибка
    """
    try:
        data = request.json or {}
        export_format = data.get('format', 'png')
        diagram_data = data.get('data', {})
        filename = data.get('filename', 'erd-diagram')
        
        if export_format == 'json':
            # Просто возвращаем JSON
            return jsonify({
                'success': True,
                'data': diagram_data,
            })
        
        elif export_format == 'svg':
            # Генерируем SVG из данных диаграммы
            svg = generate_svg_from_erd(diagram_data)
            return Response(
                svg,
                mimetype='image/svg+xml',
                headers={'Content-Disposition': f'attachment; filename={filename}.svg'}
            )
        
        elif export_format == 'png':
            # PNG экспорт требует html2canvas на фронтенде
            # На бэкенде мы можем использовать canvas или другую библиотеку
            return jsonify({
                'success': False,
                'error': 'PNG export должен выполняться на клиенте',
            }), 400
        
        else:
            return jsonify({
                'success': False,
                'error': f'Неподдерживаемый формат: {export_format}',
            }), 400
            
    except Exception as e:
        print(f"[ERD] Error exporting diagram: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


def generate_svg_from_erd(data):
    """
    Генерировать SVG из данных диаграммы
    
    Args:
        data: dict с информацией о таблицах и связях
    
    Returns:
        SVG строка
    """
    svg_lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1200 800">',
        '<style>',
        '.table { stroke: #333; fill: #fff; }',
        '.table-name { font-weight: bold; font-size: 12px; }',
        '.column { font-size: 10px; }',
        '.pk { fill: #52b788; }',
        '.fk { fill: #f0a843; }',
        '.relation { stroke: #f0a843; fill: none; stroke-width: 1.5; }',
        '</style>',
    ]
    
    # Добавляем таблицы
    x, y = 50, 50
    for table in data.get('tables', []):
        svg_lines.append(f'<rect class="table" x="{x}" y="{y}" width="200" height="120"/>')
        svg_lines.append(f'<text class="table-name" x="{x + 10}" y="{y + 20}">{table.get("name", "Unknown")}</text>')
        
        column_y = y + 35
        for col in table.get('columns', [])[:5]:  # Max 5 columns per table
            badge = 'pk' if col.get('is_pk') else 'fk' if col.get('is_fk') else ''
            svg_lines.append(
                f'<text class="column {badge}" x="{x + 10}" y="{column_y}">'
                f'{col.get("name", "")}: {col.get("type", "")}</text>'
            )
            column_y += 15
        
        x += 250
        if x > 1000:
            x = 50
            y += 200
    
    # Добавляем связи
    for rel in data.get('relationships', []):
        # Упрощенное представление связей
        svg_lines.append(
            f'<line class="relation" x1="100" y1="100" x2="200" y2="200"/>'
        )
    
    svg_lines.append('</svg>')
    return '\n'.join(svg_lines)


def get_existing_ssh_tunnel(ssh_host, ssh_port, ssh_user, remote_db_host, remote_db_port):
    """
    Проверяет, существует ли уже активный SSH туннель с такими параметрами.
    
    Returns:
        tuple: (local_host, local_port) если туннель существует, иначе None
    """
    tunnel_key = f"{ssh_host}:{ssh_port}:{ssh_user}:{remote_db_host}:{remote_db_port}"
    
    with _ssh_tunnels_lock:
        if tunnel_key in _ssh_tunnels:
            tunnel = _ssh_tunnels[tunnel_key]
            try:
                if tunnel['client'].get_transport() and tunnel['client'].get_transport().is_active():
                    print(f"[SSH] ✅ Using existing tunnel: {tunnel_key}")
                    return (tunnel['local_host'], tunnel['local_port'])
            except:
                pass
    return None

@app.route('/api/db-profiles/current', methods=['GET'])
def get_current_profile():
    """Получить текущий активный профиль"""
    profile_id = session.get('active_profile_id')
    
    if not profile_id:
        return jsonify({'profile': None})
    
    from db_profiles import DatabaseProfileManager
    profile = DatabaseProfileManager.get_profile(profile_id)
    
    if not profile:
        return jsonify({'profile': None})
    
    # Не отправляем пароль
    profile_copy = profile.copy()
    profile_copy['password'] = '****'
    
    return jsonify({'profile': profile_copy})

@app.route('/api/settings/db/test', methods=['GET', 'POST'])
def test_db_connection():
    """
    Тестирование подключения к БД.
    
    GET - текущее подключение (без SSH)
    POST с {"use_ssh": true} - тестирование через SSH туннель
    """
    try:
        # Проверяем, нужно ли использовать SSH туннель
        use_ssh = False
        if request.method == 'POST':
            data = request.json or {}
            use_ssh = data.get('use_ssh', False)
        
        ssh_settings = get_ssh_settings()
        
        # Определяем параметры подключения
        db_host = app.config['DB_HOST']
        db_port = int(app.config['DB_PORT'])
        connection_method = 'direct'
        
        # Если запрошено подключение через SSH или SSH включен в настройках
        if use_ssh or (ssh_settings and ssh_settings.get('enabled', False)):
            if not ssh_settings:
                return jsonify({
                    'success': False, 
                    'error': 'SSH настройки не найдены. Сначала настройте SSH туннель.'
                }), 400
            
            remote_db_host = ssh_settings.get('remote_db_host', '127.0.0.1')
            remote_db_port = ssh_settings.get('remote_db_port', 5432)
            
            # Проверяем, есть ли уже активный туннель
            existing = get_existing_ssh_tunnel(
                ssh_settings['ssh_host'],
                ssh_settings.get('ssh_port', 22),
                ssh_settings['ssh_user'],
                remote_db_host,
                remote_db_port
            )
            
            if existing:
                db_host, db_port = existing
                connection_method = 'ssh_tunnel'
                print(f"[TEST] Using existing SSH tunnel: {db_host}:{db_port}")
            else:
                try:
                    print(f"[TEST] Creating new SSH tunnel for test connection...")
                    local_host, local_port = create_ssh_tunnel(
                        ssh_host=ssh_settings['ssh_host'],
                        ssh_port=ssh_settings.get('ssh_port', 22),
                        ssh_user=ssh_settings['ssh_user'],
                        ssh_password=ssh_settings.get('ssh_password'),
                        ssh_key_path=ssh_settings.get('ssh_key_path'),
                        remote_db_host=remote_db_host,
                        remote_db_port=remote_db_port
                    )
                    db_host = local_host
                    db_port = local_port
                    connection_method = 'ssh_tunnel'
                    print(f"[TEST] Using new SSH tunnel: {db_host}:{db_port}")
                except Exception as ssh_e:
                    print(f"[TEST] SSH tunnel creation failed: {ssh_e}")
                    return jsonify({
                        'success': False, 
                        'error': f'Не удалось создать SSH туннель: {str(ssh_e)}'
                    }), 500
        
        # Подключаемся к БД
        try:
            print(f"[TEST] Attempting connection to {db_host}:{db_port}...")
            conn = psycopg2.connect(
                host=db_host,
                port=db_port,
                database=app.config['DB_NAME'],
                user=app.config['DB_USER'],
                password=app.config['DB_PASSWORD'],
                client_encoding='UTF8',
                connect_timeout=10
            )
            cur = conn.cursor()
            cur.execute("SELECT version()")
            version = cur.fetchone()[0]
            cur.close()
            conn.close()
            
            return jsonify({
                'success': True, 
                'data': {
                    'version': version,
                    'connection_method': connection_method,
                    'host': db_host,
                    'port': db_port
                }
            })
        except Exception as db_e:
            return jsonify({
                'success': False, 
                'error': str(db_e),
                'connection_info': {
                    'method': connection_method,
                    'host': db_host,
                    'port': db_port
                }
            }), 500
            
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ═════════════════════════════════════════════════════════════════════════════
# NETWORK BUILDER - Конструктор сети водоснабжения
# ═════════════════════════════════════════════════════════════════════════════

@app.route('/network')
def network_builder():
    """Страница конструктора сети"""
    return render_template('network_builder.html')


@app.route('/api/network/tree', methods=['GET'])
def get_network_tree():
    """
    Получить рекурсивное дерево сети водоснабжения.
    
    Query параметры:
        root_node_id: ID корневого узла (обязательный)
        accounting_month: месяц для режима учета (формат YYYY-MM)
        use_accounting: использовать режим способов учета (true/false)
    
    Returns:
        JSON с деревом узлов и связей
    """
    try:
        root_node_id = request.args.get('root_node_id', type=int)
        accounting_month = request.args.get('accounting_month')
        use_accounting = request.args.get('use_accounting', 'false').lower() == 'true'
        
        if not root_node_id or root_node_id <= 0:
            return jsonify({
                'success': False,
                'error': 'root_node_id must be a positive integer'
            }), 400
        
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        # Выполняем рекурсивный запрос
        # ВАЖНО: все типы в UNION ALL должны совпадать!
        query = """
            WITH RECURSIVE tree_cte AS (
                -- Базовый случай: выбираем корневой элемент
                SELECT 
                    'root'::VARCHAR AS line_name,
                    NULL::BIGINT AS line_id,
                    %s::BIGINT AS node_id,
                    %s::BIGINT AS child_id,
                    0::INT AS level,
                    ARRAY[%s::BIGINT] AS path,
                    %s::TEXT AS path_str,
                    NULL::TEXT AS node_name,
                    NULL::TEXT AS node_type_name,
                    %s::BIGINT AS node_calculate_parameter_id
                
                UNION ALL
                
                -- Рекурсивный случай: присоединяем детей
                SELECT 
                    rl.line_name::VARCHAR,
                    rl.line_id::BIGINT,
                    rlp.node_calculate_parameter_id::BIGINT AS node_id,
                    rlpc.node_calculate_parameter_id::BIGINT AS child_id,
                    (t.level + 1)::INT,
                    t.path || rlpc.node_calculate_parameter_id,
                    t.path_str || '->' || rlpc.node_calculate_parameter_id::TEXT,
                    rn.node_name::TEXT,
                    rnt.node_type_name::TEXT,
                    rlpc.node_calculate_parameter_id::BIGINT AS node_calculate_parameter_id
                FROM tree_cte t
                JOIN public.rul_line_parameter rlp
                    ON t.child_id = rlp.node_calculate_parameter_id
                JOIN public.rul_line_parameter_child rlpc 
                    ON rlpc.line_parameter_id = rlp.line_parameter_id
                JOIN public.rul_node_calculate_parameter rncp
                    ON rlpc.node_calculate_parameter_id = rncp.node_calculate_parameter_id
                JOIN public.rul_node rn
                    ON rn.node_id = rncp.node_id
                JOIN public.rul_node_type rnt
                    ON rnt.node_type_id = rn.node_type_id
                JOIN public.rul_line rl 
                    ON rl.line_id = rlp.line_id
                WHERE t.level < 50  -- Защита от бесконечной рекурсии
            )
            SELECT 
                tree_cte.line_id,
                tree_cte.line_name,
                tree_cte.node_id,
                tree_cte.child_id,
                tree_cte.level,
                tree_cte.path,
                tree_cte.path_str,
                tree_cte.node_name,
                tree_cte.node_type_name,
                tree_cte.node_calculate_parameter_id,
                ro.object_id::BIGINT AS object_id,
                ro.object_name::TEXT AS object_name
            FROM tree_cte
            LEFT JOIN public.rul_node_calculate_parameter rncp
                ON rncp.node_calculate_parameter_id = tree_cte.child_id
            LEFT JOIN public.rul_node rn
                ON rn.node_id = rncp.node_id
            LEFT JOIN public.rul_object ro
                ON ro.object_id = rn.object_id
            ORDER BY tree_cte.level, tree_cte.path_str
            LIMIT 10000
        """
        
        print(f"[NETWORK] Fetching tree for node ID: {root_node_id}")
        
        cur.execute(query, (root_node_id, root_node_id, root_node_id, root_node_id, root_node_id))
        rows = cur.fetchall()
        
        result = []
        for row in rows:
            result.append({
                'line_id': row['line_id'],
                'line_name': row['line_name'],
                'node_id': row['node_id'],
                'child_id': row['child_id'],
                'level': row['level'],
                'path': row['path'],
                'path_str': row['path_str'],
                'node_name': row['node_name'],
                'node_type_name': row['node_type_name'],
                'node_calculate_parameter_id': row['node_calculate_parameter_id'],
                'object_id': row['object_id'],
                'object_name': row['object_name']
            })
        
        # Если включен режим "Использовать способ учета"
        accounting_data = {}
        connection_data = {}
        if use_accounting and accounting_month:
            # Парсим месяц
            try:
                year, month = map(int, accounting_month.split('-'))
                # Начало месяца
                p_start_date = datetime(year, month, 1)
                # Конец месяца
                if month == 12:
                    p_end_date = datetime(year + 1, 1, 1) - timedelta(seconds=1)
                else:
                    p_end_date = datetime(year, month + 1, 1) - timedelta(seconds=1)
                
                print(f"[NETWORK] Accounting mode: month={accounting_month}, p_start={p_start_date}, p_end={p_end_date}")
                
                # Собираем все node_calculate_parameter_id из результата
                node_ids = list(set([row['node_calculate_parameter_id'] for row in result if row['node_calculate_parameter_id']]))
                
                if node_ids:
                    # Запрос для получения способов учета с LEFT JOIN
                    # Для каждого node_calculate_parameter_id получаем все способы учета, действующие в выбранный месяц
                    acc_query = """
                        SELECT 
                            rat.accounting_type_name,
                            ratn.node_calculate_parameter_id,
                            GREATEST(COALESCE(ratn.start_date, '1970-01-01'::timestamp), %s::timestamp) as valid_start,
                            LEAST(COALESCE(ratn.end_date, '2100-12-31'::timestamp), %s::timestamp) as valid_end
                        FROM rul_accounting_type_node ratn
                        JOIN rul_accounting_type rat 
                            ON rat.accounting_type_id = ratn.accounting_type_id
                        WHERE ratn.node_calculate_parameter_id = ANY(%s)
                            AND (ratn.start_date IS NULL OR ratn.start_date <= %s::timestamp)
                            AND (ratn.end_date IS NULL OR ratn.end_date >= %s::timestamp)
                        ORDER BY rat.accounting_type_name
                    """
                    
                    cur.execute(acc_query, (p_start_date, p_end_date, node_ids, p_end_date, p_start_date))
                    acc_rows = cur.fetchall()
                    
                    # Группируем по node_calculate_parameter_id
                    for acc_row in acc_rows:
                        ncp_id = acc_row['node_calculate_parameter_id']
                        if ncp_id not in accounting_data:
                            accounting_data[ncp_id] = {
                                'types': [],
                                'valid_periods': []
                            }
                        accounting_data[ncp_id]['types'].append(acc_row['accounting_type_name'])
                        accounting_data[ncp_id]['valid_periods'].append({
                            'start': str(acc_row['valid_start']) if acc_row['valid_start'] else None,
                            'end': str(acc_row['valid_end']) if acc_row['valid_end'] else None
                        })
                    
                    print(f"[NETWORK] ✅ Found accounting data for {len(accounting_data)} nodes")

                    connection_query = """
                        SELECT
                            rc.connection_id,
                            rc.node_calculate_parameter_id,
                            COALESCE(NULLIF(TRIM(rc.connection_name), ''), 'Подключение #' || rc.connection_id::text) AS connection_name,
                            rc.start_date,
                            rc.end_date
                        FROM public.rul_connection rc
                        WHERE rc.node_calculate_parameter_id = ANY(%s)
                            AND COALESCE(rc.deleted, 0) = 0
                            AND COALESCE(rc.blocked, 0) = 0
                            AND (rc.start_date IS NULL OR rc.start_date <= %s::timestamp)
                            AND (rc.end_date IS NULL OR rc.end_date >= %s::timestamp)
                        ORDER BY rc.node_calculate_parameter_id, rc.connection_name, rc.connection_id
                    """

                    cur.execute(connection_query, (node_ids, p_end_date, p_start_date))
                    connection_rows = cur.fetchall()

                    for conn_row in connection_rows:
                        ncp_id = conn_row['node_calculate_parameter_id']
                        if ncp_id not in connection_data:
                            connection_data[ncp_id] = {
                                'count': 0,
                                'connections': []
                            }

                        connection_data[ncp_id]['connections'].append({
                            'connection_id': conn_row['connection_id'],
                            'connection_name': conn_row['connection_name'],
                            'start_date': str(conn_row['start_date']) if conn_row['start_date'] else None,
                            'end_date': str(conn_row['end_date']) if conn_row['end_date'] else None
                        })
                        connection_data[ncp_id]['count'] += 1

                    print(f"[NETWORK] ✅ Found connection data for {len(connection_data)} nodes")
                    
            except Exception as acc_e:
                print(f"[NETWORK] ❌ Error fetching accounting data: {acc_e}")
                import traceback
                traceback.print_exc()
        
        cur.close()
        conn.close()
        
        print(f"[NETWORK] ✅ Found {len(result)} nodes in tree")
        
        response_data = {
            'success': True,
            'data': result,
            'count': len(result)
        }
        
        # Добавляем данные о способах учета если есть
        if accounting_data:
            response_data['accounting_data'] = accounting_data
        if connection_data:
            response_data['connection_data'] = connection_data
        
        return jsonify(response_data)
        
    except Exception as e:
        print(f"[NETWORK] ❌ Error fetching network tree: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/network/info', methods=['GET'])
def get_network_info():
    """
    Получить статистику о сети.
    
    Query параметры:
        root_node_id: ID корневого узла
    """
    try:
        root_node_id = request.args.get('root_node_id', type=int)
        
        if not root_node_id or root_node_id <= 0:
            return jsonify({
                'success': False,
                'error': 'Invalid root_node_id'
            }), 400
        
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        # Статистика
        query = """
            WITH RECURSIVE tree_cte AS (
                SELECT 
                    %s AS node_id,
                    %s AS child_id,
                    0 AS level
                
                UNION ALL
                
                SELECT 
                    rlp.node_calculate_parameter_id AS node_id,
                    rlpc.node_calculate_parameter_id AS child_id,
                    t.level + 1
                FROM tree_cte t
                JOIN public.rul_line_parameter rlp
                    ON t.child_id = rlp.node_calculate_parameter_id
                JOIN public.rul_line_parameter_child rlpc 
                    ON rlpc.line_parameter_id = rlp.line_parameter_id
            )
            SELECT 
                COUNT(DISTINCT child_id) as node_count,
                MAX(level) as max_depth,
                COUNT(*) as edge_count
            FROM tree_cte
        """
        
        cur.execute(query, (root_node_id, root_node_id))
        stats = cur.fetchone()
        
        cur.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'data': {
                'node_count': stats['node_count'],
                'max_depth': stats['max_depth'],
                'edge_count': stats['edge_count']
            }
        })
        
    except Exception as e:
        print(f"[NETWORK] Error getting network info: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/network/objects', methods=['GET'])
def get_network_objects():
    """
    Получить список объектов для фильтрации.
    
    Returns:
        JSON со списком объектов (object_id, object_name)
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        cur.execute("""
            SELECT object_id, object_name 
            FROM rul_object 
            ORDER BY object_name
        """)
        
        rows = cur.fetchall()
        result = [{'object_id': row['object_id'], 'object_name': row['object_name']} for row in rows]
        
        cur.close()
        conn.close()
        
        return jsonify({'success': True, 'data': result})
        
    except Exception as e:
        print(f"[NETWORK] Error getting objects: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/network/nodes', methods=['GET'])
def get_network_nodes():
    """
    Получить список узлов для выбранного объекта.
    
    Query параметры:
        object_id: ID объекта (обязательный)
    
    Returns:
        JSON со списком узлов (node_id, node_name)
    """
    try:
        object_id = request.args.get('object_id', type=int)
        
        if not object_id or object_id <= 0:
            return jsonify({
                'success': False,
                'error': 'object_id must be a positive integer'
            }), 400
        
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        cur.execute("""
            SELECT node_id, node_name 
            FROM rul_node 
            WHERE object_id = %s
            ORDER BY node_name
        """, (object_id,))
        
        rows = cur.fetchall()
        result = [{'node_id': row['node_id'], 'node_name': row['node_name']} for row in rows]
        
        cur.close()
        conn.close()
        
        return jsonify({'success': True, 'data': result})
        
    except Exception as e:
        print(f"[NETWORK] Error getting nodes: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/network/parameters', methods=['GET'])
def get_network_parameters():
    """
    Получить список расчетных параметров для выбранного узла.
    
    Query параметры:
        node_id: ID узла (обязательный)
    
    Returns:
        JSON со списком параметров (node_calculate_parameter_id, parameter_name)
    """
    try:
        node_id = request.args.get('node_id', type=int)
        
        if not node_id or node_id <= 0:
            return jsonify({
                'success': False,
                'error': 'node_id must be a positive integer'
            }), 400
        
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        cur.execute("""
            SELECT rp.parameter_name || ' (' || ru.unit_name || ')' AS parameter_name,
                   rncp.node_calculate_parameter_id
            FROM rul_parameter rp
            JOIN rul_node_calculate_parameter rncp
                ON rp.parameter_id = rncp.parameter_id
            JOIN rul_unit ru
                ON ru.unit_id = rp.unit_id
            WHERE rncp.node_id = %s
            ORDER BY rp.parameter_name
        """, (node_id,))
        
        rows = cur.fetchall()
        result = [{'node_calculate_parameter_id': row['node_calculate_parameter_id'], 'parameter_name': row['parameter_name']} for row in rows]
        
        cur.close()
        conn.close()
        
        return jsonify({'success': True, 'data': result})
        
    except Exception as e:
        print(f"[NETWORK] Error getting parameters: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


# ═════════════════════════════════════════════════════════════════════════════
# TABLE RELATIONS - Построение схемы связей таблиц
# ═════════════════════════════════════════════════════════════════════════════

@app.route('/api/table/<table_name>/relations', methods=['GET'])
@login_required
def get_table_relations(table_name):
    """
    Получить схему связей таблицы с использованием функции report_get_possible_joins.
    Поддерживает несколько уровней через итеративные вызовы.
    
    Query параметры:
        levels: количество уровней для отображения (1-5, по умолчанию 2)
    
    Returns:
        JSON с деревом связанных таблиц
    """
    try:
        levels = request.args.get('levels', 2, type=int)
        
        # Ограничиваем уровни для производительности
        if levels < 1:
            levels = 1
        elif levels > 5:
            levels = 5
        
        table_name = table_name.strip().lower()
        
        if not table_name or not table_name.replace('_', '').isalnum():
            return jsonify({
                'success': False,
                'error': 'Invalid table name'
            }), 400
        
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        # Собираем все связи итеративно (быстрее чем рекурсивный SQL)
        all_relations = []
        visited = {table_name}
        current_tables = [table_name]
        
        for level in range(1, levels + 1):
            if not current_tables:
                break
                
            # Создаем массив для SQL
            tables_array = '{' + ','.join(f'"{t}"' for t in current_tables) + '}'
            
            query = """
                SELECT 
                    target_table,
                    target_schema,
                    join_type,
                    source_column,
                    target_column,
                    constraint_name,
                    match_confidence,
                    join_suggestion
                FROM report_get_possible_joins(%s::text, 'public'::text, %s::text[], true)
            """
            
            cur.execute(query, (table_name, tables_array))
            rows = cur.fetchall()
            
            if not rows:
                break
            
            # Собираем таблицы для следующего уровня
            next_tables = []
            
            for row in rows:
                target = row['target_table']
                
                # Пропускаем уже посещенные
                if target in visited:
                    continue
                
                visited.add(target)
                
                # Преобразуем к формату фронтенда
                relation = {
                    'source_table': table_name if level == 1 else current_tables[0],
                    'target_table': target,
                    'relation_type': row['join_type'],
                    'fk_column': row['source_column'],
                    'pk_column': row['target_column'],
                    'level': level,
                    'path': [table_name, target],
                    'path_str': f"{table_name} -> {target}"
                }
                
                # Для REVERSE_FK инвертируем направление
                if row['join_type'] == 'REVERSE_FK':
                    relation['source_table'] = target
                    relation['target_table'] = table_name
                    relation['fk_column'] = row['target_column']
                    relation['pk_column'] = row['source_column']
                    relation['path'] = [target, table_name]
                    relation['path_str'] = f"{target} <- {table_name}"
                
                all_relations.append(relation)
                
                if level < levels:
                    next_tables.append(target)
            
            current_tables = next_tables
        
        cur.close()
        conn.close()
        
        # Группируем по уровням
        levels_data = {}
        for item in all_relations:
            level = item['level']
            if level not in levels_data:
                levels_data[level] = []
            levels_data[level].append(item)
        
        max_level = max([item['level'] for item in all_relations], default=1)
        
        print(f"[RELATIONS] ✅ Found {len(all_relations)} relations in {max_level} levels (FOREIGN_KEY: {len([r for r in all_relations if r['relation_type'] == 'FOREIGN_KEY'])}, REVERSE_FK: {len([r for r in all_relations if r['relation_type'] == 'REVERSE_FK'])})")
        
        return jsonify({
            'success': True,
            'data': all_relations,
            'by_levels': levels_data,
            'total': len(all_relations),
            'max_level': max_level
        })
        
    except Exception as e:
        print(f"[RELATIONS] ❌ Error fetching relations: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/table/<table_name>/columns-with-relations', methods=['GET'])
def get_table_columns_with_relations(table_name):
    """
    Получить колонки таблицы вместе с информацией о связях.
    
    Returns:
        JSON с колонками и их связями с другими таблицами
    """
    try:
        table_name = table_name.strip().lower()
        
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        # Получаем колонки с информацией о foreign key
        query = """
            SELECT 
                c.column_name,
                c.data_type,
                (c.is_nullable = 'YES') AS is_nullable,
                c.column_default,
                kcu.constraint_name,
                rc.referenced_table_name,
                kcu2.column_name AS referenced_column_name,
                tc.constraint_type
            FROM information_schema.columns c
            LEFT JOIN information_schema.key_column_usage kcu
                ON c.table_name = kcu.table_name 
                AND c.column_name = kcu.column_name
            LEFT JOIN information_schema.referential_constraints rc
                ON kcu.constraint_name = rc.constraint_name
            LEFT JOIN information_schema.key_column_usage kcu2
                ON rc.unique_constraint_name = kcu2.constraint_name
            LEFT JOIN information_schema.table_constraints tc
                ON kcu.constraint_name = tc.constraint_name
            WHERE c.table_schema = 'public'
                AND c.table_name = %s
            ORDER BY c.ordinal_position
        """
        
        cur.execute(query, (table_name,))
        rows = cur.fetchall()
        
        result = []
        for row in rows:
            col_info = {
                'column_name': row['column_name'],
                'data_type': row['data_type'],
                'is_nullable': row['is_nullable'],
                'column_default': row['column_default'],
                'is_foreign_key': row['constraint_name'] is not None,
                'is_primary_key': row['constraint_type'] == 'PRIMARY KEY'
            }
            
            if row['constraint_name']:
                col_info['foreign_key'] = {
                    'constraint_name': row['constraint_name'],
                    'referenced_table': row['referenced_table_name'],
                    'referenced_column': row['referenced_column_name']
                }
            
            result.append(col_info)
        
        cur.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'table_name': table_name,
            'columns': result,
            'total': len(result)
        })
        
    except Exception as e:
        print(f"[RELATIONS] Error getting columns with relations: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
        
# ─────────────────────────────────────────────
# SSH SETTINGS API
# ─────────────────────────────────────────────

@app.route('/api/settings/ssh', methods=['GET'])
def get_ssh_settings_api():
    """Получить текущие SSH настройки (без пароля и ключа)"""
    try:
        settings = get_ssh_settings()
        if settings:
            settings.pop('ssh_password', None)
            settings.pop('ssh_key_path', None)
        
        # Проверяем статус туннеля
        tunnel_info = None
        if settings:
            tunnel_key = f"{settings.get('ssh_host')}:{settings.get('ssh_port', 22)}:{settings.get('ssh_user')}:{settings.get('remote_db_host', '127.0.0.1')}:{settings.get('remote_db_port', 5432)}"
            
            with _ssh_tunnels_lock:
                if tunnel_key in _ssh_tunnels:
                    tunnel = _ssh_tunnels[tunnel_key]
                    try:
                        if tunnel['client'].get_transport() and tunnel['client'].get_transport().is_active():
                            tunnel_info = {
                                'active': True,
                                'local_host': tunnel['local_host'],
                                'local_port': tunnel['local_port']
                            }
                    except:
                        pass
        
        return jsonify({
            'success': True,
            'data': settings,
            'tunnel_active': tunnel_info['active'] if tunnel_info else False,
            'tunnel_info': tunnel_info
        })
    except Exception as e:
        print(f"[SSH] Error getting settings: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/settings/ssh', methods=['POST'])
def save_ssh_settings_api():
    """Сохранить SSH настройки"""
    try:
        data = request.json or {}
        
        # Валидация
        if not data.get('ssh_host') or not data.get('ssh_user'):
            return jsonify({'success': False, 'error': 'SSH Host и SSH User обязательны'}), 400
        
        if save_ssh_settings(data):
            return jsonify({'success': True, 'message': 'SSH settings saved successfully'})
        else:
            return jsonify({'success': False, 'error': 'Failed to save SSH settings'}), 500
    except Exception as e:
        print(f"[SSH] Error saving settings: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/settings/ssh/test', methods=['POST'])
def test_ssh_connection():
    """🆕 Тестировать SSH подключение"""
    try:
        data = request.json or {}
        
        if not PARAMIKO_AVAILABLE:
            return jsonify({
                'success': False,
                'error': 'paramiko не установлен. Выполните: pip install paramiko'
            }), 400
        
        print(f"[SSH] 🔧 Testing connection to {data['ssh_host']}:{data.get('ssh_port', 22)}")
        
        # Создаем временное подключение для теста
        temp_ssh = paramiko.SSHClient()
        temp_ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        try:
            if data.get('ssh_key_path'):
                print(f"[SSH] Using SSH key: {data['ssh_key_path']}")
                temp_ssh.connect(
                    hostname=data['ssh_host'],
                    port=data.get('ssh_port', 22),
                    username=data['ssh_user'],
                    key_filename=data['ssh_key_path'],
                    timeout=10,
                    banner_timeout=10
                )
            else:
                print(f"[SSH] Using password authentication")
                temp_ssh.connect(
                    hostname=data['ssh_host'],
                    port=data.get('ssh_port', 22),
                    username=data['ssh_user'],
                    password=data.get('ssh_password'),
                    timeout=10,
                    banner_timeout=10
                )
            
            print(f"[SSH] ✅ SSH connection successful")
            
            # Проверяем доступность БД на удаленном сервере
            stdin, stdout, stderr = temp_ssh.exec_command(
                f"nc -z {data.get('remote_db_host', 'localhost')} {data.get('remote_db_port', 5432)} && echo 'OK' || echo 'FAIL'"
            )
            output = stdout.read().decode().strip()
            
            temp_ssh.close()
            
            if output == 'OK':
                print(f"[SSH] ✅ Remote database port is accessible")
                return jsonify({
                    'success': True,
                    'message': 'SSH connection and remote database port are accessible'
                })
            else:
                return jsonify({
                    'success': True,
                    'message': 'SSH connection OK, but cannot verify remote database port (netcat may not be available)',
                    'warning': 'Cannot verify DB port availability'
                })
        
        except paramiko.AuthenticationException as e:
            temp_ssh.close()
            print(f"[SSH] ❌ SSH Authentication failed: {e}")
            return jsonify({
                'success': False,
                'error': f'SSH authentication failed: {str(e)}'
            }), 401
        
        except paramiko.SSHException as e:
            temp_ssh.close()
            print(f"[SSH] ❌ SSH error: {e}")
            return jsonify({
                'success': False,
                'error': f'SSH connection error: {str(e)}'
            }), 400
        
        except Exception as e:
            temp_ssh.close()
            print(f"[SSH] ❌ Connection test error: {e}")
            return jsonify({
                'success': False,
                'error': f'Connection error: {str(e)}'
            }), 400
    
    except Exception as e:
        print(f"[SSH] ❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/settings/ssh/start', methods=['POST'])
def start_ssh_tunnel_api():
    """Запустить SSH туннель (вызывается при подключении)"""
    try:
        data = request.json or {}
        settings = get_ssh_settings()
        
        if not settings:
            settings = data
        
        if not PARAMIKO_AVAILABLE:
            return jsonify({
                'success': False,
                'error': 'paramiko не установлен. Выполните: pip install paramiko'
            }), 400
        
        print(f"[SSH] 🚀 Starting SSH tunnel...")
        
        try:
            local_host, local_port = create_ssh_tunnel(
                ssh_host=settings['ssh_host'],
                ssh_port=settings.get('ssh_port', 22),
                ssh_user=settings['ssh_user'],
                ssh_password=settings.get('ssh_password'),
                ssh_key_path=settings.get('ssh_key_path'),
                remote_db_host=settings.get('remote_db_host', 'localhost'),
                remote_db_port=settings.get('remote_db_port', 5432)
            )
            
            # Проверяем доступность БД через туннель
            try:
                test_conn = psycopg2.connect(
                    host=local_host,
                    port=local_port,
                    database=app.config['DB_NAME'],
                    user=app.config['DB_USER'],
                    password=app.config['DB_PASSWORD'],
                    connect_timeout=5
                )
                test_conn.close()
                print(f"[SSH] ✅ SSH tunnel started and DB connection successful")
                return jsonify({
                    'success': True,
                    'message': f'SSH tunnel started successfully (listening on {local_host}:{local_port})'
                })
            except Exception as db_e:
                print(f"[SSH] ⚠️ SSH tunnel started but DB connection failed: {db_e}")
                return jsonify({
                    'success': True,
                    'message': f'SSH tunnel started (listening on {local_host}:{local_port})',
                    'warning': f'DB connection test failed: {str(db_e)}'
                })
        
        except Exception as e:
            print(f"[SSH] ❌ Failed to start tunnel: {e}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 400
    
    except Exception as e:
        print(f"[SSH] ❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/settings/ssh/stop', methods=['POST'])
def stop_ssh_tunnel_api():
    """Остановить SSH туннель"""
    try:
        settings = get_ssh_settings()
        if settings:
            close_ssh_tunnel(
                ssh_host=settings['ssh_host'],
                ssh_port=settings.get('ssh_port', 22),
                ssh_user=settings['ssh_user'],
                remote_db_host=settings.get('remote_db_host', 'localhost'),
                remote_db_port=settings.get('remote_db_port', 5432)
            )
        
        return jsonify({
            'success': True,
            'message': 'SSH tunnel stopped'
        })
    except Exception as e:
        print(f"[SSH] Error stopping tunnel: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# ─────────────────────────────────────────────
# CHART DATA
# ─────────────────────────────────────────────

@app.route('/api/chart-data', methods=['POST'])
def get_chart_data():
    try:
        data = request.json
        main_table = data.get('main_table')
        x_axis = data.get('x_axis')
        y_axis = data.get('y_axis')
        aggregate_function = data.get('aggregate_function', 'COUNT')
        joins = data.get('joins', [])
        conditions = data.get('conditions', [])
        limit = data.get('limit', 20)

        if not main_table or not x_axis:
            return jsonify({'success': False, 'error': 'Необходимы main_table и x_axis'}), 400

        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        joins_param = None
        if joins:
            join_tuples = [(j.get('source_table'), j['table_name'], j.get('alias', j['table_name']),
                           j['join_type'], j['left_column'], j['right_column'], j.get('confidence', 0.0))
                          for j in joins]
            joins_param = join_tuples

        conditions_param = None
        if conditions:
            cond_tuples = [(c['column_name'], c['operator'], c['value'], c.get('logic_operator', 'AND'))
                          for c in conditions]
            conditions_param = cond_tuples

        cur.execute("""
            SELECT * FROM report_get_chart_data(
                %s, 'public',
                %s::report_join[],
                %s, %s, %s,
                %s::report_condition[],
                %s
            )
        """, (main_table, joins_param if joins_param else None,
              x_axis, y_axis if y_axis else None, aggregate_function,
              conditions_param if conditions_param else None, limit))

        rows = cur.fetchall()
        result = [{'label': row['label'], 'value': float(row['value']), 'tooltip': row['tooltip']} for row in rows]
        cur.close()
        conn.close()
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        import traceback
        return jsonify({'success': False, 'error': str(e), 'details': traceback.format_exc()}), 500

@app.route('/sql-editor')
@login_required
def sql_editor():
    return render_template('sql_editor/__init__.html', user=request.user)

@app.route('/api/sql/execute', methods=['POST'])
@login_required
def sql_execute():
    import re
    data = request.json or {}
    sql  = (data.get('sql') or '').strip()
    limit = int(data.get('limit') or 100)
 
    if not sql:
        return jsonify({'success': False, 'error': 'SQL запрос не может быть пустым'}), 400
 
    profile_id = session.get('active_profile_id')
    if not profile_id:
        return jsonify({'success': False, 'error': 'Нет активного подключения к БД'}), 400
 
    profile = DatabaseProfileManager.get_profile(profile_id)
    if not profile:
        return jsonify({'success': False, 'error': 'Профиль подключения не найден'}), 400
 
    if profile.get('db_type', 'postgresql') != 'postgresql':
        return jsonify({'success': False, 'error': 'SQL-редактор поддерживает только PostgreSQL'}), 400
 
    # ✅ УЛУЧШЕННОЕ определение типа запроса
    def get_first_sql_keyword(sql_text):
        """
        Извлекает первое SQL ключевое слово, игнорируя:
        - Однострочные комментарии (-- ...)
        - Многострочные комментарии (/* ... */)
        - Пробелы и переводы строк
        """
        # Удаляем многострочные комментарии /* ... */
        sql_clean = re.sub(r'/\*.*?\*/', '', sql_text, flags=re.DOTALL)
        
        # Удаляем однострочные комментарии -- ...
        sql_clean = re.sub(r'--.*?(\n|$)', '\n', sql_clean)
        
        # Убираем лишние пробелы и переводы строк
        sql_clean = sql_clean.strip()
        
        # Извлекаем первое слово
        match = re.match(r'^\s*(\w+)', sql_clean, re.IGNORECASE)
        if match:
            return match.group(1).upper()
        return None
    
    first_keyword = get_first_sql_keyword(sql)
    
    # Список команд, которые возвращают данные
    select_keywords = {'SELECT', 'WITH', 'TABLE', 'SHOW', 'EXPLAIN', 'ANALYZE', 'VALUES'}
    
    is_select = first_keyword in select_keywords
 
    conn = None
    try:
        conn = get_db_connection()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
 
        exec_sql = sql
        
        # ✅ Добавляем LIMIT только для обычных SELECT
        if first_keyword == 'SELECT' and limit > 0:
            # Проверяем, нет ли уже LIMIT в запросе
            if not re.search(r'\bLIMIT\s+\d+', sql, re.IGNORECASE):
                exec_sql = sql.rstrip().rstrip(';') + f'\nLIMIT {limit};'
 
        print(f"[SQL EXECUTE] First keyword: {first_keyword}")
        print(f"[SQL EXECUTE] Query type: {'SELECT' if is_select else 'DML'}")
        print(f"[SQL EXECUTE] Executing: {exec_sql[:200]}...")
        
        cur.execute(exec_sql)
 
        # ✅ Если это запрос, возвращающий данные
        if is_select:
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description] if cur.description else []
            
            print(f"[SQL EXECUTE] ✅ Fetched {len(rows)} rows, {len(columns)} columns")
            
            # ✅ Преобразуем DictRow в обычные словари
            result_rows = [dict(r) for r in rows]
            
            cur.close()
            conn.close()
            
            return jsonify({
                'success': True, 
                'data': {
                    'columns': columns, 
                    'rows': result_rows
                }
            })
        else:
            # ✅ Для INSERT/UPDATE/DELETE
            affected = cur.rowcount
            conn.commit()
            
            print(f"[SQL EXECUTE] ✅ Affected {affected} rows")
            
            cur.close()
            conn.close()
            
            return jsonify({
                'success': True, 
                'rows_affected': affected
            })
 
    except Exception as e:
        print(f"[SQL EXECUTE] ❌ Error: {e}")
        import traceback
        traceback.print_exc()
        
        try: 
            if conn:
                conn.rollback()
                conn.close()
        except: 
            pass
        
        return jsonify({'success': False, 'error': str(e)}), 200

@app.route('/api/sql/schema-objects', methods=['GET'])
@login_required
def sql_schema_objects():
    obj_type = request.args.get('type', 'views')
    schema = request.args.get('schema', 'public')
    try:
        conn = get_db_connection()
        cur  = conn.cursor()
 
        queries = {
            'views':      "SELECT table_name FROM information_schema.views WHERE table_schema=%s ORDER BY table_name",
            'matviews':   "SELECT matviewname AS table_name FROM pg_matviews WHERE schemaname=%s ORDER BY matviewname",
            'functions':  "SELECT routine_name AS table_name FROM information_schema.routines WHERE routine_schema=%s AND routine_type='FUNCTION' ORDER BY routine_name",
            'procedures': "SELECT routine_name AS table_name FROM information_schema.routines WHERE routine_schema=%s AND routine_type='PROCEDURE' ORDER BY routine_name",
            'sequences':  "SELECT sequence_name AS table_name FROM information_schema.sequences WHERE sequence_schema=%s ORDER BY sequence_name",
            'triggers':   "SELECT trigger_name AS table_name FROM information_schema.triggers WHERE trigger_schema=%s ORDER BY trigger_name",
            'types':      "SELECT typname AS table_name FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace WHERE n.nspname=%s AND t.typtype='c' ORDER BY typname",
            'extensions': "SELECT extname AS table_name FROM pg_extension ORDER BY extname",
        }
        q = queries.get(obj_type)
        if not q:
            return jsonify({'success': False, 'error': 'Unknown type'}), 400
 
        if obj_type == 'extensions':
            cur.execute(q)
        else:
            cur.execute(q, (schema,))
        items = [row[0] for row in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify({'success': True, 'data': items})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 200


@app.route('/api/sql/table-detail', methods=['GET'])
@login_required
def sql_table_detail():
    table_name = request.args.get('table', '').strip()
    tab        = request.args.get('tab', 'columns')
 
    if not table_name:
        return jsonify({'success': False, 'error': 'table parameter required'}), 400
 
    try:
        conn = get_db_connection()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
 
        # ── COLUMNS ──────────────────────────────────────────────
        if tab == 'columns':
            cur.execute('''
                SELECT
                    c.column_name,
                    c.data_type || CASE
                        WHEN c.character_maximum_length IS NOT NULL
                        THEN '(' || c.character_maximum_length || ')'
                        WHEN c.numeric_precision IS NOT NULL AND c.numeric_scale IS NOT NULL
                        THEN '(' || c.numeric_precision || ',' || c.numeric_scale || ')'
                        ELSE '' END                          AS data_type,
                    c.is_nullable = 'YES'                   AS is_nullable,
                    c.column_default,
                    EXISTS (
                        SELECT 1 FROM pg_constraint pc
                        JOIN pg_class t  ON t.oid = pc.conrelid
                        JOIN pg_namespace n ON n.oid = t.relnamespace
                        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(pc.conkey)
                        WHERE n.nspname = 'public'
                          AND t.relname = c.table_name
                          AND pc.contype = 'p'
                          AND a.attname = c.column_name
                    )                                       AS is_pk,
                    EXISTS (
                        SELECT 1 FROM pg_constraint pc
                        JOIN pg_class t  ON t.oid = pc.conrelid
                        JOIN pg_namespace n ON n.oid = t.relnamespace
                        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(pc.conkey)
                        WHERE n.nspname = 'public'
                          AND t.relname = c.table_name
                          AND pc.contype = 'f'
                          AND a.attname = c.column_name
                    )                                       AS is_fk,
                    EXISTS (
                        SELECT 1 FROM pg_constraint pc
                        JOIN pg_class t  ON t.oid = pc.conrelid
                        JOIN pg_namespace n ON n.oid = t.relnamespace
                        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(pc.conkey)
                        WHERE n.nspname = 'public'
                          AND t.relname = c.table_name
                          AND pc.contype = 'u'
                          AND a.attname = c.column_name
                    )                                       AS is_unique,
                    col_description(
                        format('%%I.%%I', 'public', c.table_name)::regclass::oid,
                        c.ordinal_position
                    )                                       AS column_comment
                FROM information_schema.columns c
                WHERE c.table_schema = 'public'
                  AND c.table_name   = %s
                ORDER BY c.ordinal_position
            ''', (table_name,))
            rows = [dict(r) for r in cur.fetchall()]
            cur.close(); conn.close()
            return jsonify({'success': True, 'data': rows})
 
        # ── INDEXES ───────────────────────────────────────────────
        elif tab == 'indexes':
            cur.execute('''
                SELECT
                    i.relname                           AS indexname,
                    am.amname                           AS index_type,
                    ix.indisunique                      AS is_unique,
                    pg_get_indexdef(ix.indexrelid)      AS indexdef,
                    string_agg(a.attname, ', ' ORDER BY array_position(ix.indkey, a.attnum)) AS columns
                FROM pg_index ix
                JOIN pg_class t  ON t.oid  = ix.indrelid
                JOIN pg_class i  ON i.oid  = ix.indexrelid
                JOIN pg_am    am ON am.oid = i.relam
                JOIN pg_namespace n ON n.oid = t.relnamespace
                JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
                WHERE t.relname = %s AND n.nspname = 'public'
                GROUP BY i.relname, am.amname, ix.indisunique, ix.indexrelid
                ORDER BY i.relname
            ''', (table_name,))
            rows = [dict(r) for r in cur.fetchall()]
            cur.close(); conn.close()
            return jsonify({'success': True, 'data': rows})
 
        # ── CONSTRAINTS ───────────────────────────────────────────
        elif tab == 'constraints':
            cur.execute('''
                SELECT
                    tc.constraint_name,
                    tc.constraint_type,
                    kcu.column_name,
                    ccu.table_name  AS foreign_table,
                    ccu.column_name AS foreign_column
                FROM information_schema.table_constraints tc
                LEFT JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
                LEFT JOIN information_schema.constraint_column_usage ccu
                    ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema
                WHERE tc.table_schema = 'public' AND tc.table_name = %s
                ORDER BY tc.constraint_type, tc.constraint_name
            ''', (table_name,))
            rows = [dict(r) for r in cur.fetchall()]
            cur.close(); conn.close()
            return jsonify({'success': True, 'data': rows})
 
        # ── DDL ───────────────────────────────────────────────────
        elif tab == 'ddl':
            # Build CREATE TABLE DDL manually
            cur.execute('''
                SELECT
                    c.column_name,
                    c.data_type,
                    c.character_maximum_length,
                    c.numeric_precision,
                    c.numeric_scale,
                    c.is_nullable,
                    c.column_default,
                    c.ordinal_position,
                    col_description(
                        format('%%I.%%I', 'public', c.table_name)::regclass::oid,
                        c.ordinal_position
                    ) AS column_comment
                FROM information_schema.columns c
                WHERE c.table_schema = 'public' AND c.table_name = %s
                ORDER BY c.ordinal_position
            ''', (table_name,))
            cols = cur.fetchall()

            # Get constraints using pg_constraint for correctness (handles multi-column keys)
            cur.execute('''
                SELECT
                    pc.conname AS constraint_name,
                    CASE pc.contype
                        WHEN 'p' THEN 'PRIMARY KEY'
                        WHEN 'f' THEN 'FOREIGN KEY'
                        WHEN 'u' THEN 'UNIQUE'
                        WHEN 'c' THEN 'CHECK'
                    END AS constraint_type,
                    pg_get_constraintdef(pc.oid) AS constraint_def
                FROM pg_constraint pc
                JOIN pg_class t ON t.oid = pc.conrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE n.nspname = 'public' AND t.relname = %s
                  AND pc.contype IN ('p', 'f', 'u', 'c')
                ORDER BY pc.contype, pc.conname
            ''', (table_name,))
            constraints = cur.fetchall()
            cur.close(); conn.close()

            # Build DDL string
            lines = []
            for col in cols:
                dt = col['data_type'].upper()
                if col['character_maximum_length']:
                    dt += f"({col['character_maximum_length']})"
                elif col['numeric_precision'] and col['numeric_scale'] is not None:
                    dt += f"({col['numeric_precision']},{col['numeric_scale']})"
                nn = ' NOT NULL' if col['is_nullable'] == 'NO' else ''
                df = f" DEFAULT {col['column_default']}" if col['column_default'] else ''
                lines.append(f"    {col['column_name']} {dt}{nn}{df}")

            for c in constraints:
                lines.append(f"    CONSTRAINT {c['constraint_name']} {c['constraint_def']}")

            ddl = f"CREATE TABLE public.{table_name} (\n" + ',\n'.join(lines) + "\n);"

            # Append COMMENT ON COLUMN statements for columns that have comments
            comment_lines = []
            for col in cols:
                if col['column_comment']:
                    safe_comment = col['column_comment'].replace("'", "''")
                    tbl_ident = '"' + table_name.replace('"', '""') + '"'
                    col_ident = '"' + col['column_name'].replace('"', '""') + '"'
                    comment_lines.append(
                        f"COMMENT ON COLUMN public.{tbl_ident}.{col_ident} IS '{safe_comment}';"
                    )
            if comment_lines:
                ddl += '\n\n' + '\n'.join(comment_lines)

            return jsonify({'success': True, 'data': {'ddl': ddl}})
 
        return jsonify({'success': False, 'error': 'Unknown tab'}), 400
 
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 200


@app.route('/api/sql/routine-detail', methods=['GET'])
@login_required
def sql_routine_detail():
    name      = request.args.get('name', '').strip()
    obj_type  = request.args.get('type', 'function')  # 'function' or 'procedure'
    tab       = request.args.get('tab', 'ddl')         # 'ddl', 'params'

    if not name:
        return jsonify({'success': False, 'error': 'name parameter required'}), 400

    try:
        conn = get_db_connection()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        if tab == 'ddl':
            prokind_filter = "p.prokind IN ('f', 'w', 'a')" if obj_type == 'function' else "p.prokind = 'p'"
            cur.execute(
                "SELECT pg_get_functiondef(p.oid) AS ddl"
                " FROM pg_proc p"
                " JOIN pg_namespace n ON n.oid = p.pronamespace"
                " WHERE n.nspname = 'public' AND p.proname = %s AND " + prokind_filter +
                " LIMIT 1",
                (name,)
            )
            row = cur.fetchone()
            ddl = row['ddl'] if row else '-- DDL недоступен'
            cur.close(); conn.close()
            return jsonify({'success': True, 'data': {'ddl': ddl}})

        elif tab == 'params':
            cur.execute('''
                SELECT
                    p.parameter_name,
                    p.parameter_mode,
                    p.data_type,
                    p.parameter_default
                FROM information_schema.parameters p
                WHERE p.specific_schema = 'public'
                  AND p.specific_name LIKE %s || '%%'
                ORDER BY p.ordinal_position
            ''', (name,))
            rows = [dict(r) for r in cur.fetchall()]
            cur.close(); conn.close()
            return jsonify({'success': True, 'data': rows})

        cur.close(); conn.close()
        return jsonify({'success': False, 'error': 'Unknown tab'}), 400

    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 200


@app.route('/api/sql/table-stats', methods=['GET'])
@login_required
def sql_table_stats():
    table_name = request.args.get('table', '').strip()
    schema     = request.args.get('schema', 'public').strip()

    if not table_name:
        return jsonify({'success': False, 'error': 'table parameter required'}), 400

    try:
        conn = get_db_connection()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute('''
            SELECT
                COALESCE(s.n_live_tup, 0)                               AS row_count,
                pg_size_pretty(pg_total_relation_size(c.oid))           AS total_size,
                pg_size_pretty(pg_relation_size(c.oid))                 AS table_size,
                (SELECT COUNT(*)
                   FROM pg_index i
                  WHERE i.indrelid = c.oid)                             AS index_count
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_stat_user_tables s
                   ON s.relname = c.relname AND s.schemaname = n.nspname
            WHERE c.relname = %s AND n.nspname = %s AND c.relkind = 'r'
        ''', (table_name, schema))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            return jsonify({'success': False, 'error': 'Table not found'}), 404
        return jsonify({
            'success': True,
            'data': {
                'row_count':   int(row['row_count']  or 0),
                'total_size':  row['total_size']  or '0 bytes',
                'table_size':  row['table_size']  or '0 bytes',
                'index_count': int(row['index_count'] or 0),
            }
        })
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 200


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5005)

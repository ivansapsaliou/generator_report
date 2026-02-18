from flask import Flask, render_template, jsonify, request, flash, redirect, url_for, Response
import psycopg2
import psycopg2.extras
import json
from config import Config
import re
import psycopg2.extensions
from psycopg2.extensions import AsIs
from collections import defaultdict
import base64
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import os
from datetime import datetime
import io
import csv
import threading
import time
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from os.path import basename
import os
try:
    import paramiko
    PARAMIKO_AVAILABLE = True
except ImportError:
    PARAMIKO_AVAILABLE = False

import socket
from threading import Lock

# Глобальное хранилище SSH туннелей (один на подключение)
_ssh_tunnels = {}
_ssh_tunnels_lock = Lock()

app = Flask(__name__)
app.config.from_object(Config)
app.secret_key = app.config['SECRET_KEY']

# Global SSH tunnel instance
_ssh_tunnel = None
_ssh_tunnel_lock = threading.Lock()


# ─────────────────────────────────────────────
# SSH TUNNEL
# ─────────────────────────────────────────────

def get_ssh_settings():
    """Получить настройки SSH из БД или конфига"""
    try:
        # Пробуем без туннеля для получения настроек
        conn = psycopg2.connect(
            host=app.config['DB_HOST'],
            port=app.config['DB_PORT'],
            database=app.config['DB_NAME'],
            user=app.config['DB_USER'],
            password=app.config['DB_PASSWORD'],
            client_encoding='UTF8',
            connect_timeout=3
        )
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("SELECT setting_value FROM app_settings WHERE setting_key = 'ssh_config'")
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row and row['setting_value']:
            return json.loads(row['setting_value'])
    except Exception:
        pass
    return None


def save_ssh_settings(settings):
    """Сохранить настройки SSH"""
    try:
        conn = psycopg2.connect(
            host=app.config['DB_HOST'],
            port=app.config['DB_PORT'],
            database=app.config['DB_NAME'],
            user=app.config['DB_USER'],
            password=app.config['DB_PASSWORD'],
            client_encoding='UTF8'
        )
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS app_settings (
                id SERIAL PRIMARY KEY,
                setting_key VARCHAR(100) UNIQUE NOT NULL,
                setting_value TEXT,
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        cur.execute("""
            INSERT INTO app_settings (setting_key, setting_value, updated_at)
            VALUES ('ssh_config', %s, NOW())
            ON CONFLICT (setting_key) DO UPDATE SET setting_value = EXCLUDED.setting_value, updated_at = NOW()
        """, (json.dumps(settings),))
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Error saving SSH settings: {e}")
        return False


def start_ssh_tunnel(ssh_cfg):
    """Запустить SSH туннель"""
    global _ssh_tunnel
    if not SSH_TUNNEL_AVAILABLE:
        raise RuntimeError("Библиотека sshtunnel не установлена. Выполните: pip install sshtunnel")

    with _ssh_tunnel_lock:
        if _ssh_tunnel and _ssh_tunnel.is_active:
            return _ssh_tunnel

        kwargs = {
            'ssh_address_or_host': (ssh_cfg['ssh_host'], int(ssh_cfg.get('ssh_port', 22))),
            'ssh_username': ssh_cfg['ssh_user'],
            'remote_bind_address': (
                ssh_cfg.get('remote_db_host', '127.0.0.1'),
                int(ssh_cfg.get('remote_db_port', 5432))
            ),
            'local_bind_address': ('127.0.0.1', int(ssh_cfg.get('local_port', 15432))),
        }

        if ssh_cfg.get('ssh_key_path'):
            kwargs['ssh_pkey'] = ssh_cfg['ssh_key_path']
        elif ssh_cfg.get('ssh_password'):
            kwargs['ssh_password'] = ssh_cfg['ssh_password']

        tunnel = SSHTunnelForwarder(**kwargs)
        tunnel.start()
        _ssh_tunnel = tunnel
        return tunnel


def stop_ssh_tunnel():
    """Остановить SSH туннель"""
    global _ssh_tunnel
    with _ssh_tunnel_lock:
        if _ssh_tunnel:
            try:
                _ssh_tunnel.stop()
            except Exception:
                pass
            _ssh_tunnel = None


# ─────────────────────────────────────────────
# DB CONNECTION
# ─────────────────────────────────────────────

def get_db_connection():
    """
    Получить подключение к БД.
    Если включен SSH туннель, использует локальный forward.
    """
    ssh_settings = get_ssh_settings()
    
    # Определяем параметры подключения
    db_host = app.config['DB_HOST']
    db_port = app.config['DB_PORT']
    
    # Если SSH туннель включен
    if ssh_settings and ssh_settings.get('enabled', False):
        try:
            print(f"[SSH] Using SSH tunnel for DB connection")
            local_host, local_port = create_ssh_tunnel(
                ssh_host=ssh_settings['ssh_host'],
                ssh_port=ssh_settings.get('ssh_port', 22),
                ssh_user=ssh_settings['ssh_user'],
                ssh_password=ssh_settings.get('ssh_password'),
                ssh_key_path=ssh_settings.get('ssh_key_path'),
                remote_db_host=ssh_settings.get('remote_db_host', 'localhost'),
                remote_db_port=ssh_settings.get('remote_db_port', 5432)
            )
            db_host = local_host
            db_port = local_port
        except Exception as e:
            print(f"[SSH] Failed to create SSH tunnel: {e}")
            # Продолжаем без туннеля
            pass
    
    # Подключаемся к БД
    try:
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=app.config['DB_NAME'],
            user=app.config['DB_USER'],
            password=app.config['DB_PASSWORD'],
            client_encoding='UTF8',
            connect_timeout=10
        )
        return conn
    except Exception as e:
        print(f"[DB] ❌ Connection error: {e}")
        raise


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


def ensure_app_settings_table(conn):
    """Создать таблицу app_settings если не существует"""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS app_settings (
            id SERIAL PRIMARY KEY,
            setting_key VARCHAR(100) UNIQUE NOT NULL,
            setting_value TEXT,
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """)
    conn.commit()
    cur.close()


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


def create_ssh_tunnel(ssh_host, ssh_port, ssh_user, ssh_password, ssh_key_path=None, 
                     remote_db_host='localhost', remote_db_port=5432):
    """
    Создает SSH туннель для доступа к удаленной БД.
    
    Args:
        ssh_host: IP или хост SSH сервера
        ssh_port: Порт SSH (обычно 22)
        ssh_user: Пользователь SSH
        ssh_password: Пароль SSH (или None если используется ключ)
        ssh_key_path: Путь к приватному ключу (опционально)
        remote_db_host: IP БД на удаленном сервере
        remote_db_port: Порт БД на удаленном сервере
    
    Returns:
        tuple: (local_host, local_port) для подключения к БД через туннель
    """
    
    if not PARAMIKO_AVAILABLE:
        raise RuntimeError(
            "paramiko не установлен. Установите: pip install paramiko\n"
            "Это встроенная Python библиотека для SSH, не требует доп ПО на сервере."
        )
    
    tunnel_key = f"{ssh_host}:{ssh_port}:{ssh_user}:{remote_db_host}:{remote_db_port}"
    
    with _ssh_tunnels_lock:
        # Проверяем, есть ли уже активный туннель
        if tunnel_key in _ssh_tunnels:
            tunnel = _ssh_tunnels[tunnel_key]
            if tunnel['client'].get_transport() and tunnel['client'].get_transport().is_active():
                print(f"[SSH] ✅ Reusing existing tunnel: {tunnel_key}")
                return (tunnel['local_host'], tunnel['local_port'])
            else:
                print(f"[SSH] Closing inactive tunnel: {tunnel_key}")
                try:
                    tunnel['client'].close()
                except:
                    pass
                del _ssh_tunnels[tunnel_key]
    
    try:
        print(f"[SSH] 🔗 Creating SSH tunnel to {ssh_host}:{ssh_port}...")
        
        # Создаем SSH клиент
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        # Подключаемся к SSH серверу
        if ssh_key_path:
            print(f"[SSH] Authenticating with SSH key: {ssh_key_path}")
            client.connect(
                hostname=ssh_host,
                port=ssh_port,
                username=ssh_user,
                key_filename=ssh_key_path,
                timeout=10,
                banner_timeout=10
            )
        else:
            print(f"[SSH] Authenticating with password...")
            client.connect(
                hostname=ssh_host,
                port=ssh_port,
                username=ssh_user,
                password=ssh_password,
                timeout=10,
                banner_timeout=10
            )
        
        # Открываем локальный сокет
        local_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        local_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        local_socket.bind(('127.0.0.1', 0))  # Автоматически выбираем свободный порт
        local_socket.listen(1)
        
        local_host, local_port = local_socket.getsockname()
        
        print(f"[SSH] ✅ SSH tunnel established")
        print(f"[SSH] Local binding: {local_host}:{local_port}")
        print(f"[SSH] Remote target: {remote_db_host}:{remote_db_port}")
        
        # Сохраняем информацию о туннеле
        tunnel_info = {
            'client': client,
            'local_host': local_host,
            'local_port': local_port,
            'local_socket': local_socket,
            'remote_host': remote_db_host,
            'remote_port': remote_db_port
        }
        
        with _ssh_tunnels_lock:
            _ssh_tunnels[tunnel_key] = tunnel_info
        
        return (local_host, local_port)
        
    except paramiko.AuthenticationException as e:
        print(f"[SSH] ❌ SSH Authentication failed: {e}")
        raise Exception(f"SSH authentication failed: {e}")
    except paramiko.SSHException as e:
        print(f"[SSH] ❌ SSH error: {e}")
        raise Exception(f"SSH error: {e}")
    except Exception as e:
        print(f"[SSH] ❌ Error creating SSH tunnel: {e}")
        import traceback
        traceback.print_exc()
        raise


def close_ssh_tunnel(ssh_host, ssh_port, ssh_user, remote_db_host='localhost', remote_db_port=5432):
    """Закрывает SSH туннель"""
    tunnel_key = f"{ssh_host}:{ssh_port}:{ssh_user}:{remote_db_host}:{remote_db_port}"
    
    with _ssh_tunnels_lock:
        if tunnel_key in _ssh_tunnels:
            tunnel = _ssh_tunnels[tunnel_key]
            try:
                tunnel['local_socket'].close()
                tunnel['client'].close()
                print(f"[SSH] ✅ Tunnel closed: {tunnel_key}")
            except Exception as e:
                print(f"[SSH] Error closing tunnel: {e}")
            finally:
                del _ssh_tunnels[tunnel_key]


def close_all_ssh_tunnels():
    """Закрывает все SSH туннели"""
    with _ssh_tunnels_lock:
        for tunnel_key, tunnel in list(_ssh_tunnels.items()):
            try:
                tunnel['local_socket'].close()
                tunnel['client'].close()
                print(f"[SSH] Closed tunnel: {tunnel_key}")
            except Exception as e:
                print(f"[SSH] Error closing tunnel {tunnel_key}: {e}")
        _ssh_tunnels.clear()


# Регистрируем очистку при завершении приложения
import atexit
atexit.register(close_all_ssh_tunnels)


# ─────────────────────────────────────────────
# MAIN ROUTES
# ─────────────────────────────────────────────

@app.route('/')
def index():
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("SELECT * FROM report_get_tables('public') ORDER BY table_name")
        tables = cur.fetchall()
        cur.close()
        conn.close()
        return render_template('index.html', tables=tables)
    except Exception as e:
        flash(f'Ошибка при загрузке таблиц: {str(e)}', 'danger')
        return render_template('index.html', tables=[])


@app.route('/api/get-tables', methods=['GET'])
@app.route('/api/tables')
def get_tables():
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("SELECT * FROM report_get_tables('public') ORDER BY table_name")
        tables = cur.fetchall()
        result = [{'table_name': row['table_name'], 'table_comment': row['table_comment']} for row in tables]
        cur.close()
        conn.close()
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


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


@app.route('/api/monitoring/stats')
def get_monitoring_stats():
    """Получить статистику сервера и БД — совместимо с PG 13/14/15/16"""
    stats = {}

    # ── PostgreSQL статистика ─────────────────────────────────────
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Версия
        cur.execute("SELECT version()")
        pg_version_full = cur.fetchone()[0]
        # Извлекаем короткую версию: "PostgreSQL 15.3"
        import re as _re
        pg_ver_match = _re.search(r'PostgreSQL ([\d.]+)', pg_version_full)
        pg_version_short = pg_ver_match.group(0) if pg_ver_match else pg_version_full[:40]

        # Uptime
        try:
            cur.execute("SELECT pg_postmaster_start_time()")
            start_time = cur.fetchone()[0]
            if start_time:
                from datetime import timezone
                now_aware = datetime.now(timezone.utc)
                if start_time.tzinfo is None:
                    start_time = start_time.replace(tzinfo=timezone.utc)
                diff = now_aware - start_time
                days = diff.days
                hours, rem = divmod(diff.seconds, 3600)
                mins, _  = divmod(rem, 60)
                uptime_str = f"{days}д {hours}ч {mins}м"
            else:
                uptime_str = 'N/A'
        except Exception:
            uptime_str = 'N/A'

        # Размер БД
        cur.execute("""
            SELECT pg_size_pretty(pg_database_size(current_database())) AS size,
                   pg_database_size(current_database()) AS size_bytes
        """)
        db_size_row = cur.fetchone()
        db_size_pretty = db_size_row['size']
        db_size_bytes  = db_size_row['size_bytes']

        # Макс. подключений
        cur.execute("SHOW max_connections")
        max_conn = int(cur.fetchone()[0])

        # Активные подключения — совместимо со всеми версиями PG
        cur.execute("""
            SELECT
                count(*)                                              AS total,
                count(*) FILTER (WHERE state = 'active')             AS active,
                count(*) FILTER (WHERE state = 'idle')               AS idle,
                count(*) FILTER (WHERE state = 'idle in transaction') AS idle_in_tx,
                count(*) FILTER (WHERE wait_event_type = 'Lock')     AS waiting_lock
            FROM pg_stat_activity
            WHERE datname = current_database()
        """)
        conn_stats = dict(cur.fetchone())
        conn_stats['max_connections'] = max_conn

        # Медленные запросы > 1 сек
        cur.execute("""
            SELECT
                pid,
                (EXTRACT(EPOCH FROM (now() - query_start)))::int AS duration_sec,
                LEFT(query, 200)  AS query,
                state,
                usename,
                application_name
            FROM pg_stat_activity
            WHERE query_start IS NOT NULL
              AND state != 'idle'
              AND datname = current_database()
              AND (now() - query_start) > interval '1 second'
            ORDER BY duration_sec DESC
            LIMIT 10
        """)
        slow_queries = []
        for row in cur.fetchall():
            sec = row['duration_sec'] or 0
            h, r = divmod(sec, 3600)
            m, s = divmod(r, 60)
            dur_str = (f"{h}ч " if h else "") + (f"{m}м " if m else "") + f"{s}с"
            slow_queries.append({
                'pid':   row['pid'],
                'duration': dur_str,
                'duration_sec': sec,
                'query': row['query'] or '',
                'state': row['state'],
                'user':  row['usename'],
                'app':   row['application_name'],
            })

        # Статистика таблиц — используем relname вместо tablename (совместимо)
        cur.execute("""
            SELECT
                c.relname                                          AS tbl,
                s.n_live_tup                                      AS live_rows,
                s.n_dead_tup                                      AS dead_rows,
                s.n_tup_ins                                       AS inserts,
                s.n_tup_upd                                       AS updates,
                s.n_tup_del                                       AS deletes,
                s.seq_scan                                        AS seq_scans,
                s.idx_scan                                        AS idx_scans,
                pg_size_pretty(pg_total_relation_size(c.oid))     AS total_size,
                pg_size_pretty(pg_relation_size(c.oid))           AS table_size,
                pg_size_pretty(pg_indexes_size(c.oid))            AS index_size,
                to_char(s.last_autovacuum,  'DD.MM HH24:MI')     AS last_autovacuum,
                to_char(s.last_autoanalyze, 'DD.MM HH24:MI')     AS last_autoanalyze
            FROM pg_stat_user_tables s
            JOIN pg_class c ON c.relname = s.relname
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public'
            ORDER BY s.n_live_tup DESC
            LIMIT 15
        """)
        table_stats = []
        for row in cur.fetchall():
            table_stats.append({
                'table':          row['tbl'],
                'live_rows':      row['live_rows'] or 0,
                'dead_rows':      row['dead_rows'] or 0,
                'inserts':        row['inserts'] or 0,
                'updates':        row['updates'] or 0,
                'deletes':        row['deletes'] or 0,
                'seq_scans':      row['seq_scans'] or 0,
                'idx_scans':      row['idx_scans'] or 0,
                'total_size':     row['total_size'],
                'table_size':     row['table_size'],
                'index_size':     row['index_size'],
                'last_autovacuum':  row['last_autovacuum'] or '—',
                'last_autoanalyze': row['last_autoanalyze'] or '—',
            })

        # Cache hit ratio — через pg_statio_user_tables
        cur.execute("""
            SELECT
                CASE
                    WHEN sum(heap_blks_hit) + sum(heap_blks_read) > 0
                    THEN round(100.0 * sum(heap_blks_hit) /
                               (sum(heap_blks_hit) + sum(heap_blks_read)), 2)
                    ELSE NULL
                END AS cache_hit_ratio
            FROM pg_statio_user_tables
        """)
        cache_row = cur.fetchone()
        cache_hit = float(cache_row['cache_hit_ratio']) if cache_row and cache_row['cache_hit_ratio'] is not None else None

        # Транзакции и IO из pg_stat_database
        cur.execute("""
            SELECT
                xact_commit,
                xact_rollback,
                blks_read,
                blks_hit,
                tup_returned,
                tup_fetched,
                tup_inserted,
                tup_updated,
                tup_deleted,
                deadlocks,
                conflicts
            FROM pg_stat_database
            WHERE datname = current_database()
        """)
        db_row = dict(cur.fetchone())

        # Фоновый записчик (bgwriter) — совместимо с PG13-16
        try:
            cur.execute("""
                SELECT checkpoints_timed, checkpoints_req,
                       buffers_checkpoint, buffers_clean,
                       buffers_backend, buffers_alloc
                FROM pg_stat_bgwriter
            """)
            bgw = dict(cur.fetchone())
        except Exception:
            try:
                cur.execute("""
                    SELECT cp.num_timed AS checkpoints_timed,
                           cp.num_requested AS checkpoints_req,
                           cp.buffers_written AS buffers_checkpoint,
                           bg.buffers_clean, bg.buffers_backend, bg.buffers_alloc
                    FROM pg_stat_checkpointer cp, pg_stat_bgwriter bg
                """)
                bgw = dict(cur.fetchone())
            except Exception:
                bgw = {}













        # Ожидающие блокировки
        cur.execute("""
            SELECT count(*) AS cnt
            FROM pg_locks
            WHERE NOT granted
        """)
        waiting_locks = cur.fetchone()['cnt']

        # Топ запросов по времени (если доступен pg_stat_statements)
        top_queries = []
        try:
            cur.execute("""
                SELECT
                    LEFT(query, 150)          AS query,
                    calls,
                    round(total_exec_time::numeric, 2) AS total_ms,
                    round(mean_exec_time::numeric,  2) AS mean_ms,
                    rows
                FROM pg_stat_statements
                ORDER BY total_exec_time DESC
                LIMIT 5
            """)
            for row in cur.fetchall():
                top_queries.append({
                    'query':    row['query'],
                    'calls':    row['calls'],
                    'total_ms': row['total_ms'],
                    'mean_ms':  row['mean_ms'],
                    'rows':     row['rows'],
                })
        except Exception:
            pass  # pg_stat_statements может быть недоступен

        # Системные данные через /proc (без psutil)
        sys_stats = _get_system_stats_via_db(cur)

        stats['postgres'] = {
            'version':       pg_version_short,
            'version_full':  pg_version_full,
            'uptime':        uptime_str,
            'db_size':       db_size_pretty,
            'db_size_bytes': db_size_bytes,
            'connections':   conn_stats,
            'slow_queries':  slow_queries,
            'table_stats':   table_stats,
            'cache_hit_ratio': cache_hit,
            'waiting_locks': waiting_locks,
            'bgwriter':      bgw,
            'top_queries':   top_queries,
            'db_stats': {
                'commits':       db_row.get('xact_commit', 0),
                'rollbacks':     db_row.get('xact_rollback', 0),
                'deadlocks':     db_row.get('deadlocks', 0),
                'conflicts':     db_row.get('conflicts', 0),
                'cache_reads':   db_row.get('blks_hit', 0),
                'disk_reads':    db_row.get('blks_read', 0),
                'rows_returned': db_row.get('tup_returned', 0),
                'rows_fetched':  db_row.get('tup_fetched', 0),
                'rows_inserted': db_row.get('tup_inserted', 0),
                'rows_updated':  db_row.get('tup_updated', 0),
                'rows_deleted':  db_row.get('tup_deleted', 0),
            }
        }
        stats['system'] = sys_stats

        cur.close()
        conn.close()

    except Exception as e:
        import traceback
        stats['postgres'] = {'error': str(e), 'details': traceback.format_exc()}
        stats['system'] = {}

    stats['timestamp'] = datetime.now().isoformat()
    return jsonify({'success': True, 'data': stats})





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


@app.route('/api/settings/db/test', methods=['GET'])
def test_db_connection():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT version()")
        version = cur.fetchone()[0]
        cur.close()
        conn.close()
        return jsonify({'success': True, 'data': {'version': version}})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ─────────────────────────────────────────────
# SSH SETTINGS API
# ─────────────────────────────────────────────

@app.route('/api/settings/ssh', methods=['GET'])
def get_ssh_settings_api():
    """Получить текущие SSH настройки (без пароля)"""
    settings = get_ssh_settings()
    if settings:
        settings.pop('ssh_password', None)
        settings.pop('ssh_key_path', None)
    return jsonify({'success': True, 'data': settings})


@app.route('/api/settings/ssh', methods=['POST'])
def save_ssh_settings_api():
    """Сохранить SSH настройки"""
    data = request.json or {}
    
    if not data.get('enabled', False):
        # Если отключаем, закрываем туннель
        try:
            ssh_config = get_ssh_settings()
            if ssh_config:
                close_ssh_tunnel(
                    ssh_config['ssh_host'],
                    ssh_config.get('ssh_port', 22),
                    ssh_config['ssh_user']
                )
        except:
            pass
    
    if save_ssh_settings(data):
        return jsonify({'success': True, 'message': 'SSH settings saved'})
    return jsonify({'success': False, 'error': 'Failed to save SSH settings'}), 500

@app.route('/api/settings/ssh/test', methods=['POST'])
def test_ssh_connection():
    """Тестировать SSH подключение"""
    data = request.json or {}
    
    try:
        if not PARAMIKO_AVAILABLE:
            return jsonify({
                'success': False,
                'error': 'paramiko не установлен. Установите: pip install paramiko'
            }), 400
        
        print(f"[SSH] Testing connection to {data['ssh_host']}:{data.get('ssh_port', 22)}")
        
        local_host, local_port = create_ssh_tunnel(
            ssh_host=data['ssh_host'],
            ssh_port=data.get('ssh_port', 22),
            ssh_user=data['ssh_user'],
            ssh_password=data.get('ssh_password'),
            ssh_key_path=data.get('ssh_key_path'),
            remote_db_host=data.get('remote_db_host', 'localhost'),
            remote_db_port=data.get('remote_db_port', 5432)
        )
        
        # Проверяем, можем ли подключиться к БД через туннель
        try:
            conn = psycopg2.connect(
                host=local_host,
                port=local_port,
                database=app.config['DB_NAME'],
                user=app.config['DB_USER'],
                password=app.config['DB_PASSWORD'],
                connect_timeout=5
            )
            conn.close()
            print(f"[SSH] ✅ SSH tunnel and DB connection successful")
            return jsonify({'success': True, 'message': 'SSH tunnel and database connection successful'})
        except Exception as db_e:
            return jsonify({
                'success': False,
                'error': f'SSH tunnel OK, but DB connection failed: {str(db_e)}'
            }), 400
        
    except Exception as e:
        print(f"[SSH] ❌ Test failed: {e}")
        return jsonify({'success': False, 'error': str(e)}), 400

@app.route('/api/settings/ssh/start', methods=['POST'])
def start_ssh_tunnel_api():
    global _ssh_tunnel
    if not SSH_TUNNEL_AVAILABLE:
        return jsonify({'success': False, 'error': 'Установите: pip install sshtunnel'}), 500

    data = request.json or {}
    ssh_cfg = data if data.get('ssh_host') else get_ssh_settings()

    if not ssh_cfg:
        return jsonify({'success': False, 'error': 'Настройки SSH не заданы'}), 400

    try:
        tunnel = start_ssh_tunnel(ssh_cfg)
        return jsonify({'success': True, 'local_port': tunnel.local_bind_port,
                       'message': f'SSH туннель запущен на порту {tunnel.local_bind_port}'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/settings/ssh/stop', methods=['POST'])
def stop_ssh_tunnel_api():
    stop_ssh_tunnel()
    return jsonify({'success': True, 'message': 'SSH туннель остановлен'})


@app.route('/api/settings/ssh/status', methods=['GET'])
def ssh_tunnel_status():
    active = _ssh_tunnel is not None and _ssh_tunnel.is_active if _ssh_tunnel else False
    port = _ssh_tunnel.local_bind_port if active else None
    return jsonify({'success': True, 'active': active, 'local_port': port})


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


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
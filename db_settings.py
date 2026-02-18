"""
Database settings management.
Отвечает за получение и сохранение настроек SSH и почты в базе данных.
"""

import psycopg2
import psycopg2.extras
import json


def get_ssh_settings(config=None):
    """
    Получить настройки SSH из БД или конфига.
    
    Args:
        config: объект конфигурации приложения (app.config). Если не передан, использует глобальный импорт.
    
    Returns:
        dict: настройки SSH или None
    """
    # Если config не передан, получаем его динамически
    if config is None:
        from flask import current_app
        try:
            config = current_app.config
        except:
            return None
    
    # Сначала пытаемся получить из базы данных
    settings = None
    try:
        # Пробуем без туннеля для получения настроек
        conn = psycopg2.connect(
            host=config['DB_HOST'],
            port=config['DB_PORT'],
            database=config['DB_NAME'],
            user=config['DB_USER'],
            password=config['DB_PASSWORD'],
            client_encoding='UTF8',
            connect_timeout=3
        )
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("SELECT setting_value FROM app_settings WHERE setting_key = 'ssh_config'")
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row and row['setting_value']:
            settings = json.loads(row['setting_value'])
    except Exception:
        pass
    
    # Если нет настроек в БД, используем конфиг
    if not settings:
        if config.get('SSH_HOST'):
            settings = {
                'enabled': config.get('SSH_ENABLED', False),
                'ssh_host': config.get('SSH_HOST', ''),
                'ssh_port': config.get('SSH_PORT', 22),
                'ssh_user': config.get('SSH_USER', ''),
                'ssh_password': config.get('SSH_PASSWORD', ''),
                'ssh_key_path': config.get('SSH_KEY_PATH', ''),
                'remote_db_host': config.get('SSH_REMOTE_DB_HOST', '127.0.0.1'),
                'remote_db_port': config.get('SSH_REMOTE_DB_PORT', 5432),
                'local_port': config.get('SSH_LOCAL_PORT', 15432)
            }
    
    return settings


def save_ssh_settings(settings, config=None):
    """
    Сохранить настройки SSH в базе данных.
    
    Args:
        settings: словарь с настройками SSH
        config: объект конфигурации приложения. Если не передан, использует глобальный импорт.
    
    Returns:
        bool: True если успешно, False в случае ошибки
    """
    if config is None:
        from flask import current_app
        try:
            config = current_app.config
        except:
            return False
    
    try:
        conn = psycopg2.connect(
            host=config['DB_HOST'],
            port=config['DB_PORT'],
            database=config['DB_NAME'],
            user=config['DB_USER'],
            password=config['DB_PASSWORD'],
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


def get_mail_settings(config=None):
    """
    Получить настройки почты из БД или конфига.
    
    Args:
        config: объект конфигурации приложения. Если не передан, использует глобальный импорт.
    
    Returns:
        dict: настройки почты или None
    """
    if config is None:
        from flask import current_app
        try:
            config = current_app.config
        except:
            return None
    
    settings = None
    try:
        conn = psycopg2.connect(
            host=config['DB_HOST'],
            port=config['DB_PORT'],
            database=config['DB_NAME'],
            user=config['DB_USER'],
            password=config['DB_PASSWORD'],
            client_encoding='UTF8',
            connect_timeout=3
        )
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("SELECT setting_value FROM app_settings WHERE setting_key = 'mail_config'")
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row and row['setting_value']:
            settings = json.loads(row['setting_value'])
    except Exception:
        pass
    
    # Если нет настроек в БД, используем конфиг
    if not settings:
        if config.get('SMTP_HOST'):
            settings = {
                'smtp_host': config.get('SMTP_HOST', ''),
                'smtp_port': config.get('SMTP_PORT', 587),
                'smtp_user': config.get('SMTP_USER', ''),
                'smtp_password': config.get('SMTP_PASSWORD', ''),
                'smtp_tls': config.get('SMTP_TLS', True),
                'from_name': config.get('SMTP_FROM_NAME', 'Report Builder')
            }
    
    return settings


def save_mail_settings(settings, config=None):
    """
    Сохранить настройки почты в базе данных.
    
    Args:
        settings: словарь с настройками почты
        config: объект конфигурации приложения. Если не передан, использует глобальный импорт.
    
    Returns:
        bool: True если успешно, False в случае ошибки
    """
    if config is None:
        from flask import current_app
        try:
            config = current_app.config
        except:
            return False
    
    try:
        conn = psycopg2.connect(
            host=config['DB_HOST'],
            port=config['DB_PORT'],
            database=config['DB_NAME'],
            user=config['DB_USER'],
            password=config['DB_PASSWORD'],
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


def ensure_app_settings_table(config=None):
    """
    Создать таблицу app_settings если не существует.
    
    Args:
        config: объект конфигурации приложения. Если не передан, использует глобальный импорт.
    """
    if config is None:
        from flask import current_app
        try:
            config = current_app.config
        except:
            return
    
    try:
        conn = psycopg2.connect(
            host=config['DB_HOST'],
            port=config['DB_PORT'],
            database=config['DB_NAME'],
            user=config['DB_USER'],
            password=config['DB_PASSWORD'],
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
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error creating app_settings table: {e}")

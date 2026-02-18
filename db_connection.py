"""
Database connection and SSH tunnel utilities.

Этот модуль отвечает за подключение к базе данных и управление SSH туннелем.
"""

import psycopg2
import psycopg2.extras
import json
import socket
import threading
from threading import Lock
import paramiko

try:
    import paramiko
    PARAMIKO_AVAILABLE = True
except ImportError:
    PARAMIKO_AVAILABLE = False


# Глобальное хранилище SSH туннелей
_ssh_tunnels = {}
_ssh_tunnels_lock = Lock()


def get_ssh_tunnels():
    """Возвращает словарь активных туннелей (для внешнего использования)"""
    return _ssh_tunnels


def get_ssh_tunnels_lock():
    """Возвращает блокировку для туннелей"""
    return _ssh_tunnels_lock


def is_paramiko_available():
    """Проверяет доступность библиотеки paramiko"""
    return PARAMIKO_AVAILABLE


# ─────────────────────────────────────────────
# SSH TUNNEL
# ─────────────────────────────────────────────

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
                    return (tunnel['local_host'], tunnel['local_port'])
            except:
                pass
    return None


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
        
        # Транспорт для туннелирования
        transport = client.get_transport()
        if transport is None:
            raise Exception("Failed to get SSH transport")
        
        # Создаем listener на локальной стороне
        local_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        local_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        local_socket.bind(('127.0.0.1', 0))
        local_socket.listen(5)
        
        local_host, local_port = local_socket.getsockname()
        
        print(f"[SSH] ✅ SSH tunnel established")
        print(f"[SSH] Local binding: {local_host}:{local_port}")
        print(f"[SSH] Remote target: {remote_db_host}:{remote_db_port}")
        
        # Функция для обработки входящих соединений
        def handle_connection():
            while True:
                try:
                    client_socket, addr = local_socket.accept()
                    
                    # Открываем канал к удаленному серверу
                    try:
                        tunnel_channel = transport.open_channel(
                            "direct-tcpip",
                            (remote_db_host, remote_db_port),
                            (local_host, local_port)
                        )
                        
                        # Перенаправляем данные в обе стороны
                        def forward(src, dst):
                            try:
                                while True:
                                    data = src.recv(1024)
                                    if not data:
                                        break
                                    dst.send(data)
                            except:
                                pass
                            finally:
                                try:
                                    src.close()
                                except:
                                    pass
                                try:
                                    dst.close()
                                except:
                                    pass
                        
                        t1 = threading.Thread(target=forward, args=(client_socket, tunnel_channel))
                        t2 = threading.Thread(target=forward, args=(tunnel_channel, client_socket))
                        t1.daemon = True
                        t2.daemon = True
                        t1.start()
                        t2.start()
                        
                    except Exception as e:
                        print(f"[SSH] Error opening channel: {e}")
                        client_socket.close()
                        
                except Exception as e:
                    print(f"[SSH] Error accepting connection: {e}")
                    break
        
        # Запускаем обработчик в отдельном потоке
        handler_thread = threading.Thread(target=handle_connection, daemon=True)
        handler_thread.start()
        
        # Сохраняем информацию о туннеле
        tunnel_info = {
            'client': client,
            'transport': transport,
            'local_host': local_host,
            'local_port': local_port,
            'local_socket': local_socket,
            'handler_thread': handler_thread,
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
        for key in list(_ssh_tunnels.keys()):
            tunnel = _ssh_tunnels[key]
            try:
                tunnel['local_socket'].close()
                tunnel['client'].close()
            except:
                pass
        _ssh_tunnels.clear()
        print("[SSH] All tunnels closed")


def is_tunnel_active(ssh_host, ssh_port, ssh_user, remote_db_host, remote_db_port):
    """Проверяет, активен ли туннель"""
    tunnel_key = f"{ssh_host}:{ssh_port}:{ssh_user}:{remote_db_host}:{remote_db_port}"
    
    with _ssh_tunnels_lock:
        if tunnel_key in _ssh_tunnels:
            tunnel = _ssh_tunnels[tunnel_key]
            try:
                return tunnel['client'].get_transport() and tunnel['client'].get_transport().is_active()
            except:
                pass
    return False


def get_tunnel_info(ssh_host, ssh_port, ssh_user, remote_db_host, remote_db_port):
    """Возвращает информацию о туннеле"""
    tunnel_key = f"{ssh_host}:{ssh_port}:{ssh_user}:{remote_db_host}:{remote_db_port}"
    
    with _ssh_tunnels_lock:
        if tunnel_key in _ssh_tunnels:
            tunnel = _ssh_tunnels[tunnel_key]
            try:
                if tunnel['client'].get_transport() and tunnel['client'].get_transport().is_active():
                    return {
                        'active': True,
                        'local_host': tunnel['local_host'],
                        'local_port': tunnel['local_port']
                    }
            except:
                pass
    return None


# ─────────────────────────────────────────────
# DATABASE CONNECTION
# ─────────────────────────────────────────────

def get_db_connection(config=None, ssh_settings=None):
    """
    Получить подключение к БД.
    Если передан ssh_settings и включен туннель, использует локальный forward.
    
    Args:
        config: объект конфигурации приложения (app.config). Если не передан, использует current_app.
        ssh_settings: словарь с настройками SSH (опционально)
    
    Returns:
        psycopg2.connection: объект подключения к БД
    """
    # Если config не передан, получаем его динамически
    if config is None:
        from flask import current_app
        try:
            config = current_app.config
        except:
            raise Exception("Cannot get app config - app not in request context")
    
    from db_settings import get_ssh_settings
    
    # Если ssh_settings не передан, получаем из БД/конфига
    if ssh_settings is None:
        ssh_settings = get_ssh_settings(config)
    
    # Определяем параметры подключения
    db_host = config['DB_HOST']
    db_port = config['DB_PORT']
    
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
            database=config['DB_NAME'],
            user=config['DB_USER'],
            password=config['DB_PASSWORD'],
            client_encoding='UTF8',
            connect_timeout=10
        )
        return conn
    except Exception as e:
        print(f"[DB] ❌ Connection error: {e}")
        raise

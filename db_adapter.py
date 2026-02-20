"""
Универсальный адаптер для работы с PostgreSQL и Oracle
"""

import psycopg2
import psycopg2.extras

try:
    import oracledb
    ORACLE_AVAILABLE = True
except ImportError:
    ORACLE_AVAILABLE = False


class DatabaseAdapter:
    """Универсальный адаптер для работы с разными типами БД"""
    
    POSTGRES = 'postgresql'
    ORACLE = 'oracle'
    
    @staticmethod
    def is_oracle_available():
        """Проверка доступности библиотеки Oracle"""
        return ORACLE_AVAILABLE
    
    @staticmethod
    def connect(db_type, host, port, database, user, password, **kwargs):
        """
        Универсальное подключение к БД
        
        Args:
            db_type: тип БД ('postgresql' или 'oracle')
            host: хост
            port: порт
            database: имя БД для PostgreSQL / service_name для Oracle
            user: пользователь
            password: пароль
            **kwargs: дополнительные параметры
            
        Returns:
            connection: объект подключения
        """
        
        if db_type == DatabaseAdapter.POSTGRES:
            return psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password,
                client_encoding='UTF8',
                connect_timeout=kwargs.get('connect_timeout', 10)
            )
            
        elif db_type == DatabaseAdapter.ORACLE:
            if not ORACLE_AVAILABLE:
                raise RuntimeError(
                    "oracledb не установлен. Установите: pip install oracledb\n"
                    "Это официальная библиотека Oracle для Python."
                )
            
            # ✅ ИСПРАВЛЕНО: Поддержка service_name для Oracle
            # database передается как service_name (например: "ORCLCDB")
            try:
                dsn = oracledb.makedsn(
                    host=host,
                    port=port,
                    service_name=database  # Используем service_name вместо SID
                )
                
                print(f"[Oracle] Connecting with DSN: {dsn}")
                print(f"[Oracle] Service name: {database}")
                
                return oracledb.connect(
                    user=user,
                    password=password,
                    dsn=dsn
                )
            except Exception as e:
                # Если не получилось с service_name, пробуем как SID
                print(f"[Oracle] Failed with service_name, trying SID...")
                try:
                    dsn = oracledb.makedsn(
                        host=host,
                        port=port,
                        sid=database  # Пробуем как SID
                    )
                    print(f"[Oracle] Connecting with SID: {dsn}")
                    return oracledb.connect(
                        user=user,
                        password=password,
                        dsn=dsn
                    )
                except:
                    raise e  # Возвращаем исходную ошибку
        
        else:
            raise ValueError(f"Неизвестный тип БД: {db_type}")
    
    @staticmethod
    def test_connection(db_type, host, port, database, user, password):
        """
        Тестирование подключения к БД
        
        Returns:
            tuple: (success: bool, message: str, version: str)
        """
        
        try:
            conn = DatabaseAdapter.connect(db_type, host, port, database, user, password)
            
            # Получаем версию БД
            if db_type == DatabaseAdapter.POSTGRES:
                cur = conn.cursor()
                cur.execute("SELECT version()")
                version = cur.fetchone()[0]
                cur.close()
                
            elif db_type == DatabaseAdapter.ORACLE:
                cur = conn.cursor()
                cur.execute("SELECT banner FROM v$version WHERE rownum = 1")
                version = cur.fetchone()[0]
                cur.close()
            
            conn.close()
            
            return (True, f"Подключение успешно", version)
            
        except Exception as e:
            error_msg = str(e)
            # Делаем ошибку более понятной
            if "ORA-12514" in error_msg:
                error_msg = f"Не удалось подключиться к service_name '{database}'. Проверьте имя сервиса."
            elif "ORA-01017" in error_msg:
                error_msg = "Неверное имя пользователя или пароль"
            elif "ORA-12170" in error_msg or "ORA-12541" in error_msg:
                error_msg = f"Не удалось подключиться к {host}:{port}. Проверьте хост и порт."
                
            return (False, f"Ошибка подключения: {error_msg}", None)
    
    @staticmethod
    def get_cursor(conn, db_type):
        """
        Получить курсор с поддержкой словарей
        
        Args:
            conn: объект подключения
            db_type: тип БД
            
        Returns:
            cursor: курсор
        """
        
        if db_type == DatabaseAdapter.POSTGRES:
            return conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        elif db_type == DatabaseAdapter.ORACLE:
            cursor = conn.cursor()
            cursor.rowfactory = lambda *args: dict(zip([d[0] for d in cursor.description], args))
            return cursor
        else:
            raise ValueError(f"Неизвестный тип БД: {db_type}")
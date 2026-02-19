
import psycopg2
from sshtunnel import SSHTunnelForwarder
import sys

# Параметры SSH
SSH_HOST = "10.100.102.90"  # IP или адрес SSH сервера
SSH_PORT = 22
SSH_USERNAME = "nladmin"
SSH_PASSWORD = "D#f;mV@KbX"  # Или используй SSH ключ (см. ниже)
SSH_PKEY = None  # Путь к приватному ключу, например: "/home/user/.ssh/id_rsa"

# Параметры PostgreSQL
DB_HOST = "localhost"  # IP PostgreSQL на удаленном сервере (внутренний IP)
DB_PORT = 5432
DB_USER = "rul_report_user"
DB_PASSWORD = "1234567890!@#$%^&*()"
DB_NAME = "rul_jkh"


def connect_with_ssh_tunnel(use_key=False, key_path=None):
    """
    Подключается к PostgreSQL через SSH туннель
    use_key: True если используешь SSH ключ, False если пароль
    key_path: путь к приватному ключу (если use_key=True)
    """
    tunnel = None
    connection = None

    try:
        # Параметры туннеля
        tunnel_params = {
            "ssh_address_or_host": (SSH_HOST, SSH_PORT),
            "ssh_username": SSH_USERNAME,
            "remote_bind_address": (DB_HOST, DB_PORT),
            "local_bind_address": ('127.0.0.1', 6543),
            "allow_agent": False
        }

        # Добавляем аутентификацию
        if use_key and key_path:
            tunnel_params["ssh_pkey"] = key_path
        else:
            tunnel_params["ssh_password"] = SSH_PASSWORD

        # Создаем SSH туннель
        tunnel = SSHTunnelForwarder(**tunnel_params)

        # Запускаем туннель
        tunnel.start()
        print(f"✓ SSH туннель установлен: {tunnel.local_bind_address}")

        # Подключаемся к PostgreSQL через туннель
        connection = psycopg2.connect(
            host='127.0.0.1',
            port=tunnel.local_bind_port,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            connect_timeout=10
        )

        print("✓ Подключение к PostgreSQL установлено")

        # Проверяем подключение
        cursor = connection.cursor()
        cursor.execute("SELECT version();")
        db_version = cursor.fetchone()
        print(f"✓ База данных: {db_version[0]}")
        cursor.close()

        return connection, tunnel

    except Exception as e:
        print(f"✗ Ошибка подключения: {e}")
        if connection:
            connection.close()
        if tunnel:
            tunnel.stop()
        sys.exit(1)


def execute_query(connection, query):
    """
    Выполняет SQL запрос
    """
    try:
        cursor = connection.cursor()
        cursor.execute(query)

        # Если это SELECT запрос, получаем результаты
        if query.strip().upper().startswith('SELECT'):
            results = cursor.fetchall()
            cursor.close()
            return results
        else:
            # Для INSERT, UPDATE, DELETE коммитим изменения
            connection.commit()
            cursor.close()
            return f"Команда выполнена успешно"

    except Exception as e:
        connection.rollback()
        print(f"✗ Ошибка выполнения запроса: {e}")
        return None


def main():
    """
    Пример использования
    """
    # Подключаемся по паролю
    connection, tunnel = connect_with_ssh_tunnel(use_key=False)

    # Или подключаемся по SSH ключу:
    # connection, tunnel = connect_with_ssh_tunnel(use_key=True, key_path="/path/to/private/key")

    try:
        # Пример: получаем список таблиц
        query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        """
        tables = execute_query(connection, query)
        if tables:
            print("\nТаблицы в БД:")
            for table in tables:
                print(f"  - {table[0]}")

        # Здесь ты можешь выполнять свои запросы
        # result = execute_query(connection, "SELECT * FROM your_table LIMIT 5")

    finally:
        # Закрываем подключение
        if connection:
            connection.close()
            print("\n✓ Подключение к БД закрыто")
        if tunnel:
            tunnel.stop()
            print("✓ SSH туннель закрыт")


if __name__ == "__main__":
    main()
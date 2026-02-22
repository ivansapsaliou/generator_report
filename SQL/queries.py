"""
Сборник SQL запросов для мониторинга баз данных PostgreSQL и Oracle.

Этот файл содержит все SQL запросы, используемые в проекте для мониторинга,
с подробными описаниями их назначения и возвращаемых данных.

Структура файла:
1. PostgreSQL мониторинг
2. PostgreSQL сессии
3. Oracle мониторинг
4. SQLite мониторинг
5. Авторизация и пользователи
6. Отчёты
"""

# =============================================================================
# POSTGRESQL МОНИТОРИНГ
# =============================================================================

class PostgreSQLQueries:
    """
    SQL запросы для мониторинга PostgreSQL.
    Используются в db_monitoring.py -> _get_postgres_stats()
    """
    
    # -----------------------------------------------------------------------------
    # Версия PostgreSQL
    # -----------------------------------------------------------------------------
    VERSION = """
    SELECT version()
    """
    # Описание: Получает полную версию PostgreSQL сервера
    # Возвращает: Строку с версией (например: "PostgreSQL 14.5 ...")
    # Используется в: db_monitoring.py:37
    
    
    # -----------------------------------------------------------------------------
    # Uptime сервера
    # -----------------------------------------------------------------------------
    UPTIME = """
    SELECT pg_postmaster_start_time()
    """
    # Описание: Получает время запуска PostgreSQL сервера
    # Возвращает: Timestamp времени старта postmaster
    # Используется в: db_monitoring.py:44
    # Примечание: Используется для расчёта uptime сервера
    
    
    # -----------------------------------------------------------------------------
    # Размер базы данных
    # -----------------------------------------------------------------------------
    DATABASE_SIZE = """
    SELECT pg_size_pretty(pg_database_size(current_database())) AS size,
           pg_database_size(current_database()) AS size_bytes
    """
    # Описание: Получает размер текущей базы данных в человекочитаемом формате и байтах
    # Возвращает: 
    #   - size: Размер в формате "XX MB/GB" (для отображения)
    #   - size_bytes: Размер в байтах (для расчётов)
    # Используется в: db_monitoring.py:60-67
    
    
    # -----------------------------------------------------------------------------
    # Максимальное количество подключений
    # -----------------------------------------------------------------------------
    MAX_CONNECTIONS = """
    SHOW max_connections
    """
    # Описание: Получает максимально допустимое количество подключений
    # Возвращает: Число (например: 100)
    # Используется в: db_monitoring.py:70-71
    
    
    # -----------------------------------------------------------------------------
    # Статистика подключений
    # -----------------------------------------------------------------------------
    CONNECTIONS_STATS = """
    SELECT
        count(*)                                              AS total,
        count(*) FILTER (WHERE state = 'active')             AS active,
        count(*) FILTER (WHERE state = 'idle')               AS idle,
        count(*) FILTER (WHERE state = 'idle in transaction') AS idle_in_tx,
        count(*) FILTER (WHERE wait_event_type = 'Lock')     AS waiting_lock
    FROM pg_stat_activity
    WHERE datname = current_database()
    """
    # Описание: Получает статистику по текущим подключениям к БД
    # Возвращает:
    #   - total: Всего подключений
    #   - active: Активных (выполняют запрос)
    #   - idle: Ожидающих команды
    #   - idle_in_tx: Бездействующих в транзакции
    #   - waiting_lock: Заблокированных (ожидают освобождения ресурса)
    # Используется в: db_monitoring.py:73-91
    
    
    # -----------------------------------------------------------------------------
    # Статистика транзакций и операций БД
    # -----------------------------------------------------------------------------
    DATABASE_STATS = """
    SELECT 
        xact_commit AS commits,
        xact_rollback AS rollbacks,
        deadlocks,
        conflicts,
        tup_inserted AS rows_inserted,
        tup_updated AS rows_updated,
        tup_deleted AS rows_deleted,
        tup_fetched AS rows_fetched,
        blks_hit AS cache_reads,
        blks_read AS disk_reads
    FROM pg_stat_database
    WHERE datname = current_database()
    """
    # Описание: Получает общую статистику работы базы данных
    # Возвращает:
    #   - commits: Зафиксированные транзакции
    #   - rollbacks: Откаченные транзакции
    #   - deadlocks: Взаимоблокировки
    #   - conflicts: Конфликты репликации
    #   - rows_inserted/updated/deleted: Статистика DML операций
    #   - cache_reads/disk_reads: Чтение из кэша и с диска
    # Используется в: db_monitoring.py:95-123
    # Примечание: Используется для расчёта cache hit ratio
    
    
    # -----------------------------------------------------------------------------
    # Статистика bgwriter (фоновый процесс записи)
    # -----------------------------------------------------------------------------
    BGWRITER_STATS = """
    SELECT 
        checkpoints_timed,
        checkpoints_req,
        buffers_checkpoint,
        buffers_clean,
        buffers_backend,
        buffers_alloc
    FROM pg_stat_bgwriter
    """
    # Описание: Получает статистику фонового процесса записи
    # Возвращает:
    #   - checkpoints_timed: Запланированные контрольные точки
    #   - checkpoints_req: Принудительные контрольные точки
    #   - buffers_checkpoint: Буферы записанные при checkpoint
    #   - buffers_clean: Буферы записанные bgwriter
    #   - buffers_backend: Буферы записанные backend процессами
    #   - buffers_alloc: Всего выделено буферов
    # Используется в: db_monitoring.py:141-162
    
    
    # -----------------------------------------------------------------------------
    # Статистика таблиц
    # -----------------------------------------------------------------------------
    TABLE_STATS = """
    SELECT
        c.relname                                          AS tbl,
        s.n_live_tup                                      AS live_rows,
        s.n_dead_tup                                      AS dead_rows,
        pg_size_pretty(pg_total_relation_size(c.oid))     AS total_size,
        pg_size_pretty(pg_relation_size(c.oid))           AS table_size,
        pg_size_pretty(pg_indexes_size(c.oid))            AS index_size,
        s.n_tup_ins                                       AS inserts,
        s.n_tup_upd                                       AS updates,
        s.n_tup_del                                       AS deletes,
        s.seq_scan                                        AS seq_scans,
        s.idx_scan                                        AS idx_scans,
        to_char(s.last_autovacuum,  'DD.MM HH24:MI')     AS last_autovacuum,
        to_char(s.last_autoanalyze, 'DD.MM HH24:MI')     AS last_autoanalyze
    FROM pg_stat_user_tables s
    JOIN pg_class c ON c.relname = s.relname
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public'
    ORDER BY s.n_live_tup DESC
    LIMIT 15
    """
    # Описание: Получает статистику использования таблиц в схеме public
    # Возвращает:
    #   - tbl: Имя таблицы
    #   - live_rows: Количество живых строк
    #   - dead_rows: Количество мёртвых строк (мусор)
    #   - total_size: Общий размер (таблица + индексы)
    #   - table_size: Размер таблицы без индексов
    #   - index_size: Размер индексов
    #   - inserts/updates/deletes: Количество операций
    #   - seq_scans/idx_scans: Сканирования (последовательные/индексные)
    #   - last_autovacuum/autoanalyze: Последний запуск автоочистки
    # Используется в: db_monitoring.py:165-204
    # Примечание: Ограничено 15 таблицами с наибольшим количеством строк
    
    
    # -----------------------------------------------------------------------------
    # Медленные запросы
    # -----------------------------------------------------------------------------
    SLOW_QUERIES = """
    SELECT
        pid,
        (EXTRACT(EPOCH FROM (now() - query_start)))::int AS duration_sec,
        LEFT(query, 200) AS query,
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
    """
    # Описание: Получает текущие выполняющиеся запросы дольше 1 секунды
    # Возвращает:
    #   - pid: ID процесса
    #   - duration_sec: Длительность в секундах
    #   - query: Текст запроса (обрезан до 200 символов)
    #   - state: Состояние (active, idle in transaction и т.д.)
    #   - usename: Пользователь БД
    #   - application_name: Имя приложения
    # Используется в: db_monitoring.py:207-235
    
    
    # -----------------------------------------------------------------------------
    # Топ запросов по времени выполнения (требует pg_stat_statements)
    # -----------------------------------------------------------------------------
    TOP_QUERIES = """
    SELECT 
        LEFT(query, 150) AS query,
        calls,
        ROUND(total_exec_time::numeric, 2) AS total_ms,
        ROUND(mean_exec_time::numeric, 2) AS mean_ms,
        rows
    FROM pg_stat_statements
    ORDER BY total_exec_time DESC
    LIMIT 10
    """
    # Описание: Получает наиболее ресурсоёмкие запросы (суммарное время)
    # Требует: Расширение pg_stat_statements
    # Возвращает:
    #   - query: Текст запроса
    #   - calls: Количество вызовов
    #   - total_ms: Общее время выполнения (мс)
    #   - mean_ms: Среднее время выполнения (мс)
    #   - rows: Возвращено строк
    # Используется в: db_monitoring.py:238-262
    # Примечание: Если расширение не установлено, возвращает пустой список


# =============================================================================
# POSTGRESQL СЕССИИ
# =============================================================================

class PostgreSQLSessionQueries:
    """
    SQL запросы для управления сессиями PostgreSQL.
    Используются в pg_sessions.py
    """
    
    # -----------------------------------------------------------------------------
    # Активные сессии (без текущего подключения)
    # -----------------------------------------------------------------------------
    ACTIVE_SESSIONS = """
    SELECT 
        pid,
        usename,
        datname,
        application_name,
        client_addr,
        client_port,
        backend_start,
        state,
        state_change,
        query_start,
        wait_event_type,
        wait_event,
        query
    FROM pg_stat_activity
    WHERE pid != pg_backend_pid()
      AND state IS NOT NULL
    ORDER BY query_start DESC
    """
    # Описание: Получает список активных сессий (кроме текущего подключения)
    # Возвращает: Все поля активных сессий
    # Используется в: pg_sessions.py:61-80
    
    
    # -----------------------------------------------------------------------------
    # Все сессии (включая текущее подключение)
    # -----------------------------------------------------------------------------
    ALL_SESSIONS = """
    SELECT 
        pid,
        usename,
        datname,
        application_name,
        client_addr,
        client_port,
        backend_start,
        state,
        state_change,
        query_start,
        wait_event_type,
        wait_event,
        query
    FROM pg_stat_activity
    WHERE state IS NOT NULL
    ORDER BY query_start DESC
    """
    # Описание: Получает список всех активных сессий
    # Используется в: pg_sessions.py:82-100
    
    
    # -----------------------------------------------------------------------------
    # Статистика сессий
    # -----------------------------------------------------------------------------
    SESSIONS_STATS = """
    SELECT
        count(*) as total,
        count(*) FILTER (WHERE state = 'active') as active,
        count(*) FILTER (WHERE state = 'idle') as idle,
        count(*) FILTER (WHERE state = 'idle in transaction') as idle_in_tx,
        count(*) FILTER (WHERE wait_event_type = 'Lock') as waiting_lock
    FROM pg_stat_activity
    WHERE state IS NOT NULL
      AND pid != pg_backend_pid()
    """
    # Описание: Получает агрегированную статистику по сессиям
    # Используется в: pg_sessions.py:202-221
    
    
    # -----------------------------------------------------------------------------
    # Неактивные сессии (с таймаутом)
    # -----------------------------------------------------------------------------
    IDLE_SESSIONS = """
    SELECT 
        pid,
        usename,
        application_name,
        client_addr,
        backend_start,
        state_change,
        state,
        EXTRACT(EPOCH FROM (NOW() - state_change))::int as idle_seconds
    FROM pg_stat_activity
    WHERE state = 'idle'
      AND state_change < NOW() - interval '1 second' * %s
      AND pid != pg_backend_pid()
    ORDER BY state_change ASC
    """
    # Описание: Получает неактивные сессии старше указанного таймаута
    # Параметры: timeout_seconds (в секундах)
    # Используется в: pg_sessions.py:244-267
    
    
    # -----------------------------------------------------------------------------
    # Долго выполняющиеся запросы
    # -----------------------------------------------------------------------------
    LONG_RUNNING_QUERIES = """
    SELECT 
        pid,
        usename,
        application_name,
        client_addr,
        query_start,
        state,
        EXTRACT(EPOCH FROM (NOW() - query_start))::int as duration_seconds,
        query
    FROM pg_stat_activity
    WHERE state = 'active'
      AND query_start IS NOT NULL
      AND (NOW() - query_start) > interval '1 second' * %s
    ORDER BY query_start ASC
    """
    # Описание: Получает активные запросы, выполняющиеся дольше указанного времени
    # Параметры: timeout_seconds (в секундах)
    # Используется в: pg_sessions.py:283-306
    
    
    # -----------------------------------------------------------------------------
    # Завершение сессии (отмена запроса)
    # -----------------------------------------------------------------------------
    CANCEL_QUERY = "SELECT pg_cancel_backend(%s)"
    # Описание: Отменяет текущий запрос сессии, но оставляет соединение
    # Параметры: pid сессии
    # Используется в: pg_sessions.py:171
    
    
    # -----------------------------------------------------------------------------
    # Завершение сессии (отключение)
    # -----------------------------------------------------------------------------
    TERMINATE_SESSION = "SELECT pg_terminate_backend(%s)"
    # Описание: Полностью завершает сессию и разрывает соединение
    # Параметры: pid сессии
    # Используется в: pg_sessions.py:167


# =============================================================================
# ORACLE МОНИТОРИНГ
# =============================================================================

class OracleQueries:
    """
    SQL запросы для мониторинга Oracle Database.
    Используются в db_monitoring.py -> _get_oracle_stats()
    
    Примечание: Для большинства запросов требуются привилегии SYSDBA или DBA
    """
    
    # -----------------------------------------------------------------------------
    # Версия Oracle
    # -----------------------------------------------------------------------------
    VERSION = """
    SELECT banner FROM v$version WHERE rownum = 1
    """
    # Описание: Получает версию Oracle Database
    # Возвращает: Строку с версией (например: "Oracle Database 19c ...")
    # Используется в: db_monitoring.py:293-297
    
    
    # -----------------------------------------------------------------------------
    # Uptime инстанса
    # -----------------------------------------------------------------------------
    INSTANCE_UPTIME = """
    SELECT startup_time FROM v$instance
    """
    # Описание: Получает время запуска экземпляра БД
    # Возвращает: Timestamp времени старта
    # Используется в: db_monitoring.py:300-310
    # Примечание: Используется для расчёта uptime
    
    
    # -----------------------------------------------------------------------------
    # Размер БД
    # -----------------------------------------------------------------------------
    DATABASE_SIZE = """
    SELECT ROUND(SUM(bytes)/1024/1024/1024, 2) FROM dba_segments
    """
    # Описание: Получает общий размер всех сегментов БД
    # Требует: Доступ к dba_segments (обычно DBA привилегия)
    # Возвращает: Размер в GB
    # Используется в: db_monitoring.py:313-320
    
    
    # -----------------------------------------------------------------------------
    # Статистика сессий/подключений
    # -----------------------------------------------------------------------------
    CONNECTIONS_STATS = """
    SELECT (SELECT COUNT(*) FROM v$session WHERE status='ACTIVE' AND type!='BACKGROUND') as a,
           (SELECT COUNT(*) FROM v$session WHERE type!='BACKGROUND') as t,
           (SELECT VALUE FROM v$parameter WHERE name='processes') as m
    FROM dual
    """
    # Описание: Получает статистику подключений
    # Возвращает:
    #   - a: Активные сессии (выполняют SQL)
    #   - t: Всего сессий (без фоновых процессов)
    #   - m: Максимальное количество процессов (параметр processes)
    # Используется в: db_monitoring.py:323-336
    
    
    # -----------------------------------------------------------------------------
    # Load Profile (профиль нагрузки)
    # -----------------------------------------------------------------------------
    LOAD_PROFILE = """
    SELECT SUM(decode(name, 'session logical reads', value, 0)),
           SUM(decode(name, 'physical reads', value, 0)),
           SUM(decode(name, 'physical writes', value, 0)),
           SUM(decode(name, 'user rollbacks', value, 0)),
           SUM(decode(name, 'user commits', value, 0))
    FROM v$sysstat 
    WHERE name IN ('session logical reads', 'physical reads', 'physical writes', 'user rollbacks', 'user commits')
    """
    # Описание: Получает базовые метрики производительности
    # Возвращает:
    #   - logical_reads: Логические чтения (из памяти)
    #   - physical_reads: Физические чтения (с диска)
    #   - physical_writes: Физические записи
    #   - rollbacks: Откаты транзакций
    #   - commits: Фиксации транзакций
    # Используется в: db_monitoring.py:339-353
    
    
    # -----------------------------------------------------------------------------
    # Buffer Cache Hit Ratio
    # -----------------------------------------------------------------------------
    BUFFER_CACHE_HIT_RATIO = """
    SELECT ROUND((1-(phy.value/(cur.value+con.value)))*100,2) 
    FROM v$sysstat cur, v$sysstat con, v$sysstat phy 
    WHERE cur.name='db block gets' 
      AND con.name='consistent gets' 
      AND phy.name='physical reads'
    """
    # Описание: Рассчитывает процент попаданий в буферный кэш
    # Возвращает: Процент (0-100)
    # Используется в: db_monitoring.py:357-361
    # Примечание: Высокий показатель (>95%) желателен для производительности
    
    
    # -----------------------------------------------------------------------------
    # Library Cache Hit Ratio
    # -----------------------------------------------------------------------------
    LIBRARY_CACHE_HIT_RATIO = """
    SELECT ROUND(SUM(pins-reloads)/SUM(pins)*100,2) FROM v$librarycache
    """
    # Описание: Рассчитывает процент попаданий в библиотечный кэш
    # Возвращает: Процент (0-100)
    # Используется в: db_monitoring.py:363-368
    # Примечание: Высокий показатель (>99%) желателен
    
    
    # -----------------------------------------------------------------------------
    # Dictionary Cache Hit Ratio
    # -----------------------------------------------------------------------------
    DICTIONARY_CACHE_HIT_RATIO = """
    SELECT ROUND((1-(SUM(getmisses)/(SUM(gets)+SUM(getmisses))))*100,2) FROM v$rowcache
    """
    # Описание: Рассчитывает процент попаданий в кэш словаря данных
    # Возвращает: Процент (0-100)
    # Используется в: db_monitoring.py:370-375
    
    
    # -----------------------------------------------------------------------------
    # DML статистика
    # -----------------------------------------------------------------------------
    DML_STATS = """
    SELECT SUM(decode(name,'user commits',value,0)), 
           SUM(decode(name,'user rollbacks',value,0)), 
           SUM(decode(name,'db block changes',value,0)) 
    FROM v$sysstat 
    WHERE name IN ('user commits','user rollbacks','db block changes')
    """
    # Описание: Получает статистику DML операций
    # Возвращает:
    #   - commits: Количество коммитов
    #   - rollbacks: Количество откатов
    #   - block_changes: Изменения блоков
    # Используется в: db_monitoring.py:378-384
    
    
    # -----------------------------------------------------------------------------
    # Wait Events (события ожидания)
    # -----------------------------------------------------------------------------
    WAIT_EVENTS = """
    SELECT event,total_waits,time_waited_micro/1000000 
    FROM v$system_event 
    WHERE wait_class!='Idle' 
    ORDER BY time_waited DESC 
    FETCH FIRST 10 ROWS ONLY
    """
    # Описание: Получает топ-10 событий ожидания
    # Возвращает:
    #   - event: Имя события
    #   - total_waits: Количество ожиданий
    #   - time_waited_sec: Общее время ожидания (сек)
    # Используется в: db_monitoring.py:387-394
    # Примечание: Показывает узкие места производительности
    
    
    # -----------------------------------------------------------------------------
    # Топ таблиц по размеру
    # -----------------------------------------------------------------------------
    TOP_TABLES = """
    SELECT owner,segment_name,ROUND(SUM(bytes)/1024/1024/1024,2),ROUND(SUM(bytes)/1024/1024,2),COUNT(*) 
    FROM dba_segments 
    WHERE segment_type='TABLE' 
    GROUP BY owner,segment_name 
    ORDER BY SUM(bytes) DESC 
    FETCH FIRST 15 ROWS ONLY
    """
    # Описание: Получает топ-15 таблиц по размеру
    # Требует: Доступ к dba_segments
    # Возвращает:
    #   - owner: Владелец схемы
    #   - table_name: Имя таблицы
    #   - size_gb: Размер в GB
    #   - size_mb: Размер в MB
    #   - extents: Количество экстентов
    # Используется в: db_monitoring.py:397-404
    
    
    # -----------------------------------------------------------------------------
    # Tablespaces
    # -----------------------------------------------------------------------------
    TABLESPACES = """
    SELECT /*+parallel(4) */ 
        df.tablespace_name,
        ROUND(SUM(df.bytes)/1024/1024/1024,2),
        ROUND((SUM(df.bytes)-NVL(SUM(fs.bytes),0))/1024/1024/1024,2),
        ROUND(NVL(SUM(fs.bytes),0)/1024/1024/1024,2),
        ROUND(((SUM(df.bytes)-NVL(SUM(fs.bytes),0))/SUM(df.bytes))*100,2) 
    FROM (SELECT tablespace_name,SUM(bytes) as bytes FROM dba_data_files GROUP BY tablespace_name) df 
    LEFT JOIN (SELECT tablespace_name,SUM(bytes) as bytes FROM dba_free_space GROUP BY tablespace_name)
    fs ON df.tablespace_name=fs.tablespace_name 
    GROUP BY df.tablespace_name
    ORDER BY 5 DESC
    """
    # Описание: Получает информацию о tablespace
    # Требует: Доступ к dba_data_files и dba_free_space
    # Возвращает:
    #   - tablespace_name: Имя tablespace
    #   - size_gb: Общий размер
    #   - used_gb: Используемое место
    #   - free_gb: Свободное место
    #   - used_percent: Процент использования
    # Используется в: db_monitoring.py:407-416
    # Примечание: HINT /*+parallel(4) */ ускоряет выполнение на многопроцессорных системах
    
    
    # -----------------------------------------------------------------------------
    # Top SQL
    # -----------------------------------------------------------------------------
    TOP_SQL = """
    SELECT sql_id,
           ROUND(elapsed_time/1000000,2),
           ROUND(cpu_time/1000000,2),
           executions,
           ROUND(elapsed_time/1000000/nullif(executions,0),2),
           SUBSTR(sql_text,1,100) 
    FROM v$sqlarea 
    WHERE executions>0 
    ORDER BY elapsed_time DESC 
    FETCH FIRST 10 ROWS ONLY
    """
    # Описание: Получает топ-10 SQL запросов по времени выполнения
    # Возвращает:
    #   - sql_id: Уникальный идентификатор запроса
    #   - elapsed_sec: Общее время (сек)
    #   - cpu_sec: CPU время (сек)
    #   - executions: Количество выполнений
    #   - avg_elapsed_sec: Среднее время (сек)
    #   - sql_preview: Текст запроса
    # Используется в: db_monitoring.py:418-425
    
    
    # -----------------------------------------------------------------------------
    # SGA (System Global Area)
    # -----------------------------------------------------------------------------
    SGA = """
    SELECT name,ROUND(value/1024/1024,2) FROM v$sga
    """
    # Описание: Получает информацию о компонентах SGA
    # Возвращает: Пары (имя, размер в MB)
    # Используется в: db_monitoring.py:437-441
    
    
    # -----------------------------------------------------------------------------
    # PGA Target
    # -----------------------------------------------------------------------------
    PGA_TARGET = """
    SELECT ROUND(value/1024/1024,2) FROM v$pgastat WHERE name='target PGA memory'
    """
    # Описание: Получает целевой размер PGA
    # Возвращает: Размер в MB
    # Используется в: db_monitoring.py:445-448
    
    
    # -----------------------------------------------------------------------------
    # PGA Usage
    # -----------------------------------------------------------------------------
    PGA_USAGE = """
    SELECT ROUND(SUM(pga_used_mem)/1024/1024,2),ROUND(SUM(pga_alloc_mem)/1024/1024,2) FROM v$process
    """
    # Описание: Получает текущее использование PGA
    # Возвращает:
    #   - used_mb: Фактически используемая память
    #   - allocated_mb: Выделенная память
    # Используется в: db_monitoring.py:452-455
    
    
    # -----------------------------------------------------------------------------
    # Медленные запросы (Active Sessions)
    # -----------------------------------------------------------------------------
    SLOW_QUERIES = """
    SELECT s.sid||','||s.serial#,s.username,s.program,ROUND(s.last_call_et),s.status,SUBSTR(sq.sql_text,1,200) 
    FROM v$session s 
    LEFT JOIN v$sql sq ON s.sql_id=sq.sql_id 
    WHERE s.type='USER' AND s.status='ACTIVE' AND s.last_call_et>1 
    ORDER BY s.last_call_et DESC 
    FETCH FIRST 10 ROWS ONLY
    """
    # Описание: Получает активные сессии с выполняющимися запросами
    # Возвращает:
    #   - sid,serial#: ID сессии
    #   - username: Пользователь
    #   - program: Приложение
    #   - last_call_et: Время выполнения текущего запроса (сек)
    #   - status: Статус сессии
    #   - sql_text: Текст запроса
    # Используется в: db_monitoring.py:460-469


# =============================================================================
# SQLITE МОНИТОРИНГ (внутренняя БД мониторинга)
# =============================================================================

class SQLiteMonitoringQueries:
    """
    SQL запросы для SQLite базы данных мониторинга.
    Используются в monitoring_db.py
    """
    
    # -----------------------------------------------------------------------------
    # Создание таблицы профилей
    # -----------------------------------------------------------------------------
    CREATE_MONITORED_PROFILES = """
    CREATE TABLE IF NOT EXISTS monitored_profiles (
        id INTEGER PRIMARY KEY,
        profile_id INTEGER UNIQUE NOT NULL,
        profile_name TEXT NOT NULL,
        db_type TEXT NOT NULL,
        enabled BOOLEAN DEFAULT 1,
        last_collection TIMESTAMP,
        last_error TEXT,
        collection_duration_sec REAL
    )
    """
    # Описание: Создаёт таблицу для хранения информации о профилях мониторинга
    
    
    # -----------------------------------------------------------------------------
    # Создание таблицы текущей статистики
    # -----------------------------------------------------------------------------
    CREATE_CURRENT_STATS = """
    CREATE TABLE IF NOT EXISTS current_stats (
        id INTEGER PRIMARY KEY,
        profile_id INTEGER UNIQUE NOT NULL,
        db_type TEXT NOT NULL,
        stats_json TEXT NOT NULL,
        collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(profile_id) REFERENCES monitored_profiles(id)
    )
    """
    # Описание: Создаёт таблицу для хранения текущей статистики (перезаписывается)
    
    
    # -----------------------------------------------------------------------------
    # Создание таблицы истории метрик
    # -----------------------------------------------------------------------------
    CREATE_METRICS_HISTORY = """
    CREATE TABLE IF NOT EXISTS metrics_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        profile_id INTEGER NOT NULL,
        metric_name TEXT NOT NULL,
        metric_value REAL,
        collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(profile_id) REFERENCES monitored_profiles(id)
    )
    """
    # Описание: Создаёт таблицу для хранения истории метрик (опционально)
    
    
    # -----------------------------------------------------------------------------
    # Сохранение статистики
    # -----------------------------------------------------------------------------
    SAVE_STATS = """
    INSERT INTO current_stats (profile_id, db_type, stats_json, collected_at)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(profile_id) DO UPDATE SET
        stats_json = EXCLUDED.stats_json,
        collected_at = EXCLUDED.collected_at,
        db_type = EXCLUDED.db_type
    """
    # Описание: Сохраняет или обновляет статистику профиля
    # Параметры: profile_id, db_type, stats_json, collected_at
    
    
    # -----------------------------------------------------------------------------
    # Обновление времени последнего сбора
    # -----------------------------------------------------------------------------
    UPDATE_COLLECTION_TIME = """
    UPDATE monitored_profiles
    SET last_collection = ?, last_error = NULL
    WHERE id = ?
    """
    # Описание: Обновляет время последнего успешного сбора статистики
    
    
    # -----------------------------------------------------------------------------
    # Получение статистики профиля
    # -----------------------------------------------------------------------------
    GET_STATS = """
    SELECT stats_json, collected_at, db_type
    FROM current_stats
    WHERE profile_id = ?
    """
    # Описание: Получает сохранённую статистику для указанного профиля
    
    
    # -----------------------------------------------------------------------------
    # Регистрация профиля
    # -----------------------------------------------------------------------------
    REGISTER_PROFILE = """
    INSERT OR IGNORE INTO monitored_profiles 
    (id, profile_id, profile_name, db_type, enabled)
    VALUES (?, ?, ?, ?, 1)
    """
    # Описание: Регистрирует профиль для мониторинга
    
    
    # -----------------------------------------------------------------------------
    # Получение всех профилей
    # -----------------------------------------------------------------------------
    GET_ALL_PROFILES = """
    SELECT profile_id, profile_name, db_type, enabled, last_collection, last_error
    FROM monitored_profiles
    WHERE enabled = 1
    ORDER BY profile_id
    """
    # Описание: Получает список всех активных профилей мониторинга
    
    
    # -----------------------------------------------------------------------------
    # Логирование ошибки
    # -----------------------------------------------------------------------------
    LOG_ERROR = """
    UPDATE monitored_profiles
    SET last_error = ?, last_collection = ?
    WHERE profile_id = ?
    """
    # Описание: Логирует ошибку при сборе статистики


# =============================================================================
# ЭКСПОРТ
# =============================================================================

# Словарь всех классов запросов для удобного доступа
ALL_QUERIES = {
    'postgresql': PostgreSQLQueries,
    'postgresql_sessions': PostgreSQLSessionQueries,
    'oracle': OracleQueries,
    'sqlite_monitoring': SQLiteMonitoringQueries,
}

# Экспорт отдельных классов для импорта
__all__ = [
    'PostgreSQLQueries',
    'PostgreSQLSessionQueries', 
    'OracleQueries',
    'SQLiteMonitoringQueries',
    'DatabaseSchemaDump',
    'ALL_QUERIES',
]


# =============================================================================
# ДАМП СТРУКТУРЫ БД
# =============================================================================

class DatabaseSchemaDump:
    """
    SQL запросы для получения структуры базы данных (дамп без данных).
    Используются для экспорта схемы БД.
    """
    
    # ==========================================================================
    # POSTGRESQL - Получение структуры БД
    # ==========================================================================
    
    # Список таблиц
    PG_TABLES = """
    SELECT table_name, table_type, is_nullable
    FROM information_schema.tables
    WHERE table_schema = 'public'
    AND table_type IN ('BASE TABLE', 'VIEW')
    ORDER BY table_name
    """
    
    # Список колонок
    PG_COLUMNS = """
    SELECT 
        c.table_name,
        c.column_name,
        c.data_type,
        c.character_maximum_length,
        c.numeric_precision,
        c.numeric_scale,
        c.is_nullable,
        c.column_default,
        c.ordinal_position
    FROM information_schema.columns c
    WHERE c.table_schema = 'public'
    ORDER BY c.table_name, c.ordinal_position
    """
    
    # Список индексов
    PG_INDEXES = """
    SELECT 
        t.relname AS table_name,
        i.relname AS index_name,
        ix.indisunique AS is_unique,
        ix.indisprimary AS is_primary,
        pg_get_indexdef(ix.indexrelid) AS index_def
    FROM pg_class t
    JOIN pg_index ix ON t.oid = ix.indrelid
    JOIN pg_class i ON i.oid = ix.indexrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    WHERE n.nspname = 'public'
    AND t.relkind = 'r'
    ORDER BY t.relname, i.relname
    """
    
    # Список ограничений
    PG_CONSTRAINTS = """
    SELECT 
        tc.table_name,
        tc.constraint_name,
        tc.constraint_type,
        kcu.column_name,
        ccu.table_name AS foreign_table,
        ccu.column_name AS foreign_column
    FROM information_schema.table_constraints tc
    LEFT JOIN information_schema.key_column_usage kcu 
        ON tc.constraint_name = kcu.constraint_name
    LEFT JOIN information_schema.constraint_column_usage ccu 
        ON tc.constraint_name = ccu.constraint_name
    WHERE tc.table_schema = 'public'
    ORDER BY tc.table_name, tc.constraint_name
    """
    
    # Список последовательностей
    PG_SEQUENCES = """
    SELECT 
        sequence_name,
        start_value,
        minimum_value,
        maximum_value,
        increment
    FROM information_schema.sequences
    WHERE sequence_schema = 'public'
    ORDER BY sequence_name
    """
    
    # Список представлений
    PG_VIEWS = """
    SELECT 
        table_name,
        view_definition
    FROM information_schema.views
    WHERE table_schema = 'public'
    ORDER BY table_name
    """
    
    # Список функций
    PG_FUNCTIONS = """
    SELECT 
        routine_name,
        routine_type,
        data_type AS return_type,
        routine_definition
    FROM information_schema.routines
    WHERE routine_schema = 'public'
    ORDER BY routine_name
    """
    
    # ==========================================================================
    # ORACLE - Получение структуры БД
    # ==========================================================================
    
    # Список таблиц (пользовательские)
    ORACLE_TABLES = """
    SELECT 
        table_name,
        tablespace_name,
        num_rows,
        blocks,
        empty_blocks,
        last_analyzed,
        temporary,
        duration
    FROM user_tables
    ORDER BY table_name
    """
    
    # Список таблиц (все доступные)
    ORACLE_TABLES_ALL = """
    SELECT 
        owner,
        table_name,
        tablespace_name,
        num_rows,
        blocks,
        last_analyzed
    FROM all_tables
    WHERE owner = USER
    ORDER BY table_name
    """
    
    # Список колонок
    ORACLE_COLUMNS = """
    SELECT 
        table_name,
        column_name,
        data_type,
        data_length,
        data_precision,
        data_scale,
        nullable,
        column_id,
        data_default
    FROM user_tab_columns
    ORDER BY table_name, column_id
    """
    
    # Список индексов
    ORACLE_INDEXES = """
    SELECT 
        ui.table_name,
        ui.index_name,
        ui.uniqueness,
        ui.tablespace_name,
        ui.status,
        uc.column_name,
        uc.column_position
    FROM user_indexes ui
    JOIN user_ind_columns uc ON ui.index_name = uc.index_name
    ORDER BY ui.table_name, ui.index_name, uc.column_position
    """
    
    # Список ограничений
    ORACLE_CONSTRAINTS = """
    SELECT 
        uc.constraint_name,
        uc.constraint_type,
        uc.table_name,
        ucc.column_name,
        ucr.table_name AS foreign_table,
        uccr.column_name AS foreign_column,
        uc.status,
        uc.delete_rule
    FROM user_constraints uc
    LEFT JOIN user_cons_columns ucc ON uc.constraint_name = ucc.constraint_name
    LEFT JOIN user_constraints ucr ON uc.r_constraint_name = ucr.constraint_name
    LEFT JOIN user_cons_columns uccr ON ucr.constraint_name = uccr.constraint_name AND uccr.position = 1
    ORDER BY uc.table_name, uc.constraint_name
    """
    
    # Список представлений
    ORACLE_VIEWS = """
    SELECT 
        view_name,
        text
    FROM user_views
    ORDER BY view_name
    """
    
    # Список последовательностей
    ORACLE_SEQUENCES = """
    SELECT 
        sequence_name,
        min_value,
        max_value,
        increment_by,
        last_number,
        cache_size,
        cycle_flag
    FROM user_sequences
    ORDER BY sequence_name
    """
    
    # Список процедур и функций
    ORACLE_PROCEDURES = """
    SELECT 
        object_name,
        object_type,
        status,
        created,
        last_ddl_time
    FROM user_objects
    WHERE object_type IN ('PROCEDURE', 'FUNCTION', 'PACKAGE', 'PACKAGE BODY')
    ORDER BY object_type, object_name
    """
    
    # Список триггеров
    ORACLE_TRIGGERS = """
    SELECT 
        trigger_name,
        table_name,
        triggering_event,
        status,
        description,
        action_type
    FROM user_triggers
    ORDER BY table_name, trigger_name
    """
    
    # Исходный код процедур/функций
    ORACLE_SOURCE = """
    SELECT 
        name,
        type,
        line,
        text
    FROM user_source
    ORDER BY name, type, line
    """

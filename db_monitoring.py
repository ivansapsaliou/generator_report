"""
Универсальный модуль мониторинга для PostgreSQL и Oracle
"""

from db_adapter import DatabaseAdapter


class DatabaseMonitoring:
    """Класс для получения статистики БД"""
    
    @staticmethod
    def get_database_stats(conn, db_type):
        """
        Получить общую статистику БД
        
        Returns:
            dict: статистика базы данных
        """
        
        if db_type == DatabaseAdapter.POSTGRES:
            return DatabaseMonitoring._get_postgres_stats(conn)
        elif db_type == DatabaseAdapter.ORACLE:
            return DatabaseMonitoring._get_oracle_stats(conn)
        else:
            return {}
    
    @staticmethod
    def _get_postgres_stats(conn):
        """Статистика PostgreSQL"""
        cur = conn.cursor()
        stats = {}
        
        try:
            # Версия PostgreSQL
            cur.execute("SELECT version()")
            version_full = cur.fetchone()[0]
            import re
            pg_ver_match = re.search(r'PostgreSQL ([\d.]+)', version_full)
            stats['version'] = pg_ver_match.group(0) if pg_ver_match else version_full[:40]
            
            # Uptime
            cur.execute("SELECT pg_postmaster_start_time()")
            start_time = cur.fetchone()[0]
            if start_time:
                from datetime import datetime, timezone
                now_aware = datetime.now(timezone.utc)
                if start_time.tzinfo is None:
                    start_time = start_time.replace(tzinfo=timezone.utc)
                diff = now_aware - start_time
                days = diff.days
                hours, rem = divmod(diff.seconds, 3600)
                mins, _ = divmod(rem, 60)
                stats['uptime'] = f"{days}д {hours}ч {mins}м"
            else:
                stats['uptime'] = 'N/A'
            
            # Размер БД
            cur.execute("""
                SELECT pg_size_pretty(pg_database_size(current_database())) AS size,
                       pg_database_size(current_database()) AS size_bytes
            """)
            db_size_row = cur.fetchone()
            stats['db_size_pretty'] = db_size_row[0]
            stats['db_size'] = db_size_row[0]  # ✅ ДОБАВЛЕНО для совместимости
            stats['db_size_bytes'] = db_size_row[1]
            
            # Подключения
            cur.execute("SHOW max_connections")
            stats['max_connections'] = int(cur.fetchone()[0])
            
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
            conn_row = cur.fetchone()
            stats['connections'] = {
                'total': conn_row[0],
                'active': conn_row[1],
                'idle': conn_row[2],
                'idle_in_tx': conn_row[3],
                'waiting_lock': conn_row[4],
                'max_connections': stats['max_connections']
            }
            stats['waiting_locks'] = conn_row[4]  # ✅ ДОБАВЛЕНО для совместимости
            
            # ✅ ДОБАВЛЕНО: Статистика БД (db_stats)
            cur.execute("""
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
            """)
            db_stat_row = cur.fetchone()
            if db_stat_row:
                stats['db_stats'] = {
                    'commits': db_stat_row[0] or 0,
                    'rollbacks': db_stat_row[1] or 0,
                    'deadlocks': db_stat_row[2] or 0,
                    'conflicts': db_stat_row[3] or 0,
                    'rows_inserted': db_stat_row[4] or 0,
                    'rows_updated': db_stat_row[5] or 0,
                    'rows_deleted': db_stat_row[6] or 0,
                    'rows_fetched': db_stat_row[7] or 0,
                    'cache_reads': db_stat_row[8] or 0,
                    'disk_reads': db_stat_row[9] or 0
                }
            else:
                stats['db_stats'] = {
                    'commits': 0, 'rollbacks': 0, 'deadlocks': 0, 'conflicts': 0,
                    'rows_inserted': 0, 'rows_updated': 0, 'rows_deleted': 0, 'rows_fetched': 0,
                    'cache_reads': 0, 'disk_reads': 0
                }
            
            # ✅ ДОБАВЛЕНО: Cache hit ratio
            cache_reads = stats['db_stats']['cache_reads']
            disk_reads = stats['db_stats']['disk_reads']
            total_reads = cache_reads + disk_reads
            if total_reads > 0:
                stats['cache_hit_ratio'] = round((cache_reads / total_reads) * 100, 2)
            else:
                stats['cache_hit_ratio'] = 0
            
            # ✅ ДОБАВЛЕНО: bgwriter
            cur.execute("""
                SELECT 
                    checkpoints_timed,
                    checkpoints_req,
                    buffers_checkpoint,
                    buffers_clean,
                    buffers_backend,
                    buffers_alloc
                FROM pg_stat_bgwriter
            """)
            bgwriter_row = cur.fetchone()
            if bgwriter_row:
                stats['bgwriter'] = {
                    'checkpoints_timed': bgwriter_row[0] or 0,
                    'checkpoints_req': bgwriter_row[1] or 0,
                    'buffers_checkpoint': bgwriter_row[2] or 0,
                    'buffers_clean': bgwriter_row[3] or 0,
                    'buffers_backend': bgwriter_row[4] or 0,
                    'buffers_alloc': bgwriter_row[5] or 0
                }
            else:
                stats['bgwriter'] = {}
            
            # ✅ ИСПРАВЛЕНО: Статистика таблиц - поиск по всем схемам
            cur.execute("""
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
                """)
            table_stats = []
            for row in cur.fetchall():
                table_stats.append({
                    'table': row[0],
                    'live_rows': row[1] or 0,
                    'dead_rows': row[2] or 0,
                    'total_size': row[3],
                    'table_size': row[4],
                    'index_size': row[5],
                    'inserts': row[6] or 0,
                    'updates': row[7] or 0,
                    'deletes': row[8] or 0,
                    'seq_scans': row[9] or 0,
                    'idx_scans': row[10] or 0,
                    'last_autovacuum': row[11],
                    'last_autoanalyze': row[12]
                })
            stats['table_stats'] = table_stats
            
            # Медленные запросы
            cur.execute("""
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
            """)
            slow_queries = []
            for row in cur.fetchall():
                sec = row[1] or 0
                slow_queries.append({
                    'pid': row[0],
                    'duration_sec': sec,
                    'duration': f"{sec}с" if sec < 60 else f"{sec//60}м {sec%60}с",
                    'query': row[2],
                    'state': row[3],
                    'user': row[4],
                    'app': row[5]
                })
            stats['slow_queries'] = slow_queries
            
            # ✅ ДОБАВЛЕНО: Топ запросов из pg_stat_statements
            try:
                cur.execute("""
                    SELECT 
                        LEFT(query, 150) AS query,
                        calls,
                        ROUND(total_exec_time::numeric, 2) AS total_ms,
                        ROUND(mean_exec_time::numeric, 2) AS mean_ms,
                        rows
                    FROM pg_stat_statements
                    ORDER BY total_exec_time DESC
                    LIMIT 10
                """)
                top_queries = []
                for row in cur.fetchall():
                    top_queries.append({
                        'query': row[0],
                        'calls': row[1],
                        'total_ms': str(row[2]),
                        'mean_ms': str(row[3]),
                        'rows': row[4]
                    })
                stats['top_queries'] = top_queries
            except:
                # pg_stat_statements не установлен
                stats['top_queries'] = []
            
        except Exception as e:
            print(f"[PG Monitoring] Error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            cur.close()
        
        return stats
    
    @staticmethod
    def _get_oracle_stats(conn):
        """Статистика Oracle"""
        cur = conn.cursor()
        stats = {}
        
        try:
            # Версия Oracle
            cur.execute("SELECT banner FROM v$version WHERE rownum = 1")
            stats['version'] = cur.fetchone()[0]
            
            # Uptime (время запуска инстанса)
            cur.execute("SELECT startup_time FROM v$instance")
            startup_time = cur.fetchone()[0]
            if startup_time:
                from datetime import datetime
                now = datetime.now()
                diff = now - startup_time
                days = diff.days
                hours, rem = divmod(diff.seconds, 3600)
                mins, _ = divmod(rem, 60)
                stats['uptime'] = f"{days}д {hours}ч {mins}м"
            else:
                stats['uptime'] = 'N/A'
            
            # Размер БД (сумма всех datafiles)
            cur.execute("""
                SELECT 
                    ROUND(SUM(bytes)/1024/1024/1024, 2) AS size_gb,
                    SUM(bytes) AS size_bytes
                FROM dba_data_files
            """)
            db_size_row = cur.fetchone()
            if db_size_row and db_size_row[0]:
                stats['db_size_pretty'] = f"{db_size_row[0]} GB"
                stats['db_size_bytes'] = db_size_row[1]
            else:
                # Если нет доступа к dba_data_files, используем user_segments
                cur.execute("""
                    SELECT 
                        ROUND(SUM(bytes)/1024/1024/1024, 2) AS size_gb,
                        SUM(bytes) AS size_bytes
                    FROM user_segments
                """)
                user_size_row = cur.fetchone()
                stats['db_size_pretty'] = f"{user_size_row[0] or 0} GB (user)"
                stats['db_size_bytes'] = user_size_row[1] or 0
            
            # Подключения (сессии)
            cur.execute("""
                SELECT 
                    value 
                FROM v$parameter 
                WHERE name = 'sessions'
            """)
            max_sessions = cur.fetchone()[0]
            stats['max_connections'] = int(max_sessions)
            
            cur.execute("""
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END) AS active,
                    SUM(CASE WHEN status = 'INACTIVE' THEN 1 ELSE 0 END) AS inactive,
                    SUM(CASE WHEN blocking_session IS NOT NULL THEN 1 ELSE 0 END) AS blocked
                FROM v$session
                WHERE type = 'USER'
            """)
            conn_row = cur.fetchone()
            stats['connections'] = {
                'total': conn_row[0] or 0,
                'active': conn_row[1] or 0,
                'idle': conn_row[2] or 0,
                'idle_in_tx': 0,  # Oracle не имеет такого состояния
                'waiting_lock': conn_row[3] or 0,
                'max_connections': stats['max_connections']
            }
            
            # Медленные запросы (долгие SQL)
            cur.execute("""
                SELECT
                    s.sid,
                    s.serial#,
                    ROUND(s.last_call_et) AS duration_sec,
                    SUBSTR(sq.sql_text, 1, 200) AS query,
                    s.status,
                    s.username,
                    s.program
                FROM v$session s
                LEFT JOIN v$sql sq ON s.sql_id = sq.sql_id
                WHERE s.type = 'USER'
                  AND s.status = 'ACTIVE'
                  AND s.last_call_et > 1
                ORDER BY s.last_call_et DESC
                FETCH FIRST 10 ROWS ONLY
            """)
            slow_queries = []
            for row in cur.fetchall():
                sec = row[2] or 0
                slow_queries.append({
                    'pid': f"{row[0]},{row[1]}",  # SID,SERIAL#
                    'duration_sec': sec,
                    'duration_str': f"{sec}с" if sec < 60 else f"{sec//60}м {sec%60}с",
                    'query': row[3] or 'N/A',
                    'state': row[4],
                    'user': row[5] or 'N/A',
                    'app': row[6] or 'N/A'
                })
            stats['slow_queries'] = slow_queries
            
        except Exception as e:
            print(f"[Oracle Monitoring] Error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            cur.close()
        
        return stats


class ServerMonitoring:
    """Класс для получения статистики сервера через SSH"""
    
    @staticmethod
    def get_server_stats_via_ssh(ssh_client):
        """
        Получить статистику сервера через SSH
        
        Args:
            ssh_client: объект paramiko.SSHClient
            
        Returns:
            dict: статистика сервера
        """
        stats = {}
        
        try:
            # CPU
            stdin, stdout, stderr = ssh_client.exec_command(
                "top -bn1 | grep 'Cpu(s)' | awk '{print $2}' | cut -d'%' -f1"
            )
            cpu_usage = stdout.read().decode().strip()
            stats['cpu_percent'] = float(cpu_usage) if cpu_usage else 0
            
            # Memory
            stdin, stdout, stderr = ssh_client.exec_command(
                "free -m | awk 'NR==2{printf \"%.2f %.2f\", $3,$2}'"
            )
            mem_output = stdout.read().decode().strip().split()
            if len(mem_output) == 2:
                stats['memory_used_mb'] = float(mem_output[0])
                stats['memory_total_mb'] = float(mem_output[1])
                stats['memory_percent'] = (stats['memory_used_mb'] / stats['memory_total_mb']) * 100
            else:
                stats['memory_used_mb'] = 0
                stats['memory_total_mb'] = 0
                stats['memory_percent'] = 0
            
            # Disk
            stdin, stdout, stderr = ssh_client.exec_command(
                "df -h / | awk 'NR==2{print $3,$2,$5}'"
            )
            disk_output = stdout.read().decode().strip().split()
            if len(disk_output) == 3:
                stats['disk_used'] = disk_output[0]
                stats['disk_total'] = disk_output[1]
                stats['disk_percent'] = float(disk_output[2].replace('%', ''))
            else:
                stats['disk_used'] = '0G'
                stats['disk_total'] = '0G'
                stats['disk_percent'] = 0
            
        except Exception as e:
            print(f"[SSH Monitoring] Error: {e}")
            stats = {
                'cpu_percent': 0,
                'memory_used_mb': 0,
                'memory_total_mb': 0,
                'memory_percent': 0,
                'disk_used': '0G',
                'disk_total': '0G',
                'disk_percent': 0
            }
        
        return stats
    
    @staticmethod
    def get_local_server_stats():
        """
        Получить статистику локального сервера через /proc
        
        Returns:
            dict: статистика сервера
        """
        stats = {}
        
        try:
            # CPU через /proc/stat
            with open('/proc/stat', 'r') as f:
                cpu_line = f.readline()
                cpu_values = [int(x) for x in cpu_line.split()[1:]]
                idle = cpu_values[3]
                total = sum(cpu_values)
                # Простой расчет (без сохранения предыдущих значений)
                stats['cpu_percent'] = round(100 - (idle / total * 100), 1)
            
            # Memory через /proc/meminfo
            with open('/proc/meminfo', 'r') as f:
                meminfo = {}
                for line in f:
                    parts = line.split(':')
                    if len(parts) == 2:
                        key = parts[0].strip()
                        value = int(parts[1].strip().split()[0])
                        meminfo[key] = value
                
                mem_total = meminfo.get('MemTotal', 0) / 1024  # MB
                mem_available = meminfo.get('MemAvailable', 0) / 1024  # MB
                mem_used = mem_total - mem_available
                
                stats['memory_used_mb'] = round(mem_used, 2)
                stats['memory_total_mb'] = round(mem_total, 2)
                stats['memory_percent'] = round((mem_used / mem_total * 100), 1) if mem_total > 0 else 0
            
            # Disk через /proc/mounts и статистика
            import os
            import shutil
            disk_usage = shutil.disk_usage('/')
            disk_total_gb = disk_usage.total / (1024**3)
            disk_used_gb = disk_usage.used / (1024**3)
            
            stats['disk_used'] = f"{disk_used_gb:.1f}G"
            stats['disk_total'] = f"{disk_total_gb:.1f}G"
            stats['disk_percent'] = round((disk_used_gb / disk_total_gb * 100), 1)
            
        except Exception as e:
            print(f"[Local Monitoring] Error: {e}")
            # Fallback: возвращаем нулевые значения
            stats = {
                'cpu_percent': 0,
                'memory_used_mb': 0,
                'memory_total_mb': 0,
                'memory_percent': 0,
                'disk_used': '0G',
                'disk_total': '0G',
                'disk_percent': 0
            }
        
        return stats
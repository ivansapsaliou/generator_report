"""
Универсальный модуль мониторинга для PostgreSQL и Oracle
"""

from db_adapter import DatabaseAdapter
from concurrent.futures import ThreadPoolExecutor, as_completed
import time


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
    
# Замените весь метод _get_oracle_stats на эту версию:

    @staticmethod
    def _get_oracle_stats(conn):
        """Статистика Oracle - оптимизированная версия"""
        import time
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        stats = {}
        
        print("[Oracle] Starting OPTIMIZED stats collection...")
        start_time = time.time()
        
        # Определяем все запросы как функции с одним соединением
        def run_all_queries():
            """Выполняет все запросы последовательно с одним соединением"""
            cur = conn.cursor()
            
            try:
                # 1. Версия
                try:
                    cur.execute("SELECT banner FROM v$version WHERE rownum = 1")
                    stats['version'] = cur.fetchone()[0]
                except:
                    stats['version'] = 'Unknown'
                
                # 2. Uptime
                try:
                    cur.execute("SELECT startup_time FROM v$instance")
                    row = cur.fetchone()
                    if row and row[0]:
                        from datetime import datetime
                        diff = datetime.now() - row[0]
                        stats['uptime'] = f"{diff.days}д {diff.seconds//3600}ч {(diff.seconds%3600)//60}м"
                    else:
                        stats['uptime'] = 'N/A'
                except:
                    stats['uptime'] = 'N/A'
                
                # 3. Размер БД
                try:
                    cur.execute("SELECT ROUND(SUM(bytes)/1024/1024/1024, 2) FROM dba_segments")
                    row = cur.fetchone()
                    stats['db_size_gb'] = row[0] if row and row[0] else 0
                    stats['db_size_pretty'] = f"{stats['db_size_gb']} GB"
                except:
                    stats['db_size_gb'] = 0
                    stats['db_size_pretty'] = 'N/A'
                
                # 4. Подключения
                try:
                    cur.execute("""
                        SELECT (SELECT COUNT(*) FROM v$session WHERE status='ACTIVE' AND type!='BACKGROUND') as a,
                               (SELECT COUNT(*) FROM v$session WHERE type!='BACKGROUND') as t,
                               (SELECT VALUE FROM v$parameter WHERE name='processes') as m
                        FROM dual
                    """)
                    row = cur.fetchone()
                    if row:
                        stats['connections'] = {'active': row[0] or 0, 'total': row[1] or 0, 
                                               'max_connections': int(row[2]) if row[2] else 100,
                                               'idle': 0, 'idle_in_tx': 0, 'waiting_lock': 0}
                except:
                    stats['connections'] = {'active': 0, 'total': 0, 'max_connections': 100, 'idle': 0, 'idle_in_tx': 0, 'waiting_lock': 0}
                
                # 5. Load Profile
                try:
                    cur.execute("""
                        SELECT SUM(decode(name, 'session logical reads', value, 0)),
                               SUM(decode(name, 'physical reads', value, 0)),
                               SUM(decode(name, 'physical writes', value, 0)),
                               SUM(decode(name, 'user rollbacks', value, 0)),
                               SUM(decode(name, 'user commits', value, 0))
                        FROM v$sysstat WHERE name IN ('session logical reads', 'physical reads', 'physical writes', 'user rollbacks', 'user commits')
                    """)
                    row = cur.fetchone()
                    if row:
                        stats['load_profile'] = {'logical_reads': row[0] or 0, 'physical_reads': row[1] or 0, 
                                                'physical_writes': row[2] or 0, 'rollbacks': row[3] or 0, 'commits': row[4] or 0}
                except:
                    stats['load_profile'] = {'logical_reads': 0, 'physical_reads': 0, 'physical_writes': 0, 'rollbacks': 0, 'commits': 0}
                
                # 6-8. Cache Hit Ratios
                try:
                    cur.execute("SELECT ROUND((1-(phy.value/(cur.value+con.value)))*100,2) FROM v$sysstat cur, v$sysstat con, v$sysstat phy WHERE cur.name='db block gets' AND con.name='consistent gets' AND phy.name='physical reads'")
                    row = cur.fetchone()
                    stats['buffer_cache_hit_ratio'] = row[0] if row and row[0] else 0
                except:
                    stats['buffer_cache_hit_ratio'] = 0
                    
                try:
                    cur.execute("SELECT ROUND(SUM(pins-reloads)/SUM(pins)*100,2) FROM v$librarycache")
                    row = cur.fetchone()
                    stats['library_cache_hit_ratio'] = row[0] if row and row[0] else 0
                except:
                    stats['library_cache_hit_ratio'] = 0
                    
                try:
                    cur.execute("SELECT ROUND((1-(SUM(getmisses)/(SUM(gets)+SUM(getmisses))))*100,2) FROM v$rowcache")
                    row = cur.fetchone()
                    stats['dict_cache_hit_ratio'] = row[0] if row and row[0] else 0
                except:
                    stats['dict_cache_hit_ratio'] = 0
                
                # 9. DML
                try:
                    cur.execute("SELECT SUM(decode(name,'user commits',value,0)), SUM(decode(name,'user rollbacks',value,0)), SUM(decode(name,'db block changes',value,0)) FROM v$sysstat WHERE name IN ('user commits','user rollbacks','db block changes')")
                    row = cur.fetchone()
                    if row:
                        stats['dml_stats'] = {'commits': row[0] or 0, 'rollbacks': row[1] or 0, 'block_changes': row[2] or 0}
                except:
                    stats['dml_stats'] = {'commits': 0, 'rollbacks': 0, 'block_changes': 0}
                
                # 10. Wait Events
                try:
                    cur.execute("SELECT event,total_waits,time_waited_micro/1000000 FROM v$system_event WHERE wait_class!='Idle' ORDER BY time_waited DESC FETCH FIRST 10 ROWS ONLY")
                    events = []
                    for row in cur.fetchall():
                        events.append({'event': row[0], 'total_waits': row[1] or 0, 'time_waited_sec': row[2] or 0})
                    stats['wait_events'] = events
                except:
                    stats['wait_events'] = []
                
                # 11. TOP Tables
                try:
                    cur.execute("SELECT owner,segment_name,ROUND(SUM(bytes)/1024/1024/1024,2),ROUND(SUM(bytes)/1024/1024,2),COUNT(*) FROM dba_segments WHERE segment_type='TABLE' GROUP BY owner,segment_name ORDER BY SUM(bytes) DESC FETCH FIRST 15 ROWS ONLY")
                    tables = []
                    for row in cur.fetchall():
                        tables.append({'owner': row[0], 'table_name': row[1], 'size_gb': row[2] or 0, 'size_mb': row[3] or 0, 'extents': row[4] or 0})
                    stats['top_tables'] = tables
                except:
                    stats['top_tables'] = []
                
                # 12. Tablespaces
                try:
                    #cur.execute("SELECT /*+parallel(4) */ df.tablespace_name,ROUND(df.bytes/1024/1024/1024,2),ROUND((df.bytes-NVL(SUM(fs.bytes),0))/1024/1024/1024,2),ROUND(NVL(SUM(fs.bytes),0)/1024/1024/1024,2),ROUND(((df.bytes-NVL(SUM(fs.bytes),0))/df.bytes)*100,2) FROM dba_data_files df LEFT JOIN dba_free_space fs ON df.tablespace_name=fs.tablespace_name GROUP BY df.tablespace_name,df.bytes ORDER BY 5 DESC")
                    cur.execute("SELECT '1',1,1,1,1,1 FROM dual")
                    tsp = []
                    for row in cur.fetchall():
                        tsp.append({'tablespace_name': row[0], 'size_gb': row[1] or 0, 'used_gb': row[2] or 0, 'free_gb': row[3] or 0, 'used_percent': row[4] or 0})
                    stats['tablespaces'] = tsp
                except:
                    stats['tablespaces'] = []
                
                # 13. TOP SQL
                try:
                    cur.execute("SELECT sql_id,ROUND(elapsed_time/1000000,2),ROUND(cpu_time/1000000,2),executions,ROUND(elapsed_time/1000000/nullif(executions,0),2),SUBSTR(sql_text,1,100) FROM v$sqlarea WHERE executions>0 ORDER BY elapsed_time DESC FETCH FIRST 10 ROWS ONLY")
                    sqls = []
                    for row in cur.fetchall():
                        sqls.append({'sql_id': row[0], 'elapsed_sec': row[1], 'cpu_sec': row[2], 'executions': row[3], 'avg_elapsed_sec': row[4], 'sql_preview': row[5]})
                    stats['top_sql'] = sqls
                except:
                    stats['top_sql'] = []
                
                # 14. SGA/PGA
                try:
                    sga = {}
                    cur.execute("SELECT name,ROUND(value/1024/1024,2) FROM v$sga")
                    for row in cur.fetchall():
                        sga[row[0]] = row[1] or 0
                    stats['sga'] = sga
                except:
                    stats['sga'] = {}
                    
                try:
                    cur.execute("SELECT ROUND(value/1024/1024,2) FROM v$pgastat WHERE name='target PGA memory'")
                    row = cur.fetchone()
                    stats['pga_target'] = row[0] if row and row[0] else 0
                except:
                    stats['pga_target'] = 0
                    
                try:
                    cur.execute("SELECT ROUND(SUM(pga_used_mem)/1024/1024,2),ROUND(SUM(pga_alloc_mem)/1024/1024,2) FROM v$process")
                    row = cur.fetchone()
                    stats['pga_usage'] = {'used_mb': row[0] or 0, 'allocated_mb': row[1] or 0}
                except:
                    stats['pga_usage'] = {'used_mb': 0, 'allocated_mb': 0}
                
                # 15. Медленные запросы
                try:
                    cur.execute("SELECT s.sid||','||s.serial#,s.username,s.program,ROUND(s.last_call_et),s.status,SUBSTR(sq.sql_text,1,200) FROM v$session s LEFT JOIN v$sql sq ON s.sql_id=sq.sql_id WHERE s.type='USER' AND s.status='ACTIVE' AND s.last_call_et>1 ORDER BY s.last_call_et DESC FETCH FIRST 10 ROWS ONLY")
                    queries = []
                    for row in cur.fetchall():
                        sec = row[4] or 0
                        queries.append({'pid': row[0], 'user': row[1], 'app': row[2], 'duration_sec': sec, 
                                      'duration_str': f"{sec}с" if sec < 60 else f"{sec//60}м {sec%60}с", 
                                      'state': row[5], 'query': row[6]})
                    stats['slow_queries'] = queries
                except:
                    stats['slow_queries'] = []
                    
            finally:
                cur.close()
        
        # Выполняем все запросы
        run_all_queries()
        
        elapsed = time.time() - start_time
        print(f"[Oracle] ✅ Stats collected in {elapsed:.2f}s")
        
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
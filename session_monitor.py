"""
Database session monitoring and management.
Мониторинг сессий БД с использованием pg_stat_activity.
"""

import psycopg2
import psycopg2.extras
from datetime import datetime
import threading
from threading import Lock


class SessionMonitor:
    """Класс для мониторинга и управления сессиями PostgreSQL через pg_stat_activity"""
    
    def __init__(self, config):
        """
        Инициализация монитора сессий
        
        Args:
            config: объект конфигурации приложения (app.config)
        """
        self.config = config
        self._monitor_thread = None
        self._is_running = False
    
    def get_db_connection(self):
        """Получить подключение к БД"""
        from db_connection import get_db_connection
        return get_db_connection(self.config)
    
    def get_active_sessions(self, exclude_current=True):
        """
        Получить список активных сессий (процессов) в БД
        
        Args:
            exclude_current: исключить текущее подключение
        
        Returns:
            list: список словарей с информацией о сессиях
        """
        try:
            conn = self.get_db_connection()
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            
            if exclude_current:
                # Запрос информации о всех активных подключениях кроме текущего
                cur.execute("""
                    SELECT 
                        pid,
                        usename,
                        datname,
                        application_name,
                        client_addr,
                        client_port,
                        backend_start,
                        state,
                        query,
                        query_start,
                        state_change,
                        wait_event_type,
                        wait_event
                    FROM pg_stat_activity
                    WHERE pid != pg_backend_pid()
                    ORDER BY backend_start
                """)
            else:
                cur.execute("""
                    SELECT 
                        pid,
                        usename,
                        datname,
                        application_name,
                        client_addr,
                        client_port,
                        backend_start,
                        state,
                        query,
                        query_start,
                        state_change,
                        wait_event_type,
                        wait_event
                    FROM pg_stat_activity
                    ORDER BY backend_start
                """)
            
            sessions = []
            for row in cur.fetchall():
                try:
                    duration = (datetime.now() - row['backend_start'].replace(tzinfo=None)).total_seconds() if row['backend_start'] else 0
                except:
                    duration = 0
                
                sessions.append({
                    'pid': row['pid'],
                    'user': row['usename'],
                    'database': row['datname'],
                    'application': row['application_name'],
                    'client_addr': str(row['client_addr']) if row['client_addr'] else None,
                    'client_port': row['client_port'],
                    'backend_start': row['backend_start'].isoformat() if row['backend_start'] else None,
                    'state': row['state'],
                    'query': row['query'] if row['query'] else '',
                    'query_start': row['query_start'].isoformat() if row['query_start'] else None,
                    'state_change': row['state_change'].isoformat() if row['state_change'] else None,
                    'wait_event_type': row['wait_event_type'],
                    'wait_event': row['wait_event'],
                    'duration_seconds': duration
                })
            
            cur.close()
            conn.close()
            return sessions
        
        except Exception as e:
            print(f"Ошибка при получении активных сессий: {e}")
            return []
    
    def get_idle_sessions(self, idle_timeout_seconds=300):
        """
        Получить список неактивных сессий
        
        Args:
            idle_timeout_seconds: время в секундах, после которого сессия считается неактивной
        
        Returns:
            list: список неактивных сессий
        """
        try:
            conn = self.get_db_connection()
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            
            cur.execute(f"""
                SELECT 
                    pid,
                    usename,
                    datname,
                    application_name,
                    client_addr,
                    client_port,
                    backend_start,
                    state,
                    query,
                    query_start,
                    state_change,
                    EXTRACT(EPOCH FROM (NOW() - state_change)) as idle_seconds
                FROM pg_stat_activity
                WHERE 
                    state = 'idle'
                    AND pid != pg_backend_pid()
                    AND EXTRACT(EPOCH FROM (NOW() - state_change)) > {idle_timeout_seconds}
                ORDER BY state_change
            """)
            
            idle_sessions = []
            for row in cur.fetchall():
                idle_sessions.append({
                    'pid': row['pid'],
                    'user': row['usename'],
                    'database': row['datname'],
                    'application': row['application_name'],
                    'client_addr': str(row['client_addr']) if row['client_addr'] else None,
                    'client_port': row['client_port'],
                    'backend_start': row['backend_start'].isoformat() if row['backend_start'] else None,
                    'state': row['state'],
                    'query': row['query'] if row['query'] else '',
                    'state_change': row['state_change'].isoformat() if row['state_change'] else None,
                    'idle_seconds': float(row['idle_seconds'])
                })
            
            cur.close()
            conn.close()
            return idle_sessions
        
        except Exception as e:
            print(f"Ошибка при получении неактивных сессий: {e}")
            return []
    
    def get_long_running_queries(self, query_timeout_seconds=3600):
        """
        Получить список долго выполняющихся запросов
        
        Args:
            query_timeout_seconds: время в секундах, после которого запрос считается долгим
        
        Returns:
            list: список долго выполняющихся запросов
        """
        try:
            conn = self.get_db_connection()
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            
            cur.execute(f"""
                SELECT 
                    pid,
                    usename,
                    datname,
                    application_name,
                    client_addr,
                    client_port,
                    backend_start,
                    state,
                    query,
                    query_start,
                    EXTRACT(EPOCH FROM (NOW() - query_start)) as query_duration_seconds
                FROM pg_stat_activity
                WHERE 
                    state = 'active'
                    AND pid != pg_backend_pid()
                    AND EXTRACT(EPOCH FROM (NOW() - query_start)) > {query_timeout_seconds}
                ORDER BY query_start
            """)
            
            long_queries = []
            for row in cur.fetchall():
                long_queries.append({
                    'pid': row['pid'],
                    'user': row['usename'],
                    'database': row['datname'],
                    'application': row['application_name'],
                    'client_addr': str(row['client_addr']) if row['client_addr'] else None,
                    'client_port': row['client_port'],
                    'backend_start': row['backend_start'].isoformat() if row['backend_start'] else None,
                    'state': row['state'],
                    'query': row['query'] if row['query'] else '',
                    'query_start': row['query_start'].isoformat() if row['query_start'] else None,
                    'query_duration_seconds': float(row['query_duration_seconds'])
                })
            
            cur.close()
            conn.close()
            return long_queries
        
        except Exception as e:
            print(f"Ошибка при получении долгих запросов: {e}")
            return []
    
    def kill_session(self, pid, terminate=True):
        """
        Завершить сессию по PID
        
        Args:
            pid: процесс ID для завершения
            terminate: True для pg_terminate_backend, False для pg_cancel_backend (отмена запроса)
        
        Returns:
            tuple: (success: bool, message: str)
        """
        try:
            conn = self.get_db_connection()
            cur = conn.cursor()
            
            if terminate:
                # pg_terminate_backend - принудительное завершение сессии
                cur.execute("SELECT pg_terminate_backend(%s)", (pid,))
            else:
                # pg_cancel_backend - отмена текущего запроса (более мягкий способ)
                cur.execute("SELECT pg_cancel_backend(%s)", (pid,))
            
            result = cur.fetchone()[0]
            conn.commit()
            cur.close()
            conn.close()
            
            action = "завершена" if terminate else "отменён запрос в"
            
            if result:
                return (True, f"Сессия {pid} успешно {action}")
            else:
                return (False, f"Не удалось завершить сессию {pid} (сессия не существует или уже завершена)")
        
        except Exception as e:
            return (False, f"Ошибка при завершении сессии {pid}: {str(e)}")
    
    def cancel_query(self, pid):
        """
        Отменить текущий запрос в сессии (мягкий способ)
        
        Args:
            pid: процесс ID
        
        Returns:
            tuple: (success: bool, message: str)
        """
        return self.kill_session(pid, terminate=False)
    
    def kill_idle_sessions(self, idle_timeout_seconds=300, exclude_pids=None, terminate=True):
        """
        Завершить все неактивные сессии
        
        Args:
            idle_timeout_seconds: время в секундах для определения неактивной сессии
            exclude_pids: список PID для исключения
            terminate: True для завершения, False для отмены запросов
        
        Returns:
            dict: результаты завершения сессий
        """
        if exclude_pids is None:
            exclude_pids = []
        
        idle_sessions = self.get_idle_sessions(idle_timeout_seconds)
        results = {
            'total': len(idle_sessions),
            'killed': 0,
            'failed': 0,
            'excluded': 0,
            'details': []
        }
        
        for session in idle_sessions:
            pid = session['pid']
            
            if pid in exclude_pids:
                results['excluded'] += 1
                results['details'].append({
                    'pid': pid,
                    'status': 'excluded',
                    'message': 'PID в списке исключений'
                })
                continue
            
            success, message = self.kill_session(pid, terminate=terminate)
            if success:
                results['killed'] += 1
                results['details'].append({
                    'pid': pid,
                    'status': 'killed',
                    'message': message
                })
            else:
                results['failed'] += 1
                results['details'].append({
                    'pid': pid,
                    'status': 'failed',
                    'message': message
                })
        
        return results
    
    def kill_long_running_queries(self, query_timeout_seconds=3600, exclude_pids=None, terminate=True):
        """
        Завершить все долго выполняющиеся запросы
        
        Args:
            query_timeout_seconds: время в секундах для определения долгого запроса
            exclude_pids: список PID для исключения
            terminate: True для завершения, False для отмены запросов
        
        Returns:
            dict: результаты завершения запросов
        """
        if exclude_pids is None:
            exclude_pids = []
        
        long_queries = self.get_long_running_queries(query_timeout_seconds)
        results = {
            'total': len(long_queries),
            'killed': 0,
            'failed': 0,
            'excluded': 0,
            'details': []
        }
        
        for session in long_queries:
            pid = session['pid']
            
            if pid in exclude_pids:
                results['excluded'] += 1
                results['details'].append({
                    'pid': pid,
                    'status': 'excluded',
                    'message': 'PID в списке исключений'
                })
                continue
            
            success, message = self.kill_session(pid, terminate=terminate)
            if success:
                results['killed'] += 1
                results['details'].append({
                    'pid': pid,
                    'status': 'killed',
                    'message': message
                })
            else:
                results['failed'] += 1
                results['details'].append({
                    'pid': pid,
                    'status': 'failed',
                    'message': message
                })
        
        return results
    
    def get_session_stats(self):
        """
        Получить статистику по сессиям
        
        Returns:
            dict: статистика активных сессий
        """
        try:
            conn = self.get_db_connection()
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            
            cur.execute("""
                SELECT 
                    state,
                    datname,
                    usename,
                    COUNT(*) as count
                FROM pg_stat_activity
                WHERE pid != pg_backend_pid()
                GROUP BY state, datname, usename
                ORDER BY state, datname, usename
            """)
            
            stats = {
                'total_sessions': 0,
                'by_state': {},
                'by_database': {},
                'by_user': {},
                'idle_sessions': 0,
                'active_sessions': 0,
            }
            
            for row in cur.fetchall():
                state = row['state'] or 'unknown'
                datname = row['datname']
                usename = row['usename']
                count = row['count']
                
                stats['total_sessions'] += count
                stats['by_state'][state] = stats['by_state'].get(state, 0) + count
                stats['by_database'][datname] = stats['by_database'].get(datname, 0) + count
                stats['by_user'][usename] = stats['by_user'].get(usename, 0) + count
                
                if state == 'idle':
                    stats['idle_sessions'] += count
                elif state == 'active':
                    stats['active_sessions'] += count
            
            cur.close()
            conn.close()
            return stats
        
        except Exception as e:
            print(f"Ошибка при получении статистики: {e}")
            return {}
    
    def get_blocking_sessions(self):
        """
        Получить информацию о заблокированных сессиях (deadlocks)
        
        Returns:
            list: список заблокированных сессий
        """
        try:
            conn = self.get_db_connection()
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            
            cur.execute("""
                SELECT 
                    blocked_locks.pid AS blocked_pid,
                    blocked_activity.usename AS blocked_user,
                    blocked_activity.datname AS blocked_database,
                    blocked_activity.query AS blocked_query,
                    blocked_activity.application_name AS blocked_application,
                    blocking_locks.pid AS blocking_pid,
                    blocking_activity.usename AS blocking_user,
                    blocking_activity.query AS blocking_query,
                    blocked_activity.backend_start AS blocked_since
                FROM pg_catalog.pg_locks blocked_locks
                JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
                JOIN pg_catalog.pg_locks blocking_locks ON blocking_locks.locktype = blocked_locks.locktype
                    AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
                    AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
                    AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
                    AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
                    AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
                    AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
                    AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
                    AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
                    AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
                    AND blocking_locks.pid != blocked_locks.pid
                JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
                WHERE NOT blocked_locks.granted
            """)
            
            blocking_sessions = []
            for row in cur.fetchall():
                blocking_sessions.append({
                    'blocked_pid': row['blocked_pid'],
                    'blocked_user': row['blocked_user'],
                    'blocked_database': row['blocked_database'],
                    'blocked_query': row['blocked_query'],
                    'blocked_application': row['blocked_application'],
                    'blocked_since': row['blocked_since'].isoformat() if row['blocked_since'] else None,
                    'blocking_pid': row['blocking_pid'],
                    'blocking_user': row['blocking_user'],
                    'blocking_query': row['blocking_query']
                })
            
            cur.close()
            conn.close()
            return blocking_sessions
        
        except Exception as e:
            print(f"Ошибка при получении информации о блокировках: {e}")
            return []
    
    def monitor_loop(self, check_interval=30):
        """
        Цикл мониторинга сессий
        
        Args:
            check_interval: интервал проверки в секундах
        """
        while self._is_running:
            try:
                # Просто выполняем проверку доступности БД
                self.get_session_stats()
                threading.Event().wait(check_interval)
            
            except Exception as e:
                print(f"Ошибка в цикле мониторинга: {e}")
                threading.Event().wait(check_interval)
    
    def start_monitoring(self, check_interval=30):
        """
        Запустить фоновый мониторинг сессий
        
        Args:
            check_interval: интервал проверки в секундах
        """
        if self._is_running:
            return False
        
        self._is_running = True
        self._monitor_thread = threading.Thread(
            target=self.monitor_loop,
            args=(check_interval,),
            daemon=True
        )
        self._monitor_thread.start()
        return True
    
    def stop_monitoring(self):
        """Остановить фоновый мониторинг"""
        self._is_running = False
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5)
        return True
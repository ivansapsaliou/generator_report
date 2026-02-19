"""
PostgreSQL Session Management Module
Модуль для мониторинга и управления сессиями PostgreSQL
"""

import psycopg2
import psycopg2.extras
from datetime import datetime


class PostgreSQLSessionManager:
    """Класс для управления сессиями PostgreSQL через pg_stat_activity"""
    
    def __init__(self, config):
        """
        Инициализация менеджера сессий
        
        Args:
            config: объект конфигурации приложения (app.config)
        """
        self.config = config
    
    def get_db_connection(self):
        """Получить подключение к БД"""
        from db_connection import get_db_connection
        return get_db_connection(self.config)
    
    def get_active_sessions(self, exclude_current=True):
        """
        Получить список активных сессий БД
        
        Args:
            exclude_current: исключить текущее подключение
        
        Returns:
            list: список словарей с информацией о сессиях
        
        SQL запрос:
            SELECT 
                pid,
                usename,
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
        try:
            conn = self.get_db_connection()
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            
            if exclude_current:
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
                        state_change,
                        query_start,
                        wait_event_type,
                        wait_event,
                        query
                    FROM pg_stat_activity
                    WHERE pid != pg_backend_pid()
                      AND state IS NOT NULL
                    ORDER BY query_start DESC
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
                        state_change,
                        query_start,
                        wait_event_type,
                        wait_event,
                        query
                    FROM pg_stat_activity
                    WHERE state IS NOT NULL
                    ORDER BY query_start DESC
                """)
            
            sessions = []
            for row in cur.fetchall():
                # Расчет длительности сессии
                try:
                    if row['backend_start']:
                        duration = (datetime.now() - row['backend_start'].replace(tzinfo=None)).total_seconds()
                    else:
                        duration = 0
                except:
                    duration = 0
                
                # Расчет длительности текущего запроса
                try:
                    if row['query_start']:
                        query_duration = (datetime.now() - row['query_start'].replace(tzinfo=None)).total_seconds()
                    else:
                        query_duration = 0
                except:
                    query_duration = 0
                
                sessions.append({
                    'pid': row['pid'],
                    'usename': row['usename'],
                    'datname': row['datname'],
                    'application_name': row['application_name'],
                    'client_addr': str(row['client_addr']) if row['client_addr'] else None,
                    'client_port': row['client_port'],
                    'backend_start': row['backend_start'].isoformat() if row['backend_start'] else None,
                    'state': row['state'],
                    'state_change': row['state_change'].isoformat() if row['state_change'] else None,
                    'query_start': row['query_start'].isoformat() if row['query_start'] else None,
                    'wait_event_type': row['wait_event_type'],
                    'wait_event': row['wait_event'],
                    'query': row['query'] if row['query'] else '',
                    'duration_seconds': int(duration),
                    'query_duration_seconds': int(query_duration)
                })
            
            cur.close()
            conn.close()
            return sessions
        
        except Exception as e:
            print(f"Ошибка при получении активных сессий: {e}")
            return []
    
    def terminate_session(self, pid, force=False):
        """
        Завершить сессию по PID
        
        Args:
            pid: Process ID сессии
            force: True = pg_terminate_backend (жесткое завершение)
                   False = pg_cancel_backend (отмена запроса)
        
        Returns:
            dict: результат операции
        """
        try:
            conn = self.get_db_connection()
            cur = conn.cursor()
            
            if force:
                # Жесткое завершение - убивает всю сессию
                # SQL: SELECT pg_terminate_backend(pid);
                cur.execute("SELECT pg_terminate_backend(%s)", (pid,))
            else:
                # Мягкое завершение - отменяет текущий запрос
                # SQL: SELECT pg_cancel_backend(pid);
                cur.execute("SELECT pg_cancel_backend(%s)", (pid,))
            
            result = cur.fetchone()[0]
            conn.commit()
            cur.close()
            conn.close()
            
            return {
                'success': result,
                'pid': pid,
                'method': 'terminate' if force else 'cancel',
                'message': f"Сессия {pid} {'завершена' if force else 'отменена'}"
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'pid': pid
            }
    
    def get_sessions_stats(self):
        """
        Получить статистику по сессиям
        
        Returns:
            dict: статистика сессий
        """
        try:
            conn = self.get_db_connection()
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            
            cur.execute("""
                SELECT
                    count(*) as total,
                    count(*) FILTER (WHERE state = 'active') as active,
                    count(*) FILTER (WHERE state = 'idle') as idle,
                    count(*) FILTER (WHERE state = 'idle in transaction') as idle_in_tx,
                    count(*) FILTER (WHERE wait_event_type = 'Lock') as waiting_lock
                FROM pg_stat_activity
                WHERE state IS NOT NULL
                  AND pid != pg_backend_pid()
            """)
            
            row = cur.fetchone()
            stats = {
                'total': row['total'] or 0,
                'active': row['active'] or 0,
                'idle': row['idle'] or 0,
                'idle_in_tx': row['idle_in_tx'] or 0,
                'waiting_lock': row['waiting_lock'] or 0
            }
            
            cur.close()
            conn.close()
            return stats
        except Exception as e:
            print(f"Ошибка при получении статистики сессий: {e}")
            return {}
    
    def get_idle_sessions(self, timeout_seconds=300):
        """
        Получить неактивные сессии старше timeout_seconds
        
        Args:
            timeout_seconds: длительность неактивности в секундах (по умолчанию 5 минут)
        
        Returns:
            list: список неактивных сессий
        """
        try:
            conn = self.get_db_connection()
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            
            cur.execute("""
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
            """, (timeout_seconds,))
            
            sessions = [dict(row) for row in cur.fetchall()]
            cur.close()
            conn.close()
            return sessions
        except Exception as e:
            print(f"Ошибка при получении неактивных сессий: {e}")
            return []
    
    def get_long_running_queries(self, timeout_seconds=3600):
        """
        Получить долго выполняющиеся запросы (> timeout_seconds)
        
        Args:
            timeout_seconds: минимальная длительность запроса в секундах
        
        Returns:
            list: список долгих запросов
        """
        try:
            conn = self.get_db_connection()
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            
            cur.execute("""
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
            """, (timeout_seconds,))
            
            sessions = [dict(row) for row in cur.fetchall()]
            cur.close()
            conn.close()
            return sessions
        except Exception as e:
            print(f"Ошибка при получении долгих запросов: {e}")
            return []
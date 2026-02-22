"""
Модуль для работы с SQLite базой данных мониторинга.
Хранит метрики из всех подключённых БД.
"""

import sqlite3
import json
from datetime import datetime
from pathlib import Path


class MonitoringDB:
    """Работа с SQLite базой метрик мониторинга"""
    
    DB_PATH = Path(__file__).parent / 'monitoring.db'
    
    @classmethod
    def init_db(cls):
        """Инициализировать БД мониторинга и создать таблицы"""
        conn = sqlite3.connect(cls.DB_PATH)
        cur = conn.cursor()
        
        # Таблица для хранения профилей БД (которые мы мониторим)
        cur.execute("""
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
        """)
        
        # Таблица для хранения текущей статистики (перезаписывается)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS current_stats (
                id INTEGER PRIMARY KEY,
                profile_id INTEGER UNIQUE NOT NULL,
                db_type TEXT NOT NULL,
                stats_json TEXT NOT NULL,
                collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(profile_id) REFERENCES monitored_profiles(id)
            )
        """)
        
        # Таблица для истории метрик (опционально, для графиков)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS metrics_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                profile_id INTEGER NOT NULL,
                metric_name TEXT NOT NULL,
                metric_value REAL,
                collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(profile_id) REFERENCES monitored_profiles(id)
            )
        """)
        
        conn.commit()
        conn.close()
    
    @classmethod
    def save_stats(cls, profile_id, stats, db_type):
        """
        Сохранить статистику БД
        
        Args:
            profile_id: ID профиля БД
            stats: dict с статистикой
            db_type: тип БД ('postgresql' или 'oracle')
        """
        conn = sqlite3.connect(cls.DB_PATH)
        cur = conn.cursor()
        
        stats_json = json.dumps(stats, default=str)
        now = datetime.now().isoformat()
        
        cur.execute("""
            INSERT INTO current_stats (profile_id, db_type, stats_json, collected_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(profile_id) DO UPDATE SET
                stats_json = EXCLUDED.stats_json,
                collected_at = EXCLUDED.collected_at,
                db_type = EXCLUDED.db_type
        """, (profile_id, db_type, stats_json, now))
        
        # Обновляем время последней сборки
        cur.execute("""
            UPDATE monitored_profiles
            SET last_collection = ?, last_error = NULL
            WHERE id = ?
        """, (now, profile_id))
        
        conn.commit()
        conn.close()
    
    @classmethod
    def get_stats(cls, profile_id):
        """Получить сохранённую статистику для профиля"""
        conn = sqlite3.connect(cls.DB_PATH)
        cur = conn.cursor()
        
        cur.execute("""
            SELECT stats_json, collected_at, db_type
            FROM current_stats
            WHERE profile_id = ?
        """, (profile_id,))
        
        row = cur.fetchone()
        conn.close()
        
        if not row:
            return None
        
        stats_json, collected_at, db_type = row
        stats = json.loads(stats_json)
        stats['_meta'] = {
            'collected_at': collected_at,
            'db_type': db_type,
            'profile_id': profile_id
        }
        return stats
    
    @classmethod
    def register_profile(cls, profile_id, profile_name, db_type):
        """Зарегистрировать профиль для мониторинга"""
        conn = sqlite3.connect(cls.DB_PATH)
        cur = conn.cursor()
        
        cur.execute("""
            INSERT OR IGNORE INTO monitored_profiles 
            (id, profile_id, profile_name, db_type, enabled)
            VALUES (?, ?, ?, ?, 1)
        """, (profile_id, profile_id, profile_name, db_type))
        
        conn.commit()
        conn.close()
    
    @classmethod
    def get_all_profiles(cls):
        """Получить все зарегистрированные профили для мониторинга"""
        conn = sqlite3.connect(cls.DB_PATH)
        cur = conn.cursor()
        
        cur.execute("""
            SELECT profile_id, profile_name, db_type, enabled, last_collection, last_error
            FROM monitored_profiles
            WHERE enabled = 1
            ORDER BY profile_id
        """)
        
        profiles = []
        for row in cur.fetchall():
            profiles.append({
                'profile_id': row[0],
                'profile_name': row[1],
                'db_type': row[2],
                'enabled': row[3],
                'last_collection': row[4],
                'last_error': row[5]
            })
        
        conn.close()
        return profiles
    
    @classmethod
    def log_error(cls, profile_id, error_msg):
        """Логировать ошибку при сборке статистики"""
        conn = sqlite3.connect(cls.DB_PATH)
        cur = conn.cursor()
        
        cur.execute("""
            UPDATE monitored_profiles
            SET last_error = ?, last_collection = ?
            WHERE profile_id = ?
        """, (error_msg[:500], datetime.now().isoformat(), profile_id))
        
        conn.commit()
        conn.close()


# Инициализируем БД при импорте
MonitoringDB.init_db()
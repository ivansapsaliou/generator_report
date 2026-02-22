#!/usr/bin/env python3
"""
Фоновый скрипт для периодической сборки метрик мониторинга.

Использование:
    python monitoring_collector.py  # Запуск в режиме бесконе��ного цикла
    python monitoring_collector.py --once  # Однократный запуск
"""

import time
import argparse
import sys
from datetime import datetime
from pathlib import Path

# Добавляем проект в path
sys.path.insert(0, str(Path(__file__).parent))

from db_profiles import DatabaseProfileManager
from db_adapter import DatabaseAdapter
from db_monitoring import DatabaseMonitoring
from monitoring_db import MonitoringDB


class MonitoringCollector:
    """Сборщик метрик мониторинга"""
    
    # Интервал сборки метрик (в секундах)
    COLLECTION_INTERVAL = 300  # 5 минут
    
    @staticmethod
    def collect_all_profiles():
        """Собрать метрики для всех активных профилей"""
        profiles = DatabaseProfileManager.get_all_profiles()
        
        if not profiles:
            print("[Collector] ⚠️  No database profiles found")
            return
        
        print(f"[Collector] 📊 Starting collection for {len(profiles)} profile(s)...")
        
        for profile in profiles:
            profile_id = profile['id']
            profile_name = profile['name']
            db_type = profile.get('db_type', 'postgresql')
            
            print(f"\n[Collector] Profile #{profile_id}: {profile_name} ({db_type})")
            
            try:
                start_time = time.time()
                
                # Регистрируем профиль в БД мониторинга
                MonitoringDB.register_profile(profile_id, profile_name, db_type)
                
                # Получаем подключение
                from db_connection import get_db_connection_by_profile
                conn = get_db_connection_by_profile(profile_id)
                
                # Собираем статистику
                stats = DatabaseMonitoring.get_database_stats(conn, db_type)
                conn.close()
                
                # Сохраняем в SQLite
                duration = time.time() - start_time
                MonitoringDB.save_stats(profile_id, stats, db_type)
                
                print(f"  ✅ Collected in {duration:.2f}s")
                
            except Exception as e:
                error_msg = str(e)
                print(f"  ❌ Error: {error_msg[:100]}")
                MonitoringDB.log_error(profile_id, error_msg)
    
    @staticmethod
    def run_once():
        """Однократный запуск сборки"""
        print(f"[Collector] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Starting collection...")
        MonitoringCollector.collect_all_profiles()
        print(f"[Collector] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Collection finished")
    
    @staticmethod
    def run_daemon(interval=None):
        """Запуск в режиме демона с периодической сборкой"""
        if interval is None:
            interval = MonitoringCollector.COLLECTION_INTERVAL
        
        print(f"[Collector] 🔄 Starting daemon mode (interval: {interval}s)")
        print(f"[Collector] Press Ctrl+C to stop")
        
        try:
            while True:
                MonitoringCollector.run_once()
                print(f"[Collector] ⏳ Next collection in {interval}s...\n")
                time.sleep(interval)
        
        except KeyboardInterrupt:
            print("\n[Collector] ⏹️  Stopped by user")
            sys.exit(0)
        except Exception as e:
            print(f"[Collector] ❌ Fatal error: {e}")
            sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description='Database monitoring metrics collector'
    )
    parser.add_argument(
        '--once',
        action='store_true',
        help='Run once and exit (default: daemon mode)'
    )
    parser.add_argument(
        '--interval',
        type=int,
        default=300,
        help='Collection interval in seconds (default: 300)'
    )
    
    args = parser.parse_args()
    
    if args.once:
        MonitoringCollector.run_once()
    else:
        MonitoringCollector.run_daemon(args.interval)


if __name__ == '__main__':
    main()
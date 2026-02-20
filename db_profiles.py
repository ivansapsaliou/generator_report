import json
import os
from pathlib import Path
from datetime import datetime

class DatabaseProfileManager:
    """Управление сохранёнными подключениями к БД"""
    
    PROFILES_FILE = os.path.join(os.path.dirname(__file__), 'data', 'db_profiles.json')
    
    @staticmethod
    def ensure_storage_dir():
        """Создать директорию data если не существует"""
        os.makedirs(os.path.dirname(DatabaseProfileManager.PROFILES_FILE), exist_ok=True)
    
    @staticmethod
    def get_all_profiles():
        """Получить все сохранённые подключения"""
        DatabaseProfileManager.ensure_storage_dir()
        if not os.path.exists(DatabaseProfileManager.PROFILES_FILE):
            return []
        
        try:
            with open(DatabaseProfileManager.PROFILES_FILE, 'r', encoding='utf-8') as f:
                profiles = json.load(f)
                # Добавляем db_type если его нет (для обратной совместимости)
                for profile in profiles:
                    if 'db_type' not in profile:
                        profile['db_type'] = 'postgresql'
                return profiles
        except (json.JSONDecodeError, IOError):
            return []
    
    @staticmethod
    def get_profile(profile_id):
        """Получить подключение по ID"""
        profiles = DatabaseProfileManager.get_all_profiles()
        for profile in profiles:
            if profile['id'] == profile_id:
                # Добавляем db_type если его нет
                if 'db_type' not in profile:
                    profile['db_type'] = 'postgresql'
                return profile
        return None
    
    @staticmethod
    def save_profile(profile_data):
        """Сохранить новое или обновить существующее подключение"""
        DatabaseProfileManager.ensure_storage_dir()
        profiles = DatabaseProfileManager.get_all_profiles()
        
        # По умолчанию PostgreSQL если не указан тип
        if 'db_type' not in profile_data:
            profile_data['db_type'] = 'postgresql'
        
        # Генерируем ID если это новый профиль
        if 'id' not in profile_data:
            profile_data['id'] = max([p.get('id', 0) for p in profiles], default=0) + 1
        
        # Добавляем метаданные
        profile_data['created_at'] = profile_data.get('created_at', datetime.now().isoformat())
        profile_data['updated_at'] = datetime.now().isoformat()
        
        # Обновляем или добавляем
        existing_index = next((i for i, p in enumerate(profiles) if p['id'] == profile_data['id']), -1)
        if existing_index >= 0:
            profiles[existing_index] = profile_data
        else:
            profiles.append(profile_data)
        
        # Сохраняем файл
        with open(DatabaseProfileManager.PROFILES_FILE, 'w', encoding='utf-8') as f:
            json.dump(profiles, f, indent=2, ensure_ascii=False)
        
        return profile_data
    
    @staticmethod
    def delete_profile(profile_id):
        """Удалить подключение"""
        profiles = DatabaseProfileManager.get_all_profiles()
        profiles = [p for p in profiles if p['id'] != profile_id]
        
        DatabaseProfileManager.ensure_storage_dir()
        with open(DatabaseProfileManager.PROFILES_FILE, 'w', encoding='utf-8') as f:
            json.dump(profiles, f, indent=2, ensure_ascii=False)
        
        return True
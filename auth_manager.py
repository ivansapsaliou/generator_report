"""
Модуль управления пользователями с использованием SQLite
"""

import sqlite3
import hashlib
import secrets
from pathlib import Path
from datetime import datetime


class AuthManager:
    """Менеджер авторизации с SQLite"""
    
    # Путь к БД (в корне проекта)
    DB_PATH = Path(__file__).parent / 'users.db'
    
    @classmethod
    def init_db(cls):
        """Инициализировать БД и создать таблицу пользователей"""
        conn = sqlite3.connect(cls.DB_PATH)
        cur = conn.cursor()
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                email TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                salt TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_login TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE
            )
        """)
        
        # Создаём таблицу сессий для отслеживания входов
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                token TEXT UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP NOT NULL,
                ip_address TEXT,
                user_agent TEXT,
                FOREIGN KEY(user_id) REFERENCES users(id)
            )
        """)
        
        conn.commit()
        conn.close()
    
    @staticmethod
    def hash_password(password, salt=None):
        """Хеширование пароля с солью"""
        if salt is None:
            salt = secrets.token_hex(16)
        
        # PBKDF2 для безопасности
        password_hash = hashlib.pbkdf2_hmac(
            'sha256',
            password.encode('utf-8'),
            salt.encode('utf-8'),
            100000  # 100k итерации
        ).hex()
        
        return password_hash, salt
    
    @classmethod
    def register_user(cls, username, email, password):
        """
        Регистрация нового пользователя
        
        Returns:
            dict: {'success': bool, 'message': str, 'user_id': int}
        """
        # Валидация
        if not username or len(username) < 3:
            return {'success': False, 'message': 'Имя пользователя должно быть не менее 3 символов'}
        
        if not email or '@' not in email:
            return {'success': False, 'message': 'Укажите корректный email'}
        
        if not password or len(password) < 6:
            return {'success': False, 'message': 'Пароль должен быть не менее 6 символов'}
        
        try:
            password_hash, salt = cls.hash_password(password)
            
            conn = sqlite3.connect(cls.DB_PATH)
            cur = conn.cursor()
            
            cur.execute("""
                INSERT INTO users (username, email, password_hash, salt)
                VALUES (?, ?, ?, ?)
            """, (username, email, password_hash, salt))
            
            user_id = cur.lastrowid
            conn.commit()
            conn.close()
            
            return {
                'success': True,
                'message': 'Пользователь успешно зарегистрирован',
                'user_id': user_id
            }
        
        except sqlite3.IntegrityError as e:
            if 'username' in str(e):
                return {'success': False, 'message': 'Это имя пользователя уже занято'}
            elif 'email' in str(e):
                return {'success': False, 'message': 'Этот email уже зарегистрирован'}
            else:
                return {'success': False, 'message': 'Ошибка регистрации'}
        
        except Exception as e:
            return {'success': False, 'message': f'Ошибка: {str(e)}'}
    
    @classmethod
    def authenticate_user(cls, username, password):
        """
        Аутентификация пользователя
        
        Returns:
            dict: {'success': bool, 'message': str, 'user_id': int, 'username': str, 'email': str}
        """
        try:
            conn = sqlite3.connect(cls.DB_PATH)
            cur = conn.cursor()
            
            cur.execute("""
                SELECT id, username, email, password_hash, salt, is_active
                FROM users
                WHERE username = ?
            """, (username,))
            
            row = cur.fetchone()
            
            if not row:
                return {'success': False, 'message': 'Неверное имя пользователя или пароль'}
            
            user_id, username, email, stored_hash, salt, is_active = row
            
            if not is_active:
                return {'success': False, 'message': 'Учётная запись деактивирована'}
            
            # Проверяем пароль
            password_hash, _ = cls.hash_password(password, salt)
            
            if password_hash != stored_hash:
                return {'success': False, 'message': 'Неверное имя пользователя или пароль'}
            
            # Обновляем время последнего входа
            cur.execute("""
                UPDATE users SET last_login = CURRENT_TIMESTAMP WHERE id = ?
            """, (user_id,))
            conn.commit()
            
            conn.close()
            
            return {
                'success': True,
                'message': 'Успешная авторизация',
                'user_id': user_id,
                'username': username,
                'email': email
            }
        
        except Exception as e:
            return {'success': False, 'message': f'Ошибка: {str(e)}'}
    
    @classmethod
    def create_session(cls, user_id, token=None, ip_address=None, user_agent=None):
        """Создать сессию для пользователя"""
        if token is None:
            token = secrets.token_urlsafe(32)
        
        from datetime import timedelta
        expires_at = (datetime.now() + timedelta(days=30)).isoformat()
        
        try:
            conn = sqlite3.connect(cls.DB_PATH)
            cur = conn.cursor()
            
            cur.execute("""
                INSERT INTO sessions (user_id, token, expires_at, ip_address, user_agent)
                VALUES (?, ?, ?, ?, ?)
            """, (user_id, token, expires_at, ip_address, user_agent))
            
            conn.commit()
            conn.close()
            
            return {'success': True, 'token': token}
        
        except Exception as e:
            return {'success': False, 'message': str(e)}
    
    @classmethod
    def verify_session(cls, token):
        """Проверить валидность сессии"""
        try:
            conn = sqlite3.connect(cls.DB_PATH)
            cur = conn.cursor()
            
            cur.execute("""
                SELECT user_id, expires_at FROM sessions WHERE token = ?
            """, (token,))
            
            row = cur.fetchone()
            conn.close()
            
            if not row:
                return {'success': False, 'user_id': None}
            
            user_id, expires_at = row
            
            # Проверяем срок истечения
            if datetime.fromisoformat(expires_at) < datetime.now():
                return {'success': False, 'user_id': None}
            
            return {'success': True, 'user_id': user_id}
        
        except Exception as e:
            return {'success': False, 'user_id': None}
    
    @classmethod
    def get_user_by_id(cls, user_id):
        """Получить данные пользователя по ID"""
        try:
            conn = sqlite3.connect(cls.DB_PATH)
            cur = conn.cursor()
            
            cur.execute("""
                SELECT id, username, email, created_at, last_login, is_active
                FROM users WHERE id = ?
            """, (user_id,))
            
            row = cur.fetchone()
            conn.close()
            
            if not row:
                return None
            
            return {
                'id': row[0],
                'username': row[1],
                'email': row[2],
                'created_at': row[3],
                'last_login': row[4],
                'is_active': row[5]
            }
        
        except Exception as e:
            return None
    
    @classmethod
    def delete_session(cls, token):
        """Удалить сессию (logout)"""
        try:
            conn = sqlite3.connect(cls.DB_PATH)
            cur = conn.cursor()
            
            cur.execute("DELETE FROM sessions WHERE token = ?", (token,))
            
            conn.commit()
            conn.close()
            
            return {'success': True}
        
        except Exception as e:
            return {'success': False}
    
    @classmethod
    def change_password(cls, user_id, old_password, new_password):
        """Изменить пароль пользователя"""
        try:
            # Получаем текущий хеш и соль
            conn = sqlite3.connect(cls.DB_PATH)
            cur = conn.cursor()
            
            cur.execute("""
                SELECT password_hash, salt FROM users WHERE id = ?
            """, (user_id,))
            
            row = cur.fetchone()
            
            if not row:
                return {'success': False, 'message': 'Пользователь не найден'}
            
            stored_hash, salt = row
            
            # Проверяем старый пароль
            old_hash, _ = cls.hash_password(old_password, salt)
            
            if old_hash != stored_hash:
                return {'success': False, 'message': 'Старый пароль неверен'}
            
            # Хешируем новый пароль
            new_hash, new_salt = cls.hash_password(new_password)
            
            # Обновляем в БД
            cur.execute("""
                UPDATE users SET password_hash = ?, salt = ? WHERE id = ?
            """, (new_hash, new_salt, user_id))
            
            conn.commit()
            conn.close()
            
            return {'success': True, 'message': 'Пароль успешно изменён'}
        
        except Exception as e:
            return {'success': False, 'message': str(e)}


# Инициализируем БД при импорте модуля
AuthManager.init_db()
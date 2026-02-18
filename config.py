import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production')

    # PostgreSQL Configuration
    DB_HOST = os.environ.get('DB_HOST', '10.100.102.90')
    DB_PORT = os.environ.get('DB_PORT', '5432')
    DB_NAME = os.environ.get('DB_NAME', 'rul_jkh')
    DB_USER = os.environ.get('DB_USER', 'rul_developer')
    DB_PASSWORD = os.environ.get('DB_PASSWORD', '1234567890!@#$%^&*()')

    SQLALCHEMY_DATABASE_URI = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

    # SSH Tunnel Configuration
    SSH_ENABLED = os.environ.get('SSH_ENABLED', 'false').lower() == 'true'
    SSH_HOST = os.environ.get('SSH_HOST', '10.100.102.90')
    SSH_PORT = int(os.environ.get('SSH_PORT', 22))
    SSH_USER = os.environ.get('SSH_USER', 'nladmin')
    SSH_PASSWORD = os.environ.get('SSH_PASSWORD', 'D#f;mV@KbX')
    SSH_KEY_PATH = os.environ.get('SSH_KEY_PATH', '')
    
    # Remote database configuration (via SSH tunnel)
    SSH_REMOTE_DB_HOST = os.environ.get('SSH_REMOTE_DB_HOST', '127.0.0.1')
    SSH_REMOTE_DB_PORT = int(os.environ.get('SSH_REMOTE_DB_PORT', 5432))
    SSH_LOCAL_PORT = int(os.environ.get('SSH_LOCAL_PORT', 15432))
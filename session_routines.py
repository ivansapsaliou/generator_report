"""
Routes для управления сессиями БД.
Добавить эти роуты в основное приложение Flask.
"""

from flask import jsonify, request
from session_monitor import SessionMonitor


def register_session_routes(app, session_monitor=None):
    """
    Регистрация маршрутов для управления сессиями
    
    Args:
        app: Flask приложение
        session_monitor: экземпляр SessionMonitor (создается, если не переда��)
    """
    
    if session_monitor is None:
        session_monitor = SessionMonitor(app.config)

    
    # WEB интерфейс
    @app.route('/session-monitor')
    def monitor_page():
        """Страница монитора сессий"""
        from flask import render_template
        return render_template('session_monitor.html')
    
    @app.route('/api/sessions', methods=['GET'])
    def get_sessions():
        """Получить список активных сессий"""
        try:
            exclude_current = request.args.get('exclude_current', 'true').lower() == 'true'
            sessions = session_monitor.get_active_sessions(exclude_current=exclude_current)
            return jsonify({
                'success': True,
                'data': sessions,
                'count': len(sessions)
            })
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    
    @app.route('/api/sessions/stats', methods=['GET'])
    def get_sessions_stats():
        """Получить статистику сессий"""
        try:
            stats = session_monitor.get_session_stats()
            return jsonify({
                'success': True,
                'data': stats
            })
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    
    @app.route('/api/sessions/idle', methods=['GET'])
    def get_idle_sessions():
        """Получить список неактивных сессий"""
        try:
            idle_timeout = request.args.get('timeout', 300, type=int)
            sessions = session_monitor.get_idle_sessions(idle_timeout)
            return jsonify({
                'success': True,
                'data': sessions,
                'count': len(sessions),
                'timeout_seconds': idle_timeout
            })
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    
    @app.route('/api/sessions/long-queries', methods=['GET'])
    def get_long_queries():
        """Получить список долго выполняющихся запросов"""
        try:
            query_timeout = request.args.get('timeout', 3600, type=int)
            sessions = session_monitor.get_long_running_queries(query_timeout)
            return jsonify({
                'success': True,
                'data': sessions,
                'count': len(sessions),
                'timeout_seconds': query_timeout
            })
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    
    @app.route('/api/sessions/blocking', methods=['GET'])
    def get_blocking_sessions():
        """Получить информацию о заблокированных сессиях"""
        try:
            sessions = session_monitor.get_blocking_sessions()
            return jsonify({
                'success': True,
                'data': sessions,
                'count': len(sessions)
            })
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    
    @app.route('/api/sessions/kill/<int:pid>', methods=['POST'])
    def kill_session(pid):
        """Завершить конкретную сессию"""
        try:
            terminate = request.json.get('terminate', True) if request.json else True
            success, message = session_monitor.kill_session(pid, terminate=terminate)
            return jsonify({
                'success': success,
                'pid': pid,
                'message': message
            }), (200 if success else 400)
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    
    @app.route('/api/sessions/cancel/<int:pid>', methods=['POST'])
    def cancel_query(pid):
        """Отменить текущий запрос в сессии (мягкий способ)"""
        try:
            success, message = session_monitor.cancel_query(pid)
            return jsonify({
                'success': success,
                'pid': pid,
                'message': message
            }), (200 if success else 400)
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    
    @app.route('/api/sessions/kill-idle', methods=['POST'])
    def kill_idle_sessions():
        """Завершить все неактивные сессии"""
        try:
            data = request.json or {}
            idle_timeout = data.get('timeout', 300)
            exclude_pids = data.get('exclude_pids', [])
            terminate = data.get('terminate', True)
            
            results = session_monitor.kill_idle_sessions(idle_timeout, exclude_pids, terminate)
            return jsonify({
                'success': True,
                'data': results
            })
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    
    @app.route('/api/sessions/kill-long-queries', methods=['POST'])
    def kill_long_queries():
        """Завершить все долго выполняющиеся запросы"""
        try:
            data = request.json or {}
            query_timeout = data.get('timeout', 3600)
            exclude_pids = data.get('exclude_pids', [])
            terminate = data.get('terminate', True)
            
            results = session_monitor.kill_long_running_queries(query_timeout, exclude_pids, terminate)
            return jsonify({
                'success': True,
                'data': results
            })
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    
    @app.route('/api/monitor/start', methods=['POST'])
    def start_monitor():
        """Запустить фоновый мониторинг"""
        try:
            data = request.json or {}
            check_interval = data.get('check_interval', 30)
            
            session_monitor.start_monitoring(check_interval)
            return jsonify({
                'success': True,
                'message': 'Мониторинг запущен',
                'config': {
                    'check_interval': check_interval
                }
            })
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    
    @app.route('/api/monitor/stop', methods=['POST'])
    def stop_monitor():
        """Остановить фоновый мониторинг"""
        try:
            session_monitor.stop_monitoring()
            return jsonify({
                'success': True,
                'message': 'Мониторинг остановлен'
            })
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    
    return session_monitor
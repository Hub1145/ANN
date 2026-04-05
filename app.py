from flask import Flask, render_template, request, jsonify, send_file
from flask_socketio import SocketIO, emit
import json
import logging
import os
import threading
import requests
from bot_engine import BinanceTradingBotEngine
from translations_py import TRANSLATIONS

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SESSION_SECRET', 'binance-bot-secret')
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

config_file = 'config.json'
bot_engine = None
server_ip = "Unknown"

def _fetch_server_ip():
    global server_ip
    try:
        server_ip = requests.get('https://api.ipify.org', timeout=10).text
        logging.info(f"Detected Server IP: {server_ip}")
    except Exception as e:
        logging.warning(f"Could not determine Server IP: {e}")

threading.Thread(target=_fetch_server_ip, daemon=True).start()

def load_config():
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
        return config
    except Exception as e:
        logging.error(f"Error loading config in app.py: {e}")
        return {}

def save_config(config):
    with open(config_file, 'w') as f:
        json.dump(config, f, indent=2)

def emit_to_client(event, data):
    socketio.emit(event, data)

@app.route('/')
def index():
    return render_template('dashboard.html', translations=TRANSLATIONS, server_ip=server_ip)

# REST API routes for config are now handled via Socket.IO events ('get_config', 'update_config')

@app.route('/api/shutdown', methods=['POST'])
def shutdown():
    global bot_engine
    if bot_engine:
        bot_engine.stop()

    def kill_server():
        import time
        import signal
        time.sleep(1)
        os.kill(os.getpid(), signal.SIGINT)

    threading.Thread(target=kill_server).start()
    return jsonify({'success': True, 'message': 'Server shutting down...'})

@app.route('/api/download_logs')
def download_logs():
    try:
        log_file = 'binance_bot.log'
        if not os.path.exists(log_file):
             return jsonify({'error': 'Log file not found'}), 404
        return send_file(log_file, mimetype='text/plain', as_attachment=True, download_name='bot_log.log')
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# API key testing is now handled via Socket.IO event 'test_api_key'

@socketio.on('connect')
def handle_connect():
    global bot_engine
    if not bot_engine:
        bot_engine = BinanceTradingBotEngine(config_file, emit_to_client, server_ip=server_ip)

    emit('bot_status', {'running': bot_engine.is_running})
    emit('clear_console', {})
    for log in list(bot_engine.console_logs):
        emit('console_log', log)
    bot_engine._emit_account_update()
    bot_engine._emit_latest_prices()

@socketio.on('start_bot')
def handle_start_bot():
    global bot_engine
    try:
        if not bot_engine:
            bot_engine = BinanceTradingBotEngine(config_file, emit_to_client, server_ip=server_ip)

        if not bot_engine.is_running:
            bot_engine.start()
            emit('bot_status', {'running': True}, broadcast=True)
            emit('success', {'message': 'Bot started successfully'})
        else:
            emit('bot_status', {'running': True})
    except Exception as e:
        logging.error(f"Error starting bot: {e}")
        emit('bot_status', {'running': False})
        emit('error', {'message': str(e)})

@socketio.on('stop_bot')
def handle_stop_bot():
    global bot_engine
    try:
        if bot_engine and bot_engine.is_running:
            bot_engine.stop()
            emit('bot_status', {'running': False}, broadcast=True)
            emit('success', {'message': 'Bot stopped successfully'})
        else:
            emit('bot_status', {'running': False})
    except Exception as e:
        logging.error(f"Error stopping bot: {e}")
        emit('bot_status', {'running': bot_engine.is_running if bot_engine else False})
        emit('error', {'message': str(e)})

@socketio.on('clear_console')
def handle_clear_console():
    if bot_engine:
        bot_engine.console_logs.clear()
    emit('console_cleared', {})

@socketio.on('get_config')
def handle_get_config():
    emit('config_data', load_config())

@socketio.on('update_config')
def handle_update_config(new_config):
    global bot_engine
    try:
        # Basic validation
        required_top_keys = ['api_accounts', 'is_demo', 'symbols', 'symbol_strategies', 'language']
        if not all(k in new_config for k in required_top_keys):
            missing = [k for k in required_top_keys if k not in new_config]
            emit('error', {'message': f'Missing required configuration keys: {missing}'})
            return

        save_config(new_config)
        if bot_engine:
            bot_engine.apply_live_config_update(new_config)
        
        emit('config_data', new_config, broadcast=True)
        emit('success', {'message': 'Configuration updated successfully'})
    except Exception as e:
        emit('error', {'message': str(e)})

@socketio.on('test_api_key')
def handle_test_api_key(data):
    try:
        api_key = data.get('api_key')
        api_secret = data.get('api_secret')
        is_demo = data.get('is_demo', True)
        
        if not api_key or not api_secret:
            emit('test_api_result', {'success': False, 'message': 'API Key and Secret are required.'})
            return

        success, msg = BinanceTradingBotEngine.test_account(api_key, api_secret, is_demo=is_demo)
        
        # Translate test result message if possible
        lang = load_config().get('language', 'pt-BR')
        if msg == "Connection successful":
            msg = TRANSLATIONS[lang].get('conn_success', msg)
            
        emit('test_api_result', {'success': success, 'message': msg})
    except Exception as e:
        emit('test_api_result', {'success': False, 'message': str(e)})

@socketio.on('start_add_trade')
def handle_start_add_trade(data):
    if bot_engine:
        account_idx = data.get('account_idx')
        symbol = data.get('symbol')
        settings = data.get('settings')
        if account_idx is not None and symbol:
            bot_engine.start_add_trade(account_idx, symbol, settings)

@socketio.on('close_trade')
def handle_close_trade(data):
    if bot_engine:
        account_idx = data.get('account_idx')
        symbol = data.get('symbol')
        trade_id = data.get('trade_id')
        if account_idx is not None and symbol:
            bot_engine.close_position(account_idx, symbol, trade_id=trade_id)

@socketio.on('cancel_order')
def handle_cancel_order(data):
    if bot_engine:
        account_idx = data.get('account_idx')
        symbol = data.get('symbol')
        order_id = data.get('order_id')
        if account_idx is not None and symbol and order_id:
            bot_engine.cancel_order(account_idx, symbol, order_id)

@socketio.on('refresh_data')
def handle_refresh_data():
    if bot_engine:
        bot_engine.refresh_data()

if __name__ == '__main__':
    if not bot_engine:
        bot_engine = BinanceTradingBotEngine(config_file, emit_to_client, server_ip=server_ip)

    port = 3000
    socketio.run(app, host='0.0.0.0', port=port, debug=False, use_reloader=False, allow_unsafe_werkzeug=True)

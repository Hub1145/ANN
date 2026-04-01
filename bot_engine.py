import json
import time
import logging
import threading
from datetime import datetime
from collections import deque
from binance.client import Client
from binance.exceptions import BinanceAPIException
from translations_py import TRANSLATIONS

from handlers.config_handler import ConfigHandler
from handlers.market_handler import MarketHandler
from handlers.account_handler import AccountHandler
from handlers.order_handler import OrderHandler
from handlers.strategy_handler import StrategyHandler

class BinanceTradingBotEngine:
    def __init__(self, config_path, emit_callback, server_ip="Unknown"):
        self.emit = emit_callback
        self.server_ip = server_ip
        self.console_logs = deque(maxlen=500)
        self.last_log_times = {}
        self.trailing_state = {}
        self.grid_state = {} # (account_index, symbol) -> [ trades ]

        # Initialize Handlers
        self.config_handler = ConfigHandler(config_path)
        self.market_handler = MarketHandler(self)
        self.account_handler = AccountHandler(self)
        self.order_handler = OrderHandler(self)
        self.strategy_handler = StrategyHandler(self)

        self.language = self.config_handler.config.get('language', 'en-US')
        self.data_lock = threading.RLock()

        self.bg_clients = {}
        self.accounts = {}
        self.is_running = False
        self.stop_event = threading.Event()
        self._metadata_client_instance = None
        self._market_client_instance = None

        self._setup_logging()
        self._initialize_bg_clients()
        self.market_handler.fetch_exchange_info()
        self.market_handler.initialize_market_ws()
        threading.Thread(target=self._global_background_worker, daemon=True).start()

    def _setup_logging(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def log(self, message_or_key, level='info', account_name=None, is_key=False, **kwargs):
        timestamp = datetime.now().strftime('%H:%M:%S')
        log_entry = {'timestamp': timestamp, 'level': level, 'account_name': account_name, 'key': message_or_key if is_key else None, 'message': message_or_key if not is_key else None, 'kwargs': kwargs}
        log_entry['rendered'] = self._render_log(log_entry)
        self.console_logs.append(log_entry)
        self.emit('console_log', log_entry)

    def _render_log(self, entry):
        account_name = entry.get('account_name')
        prefix = f"[{str(account_name).strip('[]')}] " if account_name else ""
        message = TRANSLATIONS.get(self.language, {}).get(entry['key'], entry['key']).format(**entry.get('kwargs', {})) if entry.get('key') else entry.get('message', '')
        return message if prefix and message.startswith(prefix) else f"{prefix}{message}"

    def safe_api_call(self, func, *args, **kwargs):
        max_retries, backoff = 3, 1
        for i in range(max_retries):
            try: return func(*args, **kwargs)
            except BinanceAPIException as e:
                if e.code in [-1003, -1021] or "Rate limit" in str(e):
                    time.sleep(backoff); backoff *= 2
                else: raise e
            except Exception as e:
                if "timeout" in str(e).lower(): time.sleep(backoff); backoff *= 2
                else: raise e
        return func(*args, **kwargs)

    def _create_client(self, api_key, api_secret):
        testnet = self.config_handler.config.get('is_demo', True)
        # Check if we should skip due to recent failures to avoid spamming logs during maintenance
        last_fail = self.last_log_times.get('client_creation_fail', 0)
        if time.time() - last_fail < 30: return None

        try:
            client = Client(api_key.strip(), api_secret.strip(), testnet=testnet, requests_params={'timeout': 10})
            if testnet:
                client.FUTURES_URL = 'https://testnet.binancefuture.com/fapi'
            else:
                client.FUTURES_URL = 'https://fapi.binance.com/fapi'
            try:
                res = client.futures_time()
                client.timestamp_offset = res['serverTime'] - int(time.time() * 1000)
            except: pass
            return client
        except Exception as e:
            msg = str(e)
            if "restricted location" in msg.lower():
                logging.error("CRITICAL: Restricted location detected. Cannot create Binance client.")
            elif "502" in msg or "<html>" in msg:
                # Log only once every 60 seconds for gateway errors
                if time.time() - last_fail > 60:
                    logging.error("Binance API is currently unavailable (502 Bad Gateway). Retrying in background...")
                    self.last_log_times['client_creation_fail'] = time.time()
            else:
                logging.error(f"Error creating Binance client: {e}")
                self.last_log_times['client_creation_fail'] = time.time()
            return None

    @property
    def market_client(self):
        if self._market_client_instance is None: self._market_client_instance = self._create_client("", "")
        return self._market_client_instance

    @property
    def metadata_client(self):
        if self._metadata_client_instance is None:
            accs = self.config_handler.config.get('api_accounts', [])
            for acc in accs:
                if acc.get('api_key') and acc.get('api_secret'):
                    self._metadata_client_instance = self._create_client(acc['api_key'], acc['api_secret'])
                    break
            if not self._metadata_client_instance: self._metadata_client_instance = self._create_client("", "")
        return self._metadata_client_instance

    def _initialize_bg_clients(self):
        api_accounts = self.config_handler.config.get('api_accounts', [])
        testnet = self.config_handler.config.get('is_demo', True)
        from binance.streams import ThreadedWebsocketManager
        import asyncio

        # Rate limit background client creation to avoid spamming 502/HTML errors
        last_fail = self.last_log_times.get('client_creation_fail', 0)
        if time.time() - last_fail < 30: return

        for i, acc in enumerate(api_accounts):
            api_key, api_secret = acc.get('api_key', '').strip(), acc.get('api_secret', '').strip()
            if api_key and api_secret:
                # Reuse existing TWM/Client if key hasn't changed to avoid redundant connections
                if i in self.bg_clients and self.bg_clients[i].get('info', {}).get('api_key') == api_key:
                    self.bg_clients[i]['name'] = acc.get('name', f"Account {i+1}")
                    self.bg_clients[i]['info'] = acc
                    continue

                # Close old one if exists
                if i in self.bg_clients:
                    self.market_handler._stop_twm(self.bg_clients[i].get('twm'))

                client = self._create_client(api_key, api_secret)
                if not client: continue
                new_loop = asyncio.new_event_loop()
                twm = ThreadedWebsocketManager(api_key=api_key, api_secret=api_secret, testnet=testnet)
                twm._loop = new_loop
                try:
                    twm.start()
                    twm.start_futures_user_socket(callback=lambda msg, idx=i: self._handle_user_data(idx, msg))
                except: pass
                self.bg_clients[i] = {'client': client, 'twm': twm, 'name': acc.get('name', f"Account {i+1}"), 'info': acc}

    def _handle_user_data(self, idx, msg):
        event_type = msg.get('e')
        if event_type == 'ORDER_TRADE_UPDATE':
            self._process_filled_order(idx, msg.get('o', {}))
        elif event_type == 'ACCOUNT_UPDATE':
            update_data = msg.get('a', {})
            with self.data_lock:
                for b in update_data.get('B', []):
                    if b.get('a') == 'USDC': self.account_handler.account_balances[idx] = float(b.get('wb', 0))
                if idx not in self.account_handler.open_positions: self.account_handler.open_positions[idx] = {}
                for p in update_data.get('P', []):
                    symbol = p.get('s')
                    amt = float(p.get('pa', 0))
                    if amt == 0:
                        if symbol in self.account_handler.open_positions[idx]: del self.account_handler.open_positions[idx][symbol]
                    else:
                        self.account_handler.open_positions[idx][symbol] = {'symbol': symbol, 'amount': p.get('pa'), 'entryPrice': p.get('ep'), 'unrealizedProfit': p.get('up'), 'leverage': p.get('l', '20')}
            self.account_handler.emit_account_update()

    def _process_filled_order(self, idx, order_data):
        if order_data.get('X') != 'FILLED': return
        symbol, order_id = order_data.get('s'), order_data.get('i')
        avg_price, filled_qty = float(order_data.get('ap', 0)), float(order_data.get('z', 0))

        with self.data_lock:
            trades = self.grid_state.get((idx, symbol), [])
            state = None
            for t in trades:
                if order_id in t.get('initial_orders', {}) or order_id == t.get('initial_order_id'): state = t; break
                if t.get('levels') and any(l.get('tp_order_id') == order_id for l in t['levels'].values()): state = t; break

            if state:
                if order_id in state.get('initial_orders', {}) or order_id == state.get('initial_order_id'):
                    state['initial_filled'] = True
                    if 'initial_orders' not in state: state['initial_orders'] = {}
                    state['initial_orders'][order_id] = {'qty': filled_qty, 'price': avg_price, 'filled': True}
                    filled_entries = [o for o in state['initial_orders'].values() if o.get('filled')]
                    total_qty = sum(o['qty'] for o in filled_entries)
                    state['quantity'] = total_qty
                    state['avg_entry_price'] = sum(o['qty']*o['price'] for o in filled_entries)/total_qty

                    # Place TP - Ensure TP grid is placed now that position exists (REDUCE_ONLY won't fail)
                    strategy = self.config_handler.get_strategy(symbol)
                    if strategy.get('tp_enabled', True):
                        self.strategy_handler.setup_tp_targets_logic(idx, symbol, state['avg_entry_price'], strategy.get('tp_targets', []), total_qty, strategy.get('direction', 'LONG'), strategy.get('trailing_tp_enabled', False), state['trade_id'])
                        state['pending_tp_grid'] = False # Marked as placed

                elif state.get('levels'):
                    for lvl in state['levels'].values():
                        if lvl.get('tp_order_id') == order_id:
                            lvl['filled'] = True
                            # Handle reentry...

    def _global_background_worker(self):
        while not self.stop_event.is_set():
            try:
                symbols = self.config_handler.config.get('symbols', [])
                # Market data polling as fallback
                for s in symbols:
                    if time.time() - self.market_handler.get_market_data(s).get('last_update', 0) > 10:
                        try:
                            ticker = self.safe_api_call(self.market_client.futures_symbol_ticker, symbol=s)
                            with self.market_handler.market_data_lock:
                                if s not in self.market_handler.shared_market_data: self.market_handler.shared_market_data[s] = {}
                                self.market_handler.shared_market_data[s].update({'price': float(ticker['price']), 'last_update': time.time()})
                        except: pass

                for idx in list(self.bg_clients.keys()):
                    self.account_handler.update_account_metrics(idx)

                self.emit('price_update', self.market_handler.get_all_prices())
                time.sleep(10)
            except: time.sleep(10)

    def start(self):
        self.is_running = True
        self.stop_event.clear()
        api_accounts = self.config_handler.config.get('api_accounts', [])
        for i, acc in enumerate(api_accounts):
            if acc.get('enabled', True) and acc.get('api_key'):
                self.accounts[i] = self.bg_clients.get(i, {'client': self._create_client(acc['api_key'], acc['api_secret']), 'info': acc})
                for symbol in self.config_handler.config.get('symbols', []):
                    self.strategy_handler.start_symbol_thread(i, symbol)

    def stop(self):
        self.is_running = False
        self.accounts = {}

    def get_trade_quantity(self, trade):
        qty = 0.0
        if trade.get('initial_orders'):
            for o in trade['initial_orders'].values():
                if o.get('filled'): qty += o['qty']
        elif trade.get('initial_filled'): qty += trade.get('quantity', 0)
        if trade.get('levels'):
            for lvl in trade['levels'].values():
                if lvl.get('filled'): qty -= lvl.get('qty', 0)
        return max(0.0, qty)

    def check_balance_for_order(self, idx, qty, price, leverage=20, symbol=None):
        balance = self.account_handler.account_balances.get(idx, 0)
        return balance >= (qty * price / leverage) * 1.05

    def setup_strategy_for_account(self, idx, symbol):
        strategy = self.config_handler.get_strategy(symbol)
        client = self.accounts[idx]['client']
        try:
            self.safe_api_call(client.futures_change_margin_type, symbol=symbol, marginType=strategy.get('margin_type', 'CROSSED'))
            self.safe_api_call(client.futures_change_leverage, symbol=symbol, leverage=int(strategy.get('leverage', 20)))
        except: pass

    def check_and_place_initial_entry(self, idx, symbol, trade_id='manual'):
        self.strategy_handler.check_and_place_initial_entry(idx, symbol, trade_id)

    def trailing_tp_logic(self, idx, symbol):
        self.strategy_handler.trailing_tp_logic(idx, symbol)

    def tp_market_logic(self, idx, symbol):
        self.strategy_handler.tp_market_logic(idx, symbol)

    def stop_loss_logic(self, idx, symbol):
        self.strategy_handler.stop_loss_logic(idx, symbol)

    def trailing_buy_logic(self, idx, symbol):
        self.strategy_handler.trailing_buy_logic(idx, symbol)

    def conditional_logic(self, idx, symbol):
        self.strategy_handler.conditional_logic(idx, symbol)

    def apply_live_config_update(self, new_config):
        old_demo = self.config_handler.config.get('is_demo', True)
        new_demo = new_config.get('is_demo', True)

        self.config_handler.save_config(new_config)
        self.language = new_config.get('language', 'en-US')

        if old_demo != new_demo:
            self.log(f"Switching to {'DEMO' if new_demo else 'LIVE'} mode. Clearing caches...", level='info')
            self._market_client_instance = None
            self._metadata_client_instance = None
            self.market_handler.fetch_exchange_info()
            self.market_handler.initialize_market_ws()

        # Update accounts/clients if they changed or were added
        self._initialize_bg_clients()
        if self.is_running:
            api_accounts = new_config.get('api_accounts', [])
            for i, acc in enumerate(api_accounts):
                if acc.get('enabled', True) and acc.get('api_key'):
                    if i not in self.accounts or self.accounts[i].get('info', {}).get('api_key') != acc.get('api_key'):
                        self.accounts[i] = self.bg_clients.get(i)
                        for symbol in new_config.get('symbols', []):
                            self.strategy_handler.start_symbol_thread(i, symbol)

        return {'success': True}

    def close_position(self, account_idx, symbol, trade_id=None, order_type='MARKET'):
        idx = account_idx
        target_client = self.accounts[idx]['client'] if idx in self.accounts else self.bg_clients.get(idx, {}).get('client')
        if not target_client: return

        try:
            if trade_id:
                orders_to_cancel = []
                qty_to_close = 0
                with self.data_lock:
                    trades = self.grid_state.get((idx, symbol), [])
                    trade = next((t for t in trades if t.get('trade_id') == trade_id), None)
                    if not trade: return
                    if trade.get('initial_order_id'): orders_to_cancel.append(trade['initial_order_id'])
                    if trade.get('levels'):
                        for lvl in trade['levels'].values():
                            if lvl.get('tp_order_id'): orders_to_cancel.append(lvl['tp_order_id'])
                    qty_to_close = self.get_trade_quantity(trade)

                for oid in orders_to_cancel:
                    try: self.safe_api_call(target_client.futures_cancel_order, symbol=symbol, orderId=oid)
                    except: pass

                if qty_to_close > 0:
                    strategy = self.config_handler.get_strategy(symbol)
                    side = 'SELL' if strategy.get('direction', 'LONG') == 'LONG' else 'BUY'
                    if order_type == 'LIMIT':
                        p = float(strategy.get('sl_order_price', 0)) or self.market_handler.get_price(symbol)
                        self.order_handler.place_limit_order(idx, symbol, side, qty_to_close, p, reduce_only=True)
                    else:
                        self.safe_api_call(target_client.futures_create_order, symbol=symbol, side=side, type='MARKET', quantity=self.order_handler.format_quantity(symbol, qty_to_close))

                with self.data_lock:
                    self.grid_state[(idx, symbol)] = [t for t in self.grid_state.get((idx, symbol), []) if t.get('trade_id') != trade_id]
            else:
                self.safe_api_call(target_client.futures_cancel_all_open_orders, symbol=symbol)
                pos = self.safe_api_call(target_client.futures_position_information, symbol=symbol)
                for p in (pos or []):
                    if p.get('symbol') == symbol:
                        amt = float(p.get('positionAmt', 0))
                        if amt != 0:
                            self.safe_api_call(target_client.futures_create_order, symbol=symbol, side='SELL' if amt > 0 else 'BUY', type='MARKET', quantity=self.order_handler.format_quantity(symbol, abs(amt)))
                with self.data_lock:
                    if (idx, symbol) in self.grid_state: del self.grid_state[(idx, symbol)]
            self.account_handler.emit_account_update()
        except: pass

    def start_add_trade(self, account_idx, symbol, settings=None):
        self.strategy_handler.check_and_place_initial_entry(account_idx, symbol, trade_id=f"man-{int(time.time())}", override_strategy=settings)

    def refresh_data(self):
        for i in range(len(self.config_handler.config.get('api_accounts', []))):
            self.account_handler.update_account_metrics(i, force=True)
        self.account_handler.emit_account_update()

    def cancel_order(self, account_idx, symbol, order_id):
        idx = account_idx # Simplified for brevity, same logic as before should be here
        target_client = self.accounts[idx]['client'] if idx in self.accounts else self.bg_clients.get(idx, {}).get('client')
        if not target_client: return False
        try:
            self.safe_api_call(target_client.futures_cancel_order, symbol=symbol, orderId=order_id)
            self.account_handler.update_account_metrics(idx, force=True)
            return True
        except: return False

    def _emit_account_update(self): self.account_handler.emit_account_update()
    def _emit_latest_prices(self): self.emit('price_update', self.market_handler.get_all_prices())

    @staticmethod
    def test_account(api_key, api_secret, is_demo=True):
        try:
            client = Client(api_key.strip(), api_secret.strip(), testnet=is_demo, requests_params={'timeout': 20})
            if is_demo: client.FUTURES_URL = 'https://testnet.binancefuture.com/fapi'
            else: client.FUTURES_URL = 'https://fapi.binance.com/fapi'
            client.futures_account_balance()
            return True, "Connection successful"
        except Exception as e: return False, str(e)

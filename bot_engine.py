import json
import math
from decimal import Decimal, ROUND_FLOOR, ROUND_HALF_UP
import time
import logging
import threading
import asyncio
from datetime import datetime
from collections import deque
from binance.client import Client
from binance.streams import ThreadedWebsocketManager
from binance.exceptions import BinanceAPIException
from translations_py import TRANSLATIONS

class BinanceTradingBotEngine:
    def __init__(self, config_path, emit_callback, server_ip="Unknown"):
        self.config_path = config_path
        self.emit = emit_callback
        self.server_ip = server_ip
        self.console_logs = deque(maxlen=500)
        self.config = self._load_config()
        self.language = self.config.get('language', 'pt-BR')

        # Locks must be initialized before calling any methods that use them
        self.data_lock = threading.RLock()
        self.twm_lock = threading.Lock()
        self.market_data_lock = threading.Lock()
        self.config_update_lock = threading.Lock()

        self.bg_clients = {} # account_index -> { 'client': Client, 'name': str }
        self._initialize_bg_clients()
        self._metadata_client_instance = None
        self._market_client_instance = None
        self.market_twm = None

        self.is_running = False
        self.stop_event = threading.Event()

        self.accounts = {} # account_index -> { 'client': Client, 'twm': ThreadedWebsocketManager, 'info': account_config }
        self.exchange_info = {} # symbol -> info

        # Shared market data: symbol -> { 'price': float, 'last_update': float, 'info': info }
        self.shared_market_data = {}
        self.max_leverages = {} # symbol -> max_leverage

        # Grid state: (account_index, symbol) -> { 'initial_filled': bool, 'levels': { level: { 'tp_id': id, 'rb_id': id } } }
        self.grid_state = {}

        # Threads: (account_index, symbol) -> Thread
        self.symbol_threads = {}
        
        # Dashboard metrics
        self.account_balances = {} # account_index -> balance
        self.account_errors = {} # account_index -> error message
        self.account_last_update = {} # account_index -> timestamp
        self.open_positions = {} # account_index -> { symbol: position_dict }
        self.open_orders = {} # account_index -> [ order_dict ]
        # Trailing TP/SL/Buy/Peak state: key -> value
        self.trailing_state = {}
        self.last_log_times = {} # key -> timestamp
        
        # Local cache for recently placed client order IDs to prevent -4116
        # Maps (account_index, symbol) -> { client_id: timestamp }
        self.recent_client_ids = {}
        self.tp_setup_locks = {} # (account_index, symbol, trade_id) -> Lock

        self._setup_logging()
        
        # Start global background tasks immediately (pricing, metrics)
        self._background_tasks_started = False
        self._initialize_market_ws()
        threading.Thread(target=self._global_background_worker, daemon=True).start()

    def _setup_logging(self):
        numeric_level = logging.INFO
        root_logger = logging.getLogger()
        root_logger.setLevel(numeric_level)
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        ch = logging.StreamHandler()
        ch.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        root_logger.addHandler(ch)
        fh = logging.FileHandler('binance_bot.log', encoding='utf-8')
        fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        root_logger.addHandler(fh)

    def _load_config(self):
        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)

            # Security: Allow environment variables to override empty config keys
            import os
            for i, acc in enumerate(config.get('api_accounts', [])):
                env_key = os.environ.get(f'BINANCE_API_KEY_{i+1}')
                env_secret = os.environ.get(f'BINANCE_API_SECRET_{i+1}')
                if env_key and not acc.get('api_key'):
                    acc['api_key'] = env_key
                if env_secret and not acc.get('api_secret'):
                    acc['api_secret'] = env_secret

            return config
        except Exception as e:
            logging.error(f"Error loading config: {e}")
            return {}

    def _get_strategy(self, idx, symbol):
        """Helper to get global strategy for a symbol. All accounts use the same settings."""
        return self.config.get('symbol_strategies', {}).get(symbol, {})

    def _t(self, key, **kwargs):
        """Helper to get translated strings."""
        lang = self.language
        template = TRANSLATIONS.get(lang, {}).get(key, key)
        try:
            return template.format(**kwargs)
        except Exception:
            return template

    def log(self, message_or_key, level='info', account_name=None, is_key=False, **kwargs):
        # We store structured logs now to allow re-translation
        timestamp = datetime.now().strftime('%H:%M:%S')

        log_entry = {
            'timestamp': timestamp,
            'level': level,
            'account_name': account_name,
            'key': message_or_key if is_key else None,
            'message': message_or_key if not is_key else None,
            'kwargs': kwargs
        }

        # Add a rendered version for the immediate display
        rendered_msg = self._render_log(log_entry)
        log_entry['rendered'] = rendered_msg

        self.console_logs.append(log_entry)
        self.emit('console_log', log_entry)

        if level == 'error': logging.error(rendered_msg)
        elif level == 'warning': logging.warning(rendered_msg)
        else: logging.info(rendered_msg)

    def _render_log(self, entry):
        account_name = entry.get('account_name')
        prefix = f"[{account_name}] " if account_name else ""

        if entry.get('key'):
            message = self._t(entry['key'], **entry.get('kwargs', {}))
        else:
            message = entry.get('message', '')

        # Standardize prefix: if account_name is provided, use it.
        # Ensure name doesn't already contain brackets if constructed elsewhere.
        clean_name = str(account_name).strip('[]') if account_name else ""
        prefix = f"[{clean_name}] " if clean_name else ""
        
        # Avoid double prefix if the message already starts with the account name
        if prefix and message.startswith(prefix):
            return message
            
        return f"{prefix}{message}"

    def _create_client(self, api_key, api_secret):
        try:
            testnet = self.config.get('is_demo', True)
            # Enable automatic time synchronization and trim keys
            # Use a shorter timeout for the initial connection to avoid blocking too long
            client = Client(api_key.strip(), api_secret.strip(), testnet=testnet, requests_params={'timeout': 10})

            # Explicitly set Futures URL if we are in Demo mode
            if testnet:
                client.FUTURES_URL = 'https://testnet.binancefuture.com/fapi'
                client.FUTURES_DATA_URL = 'https://testnet.binancefuture.com/fapi'
            else:
                client.FUTURES_URL = 'https://fapi.binance.com/fapi'
                client.FUTURES_DATA_URL = 'https://fapi.binance.com/fapi'

            try:
                # Use futures_time for futures accounts to avoid 404s on restricted spot regions
                res = client.futures_time()
                client.timestamp_offset = res['serverTime'] - int(time.time() * 1000)
            except Exception as e:
                # Fallback to spot time if futures_time fails
                try:
                    res = client.get_server_time()
                    client.timestamp_offset = res['serverTime'] - int(time.time() * 1000)
                except Exception as e2:
                    # If both fail, we might be in a restricted region
                    if "restricted location" in str(e).lower() or "restricted location" in str(e2).lower():
                        logging.error("CRITICAL: Restricted location detected. Binance services are unavailable from this IP.")
                        return None
                    else:
                        logging.warning(f"Failed to sync time for account: {e2}")
            return client
        except Exception as e:
            if "restricted location" in str(e).lower():
                logging.error("CRITICAL: Restricted location detected. Cannot create Binance client.")
            else:
                logging.error(f"Error creating Binance client: {e}")
            return None

    def _get_client(self, api_key, api_secret):
        return self._create_client(api_key, api_secret)

    def _safe_api_call(self, func, *args, **kwargs):
        """Wrapper for Binance API calls with built-in retries and rate limit handling."""
        max_retries = 3
        backoff = 1 # seconds
        for i in range(max_retries):
            try:
                return func(*args, **kwargs)
            except BinanceAPIException as e:
                if e.code == -1003 or "Rate limit" in str(e): # Rate limit hit
                    logging.warning(f"Rate limit hit. Backing off for {backoff}s... (Attempt {i+1}/{max_retries})")
                    time.sleep(backoff)
                    backoff *= 2
                elif e.code == -1021: # Timestamp error
                    logging.warning("Timestamp error. Attempting to re-sync time...")
                    # The client handles its own offset, but we could re-create if persistent
                    time.sleep(0.5)
                else:
                    raise e
            except Exception as e:
                if "timeout" in str(e).lower():
                    logging.warning(f"API timeout. Retrying in {backoff}s... (Attempt {i+1}/{max_retries})")
                    time.sleep(backoff)
                    backoff *= 2
                else:
                    raise e
        return func(*args, **kwargs) # Last attempt before letting it bubble up

    def _initialize_market_ws(self):
        """Initializes WebSocket for market data (prices)."""
        with self.twm_lock:
            try:
                if self.market_twm:
                    try:
                        old_loop = self.market_twm._loop
                        self.market_twm.stop()
                        self.market_twm.join(timeout=2.0)
                        if old_loop and not old_loop.is_running():
                            try: old_loop.close()
                            except: pass
                    except: pass
                    self.market_twm = None

                # Check for restricted location before starting TWM
                m_client = self.market_client
                if m_client is None:
                    logging.error("CRITICAL: Market client is unavailable. Skipping market WebSocket initialization.")
                    return

                try:
                    m_client.get_server_time()
                except Exception as e:
                    if "restricted location" in str(e).lower():
                        logging.error("CRITICAL: Restricted location detected. Skipping market WebSocket initialization.")
                        return

                # Ensure this TWM gets a fresh loop to avoid sharing with other TWMs
                new_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(new_loop)

                testnet = self.config.get('is_demo', True)
                self.market_twm = ThreadedWebsocketManager(testnet=testnet)
                self.market_twm._loop = new_loop

                try:
                    self.market_twm.start()
                    time.sleep(0.1) # Small breath for the thread to start
                except Exception as e:
                    if "restricted location" in str(e).lower():
                        logging.error("CRITICAL: Market WebSocket failed - Restricted location.")
                    else:
                        logging.error(f"Failed to start market WebSocket thread: {e}")
                    self.market_twm = None
                    return

                symbols = self.config.get('symbols', [])
                if symbols:
                    for symbol in symbols:
                        try:
                            self.market_twm.start_futures_ticker_socket(
                                callback=self._handle_market_ticker,
                                symbol=symbol
                            )
                            # Add a small delay between subscriptions to avoid connection rate limits
                            time.sleep(0.2)
                        except Exception as e:
                            logging.debug(f"Failed to start stream for {symbol}: {e}")
                    logging.info(f"Started market WebSocket for {len(symbols)} symbols")
            except Exception as e:
                logging.error(f"Failed to initialize market WebSocket: {e}")

    def _handle_market_ticker(self, msg):
        """Callback for market ticker WebSocket."""
        try:
            # Handle both single ticker and array if needed
            data_list = msg if isinstance(msg, list) else [msg]

            for item in data_list:
                if item.get('e') == '24hrTicker':
                    symbol = item.get('s')
                    bid = float(item.get('b') or 0)
                    ask = float(item.get('a') or 0)
                    last = float(item.get('c') or 0)

                    with self.market_data_lock:
                        if symbol not in self.shared_market_data:
                            self.shared_market_data[symbol] = {'price': 0, 'last_update': 0}

                        data = self.shared_market_data[symbol]
                        data['price'] = last
                        data['bid'] = bid
                        data['ask'] = ask
                        data['last_update'] = time.time()
        except Exception as e:
            pass

    def _close_client(self, client):
        if not client: return
        try:
            # For sync Binance Client, close the requests session
            if hasattr(client, 'session') and client.session:
                client.session.close()
        except: pass

    def _initialize_bg_clients(self):
        """Initializes clients and WebSockets for all accounts to fetch balances/positions in background."""
        with self.twm_lock:
            api_accounts = self.config.get('api_accounts', [])
            testnet = self.config.get('is_demo', True)

            # Cleanup existing background clients if they are no longer in the config, keys changed, or mode changed
            old_bg_idxs = list(self.bg_clients.keys())
            for idx in old_bg_idxs:
                if idx >= len(api_accounts):
                    old_twm = self.bg_clients[idx].get('twm')
                    if old_twm:
                        try:
                            old_loop = old_twm._loop
                            threading.Thread(target=old_twm.stop, daemon=True).start()
                            old_twm.join(timeout=0.2)
                        except: pass
                    self._close_client(self.bg_clients[idx].get('client'))
                    del self.bg_clients[idx]
                    continue

                acc = api_accounts[idx]
                old_bg = self.bg_clients[idx]
                old_testnet = old_bg.get('is_demo', not testnet)

                if (acc.get('api_key', '').strip() != old_bg['info'].get('api_key', '').strip() or
                    acc.get('api_secret', '').strip() != old_bg['info'].get('api_secret', '').strip() or
                    old_testnet != testnet):
                    old_twm = old_bg.get('twm')
                    if old_twm:
                        try:
                            old_loop = old_twm._loop
                            # Wrap stop in a way that handles library-level crashes
                            threading.Thread(target=old_twm.stop, daemon=True).start()
                            old_twm.join(timeout=0.2)
                        except: pass
                    self._close_client(old_bg.get('client'))
                    del self.bg_clients[idx]

            new_bg_clients = self.bg_clients.copy()
            mode_str = "DEMO (Testnet)" if testnet else "LIVE (Mainnet)"

            for i, acc in enumerate(api_accounts):
                if i in new_bg_clients:
                    continue # Already have a valid one

                api_key = acc.get('api_key', '').strip()
                api_secret = acc.get('api_secret', '').strip()
                # We initialize background clients for ALL accounts that have keys,
                # so we can show their balance even if they are not enabled for trading.
                if api_key and api_secret:
                    try:
                        # Always re-create client to ensure correct environment (Demo vs Live)
                        client = self._get_client(api_key, api_secret)
                        if not client:
                             continue

                        # Start background WebSocket for this account
                        # Ensure this TWM gets a fresh loop to avoid sharing with other TWMs
                        new_loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(new_loop)
                        twm = ThreadedWebsocketManager(api_key=api_key, api_secret=api_secret, testnet=testnet)
                        twm._loop = new_loop
                        try:
                            twm.start()
                            time.sleep(0.1)
                            logging.info(f"Starting futures user socket for {acc.get('name')} (Testnet: {testnet})")
                            twm.start_futures_user_socket(callback=lambda msg, idx=i: self._handle_user_data(idx, msg))
                        except Exception as e:
                            logging.error(f"Failed to start background WebSocket for {acc.get('name')}: {e}")
                            twm = None

                        new_bg_clients[i] = {
                            'client': client,
                            'twm': twm,
                            'name': acc.get('name', f"Account {i+1}"),
                            'info': acc,
                            'is_demo': testnet
                        }
                    except Exception as e:
                        logging.error(f"Failed to init bg client/WS for {acc.get('name')}: {e}")

            self.bg_clients = new_bg_clients
            logging.info(f"Initialized {len(new_bg_clients)} background clients in {mode_str} mode.")

    @property
    def metadata_client(self):
        """Lazy loader for metadata client."""
        if self._metadata_client_instance is None:
            try:
                accs = self.config.get('api_accounts', [])
                for acc in accs:
                    if acc.get('api_key') and acc.get('api_secret'):
                        self._metadata_client_instance = self._create_client(acc['api_key'], acc['api_secret'])
                        break
                if self._metadata_client_instance is None:
                    self._metadata_client_instance = self._create_client("", "")
            except:
                pass
        return self._metadata_client_instance

    @property
    def market_client(self):
        """Lazy loader for market client."""
        if self._market_client_instance is None:
            try:
                self._market_client_instance = self._create_client("", "")
            except:
                pass
        return self._market_client_instance

    @staticmethod
    def test_account(api_key, api_secret, is_demo=True):
        try:
            client = Client(api_key.strip(), api_secret.strip(), testnet=is_demo, requests_params={'timeout': 20})
            if is_demo:
                client.FUTURES_URL = 'https://testnet.binancefuture.com/fapi'
                client.FUTURES_DATA_URL = 'https://testnet.binancefuture.com/fapi'
            else:
                client.FUTURES_URL = 'https://fapi.binance.com/fapi'
                client.FUTURES_DATA_URL = 'https://fapi.binance.com/fapi'
            
            # Check balance to verify API key
            client.futures_account_balance()
            return True, "Connection successful"
        except BinanceAPIException as e:
            return False, f"Binance Error: {e.message} (Code: {e.code})"
        except Exception as e:
            return False, f"Connection failed: {str(e)}"

    def _init_account(self, i, acc):
        try:
            # Check if we already have a background client/WS for this account
            if i in self.bg_clients:
                client = self.bg_clients[i]['client']
                twm = self.bg_clients[i]['twm']
            else:
                api_key = acc.get('api_key', '').strip()
                api_secret = acc.get('api_secret', '').strip()
                client = self._get_client(api_key, api_secret)
                if not client:
                    self.log("account_init_failed", level='error', is_key=True, name=acc.get('name', i), error="Client creation failed (Restricted location?)")
                    return

                with self.twm_lock:
                    # Ensure this TWM gets a fresh loop to avoid sharing with other TWMs
                    new_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(new_loop)
                    twm = ThreadedWebsocketManager(api_key=api_key, api_secret=api_secret, testnet=self.config.get('is_demo', True))
                    twm._loop = new_loop
                    try:
                        twm.start()
                        time.sleep(0.1)
                        twm.start_futures_user_socket(callback=lambda msg, idx=i: self._handle_user_data(idx, msg))
                    except Exception as e:
                        logging.error(f"Failed to start WebSocket for {acc.get('name')}: {e}")
                        twm = None

            self.accounts[i] = {
                'client': client,
                'twm': twm,
                'info': acc,
                'last_update': 0
            }

            # Initialize strategy for each symbol in its own thread
            for symbol in self.config.get('symbols', []):
                self._start_symbol_thread(i, symbol)

            self.log("account_init", account_name=acc.get('name'), is_key=True, name=acc.get('name', i))
        except Exception as e:
            self.log("account_init_failed", level='error', is_key=True, name=acc.get('name', i), error=str(e))

    def start(self):
        if self.is_running: return
        self.is_running = True
        self.stop_event.clear()

        self.log("bot_starting", is_key=True)

        # Initialize accounts
        api_accounts = self.config.get('api_accounts', [])
        for i, acc in enumerate(api_accounts):
            if acc.get('api_key') and acc.get('api_secret') and acc.get('enabled', True):
                self._init_account(i, acc)
                # Force immediate fetch of balances, positions and open orders
                self._update_account_metrics(i, force=True)

    def stop(self):
        self.is_running = False
        # Do NOT stop TWMs here anymore because they are now managed in _initialize_bg_clients
        # and we want them to keep running even if trading is stopped (for live balance/positions)
        self.accounts = {}
        self.log("bot_stopped", is_key=True)

    def _setup_strategy_for_account(self, idx, symbol):
        if idx not in self.accounts:
            return
        acc = self.accounts[idx]
        client = acc['client']
        if not client: return
        # Check and place initial entry logic for a specific trade_id
        strategy = self._get_strategy(idx, symbol)
        if not strategy: return

        try:
            # Get and cache exchange info centrally
            needs_info = False
            with self.market_data_lock:
                if symbol not in self.shared_market_data or 'info' not in self.shared_market_data[symbol]:
                    needs_info = True

            if needs_info:
                info = client.futures_exchange_info()
                for s in info['symbols']:
                    if s['symbol'] == symbol:
                        with self.market_data_lock:
                            self.shared_market_data[symbol] = self.shared_market_data.get(symbol, {'price': 0.0, 'last_update': 0})
                            self.shared_market_data[symbol]['info'] = s
                        break
            
            # Set leverage and margin type
            leverage = int(strategy.get('leverage') or 20)
            max_l = self.max_leverages.get(symbol, 125)
            if leverage > max_l:
                leverage = max_l
            margin_type = strategy.get('margin_type', 'CROSSED')

            try:
                self._safe_api_call(client.futures_change_margin_type, symbol=symbol, marginType=margin_type)
            except BinanceAPIException as e:
                if "No need to change margin type" not in e.message:
                    self.log("margin_type_error", level='warning', account_name=acc['info'].get('name'), is_key=True, error=e.message)

            self._safe_api_call(client.futures_change_leverage, symbol=symbol, leverage=leverage)

            self.log("leverage_set", account_name=acc['info'].get('name'), is_key=True, leverage=leverage, margin_type=margin_type)

            # Initial entry if needed
            self._check_and_place_initial_entry(idx, symbol, trade_id='manual')

        except Exception as e:
            self.log("strategy_setup_error", level='error', account_name=acc['info'].get('name'), is_key=True, error=str(e))

    def _check_and_place_initial_entry(self, idx, symbol, trade_id='manual'):
        if idx not in self.accounts: return
        acc = self.accounts[idx]
        client = acc['client']

        # Optimization: Remove forced update in loop. Rely on background worker and WebSocket.
        # self._update_account_metrics(idx, force=False)

        strategy = self._get_strategy(idx, symbol)
        if not strategy:
            self.log(f"Initial entry skipped for {symbol}: No strategy configuration found.", "warning", account_name=acc['info'].get('name'))
            return
            
        direction = strategy.get('direction', 'LONG').upper()
        side = Client.SIDE_BUY if direction == 'LONG' else Client.SIDE_SELL
        entry_type = strategy.get('entry_type', 'LIMIT').upper()
        trailing_tp_enabled = strategy.get('trailing_tp_enabled', strategy.get('trailing_enabled', False))

        trade_amount_val = float(strategy.get('trade_amount_usdc') or 0)
        leverage = int(strategy.get('leverage') or 20)
        is_pct = strategy.get('trade_amount_is_pct', False)
        
        with self.market_data_lock:
            current_price = float(self.shared_market_data.get(symbol, {}).get('price') or 0)
        
        if current_price <= 0: return
        
        entry_price = float(strategy.get('entry_price') or 0)

        # Determine calculation price for quantity
        # For LIMIT/COND_LIMIT, use entry_price to ensure notional amount (qty*price) is exact.
        # For MARKET/COND_MARKET, use current market price.

        # Strictly use set price for LIMIT and COND_LIMIT calculation
        if entry_type in ['LIMIT', 'COND_LIMIT'] and entry_price > 0:
            calc_price = entry_price
        else:
            calc_price = current_price

        # Calculate quantity for new entry
        balance = self.account_balances.get(idx, 0.0)
        if is_pct:
            quantity = (balance * (trade_amount_val / 100.0) * leverage) / calc_price
            calc_details = f"(Balance {balance:.2f} * {trade_amount_val}% * Lev {leverage}) / {calc_price}"
        else:
            quantity = (trade_amount_val * leverage) / calc_price
            calc_details = f"(Amount {trade_amount_val} * Lev {leverage}) / {calc_price}"

        # Debug log for quantity calculation
        log_key_calc = f"qty_calc_debug_{idx}_{symbol}"
        now = time.time()
        if now - self.last_log_times.get(log_key_calc, 0) > 300: # Every 5 mins
            logging.debug(f"[{acc['info'].get('name')}] Calculated quantity for {symbol}: {quantity} using {calc_details}")
            self.last_log_times[log_key_calc] = now

        # Optimization: Use cached positions/orders instead of REST calls to avoid rate limits and race conditions
        with self.data_lock:
            # Race condition fix: Check position inside data_lock
            pos_dict = self.open_positions.get(idx, {})
            p_info = pos_dict.get(symbol)
            has_pos = p_info is not None
            
            # Rate limit fix: Check open orders via cache instead of REST
            symbol_open_orders = [o for o in self.open_orders.get(idx, []) if o.get('symbol') == symbol]
            has_open_orders = len(symbol_open_orders) > 0

            if (idx, symbol) not in self.grid_state:
                self.grid_state[(idx, symbol)] = []
            
            # Check if we already have this trade_id active
            trades = self.grid_state[(idx, symbol)]
            existing_trade = next((t for t in trades if t.get('trade_id') == trade_id), None)
            
            if existing_trade:
                # Trade already active/pending.
                # Check if TP grid placement failed (e.g. ReduceOnly -2022) and needs retry.
                if existing_trade.get('initial_filled') and strategy.get('tp_enabled', True):
                    levels = existing_trade.get('levels', {})
                    # If levels dict is empty OR has unfilled levels without an order ID
                    # (excluding market mode or trailing eligible)
                    needs_tp_retry = False
                    if not levels:
                        needs_tp_retry = True
                    else:
                        for lvl in levels.values():
                            if not lvl.get('tp_order_id') and not lvl.get('filled') and not lvl.get('is_market') and not lvl.get('trailing_eligible'):
                                needs_tp_retry = True; break

                    if needs_tp_retry:
                        # Only retry every 10 seconds to avoid spamming
                        retry_key = f"tp_retry_cooldown_{idx}_{symbol}_{trade_id}"
                        if time.time() - self.last_log_times.get(retry_key, 0) > 10:
                            self.last_log_times[retry_key] = time.time()

                            tp_targets = strategy.get('tp_targets', [])
                            avg_e = existing_trade.get('avg_entry_price', current_price)
                            qty = existing_trade.get('quantity', 0)
                            trailing_enabled = strategy.get('trailing_tp_enabled', strategy.get('trailing_enabled', False))

                            # Fire and forget setup (it's debounced internally by tp_lock)
                            threading.Thread(target=self._setup_tp_targets_logic,
                                             args=(idx, symbol, avg_e, tp_targets, qty, direction, trailing_enabled, trade_id),
                                             daemon=True).start()
                return

        # NEW: use_existing only applies to the primary 'manual' task (auto-attach on start)
        # 'Add Trade' buttons always create new positions.
        use_existing = strategy.get('use_existing_assets', strategy.get('use_existing', False))
        is_primary_task = (trade_id == 'manual')


        if has_pos and (is_primary_task and use_existing):
            # Force grid placement if not already placed
            setup_tp_args = None
            with self.data_lock:
                # Check if already in state
                state = next((t for t in self.grid_state[(idx, symbol)] if t.get('trade_id') == trade_id), None)
                if not state:
                    # Create the manual trade from existing position
                    pos_qty = abs(float(p_info.get('amount') or 0))
                    state = {
                        'trade_id': trade_id,
                        'initial_filled': True,
                        'quantity': pos_qty,
                        'levels': {}
                    }
                    self.grid_state[(idx, symbol)].append(state)

                # Only place TP grid if tp_enabled
                if strategy.get('tp_enabled', True):
                    # Prefer user-defined cost basis (Were Bought For) if provided
                    entry_p = float(strategy.get('entry_price') or 0)
                    if entry_p <= 0:
                        entry_p = float(p_info.get('entryPrice') or 0)

                    if entry_p <= 0:
                        entry_p = current_price

                    state['avg_entry_price'] = entry_p
                    pos_qty = state['quantity']
                    actual_direction = 'LONG' if float(p_info.get('amount') or 0) > 0 else 'SHORT'

                    tp_targets = strategy.get('tp_targets', [])
                    if not tp_targets:
                        total_f = int(strategy.get('total_fractions') or 8)
                        dev = float(strategy.get('price_deviation') or 0.6)
                        tp_targets = [{'percent': (i + 1) * dev, 'volume': 100.0 / total_f} for i in range(total_f)]
                    setup_tp_args = (idx, symbol, entry_p, tp_targets, pos_qty, actual_direction, trailing_tp_enabled, trade_id)

            if setup_tp_args:
                self._setup_tp_targets_logic(*setup_tp_args)
            return

        if quantity <= 0: 
            self.log(f"Initial entry skipped for {symbol}: quantity {quantity} <= 0", "warning", account_name=acc['info'].get('name'))
            return

        # Entry Grid Logic (DCA)
        entry_grid_enabled = strategy.get('entry_grid_enabled', False)
        entry_targets = strategy.get('entry_targets', [])

        # Fallback: Auto-pick price if 0 on start for LIMIT/COND orders
        if entry_type != 'MARKET' and entry_price <= 0:
            with self.market_data_lock:
                m_data = self.shared_market_data.get(symbol, {})
                current_bid = m_data.get('bid', m_data.get('price', 0))
                current_ask = m_data.get('ask', m_data.get('price', 0))
            
            if current_bid > 0:
                entry_price = current_bid if direction == 'LONG' else current_ask
                self.log(f"Entry price for {symbol} ({entry_type}) was 0. Using fallback market price {entry_price}.", "info", account_name=acc['info'].get('name'))
                # Update config in memory so strategy usage below is consistent
                strategy['entry_price'] = entry_price
            else:
                log_key = f"waiting_entry_price_{idx}_{symbol}"
                if time.time() - self.last_log_times.get(log_key, 0) > 300:
                    self.log(f"Entry price for {symbol} is 0. Waiting for market price to initialize.", "info", account_name=acc['info'].get('name'))
                    self.last_log_times[log_key] = time.time()
                return

        if not has_pos:
            # Adoption logic for single orders remains if no other orders exist
            # but we no longer cancel on mismatch to allow multi-trade co-existence.
            if has_open_orders and not entry_grid_enabled and entry_type == 'LIMIT':
                for o in symbol_open_orders:
                    if o['type'] == 'LIMIT' and o['side'] == (Client.SIDE_BUY if direction == 'LONG' else Client.SIDE_SELL):
                        order_price = float(o.get('price') or 0)
                        if abs(order_price - entry_price) <= 0.00000001:
                            with self.data_lock:
                                # Ensure it's not already in grid_state
                                trades = self.grid_state.get((idx, symbol), [])
                                if not any(t.get('initial_order_id') == o['orderId'] for t in trades):
                                    self.log(f"Adopting existing {direction} order {o['orderId']} for {symbol}", "info", account_name=acc['info'].get('name'))
                                    self.grid_state[(idx, symbol)].append({'trade_id': 'auto', 'initial_order_id': o['orderId'], 'initial_filled': False, 'initial_orders': {o['orderId']: {'qty': float(o['origQty']), 'price': order_price, 'filled': False}}, 'levels': {}})
                            return
            
            if entry_grid_enabled and entry_targets:
                self.log(f"Placing ENTRY GRID for {symbol} ({len(entry_targets)} orders)", "info", account_name=acc['info'].get('name'))
                with self.data_lock:
                    state = {
                        'trade_id': trade_id,
                        'initial_filled': False,
                        'initial_order_id': 'GRID_PENDING',
                        'initial_orders': {},
                        'avg_entry_price': entry_price, # Base price for grid
                        'quantity': quantity, # Track total quantity intended for grid
                        'levels': {},
                        'consolidated_tp_id': None
                    }
                    self.grid_state[(idx, symbol)].append(state)

                placed_any = False
                for target in entry_targets:
                    dev = float(target.get('deviation') or 0) / 100.0
                    vol_pct = float(target.get('volume') or 0) / 100.0
                    order_price = entry_price * (1 + dev)
                    
                    # Target quantity adjusted for price to maintain relative volume %
                    target_qty = (quantity * vol_pct) * (calc_price / order_price)
                    
                    if self._check_balance_for_order(idx, target_qty, order_price):
                        c_id = f"trd-{trade_id}-entry-{target.get('deviation', 0)}"
                        # Adoption check
                        existing = next((oo for oo in symbol_open_orders if oo.get('clientOrderId') == c_id), None)
                        oid = None
                        if existing:
                            oid = existing['orderId']
                            self.log(f"Adopting existing Entry order {oid} for {symbol}", "info", account_name=acc['info'].get('name'))
                        else:
                            oid = self._place_limit_order(idx, symbol, side, target_qty, order_price, client_id=c_id)
                        
                        if oid:
                            placed_any = True
                            with self.data_lock:
                                state['initial_orders'][oid] = {'qty': target_qty, 'price': order_price, 'filled': False}
                
                if not placed_any:
                    with self.data_lock:
                        self.grid_state[(idx, symbol)] = [t for t in self.grid_state[(idx, symbol)] if t.get('trade_id') != trade_id]
                else:
                    # For DCA entry grid, update quantity based on intended entry
                    with self.data_lock:
                        state['quantity'] = quantity

                    # For DCA entry grid, we also place the TP grid immediately (Production Ready)
                    # based on the main entry price. It will be refined as fills happen.
                    if strategy.get('tp_enabled', True):
                        tp_targets = strategy.get('tp_targets', [])
                        if not tp_targets:
                            dev = float(strategy.get('price_deviation') or 0.6)
                            total_f = int(strategy.get('total_fractions') or 8)
                            tp_targets = [{'percent': (i + 1) * dev, 'volume': 100.0 / total_f} for i in range(total_f)]

                        trailing_enabled = strategy.get('trailing_tp_enabled', strategy.get('trailing_enabled', False))
                        self._setup_tp_targets_logic(idx, symbol, entry_price, tp_targets, quantity, direction, trailing_enabled, trade_id)
                return

            # 2. Traditional Single Entry Types
            if entry_type == 'MARKET':
                self.log("placing_market_initial", account_name=acc['info'].get('name'), is_key=True, direction=direction)
                self._execute_market_entry(idx, symbol, trade_id=trade_id)
                return

            if entry_type in ['CONDITIONAL', 'COND_LIMIT', 'COND_MARKET']:
                self.log("conditional_active", account_name=acc['info'].get('name'), is_key=True, symbol=symbol, trigger_price=entry_price)
                with self.data_lock:
                    state = {'trade_id': trade_id, 'conditional_active': True, 'conditional_type': entry_type, 'trigger_price': entry_price, 'initial_filled': False, 'levels': {}}
                    self.grid_state[(idx, symbol)].append(state)
                return

            if strategy.get('trailing_buy_enabled', False):
                self.log("trailing_buy_starting", account_name=acc['info'].get('name'), is_key=True, direction=direction, target_price=entry_price)
                with self.data_lock:
                    state = {'trade_id': trade_id, 'trailing_buy_active': True, 'trailing_buy_target': entry_price, 'trailing_buy_peak': 0, 'initial_filled': False, 'levels': {}}
                    self.grid_state[(idx, symbol)].append(state)
                return

            # LIMIT Entry
            if not self._check_balance_for_order(idx, quantity, calc_price):
                return

            # Check if we just had a price error and should back off (per trade_id to not block manual trades)
            price_err_key = f"price_error_cooldown_{idx}_{symbol}_{trade_id}"
            if time.time() - self.last_log_times.get(price_err_key, 0) < 60:
                return  # Back off for 60 seconds after a price error

            self.log("placing_initial", account_name=acc['info'].get('name'), is_key=True, direction=direction, price=entry_price)
            try:
                # Ensure state exists BEFORE placing order so TP placement can find it
                with self.data_lock:
                    if (idx, symbol) not in self.grid_state:
                        self.grid_state[(idx, symbol)] = []

                    state = {
                        'trade_id': trade_id,
                        'initial_filled': False,
                        'initial_order_id': 'PENDING',
                        'initial_orders': {},
                        'avg_entry_price': entry_price,
                        'quantity': 0.0,
                        'levels': {}
                    }
                    self.grid_state[(idx, symbol)].append(state)

                c_id = f"trd-{trade_id}-entry"
                # Adoption check
                existing = next((oo for oo in symbol_open_orders if oo.get('clientOrderId') == c_id), None)
                order_id = None
                if existing:
                    order_id = existing['orderId']
                    self.log(f"Adopting existing Entry order {order_id} for {symbol}", "info", account_name=acc['info'].get('name'))
                else:
                    order_id = self._place_limit_order(idx, symbol, side, quantity, entry_price, client_id=c_id)

                if order_id:
                    with self.data_lock:
                        state = next((t for t in self.grid_state[(idx, symbol)] if t.get('trade_id') == trade_id), None)
                        if state:
                            state['initial_order_id'] = order_id
                            state['initial_orders'][order_id] = {'qty': quantity, 'price': entry_price, 'filled': False}
                            state['quantity'] = quantity

                    # IMMEDIATELY PLACE TP GRID (Production Ready: Don't wait for fill)
                    if strategy.get('tp_enabled', True):
                        tp_targets = strategy.get('tp_targets', [])
                        if not tp_targets:
                            dev = float(strategy.get('price_deviation') or 0.6)
                            total_f = int(strategy.get('total_fractions') or 8)
                            tp_targets = [{'percent': (i + 1) * dev, 'volume': 100.0 / total_f} for i in range(total_f)]

                        trailing_enabled = strategy.get('trailing_tp_enabled', strategy.get('trailing_enabled', False))
                        self._setup_tp_targets_logic(idx, symbol, entry_price, tp_targets, quantity, direction, trailing_enabled, trade_id)
                else:
                    # Remove pending state and set a cooldown to avoid hammering the API
                    with self.data_lock:
                        self.grid_state[(idx, symbol)] = [t for t in self.grid_state[(idx, symbol)] if t.get('trade_id') != trade_id]
                    self.last_log_times[price_err_key] = time.time()
            except Exception as e:
                with self.data_lock:
                    self.grid_state[(idx, symbol)] = [t for t in self.grid_state[(idx, symbol)] if t.get('trade_id') != trade_id]
                self.log("initial_failed", level='error', account_name=acc['info'].get('name'), is_key=True, error=str(e))

    def _format_quantity(self, symbol, quantity):
        with self.market_data_lock:
            info = self.shared_market_data.get(symbol, {}).get('info')
        if not info: return f"{quantity:.8f}".rstrip('0').rstrip('.')

        step_size = "0.00000001"
        for f in info['filters']:
            if f['filterType'] == 'LOT_SIZE':
                step_size = f['stepSize']
                break

        step_d = Decimal(step_size).normalize()
        qty_d = Decimal(str(quantity))

        # Quantize quantity to step size
        # We floor to avoid 'insufficient balance' or 'quantity too high'
        result = (qty_d / step_d).quantize(Decimal('1'), rounding=ROUND_FLOOR) * step_d
        
        # Determine decimal places from step_size
        precision = max(0, -step_d.as_tuple().exponent)
        
        return format(result, f'.{precision}f')


    def _format_price(self, symbol, price):
        with self.market_data_lock:
            info = self.shared_market_data.get(symbol, {}).get('info')
        if not info: return f"{price:.8f}".rstrip('0').rstrip('.')

        tick_size = "0.00000001"
        for f in info['filters']:
            if f['filterType'] == 'PRICE_FILTER':
                tick_size = f['tickSize']
                break
        
        tick_d = Decimal(tick_size).normalize()
        price_d = Decimal(str(price))
        
        # Quantize to tick size
        result = (price_d / tick_d).quantize(Decimal('1'), rounding=ROUND_HALF_UP) * tick_d
        
        precision = max(0, -tick_d.as_tuple().exponent)
        return format(result, f'.{precision}f')

    def _handle_user_data(self, idx, msg):
        # We process user data for ALL accounts with keys, even if trading is not "started"
        # but we need to check if we have account info (from accounts or bg_clients)
        acc_name = "Unknown"
        if idx in self.accounts:
            acc_name = self.accounts[idx]['info'].get('name', f"Account {idx+1}")
        elif idx in self.bg_clients:
            acc_name = self.bg_clients[idx]['name']
        else:
            return

        event_type = msg.get('e')

        if event_type == 'ORDER_TRADE_UPDATE':
            order_data = msg.get('o', {})
            symbol = order_data.get('s')
            status = order_data.get('X')
            side = order_data.get('S')
            order_id = order_data.get('i')
            avg_price = float(order_data.get('ap') or 0)
            filled_qty = float(order_data.get('z') or 0)

            if status == 'FILLED':
                client_id = order_data.get('c', '')
                # Pattern: trd-{trade_id}-{suffix}
                trade_id = None
                if client_id.startswith('trd-'):
                    # Suffixes: -entry, -reentry-..., -tp-..., -tp-trailing
                    if '-tp-trailing' in client_id:
                        trade_id = client_id[4:client_id.find('-tp-trailing')]
                    elif '-reentry' in client_id:
                        trade_id = client_id[4:client_id.find('-reentry')]
                    elif '-tp-' in client_id:
                        trade_id = client_id[4:client_id.find('-tp-')]
                    elif '-entry' in client_id:
                        trade_id = client_id[4:client_id.find('-entry')]
                    
                    if not trade_id:
                        # Fallback to old splitting if above didn't match perfectly
                        parts = client_id.split('-')
                        if len(parts) >= 2: # Changed from 3 to 2 because 'trd-auto' would be parts[0]='trd', parts[1]='auto'
                            trade_id = parts[1]
                
                self.log("order_filled", account_name=acc_name, is_key=True, id=order_id, side=side, qty=filled_qty, symbol=symbol, price=avg_price, trade_id=trade_id)
                self._process_filled_order(idx, symbol, order_data, trade_id=trade_id)

        elif event_type == 'ACCOUNT_UPDATE':
            # Fast update for specific assets from WebSocket event
            update_data = msg.get('a', {})

            with self.data_lock:
                # Update Balances
                balances = update_data.get('B', [])
                for b in balances:
                    if b.get('a') == 'USDC':
                        self.account_balances[idx] = float(b.get('wb') or self.account_balances.get(idx, 0))

                # Update Positions incrementally
                positions_data = update_data.get('P', [])
                if idx not in self.open_positions:
                    self.open_positions[idx] = {}

                for p in positions_data:
                    symbol = p.get('s')
                    if not symbol: continue

                    amt = float(p.get('pa') or 0)
                    if amt == 0:
                        # Position closed
                        if symbol in self.open_positions[idx]:
                            del self.open_positions[idx][symbol]
                    else:
                        # Update or add position
                        existing_pos = self.open_positions[idx].get(symbol, {})
                        self.open_positions[idx][symbol] = {
                            'symbol': symbol,
                            'amount': p.get('pa', existing_pos.get('amount', '0')),
                            'entryPrice': p.get('ep', existing_pos.get('entryPrice', '0')),
                            'unrealizedProfit': p.get('up', existing_pos.get('unrealizedProfit', '0')),
                            'leverage': p.get('l', existing_pos.get('leverage', '20'))
                        }
            self.account_last_update[idx] = time.time()
            self._emit_account_update()

    def _get_trade_quantity(self, trade):
        """Calculates the net quantity remaining in a specific trade."""
        qty = 0.0
        # Sum all filled initial orders (works for both single and grid entries)
        if trade.get('initial_orders'):
            for o in trade['initial_orders'].values():
                if o.get('filled'):
                    qty += float(o.get('qty') or 0)
        elif trade.get('initial_filled'):
            qty += float(trade.get('quantity') or 0)
        
        # 3. TP Fills (Reduce entry)
        if trade.get('levels'):
            for lvl in trade['levels'].values():
                if lvl.get('filled', False):
                    qty -= float(lvl.get('qty') or 0)
        
        # 4. Re-entries (Add back ONLY if filled)
        if trade.get('consolidated_reentry_id'):
            # Note: We should ideally track the fill of the re-entry order.
            # If we assume it fills when the position increases back up, we need a fill event.
            # For now, we only count it if it's marked filled elsewhere.
            pass
        return max(0.0, qty)

    def _get_trade(self, idx, symbol, trade_id):
        """Helper to find a specific trade in the list-based grid_state."""
        with self.data_lock:
            trades = self.grid_state.get((idx, symbol), [])
            for t in trades:
                if t.get('trade_id') == trade_id:
                    return t
        return None

    def _process_filled_order(self, idx, symbol, order_data, trade_id=None):
        order_id = order_data.get('i')
        strategy = self._get_strategy(idx, symbol)
        if not strategy: return

        direction = strategy.get('direction', 'LONG')
        side = order_data.get('S')
        total_fractions = int(strategy.get('total_fractions') or 8)
        
        # Calculate total_qty based on actual fill
        avg_price = float(order_data.get('ap') or 0)
        filled_qty = float(order_data.get('z') or 0)
        if avg_price <= 0 or filled_qty <= 0: return
        
        total_qty = filled_qty
        fraction_qty = total_qty / total_fractions
        entry_price_base = float(strategy.get('entry_price') or 0)

        setup_tp_args = None
        reentry_needed_qty = None
        reentry_fill_detected = False

        with self.data_lock:
            trades = self.grid_state.get((idx, symbol), [])
            if not trades: return
            
            # If no trade_id, try to find by order_id
            state = None
            if trade_id:
                state = next((t for t in trades if t.get('trade_id') == trade_id), None)
            else:
                for t in trades:
                    # Check initial orders or TP levels
                    if order_id in t.get('initial_orders', {}) or order_id == t.get('initial_order_id'):
                        state = t; break
                    if t.get('levels'):
                        if any(lvl.get('tp_order_id') == order_id for lvl in t['levels'].values()):
                            state = t; break
                    if order_id == t.get('consolidated_reentry_id'):
                        state = t; break
                    if order_id == t.get('consolidated_tp_id'):
                        state = t; break

            if not state: return
            
            current_trade_id = state['trade_id']

            # 1. Initial Entry Filled
            is_initial_fill = False
            if not state.get('initial_filled') or state.get('initial_orders'):
                if order_id in state.get('initial_orders', {}):
                    is_initial_fill = True
                elif state.get('initial_order_id') == 'PENDING' or order_id == state.get('initial_order_id'):
                    # Check if side matches our entry side
                    expected_side = Client.SIDE_BUY if direction == 'LONG' else Client.SIDE_SELL
                    if side == expected_side:
                        is_initial_fill = True

            if is_initial_fill:
                state['initial_filled'] = True
                if 'initial_orders' not in state: state['initial_orders'] = {}
                # Update with actual filled quantity and price
                state['initial_orders'][order_id] = {'qty': filled_qty, 'price': avg_price, 'filled': True}
                
                # If it's a MARKET order, the original quantity might have been an estimate
                # and the orderId is now matched.

                # Update total quantity
                filled_entries = [o for o in state['initial_orders'].values() if o.get('filled')]
                total_filled_qty = sum(o['qty'] for o in filled_entries)
                state['quantity'] = total_filled_qty

                # Calculate weighted average entry price
                total_cost = sum(o['qty'] * o['price'] for o in filled_entries)
                weighted_avg = total_cost / total_filled_qty if total_filled_qty > 0 else avg_price
                state['avg_entry_price'] = weighted_avg

                acc_name = self.accounts[idx]['info'].get('name')
                self.log(f"Entry filled for {symbol} (Trade {current_trade_id}) at {avg_price}. Total Qty: {total_filled_qty}", account_name=acc_name, is_key=True)
                
                # Re-sync TP grid with exact filled price/quantity
                if strategy.get('tp_enabled', True):
                    tp_targets = strategy.get('tp_targets', [])
                    if not tp_targets:
                        dev = float(strategy.get('price_deviation') or 0.6)
                        total_f = int(strategy.get('total_fractions') or 8)
                        tp_targets = [{'percent': (i + 1) * dev, 'volume': 100.0 / total_f} for i in range(total_f)]
                    
                    trailing_enabled = strategy.get('trailing_tp_enabled', strategy.get('trailing_enabled', False))
                    setup_tp_args = (idx, symbol, weighted_avg, tp_targets, total_filled_qty, direction, trailing_enabled, current_trade_id)

            # 2. Check levels for TP fills
            elif state.get('levels'):
                for level, lvl_data in list(state['levels'].items()):
                    if lvl_data.get('tp_order_id') and order_id == lvl_data.get('tp_order_id'):
                        qty_filled_lvl = lvl_data.get('qty', fraction_qty)
                        self.log("tp_filled_reentry", account_name=self.accounts[idx]['info'].get('name'), is_key=True, target_level=level, qty=qty_filled_lvl, trade_id=current_trade_id)
                        lvl_data['filled'] = True
                        lvl_data['tp_order_id'] = None 
                        reentry_needed_qty = qty_filled_lvl
                        break

            # 3. Handle Re-entry Fill (Consolidated)
            if strategy.get('consolidated_reentry', True) and order_id == state.get('consolidated_reentry_id'):
                reentry_fill_detected = True

        # Perform API calls outside data_lock
        if setup_tp_args:
            self._setup_tp_targets_logic(*setup_tp_args)

        if reentry_needed_qty:
            self._handle_reentry_logic(idx, symbol, reentry_needed_qty, trade_id=current_trade_id)

        if reentry_fill_detected:
            self.log("reentry_filled_tp_all", account_name=self.accounts[idx]['info'].get('name'), is_key=True, trade_id=current_trade_id)
            with self.data_lock:
                state['consolidated_reentry_id'] = None
                state['pending_reentry_qty'] = 0.0 # Reset pool
                if avg_price > 0:
                    state['avg_entry_price'] = avg_price
                anchor = state.get('avg_entry_price', entry_price_base)
                levels_to_re_place = []
                if state.get('levels'):
                    for l, o in list(state['levels'].items()):
                        # Only re-place if not already filled or active
                        if o.get('tp_order_id') is None and not o.get('is_market') and not o.get('trailing_eligible') and not o.get('filled'):
                            pct = o.get('percent', 0)
                            tp_price = anchor * (1 + pct) if direction == 'LONG' else anchor * (1 - pct)
                            o['price'] = tp_price
                            level_qty = o.get('qty', fraction_qty)
                            levels_to_re_place.append({'level': l, 'qty': level_qty, 'price': tp_price})

            # Place orders outside lock
            for item in levels_to_re_place:
                client_id = f"trd-{current_trade_id}-tp-{item['level']}"
                tp_id = self._place_limit_order(idx, symbol, Client.SIDE_SELL if direction == 'LONG' else Client.SIDE_BUY, item['qty'], item['price'], client_id=client_id, reduce_only=True)
                with self.data_lock:
                    if (idx, symbol) in self.grid_state:
                         # Ensure trade still exists
                         s = next((t for t in self.grid_state[(idx, symbol)] if t['trade_id'] == current_trade_id), None)
                         if s and item['level'] in s['levels']:
                             s['levels'][item['level']]['tp_order_id'] = tp_id
                             s['levels'][item['level']]['filled'] = False

    def _handle_reentry_logic(self, idx, symbol, qty_filled, trade_id=None):
        """Handles the logic for placing/updating re-entry orders after a TP fill. Performs API calls."""
        strategy = self._get_strategy(idx, symbol)
        direction = strategy.get('direction', 'LONG')
        entry_price_base = float(strategy.get('entry_price') or 0)

        client = None
        old_re_id = None
        anchor = 0
        pending = 0

        with self.data_lock:
            trades = self.grid_state.get((idx, symbol), [])
            state = next((t for t in trades if t.get('trade_id') == trade_id), None)
            if not state: return

            if strategy.get('consolidated_reentry', True):
                pending = state.get('pending_reentry_qty', 0.0)
                pending += qty_filled
                state['pending_reentry_qty'] = pending

                client = self.accounts[idx]['client']
                old_re_id = state.get('consolidated_reentry_id')
                anchor = state.get('avg_entry_price', entry_price_base)

        if client and pending > 0:
            # Cancel existing re-entry order outside lock
            if old_re_id:
                try: self._safe_api_call(client.futures_cancel_order, symbol=symbol, orderId=old_re_id)
                except: pass

            # Place new consolidated re-entry outside lock
            re_side = Client.SIDE_BUY if direction == 'LONG' else Client.SIDE_SELL
            if self._check_balance_for_order(idx, pending, anchor):
                c_id = f"trd-{trade_id}-reentry"
                
                # Adoption check
                with self.data_lock:
                    symbol_open_orders = [o for o in self.open_orders.get(idx, []) if o.get('symbol') == symbol]
                existing = next((oo for oo in symbol_open_orders if oo.get('clientOrderId') == c_id), None)
                
                new_re_id = None
                if existing:
                    new_re_id = existing['orderId']
                    self.log(f"Adopting existing Re-entry order {new_re_id} for {symbol}", "info", account_name=self.accounts[idx]['info'].get('name'))
                else:
                    new_re_id = self._place_limit_order(idx, symbol, re_side, pending, anchor, client_id=c_id)
                
                if new_re_id:
                    with self.data_lock:
                        if (idx, symbol) in self.grid_state:
                            s = next((t for t in self.grid_state[(idx, symbol)] if t.get('trade_id') == trade_id), None)
                            if s: s['consolidated_reentry_id'] = new_re_id
                    self.log("reentry_updated", account_name=self.accounts[idx]['info'].get('name'), is_key=True, qty=pending, price=anchor, trade_id=trade_id)

    def _setup_tp_targets_logic(self, idx, symbol, entry_price, targets, total_qty, direction, trailing_tp_enabled=False, trade_id=None):
        """Sets up TP targets. Performs API calls. Should be called WITHOUT holding data_lock."""
        if total_qty <= 0:
            logging.warning(f"[_setup_tp_targets_logic] Skipping TP placement for {symbol}: total_qty is {total_qty} (Trade {trade_id})")
            return

        # Debounce: Ensure only one TP setup runs at a time for this specific trade
        lock_key = (idx, symbol, trade_id)
        with self.data_lock:
            # Periodic cleanup of recent_client_ids (if older than 1 hour)
            now = time.time()
            if (idx, symbol) in self.recent_client_ids:
                self.recent_client_ids[(idx, symbol)] = {cid: ts for cid, ts in self.recent_client_ids[(idx, symbol)].items() if now - ts < 3600}

            if lock_key not in self.tp_setup_locks:
                self.tp_setup_locks[lock_key] = threading.Lock()
            tp_lock = self.tp_setup_locks[lock_key]

        if not tp_lock.acquire(blocking=False):
            logging.debug(f"[_setup_tp_targets_logic] Skipping redundant TP setup for {symbol} (Trade {trade_id}) - already in progress.")
            return

        try:
            acc_name = self.accounts[idx]['info'].get('name') if idx in self.accounts else (self.bg_clients[idx]['name'] if idx in self.bg_clients else str(idx))
            strategy = self._get_strategy(idx, symbol)
            consolidated_tp = strategy.get('consolidated_tp', False)
            tp_market_mode = strategy.get('tp_market_mode', False)
            
            with self.data_lock:
                trades = self.grid_state.get((idx, symbol), [])
                state = next((t for t in trades if t.get('trade_id') == trade_id), None)
                if not state: return
                
                client = self.accounts[idx]['client']
                
                # 1. Cancel existing orders to prevent orphaned orders (Individual levels)
                if state.get('levels'):
                    for lvl in state['levels'].values():
                        o_id = lvl.get('tp_order_id')
                        if o_id:
                            try: self._safe_api_call(client.futures_cancel_order, symbol=symbol, orderId=o_id)
                            except: pass

                # 2. Cancel existing consolidated TP
                old_ctp_id = state.get('consolidated_tp_id')
                if old_ctp_id:
                    try: self._safe_api_call(client.futures_cancel_order, symbol=symbol, orderId=old_ctp_id)
                    except: pass
                    state['consolidated_tp_id'] = None

                state['levels'] = {} # Reset levels for new targets

                # Gather all order parameters and initialize state levels
                orders_to_place = []

                if consolidated_tp:
                    target_vol_total = sum(float(t.get('volume') or 0) for t in targets) / 100.0
                    qty = total_qty * target_vol_total

                    first_target = targets[0] if targets else {'percent': 0.6}
                    pct = float(first_target.get('percent') or 0) / 100.0
                    tp_price = entry_price * (1 + pct) if direction == 'LONG' else entry_price * (1 - pct)
                    side = Client.SIDE_SELL if direction == 'LONG' else Client.SIDE_BUY

                    if not tp_market_mode:
                        orders_to_place.append({'level': 1, 'side': side, 'qty': qty, 'price': tp_price, 'is_consolidated': True})

                    state['levels'][1] = {
                        'tp_order_id': None,
                        'price': tp_price,
                        'percent': pct,
                        'qty': qty,
                        'side': side,
                        'is_market': tp_market_mode,
                        'trailing_eligible': False,
                        'filled': False
                    }
                else:
                    for i, target in enumerate(targets, 1):
                        pct = float(target.get('percent') or 0) / 100.0
                        volume_pct = float(target.get('volume') or 0) / 100.0
                        qty = total_qty * volume_pct

                        if direction == 'LONG':
                            tp_price = entry_price * (1 + pct)
                            side = Client.SIDE_SELL
                        else:
                            tp_price = entry_price * (1 - pct)
                            side = Client.SIDE_BUY

                        is_last = (i == len(targets))
                        trailing_eligible = is_last and trailing_tp_enabled

                        if not tp_market_mode and not trailing_eligible:
                            orders_to_place.append({'level': i, 'side': side, 'qty': qty, 'price': tp_price})

                        state['levels'][i] = {
                            'tp_order_id': None,
                            're_entry_order_id': None,
                            'price': tp_price,
                            'percent': pct,
                            'qty': qty,
                            'side': side,
                            'is_market': tp_market_mode,
                            'trailing_eligible': trailing_eligible,
                            'filled': False
                        }

            if consolidated_tp:
                # We assume at least one order if targets exist
                if orders_to_place:
                    o = orders_to_place[0]
                    self.log("tp_aggregated_summary", account_name=acc_name, is_key=True, symbol=symbol, qty=f"{o['qty']:.2f}", price=f"{o['price']:.2f}")
            else:
                self.log("tp_placement_summary", account_name=acc_name, is_key=True, symbol=symbol, count=len(orders_to_place), total_qty=f"{total_qty:.2f}")
            
            # Fetch open orders from cache to avoid duplicate ClientID (-4116)
            with self.data_lock:
                open_orders_list = self.open_orders.get(idx, [])
                recent_ids = self.recent_client_ids.get((idx, symbol), {})

            symbol_open_orders = [o for o in open_orders_list if o.get('symbol') == symbol]

            # Place orders outside lock
            for o in orders_to_place:
                client_id = f"trd-{trade_id}-tp-{o['level']}"

                # Check for existing order with same client_id in REST cache OR our local recent cache
                existing_order = next((oo for oo in symbol_open_orders if oo.get('clientOrderId') == client_id), None)
                is_recent = client_id in recent_ids and (time.time() - recent_ids[client_id] < 10)

                order_id = None
                if existing_order:
                    order_id = existing_order['orderId']
                    self.log(f"Adopting existing TP order {order_id} (Client ID {client_id}) for {symbol}", "info", account_name=self.accounts[idx]['info'].get('name'))
                elif is_recent:
                    # Skip placement but don't mark as error - it's already in flight
                    logging.debug(f"Skipping placement of {client_id} for {symbol} - already in flight.")
                    continue
                else:
                    order_id = self._place_limit_order(idx, symbol, o['side'], o['qty'], o['price'], client_id=client_id, reduce_only=True)

                # If placement failed but it's a known 'retry' scenario, mark level as unplaced
                if not order_id:
                    # Check if it was rejected due to ReduceOnly
                    if self.trailing_state.get((idx, symbol, client_id)) == 'REJECTED_REDUCE_ONLY':
                        # Do NOT mark as filled. Leaving it unplaced will allow _symbol_logic_worker
                        # to retry this setup in the next loop iteration once the fill is confirmed.
                        logging.debug(f"Level {o['level']} for {symbol} (Trade {trade_id}) marked for retry.")
                        continue

                    # NEW: If it's NOT a Market level, we SHOULD NOT mark as filled if placement failed.
                    # This prevents falling back to market execution prematurely for limit orders.
                    if not o.get('is_market'):
                        logging.debug(f"Level {o['level']} placement failed for {symbol}. Will NOT fallback to market.")
                        continue

                if order_id:
                    with self.data_lock:
                        if (idx, symbol) in self.grid_state:
                            lvl = o['level']
                            s = next((t for t in self.grid_state[(idx, symbol)] if t.get('trade_id') == trade_id), None)
                            if s and lvl in s.get('levels', {}):
                                s['levels'][lvl]['tp_order_id'] = order_id
                            if o.get('is_consolidated') and s:
                                s['consolidated_tp_id'] = order_id
                else:
                    with self.data_lock:
                        if (idx, symbol) in self.grid_state:
                            s = next((t for t in self.grid_state[(idx, symbol)] if t.get('trade_id') == trade_id), None)
                            if s and o['level'] in s.get('levels', {}):
                                s['levels'][o['level']]['filled'] = True
        finally:
            tp_lock.release()

    def _check_balance_for_order(self, idx, qty, price, leverage=None, symbol=None):
        # Specifically check USDC balance for USDC-M pairs
        balance = self.account_balances.get(idx, 0)
        notional = qty * price

        # If leverage is not provided, try to find it in config
        if leverage is None and symbol:
            strategy = self._get_strategy(idx, symbol)
            if strategy:
                leverage = int(strategy.get('leverage') or 20)
        
        if leverage is None:
            leverage = 20 # Default fallback

        # Margin required = Notional / Leverage
        # Add a 5% buffer for fees and price movements
        margin_required = (notional / leverage) * 1.05
        return balance >= margin_required

    def _place_limit_order(self, idx, symbol, side, qty, price, client_id=None, reduce_only=False):
        """Places a limit order. ENSURE NO LOCKS ARE HELD WHEN CALLING THIS."""
        # Use target_client to support both active and background (for safety/cancels if needed,
        # though trading usually requires active accounts)
        target_client = self.accounts[idx]['client'] if idx in self.accounts else self.bg_clients.get(idx, {}).get('client')
        if not target_client: return None

        acc_name = self.accounts[idx]['info'].get('name') if idx in self.accounts else self.bg_clients.get(idx, {}).get('name', str(idx))

        strategy = self._get_strategy(idx, symbol)
        leverage = int(strategy.get('leverage') or 20)

        # Validate balance before placing re-buy/re-sell orders
        if not self._check_balance_for_order(idx, qty, price, leverage=leverage):
            log_key = f"insufficient_balance_{idx}_{symbol}"
            now = time.time()
            if now - self.last_log_times.get(log_key, 0) > 60: # Log at most once per minute per symbol
                self.log("insufficient_balance", level='warning', account_name=acc_name, is_key=True, qty=qty, price=price)
                self.last_log_times[log_key] = now
            return None

        try:
            formatted_qty_str = self._format_quantity(symbol, qty)
            formatted_price_str = self._format_price(symbol, price)

            f_qty = float(formatted_qty_str)
            f_price = float(formatted_price_str)

            if f_qty <= 0:
                self.log(f"Limit order skipped for {symbol}: Calculated quantity {qty} (formatted: {formatted_qty_str}) <= 0.", level='warning', account_name=acc_name)
                return None

            # Min Notional Check
            notional = f_qty * f_price
            min_notional = 5.0
            with self.market_data_lock:
                info = self.shared_market_data.get(symbol, {}).get('info')
                if info:
                    for f in info.get('filters', []):
                        if f.get('filterType') == 'MIN_NOTIONAL':
                            min_notional = float(f.get('notional') or f.get('minNotional') or 5.0)
                            break

            if notional < min_notional:
                self.log("order_skipped_min_notional", level='warning', account_name=acc_name, is_key=True, symbol=symbol, notional=f"{notional:.2f}", min_notional=f"{min_notional:.2f}")
                return None

            self.log("placing_limit_order", account_name=acc_name, is_key=True, symbol=symbol, side=side, qty=formatted_qty_str, price=formatted_price_str)

            params = {
                'symbol': symbol,
                'side': side,
                'type': Client.FUTURE_ORDER_TYPE_LIMIT,
                'timeInForce': Client.TIME_IN_FORCE_GTC,
                'quantity': formatted_qty_str,
                'price': formatted_price_str,
                'reduceOnly': 'true' if reduce_only else 'false'
            }
            if client_id:
                params['newClientOrderId'] = client_id

            order = self._safe_api_call(target_client.futures_create_order, **params)

            # Reset reject-wait if successful
            wait_key = (idx, symbol, client_id)
            if wait_key in self.trailing_state: del self.trailing_state[wait_key]

            # Cache client_id immediately to prevent -4116 in rapid succession
            if client_id:
                with self.data_lock:
                    if (idx, symbol) not in self.recent_client_ids:
                        self.recent_client_ids[(idx, symbol)] = {}
                    self.recent_client_ids[(idx, symbol)][client_id] = time.time()

            return order['orderId']
        except BinanceAPIException as e:
            # Catch specific price out of range errors (including -4016 and -4025)
            if e.code in [-4016, -4025] or "Price out of range" in e.message or "Price higher than" in e.message or "Price lower than" in e.message:
                self.log("limit_order_price_error", level='warning', account_name=acc_name, is_key=True, symbol=symbol, error=e.message)
            elif e.code == -2022:
                # ReduceOnly rejected - likely entry not yet confirmed in Binance risk engine
                # Cache the rejection so setup_tp_logic knows to retry later
                if client_id:
                    self.trailing_state[(idx, symbol, client_id)] = 'REJECTED_REDUCE_ONLY'
                logging.debug(f"ReduceOnly rejected for {symbol} ({client_id}). Entry likely in-flight.")
            else:
                self.log("limit_order_failed", level='error', account_name=acc_name, is_key=True, error=str(e))
            return None
        except Exception as e:
            self.log("limit_order_failed", level='error', account_name=self.accounts[idx]['info'].get('name'), is_key=True, error=str(e))
            return None

    def _update_account_metrics(self, idx, force=False):
        # Determine client to use (Active trade client preferred, then background client)
        acc = self.accounts.get(idx)
        if not acc:
            acc = self.bg_clients.get(idx)

        if not acc: return
        client = acc['client']

        try:
            # Balance (Weight 1) - Update every 30s unless forced
            balance_throttle = 10 if idx in self.accounts else 30
            if force or time.time() - acc.get('last_balance_update', 0) > balance_throttle:
                acc['last_balance_update'] = time.time()
                balances = self._safe_api_call(client.futures_account_balance)
                usdc_balance = 0.0
                if balances:
                    for b in balances:
                        if b['asset'] == 'USDC':
                            usdc_balance = float(b.get('balance') or 0)
                            break

                with self.data_lock:
                    self.account_balances[idx] = usdc_balance
                    self.account_last_update[idx] = time.time()

            # Positions (Weight 5) - Update every 10s unless forced (WebSocket is primary)
            pos_throttle = 10 if idx in self.accounts else 30
            if force or time.time() - acc.get('last_pos_update', 0) > pos_throttle:
                acc['last_pos_update'] = time.time()

                # Using futures_account for active accounts (more detailed),
                # but futures_position_information for background to save weight if needed
                if idx in self.accounts:
                    account_info = self._safe_api_call(client.futures_account)
                    raw_positions = account_info.get('positions', [])
                    pos_key = 'positionAmt'
                    price_key = 'entryPrice'
                    up_key = 'unrealizedProfit'
                else:
                    raw_positions = self._safe_api_call(client.futures_position_information)
                    pos_key = 'positionAmt'
                    price_key = 'entryPrice'
                    up_key = 'unRealizedProfit'

                new_positions = {}
                for p in raw_positions:
                    sym = p.get('symbol')
                    if not sym: continue
                    amt = float(p.get(pos_key) or 0)
                    if amt != 0:
                        new_positions[sym] = {
                            'symbol': sym,
                            'amount': p.get(pos_key, '0'),
                            'entryPrice': p.get(price_key, '0'),
                            'unrealizedProfit': p.get(up_key, '0'),
                            'leverage': p.get('leverage', '20')
                        }
                with self.data_lock:
                    self.open_positions[idx] = new_positions

            # Open Orders (Weight 1) - Update every 10s
            orders_throttle = 10 if idx in self.accounts else 30
            if force or time.time() - acc.get('last_orders_update', 0) > orders_throttle:
                acc['last_orders_update'] = time.time()
                open_orders = self._safe_api_call(client.futures_get_open_orders)
                with self.data_lock:
                    self.open_orders[idx] = open_orders

            self._emit_account_update()

        except BinanceAPIException as e:
            if e.code == -2015:
                self.account_errors[idx] = "Invalid API Keys or insufficient permissions. Check if IP " + str(self.server_ip) + " is whitelisted."
                log_key = f"invalid_api_{idx}"
                now = time.time()
                if now - self.last_log_times.get(log_key, 0) > 300:
                    self.log(f"Invalid API Keys or insufficient permissions for {acc['info'].get('name')}. Ensure Futures are enabled and IP {self.server_ip} is whitelisted.", level='error', account_name=acc['info'].get('name'))
                    self.last_log_times[log_key] = now
            else:
                logging.error(f"Error updating metrics for account {idx}: {e}")
        except Exception as e:
            logging.error(f"Error updating metrics for account {idx}: {e}")

    def _update_bg_account_metrics(self, idx):
        """Updates balance and positions for background accounts (non-trading)."""
        acc = self.bg_clients.get(idx)
        if not acc: return
        client = acc['client']
        try:
            # Use lightweight balance call (Weight 1)
            balances = self._safe_api_call(client.futures_account_balance)
            usdc_balance = 0.0
            for b in balances:
                if b['asset'] == 'USDC':
                    usdc_balance = float(b.get('balance') or 0)
                    break

            with self.data_lock:
                self.account_balances[idx] = usdc_balance
                self.account_errors[idx] = None

            # Positions - Update every 30s for background accounts as fallback (WebSocket is primary)
            if time.time() - acc.get('last_pos_update', 0) > 30:
                acc['last_pos_update'] = time.time()
                # Use futures_position_information (Weight 5)
                pos_info = self._safe_api_call(client.futures_position_information)
                new_positions = {}
                for p in pos_info:
                    sym = p.get('symbol')
                    if not sym: continue
                    amt = float(p.get('positionAmt') or 0)
                    if amt != 0:
                        new_positions[sym] = {
                            'symbol': sym,
                            'amount': p.get('positionAmt', '0'),
                            'entryPrice': p.get('entryPrice', '0'),
                            'unrealizedProfit': p.get('unRealizedProfit', p.get('unrealizedProfit', '0')),
                            'leverage': p.get('leverage', '20')
                        }
                with self.data_lock:
                    self.open_positions[idx] = new_positions
        except BinanceAPIException as e:
            if e.code == -2015:
                self.account_errors[idx] = "Invalid API Key/Permissions"
                # Don't spam but keep track
                pass
            else:
                # logging.debug(f"Error updating bg metrics for account {idx}: {e}")
                pass
        except Exception as e:
            # logging.error(f"Error updating bg metrics for account {idx}: {e}")
            pass

    def _emit_account_update(self):
        with self.data_lock:
            total_balance = sum(list(self.account_balances.values()))
            total_pnl = 0.0

            all_positions = []
            external_positions = []

            # Use a copy of indices to avoid thread issues
            all_idxs = set(list(self.account_balances.keys()) + list(self.open_positions.keys()))

            for idx in all_idxs:
                # Ensure idx is an integer for list indexing
                try:
                    num_idx = int(idx)
                except (ValueError, TypeError):
                    num_idx = -1

                pos_dict = self.open_positions.get(idx, {})
                api_accounts = self.config.get('api_accounts', [])
                
                if num_idx >= 0 and num_idx < len(api_accounts):
                    acc_name = api_accounts[num_idx].get('name', f"Account {num_idx+1}")
                else:
                    # Fallback for keys that don't match config indices (e.g. background clients or deleted accounts)
                    if isinstance(idx, int) or (isinstance(idx, str) and idx.isdigit()):
                         acc_name = f"Account {num_idx+1}"
                    else:
                         acc_name = str(idx)
                
                for symbol, p in pos_dict.items():
                    p_copy = p.copy()
                    p_copy['account'] = acc_name
                    p_copy['account_idx'] = idx
                    total_pnl += float(p_copy.get('unrealizedProfit') or 0)

                    symbol = p_copy['symbol']
                    trades = self.grid_state.get((idx, symbol), [])
                    p_copy['trade_count'] = len(trades)
                    p_copy['trades'] = []
                    
                    tracked_qty = 0.0
                    tracked_notional = 0.0

                    with self.market_data_lock:
                        current_price = float(self.shared_market_data.get(symbol, {}).get('price') or p_copy.get('entryPrice') or 0)

                    for t in trades:
                        t_qty = self._get_trade_quantity(t)
                        is_filled = t.get('initial_filled', False)

                        trade_pnl = 0.0
                        if is_filled and t_qty > 0:
                            avg_e = float(t.get('avg_entry_price') or 0)
                            strategy = self._get_strategy(idx, symbol)
                            direction = strategy.get('direction', 'LONG')
                            multiplier = 1 if direction == 'LONG' else -1
                            trade_pnl = (current_price - avg_e) * t_qty * multiplier

                            tracked_qty += t_qty
                            tracked_notional += (t_qty * avg_e)

                        if t_qty == 0 and not is_filled:
                            t_qty = float(t.get('quantity') or 0)

                        p_copy['trades'].append({
                            'trade_id': t.get('trade_id'),
                            'entry_price': t.get('avg_entry_price', 0),
                            'amount': t_qty,
                            'filled': is_filled,
                            'pnl': trade_pnl
                        })

                    # Check for external portion
                    total_qty_pa = abs(float(p_copy.get('amount') or 0))
                    diff_qty = total_qty_pa - tracked_qty
                    # Floating point precision check: if difference is less than 0.001% of total, assume it's the same
                    if diff_qty > 1e-4 and (diff_qty / total_qty_pa) > 1e-5:
                        total_avg = float(p_copy.get('entryPrice') or 0)
                        total_notional_val = total_avg * total_qty_pa
                        ext_notional = total_notional_val - tracked_notional
                        ext_avg = ext_notional / diff_qty if diff_qty > 0 else total_avg

                        strategy = self._get_strategy(idx, symbol)
                        direction = strategy.get('direction', 'LONG')
                        multiplier = 1 if direction == 'LONG' else -1
                        ext_pnl = (current_price - ext_avg) * diff_qty * multiplier

                        p_copy['trades'].append({
                            'trade_id': 'External',
                            'entry_price': ext_avg,
                            'amount': diff_qty,
                            'filled': True,
                            'pnl': ext_pnl
                        })

                    p_copy['is_external'] = (len(trades) == 0)
                    all_positions.append(p_copy)
                    if p_copy['is_external']:
                        external_positions.append(p_copy)

            # Gather all open orders from all accounts
            payload_orders = []
            api_accounts = self.config.get('api_accounts', []) # Re-fetch for clarity
            for o_idx, orders in self.open_orders.items():
                # Handle index being a string or integer
                try:
                    acc_id = int(o_idx)
                except:
                    acc_id = -1
                
                # Find account name from config
                acc_name = "Unknown"
                if acc_id >= 0 and acc_id < len(api_accounts):
                    acc_name = api_accounts[acc_id].get('name', f"Account {acc_id+1}")
                else:
                    acc_name = f"Account {o_idx}"

                for o in orders:
                    payload_orders.append({
                        'account': acc_name,
                        'account_idx': acc_id if acc_id >= 0 else o_idx,
                        'symbol': o.get('symbol'),
                        'orderId': o.get('orderId'),
                        'side': o.get('side'),
                        'type': o.get('type'),
                        'qty': o.get('origQty'),
                        'price': o.get('price') or o.get('stopPrice') or '0'
                    })

            payload = {
                'total_balance': total_balance,
                'total_equity': total_balance + total_pnl,
                'total_pnl': total_pnl,
                'positions': all_positions,
                'external_positions': external_positions,
                'open_orders': payload_orders,
                'running': self.is_running,
                'accounts': [
                {
                    'name': self.config.get('api_accounts', [])[idx].get('name', f"Account {idx+1}") if idx < len(self.config.get('api_accounts', [])) else f"Account {idx+1}",
                    'balance': self.account_balances.get(idx, 0.0),
                    'active': idx in self.accounts,
                    'has_client': idx in self.bg_clients,
                    'error': self.account_errors.get(idx),
                    'last_update': self.account_last_update.get(idx, 0)
                } for idx in range(len(self.config.get('api_accounts', [])))
            ]
            }
        self.emit('account_update', payload)

    def apply_live_config_update(self, new_config):
        with self.config_update_lock:
            old_config = self.config
            self.config = new_config

            # Security: Re-apply environment overrides to incoming config
            import os
            for i, acc in enumerate(self.config.get('api_accounts', [])):
                env_key = os.environ.get(f'BINANCE_API_KEY_{i+1}')
                env_secret = os.environ.get(f'BINANCE_API_SECRET_{i+1}')
                if env_key and not acc.get('api_key'):
                    acc['api_key'] = env_key
                if env_secret and not acc.get('api_secret'):
                    acc['api_secret'] = env_secret

            # Handle Language Change
            lang_changed = old_config.get('language') != self.config.get('language')
            if lang_changed:
                self.language = self.config.get('language', 'pt-BR')
                # Update all existing logs in the queue
                for entry in self.console_logs:
                    entry['rendered'] = self._render_log(entry)
                # Re-emit the whole status to refresh UI text
                self.emit('bot_status', {'running': self.is_running})
                self.emit('clear_console', {})
                for log in list(self.console_logs):
                    self.emit('console_log', log)

            # Check if critical parts changed
            demo_changed = old_config.get('is_demo') != self.config.get('is_demo')
            symbols_changed = set(old_config.get('symbols', [])) != set(self.config.get('symbols', []))

            # Detect account changes
            old_accs = old_config.get('api_accounts', [])
            new_accs = self.config.get('api_accounts', [])
            accs_changed = len(old_accs) != len(new_accs)
            if not accs_changed:
                for oa, na in zip(old_accs, new_accs):
                    if (oa.get('api_key') != na.get('api_key') or
                        oa.get('api_secret') != na.get('api_secret') or
                        oa.get('enabled') != na.get('enabled')):
                        accs_changed = True
                        break

            if demo_changed:
                mode_str = "DEMO (Testnet)" if self.config.get('is_demo') else "LIVE (Mainnet)"
                self.log(f"Switching to {mode_str} mode. Clearing caches...", level='warning')

                # Clear all environment-specific data
                with self.market_data_lock:
                    self.shared_market_data = {}
                    self.max_leverages = {}

                with self.data_lock:
                    self.grid_state = {}
                    self.trailing_state = {}
                    self.account_balances = {}
                    self.open_positions = {}

                # Full restart of WebSockets
                self._initialize_bg_clients()
                self._initialize_market_ws()

                self._close_client(self._metadata_client_instance)
                self._close_client(self._market_client_instance)
                self._metadata_client_instance = None
                self._market_client_instance = None
            else:
                # Surgical updates if not a full mode change
                if accs_changed:
                    self._initialize_bg_clients()
                    self._close_client(self._metadata_client_instance)
                    self._metadata_client_instance = None

                if symbols_changed:
                    self._initialize_market_ws()

            # Cleanup stale data for removed accounts or cleared keys
            num_accounts = len(self.config.get('api_accounts', []))
            api_accounts = self.config.get('api_accounts', [])

            with self.data_lock:
                for idx in list(self.account_balances.keys()):
                    if idx >= num_accounts:
                        del self.account_balances[idx]
                    else:
                        acc = api_accounts[idx]
                        if not acc.get('api_key') or not acc.get('api_secret'):
                            del self.account_balances[idx]

                for idx in list(self.open_positions.keys()):
                    if idx >= num_accounts:
                        del self.open_positions[idx]
                    else:
                        acc = api_accounts[idx]
                        if not acc.get('api_key') or not acc.get('api_secret'):
                            del self.open_positions[idx]

            self._emit_account_update()

            if self.is_running:
                if demo_changed:
                    # engine.stop() resets self.accounts but we need to keep it surgical if possible
                    # but for demo switch, a full cycle is safest to clear order IDs
                    self.stop()
                    self.start()
                    return {"success": True}

                api_accounts = self.config.get('api_accounts', [])
                symbols = self.config.get('symbols', [])

                # Handle account changes (Surgical updates for active trading)
                active_idxs = list(self.accounts.keys())
                for idx in active_idxs:
                    if idx >= len(api_accounts):
                        # Account removed from config
                        self._close_client(self.accounts[idx].get('client'))
                        del self.accounts[idx]
                        with self.data_lock:
                            for key in list(self.grid_state.keys()):
                                if key[0] == idx: del self.grid_state[key]
                            for key in list(self.symbol_threads.keys()):
                                if key[0] == idx: del self.symbol_threads[key]

                for i, acc_config in enumerate(api_accounts):
                    enabled = acc_config.get('enabled', True)
                    api_key = acc_config.get('api_key', '').strip()
                    api_secret = acc_config.get('api_secret', '').strip()
                    has_keys = api_key and api_secret

                    if i in self.accounts:
                        old_acc_config = self.accounts[i]['info']
                        keys_changed = (old_acc_config.get('api_key', '').strip() != api_key or
                                        old_acc_config.get('api_secret', '').strip() != api_secret)

                        if not enabled or not has_keys or keys_changed:
                            if i not in self.bg_clients:
                                self._close_client(self.accounts[i].get('client'))
                            del self.accounts[i]
                            with self.data_lock:
                                for key in list(self.grid_state.keys()):
                                    if key[0] == i: del self.grid_state[key]
                                for key in list(self.symbol_threads.keys()):
                                    if key[0] == i: del self.symbol_threads[key]
                        else:
                            self.accounts[i]['info'] = acc_config

                    if i not in self.accounts and enabled and has_keys:
                        self._init_account(i, acc_config)

                for idx in self.accounts:
                    # 1. Start threads for new symbols
                    for symbol in symbols:
                        self._start_symbol_thread(idx, symbol)

                    # 2. Update leverage for all active symbols
                    for symbol in symbols:
                        strategy = self._get_strategy(idx, symbol)
                        if strategy:
                            leverage = int(strategy.get('leverage') or 20)
                            try:
                                # Clamp to max allowed
                                max_l = self.max_leverages.get(symbol, 125)
                                if leverage > max_l: leverage = max_l

                                self._safe_api_call(self.accounts[idx]['client'].futures_change_leverage, symbol=symbol, leverage=leverage)
                            except Exception as e:
                                pass

            return {"success": True}

    def close_position(self, account_idx, symbol, trade_id=None, order_type=Client.FUTURE_ORDER_TYPE_MARKET):
        """Closes a position. If trade_id is provided, only closes that specific trade's portion."""
        # Find the index for account_idx if it's a name
        idx = None
        if isinstance(account_idx, int):
            idx = account_idx
        else:
            for i, acc in self.accounts.items():
                if acc['info'].get('name') == account_idx:
                    idx = i; break
            if idx is None:
                for i, acc in self.bg_clients.items():
                    if acc.get('name') == account_idx:
                        idx = i; break

        if idx is None: return

        target_client = self.accounts[idx]['client'] if idx in self.accounts else self.bg_clients.get(idx, {}).get('client')
        if not target_client: return

        try:
            if trade_id:
                # --- Specific trade close ---
                # Step 1: Gather order IDs to cancel under the lock (no API calls)
                orders_to_cancel = []
                qty_to_close = 0
                strategy = None
                with self.data_lock:
                    trades = self.grid_state.get((idx, symbol), [])
                    trade = next((t for t in trades if t.get('trade_id') == trade_id), None)
                    if not trade:
                        return

                    if trade.get('initial_order_id') and trade['initial_order_id'] != 'PENDING':
                        orders_to_cancel.append(trade['initial_order_id'])
                    if trade.get('levels'):
                        for lvl in trade['levels'].values():
                            if lvl.get('tp_order_id'):
                                orders_to_cancel.append(lvl['tp_order_id'])
                    if trade.get('consolidated_tp_id'):
                        orders_to_cancel.append(trade['consolidated_tp_id'])
                    if trade.get('consolidated_reentry_id'):
                        orders_to_cancel.append(trade['consolidated_reentry_id'])

                    qty_to_close = self._get_trade_quantity(trade)
                    strategy = self._get_strategy(idx, symbol)

                # Step 2: API calls OUTSIDE the lock to avoid deadlock
                for oid in orders_to_cancel:
                    try:
                        self._safe_api_call(target_client.futures_cancel_order, symbol=symbol, orderId=oid)
                    except:
                        pass

                if qty_to_close > 0:
                    direction = strategy.get('direction', 'LONG') if strategy else 'LONG'
                    side = Client.SIDE_SELL if direction == 'LONG' else Client.SIDE_BUY

                    if order_type == Client.FUTURE_ORDER_TYPE_LIMIT:
                        price = float(strategy.get('sl_order_price') or strategy.get('stop_loss_price') or 0)
                        if price <= 0:
                            with self.market_data_lock:
                                price = float(self.shared_market_data.get(symbol, {}).get('price') or 0)

                        if price > 0:
                            self._place_limit_order(idx, symbol, side, qty_to_close, price, reduce_only=True)
                        else:
                            self._execute_market_close_partial(idx, symbol, qty_to_close, side)
                    else:
                        self._execute_market_close_partial(idx, symbol, qty_to_close, side)

                # Step 3: Remove trade from state under the lock, then emit update
                with self.data_lock:
                    trades = self.grid_state.get((idx, symbol), [])
                    self.grid_state[(idx, symbol)] = [t for t in trades if t.get('trade_id') != trade_id]
                    if not self.grid_state[(idx, symbol)]:
                        del self.grid_state[(idx, symbol)]

                acc_name = self.config.get('api_accounts', [])[idx].get('name', str(idx)) if idx < len(self.config.get('api_accounts', [])) else str(idx)
                self.log(f"Closed trade {trade_id} for {symbol}", account_name=acc_name, is_key=True)

            else:
                # --- Full symbol close ---
                # API calls OUTSIDE any lock
                self._safe_api_call(target_client.futures_cancel_all_open_orders, symbol=symbol)

                pos = self._safe_api_call(target_client.futures_position_information, symbol=symbol)
                for p in (pos or []):
                    if p.get('symbol') == symbol:
                        amt = float(p.get('positionAmt') or 0)
                        if amt != 0:
                            side = Client.SIDE_SELL if amt > 0 else Client.SIDE_BUY
                            abs_amt = abs(amt)
                            with self.market_data_lock:
                                current_price = float(self.shared_market_data.get(symbol, {}).get('price') or 0)

                            if current_price > 0:
                                notional = abs_amt * current_price
                                min_notional = 5.0
                                with self.market_data_lock:
                                    info = self.shared_market_data.get(symbol, {}).get('info')
                                    if info:
                                        for f in info.get('filters', []):
                                            if f.get('filterType') == 'MIN_NOTIONAL':
                                                min_notional = float(f.get('notional') or f.get('minNotional') or 5.0)
                                                break
                                if notional < min_notional:
                                    self.log(f"Cannot close dust position for {symbol} - Notional {notional:.2f} < Min {min_notional}.", level='warning', account_name=str(account_idx))
                                    continue

                            self._safe_api_call(target_client.futures_create_order,
                                symbol=symbol,
                                side=side,
                                type=Client.FUTURE_ORDER_TYPE_MARKET,
                                quantity=self._format_quantity(symbol, abs_amt)
                            )

                # Remove state under lock
                with self.data_lock:
                    if isinstance(account_idx, int):
                        if (account_idx, symbol) in self.grid_state:
                            del self.grid_state[(account_idx, symbol)]
                    else:
                        for (gidx, gsym) in list(self.grid_state.keys()):
                            if gsym == symbol:
                                api_accounts = self.config.get('api_accounts', [])
                                a_name = api_accounts[gidx].get('name') if gidx < len(api_accounts) else None
                                if a_name == account_idx:
                                    del self.grid_state[(gidx, gsym)]

                acc_name = self.config.get('api_accounts', [])[account_idx].get('name') if isinstance(account_idx, int) and account_idx < len(self.config.get('api_accounts', [])) else str(account_idx)
                self.log("pos_closed", account_name=acc_name, is_key=True, symbol=symbol)

            # Always emit an account update so frontend reflects the change
            self._emit_account_update()

        except Exception as e:
            self.log("error_closing_pos", level='error', account_name=str(account_idx), is_key=True, error=str(e))

    def cancel_order(self, account_idx, symbol, order_id):
        """Cancels a specific order for an account."""
        # Find the index for account_idx if it's a name
        idx = None
        if isinstance(account_idx, int):
            idx = account_idx
        else:
            for i, acc in self.accounts.items():
                if acc['info'].get('name') == account_idx:
                    idx = i; break
            if idx is None:
                for i, acc in self.bg_clients.items():
                    if acc.get('name') == account_idx:
                        idx = i; break

        if idx is None:
            self.log(f"Account {account_idx} not found for cancellation", "error")
            return False

        target_client = self.accounts[idx]['client'] if idx in self.accounts else self.bg_clients.get(idx, {}).get('client')
        if not target_client: return False

        try:
            self._safe_api_call(target_client.futures_cancel_order, symbol=symbol, orderId=order_id)
            acc_name = self.accounts[idx]['info'].get('name') if idx in self.accounts else self.bg_clients[idx]['name']
            self.log(f"Order {order_id} for {symbol} cancelled manually.", "info", account_name=acc_name)
            # Force immediate UI refresh
            self._update_account_metrics(idx, force=True)
            return True
        except Exception as e:
            acc_name = self.accounts[idx]['info'].get('name') if idx in self.accounts else self.bg_clients.get(idx, {}).get('name', str(account_idx))
            self.log(f"Failed to cancel order {order_id}: {str(e)}", "error", account_name=acc_name)
            return False

    def _global_background_worker(self):
        """Global worker for fetching prices once and updating shared metrics."""
        while not self.stop_event.is_set():
            try:
                # 1. Update shared market data (prices)
                # Ensure we use a fresh list of symbols from the live config
                symbols = list(self.config.get('symbols', []))
                
                # Use metadata client or first active client for non-public metadata if needed
                active_client = None
                for acc in list(self.accounts.values()):
                    if acc.get('client'):
                        active_client = acc['client']
                        break
                if not active_client:
                    active_client = self.metadata_client

                if symbols:
                    # Optimize: Check if we need to fetch exchange info for any missing symbols
                    missing_info = [s for s in symbols if s not in self.shared_market_data or 'info' not in self.shared_market_data[s]]
                    if missing_info:
                        try:
                            # exchange_info is public, but we'll use market_client
                            m_client = self.market_client
                            if m_client:
                                ex_info = self._safe_api_call(m_client.futures_exchange_info)
                                with self.market_data_lock:
                                    for s_data in ex_info['symbols']:
                                        s_name = s_data['symbol']
                                        if s_name in symbols:
                                            if s_name not in self.shared_market_data:
                                                self.shared_market_data[s_name] = {'price': 0, 'last_update': 0}
                                            self.shared_market_data[s_name]['info'] = s_data
                        except Exception as e:
                            logging.error(f"Error fetching exchange info: {e}")

                    # 1a. WebSocket Health Check & Fallback
                    now = time.time()
                    price_map_emit = {}
                    m_client = self.market_client

                    symbols_to_poll = []
                    with self.market_data_lock:
                        for symbol in symbols:
                            data = self.shared_market_data.get(symbol)
                            if not data or (now - data.get('last_update', 0) > 10):
                                symbols_to_poll.append(symbol)

                    for symbol in symbols_to_poll:
                        try:
                            if m_client:
                                ticker = self._safe_api_call(m_client.futures_symbol_ticker, symbol=symbol)
                                with self.market_data_lock:
                                    if symbol not in self.shared_market_data:
                                        self.shared_market_data[symbol] = {'price': 0, 'last_update': 0}
                                    data = self.shared_market_data[symbol]
                                    data['price'] = float(ticker.get('price') or 0)
                                    data['last_update'] = time.time()
                        except: pass

                    with self.market_data_lock:
                        for symbol in symbols:
                            data = self.shared_market_data.get(symbol)
                            if data and 'price' in data:
                                price_map_emit[symbol] = {
                                    'bid': data.get('bid', data['price']),
                                    'ask': data.get('ask', data['price']),
                                    'last': data['price']
                                }
                    
                    if price_map_emit:
                        self.emit('price_update', price_map_emit)

                    # 1b. Fetch max leverage if not cached - Needs authentication
                    for symbol in symbols:
                        if symbol not in self.max_leverages:
                            try:
                                # Try active_client (first authenticated client)
                                if active_client:
                                    # This call needs authentication and can fail with -2015
                                    brackets = self._safe_api_call(active_client.futures_leverage_bracket, symbol=symbol)
                                    if brackets and len(brackets) > 0:
                                        # Handle different response formats
                                        if isinstance(brackets, list) and len(brackets) > 0:
                                            bracket_info = brackets[0]
                                            if 'brackets' in bracket_info:
                                                max_l = bracket_info['brackets'][0]['initialLeverage']
                                                self.max_leverages[symbol] = max_l
                                            elif 'initialLeverage' in bracket_info:
                                                self.max_leverages[symbol] = bracket_info['initialLeverage']
                            except Exception:
                                pass
                    
                    # Log warnings for symbols not found (typos or unsupported)
                    for s in symbols:
                        if s not in price_map_emit:
                            log_key = f"symbol_not_found_{s}"
                            now = time.time()
                            if now - self.last_log_times.get(log_key, 0) > 300: # Every 5 mins
                                self.log(f"Warning: Symbol {s} not found on Binance. Please check if it is a valid USDC pair.", level='warning')
                                self.last_log_times[log_key] = now

                    # 2. Update balances for all accounts (trading or background)
                    # We iterate over all accounts that have a client
                    all_bg_idxs = list(self.bg_clients.keys())
                    for idx in all_bg_idxs:
                        try:
                            # If it's also an active trading account, we use its specific metric update
                            if idx in self.accounts:
                                self._update_account_metrics(idx)
                            else:
                                self._update_bg_account_metrics(idx)
                        except Exception:
                            pass
                    
                    self._emit_account_update()
                    self.emit('max_leverages', self.max_leverages)
                    
                time.sleep(10) # Reduced polling frequency to prioritize WebSockets
            except Exception as e:
                # logging.error(f"Global worker error: {e}")
                time.sleep(10)

    def _emit_latest_prices(self):
        """Broadcasts the current last-known state from shared memory."""
        with self.market_data_lock:
            # Reconstruct price map from shared storage with full {bid, ask, last}
            price_map = {}
            for s, data in list(self.shared_market_data.items()):
                price_map[s] = {
                    'bid': data.get('bid', data['price']),
                    'ask': data.get('ask', data['price']),
                    'last': data['price']
                }
            if price_map:
                self.emit('price_update', price_map)
            self.emit('max_leverages', self.max_leverages)

    def _start_symbol_thread(self, idx, symbol):
        key = (idx, symbol)
        with self.data_lock:
            if key in self.symbol_threads and not self.symbol_threads[key].is_alive():
                del self.symbol_threads[key]

            if key not in self.symbol_threads:
                t = threading.Thread(target=self._symbol_logic_worker, args=(idx, symbol), daemon=True)
                self.symbol_threads[key] = t
                t.start()
                self.log("started_thread", account_name=self.accounts[idx]['info'].get('name'), is_key=True, symbol=symbol)

    def _symbol_logic_worker(self, idx, symbol):
        """Dedicated worker for each symbol's grid and trailing logic."""
        try:
            if idx not in self.accounts: return
            current_client = self.accounts[idx].get('client')
            self._setup_strategy_for_account(idx, symbol)

            while self.is_running and not self.stop_event.is_set():
                if idx not in self.accounts or self.accounts[idx].get('client') != current_client:
                    break
                try:
                    # Check if this symbol still in config (for live removal)
                    if symbol not in self.config.get('symbols', []):
                        self.log("stopping_thread", is_key=True, symbol=symbol)
                        break
                    # 2. Check and place initial entry
                    self._check_and_place_initial_entry(idx, symbol, trade_id='manual')

                    # 3. Trailing Take Profit Logic
                    self._trailing_tp_logic(idx, symbol)

                    # 4. Market Take Profit Logic (if enabled)
                    self._tp_market_logic(idx, symbol)

                    # 5. Stop Loss Logic
                    self._stop_loss_logic(idx, symbol)
                    self._trailing_buy_logic(idx, symbol)
                    self._conditional_logic(idx, symbol)
                    time.sleep(1)
                except Exception as e:
                    logging.error(f"Symbol logic worker error ({symbol}): {e}")
                    time.sleep(5)
        finally:
            with self.data_lock:
                key = (idx, symbol)
                if self.symbol_threads.get(key) == threading.current_thread():
                    del self.symbol_threads[key]


    def _trailing_buy_logic(self, idx, symbol):
        with self.data_lock:
            trades = self.grid_state.get((idx, symbol), [])
            if not trades: return
            
        strategy = self._get_strategy(idx, symbol)
        direction = strategy.get('direction', 'LONG')
        dev_pct = float(strategy.get('trailing_buy_deviation') or 0.1)

        with self.market_data_lock:
            current_price = float(self.shared_market_data.get(symbol, {}).get('price') or 0)
        
        if current_price <= 0: return

        for trade in trades:
            if not trade.get('trailing_buy_active'): continue
            
            target_price = float(trade.get('trailing_buy_target') or 0)
            trade_id = trade.get('trade_id', 'manual')

            # For LONG trailing buy: price must first hit target, then we track the LOWEST price, then we buy when it bounces up by dev_pct.
            if direction == 'LONG':
                if current_price <= target_price:
                    with self.data_lock:
                        if trade.get('trailing_buy_peak', 0) == 0 or current_price < trade['trailing_buy_peak']:
                            trade['trailing_buy_peak'] = current_price

                        peak = trade['trailing_buy_peak']
                    
                    # Check for bounce
                    retrace = (current_price - peak) / peak * 100
                    if retrace >= dev_pct:
                        self.log("trailing_buy_triggered", account_name=self.accounts[idx]['info'].get('name'), is_key=True, symbol=symbol, bounce=f"{retrace:.2f}", price=current_price)
                        with self.data_lock:
                            trade['trailing_buy_active'] = False
                        self._execute_market_entry(idx, symbol, trade_id=trade_id)
            else: # SHORT
                if current_price >= target_price:
                    with self.data_lock:
                        if trade.get('trailing_buy_peak', 0) == 0 or current_price > trade['trailing_buy_peak']:
                            trade['trailing_buy_peak'] = current_price

                        peak = trade['trailing_buy_peak']
                    
                    # Check for dip
                    retrace = (peak - current_price) / peak * 100
                    if retrace >= dev_pct:
                        self.log("trailing_sell_triggered", account_name=self.accounts[idx]['info'].get('name'), is_key=True, symbol=symbol, dip=f"{retrace:.2f}", price=current_price)
                        with self.data_lock:
                            trade['trailing_buy_active'] = False
                        self._execute_market_entry(idx, symbol, trade_id=trade_id)

    def _conditional_logic(self, idx, symbol):
        with self.data_lock:
            trades = self.grid_state.get((idx, symbol), [])
            if not trades: return
            
        strategy = self._get_strategy(idx, symbol)
        direction = strategy.get('direction', 'LONG')

        with self.market_data_lock:
            current_price = float(self.shared_market_data.get(symbol, {}).get('price') or 0)
        
        if current_price <= 0: return

        for trade in trades:
            if not trade.get('conditional_active'): continue
            
            trigger_price = float(trade.get('trigger_price') or 0)
            cond_type = trade.get('conditional_type', 'CONDITIONAL')
            trade_id = trade.get('trade_id', 'manual')

            # Trigger logic
            triggered = False
            if direction == 'LONG':
                if current_price >= trigger_price: triggered = True
            else: # SHORT
                if current_price <= trigger_price: triggered = True

            if triggered:
                self.log("conditional_triggered", account_name=self.accounts[idx]['info'].get('name'), is_key=True, symbol=symbol, price=current_price)
                with self.data_lock:
                    trade['conditional_active'] = False
                
                if cond_type == 'COND_MARKET' or cond_type == 'CONDITIONAL':
                    self._execute_market_entry(idx, symbol, trade_id=trade_id)
                else: # COND_LIMIT
                    order_price = trigger_price # Use same price for trigger and execution
                    trade_amount_usdc = float(strategy.get('trade_amount_usdc') or 0)
                    leverage = int(strategy.get('leverage') or 20)
                    quantity = (trade_amount_usdc * leverage) / order_price
                    side = Client.SIDE_BUY if direction == 'LONG' else Client.SIDE_SELL
                    
                    try:
                        # Use trade_id-specific client ID
                        client_id = f"trd-{trade_id}-entry"
                        new_id = self._place_limit_order(idx, symbol, side, quantity, order_price, client_id=client_id)
                        if new_id:
                            with self.data_lock:
                                trade['initial_order_id'] = new_id
                                trade['initial_orders'] = {new_id: {'qty': quantity, 'price': order_price, 'filled': False}}
                                trade['quantity'] = quantity
                                trade['avg_entry_price'] = order_price

                            # IMMEDIATELY PLACE TP GRID (Production Ready: Don't wait for fill)
                            if strategy.get('tp_enabled', True):
                                tp_targets = strategy.get('tp_targets', [])
                                if not tp_targets:
                                    dev = float(strategy.get('price_deviation') or 0.6)
                                    total_f = int(strategy.get('total_fractions') or 8)
                                    tp_targets = [{'percent': (i + 1) * dev, 'volume': 100.0 / total_f} for i in range(total_f)]

                                trailing_enabled = strategy.get('trailing_tp_enabled', strategy.get('trailing_enabled', False))
                                self._setup_tp_targets_logic(idx, symbol, order_price, tp_targets, quantity, direction, trailing_enabled, trade_id)
                    except Exception as e:
                        self.log("cond_limit_failed", level='error', account_name=self.accounts[idx]['info'].get('name'), is_key=True, error=str(e))

    def _execute_market_entry(self, idx, symbol, trade_id='manual'):
        strategy = self._get_strategy(idx, symbol)
        trade_amount_usdc = float(strategy.get('trade_amount_usdc') or 0)
        leverage = int(strategy.get('leverage') or 20)
        direction = strategy.get('direction', 'LONG')
        
        with self.market_data_lock:
            current_price = float(self.shared_market_data.get(symbol, {}).get('price') or 0)
        
        if current_price <= 0: return
        quantity = (trade_amount_usdc * leverage) / current_price
        side = Client.SIDE_BUY if direction == 'LONG' else Client.SIDE_SELL
        
        if not self._check_balance_for_order(idx, quantity, current_price):
            log_key = f"insufficient_balance_{idx}_{symbol}"
            now = time.time()
            if now - self.last_log_times.get(log_key, 0) > 60:
                self.log("insufficient_balance", level='warning', account_name=self.accounts[idx]['info'].get('name'), is_key=True, qty=quantity, price=current_price)
                self.last_log_times[log_key] = now
            return

        try:
            self.log("executing_market_entry", account_name=self.accounts[idx]['info'].get('name'), is_key=True, symbol=symbol, direction=direction)
            
            client_id = f"trd-{trade_id}-entry"
            order = self._safe_api_call(self.accounts[idx]['client'].futures_create_order,
                symbol=symbol,
                side=side,
                type=Client.FUTURE_ORDER_TYPE_MARKET,
                quantity=self._format_quantity(symbol, quantity),
                newClientOrderId=client_id
            )

            order_id = order.get('orderId')

            # Ensure trade state exists for tracking fill
            with self.data_lock:
                if (idx, symbol) not in self.grid_state:
                    self.grid_state[(idx, symbol)] = []
                
                new_trade = {
                    'trade_id': trade_id,
                    'initial_filled': False,
                    'initial_order_id': order_id,
                    'initial_orders': {order_id: {'qty': quantity, 'price': current_price, 'filled': False}},
                    'quantity': quantity,
                    'levels': {}
                }
                self.grid_state[(idx, symbol)].append(new_trade)

            # IMMEDIATELY PLACE TP GRID (Production Ready: Don't wait for fill)
            if strategy.get('tp_enabled', True):
                tp_targets = strategy.get('tp_targets', [])
                if not tp_targets:
                    dev = float(strategy.get('price_deviation') or 0.6)
                    total_f = int(strategy.get('total_fractions') or 8)
                    tp_targets = [{'percent': (i + 1) * dev, 'volume': 100.0 / total_f} for i in range(total_f)]

                trailing_enabled = strategy.get('trailing_tp_enabled', strategy.get('trailing_enabled', False))
                # For MARKET entry, we use current_price as proxy until fill event provides exact avg price
                self._setup_tp_targets_logic(idx, symbol, current_price, tp_targets, quantity, direction, trailing_enabled, trade_id)
        except Exception as e:
            self.log("market_entry_failed", level='error', account_name=self.accounts[idx]['info'].get('name'), is_key=True, error=str(e))
            # Cleanup if failed
            with self.data_lock:
                if (idx, symbol) in self.grid_state:
                    self.grid_state[(idx, symbol)] = [t for t in self.grid_state[(idx, symbol)] if t.get('trade_id') != trade_id]

    def _tp_market_logic(self, idx, symbol):
        """Monitors price for Take Profit execution if set to Market mode or as a fallback for unplaced Limit orders."""
        with self.market_data_lock:
            current_price = float(self.shared_market_data.get(symbol, {}).get('price') or 0)
        
        if current_price <= 0: return

        strategy = self._get_strategy(idx, symbol)
        direction = strategy.get('direction', 'LONG')

        to_execute = []
        acc_name = None
        with self.data_lock:
            trades = self.grid_state.get((idx, symbol), [])
            if not trades: return

            acc_name = self.accounts[idx]['info'].get('name')

            for trade in trades:
                if not trade.get('initial_filled'): continue
                
                levels = trade.get('levels', {})
                if not levels: continue
                
                trade_id = trade.get('trade_id', 'auto')

                for lvl_idx, lvl in list(levels.items()):
                    # Only trigger if it's explicitly MARKET mode OR if it's a fallback for an UNPLACED limit order
                    # trailing_eligible targets are handled by _trailing_tp_logic
                    if not lvl.get('tp_order_id') and not lvl.get('filled') and not lvl.get('trailing_eligible') and lvl.get('is_market'):
                        target_price = lvl['price']
                        triggered = False
                        if direction == 'LONG':
                            if current_price >= target_price: triggered = True
                        else: # SHORT
                            if current_price <= target_price: triggered = True
                        
                        if triggered:
                            to_execute.append((trade_id, lvl_idx, lvl['qty'], lvl['side']))

        for tid, lvl_idx, qty, side in to_execute:
            self.log("tp_market_triggered", account_name=acc_name, is_key=True, symbol=symbol, target_level=lvl_idx, price=current_price)
            # Execute Market Order outside lock
            success = self._execute_market_close_partial(idx, symbol, qty, side)
            if success:
                with self.data_lock:
                    trade = self._get_trade(idx, symbol, tid)
                    if trade and lvl_idx in trade['levels']:
                        trade['levels'][lvl_idx]['filled'] = True
                    self.log("tp_filled_market", account_name=acc_name, is_key=True, symbol=symbol, target_level=lvl_idx)

                self._handle_reentry_logic(idx, symbol, qty, trade_id=tid)

    def _execute_market_close_partial(self, idx, symbol, qty, side):
        """Executes a market order to close part of a position."""
        if idx not in self.accounts: return False
        acc = self.accounts[idx]
        client = acc['client']
        try:
            qty_str = self._format_quantity(symbol, qty)
            f_qty = float(qty_str)
            if f_qty <= 0:
                self.log("order_skipped_qty", level='warning', account_name=acc_name, is_key=True, symbol=symbol, qty=qty_str)
                return True # Mark as "handled" to stop the loop

            # Notional check
            with self.market_data_lock:
                current_price = float(self.shared_market_data.get(symbol, {}).get('price') or 0)

            if current_price > 0:
                notional = f_qty * current_price
                min_notional = 5.0
                with self.market_data_lock:
                    info = self.shared_market_data.get(symbol, {}).get('info')
                    if info:
                        for f in info.get('filters', []):
                            if f.get('filterType') == 'MIN_NOTIONAL':
                                min_notional = float(f.get('notional') or f.get('minNotional') or 5.0)
                                break
                if notional < min_notional:
                    self.log("order_skipped_min_notional", level='warning', account_name=acc_name, is_key=True, symbol=symbol, notional=f"{notional:.2f}", min_notional=f"{min_notional:.2f}")
                    return True # Mark as "handled" to stop the loop

            self._safe_api_call(client.futures_create_order,
                symbol=symbol,
                side=side,
                type=Client.FUTURE_ORDER_TYPE_MARKET,
                quantity=qty_str
            )
            self.log(f"Market close partial for {symbol} - Qty: {qty_str}", account_name=acc['info'].get('name'), is_key=True)
            return True
        except Exception as e:
            self.log("market_close_failed", level='error', account_name=self.accounts[idx]['info'].get('name'), is_key=True, error=str(e))
            return False

    def _stop_loss_logic(self, idx, symbol):
        strategy = self._get_strategy(idx, symbol)
        if not strategy.get('stop_loss_enabled'): return
        
        sl_price = float(strategy.get('stop_loss_price') or 0)
        if sl_price <= 0: return

        with self.market_data_lock:
            current_price = float(self.shared_market_data.get(symbol, {}).get('price') or 0)

        if current_price == 0: return

        to_close = []
        with self.data_lock:
            trades = self.grid_state.get((idx, symbol), [])
            if not trades: return
            
            acc_name = self.accounts[idx]['info'].get('name')
            direction = strategy.get('direction', 'LONG')
            
            for trade in trades:
                if not trade.get('initial_filled'): continue
                
                trade_id = trade.get('trade_id', 'auto')
                
                # Trailing Stop Loss Logic (Per trade)
                # Note: We use trade-specific SL price if it exists, otherwise fallback to strategy
                t_sl_price = float(trade.get('stop_loss_price') or sl_price)

                if strategy.get('trailing_sl_enabled'):
                    peak_key = (idx, symbol, trade_id, 'sl_peak')
                    if peak_key not in self.trailing_state:
                        self.trailing_state[peak_key] = {'peak': current_price}
                    
                    peak = self.trailing_state[peak_key]['peak']
                    if direction == 'LONG':
                        if current_price > peak:
                            diff = current_price - peak
                            t_sl_price += diff # Move SL up with price
                            self.trailing_state[peak_key]['peak'] = current_price
                            trade['stop_loss_price'] = t_sl_price
                    else:
                        if current_price < peak:
                            diff = peak - current_price
                            t_sl_price -= diff # Move SL down with price
                            self.trailing_state[peak_key]['peak'] = current_price
                            trade['stop_loss_price'] = t_sl_price

                # Move to Breakeven Logic
                if strategy.get('move_to_breakeven'):
                    anchor = trade.get('avg_entry_price', float(strategy.get('entry_price') or 0))
                    if direction == 'LONG' and current_price > anchor * 1.005: # 0.5% in profit
                         if t_sl_price < anchor:
                             t_sl_price = anchor
                             trade['stop_loss_price'] = t_sl_price
                    elif direction == 'SHORT' and current_price < anchor * 0.995:
                         if t_sl_price > anchor:
                             t_sl_price = anchor
                             trade['stop_loss_price'] = t_sl_price

                # Stop Loss Timeout Logic
                triggered = (direction == 'LONG' and current_price <= t_sl_price) or \
                            (direction == 'SHORT' and current_price >= t_sl_price)
                
                if triggered:
                    if strategy.get('sl_timeout_enabled'):
                        timeout_sec = int(strategy.get('sl_timeout_duration') or 10)
                        trigger_key = (idx, symbol, trade_id, 'sl_trigger_time')
                        if trigger_key not in self.trailing_state:
                            self.trailing_state[trigger_key] = time.time()
                            self.log("sl_timeout_started", account_name=acc_name, is_key=False, sec=timeout_sec)
                            continue # Check next trade
                        
                        elapsed = time.time() - self.trailing_state[trigger_key]
                        if elapsed < timeout_sec:
                            continue # Still waiting
                    
                    sl_type = strategy.get('sl_type', 'COND_MARKET')
                    self.log("stop_loss_triggered", account_name=acc_name, is_key=True, symbol=symbol, price=current_price, type=sl_type)
                    trade['initial_filled'] = False
                    # Clean up trigger time
                    trigger_key = (idx, symbol, trade_id, 'sl_trigger_time')
                    if trigger_key in self.trailing_state: del self.trailing_state[trigger_key]
                    
                    to_close.append((trade_id, sl_type))
                else:
                    # Price recovered, reset timeout if any
                    trigger_key = (idx, symbol, trade_id, 'sl_trigger_time')
                    if trigger_key in self.trailing_state: 
                        del self.trailing_state[trigger_key]
                        self.log("sl_timeout_reset", account_name=acc_name, is_key=False)

        for tid, sl_type in to_close:
            # Handle Limit vs Market closure for SL
            order_type = Client.FUTURE_ORDER_TYPE_LIMIT if sl_type == 'COND_LIMIT' else Client.FUTURE_ORDER_TYPE_MARKET
            self.close_position(acc_name, symbol, trade_id=tid, order_type=order_type)

    def _trailing_tp_logic(self, idx, symbol):
        strategy = self._get_strategy(idx, symbol)
        if not strategy.get('trailing_tp_enabled', strategy.get('trailing_enabled', False)): return

        with self.data_lock:
            trades = self.grid_state.get((idx, symbol), [])
            if not trades: return
            
            direction = strategy.get('direction', 'LONG')
            deviation = float(strategy.get('trailing_deviation') or 0.5) / 100.0

        with self.market_data_lock:
            current_price = float(self.shared_market_data.get(symbol, {}).get('price') or 0)

        if current_price == 0: return

        to_close = []
        with self.data_lock:
            for trade in trades:
                if not trade.get('initial_filled'): continue
                
                trade_id = trade.get('trade_id', 'auto')
                anchor = trade.get('avg_entry_price', float(strategy.get('entry_price') or 0))
                profit_pct = (current_price - anchor) / anchor if direction == 'LONG' else (anchor - current_price) / anchor

                peak_key = (idx, symbol, trade_id, 'tp_peak')

                if not trade.get('tp_trailing_active'):
                    if profit_pct > 0.001: 
                         trade['tp_trailing_active'] = True
                         self.trailing_state[peak_key] = current_price
                         self.log("trailing_tp_tracking", account_name=self.accounts[idx]['info'].get('name'), is_key=False, symbol=symbol)
                    continue

                # Update peak
                peak = self.trailing_state.get(peak_key, current_price)
                trigger_close_trade = False
                if direction == 'LONG':
                    if current_price > peak:
                        self.trailing_state[peak_key] = current_price
                    elif current_price <= peak * (1 - deviation):
                        trigger_close_trade = True
                else:
                    if current_price < peak:
                        self.trailing_state[peak_key] = current_price
                    elif current_price >= peak * (1 + deviation):
                        trigger_close_trade = True

                if trigger_close_trade:
                    self.log("trailing_tp_triggered", account_name=self.accounts[idx]['info'].get('name'), is_key=True, symbol=symbol, price=current_price)
                    trade['initial_filled'] = False
                    trade['tp_trailing_active'] = False
                    if peak_key in self.trailing_state: del self.trailing_state[peak_key]
                    to_close.append(trade_id)

        for tid in to_close:
            self.close_position(self.accounts[idx]['info'].get('name'), symbol, trade_id=tid)

    def start_add_trade(self, idx, symbol, settings=None):
        """Initiates a new independent 'Add Trade' for a symbol."""
        acc_name = self.accounts[idx]['info'].get('name') if idx in self.accounts else f"Account {idx+1}"
        self.log("add_trade_initiated", account_name=acc_name, is_key=True, symbol=symbol)
        
        # Generate a unique trade ID for manual entry
        import uuid
        trade_id = f"man-{str(uuid.uuid4())[:8]}"
        
        # Trigger entry logic with this specific trade_id
        # Note: _check_and_place_initial_entry will use strategy settings
        self._check_and_place_initial_entry(idx, symbol, trade_id=trade_id)

    def refresh_data(self):
        """Forces an immediate refresh of all accounts and emits data."""
        for i in range(len(self.config.get('api_accounts', []))):
            self._update_account_metrics(i, force=True)
        self._emit_account_update()

    def get_status(self):
        return {
            'running': self.is_running,
            'accounts_count': len(self.accounts),
            'total_balance': sum(list(self.account_balances.values()))
        }

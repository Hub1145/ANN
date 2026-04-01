import time
import logging
import threading
import asyncio
from binance.streams import ThreadedWebsocketManager

class MarketHandler:
    def __init__(self, engine):
        self.engine = engine
        self.shared_market_data = {} # symbol -> { 'price': float, 'last_update': float, 'info': info }
        self.max_leverages = {} # symbol -> max_leverage
        self.exchange_info = {} # symbol -> { 'price_precision': int, 'qty_precision': int, 'tick_size': float, 'step_size': float }
        self.market_twm = None
        self.twm_lock = threading.Lock()
        self.market_data_lock = threading.Lock()

    def fetch_exchange_info(self):
        """Fetches and caches futures exchange information (precision, filters)."""
        try:
            client = self.engine.market_client
            if not client:
                return

            info = client.futures_exchange_info()
            for symbol_data in info.get('symbols', []):
                symbol = symbol_data['symbol']

                filters = {f['filterType']: f for f in symbol_data.get('filters', [])}

                price_filter = filters.get('PRICE_FILTER', {})
                lot_size = filters.get('LOT_SIZE', {})
                min_notional_filter = filters.get('MIN_NOTIONAL', {})

                self.exchange_info[symbol] = {
                    'price_precision': int(symbol_data.get('pricePrecision', 8)),
                    'qty_precision': int(symbol_data.get('quantityPrecision', 8)),
                    'tick_size': float(price_filter.get('tickSize', 0.00000001)),
                    'step_size': float(lot_size.get('stepSize', 0.00000001)),
                    'min_notional': float(min_notional_filter.get('notional') or min_notional_filter.get('minNotional') or 5.0)
                }
            logging.info(f"Fetched exchange info for {len(self.exchange_info)} symbols.")
        except Exception as e:
            logging.error(f"Failed to fetch exchange info: {e}")

    def initialize_market_ws(self):
        """Initializes WebSocket for market data (prices)."""
        with self.twm_lock:
            try:
                if self.market_twm:
                    self._stop_twm(self.market_twm)
                    self.market_twm = None

                m_client = self.engine.market_client
                if m_client is None:
                    logging.error("CRITICAL: Market client is unavailable. Skipping market WebSocket initialization.")
                    return

                try:
                    m_client.get_server_time()
                except Exception as e:
                    if "restricted location" in str(e).lower():
                        logging.error("CRITICAL: Restricted location detected. Skipping market WebSocket initialization.")
                        return

                new_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(new_loop)

                testnet = self.engine.config_handler.config.get('is_demo', True)
                self.market_twm = ThreadedWebsocketManager(testnet=testnet)
                self.market_twm._loop = new_loop

                try:
                    self.market_twm.start()
                    time.sleep(0.1)
                except Exception as e:
                    logging.error(f"Failed to start market WebSocket thread: {e}")
                    self.market_twm = None
                    return

                symbols = self.engine.config_handler.config.get('symbols', [])
                if symbols:
                    for symbol in symbols:
                        try:
                            self.market_twm.start_futures_ticker_socket(
                                callback=self._handle_market_ticker,
                                symbol=symbol
                            )
                            time.sleep(0.2)
                        except Exception as e:
                            logging.debug(f"Failed to start stream for {symbol}: {e}")
                    logging.info(f"Started market WebSocket for {len(symbols)} symbols")
            except Exception as e:
                logging.error(f"Failed to initialize market WebSocket: {e}")

    def _stop_twm(self, twm):
        if not twm: return
        try:
            # Wrap stop in a way that handles library-level crashes
            threading.Thread(target=twm.stop, daemon=True).start()
            twm.join(timeout=0.2)
        except: pass

    def _handle_market_ticker(self, msg):
        """Callback for market ticker WebSocket."""
        try:
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

    def get_price(self, symbol):
        with self.market_data_lock:
            return float(self.shared_market_data.get(symbol, {}).get('price') or 0)

    def get_market_data(self, symbol):
        with self.market_data_lock:
            return self.shared_market_data.get(symbol, {}).copy()

    def get_all_prices(self):
        with self.market_data_lock:
            price_map = {}
            for s, data in self.shared_market_data.items():
                price_map[s] = {
                    'bid': data.get('bid', data['price']),
                    'ask': data.get('ask', data['price']),
                    'last': data['price']
                }
            return price_map

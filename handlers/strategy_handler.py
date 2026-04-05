import time
import logging
import threading
from binance.client import Client
from binance.exceptions import BinanceAPIException

class StrategyHandler:
    def __init__(self, engine):
        self.engine = engine
        self.symbol_threads = {} # (account_index, symbol) -> Thread
        self.tp_setup_locks = {} # (account_index, symbol, trade_id) -> Lock

    def start_symbol_thread(self, idx, symbol):
        key = (int(idx), symbol)
        with self.engine.data_lock:
            if key in self.symbol_threads and not self.symbol_threads[key].is_alive():
                del self.symbol_threads[key]
            if key not in self.symbol_threads:
                t = threading.Thread(target=self._symbol_logic_worker, args=(int(idx), symbol), daemon=True)
                self.symbol_threads[key] = t
                t.start()
                acc = self.engine.accounts.get(int(idx))
                acc_name = acc.get('info', {}).get('name') if acc else f"Account {int(idx)+1}"
                self.engine.log("started_thread", account_name=acc_name, is_key=True, symbol=symbol)

    def _symbol_logic_worker(self, idx, symbol):
        idx = int(idx)
        try:
            if idx not in self.engine.accounts: return
            current_client = self.engine.accounts[idx].get('client')
            self.engine.setup_strategy_for_account(idx, symbol)

            while self.engine.is_running and not self.engine.stop_event.is_set():
                if idx not in self.engine.accounts or self.engine.accounts[idx].get('client') != current_client:
                    break
                try:
                    if symbol not in self.engine.config_handler.config.get('symbols', []):
                        self.engine.log("stopping_thread", is_key=True, symbol=symbol)
                        break

                    self.check_and_place_initial_entry(idx, symbol, trade_id='manual')
                    self.trailing_tp_logic(idx, symbol)
                    self.tp_market_logic(idx, symbol)
                    self.stop_loss_logic(idx, symbol)
                    self.trailing_buy_logic(idx, symbol)
                    self.conditional_logic(idx, symbol)
                    time.sleep(1)
                except Exception as e:
                    logging.error(f"Symbol logic worker error ({symbol}): {e}")
                    time.sleep(5)
        finally:
            with self.engine.data_lock:
                key = (idx, symbol)
                if self.symbol_threads.get(key) == threading.current_thread():
                    del self.symbol_threads[key]

    def check_and_place_initial_entry(self, idx, symbol, trade_id='manual', override_strategy=None):
        idx = int(idx)
        strategy = override_strategy or self.engine.config_handler.get_strategy(symbol)
        price = self.engine.market_handler.get_price(symbol)
        if price <= 0: return

        use_existing = strategy.get('use_existing', False) or strategy.get('use_existing_assets', False)

        with self.engine.data_lock:
            if (idx, symbol) not in self.engine.grid_state: self.engine.grid_state[(idx, symbol)] = []

            pos_dict = self.engine.account_handler.open_positions.get(idx, {})
            p_info = pos_dict.get(symbol)
            has_pos = p_info is not None

            trades = self.engine.grid_state.get((idx, symbol), [])
            if any(t['trade_id'] == trade_id for t in trades): return

            if use_existing and has_pos:
                # Attach to existing position
                if (idx, symbol) not in self.engine.grid_state: self.engine.grid_state[(idx, symbol)] = []

                # Check if we already have a trade for this trade_id
                if not any(t['trade_id'] == trade_id for t in self.engine.grid_state[(idx, symbol)]):
                    actual_qty = abs(float(p_info.get('amount', 0)))
                    actual_price = float(p_info.get('entryPrice', price))

                    direction = strategy.get('direction', 'LONG')
                    self.engine.grid_state[(idx, symbol)].append({
                        'trade_id': trade_id, 'initial_filled': True, 'initial_order_id': 'existing',
                        'quantity': actual_qty, 'avg_entry_price': actual_price, 'direction': direction,
                        'strategy': strategy, 'levels': {}
                    })

                    # Immediate TP/SL
                    self.setup_sl_logic(idx, symbol, actual_price, trade_id, override_strategy=strategy)
                    if strategy.get('tp_enabled', True):
                        self.setup_tp_targets_logic(idx, symbol, actual_price, strategy.get('tp_targets', []), actual_qty, direction_override=direction, trailing_tp_enabled=strategy.get('trailing_tp_enabled', False), trade_id=trade_id, override_strategy=strategy, force_reset=True)
                return

            if strategy.get('entry_type') == 'MARKET':
                self._execute_market_entry(idx, symbol, trade_id, override_strategy=strategy)
            elif strategy.get('entry_type') == 'LIMIT':
                limit_p = float(strategy.get('entry_price', price))
                self._execute_limit_entry(idx, symbol, limit_p, trade_id, override_strategy=strategy)

    def _execute_market_entry(self, idx, symbol, trade_id, override_strategy=None):
        idx = int(idx)
        strategy = override_strategy or self.engine.config_handler.get_strategy(symbol)
        price = self.engine.market_handler.get_price(symbol)
        if price <= 0: return
        qty = (float(strategy.get('trade_amount_usdc', 10)) * int(strategy.get('leverage', 20))) / price

        client = self.engine.accounts[idx]['client']
        direction = strategy.get('direction', 'LONG')
        side = 'BUY' if direction == 'LONG' else 'SELL'

        self.engine.log("placing_market_initial", account_name=self.engine.accounts[idx]['info'].get('name'), is_key=True, direction=direction)
        order = self.engine.safe_api_call(client.futures_create_order,
            symbol=symbol, side=side, type=Client.FUTURE_ORDER_TYPE_MARKET, quantity=self.engine.order_handler.format_quantity(symbol, qty))

        with self.engine.data_lock:
            if (idx, symbol) not in self.engine.grid_state: self.engine.grid_state[(idx, symbol)] = []
            oid = order['orderId']
            actual_qty = float(order.get('cumQty', qty))
            actual_price = float(order.get('avgPrice', price)) or price

            self.engine.grid_state[(idx, symbol)].append({
                'trade_id': trade_id, 'initial_filled': True, 'initial_order_id': oid,
                'initial_orders': {oid: {'qty': actual_qty, 'price': actual_price, 'filled': True}},
                'quantity': actual_qty, 'avg_entry_price': actual_price, 'direction': direction,
                'strategy': strategy, 'levels': {}
            })

            # Immediate TP/SL
            self.setup_sl_logic(idx, symbol, actual_price, trade_id, override_strategy=strategy)
            if strategy.get('tp_enabled', True):
                self.setup_tp_targets_logic(idx, symbol, actual_price, strategy.get('tp_targets', []), actual_qty, direction_override=direction, trailing_tp_enabled=strategy.get('trailing_tp_enabled', False), trade_id=trade_id, override_strategy=strategy, force_reset=True)

    def _execute_limit_entry(self, idx, symbol, price, trade_id, override_strategy=None):
        idx = int(idx)
        strategy = override_strategy or self.engine.config_handler.get_strategy(symbol)
        # Use entry_price from strategy if available
        price = float(strategy.get('entry_price', price))
        qty = (float(strategy.get('trade_amount_usdc', 10)) * int(strategy.get('leverage', 20))) / price

        direction = strategy.get('direction', 'LONG')
        side = 'BUY' if direction == 'LONG' else 'SELL'
        order_id = self.engine.order_handler.place_limit_order(idx, symbol, side, qty, price, client_id=f"entry-{trade_id}")

        if order_id:
            self.engine.log("placing_limit_order", account_name=self.engine.accounts[idx]['info'].get('name'), is_key=True, symbol=symbol, side=side, qty=self.engine.order_handler.format_quantity(symbol, qty), price=self.engine.order_handler.format_price(symbol, price))
            with self.engine.data_lock:
                if (idx, symbol) not in self.engine.grid_state: self.engine.grid_state[(idx, symbol)] = []
                # Register the trade in grid_state
                self.engine.grid_state[(idx, symbol)].append({
                    'trade_id': trade_id, 'initial_filled': False, 'initial_order_id': order_id,
                    'quantity': qty, 'avg_entry_price': price, 'direction': direction,
                    'strategy': strategy, 'levels': {},
                    'pending_tp_grid': strategy.get('tp_enabled', True)
                })

                # Immediate SL placement (STOP_MARKET with closePosition=True works even without position)
                self.setup_sl_logic(idx, symbol, price, trade_id, override_strategy=strategy)

                # Take Profit placement is now deferred until the entry order fills.
                # This ensures we can use reduce_only=True safely.

    def trailing_tp_logic(self, idx, symbol):
        idx = int(idx)
        strategy = self.engine.config_handler.get_strategy(symbol)
        if not strategy.get('trailing_tp_enabled', False): return
        price = self.engine.market_handler.get_price(symbol)
        if price <= 0: return

        with self.engine.data_lock:
            trades = self.engine.grid_state.get((idx, symbol), [])
            for t in trades:
                if not t.get('initial_filled'): continue
                last_target_id = max(t['levels'].keys()) if t.get('levels') else None
                if not last_target_id: continue

                lvl = t['levels'][last_target_id]
                if not lvl.get('trailing_eligible') or lvl.get('filled'): continue

                direction = t.get('direction', strategy.get('direction', 'LONG'))
                activation_price = lvl['price']

                if not lvl.get('trailing_activated', False):
                    activated = (direction == 'LONG' and price >= activation_price) or (direction == 'SHORT' and price <= activation_price)
                    if activated:
                        lvl['trailing_activated'] = True
                        lvl['best_price'] = price
                        self.engine.log("trailing_tp_activated", account_name=self.engine.accounts[idx]['info'].get('name'), is_key=True, symbol=symbol, price=price)
                else:
                    if direction == 'LONG':
                        if price > lvl['best_price']: lvl['best_price'] = price
                    else:
                        if price < lvl['best_price']: lvl['best_price'] = price

                    callback_pct = float(strategy.get('trailing_deviation', 0.5)) / 100.0
                    triggered = False
                    if direction == 'LONG':
                        if price <= lvl['best_price'] * (1 - callback_pct): triggered = True
                    else:
                        if price >= lvl['best_price'] * (1 + callback_pct): triggered = True

                    if triggered:
                        self.engine.log("trailing_tp_triggered", account_name=self.engine.accounts[idx]['info'].get('name'), is_key=True, symbol=symbol, price=price)
                        self.engine.close_position(idx, symbol, trade_id=t['trade_id'], order_type='MARKET')
                        lvl['filled'] = True

    def tp_market_logic(self, idx, symbol):
        idx = int(idx)
        strategy = self.engine.config_handler.get_strategy(symbol)
        price = self.engine.market_handler.get_price(symbol)
        if price <= 0: return
        to_exec = []
        with self.engine.data_lock:
            for t in self.engine.grid_state.get((idx, symbol), []):
                # Use direction from trade state for consistent logic
                direction = t.get('direction', strategy.get('direction', 'LONG'))

                # Proactive Limit TP Placement / Check Initialization
                recent_tp_setup = time.time() - t.get('last_tp_setup_attempt', 0) < 10

                # Check if TP levels are initialized or any order is missing
                is_tp_enabled = strategy.get('tp_enabled', True)
                levels_missing = (not t.get('levels')) and is_tp_enabled

                any_tp_missing = levels_missing or any(not lvl.get('tp_order_id') and not lvl.get('filled') and not lvl.get('is_market') for lid, lvl in t.get('levels', {}).items())

                # We only place TP if the initial entry is filled (to ensure position exists for reduceOnly)
                if any_tp_missing and not recent_tp_setup and (t.get('initial_filled') or t.get('initial_order_id') == 'existing'):
                    t['last_tp_setup_attempt'] = time.time()
                    self.setup_tp_targets_logic(idx, symbol, t['avg_entry_price'], strategy.get('tp_targets', []), t['quantity'], direction_override=direction, trailing_tp_enabled=strategy.get('trailing_tp_enabled', False), trade_id=t['trade_id'], override_strategy=strategy, force_reset=levels_missing)

                # Market TP check (runs even if placement in progress)
                for lid, lvl in t.get('levels', {}).items():
                    if not lvl.get('tp_order_id') and not lvl.get('filled') and lvl.get('is_market'):
                        triggered = (direction == 'LONG' and price >= lvl['price']) or (direction == 'SHORT' and price <= lvl['price'])
                        if triggered: to_exec.append((t['trade_id'], lid, lvl['qty'], lvl['side']))

        for tid, lid, qty, side in to_exec:
            if self.engine.close_position(idx, symbol, trade_id=tid, order_type='MARKET'):
                with self.engine.data_lock:
                    s = next((t for t in self.engine.grid_state.get((idx, symbol), []) if t['trade_id'] == tid), None)
                    if s: s['levels'][lid]['filled'] = True

    def setup_sl_logic(self, idx, symbol, entry_price, trade_id, override_strategy=None):
        idx = int(idx)
        strategy = override_strategy or self.engine.config_handler.get_strategy(symbol)
        if not strategy.get('stop_loss_enabled'): return

        sl_price = float(strategy.get('stop_loss_price', 0))
        if sl_price <= 0: return

        # Place STOP_MARKET with closePosition=True
        client_id = f"trd-{trade_id}-sl"
        with self.engine.data_lock:
            trades = self.engine.grid_state.get((idx, symbol), [])
            t = next((tr for tr in trades if tr['trade_id'] == trade_id), None)
            if not t: return
            if t.get('sl_order_id'): return # Already placed

            # Determine side for SL using trade's direction
            direction = t.get('direction', strategy.get('direction', 'LONG'))
            side = 'SELL' if direction == 'LONG' else 'BUY'

        order_id = self.engine.order_handler.place_stop_order(idx, symbol, side, sl_price, client_id=client_id, close_position=True)
        if order_id:
            self.engine.log("sl_placement_summary", account_name=self.engine.accounts[idx]['info'].get('name'), is_key=True, symbol=symbol, price=self.engine.order_handler.format_price(symbol, sl_price))
            with self.engine.data_lock:
                t['sl_order_id'] = order_id

    def stop_loss_logic(self, idx, symbol):
        idx = int(idx)
        # Stop loss is handled by Binance STOP_MARKET orders.
        strategy = self.engine.config_handler.get_strategy(symbol)
        if not strategy.get('stop_loss_enabled'): return

        with self.engine.data_lock:
            for t in self.engine.grid_state.get((idx, symbol), []):
                # We skip re-placing if an order ID exists or we're waiting for entry to fill
                # (though STOP_MARKET works without position, we throttle retries)
                recent_sl_setup = time.time() - t.get('last_sl_setup_attempt', 0) < 10

                if not t.get('sl_order_id') and not recent_sl_setup:
                    t['last_sl_setup_attempt'] = time.time()
                    self.setup_sl_logic(idx, symbol, t['avg_entry_price'], t['trade_id'], override_strategy=strategy)

    def trailing_buy_logic(self, idx, symbol):
        idx = int(idx)
        strategy = self.engine.config_handler.get_strategy(symbol)
        if not strategy: return
        if not strategy.get('trailing_buy_enabled'): return
        price = self.engine.market_handler.get_price(symbol)
        if price <= 0: return

        with self.engine.data_lock:
            if len(self.engine.grid_state.get((idx, symbol), [])) > 0: return
            state = self.engine.trailing_state.get((idx, symbol), {})
            if not state:
                # Activation price for trailing buy is the entry_price
                act_p = float(strategy.get('entry_price', 0))
                direction = strategy.get('direction', 'LONG')
                # If entry_price is 0, activate immediately
                activated = (act_p == 0) or (direction == 'LONG' and price <= act_p) or (direction == 'SHORT' and price >= act_p)
                if activated:
                    self.engine.trailing_state[(idx, symbol)] = {'best_price': price, 'activated': True}
                    self.engine.log("trailing_buy_activated", account_name=self.engine.accounts[idx]['info'].get('name'), is_key=True, symbol=symbol, price=price)
            else:
                direction = strategy.get('direction', 'LONG')
                if direction == 'LONG':
                    if price < state['best_price']: state['best_price'] = price
                else:
                    if price > state['best_price']: state['best_price'] = price

                callback = float(strategy.get('trailing_buy_deviation', 1.0)) / 100.0
                triggered = (direction == 'LONG' and price >= state['best_price'] * (1 + callback)) or \
                            (direction == 'SHORT' and price <= state['best_price'] * (1 - callback))
                if triggered:
                    self.engine.log("trailing_buy_triggered", account_name=self.engine.accounts[idx]['info'].get('name'), is_key=True, symbol=symbol, price=price)
                    self._execute_market_entry(idx, symbol, trade_id=f"trail-{int(time.time())}")
                    del self.engine.trailing_state[(idx, symbol)]

    def conditional_logic(self, idx, symbol):
        idx = int(idx)
        strategy = self.engine.config_handler.get_strategy(symbol)
        if not strategy: return
        if strategy.get('entry_type') not in ['CONDITIONAL', 'COND_LIMIT', 'COND_MARKET']: return
        price = self.engine.market_handler.get_price(symbol)
        if price <= 0: return
        with self.engine.data_lock:
            # Prevent multiple entries for the same conditional order
            if len(self.engine.grid_state.get((idx, symbol), [])) > 0: return

            # Use entry_price for conditional trigger
            cond_p = float(strategy.get('entry_price', 0))
            if cond_p <= 0: return # Skip if trigger price is invalid

            direction = strategy.get('direction', 'LONG')

            # Logic check: LONG triggers when price >= cond_p, SHORT triggers when price <= cond_p
            triggered = (direction == 'LONG' and price >= cond_p) or (direction == 'SHORT' and price <= cond_p)
            if triggered:
                if strategy.get('entry_type') in ['COND_MARKET', 'CONDITIONAL']:
                    self._execute_market_entry(idx, symbol, trade_id=f"cond-{int(time.time())}")
                else:
                    limit_p = float(strategy.get('entry_price', price))
                    self._execute_limit_entry(idx, symbol, limit_p, trade_id=f"cond-{int(time.time())}")

    def setup_tp_targets_logic(self, idx, symbol, entry_price, targets, total_qty, direction_override=None, trailing_tp_enabled=False, trade_id=None, override_strategy=None, force_reset=False):
        idx = int(idx)
        formatted_qty = float(self.engine.order_handler.format_quantity(symbol, total_qty))
        if formatted_qty <= 0:
            return

        lock_key = (idx, symbol, trade_id)
        with self.engine.data_lock:
            if lock_key not in self.tp_setup_locks: self.tp_setup_locks[lock_key] = threading.Lock()
            tp_lock = self.tp_setup_locks[lock_key]

        if not tp_lock.acquire(blocking=False):
            return

        try:
            with self.engine.data_lock:
                trades = self.engine.grid_state.get((idx, symbol), [])
                state = next((t for t in trades if t.get('trade_id') == trade_id), None)
                if not state: return

                acc_name = self.engine.accounts[idx]['info'].get('name') if idx in self.engine.accounts else str(idx)
                strategy = override_strategy or self.engine.config_handler.get_strategy(symbol)
                consolidated_tp = strategy.get('consolidated_tp', False)
                tp_market_mode = strategy.get('tp_market_mode', False)
                direction = direction_override or state.get('direction', 'LONG')
                client = self.engine.accounts[idx]['client']

                # If we have levels already and not forcing reset, we skip the wipe/re-init
                if state.get('levels') and not force_reset:
                    pass
                else:
                    # Cancel existing if resetting
                    if state.get('levels'):
                        for lvl in state['levels'].values():
                            o_id = lvl.get('tp_order_id')
                            if o_id:
                                try: self.engine.safe_api_call(client.futures_cancel_order, symbol=symbol, orderId=o_id)
                                except: pass

                    old_ctp_id = state.get('consolidated_tp_id')
                    if old_ctp_id:
                        try: self.engine.safe_api_call(client.futures_cancel_order, symbol=symbol, orderId=old_ctp_id)
                        except: pass
                        state['consolidated_tp_id'] = None

                    # Initialize levels
                    state['levels'] = {}
                    if consolidated_tp:
                        target_vol_total = sum(float(t.get('volume') or 0) for t in targets) / 100.0
                        qty = total_qty * target_vol_total
                        first_target = targets[0] if targets else {'percent': 0.6}
                        pct = float(first_target.get('percent') or 0)
                        tp_price = entry_price * (1 + pct / 100.0) if direction == 'LONG' else entry_price * (1 - pct / 100.0)
                        side = Client.SIDE_SELL if direction == 'LONG' else Client.SIDE_BUY
                        state['levels'][1] = {
                            'tp_order_id': None, 'price': tp_price, 'percent': pct,
                            'qty': float(self.engine.order_handler.format_quantity(symbol, qty)),
                            'side': side, 'is_market': tp_market_mode, 'trailing_eligible': False, 'filled': False, 'is_consolidated': True
                        }
                    else:
                        for i, target in enumerate(targets, 1):
                            pct = float(target.get('percent') or 0)
                            qty = total_qty * (float(target.get('volume') or 0) / 100.0)
                            side = Client.SIDE_SELL if direction == 'LONG' else Client.SIDE_BUY
                            tp_price = entry_price * (1 + pct / 100.0) if direction == 'LONG' else entry_price * (1 - pct / 100.0)
                            trailing_eligible = (i == len(targets)) and trailing_tp_enabled
                            state['levels'][i] = {
                                'tp_order_id': None, 'price': tp_price, 'percent': pct,
                                'qty': float(self.engine.order_handler.format_quantity(symbol, qty)),
                                'side': side, 'is_market': tp_market_mode, 'trailing_eligible': trailing_eligible, 'filled': False
                            }

                # Build orders_to_place from missing orders
                orders_to_place = []
                for lid, lvl in state['levels'].items():
                    if not lvl.get('tp_order_id') and not lvl.get('filled') and not lvl.get('is_market') and not lvl.get('trailing_eligible'):
                        orders_to_place.append({'level': lid, 'side': lvl['side'], 'qty': lvl['qty'], 'price': lvl['price'], 'is_consolidated': lvl.get('is_consolidated', False)})

            newly_placed_count = 0
            for o in orders_to_place:
                order_id = None
                client_id = f"trd-{trade_id}-tp-{o['level']}"
                with self.engine.data_lock:
                    open_orders = self.engine.account_handler.open_orders.get(idx, [])
                    recent_ids = self.engine.order_handler.recent_client_ids.get((idx, symbol), {})

                existing = next((oo for oo in open_orders if oo.get('symbol') == symbol and oo.get('clientOrderId') == client_id), None)
                if existing:
                    order_id = existing['orderId']
                elif client_id in recent_ids and (time.time() - recent_ids[client_id] < 10):
                    # Keep track of the order ID if it was recently placed but not in open_orders yet
                    continue
                else:
                    # Switched back to reduce_only=True since we now wait for the entry fill.
                    order_id = self.engine.order_handler.place_limit_order(idx, symbol, o['side'], o['qty'], o['price'], client_id=client_id, reduce_only=True)
                    if order_id:
                        newly_placed_count += 1

                if order_id:
                    with self.engine.data_lock:
                        s = next((t for t in self.engine.grid_state.get((idx, symbol), []) if t.get('trade_id') == trade_id), None)
                        if s and o['level'] in s['levels']:
                            s['levels'][o['level']]['tp_order_id'] = order_id
                            if o.get('is_consolidated'): s['consolidated_tp_id'] = order_id

            if newly_placed_count > 0:
                if consolidated_tp:
                    self.engine.log("tp_aggregated_summary", account_name=acc_name, is_key=True, symbol=symbol, qty=self.engine.order_handler.format_quantity(symbol, total_qty), price=self.engine.order_handler.format_price(symbol, state['levels'][1]['price']))
                else:
                    self.engine.log("tp_placement_summary", account_name=acc_name, is_key=True, symbol=symbol, count=newly_placed_count, total_qty=self.engine.order_handler.format_quantity(symbol, total_qty))
        finally:
            tp_lock.release()

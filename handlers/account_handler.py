import time
import logging
from binance.exceptions import BinanceAPIException
from binance.client import Client

class AccountHandler:
    def __init__(self, engine):
        self.engine = engine
        self.account_balances = {} # account_index -> balance
        self.account_errors = {} # account_index -> error message
        self.account_last_update = {} # account_index -> timestamp
        self.open_positions = {} # account_index -> { symbol: position_dict }
        self.open_orders = {} # account_index -> [ order_dict ]

    def update_account_metrics(self, idx, force=False):
        acc = self.engine.accounts.get(idx)
        if not acc:
            acc = self.engine.bg_clients.get(idx)

        if not acc:
            api_accounts = self.engine.config_handler.config.get('api_accounts', [])
            if 0 <= idx < len(api_accounts) and api_accounts[idx].get('enabled', True):
                self.account_errors[idx] = "Initializing..."
                self.emit_account_update()
            return
        client = acc['client']

        try:
            balance_throttle = 10 if idx in self.engine.accounts else 30
            if force or time.time() - acc.get('last_balance_update', 0) > balance_throttle:
                acc['last_balance_update'] = time.time()

                total_wallet_balance = 0.0
                stable_assets = ['USDT', 'USDC']

                if not hasattr(self, 'per_asset_balances'): self.per_asset_balances = {}
                if idx not in self.per_asset_balances: self.per_asset_balances[idx] = {}

                if idx in self.engine.accounts:
                    # For active accounts, use futures_account() and strictly use USDC
                    account_info = self.engine.safe_api_call(client.futures_account)
                    usdc_bal = 0.0
                    for asset_data in account_info.get('assets', []):
                        a_name = asset_data['asset']
                        a_bal = float(asset_data['walletBalance'])
                        self.per_asset_balances[idx][a_name] = a_bal
                        if a_name == 'USDC':
                            usdc_bal = a_bal
                    total_wallet_balance = usdc_bal
                else:
                    # For background accounts, use futures_account_balance()
                    balances = self.engine.safe_api_call(client.futures_account_balance)
                    if balances:
                        usdc_bal = 0.0
                        for b in balances:
                            a_name = b.get('asset')
                            a_bal = float(b.get('balance') or 0)
                            self.per_asset_balances[idx][a_name] = a_bal
                            if a_name == 'USDC':
                                usdc_bal = a_bal
                        total_wallet_balance = usdc_bal

                with self.engine.data_lock:
                    self.account_balances[idx] = total_wallet_balance
                    self.account_last_update[idx] = time.time()
                    if idx in self.account_errors and "Restricted" not in self.account_errors[idx]:
                        del self.account_errors[idx]

                if total_wallet_balance == 0 and idx in self.engine.accounts:
                    log_key = f"zero_balance_{idx}"
                    if time.time() - self.engine.last_log_times.get(log_key, 0) > 300:
                        acc_name = acc.get('name') or acc.get('info', {}).get('name') or f"Account {idx+1}"
                        self.engine.log(f"Balance for {acc_name} is 0.00. Please ensure you have funds (USDC) in your Futures wallet.", level='warning', account_name=acc_name)
                        self.engine.last_log_times[log_key] = time.time()

            # Positions (Weight 5) - Update every 10s unless forced
            pos_throttle = 10 if idx in self.engine.accounts else 30
            if force or time.time() - acc.get('last_pos_update', 0) > pos_throttle:
                acc['last_pos_update'] = time.time()

                if idx in self.engine.accounts:
                    account_info = self.engine.safe_api_call(client.futures_account)
                    raw_positions = account_info.get('positions', [])
                    pos_key = 'positionAmt'
                    price_key = 'entryPrice'
                    up_key = 'unrealizedProfit'
                else:
                    raw_positions = self.engine.safe_api_call(client.futures_position_information)
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
                with self.engine.data_lock:
                    self.open_positions[idx] = new_positions

            # Open Orders (Weight 1) - Update every 10s
            orders_throttle = 10 if idx in self.engine.accounts else 30
            if force or time.time() - acc.get('last_orders_update', 0) > orders_throttle:
                acc['last_orders_update'] = time.time()
                open_orders = self.engine.safe_api_call(client.futures_get_open_orders)
                with self.engine.data_lock:
                    self.open_orders[idx] = open_orders

            self.emit_account_update()

        except BinanceAPIException as e:
            if e.code == -2015:
                self.account_errors[idx] = "Invalid API Keys or insufficient permissions. Check if IP " + str(self.engine.server_ip) + " is whitelisted."
                log_key = f"invalid_api_{idx}"
                now = time.time()
                if now - self.engine.last_log_times.get(log_key, 0) > 300:
                    self.engine.log(f"Invalid API Keys or insufficient permissions for {acc['info'].get('name')}. Ensure Futures are enabled and IP {self.engine.server_ip} is whitelisted.", level='error', account_name=acc['info'].get('name'))
                    self.engine.last_log_times[log_key] = now
            else:
                logging.error(f"Error updating metrics for account {idx}: {e}")
                self.account_errors[idx] = str(e)
        except Exception as e:
            logging.error(f"Error updating metrics for account {idx}: {e}")
            self.account_errors[idx] = str(e)

    def emit_account_update(self):
        with self.engine.data_lock:
            total_balance = sum(list(self.account_balances.values()))
            total_pnl = 0.0

            all_positions = []
            external_positions = []

            all_idxs = set(list(self.account_balances.keys()) + list(self.open_positions.keys()))
            api_accounts = self.engine.config_handler.config.get('api_accounts', [])

            for idx in all_idxs:
                try: num_idx = int(idx)
                except: num_idx = -1

                pos_dict = self.open_positions.get(idx, {})
                acc_name = api_accounts[num_idx].get('name', f"Account {num_idx+1}") if 0 <= num_idx < len(api_accounts) else str(idx)

                for symbol, p in pos_dict.items():
                    p_copy = p.copy()
                    p_copy['account'] = acc_name
                    p_copy['account_idx'] = idx
                    total_pnl += float(p_copy.get('unrealizedProfit') or 0)

                    trades = self.engine.grid_state.get((idx, symbol), [])
                    p_copy['trades'] = []

                    # Strictly track what's actually filled on exchange
                    filled_tracked_qty = 0.0
                    filled_tracked_notional = 0.0

                    # Also track pending to avoid "Double Row" UI glitch, but don't mix with exchange reality
                    pending_tracked_qty = 0.0

                    current_price = self.engine.market_handler.get_price(symbol) or float(p_copy.get('entryPrice') or 0)

                    for t in trades:
                        t_qty = self.engine.get_trade_quantity(t)
                        is_filled = t.get('initial_filled', False)

                        trade_pnl = 0.0
                        if is_filled and t_qty > 0:
                            avg_e = float(t.get('avg_entry_price') or 0)
                            strategy = self.engine.config_handler.get_strategy(symbol)
                            direction = strategy.get('direction', 'LONG')
                            multiplier = 1 if direction == 'LONG' else -1
                            trade_pnl = (current_price - avg_e) * t_qty * multiplier

                            filled_tracked_qty += t_qty
                            filled_tracked_notional += (t_qty * avg_e)
                        else:
                            pending_tracked_qty += float(t.get('quantity') or 0)

                        trade_levels = []
                        for lid, lvl in t.get('levels', {}).items():
                            trade_levels.append({
                                'level': lid,
                                'price': lvl.get('price'),
                                'qty': lvl.get('qty'),
                                'filled': lvl.get('filled'),
                                'order_id': lvl.get('tp_order_id'),
                                'is_market': lvl.get('is_market'),
                                'is_trailing': lvl.get('trailing_eligible')
                            })

                        p_copy['trades'].append({
                            'trade_id': t.get('trade_id'),
                            'entry_price': t.get('avg_entry_price', 0),
                            'amount': t_qty if is_filled else float(t.get('quantity') or 0),
                            'filled': is_filled,
                            'pnl': trade_pnl,
                            'levels': trade_levels,
                            'sl_order_id': t.get('sl_order_id')
                        })

                    total_qty_pa = abs(float(p_copy.get('amount') or 0))
                    # Difference between exchange total and what the bot knows is filled
                    diff_qty = total_qty_pa - filled_tracked_qty

                    # UI Glitch Fix: If diff matches pending exactly, it's likely just the WebSocket delay.
                    # We only show "External" if diff is NOT explained by pending trades.
                    unaccounted_qty = diff_qty - pending_tracked_qty
                    # Floating point precision check on unaccounted quantity
                    if unaccounted_qty > 1e-4 and (total_qty_pa > 0 and (unaccounted_qty / total_qty_pa) > 1e-5):
                        total_avg = float(p_copy.get('entryPrice') or 0)
                        total_notional_val = total_avg * total_qty_pa
                        ext_notional = total_notional_val - filled_tracked_notional
                        ext_avg = ext_notional / diff_qty if diff_qty > 0 else total_avg

                        strategy = self.engine.config_handler.get_strategy(symbol)
                        direction = strategy.get('direction', 'LONG')
                        multiplier = 1 if direction == 'LONG' else -1
                        ext_pnl = (current_price - ext_avg) * diff_qty * multiplier

                        p_copy['trades'].append({
                            'trade_id': 'External',
                            'entry_price': ext_avg,
                            'amount': unaccounted_qty,
                            'filled': True,
                            'pnl': ext_pnl
                        })

                    p_copy['is_external'] = (len(trades) == 0)
                    all_positions.append(p_copy)
                    if p_copy['is_external']: external_positions.append(p_copy)

            # Open Orders
            payload_orders = []
            for o_idx, orders in self.open_orders.items():
                try: acc_id = int(o_idx)
                except: acc_id = -1
                acc_n = api_accounts[acc_id].get('name', f"Account {acc_id+1}") if 0 <= acc_id < len(api_accounts) else f"Account {o_idx}"

                for o in orders:
                    payload_orders.append({
                        'account': acc_n,
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
                'running': self.engine.is_running,
                'accounts': [
                    {
                        'name': api_accounts[i].get('name', f"Account {i+1}") if i < len(api_accounts) else f"Account {i+1}",
                        'balance': self.account_balances.get(i, 0.0),
                        'active': i in self.engine.accounts,
                        'has_client': i in self.engine.bg_clients,
                        'error': self.account_errors.get(i),
                        'last_update': self.account_last_update.get(i, 0)
                    } for i in range(len(api_accounts))
                ]
            }
        self.engine.emit('account_update', payload)

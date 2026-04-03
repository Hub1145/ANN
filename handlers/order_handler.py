import time
import logging
from decimal import Decimal, ROUND_FLOOR, ROUND_HALF_UP
from binance.client import Client
from binance.exceptions import BinanceAPIException

class OrderHandler:
    def __init__(self, engine):
        self.engine = engine
        self.recent_client_ids = {} # (account_index, symbol) -> { client_id: timestamp }

    def place_stop_order(self, idx, symbol, side, stop_price, qty=None, client_id=None, close_position=True):
        target_client = self.engine.accounts[idx]['client'] if idx in self.engine.accounts else self.engine.bg_clients.get(idx, {}).get('client')
        if not target_client: return None

        acc_name = self.engine.accounts[idx]['info'].get('name') if idx in self.engine.accounts else self.engine.bg_clients.get(idx, {}).get('name', str(idx))

        try:
            params = {
                'symbol': symbol,
                'side': side,
                'type': Client.FUTURE_ORDER_TYPE_STOP_MARKET,
                'stopPrice': self.format_price(symbol, stop_price),
                'closePosition': 'true' if close_position else 'false'
            }
            if not close_position and qty:
                params['quantity'] = self.format_quantity(symbol, qty)
            if client_id: params['newClientOrderId'] = client_id

            # Removed redundant logging of placement to prevent spam. StrategyHandler handles specific feedback.
            order = self.engine.safe_api_call(target_client.futures_create_order, **params)
            return order['orderId']
        except BinanceAPIException as e:
            if e.code == -2022:
                if client_id: self.engine.trailing_state[(idx, symbol, client_id)] = 'REJECTED_REDUCE_ONLY'
                log_key = f"reduce_only_pending_{idx}_{symbol}"
                if time.time() - self.engine.last_log_times.get(log_key, 0) > 60:
                    self.engine.log("reduce_only_pending", level='info', account_name=acc_name, is_key=True, symbol=symbol)
                    self.engine.last_log_times[log_key] = time.time()
            else:
                self.engine.log("stop_order_failed", level='error', account_name=acc_name, is_key=True, error=str(e))
            return None
        except Exception as e:
            self.engine.log("stop_order_failed", level='error', account_name=acc_name, is_key=True, error=str(e))
            return None

    def place_limit_order(self, idx, symbol, side, qty, price, client_id=None, reduce_only=False):
        target_client = self.engine.accounts[idx]['client'] if idx in self.engine.accounts else self.engine.bg_clients.get(idx, {}).get('client')
        if not target_client: return None

        acc_name = self.engine.accounts[idx]['info'].get('name') if idx in self.engine.accounts else self.engine.bg_clients.get(idx, {}).get('name', str(idx))
        strategy = self.engine.config_handler.get_strategy(symbol)
        leverage = int(strategy.get('leverage') or 20)

        if not self.engine.check_balance_for_order(idx, qty, price, leverage=leverage, symbol=symbol):
            log_key = f"insufficient_balance_{idx}_{symbol}"
            if time.time() - self.engine.last_log_times.get(log_key, 0) > 60:
                self.engine.log("insufficient_balance", level='warning', account_name=acc_name, is_key=True, qty=qty, price=price)
                self.engine.last_log_times[log_key] = time.time()
            return None

        try:
            formatted_qty_str = self.format_quantity(symbol, qty)
            formatted_price_str = self.format_price(symbol, price)
            f_qty = float(formatted_qty_str)
            f_price = float(formatted_price_str)

            if f_qty <= 0:
                self.engine.log(f"Limit order skipped for {symbol}: Calculated quantity {qty} <= 0.", level='warning', account_name=acc_name)
                return None

            notional = f_qty * f_price
            ex_info = self.engine.market_handler.exchange_info.get(symbol, {})
            min_notional = ex_info.get('min_notional', 5.0)

            if notional < min_notional:
                self.engine.log("order_skipped_min_notional", level='warning', account_name=acc_name, is_key=True, symbol=symbol, notional=f"{notional:.2f}", min_notional=f"{min_notional:.2f}")
                return None

            # Removed redundant logging of placement to prevent spam. StrategyHandler handles specific feedback.
            params = {
                'symbol': symbol,
                'side': side,
                'type': Client.FUTURE_ORDER_TYPE_LIMIT,
                'timeInForce': Client.TIME_IN_FORCE_GTC,
                'quantity': formatted_qty_str,
                'price': formatted_price_str,
                'reduceOnly': 'true' if reduce_only else 'false'
            }
            if client_id: params['newClientOrderId'] = client_id

            if client_id:
                with self.engine.data_lock:
                    if (idx, symbol) not in self.recent_client_ids: self.recent_client_ids[(idx, symbol)] = {}
                    self.recent_client_ids[(idx, symbol)][client_id] = time.time()

            order = self.engine.safe_api_call(target_client.futures_create_order, **params)

            if client_id:
                wait_key = (idx, symbol, client_id)
                if wait_key in self.engine.trailing_state: del self.engine.trailing_state[wait_key]

            return order['orderId']
        except BinanceAPIException as e:
            if e.code in [-4016, -4025] or "Price out of range" in e.message:
                self.engine.log("limit_order_price_error", level='warning', account_name=acc_name, is_key=True, symbol=symbol, error=e.message)
            elif e.code == -2022:
                if client_id: self.engine.trailing_state[(idx, symbol, client_id)] = 'REJECTED_REDUCE_ONLY'
                log_key = f"reduce_only_pending_{idx}_{symbol}"
                if time.time() - self.engine.last_log_times.get(log_key, 0) > 60:
                    self.engine.log("reduce_only_pending", level='info', account_name=acc_name, is_key=True, symbol=symbol)
                    self.engine.last_log_times[log_key] = time.time()
            elif e.code == -4120:
                # Silently catch Algo Order API requirement; we prefer regular LIMIT for standard TP
                pass
            elif e.code == -1007:
                # Backend timeout - usually retryable but we log it as info to avoid level='error' panic
                self.engine.log("limit_order_timeout", level='info', account_name=acc_name, is_key=True)
            else:
                self.engine.log("limit_order_failed", level='error', account_name=acc_name, is_key=True, error=str(e))
            return None
        except Exception as e:
            self.engine.log("limit_order_failed", level='error', account_name=acc_name, is_key=True, error=str(e))
            return None

    def format_quantity(self, symbol, quantity):
        ex_info = self.engine.market_handler.exchange_info.get(symbol)
        if not ex_info:
            return f"{quantity:.8f}".rstrip('0').rstrip('.')

        step_size = str(ex_info['step_size'])
        step_d = Decimal(step_size).normalize()
        qty_d = Decimal(str(quantity))
        result = (qty_d / step_d).quantize(Decimal('1'), rounding=ROUND_FLOOR) * step_d
        precision = ex_info['qty_precision']
        return format(result, f'.{precision}f')

    def format_price(self, symbol, price):
        ex_info = self.engine.market_handler.exchange_info.get(symbol)
        if not ex_info:
            return f"{price:.8f}".rstrip('0').rstrip('.')

        tick_size = str(ex_info['tick_size'])
        tick_d = Decimal(tick_size).normalize()
        price_d = Decimal(str(price))
        result = (price_d / tick_d).quantize(Decimal('1'), rounding=ROUND_HALF_UP) * tick_d
        precision = ex_info['price_precision']
        return format(result, f'.{precision}f')

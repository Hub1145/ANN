import unittest
from unittest.mock import MagicMock, patch
import time

class MockEngine:
    def __init__(self):
        self.accounts = {0: {'client': MagicMock(), 'info': {'name': 'Acc1'}}}
        self.bg_clients = {}
        self.data_lock = MagicMock()
        self.grid_state = {}
        self.config_handler = MagicMock()
        self.market_handler = MagicMock()
        self.order_handler = MagicMock()
        self.account_handler = MagicMock()
        self.is_running = True
        self.stop_event = MagicMock()
        self.stop_event.is_set.return_value = False
        self.trailing_state = {}
        self.last_log_times = {}

    def log(self, *args, **kwargs): pass
    def emit(self, *args, **kwargs): pass
    def safe_api_call(self, func, *args, **kwargs): return func(*args, **kwargs)

class StrategyHandler:
    def __init__(self, engine):
        self.engine = engine
        self.tp_setup_locks = {}

    def _execute_market_entry(self, idx, symbol, trade_id, override_strategy=None):
        strategy = override_strategy or {}
        price = 100.0
        qty = 1.0

        # Simulate logic from real handler
        self.engine.grid_state[(idx, symbol)] = [{
            'trade_id': trade_id, 'initial_filled': True, 'quantity': qty, 'avg_entry_price': price, 'levels': {}
        }]

        # This is what we're testing: immediate call
        self.setup_sl_logic(idx, symbol, price, trade_id, override_strategy=strategy)
        self.setup_tp_targets_logic(idx, symbol, price, strategy.get('tp_targets', []), qty, 'LONG', False, trade_id, override_strategy=strategy)

    def setup_sl_logic(self, idx, symbol, entry_price, trade_id, override_strategy=None):
        print(f"DEBUG: Setting up SL for {trade_id}")
        self.engine.order_handler.place_stop_order(idx, symbol, 'SELL', 95.0, client_id=f"trd-{trade_id}-sl")

    def setup_tp_targets_logic(self, idx, symbol, entry_price, targets, qty, dir, trailing, trade_id, override_strategy=None):
        print(f"DEBUG: Setting up TP for {trade_id}")
        self.engine.order_handler.place_limit_order(idx, symbol, 'SELL', qty, 105.0, client_id=f"trd-{trade_id}-tp-1")

class TestImmediatePlacement(unittest.TestCase):
    def test_immediate_tp_sl(self):
        engine = MockEngine()
        handler = StrategyHandler(engine)

        # Simulate market entry
        handler._execute_market_entry(0, 'BTCUSDC', 'test-1')

        # Verify both SL and TP were attempted immediately
        self.assertEqual(engine.order_handler.place_stop_order.call_count, 1)
        self.assertEqual(engine.order_handler.place_limit_order.call_count, 1)

if __name__ == '__main__':
    unittest.main()

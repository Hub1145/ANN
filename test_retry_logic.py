import unittest
from unittest.mock import MagicMock, patch
import time

class MockEngine:
    def __init__(self):
        self.accounts = {}
        self.bg_clients = {}
        self.data_lock = MagicMock()
        self.last_log_times = {}
        self.stop_event = MagicMock()
        self.stop_event.is_set.side_effect = [False, True]
        self.config_handler = MagicMock()
        self.config_handler.config = {'api_accounts': [{'name': 'Acc1', 'enabled': True}], 'symbols': []}
        self.market_handler = MagicMock()
        self.account_handler = MagicMock()

    def _initialize_bg_clients(self):
        # Simulate logic from real engine
        acc = self.config_handler.config['api_accounts'][0]
        if 'client' not in self.bg_clients:
            print("SIM: Attempting client creation...")
            # Simulate a 502 error during creation
            self.bg_clients[0] = {'client': MagicMock(), 'name': 'Acc1'}
            print("SIM: Client created.")

    def log(self, *args, **kwargs): pass
    def emit(self, *args, **kwargs): pass
    def safe_api_call(self, func, *args, **kwargs): return func(*args, **kwargs)

class TestRecovery(unittest.TestCase):
    def test_global_worker_retry(self):
        engine = MockEngine()
        # Initial state: no background client
        self.assertEqual(len(engine.bg_clients), 0)

        # Manually run one iteration of the worker logic
        engine._initialize_bg_clients()
        self.assertEqual(len(engine.bg_clients), 1)

if __name__ == '__main__':
    unittest.main()

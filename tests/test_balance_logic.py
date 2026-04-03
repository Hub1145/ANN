import unittest
from unittest.mock import MagicMock
import time

class MockEngine:
    def __init__(self):
        self.accounts = {}
        self.bg_clients = {}
        self.data_lock = MagicMock()
        self.last_log_times = {}
        self.server_ip = "1.2.3.4"
    def safe_api_call(self, func, *args, **kwargs):
        return func(*args, **kwargs)
    def log(self, *args, **kwargs):
        print(f"LOG: {args} {kwargs}")
    def emit(self, *args, **kwargs):
        pass

class AccountHandler:
    def __init__(self, engine):
        self.engine = engine
        self.account_balances = {}
        self.account_errors = {}
        self.account_last_update = {}
        self.open_positions = {}
        self.open_orders = {}

    def update_account_metrics(self, idx, force=False):
        acc = self.engine.accounts.get(idx)
        if not acc:
            acc = self.engine.bg_clients.get(idx)

        if not acc:
            # If we expected a client but don't have one, we should probably report it
            return
        client = acc['client']

        try:
            # Balance (Weight 1)
            balances = self.engine.safe_api_call(client.futures_account_balance)
            print(f"DEBUG: Balances received: {balances}")
            usdc_balance = 0.0
            if balances:
                for b in balances:
                    if b['asset'] == 'USDC':
                        usdc_balance = float(b.get('balance') or 0)
                        break

            self.account_balances[idx] = usdc_balance
            print(f"DEBUG: Setting account_balances[{idx}] = {usdc_balance}")

        except Exception as e:
            print(f"DEBUG: Exception in update_account_metrics: {e}")

class TestBalance(unittest.TestCase):
    def test_usdc_balance(self):
        engine = MockEngine()
        client = MagicMock()
        # Sample response from Binance API for futures_account_balance
        client.futures_account_balance.return_value = [
            {"asset": "USDT", "balance": "100.50", "crossWalletBalance": "100.50"},
            {"asset": "USDC", "balance": "50.25", "crossWalletBalance": "50.25"}
        ]
        engine.bg_clients[0] = {'client': client, 'info': {'name': 'Test'}}

        handler = AccountHandler(engine)
        handler.update_account_metrics(0, force=True)
        self.assertEqual(handler.account_balances[0], 50.25)

    def test_no_usdc_balance(self):
        engine = MockEngine()
        client = MagicMock()
        client.futures_account_balance.return_value = [
            {"asset": "USDT", "balance": "100.50", "crossWalletBalance": "100.50"}
        ]
        engine.bg_clients[0] = {'client': client, 'info': {'name': 'Test'}}

        handler = AccountHandler(engine)
        handler.update_account_metrics(0, force=True)
        # If user has USDT but we only look for USDC, it returns 0.0. This might be what the user means.
        self.assertEqual(handler.account_balances[0], 0.0)

if __name__ == '__main__':
    unittest.main()

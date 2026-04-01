import unittest
from unittest.mock import MagicMock
import time

class MockEngine:
    def __init__(self):
        self.accounts = {0: {}}
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

        if not acc: return
        client = acc['client']

        try:
            balances = self.engine.safe_api_call(client.futures_account_balance)

            total_wallet_balance = 0.0
            if not hasattr(self, 'per_asset_balances'):
                self.per_asset_balances = {}
            if idx not in self.per_asset_balances:
                self.per_asset_balances[idx] = {}

            if balances:
                for b in balances:
                    asset = b.get('asset')
                    bal = float(b.get('balance') or 0)
                    self.per_asset_balances[idx][asset] = bal
                    total_wallet_balance += bal

            self.account_balances[idx] = total_wallet_balance
            print(f"DEBUG: Setting account_balances[{idx}] = {total_wallet_balance}")

            if total_wallet_balance == 0 and idx in self.engine.accounts:
                print(f"DEBUG: Warning: Balance for account {idx} is 0.00")

        except Exception as e:
            print(f"DEBUG: Exception in update_account_metrics: {e}")
            self.account_errors[idx] = str(e)

class TestBalanceImproved(unittest.TestCase):
    def test_multi_asset_balance(self):
        engine = MockEngine()
        client = MagicMock()
        client.futures_account_balance.return_value = [
            {"asset": "USDT", "balance": "100.50", "crossWalletBalance": "100.50"},
            {"asset": "USDC", "balance": "50.25", "crossWalletBalance": "50.25"}
        ]
        engine.accounts[0] = {'client': client, 'info': {'name': 'Test'}}

        handler = AccountHandler(engine)
        handler.update_account_metrics(0, force=True)
        # Should sum USDT and USDC
        self.assertEqual(handler.account_balances[0], 150.75)
        self.assertEqual(handler.per_asset_balances[0]['USDT'], 100.50)
        self.assertEqual(handler.per_asset_balances[0]['USDC'], 50.25)

    def test_zero_balance_warning(self):
        engine = MockEngine()
        client = MagicMock()
        client.futures_account_balance.return_value = []
        engine.accounts[0] = {'client': client, 'info': {'name': 'Test'}}

        handler = AccountHandler(engine)
        handler.update_account_metrics(0, force=True)
        self.assertEqual(handler.account_balances[0], 0.0)

if __name__ == '__main__':
    unittest.main()

import json
import logging
import os

class ConfigHandler:
    def __init__(self, config_path):
        self.config_path = config_path
        self.config = self.load_config()

    def load_config(self):
        try:
            if not os.path.exists(self.config_path):
                return {}
            with open(self.config_path, 'r') as f:
                config = json.load(f)

            # Security: Allow environment variables to override empty config keys
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

    def save_config(self, config):
        try:
            with open(self.config_path, 'w') as f:
                json.dump(config, f, indent=2)
            self.config = config
            return True
        except Exception as e:
            logging.error(f"Error saving config: {e}")
            return False

    def get_strategy(self, symbol):
        return self.config.get('symbol_strategies', {}).get(symbol, {})

    def apply_env_overrides(self, config):
        for i, acc in enumerate(config.get('api_accounts', [])):
            env_key = os.environ.get(f'BINANCE_API_KEY_{i+1}')
            env_secret = os.environ.get(f'BINANCE_API_SECRET_{i+1}')
            if env_key and not acc.get('api_key'):
                acc['api_key'] = env_key
            if env_secret and not acc.get('api_secret'):
                acc['api_secret'] = env_secret
        return config

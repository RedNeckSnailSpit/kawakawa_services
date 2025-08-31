# modules/config_handler.py

import json
import mysql.connector
from mysql.connector import Error
from pathlib import Path

class ConfigHandler:
    def __init__(self):
        self.project_root = Path(__file__).resolve().parents[1]
        self.config_path = self.project_root / 'config.json'
        self.config = None

    def prompt_config(self) -> dict:
        # default values
        defaults = {
            "host": "localhost",
            "port": 3306,
            "database": "dev",
            "user": "root",
            "password": ""
        }

        print("🔧  Database configuration setup:")
        host    = input(f"  • Database Host [{defaults['host']}]: ").strip() or defaults['host']
        port_in = input(f"  • Database Port [{defaults['port']}]: ").strip()
        port    = int(port_in) if port_in else defaults['port']
        name    = input(f"  • Database Name [{defaults['database']}]: ").strip() or defaults['database']
        user    = input(f"  • Database Username [{defaults['user']}]: ").strip() or defaults['user']
        pwd     = input(f"  • Database Password [{'<empty>' if not defaults['password'] else defaults['password']}]: ").strip() or defaults['password']

        return {
            "host": host,
            "port": port,
            "database": name,
            "user": user,
            "password": pwd
        }

    def save_config(self, cfg: dict):
        with open(self.config_path, 'w') as f:
            json.dump(cfg, f, indent=2)
        print(f"✅  Saved config to {self.config_path}")

    def load_config(self) -> dict:
        with open(self.config_path, 'r') as f:
            return json.load(f)

    def validate_config(self, cfg: dict) -> bool:
        try:
            conn = mysql.connector.connect(
                host=cfg["host"],
                port=cfg["port"],
                database=cfg["database"],
                user=cfg["user"],
                password=cfg["password"],
                connect_timeout=5
            )
            conn.close()
            return True
        except Error as e:
            print(f"❌  Connection failed: {e}")
            return False

    def get_config(self) -> dict:
        """
        Load existing config or run interactive setup until valid.
        """
        while True:
            if not self.config_path.exists():
                cfg = self.prompt_config()
                self.save_config(cfg)
            else:
                try:
                    cfg = self.load_config()
                    print(f"🔍  Loaded config from {self.config_path}")
                except (json.JSONDecodeError, IOError) as e:
                    print(f"❌  Failed to read config: {e}")
                    cfg = self.prompt_config()
                    self.save_config(cfg)

            if self.validate_config(cfg):
                self.config = cfg
                return cfg

            print("🔄  Invalid config; let's try again.\n")

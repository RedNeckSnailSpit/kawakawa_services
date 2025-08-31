# modules/database_handler.py

import mysql.connector
from modules.config_handler import ConfigHandler


class DatabaseHandler:
    def __init__(self):
        config_handler = ConfigHandler()
        self.db_cfg = config_handler.get_config()
        self._conn = None
        self._ensure_tables()

    def _connect(self):
        if self._conn is None or not self._conn.is_connected():
            self._conn = mysql.connector.connect(
                host=self.db_cfg["host"],
                port=self.db_cfg["port"],
                database=self.db_cfg["database"],
                user=self.db_cfg["user"],
                password=self.db_cfg["password"],
            )
        return self._conn

    def _ensure_tables(self):
        import os
        from modules.constants import DB_HARD_DROP, DB_SOFT_DROP, DB_TABLES, APP_STATE

        # Check if dev features should be enabled
        environment = os.getenv("ENVIRONMENT", "").upper()
        dev_features_enabled = APP_STATE == "dev" and environment == "DEV"

        # Only use drop flags if dev features are enabled
        use_hard_drop = DB_HARD_DROP and dev_features_enabled
        use_soft_drop = DB_SOFT_DROP and dev_features_enabled

        conn = self._connect()
        cursor = conn.cursor()

        # Handle database drops
        if use_hard_drop:
            # Drop all tables
            cursor.execute("SHOW TABLES;")
            tables = [row[0] for row in cursor.fetchall()]
            for table in tables:
                cursor.execute(f"DROP TABLE IF EXISTS `{table}`;")
            print(f"Hard drop: Dropped {len(tables)} tables")

        elif use_soft_drop and DB_TABLES:
            # Drop only specified tables
            dropped_count = 0
            for table in DB_TABLES:
                cursor.execute(f"DROP TABLE IF EXISTS `{table}`;")
                dropped_count += 1
            print(f"Soft drop: Dropped {dropped_count} specified tables")

        # Existing settings tables
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                setting_name VARCHAR(255) PRIMARY KEY,
                setting_value BLOB NOT NULL
            );
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS guild_settings (
                guild_id BIGINT NOT NULL,
                setting_name VARCHAR(255) NOT NULL,
                setting_value BLOB NOT NULL,
                PRIMARY KEY (guild_id, setting_name)
            );
        """)

        # Prosperous Universe tables
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS locations (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            );
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS items (
                id INT AUTO_INCREMENT PRIMARY KEY,
                ticker VARCHAR(10) UNIQUE NOT NULL,
                name VARCHAR(255) NOT NULL,
                category VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            );
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS prices (
                id INT AUTO_INCREMENT PRIMARY KEY,
                ticker VARCHAR(10) NOT NULL,
                location VARCHAR(255) NOT NULL,
                price DECIMAL(10,2) NOT NULL,
                is_default BOOLEAN DEFAULT FALSE,
                last_updated DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY unique_ticker_location (ticker, location),
                FOREIGN KEY (ticker) REFERENCES items(ticker) ON UPDATE CASCADE,
                FOREIGN KEY (location) REFERENCES locations(name) ON UPDATE CASCADE
            );
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS shipping (
                id INT AUTO_INCREMENT PRIMARY KEY,
                from_location VARCHAR(255) NOT NULL,
                to_location VARCHAR(255) NOT NULL,
                cost DECIMAL(10,2) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY unique_route (from_location, to_location),
                FOREIGN KEY (from_location) REFERENCES locations(name) ON UPDATE CASCADE,
                FOREIGN KEY (to_location) REFERENCES locations(name) ON UPDATE CASCADE
            );
        """)

        conn.commit()
        cursor.close()

    def get_tables(self):
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES;")
        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return tables

    def get_setting(self, name: str) -> bytes | None:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT setting_value FROM settings WHERE setting_name = %s;",
            (name,)
        )
        row = cursor.fetchone()
        cursor.close()
        return row[0] if row else None

    def upsert_setting(self, name: str, value: bytes) -> None:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO settings (setting_name, setting_value)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE
              setting_value = VALUES(setting_value);
        """, (name, value))
        conn.commit()
        cursor.close()

    def get_guild_setting(self, guild_id: int, name: str) -> bytes | None:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT setting_value FROM guild_settings WHERE guild_id = %s AND setting_name = %s;",
            (guild_id, name)
        )
        row = cursor.fetchone()
        cursor.close()
        return row[0] if row else None

    def upsert_guild_setting(self, guild_id: int, name: str, value: bytes) -> None:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO guild_settings (guild_id, setting_name, setting_value)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
              setting_value = VALUES(setting_value);
        """, (guild_id, name, value))
        conn.commit()
        cursor.close()

    # === Prosperous Universe specific methods ===

    def batch_upsert_locations(self, locations: list) -> None:
        """Batch insert or update locations."""
        if not locations:
            return

        conn = self._connect()
        cursor = conn.cursor()

        # Remove duplicates while preserving order
        unique_locations = list(dict.fromkeys(locations))
        values = [(loc,) for loc in unique_locations if loc.strip()]

        if values:
            cursor.executemany("""
                INSERT INTO locations (name) VALUES (%s)
                ON DUPLICATE KEY UPDATE name = VALUES(name);
            """, values)
            conn.commit()
            print(f"📍 Batch upserted {len(values)} locations")
        cursor.close()

    def batch_upsert_items(self, items: list) -> None:
        """Batch insert or update items. Items should be tuples of (ticker, name, category)."""
        if not items:
            return

        conn = self._connect()
        cursor = conn.cursor()

        # Remove duplicates by ticker while preserving latest data
        items_dict = {}
        for ticker, name, category in items:
            if ticker and ticker.strip():
                items_dict[ticker.strip()] = (ticker.strip(), name.strip(), category)

        values = list(items_dict.values())

        if values:
            cursor.executemany("""
                INSERT INTO items (ticker, name, category) VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    name = VALUES(name),
                    category = VALUES(category);
            """, values)
            conn.commit()
            print(f"📦 Batch upserted {len(values)} items")
        cursor.close()

    def batch_upsert_prices(self, prices: list) -> None:
        """Batch insert or update prices. Prices should be tuples of (ticker, location, price, is_default, last_updated)."""
        if not prices:
            return

        conn = self._connect()
        cursor = conn.cursor()

        # Remove duplicates by ticker-location pair while preserving latest data
        prices_dict = {}
        for ticker, location, price, is_default, last_updated in prices:
            if ticker and location:
                key = (ticker.strip(), location.strip())
                prices_dict[key] = (ticker.strip(), location.strip(), price, is_default, last_updated)

        values = list(prices_dict.values())

        if values:
            cursor.executemany("""
                INSERT INTO prices (ticker, location, price, is_default, last_updated) 
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    price = VALUES(price),
                    is_default = VALUES(is_default),
                    last_updated = VALUES(last_updated);
            """, values)
            conn.commit()
            print(f"💰 Batch upserted {len(values)} prices")
        cursor.close()

    def batch_upsert_shipping(self, shipping_routes: list) -> None:
        """Batch insert or update shipping routes. Routes should be tuples of (from_location, to_location, cost)."""
        if not shipping_routes:
            return

        conn = self._connect()
        cursor = conn.cursor()

        # Remove duplicates by route pair while preserving latest data
        routes_dict = {}
        for from_loc, to_loc, cost in shipping_routes:
            if from_loc and to_loc:
                key = (from_loc.strip(), to_loc.strip())
                routes_dict[key] = (from_loc.strip(), to_loc.strip(), cost)

        values = list(routes_dict.values())

        if values:
            cursor.executemany("""
                INSERT INTO shipping (from_location, to_location, cost) 
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE cost = VALUES(cost);
            """, values)
            conn.commit()
            print(f"🚛 Batch upserted {len(values)} shipping routes")
        cursor.close()

    # Legacy single-record methods (kept for backwards compatibility)
    def upsert_location(self, name: str) -> None:
        """Insert or update a single location."""
        self.batch_upsert_locations([name])

    def upsert_item(self, ticker: str, name: str, category: str = None) -> None:
        """Insert or update a single item."""
        self.batch_upsert_items([(ticker, name, category)])

    def upsert_price(self, ticker: str, location: str, price: float, is_default: bool = False,
                     last_updated: str = None) -> None:
        """Insert or update a single price."""
        self.batch_upsert_prices([(ticker, location, price, is_default, last_updated)])

    def upsert_shipping(self, from_location: str, to_location: str, cost: float) -> None:
        """Insert or update a single shipping route."""
        self.batch_upsert_shipping([(from_location, to_location, cost)])

    def get_price(self, ticker: str, location: str = None) -> float | None:
        """Get price for a ticker at a specific location, or default price if location not specified."""
        conn = self._connect()
        cursor = conn.cursor()

        if location:
            # Try to get specific location price first
            cursor.execute(
                "SELECT price FROM prices WHERE ticker = %s AND location = %s;",
                (ticker, location)
            )
            row = cursor.fetchone()
            if row:
                cursor.close()
                return float(row[0])

            # Fall back to default price
            cursor.execute(
                "SELECT price FROM prices WHERE ticker = %s AND is_default = TRUE;",
                (ticker,)
            )
            row = cursor.fetchone()
            cursor.close()
            return float(row[0]) if row else None
        else:
            # Get default price
            cursor.execute(
                "SELECT price FROM prices WHERE ticker = %s AND is_default = TRUE;",
                (ticker,)
            )
            row = cursor.fetchone()
            cursor.close()
            return float(row[0]) if row else None

    def get_shipping_cost(self, from_location: str, to_location: str) -> float | None:
        """Get shipping cost between two locations."""
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT cost FROM shipping WHERE from_location = %s AND to_location = %s;",
            (from_location, to_location)
        )
        row = cursor.fetchone()
        cursor.close()
        return float(row[0]) if row else None

    def get_all_items(self) -> list:
        """Get all items with their tickers and names."""
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("SELECT ticker, name, category FROM items ORDER BY ticker;")
        items = cursor.fetchall()
        cursor.close()
        return items

    def get_all_locations(self) -> list:
        """Get all location names."""
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM locations ORDER BY name;")
        locations = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return locations
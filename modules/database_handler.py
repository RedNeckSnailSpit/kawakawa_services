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

        # NEW INVENTORY TABLES

        # Players/Users table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS players (
                id INT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(255) UNIQUE NOT NULL,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Planets table (for location mapping)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS planets (
                id VARCHAR(36) PRIMARY KEY,  -- Planet ID from FIO API
                identifier VARCHAR(50),      -- Planet identifier like "UV-351a"
                name VARCHAR(255) NOT NULL,  -- Planet name like "Katoa"
                founded_epoch_ms BIGINT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Sites table (bases on planets)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sites (
                id VARCHAR(36) PRIMARY KEY,  -- Site ID from FIO API
                planet_id VARCHAR(36),
                username VARCHAR(255) NOT NULL,
                invested_permits INT DEFAULT 0,
                maximum_permits INT DEFAULT 3,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (planet_id) REFERENCES planets(id) ON UPDATE CASCADE,
                FOREIGN KEY (username) REFERENCES players(username) ON UPDATE CASCADE
            );
        """)

        # Ships table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ships (
                addressable_id VARCHAR(36) PRIMARY KEY,  -- Ship's location ID
                name VARCHAR(255) NOT NULL,
                username VARCHAR(255) NOT NULL,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (username) REFERENCES players(username) ON UPDATE CASCADE
            );
        """)

        # Storage containers table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS storage_containers (
                storage_id VARCHAR(36) PRIMARY KEY,  -- Storage ID from FIO API
                addressable_id VARCHAR(36),          -- Location ID (links to ships/sites)
                username VARCHAR(255) NOT NULL,
                container_name VARCHAR(255),         -- Name (for ships) or NULL
                storage_type ENUM('FTL_FUEL_STORE', 'STL_FUEL_STORE', 'SHIP_STORE', 'WAREHOUSE_STORE', 'STORE') NOT NULL,
                weight_capacity DECIMAL(10,2) DEFAULT 0,
                weight_load DECIMAL(10,2) DEFAULT 0,
                volume_capacity DECIMAL(10,2) DEFAULT 0,
                volume_load DECIMAL(10,2) DEFAULT 0,
                fixed_store BOOLEAN DEFAULT FALSE,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (username) REFERENCES players(username) ON UPDATE CASCADE
            );
        """)

        # Inventory items table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inventory_items (
                id INT AUTO_INCREMENT PRIMARY KEY,
                storage_id VARCHAR(36) NOT NULL,      -- Links to storage_containers
                material_id VARCHAR(36) NOT NULL,     -- Material ID from FIO API
                material_ticker VARCHAR(10) NOT NULL,
                material_name VARCHAR(255) NOT NULL,
                material_category VARCHAR(36),
                amount INT NOT NULL DEFAULT 0,
                material_weight DECIMAL(8,5) DEFAULT 0,
                material_volume DECIMAL(8,5) DEFAULT 0,
                total_weight DECIMAL(10,2) DEFAULT 0,
                total_volume DECIMAL(10,2) DEFAULT 0,
                material_value DECIMAL(10,2) DEFAULT 0,
                material_value_currency VARCHAR(3) DEFAULT 'CIS',
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY unique_storage_material (storage_id, material_id),
                FOREIGN KEY (storage_id) REFERENCES storage_containers(storage_id) ON DELETE CASCADE,
                FOREIGN KEY (material_ticker) REFERENCES items(ticker) ON UPDATE CASCADE
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

    # === Inventory Management Methods ===

    def upsert_player(self, username: str) -> None:
        """Insert or update a player record."""
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO players (username) VALUES (%s)
            ON DUPLICATE KEY UPDATE username = VALUES(username);
        """, (username,))
        conn.commit()
        cursor.close()

    def upsert_planet(self, planet_id: str, identifier: str, name: str, founded_epoch_ms: int = None) -> None:
        """Insert or update planet information."""
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO planets (id, identifier, name, founded_epoch_ms) 
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                identifier = VALUES(identifier),
                name = VALUES(name),
                founded_epoch_ms = VALUES(founded_epoch_ms);
        """, (planet_id, identifier, name, founded_epoch_ms))
        conn.commit()
        cursor.close()

    def upsert_site(self, site_id: str, planet_id: str, username: str, invested_permits: int = 0,
                    maximum_permits: int = 3) -> None:
        """Insert or update site information."""
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO sites (id, planet_id, username, invested_permits, maximum_permits) 
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                planet_id = VALUES(planet_id),
                invested_permits = VALUES(invested_permits),
                maximum_permits = VALUES(maximum_permits);
        """, (site_id, planet_id, username, invested_permits, maximum_permits))
        conn.commit()
        cursor.close()

    def upsert_ship(self, addressable_id: str, name: str, username: str) -> None:
        """Insert or update ship information."""
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO ships (addressable_id, name, username) 
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                name = VALUES(name);
        """, (addressable_id, name, username))
        conn.commit()
        cursor.close()

    def upsert_storage_container(self, storage_id: str, addressable_id: str, username: str,
                                 container_name: str, storage_type: str, weight_capacity: float,
                                 weight_load: float, volume_capacity: float, volume_load: float,
                                 fixed_store: bool) -> None:
        """Insert or update storage container information."""
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO storage_containers 
            (storage_id, addressable_id, username, container_name, storage_type, 
             weight_capacity, weight_load, volume_capacity, volume_load, fixed_store) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                addressable_id = VALUES(addressable_id),
                container_name = VALUES(container_name),
                storage_type = VALUES(storage_type),
                weight_capacity = VALUES(weight_capacity),
                weight_load = VALUES(weight_load),
                volume_capacity = VALUES(volume_capacity),
                volume_load = VALUES(volume_load),
                fixed_store = VALUES(fixed_store);
        """, (storage_id, addressable_id, username, container_name, storage_type,
              weight_capacity, weight_load, volume_capacity, volume_load, fixed_store))
        conn.commit()
        cursor.close()

    def clear_inventory_items(self, storage_id: str) -> None:
        """Clear all inventory items for a specific storage container."""
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM inventory_items WHERE storage_id = %s;", (storage_id,))
        conn.commit()
        cursor.close()

    def upsert_inventory_item(self, storage_id: str, material_id: str, material_ticker: str,
                              material_name: str, material_category: str, amount: int,
                              material_weight: float, material_volume: float, total_weight: float,
                              total_volume: float, material_value: float,
                              material_value_currency: str = 'CIS') -> None:
        """Insert or update an inventory item."""
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO inventory_items 
            (storage_id, material_id, material_ticker, material_name, material_category, amount, 
             material_weight, material_volume, total_weight, total_volume, material_value, material_value_currency) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                material_ticker = VALUES(material_ticker),
                material_name = VALUES(material_name),
                material_category = VALUES(material_category),
                amount = VALUES(amount),
                material_weight = VALUES(material_weight),
                material_volume = VALUES(material_volume),
                total_weight = VALUES(total_weight),
                total_volume = VALUES(total_volume),
                material_value = VALUES(material_value),
                material_value_currency = VALUES(material_value_currency);
        """, (storage_id, material_id, material_ticker, material_name, material_category, amount,
              material_weight, material_volume, total_weight, total_volume, material_value,
              material_value_currency))
        conn.commit()
        cursor.close()

    def sync_user_inventory_data(self, fio_handler, username: str) -> bool:
        """
        Sync all inventory data for a specific user using FIO API.
        This is the main method that orchestrates the entire inventory sync.
        """
        try:
            print(f"Syncing inventory data for user: {username}")

            # Step 1: Ensure player exists
            self.upsert_player(username)

            # Step 2: Get storage data
            storage_data, storage_status = fio_handler.storage(username)
            if storage_status != 200 or not storage_data:
                print(f"Failed to get storage data for {username} (Status: {storage_status})")
                return False

            # Step 3: Get planets and sites data for location resolution
            planets_data, planets_status = fio_handler.sites_planets(username)
            location_map = {}  # addressable_id -> planet_name

            if planets_status == 200 and planets_data:
                for planet_id in planets_data:
                    sites_data, sites_status = fio_handler.sites(username, planet_id)
                    if sites_status == 200 and sites_data:
                        # Handle both single site and multiple sites
                        sites = sites_data if isinstance(sites_data, list) else [sites_data]

                        for site in sites:
                            # Store planet info
                            self.upsert_planet(
                                planet_id,
                                site.get('PlanetIdentifier'),
                                site.get('PlanetName'),
                                site.get('PlanetFoundedEpochMs')
                            )

                            # Store site info
                            site_id = site.get('SiteId')
                            if site_id:
                                self.upsert_site(
                                    site_id, planet_id, username,
                                    site.get('InvestedPermits', 0),
                                    site.get('MaximumPermits', 3)
                                )
                                location_map[site_id] = site.get('PlanetName')

            # Step 4: Process storage data
            ships_found = set()  # Track ships we've seen

            for storage in storage_data:
                addressable_id = storage.get('AddressableId')
                storage_id = storage.get('StorageId')
                storage_type = storage.get('Type')
                container_name = storage.get('Name')

                # Handle ships (they have names and move around)
                if storage_type in ['SHIP_STORE', 'FTL_FUEL_STORE',
                                    'STL_FUEL_STORE'] and container_name and container_name != 'None':
                    if addressable_id not in ships_found:
                        self.upsert_ship(addressable_id, container_name, username)
                        ships_found.add(addressable_id)

                # Store storage container info
                self.upsert_storage_container(
                    storage_id, addressable_id, username, container_name, storage_type,
                    storage.get('WeightCapacity', 0), storage.get('WeightLoad', 0),
                    storage.get('VolumeCapacity', 0), storage.get('VolumeLoad', 0),
                    storage.get('FixedStore', False)
                )

                # Clear existing inventory items for this container
                self.clear_inventory_items(storage_id)

                # Add inventory items
                storage_items = storage.get('StorageItems', [])
                for item in storage_items:
                    # Also ensure the item exists in the items table
                    self.upsert_item(
                        item.get('MaterialTicker'),
                        item.get('MaterialName'),
                        item.get('MaterialCategory')
                    )

                    # Add to inventory
                    self.upsert_inventory_item(
                        storage_id, item.get('MaterialId'), item.get('MaterialTicker'),
                        item.get('MaterialName'), item.get('MaterialCategory'),
                        item.get('MaterialAmount', 0), item.get('MaterialWeight', 0),
                        item.get('MaterialVolume', 0), item.get('TotalWeight', 0),
                        item.get('TotalVolume', 0), item.get('MaterialValue', 0),
                        item.get('MaterialValueCurrency', 'CIS')
                    )

            print(f"Successfully synced inventory data for {username}")
            return True

        except Exception as e:
            print(f"Error syncing inventory data for {username}: {e}")
            return False

    def get_user_inventory_summary(self, username: str) -> dict:
            """Get a summary of a user's inventory across all locations."""
            conn = self._connect()
            cursor = conn.cursor()

            # Get storage containers with location info
            cursor.execute("""
                SELECT 
                    sc.storage_id, sc.container_name, sc.storage_type,
                    sc.weight_load, sc.weight_capacity, sc.volume_load, sc.volume_capacity,
                    p.name as planet_name, s.name as ship_name
                FROM storage_containers sc
                LEFT JOIN sites site ON sc.addressable_id = site.id
                LEFT JOIN planets p ON site.planet_id = p.id
                LEFT JOIN ships s ON sc.addressable_id = s.addressable_id
                WHERE sc.username = %s
                ORDER BY sc.storage_type, sc.container_name;
            """, (username,))

            containers = cursor.fetchall()

            # Get inventory items count by container
            cursor.execute("""
                SELECT sc.storage_id, COUNT(ii.id) as item_count, SUM(ii.amount) as total_items
                FROM storage_containers sc
                LEFT JOIN inventory_items ii ON sc.storage_id = ii.storage_id
                WHERE sc.username = %s
                GROUP BY sc.storage_id;
            """, (username,))

            item_counts = {row[0]: {'unique_items': row[1], 'total_quantity': row[2]} for row in cursor.fetchall()}
            cursor.close()

            summary = {
                'username': username,
                'containers': [],
                'totals': {
                    'containers': len(containers),
                    'total_weight_used': 0,
                    'total_weight_capacity': 0,
                    'total_volume_used': 0,
                    'total_volume_capacity': 0
                }
            }

            for container in containers:
                storage_id, container_name, storage_type, weight_load, weight_capacity, volume_load, volume_capacity, planet_name, ship_name = container

                location = planet_name or ship_name or "Unknown"
                counts = item_counts.get(storage_id, {'unique_items': 0, 'total_quantity': 0})

                summary['containers'].append({
                    'storage_id': storage_id,
                    'name': container_name,
                    'type': storage_type,
                    'location': location,
                    'weight_used': float(weight_load),
                    'weight_capacity': float(weight_capacity),
                    'volume_used': float(volume_load),
                    'volume_capacity': float(volume_capacity),
                    'unique_items': counts['unique_items'],
                    'total_quantity': counts['total_quantity'] or 0
                })

                # Add to totals
                summary['totals']['total_weight_used'] += float(weight_load)
                summary['totals']['total_weight_capacity'] += float(weight_capacity)
                summary['totals']['total_volume_used'] += float(volume_load)
                summary['totals']['total_volume_capacity'] += float(volume_capacity)

            return summary
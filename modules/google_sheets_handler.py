# modules/google_sheets_handler.py

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import gspread
from google.oauth2.service_account import Credentials

from modules.database_handler import DatabaseHandler


class GoogleSheetsHandler:
    def __init__(self):
        self.db = DatabaseHandler()
        self.project_root = Path(__file__).resolve().parents[1]
        self.credentials_path = self.project_root / 'google.json'
        self.client = None
        self._authenticate()

    def _authenticate(self):
        """Authenticate with Google Sheets API using service account credentials."""
        if not self.credentials_path.exists():
            print(f"❌ Google credentials file not found at {self.credentials_path}")
            print("Please follow these steps to get your credentials:")
            print("1. Go to https://console.cloud.google.com/")
            print("2. Create a new project or select an existing one")
            print("3. Enable the Google Sheets API")
            print("4. Create a service account")
            print("5. Download the JSON key file")
            print("6. Rename it to 'google.json' and place it in your project root")
            print("7. Share your Google Sheet with the service account email")
            raise FileNotFoundError("Google credentials file not found")

        try:
            # Define the scope
            scope = [
                'https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive'
            ]

            # Load credentials
            credentials = Credentials.from_service_account_file(
                self.credentials_path,
                scopes=scope
            )

            # Create client
            self.client = gspread.authorize(credentials)
            print("✅ Successfully authenticated with Google Sheets API")

        except Exception as e:
            print(f"❌ Failed to authenticate with Google Sheets: {e}")
            raise

    def open_spreadsheet(self, spreadsheet_id: str):
        """Open a Google Spreadsheet by ID."""
        try:
            return self.client.open_by_key(spreadsheet_id)
        except Exception as e:
            print(f"❌ Failed to open spreadsheet: {e}")
            raise

    def parse_prices_sheet(self, worksheet) -> Tuple[Optional[str], List[Dict]]:
        """
        Parse the Prices sheet and return last updated date and price data.
        Returns: (last_updated_date, list_of_price_records)
        """
        try:
            # Get all values from the sheet
            all_values = worksheet.get_all_values()

            if len(all_values) < 3:
                print("❌ Prices sheet doesn't have enough rows")
                return None, []

            # Extract last updated date from D1
            last_updated = None
            if len(all_values) > 0 and len(all_values[0]) > 3:
                updated_cell = all_values[0][3]  # D1
                if updated_cell:
                    # Extract date from format like "Updated 2023-Feb-19"
                    date_match = re.search(r'(\d{4}-\w{3}-\d{2})', updated_cell)
                    if date_match:
                        try:
                            last_updated = datetime.strptime(date_match.group(1), '%Y-%b-%d').date()
                        except ValueError:
                            print(f"⚠️  Could not parse date from: {updated_cell}")

            # Extract headers from row 2 (index 1)
            if len(all_values) < 2:
                print("❌ No header row found")
                return last_updated, []

            headers = all_values[1]  # Row 2
            print(f"📋 Headers found: {headers}")

            # Prepare batch data
            all_locations = set()
            all_items = []
            all_prices = []
            price_records = []

            # Process data starting from row 3 (index 2)
            for i in range(2, len(all_values), 2):  # Step by 2 for alternating rows
                if i + 1 >= len(all_values):
                    break

                item_row = all_values[i]  # Odd row (3, 5, 7, ...)
                price_row = all_values[i + 1]  # Even row (4, 6, 8, ...)

                # Extract ticker and name from columns B and C
                if len(item_row) < 3:
                    continue

                ticker = item_row[1].strip()  # Column B
                item_name = item_row[2].strip()  # Column C

                if not ticker or not item_name:
                    continue

                print(f"🔍 Processing {ticker} - {item_name}")

                # Extract category from column A if available
                category = item_row[0].strip() if len(item_row) > 0 else None

                # Add to batch items list
                all_items.append((ticker, item_name, category))

                # Process locations and prices from column D onwards
                item_locations = 0

                for col_idx in range(3, min(len(item_row), len(price_row))):  # Start from column D (index 3)
                    location = item_row[col_idx].strip()
                    price_str = price_row[col_idx].strip()

                    if not location and not price_str:
                        # Empty columns indicate end of data for this item
                        break

                    if location and price_str:
                        try:
                            price = float(price_str)

                            # Add to batch data
                            all_locations.add(location)

                            # Determine if this is the default price (column E)
                            is_default = col_idx == 4  # Column E (index 4)

                            # Add to batch prices
                            all_prices.append((ticker, location, price, is_default, last_updated))

                            # Keep for return data
                            price_records.append({
                                'ticker': ticker,
                                'item_name': item_name,
                                'location': location,
                                'price': price,
                                'is_default': is_default,
                                'category': category
                            })

                            item_locations += 1

                        except ValueError:
                            print(f"  ⚠️  Invalid price '{price_str}' for {location}")

                print(f"  ✅ Found {item_locations} prices for {ticker}")

            # Perform batch operations
            print(f"🚀 Starting batch operations...")
            self.db.batch_upsert_locations(list(all_locations))
            self.db.batch_upsert_items(all_items)
            self.db.batch_upsert_prices(all_prices)

            print(f"📊 Processed {len(price_records)} total price records")
            return last_updated, price_records

        except Exception as e:
            print(f"❌ Error parsing prices sheet: {e}")
            return None, []

    def parse_shipping_sheet(self, worksheet) -> List[Dict]:
        """
        Parse the Shipping sheet and return shipping data.
        Returns: list_of_shipping_records
        """
        try:
            # Get all values from the sheet
            all_values = worksheet.get_all_values()

            if len(all_values) < 2:
                print("❌ Shipping sheet doesn't have enough data")
                return []

            # Row 1 contains "From" locations (starting from column B)
            from_locations = []
            if len(all_values) > 0:
                from_row = all_values[0]
                for i in range(1, len(from_row)):  # Start from column B (index 1)
                    location = from_row[i].strip()
                    if location:
                        from_locations.append(location)

            print(f"🚚 From locations: {from_locations}")

            # Prepare batch data
            all_locations = set(from_locations)
            all_shipping_routes = []
            shipping_records = []

            # Process each row starting from row 2
            for row_idx in range(1, len(all_values)):
                row = all_values[row_idx]

                if len(row) < 1:
                    continue

                # Column A contains "To" location
                to_location = row[0].strip()
                if not to_location:
                    continue

                # Add to batch locations
                all_locations.add(to_location)

                # Process shipping costs for each from_location
                for col_idx in range(1, min(len(row), len(from_locations) + 1)):
                    if col_idx - 1 >= len(from_locations):
                        break

                    from_location = from_locations[col_idx - 1]
                    cost_str = row[col_idx].strip()

                    if cost_str:
                        try:
                            cost = float(cost_str)

                            # Add to batch shipping routes
                            all_shipping_routes.append((from_location, to_location, cost))

                            # Keep for return data
                            shipping_records.append({
                                'from_location': from_location,
                                'to_location': to_location,
                                'cost': cost
                            })

                        except ValueError:
                            print(f"  ⚠️  Invalid cost '{cost_str}' for {from_location} → {to_location}")

            # Perform batch operations
            print(f"🚀 Starting shipping batch operations...")
            self.db.batch_upsert_locations(list(all_locations))
            self.db.batch_upsert_shipping(all_shipping_routes)

            print(f"📦 Processed {len(shipping_records)} shipping routes")
            return shipping_records

        except Exception as e:
            print(f"❌ Error parsing shipping sheet: {e}")
            return []

    def sync_spreadsheet_data(self, spreadsheet_id: str, prices_sheet_name: str = "Prices",
                              shipping_sheet_name: str = "Shipping"):
        """
        Main method to sync data from Google Sheets to database.
        """
        print(f"🔄 Starting sync for spreadsheet: {spreadsheet_id}")

        try:
            # Open the spreadsheet
            spreadsheet = self.open_spreadsheet(spreadsheet_id)
            print(f"📊 Opened spreadsheet: {spreadsheet.title}")

            # Process Prices sheet
            try:
                prices_sheet = spreadsheet.worksheet(prices_sheet_name)
                print(f"📋 Processing {prices_sheet_name} sheet...")
                last_updated, price_records = self.parse_prices_sheet(prices_sheet)
                if last_updated:
                    print(f"📅 Prices last updated: {last_updated}")
            except Exception as e:
                print(f"⚠️  Could not process {prices_sheet_name} sheet: {e}")
                price_records = []

            # Process Shipping sheet
            try:
                shipping_sheet = spreadsheet.worksheet(shipping_sheet_name)
                print(f"🚚 Processing {shipping_sheet_name} sheet...")
                shipping_records = self.parse_shipping_sheet(shipping_sheet)
            except Exception as e:
                print(f"⚠️  Could not process {shipping_sheet_name} sheet: {e}")
                shipping_records = []

            print(f"✅ Sync completed!")
            print(f"   📊 Price records: {len(price_records)}")
            print(f"   🚚 Shipping records: {len(shipping_records)}")

            return {
                'prices': price_records,
                'shipping': shipping_records,
                'last_updated': last_updated
            }

        except Exception as e:
            print(f"❌ Failed to sync spreadsheet: {e}")
            raise
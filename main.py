import os
import time
import signal
import sys
from datetime import datetime, timedelta
from modules.database_handler import DatabaseHandler
from modules.google_sheets_handler import GoogleSheetsHandler
from modules.constants import (
    APP_NAME, APP_VERSION, APP_STATE,
    CONFIG_HARD_DROP, DB_HARD_DROP, DB_SOFT_DROP, DB_TABLES
)


class SyncScheduler:
    """Handles scheduling and running of sync operations."""

    def __init__(self):
        self.running = True
        self.db = None
        self.sheets_handler = None
        self.last_sync_time = None

        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        print(f"\n📴 Received signal {signum}, shutting down gracefully...")
        self.running = False

    def _get_next_midnight(self):
        """Calculate the next midnight datetime."""
        now = datetime.now()
        next_midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        return next_midnight

    def _get_time_until_next_sync(self):
        """Get seconds until next midnight sync."""
        now = datetime.now()
        next_midnight = self._get_next_midnight()
        return (next_midnight - now).total_seconds()

    def _format_countdown(self, seconds):
        """Format seconds into a readable countdown string."""
        if seconds <= 0:
            return "Sync starting..."

        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)

        if hours > 0:
            return f"{hours:02d}h {minutes:02d}m {secs:02d}s"
        else:
            return f"{minutes:02d}m {secs:02d}s"

    def _display_status(self, seconds_until_sync):
        """Display current status with countdown."""
        countdown = self._format_countdown(seconds_until_sync)
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        status_line = f"\r🕐 {current_time} | Next sync in: {countdown}"
        if self.last_sync_time:
            last_sync_str = self.last_sync_time.strftime("%Y-%m-%d %H:%M:%S")
            status_line += f" | Last sync: {last_sync_str}"

        print(status_line, end="", flush=True)

    def run_continuous_sync(self):
        """Main loop for continuous sync operation."""
        print(f"🚀 Starting continuous sync mode...")
        print(f"📅 Sync will occur daily at midnight (server time)")
        print(f"⏹️  Press Ctrl+C to stop gracefully\n")

        # Run initial sync on startup
        print("🔄 Running initial sync...")
        success = self._perform_sync()

        if success:
            self.last_sync_time = datetime.now()
            print(f"\n✅ Initial sync completed at {self.last_sync_time.strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            print(f"\n❌ Initial sync failed")

        print("\n" + "=" * 70)
        print("📊 Entering continuous monitoring mode...")
        print("=" * 70)

        # Main monitoring loop
        while self.running:
            try:
                seconds_until_sync = self._get_time_until_next_sync()

                # Display status
                self._display_status(seconds_until_sync)

                # Check if it's time to sync (within 1 second of midnight)
                if seconds_until_sync <= 1:
                    print(f"\n\n🔄 Starting scheduled sync at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    print("=" * 50)

                    success = self._perform_sync()

                    if success:
                        self.last_sync_time = datetime.now()
                        print(f"✅ Scheduled sync completed at {self.last_sync_time.strftime('%Y-%m-%d %H:%M:%S')}")
                    else:
                        print(f"❌ Scheduled sync failed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

                    print("=" * 70)
                    print("📊 Returning to monitoring mode...")
                    print("=" * 70)

                # Sleep for 1 second before next check
                time.sleep(1)

            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"\n❌ Error in main loop: {e}")
                if should_enable_dev_features():
                    import traceback
                    traceback.print_exc()
                time.sleep(5)  # Wait before retrying

        print(f"\n👋 Sync scheduler stopped at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    def _perform_sync(self):
        """Perform the actual sync operation."""
        try:
            # Ensure handlers are initialized
            if not self.db:
                self.db = DatabaseHandler()
            if not self.sheets_handler:
                self.sheets_handler = GoogleSheetsHandler()

            return sync_sheets_data(self.sheets_handler, self.db)
        except Exception as e:
            print(f"❌ Sync operation failed: {e}")
            if should_enable_dev_features():
                import traceback
                traceback.print_exc()
            return False


def get_version_string(version, state):
    """Return version string with suffix based on state."""
    allowed_states = {"dev", "staging", "alpha", "beta", "release"}
    if state not in allowed_states:
        return f"v{version}"

    if state in ("dev", "staging", "alpha", "beta"):
        suffix = state[0]
        return f"v{version}{suffix}"
    else:
        return f"v{version}"


def should_enable_dev_features():
    """Check if development features should be enabled."""
    environment = os.getenv("ENVIRONMENT", "").upper()
    return APP_STATE == "dev" and environment == "DEV"


def get_stored_settings(db):
    """Retrieve stored Google Sheets settings from database."""
    try:
        spreadsheet_id = db.get_setting('google_sheets_id')
        prices_sheet = db.get_setting('google_prices_sheet')
        shipping_sheet = db.get_setting('google_shipping_sheet')

        settings = {
            'spreadsheet_id': spreadsheet_id.decode('utf-8') if spreadsheet_id else None,
            'prices_sheet': prices_sheet.decode('utf-8') if prices_sheet else None,
            'shipping_sheet': shipping_sheet.decode('utf-8') if shipping_sheet else None
        }

        print(
            f"🔍 Loaded settings: ID={settings['spreadsheet_id'][:10] + '...' if settings['spreadsheet_id'] else 'None'}, "
            f"Prices='{settings['prices_sheet']}', Shipping='{settings['shipping_sheet']}'")

        return settings
    except Exception as e:
        print(f"⚠️  Could not load stored settings: {e}")
        return {
            'spreadsheet_id': None,
            'prices_sheet': None,
            'shipping_sheet': None
        }


def save_sheets_settings(db, spreadsheet_id, prices_sheet, shipping_sheet):
    """Save Google Sheets settings to database."""
    try:
        db.upsert_setting('google_sheets_id', spreadsheet_id.encode('utf-8'))
        db.upsert_setting('google_prices_sheet', prices_sheet.encode('utf-8'))
        db.upsert_setting('google_shipping_sheet', shipping_sheet.encode('utf-8'))
        print("💾 Settings saved to database")
    except Exception as e:
        print(f"⚠️  Could not save settings: {e}")


def sync_sheets_data(sheets_handler, db):
    """Handle Google Sheets synchronization."""
    print("\n🔄 Google Sheets Synchronization")
    print("=" * 50)

    # Load stored settings
    stored_settings = get_stored_settings(db)

    # Get spreadsheet ID - ask if not stored
    if stored_settings['spreadsheet_id']:
        print(f"📊 Using stored spreadsheet ID: {stored_settings['spreadsheet_id'][:10]}...")
        spreadsheet_id = stored_settings['spreadsheet_id']
    else:
        print("📊 No spreadsheet ID found. Please configure:")
        spreadsheet_id = input("Enter your Google Sheets ID: ").strip()
        if not spreadsheet_id:
            print("❌ No spreadsheet ID provided, skipping sync")
            return False

    # Get sheet names - ask if not stored
    if stored_settings['prices_sheet'] and stored_settings['shipping_sheet']:
        prices_sheet = stored_settings['prices_sheet']
        shipping_sheet = stored_settings['shipping_sheet']
        print(f"📋 Using stored sheet names: Prices='{prices_sheet}', Shipping='{shipping_sheet}'")
    else:
        print("📋 Sheet names not configured. Please enter:")
        prices_sheet = input("Prices sheet name [Prices]: ").strip() or 'Prices'
        shipping_sheet = input("Shipping sheet name [Shipping]: ").strip() or 'Shipping'

    # Save settings if any were entered
    if not stored_settings['spreadsheet_id'] or not stored_settings['prices_sheet'] or not stored_settings[
        'shipping_sheet']:
        save_sheets_settings(db, spreadsheet_id, prices_sheet, shipping_sheet)

    try:
        print(f"\n🚀 Starting sync for spreadsheet: {spreadsheet_id}")
        result = sheets_handler.sync_spreadsheet_data(
            spreadsheet_id,
            prices_sheet,
            shipping_sheet
        )

        print("\n📈 Sync Results:")
        print("=" * 30)
        print(f"✅ Price records synced: {len(result['prices'])}")
        print(f"✅ Shipping routes synced: {len(result['shipping'])}")
        if result['last_updated']:
            print(f"📅 Prices last updated: {result['last_updated']}")

        return True

    except Exception as e:
        print(f"❌ Sync failed: {e}")
        if should_enable_dev_features():
            import traceback
            traceback.print_exc()
        return False


def run_setup_mode():
    """Interactive setup mode for configuring Google Sheets settings."""
    print("🔧 Setup Mode")
    print("=" * 50)

    try:
        # Initialize database
        print("🗄️  Initializing database connection...")
        db = DatabaseHandler()

        # Get current settings
        stored_settings = get_stored_settings(db)

        print("\n📊 Current Settings:")
        print(
            f"   Spreadsheet ID: {stored_settings['spreadsheet_id'][:10] + '...' if stored_settings['spreadsheet_id'] else 'Not set'}")
        print(f"   Prices Sheet: {stored_settings['prices_sheet'] or 'Not set'}")
        print(f"   Shipping Sheet: {stored_settings['shipping_sheet'] or 'Not set'}")

        # Get new settings
        print("\n📝 Enter new settings (press Enter to keep current):")

        new_spreadsheet_id = input(
            f"Google Sheets ID [{stored_settings['spreadsheet_id'][:10] + '...' if stored_settings['spreadsheet_id'] else 'None'}]: ").strip()
        if not new_spreadsheet_id and stored_settings['spreadsheet_id']:
            new_spreadsheet_id = stored_settings['spreadsheet_id']

        new_prices_sheet = input(f"Prices sheet name [{stored_settings['prices_sheet'] or 'Prices'}]: ").strip()
        if not new_prices_sheet:
            new_prices_sheet = stored_settings['prices_sheet'] or 'Prices'

        new_shipping_sheet = input(f"Shipping sheet name [{stored_settings['shipping_sheet'] or 'Shipping'}]: ").strip()
        if not new_shipping_sheet:
            new_shipping_sheet = stored_settings['shipping_sheet'] or 'Shipping'

        if new_spreadsheet_id:
            save_sheets_settings(db, new_spreadsheet_id, new_prices_sheet, new_shipping_sheet)
            print("\n✅ Settings updated successfully!")

            # Test the connection
            test_sync = input("\n🧪 Test sync with new settings? (y/N): ").strip().lower()
            if test_sync == 'y':
                sheets_handler = GoogleSheetsHandler()
                success = sync_sheets_data(sheets_handler, db)
                if success:
                    print("\n🎉 Test sync successful! You can now run in continuous mode.")
                else:
                    print("\n❌ Test sync failed. Please check your settings.")
        else:
            print("\n❌ No spreadsheet ID provided. Settings not updated.")

    except Exception as e:
        print(f"\n❌ Setup failed: {e}")
        if should_enable_dev_features():
            import traceback
            traceback.print_exc()


def main():
    # Generate version string
    version_string = get_version_string(APP_VERSION, APP_STATE)
    print(f"🚀 {APP_NAME} {version_string}")
    print("   Prosperous Universe Data Sync")
    print("=" * 50)

    # Check if dev features should be enabled
    dev_features_enabled = should_enable_dev_features()
    if dev_features_enabled:
        print("🔧 Development features enabled")

    # Check for command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] in ['--setup', '-s']:
            run_setup_mode()
            return
        elif sys.argv[1] in ['--help', '-h']:
            print("\nUsage:")
            print("  python main.py           - Run continuous sync mode")
            print("  python main.py --setup   - Run interactive setup mode")
            print("  python main.py --help    - Show this help message")
            return

    try:
        # Initialize and run continuous sync
        scheduler = SyncScheduler()
        scheduler.run_continuous_sync()

    except KeyboardInterrupt:
        print("\n\n👋 Interrupted by user, goodbye!")

    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        if dev_features_enabled:
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    main()
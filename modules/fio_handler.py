import requests
from modules.database_handler import DatabaseHandler


class FIOHandler:
    def __init__(self):
        """Initialize the FIO Handler and ensure API key is configured."""
        self.db_handler = DatabaseHandler()
        self.api_key = None
        self.base_url = "https://rest.fnar.net"

        # Get or configure API key during initialization
        self._setup_api_key()

    def _setup_api_key(self):
        """Set up the API key by retrieving from DB or prompting user."""
        # Try to get existing API key from database
        stored_key = self.db_handler.get_setting('fio_api_key')

        if stored_key is not None:
            # Convert bytes back to string if needed
            if isinstance(stored_key, bytes):
                stored_key = stored_key.decode('utf-8')

            # Verify the stored key is still valid (silent check)
            if self._validate_key_silent(stored_key):
                self.api_key = stored_key
                print("✓ FIO API key loaded and verified successfully")
                return
            else:
                print("⚠ Stored API key is no longer valid")

        # No valid key found, get one from user
        self._prompt_for_api_key()

    def _prompt_for_api_key(self):
        """Prompt user for API key until a valid one is provided."""
        while True:
            try:
                api_key = input("Please enter your FIO API key: ").strip()

                if not api_key:
                    print("❌ API key cannot be empty. Please try again.")
                    continue

                # Test the API key
                if self.auth(api_key):
                    # Save valid key to database
                    self.db_handler.upsert_setting('fio_api_key', api_key.encode('utf-8'))
                    self.api_key = api_key
                    print("✓ API key saved and verified successfully")
                    break
                else:
                    print("❌ Invalid API key. Please check your key and try again.")

            except KeyboardInterrupt:
                print("\n\n❌ Setup cancelled by user. Exiting...")
                exit(1)
            except Exception as e:
                print(f"❌ Error during API key setup: {e}")
                print("Please try again.")

    def _validate_key_silent(self, api_key: str) -> bool:
        """
        Validate API key without printing authentication messages.

        Args:
            api_key (str): The FIO API key to validate

        Returns:
            bool: True if authentication successful, False otherwise
        """
        try:
            headers = {
                'Authorization': api_key,
                'User-Agent': 'FIO-Handler/1.0'
            }

            response = requests.get(
                f"{self.base_url}/auth",
                headers=headers,
                timeout=10
            )

            return response.status_code == 200

        except requests.exceptions.RequestException:
            return False
        except Exception:
            return False

    def auth(self, api_key: str) -> bool:
        """
        Authenticate with the FIO API using the provided API key.

        Args:
            api_key (str): The FIO API key to validate

        Returns:
            bool: True if authentication successful, False otherwise
        """
        try:
            headers = {
                'Authorization': api_key,
                'User-Agent': 'FIO-Handler/1.0'
            }

            response = requests.get(
                f"{self.base_url}/auth",
                headers=headers,
                timeout=10
            )

            if response.status_code == 200:
                username = response.text.strip()
                print(f"✓ Authenticated as: {username}")
                return True
            elif response.status_code == 401:
                return False
            else:
                print(f"⚠ Unexpected response from auth endpoint: {response.status_code}")
                return False

        except requests.exceptions.RequestException as e:
            print(f"❌ Network error during authentication: {e}")
            return False
        except Exception as e:
            print(f"❌ Unexpected error during authentication: {e}")
            return False

    def get_username(self) -> str:
        """
        Get the username associated with the current API key.

        Returns:
            str: Username if successful, None if failed
        """
        if not self.api_key:
            print("❌ No API key configured")
            return None

        try:
            headers = {
                'Authorization': self.api_key,
                'User-Agent': 'FIO-Handler/1.0'
            }

            response = requests.get(
                f"{self.base_url}/auth",
                headers=headers,
                timeout=10
            )

            if response.status_code == 200:
                return response.text.strip()
            else:
                print(f"❌ Failed to get username: HTTP {response.status_code}")
                return None

        except Exception as e:
            print(f"❌ Error getting username: {e}")
            return None

    def is_authenticated(self) -> bool:
        """Check if the handler has a valid API key configured."""
        return self.api_key is not None and self._validate_key_silent(self.api_key)

    def company_code(self, company_code: str):
        """
        Get company information by company code.

        Args:
            company_code (str): The company code to look up

        Returns:
            tuple: (json_data, status_code) or (None, None) if request failed
        """
        if not self.api_key:
            return None, None

        try:
            headers = {
                'Authorization': self.api_key,
                'User-Agent': 'FIO-Handler/1.0',
                'Accept': 'application/json'
            }

            response = requests.get(
                f"{self.base_url}/company/code/{company_code}",
                headers=headers,
                timeout=10
            )

            # Handle JSON response
            try:
                json_data = response.json() if response.text.strip() else None
            except:
                json_data = None

            return json_data, response.status_code

        except Exception as e:
            print(f"❌ Error in company_code request: {e}")
            return None, None

    def company_name(self, company_name: str):
        """
        Get company information by company name.

        Args:
            company_name (str): The company name to look up

        Returns:
            tuple: (json_data, status_code) or (None, None) if request failed
        """
        if not self.api_key:
            return None, None

        try:
            headers = {
                'Authorization': self.api_key,
                'User-Agent': 'FIO-Handler/1.0',
                'Accept': 'application/json'
            }

            response = requests.get(
                f"{self.base_url}/company/name/{company_name}",
                headers=headers,
                timeout=10
            )

            # Handle JSON response
            try:
                json_data = response.json() if response.text.strip() else None
            except:
                json_data = None

            return json_data, response.status_code

        except Exception as e:
            print(f"❌ Error in company_name request: {e}")
            return None, None

    def production(self, username: str):
        """
        Get production information for a user.

        Args:
            username (str): The username to get production data for

        Returns:
            tuple: (json_data, status_code) or (None, None) if request failed
        """
        if not self.api_key:
            return None, None

        try:
            headers = {
                'Authorization': self.api_key,
                'User-Agent': 'FIO-Handler/1.0',
                'Accept': 'application/json'
            }

            response = requests.get(
                f"{self.base_url}/production/{username}",
                headers=headers,
                timeout=10
            )

            # Handle JSON response
            try:
                json_data = response.json() if response.text.strip() else None
            except:
                json_data = None

            return json_data, response.status_code

        except Exception as e:
            print(f"❌ Error in production request: {e}")
            return None, None

    def sites_planets(self, username: str):
        """
        Get list of planet IDs where a user has sites.

        Args:
            username (str): The username to get planet list for

        Returns:
            tuple: (json_data, status_code) or (None, None) if request failed
        """
        if not self.api_key:
            return None, None

        try:
            headers = {
                'Authorization': self.api_key,
                'User-Agent': 'FIO-Handler/1.0',
                'Accept': 'application/json'
            }

            response = requests.get(
                f"{self.base_url}/sites/planets/{username}",
                headers=headers,
                timeout=10
            )

            # Handle JSON response
            try:
                json_data = response.json() if response.text.strip() else None
            except:
                json_data = None

            return json_data, response.status_code

        except Exception as e:
            print(f"❌ Error in sites_planets request: {e}")
            return None, None

    def sites(self, username: str, planet_identifier: str):
        """
        Get site information for a user on a specific planet.

        Args:
            username (str): The username to get site data for
            planet_identifier (str): Planet ID, Natural ID, or name

        Returns:
            tuple: (json_data, status_code) or (None, None) if request failed
        """
        if not self.api_key:
            return None, None

        try:
            headers = {
                'Authorization': self.api_key,
                'User-Agent': 'FIO-Handler/1.0',
                'Accept': 'application/json'
            }

            response = requests.get(
                f"{self.base_url}/sites/{username}/{planet_identifier}",
                headers=headers,
                timeout=10
            )

            # Handle JSON response
            try:
                json_data = response.json() if response.text.strip() else None
            except:
                json_data = None

            return json_data, response.status_code

        except Exception as e:
            print(f"❌ Error in sites request: {e}")
            return None, None

    def storage(self, username: str):
        """
        Get storage information for a user.

        Args:
            username (str): The username to get storage data for

        Returns:
            tuple: (json_data, status_code) or (None, None) if request failed
        """
        if not self.api_key:
            return None, None

        try:
            headers = {
                'Authorization': self.api_key,
                'User-Agent': 'FIO-Handler/1.0',
                'Accept': 'application/json'
            }

            response = requests.get(
                f"{self.base_url}/storage/{username}",
                headers=headers,
                timeout=10
            )

            # Handle JSON response
            try:
                json_data = response.json() if response.text.strip() else None
            except:
                json_data = None

            return json_data, response.status_code

        except Exception as e:
            print(f"❌ Error in storage request: {e}")
            return None, None

    def sites_warehouses(self, username: str):
        """
        Get warehouse information for a user.

        Args:
            username (str): The username to get warehouse data for

        Returns:
            tuple: (json_data, status_code) or (None, None) if request failed
        """
        if not self.api_key:
            return None, None

        try:
            headers = {
                'Authorization': self.api_key,
                'User-Agent': 'FIO-Handler/1.0',
                'Accept': 'application/json'
            }

            response = requests.get(
                f"{self.base_url}/sites/warehouses/{username}",
                headers=headers,
                timeout=10
            )

            # Handle JSON response
            try:
                json_data = response.json() if response.text.strip() else None
            except:
                json_data = None

            return json_data, response.status_code

        except Exception as e:
            print(f"❌ Error in sites_warehouses request: {e}")
            return None, None

    def burnrate(self, username: str):
        """
        Get burnrate (consumption) information for a user in CSV format.

        Args:
            username (str): The username to get burnrate data for

        Returns:
            tuple: (csv_data, status_code) or (None, None) if request failed
        """
        if not self.api_key:
            return None, None

        try:
            # Note: This endpoint uses apikey as query parameter instead of Authorization header
            response = requests.get(
                f"{self.base_url}/csv/burnrate",
                params={
                    'apikey': self.api_key,
                    'username': username
                },
                headers={
                    'User-Agent': 'FIO-Handler/1.0',
                    'Accept': 'application/csv'
                },
                timeout=10
            )

            # Return raw CSV text instead of trying to parse as JSON
            csv_data = response.text if response.text.strip() else None
            return csv_data, response.status_code

        except Exception as e:
            print(f"❌ Error in burnrate request: {e}")
            return None, None


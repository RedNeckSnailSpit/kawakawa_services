# Kawakawa Services

Backend services for **Kawakawa Corporation**, providing automation and integration to support the web frontend:

- Sync item prices to the database  
- Sync shipping prices to the database  
- Planned: Discord & Telegram bots for cross-platform notifications and commands  

This project serves as the core backend logic for Kawakawa’s internal tools.

## Quickstart Guide

Follow these steps to get **Kawakawa Services** up and running.

### Prerequisites

- **MariaDB/MySQL**: [Install Guide](https://mariadb.com/kb/en/getting-installing-and-upgrading-mariadb/)  
- **Python 3.13+**: [Download Python](https://www.python.org/downloads/) (Windows: make sure to check 'add to path' during installation)

### Google API Credentials

1. Go to the [Google Cloud Console](https://console.cloud.google.com/).  
2. Create a project and enable the **Google Sheets API**.  
3. Create **Service Account credentials** and download the JSON key file.  
4. Save the file as `google.json` in the root directory of the project.

### Optional: Virtual Environment

It’s recommended to use a virtual environment to isolate dependencies.

**Linux / macOS:**

```bash
python3 -m venv venv
source venv/bin/activate
````

**Windows (cmd):**

```cmd
python -m venv venv
venv\Scripts\activate
```

**Windows (PowerShell):**

```powershell
python -m venv venv
venv\Scripts\Activate.ps1
```

### Install Dependencies

```bash
pip install -r requirements.txt
```
**Note**: On some systems you may need to use `pip3` instead of `pip` 

### Run the Script

```bash
python main.py   # or `python3 main.py` depending on your environment
```

1. Enter your **MariaDB connection details** when prompted. The script will test the connection and save it to `config.json`. **Make sure to keep your config.json secure, it contains plain text credentials to your DB**
2. Script will create all required tables in the DB, you just need to create the user and database
3. Enter your **Google Sheets ID** (for a link like `https://docs.google.com/spreadsheets/d/101234567890L6gpFKoGLiPRt1MyzUYzghsGCBmaLlU4/edit`, the ID is `101234567890L6gpFKoGLiPRt1MyzUYzghsGCBmaLlU4`).
4. Enter the names of the **Pricing** and **Shipping** sheets. These will be saved for reuse.
5. The script will now run daily at **midnight server time**, syncing prices with the database. It’s recommended to run it inside a `screen`, `tmux`, or another background service.

## License

This project is released under the **GNU General Public License v3.0 (GPL-3.0)**.  
See the [LICENSE](LICENSE) file for details.

## Repository

Source code is hosted at:  
[https://github.com/RedNeckSnailSpit/kawakawa_services](https://github.com/RedNeckSnailSpit/kawakawa_services)

---

© 2025 RedNeckSnailSpit. Licensed under GPL v3.

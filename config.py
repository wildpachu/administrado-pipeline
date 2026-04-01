from dotenv import load_dotenv
import os

load_dotenv()

# Credentials
USERNAME = os.getenv("APP_USERNAME")
PASSWORD = os.getenv("PASSWORD")

# Feature flags
EXTRACT_OWN_ENABLED = True

# Own store
OWN_STORE_NAME = "IMAGEN"

# Google Sheets
CREDENTIALS_FILE = "credentials.json"
GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/1rZCFFgrQnuXvwEtjdXG9lCH69CoFgt1eN95HskGlCa8/edit?usp=sharing"

# Store IDs
STORE_IDS = [
    "232136079",
    "1823597259"
]

COMPETITOR_IDS = [
    "ce6473c7ebcd2e6ddb359288e329c49a368229bb078992a3f777b29fe66076aa",
    "d301bdf0327cb4193a0738dcbb66245325efbae3b33850a24424f3f36642c2dd",
    "a0b3e270cde291185512fbd8efeb667daae2d856260767241132bff9785c2cef",
    "51e5c7df8ce0016749c74ba1d86f5ef159c8b9309694b03c90656c5701ad7943",
    "f11a5121980baa4241ba184f55c247c20e489a5dff93f0b7543add2d5fb47f83",
    "873ae8e1b014962db0554d4843682d1fb6cdf72dd3b86ef4149e4eff0705f9e1",
    "ffea22ed753d3da1d1025e3abe86924b8834ad257df6874fbdc79d1720837968"
]

# Paths
PATH_RAW_OWN         = "data/raw/own"
PATH_RAW_MARKET      = "data/raw/market"
PATH_PROCESSED_OWN   = "data/processed/own"
PATH_PROCESSED_MARKET = "data/processed/market"
PATH_LOGS            = "logs"

# Timeouts (milliseconds)
DOWNLOAD_TIMEOUT    = 90000
NAVIGATION_TIMEOUT  = 3000


def validate():
    """Raises EnvironmentError if required config values are missing."""
    missing = [name for name, val in [("APP_USERNAME", USERNAME), ("PASSWORD", PASSWORD)] if not val]
    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {missing}. "
            f"Copy .env.example to .env and fill in your credentials."
        )
    if not os.path.exists(CREDENTIALS_FILE):
        raise FileNotFoundError(
            f"Google credentials file not found: '{CREDENTIALS_FILE}'. "
            f"Place your service account JSON at the project root."
        )

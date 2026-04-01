import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe


def _get_client(credentials_file: str) -> gspread.Client:
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive',
    ]
    creds = Credentials.from_service_account_file(credentials_file, scopes=scopes)
    return gspread.authorize(creds)


def upload_dataframe(df: pd.DataFrame, credentials_file: str, sheet_url: str, worksheet: str) -> None:
    """Uploads a DataFrame to the given worksheet, replacing existing content."""
    gc = _get_client(credentials_file)
    sheet = gc.open_by_url(sheet_url).worksheet(worksheet)
    sheet.clear()
    set_with_dataframe(sheet, df)


def append_dataframe(df: pd.DataFrame, credentials_file: str, sheet_url: str, worksheet: str) -> None:
    """Appends a DataFrame below existing content in the given worksheet."""
    gc = _get_client(credentials_file)
    sheet = gc.open_by_url(sheet_url).worksheet(worksheet)
    existing = sheet.get_all_values()
    # If sheet is empty, write with header; otherwise skip header row
    start_row = len(existing) + 1
    set_with_dataframe(sheet, df, row=start_row, include_column_header=(start_row == 1))


def read_sheet(credentials_file: str, sheet_url: str, worksheet: str) -> pd.DataFrame:
    """Reads a Google Sheets worksheet and returns a DataFrame."""
    gc = _get_client(credentials_file)
    sheet = gc.open_by_url(sheet_url).worksheet(worksheet)
    return pd.DataFrame(sheet.get_all_records())

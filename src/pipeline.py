"""src/pipeline.py — Core ETL orchestration for Sheet Pipeline.

Exposes run_pipeline() used by both the CLI (main.py) and the desktop UI
(ui/mockup.py). Keeping the logic here avoids duplication and ensures both
entry points always run the same pipeline.
"""
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta

import pandas as pd

from config import (
    USERNAME, PASSWORD,
    STORE_IDS, COMPETITOR_IDS,
    PATH_RAW_OWN, PATH_RAW_MARKET,
    PATH_PROCESSED_OWN, PATH_PROCESSED_MARKET,
    CREDENTIALS_FILE, GOOGLE_SHEET_URL,
    EXTRACT_OWN_ENABLED,
)
from src.extract.extract_own import download_store_sales
from src.extract.extract_market import download_competitor_reports
from src.transform.transform import process_market_data
from src.transform.transform_own import process_own_data, build_mla_dict, build_sku_attributes
from src.load.load_sheets import read_sheet, upload_dataframe, append_dataframe


@dataclass
class PipelineResult:
    """Holds row counts from a completed pipeline run."""
    market_rows: int
    own_rows: int

    @property
    def total_rows(self) -> int:
        return self.market_rows + self.own_rows


def get_target_dates() -> list[str]:
    """Returns the list of dates (YYYYMMDD strings) to process.

    Monday → [Friday, Saturday, Sunday] to catch the full weekend.
    Any other day → [yesterday].
    """
    today = datetime.now()
    if today.weekday() == 0:  # Monday
        return [
            (today - timedelta(days=3)).strftime('%Y%m%d'),
            (today - timedelta(days=2)).strftime('%Y%m%d'),
            (today - timedelta(days=1)).strftime('%Y%m%d'),
        ]
    return [(today - timedelta(days=1)).strftime('%Y%m%d')]


def build_sku_dict(df_skus: pd.DataFrame) -> dict[str, str]:
    """Builds a {tipo+dimension: sku} lookup dict from the Maestro SKU sheet.

    Example: a row with 'con cm'="Blackout 160x200cm" and 'SKU'="CORT0001"
    produces the key "Blackout160x200cm" → "CORT0001".
    """
    df_skus['reference'] = df_skus['con cm'].astype(str).str.replace(' ', '')
    return dict(zip(df_skus['reference'], df_skus['SKU']))


def run_pipeline() -> PipelineResult:
    """Runs the full ETL pipeline: extract → transform → load.

    Assumes setup_dirs() and logging have already been configured by the caller.
    Returns a PipelineResult with row counts for each data source.
    """
    target_dates = get_target_dates()
    date_suffix  = target_dates[0] if len(target_dates) == 1 else f"{target_dates[0]}_{target_dates[-1]}"
    logging.info(f"Processing dates: {target_dates}")

    # --- EXTRACT ---
    if EXTRACT_OWN_ENABLED:
        download_store_sales(USERNAME, PASSWORD, STORE_IDS)

    for date in target_dates:
        d = f"{date[6:8]}-{date[4:6]}-{date[0:4]}"
        download_competitor_reports(USERNAME, PASSWORD, COMPETITOR_IDS, date_start=d, date_end=d)

    # --- LOAD REFERENCE DATA ---
    logging.info("Loading reference sheets...")
    df_skus = read_sheet(CREDENTIALS_FILE, GOOGLE_SHEET_URL, 'Maestro SKU')
    df_mla  = read_sheet(CREDENTIALS_FILE, GOOGLE_SHEET_URL, 'Maestro MLA')

    # --- TRANSFORM: MARKET ---
    logging.info("Starting market transform...")
    df_market = process_market_data(
        folder_path=PATH_RAW_MARKET,
        sku_dict=build_sku_dict(df_skus),
        target_dates=target_dates,
        output_path=f"{PATH_PROCESSED_MARKET}/market_clean_{date_suffix}.csv",
    )

    # --- TRANSFORM: OWN ---
    df_own = None
    if EXTRACT_OWN_ENABLED:
        logging.info("Starting own transform...")
        df_own = process_own_data(
            folder_path=PATH_RAW_OWN,
            mla_dict=build_mla_dict(df_mla),
            sku_attributes=build_sku_attributes(df_skus),
            target_dates=target_dates,
            output_path=f"{PATH_PROCESSED_OWN}/own_clean_{date_suffix}.csv",
        )

    # --- LOAD ---
    if not df_market.empty:
        logging.info("Uploading market data to Google Sheets...")
        upload_dataframe(df_market, CREDENTIALS_FILE, GOOGLE_SHEET_URL, 'Limpios')

    if df_own is not None and not df_own.empty:
        logging.info("Appending own data to Google Sheets...")
        append_dataframe(df_own, CREDENTIALS_FILE, GOOGLE_SHEET_URL, 'Limpios')

    market_rows = len(df_market) if not df_market.empty else 0
    own_rows    = len(df_own)    if df_own is not None and not df_own.empty else 0

    if market_rows + own_rows == 0:
        logging.warning("No data to upload.")
    else:
        logging.info("Pipeline complete.")

    return PipelineResult(market_rows=market_rows, own_rows=own_rows)

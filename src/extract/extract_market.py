"""src/extract/extract_market.py — Downloads competitor price reports from Administrado."""
import logging
from pathlib import Path
from datetime import datetime, timedelta

from playwright.sync_api import sync_playwright

from config import PATH_RAW_MARKET, DOWNLOAD_TIMEOUT, NAVIGATION_TIMEOUT
from src.extract.auth import login


def download_competitor_reports(
    username: str,
    password: str,
    competitor_ids_list: list[str],
    date_start: str | None = None,
    date_end: str | None = None,
) -> None:
    """Downloads competitor price reports for a given date range from Administrado.

    Each competitor × date combination produces one .xlsx file saved to
    PATH_RAW_MARKET. Date strings use DD-MM-YYYY format.

    Args:
        username: Administrado account username or email.
        password: Administrado account password.
        competitor_ids_list: List of Administrado competitor hash IDs.
        date_start: Start date as "DD-MM-YYYY". Defaults to yesterday.
        date_end: End date as "DD-MM-YYYY". Defaults to date_start.
    """
    download_path = Path(PATH_RAW_MARKET)
    download_path.mkdir(parents=True, exist_ok=True)

    if date_start is None:
        start = datetime.now() - timedelta(days=1)
        end = start
    elif date_end is None:
        start = datetime.strptime(date_start, "%d-%m-%Y")
        end = start
    else:
        start = datetime.strptime(date_start, "%d-%m-%Y")
        end = datetime.strptime(date_end, "%d-%m-%Y")

    dates = [start + timedelta(days=x) for x in range((end - start).days + 1)]

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        login(page, username, password)

        for date_obj in dates:
            date_str = date_obj.strftime("%d-%m-%Y")
            logging.info(f"=============================================")
            logging.info(f"   DOWNLOADING DATE: {date_str}")
            logging.info(f"=============================================")

            for i, competitor_id in enumerate(competitor_ids_list, 1):
                logging.info(f"--- [{i}/{len(competitor_ids_list)}] COMPETITOR: {competitor_id[:8]}... ---")

                url = (
                    f"https://www.administrado.net/seller/competidores_v3/{competitor_id}"
                    f"?plazo=personalizado&inicio={date_str}&fin={date_str}"
                )
                page.goto(url)
                page.wait_for_load_state("networkidle")
                page.wait_for_timeout(NAVIGATION_TIMEOUT)

                try:
                    with page.expect_download(timeout=DOWNLOAD_TIMEOUT) as download_info:
                        page.get_by_text("Descargar Excel", exact=False).first.click()

                    download = download_info.value
                    filename = download.suggested_filename
                    if not filename.endswith(".xlsx"):
                        filename += ".xlsx"

                    download.save_as(download_path / filename)
                    logging.info(f"Saved: {filename}")

                except Exception as e:
                    logging.error(f"Error on competitor {competitor_id[:8]}...: {e}", exc_info=True)

        logging.info("--- PROCESS COMPLETE ---")
        context.close()
        browser.close()

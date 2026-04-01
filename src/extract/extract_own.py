"""src/extract/extract_own.py — Downloads own-store sales reports from Administrado."""
import logging
from pathlib import Path

from playwright.sync_api import sync_playwright

from config import PATH_RAW_OWN, DOWNLOAD_TIMEOUT, NAVIGATION_TIMEOUT
from src.extract.auth import login


def download_store_sales(username: str, password: str, store_ids_list: list[str]) -> None:
    """Downloads the latest weekly sales Excel for each store in store_ids_list.

    Navigates Administrado's UI via Playwright, switches store context for each
    ID, and saves the resulting .xlsx files to PATH_RAW_OWN.

    Args:
        username: Administrado account username or email.
        password: Administrado account password.
        store_ids_list: List of Administrado store IDs (fvp parameter values).
    """
    download_path = Path(PATH_RAW_OWN)
    download_path.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        login(page, username, password)

        for i, store_id in enumerate(store_ids_list, 1):
            logging.info(f"--- [{i}/{len(store_ids_list)}] PROCESSING STORE: {store_id} ---")

            # Switch store context
            page.goto(f"https://www.administrado.net/seller/preguntas/estadisticas?fvp={store_id}")
            page.wait_for_load_state("networkidle")
            page.wait_for_timeout(2000)
            page.reload()

            # Navigate to sales
            logging.info("Navigating to sales section...")
            page.goto("https://www.administrado.net/seller/ventas3")
            page.wait_for_load_state("networkidle")
            page.wait_for_timeout(NAVIGATION_TIMEOUT)

            try:
                page.get_by_text("Descargar Excel de ventas", exact=False).first.click()

                download_option = page.get_by_text("ltima semana", exact=False).first
                download_option.wait_for(state="visible", timeout=5000)

                with page.expect_download(timeout=DOWNLOAD_TIMEOUT) as download_info:
                    download_option.click(force=True)

                download = download_info.value
                filename = download.suggested_filename
                if not filename.endswith(".xlsx"):
                    filename += ".xlsx"

                download.save_as(download_path / filename)
                logging.info(f"Saved: {filename}")

            except Exception as e:
                logging.error(f"Error on store {store_id}: {e}", exc_info=True)

        logging.info("--- PROCESS COMPLETE ---")
        context.close()
        browser.close()

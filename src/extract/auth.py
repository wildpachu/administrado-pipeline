"""src/extract/auth.py — Administrado login helper."""
import logging
from playwright.sync_api import Page


def login(page: Page, username: str, password: str) -> None:
    """Authenticates against Administrado using an existing Playwright page.

    Args:
        page: An active Playwright page object.
        username: Administrado account username or email.
        password: Administrado account password.
    """
    logging.info("Logging in to Administrado...")
    page.goto("https://www.administrado.net/login")
    page.fill('input[type="text"], input[type="email"]', username)
    page.fill('input[type="password"]', password)
    page.locator('button[type="submit"], button:has-text("Ingresar")').first.click()
    page.wait_for_load_state("networkidle")

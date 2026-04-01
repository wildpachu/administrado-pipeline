"""main.py — CLI entry point for Sheet Pipeline.

Usage:
    python main.py
"""
from config import validate as validate_config
from src.utils.utils import setup_dirs, setup_logger
from src.pipeline import run_pipeline


if __name__ == "__main__":
    setup_dirs()
    setup_logger()
    validate_config()
    run_pipeline()
